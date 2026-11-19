import { after, before, beforeEach, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { randomBytes } from 'node:crypto'
import type { AddressInfo } from 'node:net'
import { migrate, searchWriterSeen, type Db } from '../src/db.js'
import type { EventDoc } from '../src/index/assemble.js'
import { Handler, type JetstreamEvent } from '../src/ingest/handler.js'
import { Projector } from '../src/ingest/projector.js'
import { incompleteAmong, setIndexState } from '../src/ingest/store.js'
import { CursorError, decodeCursor, encodeCursor } from '../src/query/cursor.js'
import { bindingOf, parseEventQuery } from '../src/query/params.js'
import { createQueryServer } from '../src/query/server.js'
import { XrpcError, getEvent, queryEvents, type QueryDeps, type Search } from '../src/query/service.js'
import { bindingHash } from '../src/query/cursor.js'
import { SearchControl, controlPool, readControl } from '../src/search/control.js'
import { PostgresBackend, pageSql, searchPool } from '../src/search/postgres.js'
import { SearchUpkeep, comparable } from '../src/search/verify.js'
import { EVENT, SIDECAR, eventValue, geo, sidecarValue } from './fixtures.js'
import pg from 'pg'
import { databaseUrl, freshDb } from './support.js'

const dbUrl = (name: string) => {
  const url = new URL(databaseUrl!)
  url.pathname = `/${name}`
  return url.toString()
}

const AUTHOR = 'did:plc:ewvi7nxzyoun6zhxrhs64oiz'
const uriOf = (n: number) => `at://${AUTHOR}/${SIDECAR}/3ks${String(n).padStart(10, '0')}`

/** A complete document at a point, with a name that tells versions apart. */
function doc(n: number, name = `event ${n}`, lat = 37.78 + n / 1000, lon = -122.43): EventDoc {
  const uri = uriOf(n)
  const eventUri = `at://${AUTHOR}/${EVENT}/3ke${String(n).padStart(10, '0')}`
  return {
    uri,
    eventUri,
    did: AUTHOR,
    sidecarCid: 'bafysidecar',
    eventCid: 'bafyevent',
    view: { uri, eventUri, event: eventValue({ name }), location: geo(String(lat), String(lon)) },
    point: { lat, lon },
    resolved: true,
    resolution: { status: 'resolved', latitude: lat, longitude: lon } as never,
    startsAt: '2026-10-16T01:30:00.000Z',
    endsAt: '2026-10-16T03:30:00.000Z',
    timeValid: true,
    indexedAt: new Date().toISOString(),
  }
}

const unavailable = (e: unknown) => e instanceof XrpcError && e.status === 503 && e.error === 'ServiceUnavailable'
const expired = (e: unknown) => e instanceof CursorError && e.error === 'CursorExpired'

describe('Postgres search backend', { skip: !databaseUrl && 'TEST_DATABASE_URL not set' }, () => {
  let db: Db
  let pg: PostgresBackend
  const q = (params: string) => parseEventQuery(new URLSearchParams(params))
  const names = async (snapshot: string, params = 'limit=100') =>
    (await pg.page(snapshot, q(params), undefined, 100)).hits.map((h) => (h.doc.view.event as { name: string }).name)

  before(async () => {
    db = await freshDb('pg_search_test')
    pg = new PostgresBackend(db)
  })
  beforeEach(async () => {
    await db.query('truncate event_docs')
  })
  after(() => db.end())

  it('gives every put and remove a new, higher version, keeping one current row per event', async () => {
    const v1 = await pg.put(doc(1, 'first'))
    const v2 = await pg.put(doc(1, 'second'))
    const v3 = await pg.remove(uriOf(1))
    const v4 = await pg.remove(uriOf(2))
    assert.ok(v1 < v2 && v2 < v3 && v3 < v4)
    assert.equal(await pg.get(uriOf(1)), undefined)
    assert.equal(await pg.count(), 0)
    const rows = await db.query<{ version: string; closed_version: string | null }>('select version, closed_version from event_docs order by version')
    assert.deepEqual(
      rows.rows.map((r) => [BigInt(r.version), r.closed_version && BigInt(r.closed_version)]),
      [
        [v1, v2],
        [v2, v3],
      ],
    )
    await pg.put(doc(1, 'third'))
    assert.equal(((await pg.get(uriOf(1)))!.view.event as { name: string }).name, 'third')
  })

  it('shows each paging session the data as of its snapshot, whatever is written after', async () => {
    await pg.put(doc(1, 'one'))
    await pg.put(doc(2, 'two'))
    const before = await pg.openSnapshot()
    await pg.put(doc(1, 'one, edited'))
    await pg.remove(uriOf(2))
    await pg.put(doc(3, 'three'))
    assert.deepEqual((await names(before)).sort(), ['one', 'two'])
    assert.deepEqual((await names(await pg.openSnapshot())).sort(), ['one, edited', 'three'])
  })

  it('never shows an event twice or skips one while it is edited between pages', async () => {
    for (let n = 1; n <= 6; n++) await pg.put(doc(n))
    const params = 'latitude=37.78&longitude=-122.43&limit=2'
    const snapshot = await pg.openSnapshot()
    const first = await pg.page(snapshot, q(params), undefined, 2)
    // Move event 1 (already shown) to the far end and event 6 (not yet shown) to the front.
    await pg.put(doc(1, 'moved away', 37.9))
    await pg.put(doc(6, 'moved near', 37.78))
    let after = first.hits[1].sort
    const seen = first.hits.map((h) => h.doc.uri)
    for (;;) {
      const page = await pg.page(snapshot, q(params), after, 2)
      seen.push(...page.hits.map((h) => h.doc.uri))
      if (page.hits.length < 2) break
      after = page.hits[1].sort
    }
    assert.deepEqual(seen, [1, 2, 3, 4, 5, 6].map(uriOf))
  })

  it('sees every write that finished before a snapshot opened and none that started after', async () => {
    const writes: { start: number; end: number; version: bigint }[] = []
    const snapshots: { start: number; end: number; w: bigint }[] = []
    const t = () => performance.now()
    await Promise.all([
      ...Array.from({ length: 4 }, (_, writer) =>
        (async () => {
          for (let i = 0; i < 25; i++) {
            const start = t()
            const version = i % 5 === 4 ? await pg.remove(uriOf(writer)) : await pg.put(doc(writer, `w${writer} ${i}`))
            writes.push({ start, end: t(), version })
          }
        })(),
      ),
      (async () => {
        for (let i = 0; i < 40; i++) {
          const start = t()
          const w = BigInt(await pg.openSnapshot())
          snapshots.push({ start, end: t(), w })
          await new Promise((r) => setImmediate(r))
        }
      })(),
    ])
    let checked = 0
    for (const s of snapshots) {
      for (const w of writes) {
        if (w.end < s.start) assert.ok(w.version <= s.w, 'a finished write is in the snapshot'), checked++
        if (w.start > s.end) assert.ok(w.version > s.w, 'a later write is not'), checked++
      }
    }
    assert.ok(checked > 100)
  })

  it('stores, filters and pages start times in any year the API accepts, 0000 included', async () => {
    const at = (n: number, startsAt: string) => ({ ...doc(n), startsAt, endsAt: undefined })
    await pg.put(at(1, '0000-01-01T00:00:00.000Z'))
    await pg.put(at(2, '0001-06-01T12:00:00.500Z'))
    await pg.put(at(3, '2026-10-16T01:30:00.123Z'))
    const snapshot = await pg.openSnapshot()
    const windowed = await pg.page(snapshot, q('windowStart=0000-01-01T00:00:00Z&windowEnd=0002-01-01T00:00:00Z&sort=startsAt'), undefined, 10)
    assert.deepEqual(windowed.hits.map((h) => h.doc.uri), [uriOf(1), uriOf(2)])
    const first = await pg.page(snapshot, q('sort=startsAt&limit=1'), undefined, 1)
    assert.equal(first.hits[0].sort[0], Date.parse('0000-01-01T00:00:00.000Z'))
    const rest = await pg.page(snapshot, q('sort=startsAt&limit=1'), first.hits[0].sort, 10)
    assert.deepEqual(rest.hits.map((h) => h.doc.uri), [uriOf(2), uriOf(3)])
    const next = await pg.page(snapshot, q('sort=startsAt&limit=1'), rest.hits[0].sort, 10)
    assert.deepEqual(next.hits.map((h) => h.doc.uri), [uriOf(3)], 'millisecond-exact keyset')
  })

  it('pages by start time on the (starts_at, uri) index', async () => {
    const { text, values } = pageSql('9223372036854775807', q('sort=startsAt&limit=10'), [Date.parse('2026-01-01'), uriOf(1)], 11)
    const client = await db.connect()
    try {
      await client.query('set enable_seqscan = off; set enable_bitmapscan = off; set enable_sort = off')
      const plan = (await client.query(`explain ${text}`, values)).rows.map((r) => r['QUERY PLAN']).join('\n')
      assert.match(plan, /event_docs_current_start/)
      // The current-rows branch walks the index in order: no Sort between its Limit and the scan.
      const current = plan.slice(plan.indexOf('*SELECT* 1'), plan.indexOf('*SELECT* 2'))
      assert.ok(current.includes('event_docs_current_start'), current)
      assert.ok(!/Sort/.test(current.replace(/Sort Key: .*$/gm, '').replace(/Sort Method: .*$/gm, '')), `the current-rows branch sorts:\n${current}`)
    } finally {
      await client.query('reset enable_seqscan; reset enable_bitmapscan; reset enable_sort').catch(() => {})
      client.release()
    }
  })

  it('gives up waiting for a search connection after 2 seconds when all five are busy', async () => {
    const pool = searchPool(dbUrl('pg_search_test'))
    const held: pg.PoolClient[] = []
    try {
      for (let i = 0; i < 5; i++) held.push(await pool.connect())
      const started = Date.now()
      await assert.rejects(new PostgresBackend(db, pool).count(), /timeout/)
      const waited = Date.now() - started
      assert.ok(waited >= 1_900 && waited < 2_600, `gave up after ${waited} ms`)
    } finally {
      for (const c of held) c.release()
      await pool.end()
    }
  })

  it('deletes closed rows 61 minutes after they closed, and not before', async () => {
    await pg.put(doc(1, 'a'))
    await pg.put(doc(1, 'b'))
    await pg.put(doc(1, 'c'))
    await db.query(`update event_docs set closed_at = now() - interval '62 minutes' where doc->'view'->'event'->>'name' = 'a'`)
    await db.query(`update event_docs set closed_at = now() - interval '59 minutes' where doc->'view'->'event'->>'name' = 'b'`)
    assert.equal(await pg.pruneClosed(), 1)
    assert.equal((await db.query('select 1 from event_docs')).rowCount, 2)
  })

  it('stops a search at 2 seconds on the search pool and answers ServiceUnavailable', async () => {
    const pool = searchPool(new URL(databaseUrl!).toString().replace(/\/[^/]*$/, '/pg_search_test'))
    try {
      await assert.rejects(pool.query('select pg_sleep(3)'), (e: unknown) => (e as { code?: string }).code === '57014')
      const slow = new PostgresBackend(db, { query: () => pool.query('select pg_sleep(3)') } as never)
      const deps = {
        db,
        search: { postgres: slow, control: { current: async () => ({ pgVerified: true }), refresh: async () => ({ pgVerified: true }) } as never },
        serviceId: 'test',
        cursorKey: randomBytes(32),
      } as QueryDeps
      await assert.rejects(getEvent(deps, uriOf(1)), unavailable)
    } finally {
      await pool.end()
    }
  })
})

describe('search routing', { skip: !databaseUrl && 'TEST_DATABASE_URL not set' }, () => {
  let db: Db
  let search: Search
  let deps: QueryDeps
  const PARAMS = 'latitude=37.78&longitude=-122.43&limit=2'
  const ask = (params: string, cursor?: string, d = deps) => {
    const sp = new URLSearchParams(params)
    if (cursor) sp.set('cursor', cursor)
    return queryEvents(d, parseEventQuery(sp))
  }
  const setVerified = (v: boolean) => db.query('update search_control set pg_verified = $1, fill_generation = search_generation_now()', [v]).then(() => search.control.refresh())

  before(async () => {
    db = await freshDb('routing_test')
    const postgres = new PostgresBackend(db)
    for (let n = 1; n <= 5; n++) {
      await postgres.put(doc(n))
      await setIndexState(db, uriOf(n), AUTHOR, true, null)
    }
    search = { postgres, control: new SearchControl(db) }
    deps = { db, search, serviceId: 'test', cursorKey: randomBytes(32) }
  })
  after(async () => {
    await db.end()
  })

  it('expires every Elasticsearch cursor, with or without a backend field, and never sends one there', async () => {
    await setVerified(true)
    const page = await ask(PARAMS)
    const bind = bindingHash(bindingOf(parseEventQuery(new URLSearchParams(PARAMS))))
    const state = decodeCursor(page.cursor!, deps.cursorKey, { svc: 'test', bind })
    assert.equal(state.be, 'postgres')
    const { be: _be, ...old } = state
    // Even within elastic_cursors_until (every start still covers an hour).
    await db.query(`update search_control set elastic_cursors_until = now() + interval '61 minutes'`)
    await assert.rejects(ask(PARAMS, encodeCursor(old, deps.cursorKey)), expired)
    await assert.rejects(ask(PARAMS, encodeCursor({ ...old, be: 'elastic' }, deps.cursorKey)), expired)
    assert.equal((await ask(PARAMS, page.cursor)).events.length, 2)
  })

  it('serves from Postgres only once Postgres is verified', async () => {
    const forced = deps
    await setVerified(false)
    await assert.rejects(ask(PARAMS, undefined, forced), unavailable)
    await setVerified(true)
    const page = await ask(PARAMS, undefined, forced)
    assert.equal(forced.search.last?.backend, 'postgres')
    const state = decodeCursor(page.cursor!, deps.cursorKey, { svc: 'test', bind: bindingHash(bindingOf(parseEventQuery(new URLSearchParams(PARAMS)))) })
    assert.equal(state.be, 'postgres')
    // A Postgres cursor is always accepted, whatever elastic_cursors_until says.
    await db.query(`update search_control set elastic_cursors_until = now() - interval '1 second'`)
    await search.control.refresh()
    assert.equal((await ask(PARAMS, page.cursor, forced)).events.length, 2)
  })

  /** search_control on a database whose every query stalls (or answers after `ms`). */
  const slowControl = (ms = Infinity) =>
    new SearchControl({
      query: (text: string, values?: unknown[]) =>
        ms === Infinity ? new Promise(() => {}) : new Promise((r) => setTimeout(r, ms)).then(() => db.query(text, values)),
    } as unknown as Db)

  it('cuts off a stalled search_control read after 500 ms and refuses getEvent rather than hang', async () => {
    const started = Date.now()
    await assert.rejects(getEvent({ ...deps, search: { ...search, control: slowControl() } }, uriOf(2)), unavailable)
    assert.ok(Date.now() - started < 1_500, `took ${Date.now() - started} ms`)
  })

  it('refuses a first page within a second, not hanging, when search_control stalls and Postgres cannot be shown verified', async () => {
    const started = Date.now()
    await assert.rejects(ask(PARAMS, undefined, { ...deps, search: { ...search, control: slowControl() } }), unavailable)
    assert.ok(Date.now() - started < 1_500, `took ${Date.now() - started} ms`)
  })

  it('reports in health what routing does when search_control answers too slowly', async () => {
    await setVerified(true)
    const forced = { ...search, control: slowControl(650) }
    await assert.rejects(ask(PARAMS, undefined, { ...deps, search: forced }), unavailable)
    const server = createQueryServer({ db, search: forced, serviceId: 'test', cursorKey: deps.cursorKey, rateLimitPerMinute: 100, pagingSessionsPerHour: 100 })
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
    try {
      const res = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/xrpc/_health`)
      const body = (await res.json()) as { index: { ok: boolean }; search: { unavailable?: boolean } }
      assert.equal(body.index.ok, false)
      assert.equal(body.search.unavailable, true)
    } finally {
      await new Promise<void>((r) => server.close(() => r()))
    }
  })

  it('marks this service\'s connections as writers of event_docs, and no others', async () => {
    assert.equal(await searchWriterSeen(db), true)
    const other = new pg.Pool({ connectionString: dbUrl('routing_test') })
    try {
      assert.equal(await searchWriterSeen(other), false)
    } finally {
      await other.end()
    }
  })

  it('migrates again without locking ingestion tables an older writer holds', async () => {
    const old = new pg.Client({ connectionString: dbUrl('routing_test') })
    await old.connect()
    try {
      await old.query('begin')
      await old.query('lock table checkpoints in row exclusive mode')
      const done = await Promise.race([migrate(db).then(() => true), new Promise((r) => setTimeout(() => r(false), 3_000))])
      assert.equal(done, true, 'a repeat migration waited on checkpoints')
    } finally {
      await old.query('rollback').catch(() => {})
      await old.end()
    }
  })

  it('reports new sessions as not served in health when search_control answers too slowly', async () => {
    const slow = { ...search, control: slowControl(650) }
    await assert.rejects(ask(PARAMS, undefined, { ...deps, search: slow }), unavailable)
    const server = createQueryServer({ db, search: slow, serviceId: 'test', cursorKey: deps.cursorKey, rateLimitPerMinute: 100, pagingSessionsPerHour: 100 })
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
    try {
      const res = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/xrpc/_health`)
      const body = (await res.json()) as { index: { ok: boolean } }
      assert.equal(body.index.ok, false)
    } finally {
      await new Promise<void>((r) => server.close(() => r()))
    }
  })

  it('answers every first page within seconds while another transaction holds the control row', async () => {
    await setVerified(true)
    await db.query(`update search_control set elastic_cursors_until = now() + interval '5 minutes'`)
    const pool = controlPool(dbUrl('routing_test'))
    const holder = new pg.Client({ connectionString: dbUrl('routing_test') })
    await holder.connect()
    try {
      await holder.query('begin')
      await holder.query('update search_control set updated_at = now()')
      const control = new SearchControl(pool)
      const started = Date.now()
      const results = await Promise.allSettled(
        Array.from({ length: 12 }, () => ask(PARAMS, undefined, { ...deps, search: { ...search, control } })),
      )
      const took = Date.now() - started
      assert.ok(took < 5_000, `took ${took} ms`)
      assert.ok(results.every((r) => r.status === 'fulfilled'), JSON.stringify(results.map((r) => r.status)))
    } finally {
      await holder.query('rollback').catch(() => {})
      await holder.end()
      await pool.end()
    }
  })

  /** Health's index, from a server on `s`. */
  const healthIndex = async (s: Search) => {
    const server = createQueryServer({ db, search: s, serviceId: 'test', cursorKey: deps.cursorKey, rateLimitPerMinute: 100, pagingSessionsPerHour: 100 })
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
    try {
      const res = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/xrpc/_health`)
      return ((await res.json()) as { index: { ok: boolean; backend?: string } }).index
    } finally {
      await new Promise<void>((r) => server.close(() => r()))
    }
  }

  it('migrates again while a projection holds event_docs and then reads accounts, without a deadlock', async () => {
    const a = new pg.Client({ connectionString: dbUrl('routing_test') })
    await a.connect()
    try {
      await a.query('begin')
      await a.query('update event_docs set closed_at = closed_at where false')
      let done = false
      const migrating = migrate(db).finally(() => (done = true))
      // Barrier: the migration is waiting on a lock (event_docs, held by a), or done.
      for (let i = 0; !done; i++) {
        const waiting = await db.query(`select 1 from pg_stat_activity where wait_event_type = 'Lock' and datname = current_database() and pid <> $1`, [(a as unknown as { processID: number }).processID])
        if (waiting.rowCount) break
        if (i > 500) throw new Error('the migration neither finished nor waited on a lock')
        await new Promise((r) => setTimeout(r, 10))
      }
      await a.query('select active from accounts limit 1')
      await a.query('commit')
      await migrating
    } finally {
      await a.query('rollback').catch(() => {})
      await a.end()
    }
  })

  it('lets two older writers change different ingestion tables at once: the trigger takes no row lock', async () => {
    const a = new pg.Client({ connectionString: dbUrl('routing_test') })
    const b = new pg.Client({ connectionString: dbUrl('routing_test') })
    await a.connect()
    await b.connect()
    try {
      await a.query('begin')
      await a.query(`update accounts set active = active where did = $1`, [AUTHOR])
      await b.query('begin')
      const done = await Promise.race([
        b.query(`update index_state set updated_at = updated_at where uri = $1`, [uriOf(1)]).then(() => true),
        new Promise((r) => setTimeout(() => r(false), 2_000)),
      ])
      assert.equal(done, true, 'the second writer waited on the first through the trigger')
    } finally {
      await a.query('rollback').catch(() => {})
      await b.query('rollback').catch(() => {})
      await a.end()
      await b.end()
    }
    // Verification is void at the moved generation, as it should be.
    assert.equal((await readControl(db)).pgVerified, false)
    await db.query('update search_control set fill_generation = search_generation_now()')
    await setVerified(true)
  })

  it('destroys a search_control connection that does not answer, so it cannot hold a slot', async () => {
    const released: (Error | undefined)[] = []
    let n = 0
    const pool = {
      connect: async () => {
        const hung = n++ === 0
        return {
          query: (text: string, values?: unknown[]) => (hung ? new Promise(() => {}) : db.query(text, values)),
          release: (err?: Error) => released.push(err),
          on: () => {},
          off: () => {},
        }
      },
    } as unknown as Db
    const control = new SearchControl(pool)
    await assert.rejects(control.refresh(), /did not answer/)
    assert.ok(released[0] instanceof Error, 'the hung connection was released with an error, so the pool destroys it')
    await control.refresh()
    assert.equal(released[1], undefined)
  })

  it('counts for health behind the same write lock a first page waits for', async () => {
    const holder = new pg.Client({ connectionString: dbUrl('routing_test') })
    await holder.connect()
    try {
      await holder.query('begin')
      await holder.query('select pg_advisory_xact_lock($1)', [7140015])
      let counted = false
      const counting = search.postgres.count().then(() => (counted = true))
      await new Promise((r) => setTimeout(r, 300))
      assert.equal(counted, false, 'counted while a projection held the write lock')
      await holder.query('commit')
      await counting
    } finally {
      await holder.query('rollback').catch(() => {})
      await holder.end()
    }
  })

  it('bounds a search_control call to 500 ms in all, waiting for a connection included', async () => {
    const pool = {
      connect: async () => {
        await new Promise((r) => setTimeout(r, 300))
        return {
          query: async (text: string, values?: unknown[]) => {
            await new Promise((r) => setTimeout(r, 300))
            return db.query(text, values)
          },
          release: () => {},
          on: () => {},
          off: () => {},
        }
      },
    } as unknown as Db
    const started = Date.now()
    await assert.rejects(new SearchControl(pool).refresh(), /did not answer/)
    const took = Date.now() - started
    assert.ok(took < 560, `took ${took} ms`)
  })

  it('survives a checked-out connection that the server ends, and does not pool it again', async () => {
    const pool = controlPool(dbUrl('routing_test'), () => {})
    try {
      const control = new SearchControl(pool)
      const real = pool.connect.bind(pool)
      let pid: number | undefined
      ;(pool as unknown as { connect: () => Promise<pg.PoolClient> }).connect = async () => {
        const c = await real()
        pid = (await c.query<{ p: number }>('select pg_backend_pid() as p')).rows[0].p
        // The server ends this connection while it is checked out, before its query.
        const query = c.query.bind(c) as (...a: unknown[]) => Promise<unknown>
        ;(c as unknown as { query: unknown }).query = async (...a: unknown[]) => {
          await db.query('select pg_terminate_backend($1)', [pid])
          await new Promise((r) => setTimeout(r, 100))
          return query(...a)
        }
        return c
      }
      await assert.rejects(control.refresh())
      ;(pool as unknown as { connect: typeof real }).connect = real
      await control.refresh()
      const live = await db.query('select 1 from pg_stat_activity where pid = $1', [pid])
      assert.equal(live.rowCount, 0)
    } finally {
      await pool.end()
    }
  })

  it('stops serving from Postgres as soon as its verification is cleared, with no cached answer', async () => {
    await setVerified(true)
    const forced = deps
    await ask(PARAMS, undefined, forced)
    await db.query('update search_control set pg_verified = false')
    await assert.rejects(ask(PARAMS, undefined, forced), unavailable)
    await assert.rejects(getEvent(forced, uriOf(2)), unavailable)
    await setVerified(true)
  })

  it('reports the backend serving new sessions in health', async () => {
    await setVerified(true)
    const server = createQueryServer({ db, search, serviceId: 'test', cursorKey: deps.cursorKey, rateLimitPerMinute: 100, pagingSessionsPerHour: 100 })
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
    try {
      const res = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/xrpc/_health`)
      const body = (await res.json()) as { index: { ok: boolean; backend: string; events: number }; search: { pgVerified: boolean } }
      assert.deepEqual(body.index, { ok: true, backend: 'postgres', events: 5 })
      assert.equal(body.search.pgVerified, true)
    } finally {
      await new Promise<void>((r) => server.close(() => r()))
    }
  })
})

describe('first fill and the verifying comparison', { skip: !databaseUrl && 'TEST_DATABASE_URL not set' }, () => {
  let db: Db
  let projector: Projector
  let handler: Handler
  let time = 1_760_000_000_000_000
  const commit = (collection: string, rkey: string, value: Record<string, unknown>): JetstreamEvent => ({
    did: AUTHOR,
    time_us: time++,
    kind: 'commit',
    commit: { rev: `3m${String(time).slice(-11)}`, operation: 'create', collection, rkey, record: value, cid: 'bafyreibjytuinufvu3gimfxyqjbptuws7g3sdekx2igq5dts4ar7jgxjbq' },
  })

  before(async () => {
    db = await freshDb('upkeep_test')
    projector = new Projector({ db, plcUrl: 'http://127.0.0.1:1', log: () => {} })
    handler = new Handler(db, projector, () => {})
    for (let n = 1; n <= 3; n++) {
      await handler.handle(commit(EVENT, `3ke${n}`, eventValue({ name: `e${n}` })))
      await handler.handle(commit(SIDECAR, `3ks${n}`, sidecarValue(AUTHOR, `3ke${n}`, geo('37.78', '-122.43'))))
    }
    // As before PR 1: Postgres empty.
    await db.query('truncate event_docs')
  })
  after(async () => {
    await db.end()
  })

  it('fills Postgres from stored records, compares it with stored records assembled again, and records verified', async () => {
    const log: string[] = []
    const upkeep = new SearchUpkeep(db, projector, (m) => log.push(m))
    await upkeep.run()
    const row = await readControl(db)
    assert.equal(row.firstFillDone, true)
    assert.equal(row.pgVerified, true, `${row.note} / ${log.join(' / ')}`)
    assert.match(row.note!, /3 documents/)
    assert.equal(await projector.postgres.count(), 3)

    // A later comparison that does not pass clears it (only upkeep's first
    // comparison runs while verified, as another ingester verifies meanwhile).
    const removed = (await projector.postgres.get(`at://${AUTHOR}/${SIDECAR}/3ks1`))!
    await projector.postgres.remove(removed.uri)
    const again = await upkeep.compare()
    assert.equal(again.ok, false)
    assert.equal((await readControl(db)).pgVerified, false)
    // From PR 2 a difference voids the fill: the next upkeep fills again,
    // which repairs it, and compares.
    assert.equal((await readControl(db)).firstFillDone, false)
    await upkeep.run()
    assert.ok(await projector.postgres.get(removed.uri))
    assert.equal((await readControl(db)).pgVerified, true)
  })

  it('names what differs, and the next upkeep fills again and repairs it', async () => {
    await db.query('update search_control set pg_verified = false, compared_at = null')
    await projector.postgres.remove(`at://${AUTHOR}/${SIDECAR}/3ks1`)
    await projector.postgres.put({ ...(await projector.postgres.get(`at://${AUTHOR}/${SIDECAR}/3ks2`))!, eventCid: 'bafychanged' })
    const opened: string[] = []
    const log: string[] = []
    const upkeep = new SearchUpkeep(db, projector, (m) => log.push(m), Date.now, { open: async (key) => void opened.push(key) })
    await upkeep.run()
    let row = await readControl(db)
    assert.equal(row.pgVerified, false)
    assert.equal(row.firstFillDone, false, 'a difference voids the fill')
    assert.deepEqual(opened, ['comparison'], 'from PR 2 a failed comparison opens an issue')
    // Public: counts only. The uris are in the ingester log.
    assert.match(row.note!, /1 only in stored records/)
    assert.match(row.note!, /1 with different content/)
    assert.doesNotMatch(row.note!, /at:\/\//)
    assert.ok(log.some((m) => /3ks1/.test(m) && /3ks2/.test(m)), log.join(' / '))
    await upkeep.run()
    row = await readControl(db)
    assert.equal(row.pgVerified, true, row.note ?? '')
    assert.equal((await projector.postgres.get(`at://${AUTHOR}/${SIDECAR}/3ks2`))!.eventCid === 'bafychanged', false)
  })

  it('fails on a stale suppression state, and the fill rewrites it', async () => {
    const s1 = `at://${AUTHOR}/${SIDECAR}/3ks1`
    await db.query('update search_control set pg_verified = true, first_fill_done = true, fill_generation = search_generation_now(), checked_by = 2')
    // A complete sidecar marked incomplete: getEvent would answer it, queryEvents would drop it.
    await db.query(`update index_state set complete = false, reason = 'orphan' where uri = $1`, [s1])
    const upkeep = new SearchUpkeep(db, projector, () => {})
    const res = await upkeep.compare()
    assert.equal(res.ok, false)
    assert.match(res.note, /1 with a stale suppression state/)
    await upkeep.run()
    assert.equal((await db.query('select complete from index_state where uri = $1', [s1])).rows[0].complete, true)
    assert.equal((await readControl(db)).pgVerified, true)
  })

  it('on the first PR 2 start, fills and compares a database PR 1 verified, keeping verification meanwhile', async () => {
    // PR 1 verified it against today's index, which missed a sidecar its
    // index_state never recorded.
    const s1 = `at://${AUTHOR}/${SIDECAR}/3ks1`
    const doc = (await projector.postgres.get(s1))!
    await projector.postgres.remove(s1)
    await db.query('delete from index_state where uri = $1', [s1])
    await db.query('update search_control set pg_verified = true, first_fill_done = true, fill_generation = search_generation_now(), checked_by = 0, compared_at = now()')
    let verifiedDuringFill: boolean | undefined
    const real = projector.fillSidecar.bind(projector)
    projector.fillSidecar = async (uri: string) => {
      verifiedDuringFill ??= (await readControl(db)).pgVerified
      return real(uri)
    }
    try {
      await new SearchUpkeep(db, projector, () => {}).run()
    } finally {
      projector.fillSidecar = real
    }
    assert.equal(verifiedDuringFill, true)
    assert.deepEqual(await projector.postgres.get(s1).then((d) => d && comparable(d)), comparable(doc))
    assert.equal((await db.query('select complete from index_state where uri = $1', [s1])).rows[0].complete, true)
    const row = await readControl(db)
    assert.equal(row.pgVerified, true)
    assert.equal(row.checkedBy, 2)
  })

it('fills again and compares again after code that does not write Postgres changed index_state', async () => {
    const upkeep = new SearchUpkeep(db, projector, () => {})
    await upkeep.run()
    assert.equal((await readControl(db)).pgVerified, true)

    // This service before the change (after a revert, say) touches an
    // event: index_state moves on, event_docs does not.
    const s1 = `at://${AUTHOR}/${SIDECAR}/3ks1`
    const old = new pg.Client({ connectionString: dbUrl('upkeep_test') })
    await old.connect()
    await old.query(`update index_state set updated_at = now() where uri = $1`, [s1])
    await old.end()
    let row = await readControl(db)
    assert.equal(row.pgVerified, false)
    assert.equal(row.firstFillDone, false)

    // This service's own writes do not trip it: the generation stays put.
    const generation = row.generation
    await setIndexState(db, s1, AUTHOR, true, null)
    assert.equal((await readControl(db)).generation, generation)
    await upkeep.run()
    row = await readControl(db)
    assert.equal(row.firstFillDone, true)
    assert.equal(row.pgVerified, true, row.note ?? '')
  })

  it('clears verification when old code changes records or accounts, even if it fails before index_state', async () => {
    const upkeep = new SearchUpkeep(db, projector, () => {})
    await db.query('update search_control set compared_at = null')
    await upkeep.run()
    assert.equal((await readControl(db)).pgVerified, true)
    for (const sql of [`update accounts set active = false where did = '${AUTHOR}'`, `update records set updated_at = now() where did = '${AUTHOR}'`]) {
      await db.query('update search_control set pg_verified = true, first_fill_done = true, fill_generation = search_generation_now()')
      const before = (await readControl(db)).generation
      const old = new pg.Client({ connectionString: dbUrl('upkeep_test') })
      await old.connect()
      await old.query('begin')
      await old.query(sql)
      await old.query('commit')
      await old.end()
      const row = await readControl(db)
      assert.equal(row.pgVerified, false, sql)
      assert.equal(row.firstFillDone, false, sql)
      assert.notEqual(row.generation, before, sql)
    }
    await db.query(`update accounts set active = true where did = '${AUTHOR}'`)
    await db.query('update search_control set compared_at = null')
    await upkeep.run()
    assert.equal((await readControl(db)).pgVerified, true)
  })

  it('clears verification when any connection truncates an ingestion table', async () => {
    await db.query('update search_control set pg_verified = true, first_fill_done = true, fill_generation = search_generation_now()')
    await db.query('create table keep_state as select * from index_state')
    await db.query('truncate index_state')
    assert.equal((await readControl(db)).pgVerified, false)
    await db.query('insert into index_state select * from keep_state')
    await db.query('drop table keep_state')
    await db.query('update search_control set compared_at = null')
    await new SearchUpkeep(db, projector, () => {}).run()
    assert.equal((await readControl(db)).pgVerified, true)
  })

  it('keeps verification when a plain connection changes checkpoints or skipped_events, which do not feed event_docs', async () => {
    await db.query('update search_control set pg_verified = true, first_fill_done = true, fill_generation = search_generation_now()')
    const before = (await readControl(db)).generation
    const old = new pg.Client({ connectionString: dbUrl('upkeep_test') })
    await old.connect()
    try {
      await old.query(`insert into skipped_events (time_us, did, kind, event, attempts) values (1, 'did:plc:x', 'commit', '{}', 3)`)
      await old.query('delete from skipped_events where time_us = 1')
      await old.query(`insert into checkpoints (name, cursor) values ('manual-test', 1) on conflict (name) do update set cursor = 1`)
      await old.query(`delete from checkpoints where name = 'manual-test'`)
    } finally {
      await old.end()
    }
    const row = await readControl(db)
    assert.equal(row.generation, before)
    assert.equal(row.pgVerified, true)
  })

  it('does not verify when index_state changes from old code while the comparison runs', async () => {
    await db.query('update search_control set pg_verified = false, compared_at = null')
    const upkeep = new SearchUpkeep(db, projector, () => {})
    const real = projector.assembled.bind(projector)
    let once = false
    projector.assembled = async (uri: string, q?: Db) => {
      const doc = await real(uri, q)
      if (!once) {
        once = true
        const old = new pg.Client({ connectionString: dbUrl('upkeep_test') })
        await old.connect()
        await old.query(`update index_state set updated_at = now() where uri = $1`, [`at://${AUTHOR}/${SIDECAR}/3ks2`])
        await old.end()
      }
      return doc
    }
    try {
      const res = await upkeep.compare()
      assert.equal(res.ok, false)
      const row = await readControl(db)
      assert.equal(row.pgVerified, false)
      assert.equal(row.firstFillDone, false)
    } finally {
      projector.assembled = real
    }
    await upkeep.run()
    assert.equal((await readControl(db)).pgVerified, true)
  })

  it('never writes a projection over a later one, whichever ingester started first', async () => {
    const s3 = `at://${AUTHOR}/${SIDECAR}/3ks3`
    // Ingester A's fill reaches the write lock only after ingester B has
    // deleted the sidecar's record: A must write what the records say then.
    const holder = new pg.Client({ connectionString: dbUrl('upkeep_test') })
    await holder.connect()
    await holder.query('begin')
    await holder.query('select pg_advisory_xact_lock($1)', [7140015])
    let fill: Promise<void> | undefined
    try {
      fill = projector.fillSidecar(s3)
      // Barrier: the fill is waiting on the write lock.
      for (let i = 0; ; i++) {
        const waiting = await db.query(`select 1 from pg_locks where locktype = 'advisory' and objid = 7140015 and not granted`)
        if (waiting.rowCount) break
        if (i > 200) throw new Error('the fill never waited on the write lock')
        await new Promise((r) => setTimeout(r, 10))
      }
      await db.query(`update records set deleted = true, value = null where uri = $1`, [s3])
    } finally {
      await holder.query('commit').catch(() => {})
      await holder.end()
    }
    await fill
    assert.equal(await projector.postgres.get(s3), undefined)
    // Ingester B's own projection of the delete agrees.
    await projector.projectSidecar(s3, { hydrate: false })
    assert.equal(await projector.postgres.get(s3), undefined)
  })

  it('runs upkeep on one ingester at a time', async () => {
    const holder = new pg.Client({ connectionString: dbUrl('upkeep_test') })
    await holder.connect()
    try {
      await holder.query('select pg_advisory_lock($1)', [7140016])
      await db.query('update search_control set pg_verified = false, compared_at = null')
      let compared = false
      const upkeep = new SearchUpkeep(db, projector, () => {})
      upkeep.compare = async () => ((compared = true), { ok: true, note: '' })
      await upkeep.run()
      assert.equal(compared, false, 'ran while another ingester held upkeep')
    } finally {
      await holder.end()
    }
    await new SearchUpkeep(db, projector, () => {}).run()
    assert.equal((await readControl(db)).pgVerified, true)
  })

  it('records nothing from an upkeep run whose lock session has ended', async () => {
    await db.query('update search_control set pg_verified = false, first_fill_done = true, fill_generation = search_generation_now(), compared_at = null')
    const extra = { ...(await projector.postgres.get(`at://${AUTHOR}/${SIDECAR}/3ks1`))!, uri: `at://${AUTHOR}/${SIDECAR}/3ksgone` }
    await projector.postgres.put(extra)
    const real = projector.assembled.bind(projector)
    let once = false
    projector.assembled = async (uri: string, q?: Db) => {
      if (once) return real(uri, q)
      once = true
      // The server ends the session holding the upkeep lock mid-comparison;
      // another ingester could now take the lock and verify.
      const holder = await db.query<{ pid: number }>(`select pid from pg_locks where locktype = 'advisory' and objid = 7140016 and granted`)
      assert.equal(holder.rowCount, 1)
      await db.query('select pg_terminate_backend($1)', [holder.rows[0].pid])
      await new Promise((r) => setTimeout(r, 100))
      // Another ingester's upkeep, holding the lock now, verifies.
      await db.query('update search_control set pg_verified = true, compared_at = now()')
      return real(uri, q)
    }
    try {
      await new SearchUpkeep(db, projector, () => {}).run()
    } finally {
      projector.assembled = real
      await projector.postgres.remove(extra.uri)
    }
    const row = await readControl(db)
    assert.equal(row.pgVerified, true, 'a run that lost its lock recorded its comparison over a later one')
    await db.query('update search_control set compared_at = null')
  })

  it('leaves verification as it is when a comparison cannot run', async () => {
    await db.query('update search_control set pg_verified = true, first_fill_done = true, fill_generation = search_generation_now()')
    const real = projector.assembled.bind(projector)
    projector.assembled = async () => Promise.reject(new Error('connection terminated'))
    try {
      const res = await new SearchUpkeep(db, projector, () => {}).compare()
      assert.equal(res.ok, false)
      assert.equal((await readControl(db)).pgVerified, true)
    } finally {
      projector.assembled = real
    }
  })

  it('counts a write by old code only once it is visible, even if it is still open when the run reads the generation', async () => {
    const s1 = `at://${AUTHOR}/${SIDECAR}/3ks1`
    const stored = (await db.query<{ value: unknown }>('select value from records where uri = $1', [s1])).rows[0].value
    const upkeep = new SearchUpkeep(db, projector, () => {})
    // Old code deletes the sidecar's record and moves the generation, but has not committed yet.
    const old = new pg.Client({ connectionString: dbUrl('upkeep_test') })
    await old.connect()
    await old.query('begin')
    await old.query('update records set deleted = true, value = null where uri = $1', [s1])
    let finished = false
    const fill = upkeep.firstFill().finally(() => (finished = true))
    try {
      await new Promise((r) => setTimeout(r, 500))
      assert.equal(finished, false, 'the fill finished while a write that moved the generation was still open')
      await old.query('commit')
    } finally {
      await old.query('rollback').catch(() => {})
      await old.end()
    }
    await fill
    assert.equal((await readControl(db)).firstFillDone, true)
    assert.equal(await projector.postgres.get(s1), undefined, 'the fill did not see the committed delete')

    // A refill waits the same way for a write that then rolls back.
    const rolling = new pg.Client({ connectionString: dbUrl('upkeep_test') })
    await rolling.connect()
    await rolling.query('begin')
    await rolling.query(`update index_state set updated_at = now() where uri = $1`, [s1])
    assert.equal((await readControl(db)).firstFillDone, false)
    finished = false
    const refill = upkeep.firstFill().finally(() => (finished = true))
    try {
      await new Promise((r) => setTimeout(r, 500))
      assert.equal(finished, false, 'the refill finished while a write that moved the generation was still open')
      await rolling.query('rollback')
    } finally {
      await rolling.end()
    }
    await refill
    assert.equal((await readControl(db)).firstFillDone, true)

    // A transaction that stays open fails the fill, which records nothing.
    await db.query('update search_control set first_fill_done = false')
    const held = new pg.Client({ connectionString: dbUrl('upkeep_test') })
    await held.connect()
    await held.query('begin')
    await held.query('select pg_current_xact_id()')
    upkeep.settleMs = 300
    try {
      await assert.rejects(upkeep.firstFill(), /did not end within 300 ms/)
      assert.equal((await readControl(db)).firstFillDone, false)
    } finally {
      await held.query('rollback')
      await held.end()
    }

    await db.query('update records set deleted = false, value = $2 where uri = $1', [s1, stored])
    await projector.fillSidecar(s1)
    await db.query('update search_control set pg_verified = false, compared_at = null')
    await new SearchUpkeep(db, projector, () => {}).run()
    assert.equal((await readControl(db)).pgVerified, true)
  })

  it('clears verification when a comparison finds a difference, whoever set it', async () => {
    await db.query('update search_control set pg_verified = true, first_fill_done = true, fill_generation = search_generation_now(), compared_at = null')
    const upkeep = new SearchUpkeep(db, projector, () => {})
    const extra = { ...(await projector.postgres.get(`at://${AUTHOR}/${SIDECAR}/3ks1`))!, uri: `at://${AUTHOR}/${SIDECAR}/3ksextra` }
    await projector.postgres.put(extra)
    const res = await upkeep.compare()
    assert.equal(res.ok, false)
    assert.equal((await readControl(db)).pgVerified, false)
    await projector.postgres.remove(extra.uri)
  })

  it('fills and compares a sidecar whose index_state row was never written', async () => {
    // Old code stored the records and stopped before index_state.
    const event = commit(EVENT, '3ke9', eventValue({ name: 'no state' }))
    const side = commit(SIDECAR, '3ks9', sidecarValue(AUTHOR, '3ke9', geo('37.78', '-122.43')))
    await handler.handle(event)
    await handler.handle(side)
    const s9 = `at://${AUTHOR}/${SIDECAR}/3ks9`
    await db.query('delete from index_state where uri = $1', [s9])
    await projector.postgres.remove(s9)
    await db.query('update search_control set first_fill_done = false, compared_at = null')
    const upkeep = new SearchUpkeep(db, projector, () => {})
    // The comparison alone sees it missing from Postgres.
    await db.query('update search_control set first_fill_done = true, fill_generation = search_generation_now(), pg_verified = false')
    const res = await upkeep.compare()
    assert.equal(res.ok, false)
    assert.match(res.note, /1 only in stored records/)
    // The fill projects it, and then the comparison passes.
    await db.query('update search_control set first_fill_done = false')
    await upkeep.run()
    assert.ok(await projector.postgres.get(s9))
    assert.equal((await readControl(db)).pgVerified, true)
    await projector.postgres.remove(s9)
    await db.query(`delete from records where uri in ($1, $2)`, [s9, `at://${AUTHOR}/${EVENT}/3ke9`])
    await db.query('delete from index_state where uri = $1', [s9])
    await db.query('update search_control set fill_generation = search_generation_now(), compared_at = null')
  })

  it('reads a difference again before failing, so a write between the two tables does not fail the check', async () => {
    const s2 = `at://${AUTHOR}/${SIDECAR}/3ks2`
    await db.query('update search_control set pg_verified = false, compared_at = null, first_fill_done = true, fill_generation = search_generation_now()')
    const upkeep = new SearchUpkeep(db, projector, () => {})
    const current = (await projector.postgres.get(s2))!
    // Postgres lags the stored records in the first snapshot, and has caught up by the second read.
    await projector.postgres.put({ ...current, eventCid: 'bafylagging' })
    const real = projector.assembled.bind(projector)
    let caughtUp = false
    projector.assembled = async (uri: string, q?: Db) => {
      if (uri === s2 && !caughtUp) {
        caughtUp = true
        await projector.postgres.put(current)
      }
      return real(uri, q)
    }
    try {
      const res = await upkeep.compare()
      assert.equal(res.ok, true, res.note)
    } finally {
      projector.assembled = real
    }
  })

  it('ignores indexedAt and key order when comparing', () => {
    const a = doc(1)
    const { indexedAt: _i, view, ...rest } = a
    // Same content, keys in another order, a different indexedAt.
    const b = JSON.parse(JSON.stringify({ indexedAt: '2030-01-01T00:00:00.000Z', view: { location: view.location, event: view.event, eventUri: view.eventUri, uri: view.uri }, ...rest }))
    assert.equal(comparable(a), comparable(b))
  })
})

describe('projector writes', { skip: !databaseUrl && 'TEST_DATABASE_URL not set' }, () => {
  let db: Db
  let projector: Projector
  let handler: Handler
  let time = 1_770_000_000_000_000
  const commit = (collection: string, rkey: string, value?: Record<string, unknown>): JetstreamEvent => ({
    did: AUTHOR,
    time_us: time++,
    kind: 'commit',
    commit: {
      rev: `3m${String(time).slice(-11)}`,
      operation: value ? 'create' : 'delete',
      collection,
      rkey,
      ...(value ? { record: value, cid: 'bafyreibjytuinufvu3gimfxyqjbptuws7g3sdekx2igq5dts4ar7jgxjbq' } : {}),
    },
  })
  const sidecar = `at://${AUTHOR}/${SIDECAR}/3ksw1`

  before(async () => {
    db = await freshDb('projector_pg_test')
    projector = new Projector({ db, plcUrl: 'http://127.0.0.1:1', log: () => {} })
    handler = new Handler(db, projector, () => {})
  })
  after(() => db.end())

  it('writes Postgres only: a projection reaches event_docs and its removal takes it out', async () => {
    await handler.handle(commit(EVENT, '3kew1', eventValue({ name: 'first' })))
    await handler.handle(commit(SIDECAR, '3ksw1', sidecarValue(AUTHOR, '3kew1', geo('37.78', '-122.43'))))
    assert.ok(await projector.postgres.get(sidecar))
    assert.equal(comparable((await projector.postgres.get(sidecar))!), comparable((await projector.assembled(sidecar))!))
    await handler.handle(commit(EVENT, '3kew1'))
    assert.equal(await projector.postgres.get(sidecar), undefined)
  })

  it('takes every event of a suppressed account out of Postgres and marks it', async () => {
    const uris = []
    for (const n of [2, 3]) {
      await handler.handle(commit(EVENT, `3kew${n}`, eventValue({ name: `e${n}` })))
      await handler.handle(commit(SIDECAR, `3ksw${n}`, sidecarValue(AUTHOR, `3kew${n}`, geo('37.78', '-122.43'))))
      uris.push(`at://${AUTHOR}/${SIDECAR}/3ksw${n}`)
    }
    await db.query('update accounts set active = false where did = $1', [AUTHOR])
    await db.query('insert into accounts (did, active) values ($1, false) on conflict (did) do update set active = false', [AUTHOR])
    await projector.projectAccount(AUTHOR)
    for (const uri of uris) assert.equal(await projector.postgres.get(uri), undefined, uri)
    const states = await db.query<{ complete: boolean }>('select complete from index_state where uri = any($1)', [uris])
    assert.deepEqual(states.rows.map((r) => r.complete), [false, false])
    assert.equal((await incompleteAmong(db, uris)).size, 2)
  })
})
