import { after, before, beforeEach, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { randomBytes } from 'node:crypto'
import type { AddressInfo } from 'node:net'
import { migrate, searchWriterSeen, type Db } from '../src/db.js'
import type { EventDoc } from '../src/index/assemble.js'
import { ElasticBackend, connectElastic, createManagedIndex, ensureIndex, putManaged, removeManaged, type Elastic } from '../src/index/elastic.js'
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
import { databaseUrl, elasticUrl, freshDb, memoryElastic } from './support.js'

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
        search: { elastic: undefined as never, postgres: slow, control: { current: async () => ({ pgVerified: true }), refresh: async () => ({ pgVerified: true }) } as never, force: 'postgres' },
        serviceId: 'test',
        cursorKey: randomBytes(32),
      } as QueryDeps
      await assert.rejects(getEvent(deps, uriOf(1)), unavailable)
    } finally {
      await pool.end()
    }
  })
})

describe('search routing', { skip: (!databaseUrl || !elasticUrl) && 'TEST_DATABASE_URL and TEST_ELASTICSEARCH_URL not set' }, () => {
  const INDEX = `test-routing-${Date.now()}`
  let db: Db
  let es: Elastic
  let search: Search
  let deps: QueryDeps
  const PARAMS = 'latitude=37.78&longitude=-122.43&limit=2'
  const ask = (params: string, cursor?: string, d = deps) => {
    const sp = new URLSearchParams(params)
    if (cursor) sp.set('cursor', cursor)
    return queryEvents(d, parseEventQuery(sp))
  }
  /** Elasticsearch that fails every call, counting them. */
  const deadElastic = () => {
    const dead = new ElasticBackend(connectElastic('http://127.0.0.1:1'), INDEX) as ElasticBackend & { calls: number }
    dead.calls = 0
    for (const m of ['get', 'openSnapshot', 'page', 'count'] as const) {
      const real = dead[m].bind(dead) as (...a: unknown[]) => Promise<unknown>
      ;(dead as unknown as Record<string, unknown>)[m] = (...a: unknown[]) => (dead.calls++, real(...a))
    }
    return dead
  }
  const setVerified = (v: boolean) => db.query('update search_control set pg_verified = $1, fill_generation = search_generation_now()', [v]).then(() => search.control.refresh())

  before(async () => {
    db = await freshDb('routing_test')
    es = connectElastic(elasticUrl!)
    await ensureIndex(es, INDEX)
    const elastic = new ElasticBackend(es, INDEX)
    const postgres = new PostgresBackend(db)
    for (let n = 1; n <= 5; n++) {
      await postgres.put(doc(n))
      await elastic.put(doc(n))
      await setIndexState(db, uriOf(n), AUTHOR, true, null)
    }
    await es.indices.refresh({ index: INDEX })
    // INDEX plays the current managed index (spec 5.2): in use, current, nothing pending.
    await db.query(`update search_control set es_in_use = true, es_current = true, elastic_index = $1`, [INDEX])
    search = { elastic, postgres, control: new SearchControl(db), force: 'elastic' }
    deps = { db, search, serviceId: 'test', cursorKey: randomBytes(32) }
  })
  after(async () => {
    await es.indices.delete({ index: INDEX }).catch(() => {})
    await db.end()
  })

  it('serves from Elasticsearch by default and covers its session with elastic_cursors_until', async () => {
    await db.query(`update search_control set elastic_cursors_until = now() - interval '1 minute'`)
    await search.control.refresh()
    const page = await ask(PARAMS)
    assert.equal(search.last?.backend, 'elastic')
    const state = decodeCursor(page.cursor!, deps.cursorKey, { svc: 'test', bind: bindingHash(bindingOf(parseEventQuery(new URLSearchParams(PARAMS)))) })
    assert.equal(state.be, 'elastic')
    const until = (await readControl(db)).elasticCursorsUntil.getTime()
    assert.ok(until >= Date.now() + 60 * 60_000, 'at least an hour of cover')
    const next = await ask(PARAMS, page.cursor)
    assert.equal(next.events.length, 2)
  })

  it('reads a cursor without a backend as an Elasticsearch one, and expires it after elastic_cursors_until without calling Elasticsearch', async () => {
    const page = await ask(PARAMS)
    const bind = bindingHash(bindingOf(parseEventQuery(new URLSearchParams(PARAMS))))
    const state = decodeCursor(page.cursor!, deps.cursorKey, { svc: 'test', bind })
    const { be: _be, ...old } = state
    const oldCursor = encodeCursor(old, deps.cursorKey)
    assert.equal((await ask(PARAMS, oldCursor)).events.length, 2)

    await db.query(`update search_control set elastic_cursors_until = now() - interval '1 second'`)
    await search.control.refresh()
    const dead = deadElastic()
    await assert.rejects(ask(PARAMS, oldCursor, { ...deps, search: { ...search, elastic: dead } }), expired)
    assert.equal(dead.calls, 0)
  })

  it('looks again before expiring an Elasticsearch cursor, in case another replica moved the time on', async () => {
    const page = await ask(PARAMS)
    const stale = new SearchControl(db)
    await db.query(`update search_control set elastic_cursors_until = now() - interval '1 second'`)
    await stale.refresh()
    await db.query(`update search_control set elastic_cursors_until = now() + interval '61 minutes'`)
    assert.equal((await ask(PARAMS, page.cursor, { ...deps, search: { ...search, control: stale } })).events.length, 2)
  })

  it('falls back to Postgres once verified when Elasticsearch fails a first page, and keeps the session there', async () => {
    await setVerified(true)
    const dead = deadElastic()
    const d = { ...deps, search: { ...search, elastic: dead } }
    const page = await ask(PARAMS, undefined, d)
    assert.equal(page.events.length, 2)
    assert.equal(d.search.last?.backend, 'postgres')
    // The rest of the session stays on Postgres even with Elasticsearch healthy again.
    const next = await ask(PARAMS, page.cursor, deps)
    assert.deepEqual(
      [...page.events, ...next.events].map((e) => e.uri),
      [1, 2, 3, 4].map(uriOf),
    )
    assert.equal((await getEvent(d, uriOf(3))).value.uri, uriOf(3))
  })

  it('answers ServiceUnavailable when Elasticsearch fails and Postgres is not verified', async () => {
    await setVerified(false)
    const d = { ...deps, search: { ...search, elastic: deadElastic() } }
    await assert.rejects(ask(PARAMS, undefined, d), unavailable)
    await assert.rejects(getEvent(d, uriOf(3)), unavailable)
  })

  it('serves from Postgres with SEARCH_BACKEND_FORCE=postgres only once Postgres is verified', async () => {
    const forced = { ...deps, search: { ...search, force: 'postgres' as const } }
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

  it('gives the same answers from both backends for every combination of filters and sorts', async () => {
    const points = ['', 'latitude=37.78&longitude=-122.43']
    const radii = ['', 'radiusMeters=3000']
    const boxes = ['', 'west=-122.5&south=37.7&east=-122.4&north=37.783']
    const windows = ['', 'windowStart=2026-10-16T02:00:00Z', 'windowEnd=2026-10-16T01:00:00Z']
    const authors = ['', `author=${AUTHOR}`]
    const sorts = ['', 'sort=startsAt', 'sort=distance']
    await setVerified(true)
    let compared = 0
    for (const pt of points)
      for (const r of radii)
        for (const b of boxes)
          for (const w of windows)
            for (const a of authors)
              for (const s of sorts) {
                const params = [pt, r, b, w, a, s, 'limit=100'].filter(Boolean).join('&')
                let parsed
                try {
                  parsed = parseEventQuery(new URLSearchParams(params))
                } catch {
                  continue
                }
                const run = async (force: 'elastic' | 'postgres') =>
                  (await queryEvents({ ...deps, search: { ...search, force } }, parsed)).events.map((e) => e.uri)
                assert.deepEqual(await run('postgres'), await run('elastic'), params)
                compared++
              }
    assert.ok(compared >= 90, `${compared} combinations`)
  })

  it('refuses getEvent when search_control cannot be read: neither backend can be shown to may serve', async () => {
    const broken = { current: async () => Promise.reject(new Error('connect ECONNREFUSED')), refresh: async () => Promise.reject(new Error('down')) } as unknown as SearchControl
    await assert.rejects(getEvent({ ...deps, search: { ...search, control: broken } }, uriOf(2)), unavailable)
  })

  /** search_control on a database whose every query stalls (or answers after `ms`). */
  const slowControl = (ms = Infinity) =>
    new SearchControl({
      query: (text: string, values?: unknown[]) =>
        ms === Infinity ? new Promise(() => {}) : new Promise((r) => setTimeout(r, ms)).then(() => db.query(text, values)),
    } as unknown as Db)

  it('cuts off a stalled search_control read after 500 ms and refuses getEvent rather than hanging', async () => {
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
    const forced = { ...search, force: 'postgres' as const, control: slowControl(650) }
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

  /** Names a managed index that does not exist in search_control while `fn` runs. */
  const withMissingIndex = async (fn: () => Promise<void>) => {
    await db.query(`update search_control set elastic_index = $1`, [`${INDEX}-missing`])
    try {
      await fn()
    } finally {
      await db.query(`update search_control set elastic_index = $1`, [INDEX])
    }
  }

  it('falls back to a verified Postgres for getEvent when the managed index does not exist', async () => {
    await setVerified(true)
    await withMissingIndex(async () => {
      const res = await getEvent(deps, uriOf(2))
      assert.equal(res.value.uri, uriOf(2))
    })
  })

  it('reports a slow Elasticsearch in health as before while Postgres is not verified', async () => {
    await setVerified(false)
    const slow = {
      name: 'elastic',
      count: async (opts: { signal?: AbortSignal } = {}) => {
        assert.equal(opts.signal, undefined, 'no 2-second limit before verification')
        await new Promise((r) => setTimeout(r, 2_300))
        return 5
      },
    } as unknown as ElasticBackend
    const server = createQueryServer({ db, search: { ...search, elastic: slow }, serviceId: 'test', cursorKey: deps.cursorKey, rateLimitPerMinute: 100, pagingSessionsPerHour: 100 })
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
    try {
      const res = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/xrpc/_health`)
      const body = (await res.json()) as { index: { ok: boolean; backend: string; events: number } }
      assert.deepEqual(body.index, { ok: true, backend: 'elastic', events: 5 })
    } finally {
      await new Promise<void>((r) => server.close(() => r()))
      await setVerified(true)
    }
  })

  it('waits for a slow Elasticsearch as before while Postgres is not verified', async () => {
    await setVerified(false)
    const slow = {
      name: 'elastic',
      get: async (uri: string, opts: { signal?: AbortSignal } = {}) => {
        assert.equal(opts.signal, undefined, 'no 2-second limit before verification')
        await new Promise((r) => setTimeout(r, 2_300))
        return search.elastic.get(uri)
      },
    } as unknown as ElasticBackend
    const res = await getEvent({ ...deps, search: { ...search, elastic: slow } }, uriOf(2))
    assert.equal(res.value.uri, uriOf(2))
    await setVerified(true)
  })

  it('answers ServiceUnavailable, not an error, when the managed index does not exist', async () => {
    await setVerified(false)
    await withMissingIndex(() => assert.rejects(ask(PARAMS), unavailable))
    await setVerified(true)
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

  it('reports new sessions as not served in health when search_control answers too slowly and no cover is cached, Elasticsearch chosen', async () => {
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
      // The cover cannot be written, so Elasticsearch does not serve; the verified Postgres does.
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

  it('agrees with a first page in health when search_control fails, even with a cached cover', async () => {
    const control = new SearchControl(db)
    await control.coverElasticSession()
    // From now on every read fails: whether the managed index may serve cannot be read.
    control.refresh = async () => Promise.reject(new Error('down'))
    const s = { ...search, control }
    await assert.rejects(ask(PARAMS, undefined, { ...deps, search: s }), unavailable)
    const index = await healthIndex(s)
    assert.equal(index.ok, false)
  })

  it('agrees with a first page in health when verification is cleared during a failing Elasticsearch attempt', async () => {
    await setVerified(true)
    const clearing = {
      name: 'elastic',
      count: async () => {
        await db.query('update search_control set pg_verified = false')
        throw new Error('connect ECONNREFUSED')
      },
    } as unknown as ElasticBackend
    const index = await healthIndex({ ...search, elastic: clearing })
    assert.equal(index.ok, false)
    await setVerified(true)
  })

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

  it('looks at verification again before falling back to Postgres', async () => {
    await setVerified(true)
    const clearing = {
      name: 'elastic',
      get: async () => {
        await db.query('update search_control set pg_verified = false')
        throw new Error('connect ECONNREFUSED')
      },
    } as unknown as ElasticBackend
    await assert.rejects(getEvent({ ...deps, search: { ...search, elastic: clearing } }, uriOf(2)), unavailable)
    await setVerified(true)
  })

  it('covers cursors from a previous deployment when it starts', async () => {
    await db.query(`update search_control set elastic_cursors_until = now() - interval '5 minutes'`)
    await migrate(db)
    assert.ok((await readControl(db)).elasticCursorsUntil.getTime() >= Date.now() + 60 * 60_000)
  })

  it('stops serving from Postgres as soon as its verification is cleared, with no cached answer', async () => {
    await setVerified(true)
    const forced = { ...deps, search: { ...search, force: 'postgres' as const } }
    await ask(PARAMS, undefined, forced)
    await db.query('update search_control set pg_verified = false')
    await assert.rejects(ask(PARAMS, undefined, forced), unavailable)
    await assert.rejects(getEvent(forced, uriOf(2)), unavailable)
    await setVerified(true)
  })

  it('answers a last page without waiting for Elasticsearch to close its snapshot', async () => {
    const stuck = Object.assign(Object.create(search.elastic), { closeSnapshot: () => new Promise<void>(() => {}) }) as ElasticBackend
    const started = Date.now()
    const page = await ask('latitude=37.78&longitude=-122.43&limit=100', undefined, { ...deps, search: { ...search, elastic: stuck, force: 'elastic' } })
    assert.equal(page.events.length, 5)
    assert.equal(page.cursor, undefined)
    assert.ok(Date.now() - started < 1000)
  })

  it('falls back without waiting for Elasticsearch to close a snapshot', async () => {
    await setVerified(true)
    const stuck = {
      name: 'elastic',
      openSnapshot: async () => 'pit',
      page: async () => Promise.reject(new Error('search timed out')),
      closeSnapshot: () => new Promise<void>(() => {}),
    } as unknown as ElasticBackend
    const started = Date.now()
    const page = await ask(PARAMS, undefined, { ...deps, search: { ...search, elastic: stuck } })
    assert.equal(page.events.length, 2)
    assert.ok(Date.now() - started < 1000, 'answered from Postgres at once')
  })

  it('reports Postgres in health when Elasticsearch is down and Postgres serves in its place', async () => {
    await setVerified(true)
    const server = createQueryServer({ db, search: { ...search, elastic: deadElastic() }, serviceId: 'test', cursorKey: deps.cursorKey, rateLimitPerMinute: 100, pagingSessionsPerHour: 100 })
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
    try {
      const res = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/xrpc/_health`)
      const body = (await res.json()) as { index: { ok: boolean; backend: string; events: number } }
      assert.equal(body.index.ok, true)
      assert.equal(body.index.backend, 'postgres')
      assert.equal(body.index.events, 5)
    } finally {
      await new Promise<void>((r) => server.close(() => r()))
    }
  })

  it('reports the backend serving new sessions in health', async () => {
    await setVerified(true)
    const server = createQueryServer({ db, search: { ...search, force: 'postgres' }, serviceId: 'test', cursorKey: deps.cursorKey, rateLimitPerMinute: 100, pagingSessionsPerHour: 100 })
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

describe('first fill and the verifying comparison', { skip: (!databaseUrl || !elasticUrl) && 'TEST_DATABASE_URL and TEST_ELASTICSEARCH_URL not set' }, () => {
  const INDEX = `test-upkeep-${Date.now()}`
  let db: Db
  let es: Elastic
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
    es = connectElastic(elasticUrl!)
    // From PR 2 the comparison's independent copy is the current managed index.
    await createManagedIndex(es, INDEX)
    await db.query(`update search_control set es_in_use = true, es_current = true, elastic_index = $1`, [INDEX])
    projector = new Projector({ db, es, index: INDEX, plcUrl: 'http://127.0.0.1:1', log: () => {} })
    handler = new Handler(db, projector, () => {})
    for (let n = 1; n <= 3; n++) {
      await handler.handle(commit(EVENT, `3ke${n}`, eventValue({ name: `e${n}` })))
      await handler.handle(commit(SIDECAR, `3ks${n}`, sidecarValue(AUTHOR, `3ke${n}`, geo('37.78', '-122.43'))))
    }
    // The managed index written, Postgres emptied, as before a first fill.
    await db.query('truncate event_docs')
    await db.query('truncate es_pending')
  })
  after(async () => {
    await es.indices.delete({ index: INDEX }).catch(() => {})
    await db.end()
  })

  it('fills Postgres from index_state, compares it with today\'s index, and records verified', async () => {
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
    await projector.postgres.put(removed)
    assert.equal((await upkeep.compare()).ok, true)
    assert.equal((await readControl(db)).pgVerified, true)
  })

  it('names what differs, and compares again only after an hour', async () => {
    await db.query('update search_control set pg_verified = false, compared_at = null')
    await projector.postgres.remove(`at://${AUTHOR}/${SIDECAR}/3ks1`)
    await projector.postgres.put({ ...(await projector.postgres.get(`at://${AUTHOR}/${SIDECAR}/3ks2`))!, eventCid: 'bafychanged' })
    let now = Date.now()
    const opened: string[] = []
    const upkeep = new SearchUpkeep(db, projector, () => {}, () => now, { open: async (key) => void opened.push(key) })
    await upkeep.run()
    let row = await readControl(db)
    assert.equal(row.pgVerified, false)
    assert.deepEqual(opened, ['comparison'], 'from PR 2 a failed comparison opens an issue')
    assert.match(row.note!, /1 only in the managed index [^ ]+ \(at:\/\/.*3ks1\)/)
    assert.match(row.note!, /1 with different content \(at:\/\/.*3ks2\)/)

    await projector.fillPostgres(`at://${AUTHOR}/${SIDECAR}/3ks1`)
    await projector.fillPostgres(`at://${AUTHOR}/${SIDECAR}/3ks2`)
    now += 30 * 60_000
    await upkeep.run()
    assert.equal((await readControl(db)).pgVerified, false, 'not compared again within the hour')
    now += 31 * 60_000
    await upkeep.run()
    row = await readControl(db)
    assert.equal(row.pgVerified, true, row.note ?? '')
  })

  it('fills again and compares again after code that does not write Postgres changed index_state', async () => {
    const upkeep = new SearchUpkeep(db, projector, () => {})
    await upkeep.run()
    assert.equal((await readControl(db)).pgVerified, true)

    // This service before the change (after a revert, say) edits an event:
    // index_state and today's index move on, event_docs does not.
    const s1 = `at://${AUTHOR}/${SIDECAR}/3ks1`
    const edited = { ...(await projector.postgres.get(s1))!, eventCid: 'bafyedited' }
    await projector.elastic.put(edited)
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
    // The refill reads the stored records, which never had the edit, and
    // writes them to Postgres and to the managed index alike (in use).
    assert.equal(row.pgVerified, true, row.note ?? '')
    assert.notEqual((await projector.elastic.get(s1))!.eventCid, 'bafyedited')
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

  it('does not verify when index_state changes from old code while the comparison runs', async () => {
    await db.query('update search_control set pg_verified = false, compared_at = null')
    const upkeep = new SearchUpkeep(db, projector, () => {})
    const real = projector.elastic.allDocs.bind(projector.elastic)
    projector.elastic.allDocs = async () => {
      const docs = await real()
      const old = new pg.Client({ connectionString: dbUrl('upkeep_test') })
      await old.connect()
      await old.query(`update index_state set updated_at = now() where uri = $1`, [`at://${AUTHOR}/${SIDECAR}/3ks2`])
      await old.end()
      return docs
    }
    try {
      const res = await upkeep.compare()
      assert.equal(res.ok, false)
      const row = await readControl(db)
      assert.equal(row.pgVerified, false)
      assert.equal(row.firstFillDone, false)
    } finally {
      projector.elastic.allDocs = real
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
      fill = projector.fillPostgres(s3)
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
    const real = projector.elastic.allDocs.bind(projector.elastic)
    projector.elastic.allDocs = async () => {
      // The server ends the session holding the upkeep lock mid-comparison;
      // another ingester could now take the lock and verify.
      const holder = await db.query<{ pid: number }>(`select pid from pg_locks where locktype = 'advisory' and objid = 7140016 and granted`)
      assert.equal(holder.rowCount, 1)
      await db.query('select pg_terminate_backend($1)', [holder.rows[0].pid])
      await new Promise((r) => setTimeout(r, 100))
      // Another ingester's upkeep, holding the lock now, verifies.
      await db.query('update search_control set pg_verified = true, compared_at = now()')
      return real()
    }
    try {
      await new SearchUpkeep(db, projector, () => {}).run()
    } finally {
      projector.elastic.allDocs = real
      await projector.postgres.remove(extra.uri)
    }
    const row = await readControl(db)
    assert.equal(row.pgVerified, true, 'a run that lost its lock recorded its comparison over a later one')
    await db.query('update search_control set compared_at = null')
  })

  it('leaves verification as it is when a comparison cannot run', async () => {
    await db.query('update search_control set pg_verified = true, first_fill_done = true, fill_generation = search_generation_now()')
    const real = projector.elastic.allDocs.bind(projector.elastic)
    projector.elastic.allDocs = async () => Promise.reject(new Error('connect ECONNREFUSED'))
    try {
      const res = await new SearchUpkeep(db, projector, () => {}).compare()
      assert.equal(res.ok, false)
      assert.equal((await readControl(db)).pgVerified, true)
    } finally {
      projector.elastic.allDocs = real
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
    await projector.fillPostgres(s1)
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
  let es: ReturnType<typeof memoryElastic>
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
    es = memoryElastic()
    projector = new Projector({ db, es, index: 'events', plcUrl: 'http://127.0.0.1:1', log: () => {} })
    handler = new Handler(db, projector, () => {})
    await db.query(`update search_control set es_in_use = true, es_current = true, elastic_index = 'events'`)
  })
  after(() => db.end())

  it('sends Elasticsearch nothing while it is not in use, and stops writing today\'s index', async () => {
    await db.query(`update search_control set es_in_use = false, es_current = false, elastic_index = null`)
    const calls = es.calls
    await handler.handle(commit(EVENT, '3kew9', eventValue({ name: 'quiet' })))
    await handler.handle(commit(SIDECAR, '3ksw9', sidecarValue(AUTHOR, '3kew9', geo('37.78', '-122.43'))))
    assert.ok(await projector.postgres.get(`at://${AUTHOR}/${SIDECAR}/3ksw9`))
    assert.equal(es.calls, calls)
    assert.equal((await db.query('select count(*)::int as n from es_pending')).rows[0].n, 0)
    await db.query(`update search_control set es_in_use = true, es_current = true, elastic_index = 'events'`)
  })

  it('writes Postgres first, then the managed index with Postgres\'s version; a failure leaves the write pending and the event goes on', async () => {
    await handler.handle(commit(EVENT, '3kew1', eventValue({ name: 'first' })))
    es.down = true
    await handler.handle(commit(SIDECAR, '3ksw1', sidecarValue(AUTHOR, '3kew1', geo('37.78', '-122.43'))))
    assert.ok(await projector.postgres.get(sidecar), 'Postgres was written')
    assert.equal(es.docs.has(sidecar), false)
    const pending = await db.query<{ version: string }>('select version::text from es_pending where uri = $1', [sidecar])
    assert.equal(pending.rowCount, 1)
    es.down = false
    await projector.managed.replay(0)
    assert.ok(es.docs.has(sidecar))
    const current = await db.query<{ version: string }>('select version::text from event_docs where uri = $1 and closed_version is null', [sidecar])
    assert.equal(String(es.versions().get(sidecar)), current.rows[0].version)
    assert.equal(comparable((await projector.postgres.get(sidecar))!), comparable(es.docs.get(sidecar) as EventDoc))
    assert.equal((await db.query('select 1 from es_pending')).rowCount, 0)

    await handler.handle(commit(EVENT, '3kew1'))
    assert.equal(await projector.postgres.get(sidecar), undefined)
    assert.equal(es.docs.has(sidecar), false)
  })

  it('never lets an older write replace a newer one in the managed index', async () => {
    const uri = `at://${AUTHOR}/${SIDECAR}/3ksw7`
    const d = (await projector.postgres.get(uri)) ?? { ...doc(7), uri }
    const v1 = await projector.postgres.put({ ...d, view: { ...d.view, event: eventValue({ name: 'old' }) } })
    const v2 = await projector.postgres.put({ ...d, view: { ...d.view, event: eventValue({ name: 'new' }) } })
    await putManaged(es, 'events', { ...d, view: { ...d.view, event: eventValue({ name: 'new' }) } }, v2)
    await putManaged(es, 'events', { ...d, view: { ...d.view, event: eventValue({ name: 'old' }) } }, v1)
    assert.equal((es.docs.get(uri) as EventDoc).view.event.name, 'new')
    const v3 = await projector.postgres.remove(uri)
    await removeManaged(es, 'events', uri, v3)
    await putManaged(es, 'events', { ...d, view: { ...d.view, event: eventValue({ name: 'old' }) } }, v2)
    assert.equal(es.docs.has(uri), false, 'a delete is remembered, so an older put after it is refused')
    await db.query('delete from es_pending')
  })

  it('takes every event of a suppressed account out of Postgres and marks it, even while Elasticsearch fails', async () => {
    const uris = []
    for (const n of [2, 3]) {
      await handler.handle(commit(EVENT, `3kew${n}`, eventValue({ name: `e${n}` })))
      await handler.handle(commit(SIDECAR, `3ksw${n}`, sidecarValue(AUTHOR, `3kew${n}`, geo('37.78', '-122.43'))))
      uris.push(`at://${AUTHOR}/${SIDECAR}/3ksw${n}`)
    }
    await db.query('update accounts set active = false where did = $1', [AUTHOR])
    await db.query('insert into accounts (did, active) values ($1, false) on conflict (did) do update set active = false', [AUTHOR])
    es.down = true
    await projector.projectAccount(AUTHOR)
    es.down = false
    for (const uri of uris) assert.equal(await projector.postgres.get(uri), undefined, uri)
    const states = await db.query<{ complete: boolean }>('select complete from index_state where uri = any($1)', [uris])
    assert.deepEqual(states.rows.map((r) => r.complete), [false, false])
    assert.equal((await incompleteAmong(db, uris)).size, 2)
  })
})
