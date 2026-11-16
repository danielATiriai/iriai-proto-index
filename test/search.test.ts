import { after, before, beforeEach, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { randomBytes } from 'node:crypto'
import type { AddressInfo } from 'node:net'
import { migrate, searchWriterSeen, type Db } from '../src/db.js'
import type { EventDoc } from '../src/index/assemble.js'
import { ElasticBackend, connectElastic, ensureIndex, type Elastic } from '../src/index/elastic.js'
import { Handler, type JetstreamEvent } from '../src/ingest/handler.js'
import { Projector } from '../src/ingest/projector.js'
import { incompleteAmong, setIndexState } from '../src/ingest/store.js'
import { CursorError, decodeCursor, encodeCursor } from '../src/query/cursor.js'
import { bindingOf, parseEventQuery } from '../src/query/params.js'
import { createQueryServer } from '../src/query/server.js'
import { XrpcError, getEvent, queryEvents, type QueryDeps, type Search } from '../src/query/service.js'
import { bindingHash } from '../src/query/cursor.js'
import { SearchControl, readControl } from '../src/search/control.js'
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
  const setVerified = (v: boolean) => db.query('update search_control set pg_verified = $1', [v]).then(() => search.control.refresh())

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

  it('keeps serving getEvent from Elasticsearch when search_control cannot be read', async () => {
    const broken = { current: async () => Promise.reject(new Error('connect ECONNREFUSED')), refresh: async () => Promise.reject(new Error('down')) } as unknown as SearchControl
    const res = await getEvent({ ...deps, search: { ...search, control: broken } }, uriOf(2))
    assert.equal(res.value.uri, uriOf(2))
  })

  it('cuts off a stalled search_control read after 500 ms and serves getEvent from Elasticsearch', async () => {
    const stalled = { refresh: () => new Promise(() => {}) } as unknown as SearchControl
    const started = Date.now()
    const res = await getEvent({ ...deps, search: { ...search, control: stalled } }, uriOf(2))
    assert.equal(res.value.uri, uriOf(2))
    assert.ok(Date.now() - started < 1_500, `took ${Date.now() - started} ms`)
  })

  it('falls back to a verified Postgres for getEvent when today\'s index does not exist', async () => {
    await setVerified(true)
    const missing = new ElasticBackend(es, `${INDEX}-missing`)
    const res = await getEvent({ ...deps, search: { ...search, elastic: missing } }, uriOf(2))
    assert.equal(res.value.uri, uriOf(2))
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

  it('answers ServiceUnavailable, not an error, when today\'s index does not exist yet', async () => {
    await setVerified(false)
    const missing = new ElasticBackend(es, `${INDEX}-missing`)
    await assert.rejects(ask(PARAMS, undefined, { ...deps, search: { ...search, elastic: missing } }), unavailable)
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
    await ensureIndex(es, INDEX)
    projector = new Projector({ db, es, index: INDEX, plcUrl: 'http://127.0.0.1:1', log: () => {} })
    handler = new Handler(db, projector, () => {})
    for (let n = 1; n <= 3; n++) {
      await handler.handle(commit(EVENT, `3ke${n}`, eventValue({ name: `e${n}` })))
      await handler.handle(commit(SIDECAR, `3ks${n}`, sidecarValue(AUTHOR, `3ke${n}`, geo('37.78', '-122.43'))))
    }
    // As before this change: today's index written, Postgres empty.
    await db.query('truncate event_docs')
  })
  after(async () => {
    await es.indices.delete({ index: INDEX }).catch(() => {})
    await db.end()
  })

  it('fills Postgres from index_state, compares it with today\'s index, and latches verified', async () => {
    const log: string[] = []
    const upkeep = new SearchUpkeep(db, projector, (m) => log.push(m))
    await upkeep.run()
    const row = await readControl(db)
    assert.equal(row.firstFillDone, true)
    assert.equal(row.pgVerified, true, row.note ?? '')
    assert.match(row.note!, /3 documents/)
    assert.equal(await projector.postgres.count(), 3)

    // Verified stays set, even if a later comparison would not pass.
    await projector.postgres.remove(`at://${AUTHOR}/${SIDECAR}/3ks1`)
    const again = await upkeep.compare()
    assert.equal(again.ok, false)
    assert.equal((await readControl(db)).pgVerified, true)
  })

  it('names what differs, and compares again only after an hour', async () => {
    await db.query('update search_control set pg_verified = false, compared_at = null')
    await projector.postgres.put({ ...(await projector.postgres.get(`at://${AUTHOR}/${SIDECAR}/3ks2`))!, eventCid: 'bafychanged' })
    let now = Date.now()
    const upkeep = new SearchUpkeep(db, projector, () => {}, () => now)
    await upkeep.run()
    let row = await readControl(db)
    assert.equal(row.pgVerified, false)
    assert.match(row.note!, /1 only in Elasticsearch \(at:\/\/.*3ks1\)/)
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
    // The refill reads the stored records, which never had the edit, so it differs from today's index until that is projected too.
    assert.equal(row.pgVerified, false)
    await projector.projectSidecar(s1)
    await db.query('update search_control set compared_at = null')
    await upkeep.run()
    assert.equal((await readControl(db)).pgVerified, true)
  })

  it('clears verification when old code changes records or accounts, even if it fails before index_state', async () => {
    const upkeep = new SearchUpkeep(db, projector, () => {})
    await db.query('update search_control set compared_at = null')
    await upkeep.run()
    assert.equal((await readControl(db)).pgVerified, true)
    for (const sql of [`update accounts set active = false where did = '${AUTHOR}'`, `update records set updated_at = now() where did = '${AUTHOR}'`]) {
      await db.query('update search_control set pg_verified = true, first_fill_done = true')
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
  })
  after(() => db.end())

  it('writes Postgres first, then today\'s index; an Elasticsearch failure fails the event, and the replay writes both', async () => {
    await handler.handle(commit(EVENT, '3kew1', eventValue({ name: 'first' })))
    es.down = true
    await assert.rejects(handler.handle(commit(SIDECAR, '3ksw1', sidecarValue(AUTHOR, '3kew1', geo('37.78', '-122.43')))))
    assert.ok(await projector.postgres.get(sidecar), 'Postgres was written before Elasticsearch failed')
    assert.equal(es.docs.has(sidecar), false)
    es.down = false
    await handler.handle(commit(SIDECAR, '3ksw1', sidecarValue(AUTHOR, '3kew1', geo('37.78', '-122.43'))))
    assert.ok(es.docs.has(sidecar))
    assert.equal(comparable((await projector.postgres.get(sidecar))!), comparable(es.docs.get(sidecar) as EventDoc))

    await handler.handle(commit(EVENT, '3kew1'))
    assert.equal(await projector.postgres.get(sidecar), undefined)
    assert.equal(es.docs.has(sidecar), false)
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
    await assert.rejects(projector.projectAccount(AUTHOR))
    es.down = false
    for (const uri of uris) assert.equal(await projector.postgres.get(uri), undefined, uri)
    const states = await db.query<{ complete: boolean }>('select complete from index_state where uri = any($1)', [uris])
    assert.deepEqual(states.rows.map((r) => r.complete), [false, false])
    assert.equal((await incompleteAmong(db, uris)).size, 2)
  })
})
