import { after, before, beforeEach, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import type { Db } from '../src/db.js'
import type { EventDoc } from '../src/index/assemble.js'
import { createManagedIndex, putManaged, type ElasticBackend } from '../src/index/elastic.js'
import { DEFAULT_THRESHOLDS } from '../src/settings.js'
import { differences } from '../src/search/agreement.js'
import type { SearchBackend } from '../src/search/backend.js'
import { readControl, route, type RouteRow } from '../src/search/control.js'
import { SearchController } from '../src/search/controller.js'
import { GitHubIssues, type Issues } from '../src/search/issues.js'
import { ManagedIndex } from '../src/search/managed.js'
import { PostgresBackend } from '../src/search/postgres.js'
import { BUCKETS_MS, addStats, bucketOf, lastDays, p95, shapeOf, storedShapes } from '../src/search/stats.js'
import { EVENT, SIDECAR, eventValue, geo } from './fixtures.js'
import { databaseUrl, freshDb, memoryElastic } from './support.js'

const AUTHOR = 'did:plc:ewvi7nxzyoun6zhxrhs64oiz'
const LEGACY = 'placeproto-events-v1'
const uriOf = (n: number) => `at://${AUTHOR}/${SIDECAR}/3kc${String(n).padStart(10, '0')}`

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

const row = (over: Partial<RouteRow> = {}): RouteRow => ({
  pgVerified: true,
  state: 'off',
  esInUse: true,
  esCurrent: true,
  elasticIndex: 'placeproto-events-1',
  pendingStale: false,
  ...over,
})

describe('routing a new session (spec 5.2)', () => {
  it('follows the state unless forced, serves the managed index only when it may, and falls back to a verified Postgres', () => {
    assert.equal(route(undefined, row())?.backend, 'postgres')
    assert.equal(route(undefined, row({ state: 'on' }))?.backend, 'elastic')
    assert.equal(route('elastic', row())?.backend, 'elastic')
    assert.equal(route('postgres', row({ state: 'on' }))?.backend, 'postgres')
    for (const bad of [{ esInUse: false }, { esCurrent: false }, { elasticIndex: null }, { pendingStale: true }]) {
      assert.equal(route('elastic', row(bad))?.backend, 'postgres', JSON.stringify(bad))
      assert.equal(route('elastic', row({ ...bad, pgVerified: false })), undefined, JSON.stringify(bad))
    }
    assert.equal(route(undefined, row({ pgVerified: false })), undefined)
    assert.equal(route(undefined, row({ state: 'on', pgVerified: false }))?.backend, 'elastic')
  })
})

describe('search stats (spec 6.1)', { skip: !databaseUrl && 'TEST_DATABASE_URL not set' }, () => {
  let db: Db
  before(async () => {
    db = await freshDb('stats_test')
  })
  after(() => db.end())

  it('reads the thresholds exactly at bucket edges', () => {
    assert.equal(bucketOf(249.9) < bucketOf(250), true)
    const hist = new Array(BUCKETS_MS.length + 1).fill(0)
    hist[bucketOf(250)] = 100
    assert.equal(p95(hist)!.lower, 250)
    hist.fill(0)
    hist[bucketOf(99)] = 100
    assert.equal(p95(hist)!.upper, 100)
    hist.fill(0)
    hist[bucketOf(5_000)] = 1
    assert.equal(p95(hist)!.upper, Infinity)
    assert.equal(p95([0, 0]), undefined)
  })

  it('adds counts from several replicas and keeps at most 200 distinct shapes', async () => {
    const day = new Date(Date.now() - 86_400_000).toISOString().slice(0, 10)
    const h = (ms: number, n: number) => {
      const a = new Array(BUCKETS_MS.length + 1).fill(0)
      a[bucketOf(ms)] = n
      return a
    }
    const shapes = Array.from({ length: 150 }, (_, i) => JSON.stringify({ sort: 'uri', i }))
    await addStats(db, { day, calls: 300, pg: h(300, 300), es: [], pgTimeouts: 0, shapes })
    await addStats(db, { day, calls: 300, pg: h(300, 300), es: [], pgTimeouts: 4, shapes: [...shapes.slice(0, 10), ...Array.from({ length: 100 }, (_, i) => JSON.stringify({ sort: 'uri', j: i }))] })
    const [y] = await lastDays(db, 1)
    assert.equal(y.calls, 600)
    assert.equal(y.pgP95!.lower, 300)
    assert.equal(y.pgP95!.samples, 600)
    assert.equal(y.pgTimeoutRate, 4 / 600)
    const stored = await db.query<{ n: number }>('select jsonb_array_length(shapes) as n from search_stats where day = $1', [day])
    assert.equal(stored.rows[0].n, 200)
    assert.equal((await storedShapes(db, 500)).length, 200)
  })

  it('rounds coordinates in a shape and leaves out the cursor and limit', () => {
    const s = JSON.parse(shapeOf({ latitude: 37.123456, longitude: -122.98765, sort: 'distance', limit: 7, cursor: 'x' }))
    assert.deepEqual(s, { latitude: 37.123, longitude: -122.988, sort: 'distance' })
  })
})

describe('comparing the first 50 results (spec 6.4, verifying)', () => {
  const backend = (docs: EventDoc[]): SearchBackend =>
    ({
      name: 'postgres',
      openSnapshot: async () => '1',
      closeSnapshot: async () => {},
      page: async () => ({ hits: docs.map((d) => ({ doc: d, sort: [] })), snapshot: '1' }),
    }) as unknown as SearchBackend
  const q = { latitude: 37.78, longitude: -122.43, sort: 'distance' as const, limit: 50 }

  it('treats results under 1 m apart as tied, and nothing else', async () => {
    // Two events 0.5 m apart in either order agree; two 5 m apart do not.
    const a = doc(1, 'a', 37.781, -122.43)
    const b = doc(2, 'b', 37.781 + 0.5 / 111195, -122.43)
    const c = doc(3, 'c', 37.781 + 5 / 111195, -122.43)
    assert.deepEqual(await differences(backend([a, b, c]), backend([b, a, c]), q), [])
    assert.notDeepEqual(await differences(backend([a, c, b]), backend([a, b, c]), q), [])
  })

  it('ignores an event within 1 m of the radius edge, and finds a missing one elsewhere', async () => {
    const edge = doc(4, 'edge', 37.78 + 1000 / 111195, -122.43)
    const inside = doc(5, 'inside', 37.7805, -122.43)
    const radius = { ...q, radiusMeters: 1000 }
    assert.deepEqual(await differences(backend([inside, edge]), backend([inside]), radius), [])
    assert.deepEqual(await differences(backend([inside, edge]), backend([edge]), radius), [inside.uri])
  })

  it('compares start-time order exactly', async () => {
    const byStart = { sort: 'startsAt' as const, limit: 50 }
    assert.deepEqual(await differences(backend([doc(1), doc(2)]), backend([doc(2), doc(1)]), byStart).then((d) => d.sort()), [uriOf(1), uriOf(2)])
  })
})

describe('GitHub issues', { skip: !databaseUrl && 'TEST_DATABASE_URL not set' }, () => {
  let db: Db
  before(async () => {
    db = await freshDb('issues_test')
  })
  after(() => db.end())

  it('opens one issue per cause a day, and only logs without a token', async () => {
    const sent: string[] = []
    const fake = (async (_url: string, init: { body: string }) => {
      sent.push(JSON.parse(init.body).title)
      return { ok: true, status: 201 }
    }) as unknown as typeof fetch
    const issues = new GitHubIssues(db, 'token', 'o/r', () => {}, fake)
    await issues.open('k', 'first', 'body')
    await issues.open('k', 'again', 'body')
    await issues.open('other', 'second', 'body')
    assert.deepEqual(sent, ['first', 'second'])
    const logs: string[] = []
    await new GitHubIssues(db, undefined, 'o/r', (m) => logs.push(m), fake).open('third', 'third', 'body')
    assert.deepEqual(sent, ['first', 'second'])
    assert.match(logs[0], /GITHUB_ALERT_TOKEN not set/)
  })

  it('tries again at the next tick when GitHub refuses', async () => {
    let calls = 0
    const refusing = (async () => (calls++, { ok: false, status: 401 })) as unknown as typeof fetch
    const issues = new GitHubIssues(db, 'token', 'o/r', () => {}, refusing)
    await issues.open('refused', 't', 'b')
    await issues.open('refused', 't', 'b')
    assert.equal(calls, 2)
  })
})

describe('the managed index and the controller', { skip: !databaseUrl && 'TEST_DATABASE_URL not set' }, () => {
  let db: Db
  let es: ReturnType<typeof memoryElastic>
  let pg: PostgresBackend
  let managed: ManagedIndex
  let opened: string[]
  const issues: Issues = { open: async (key) => void opened.push(key) }
  const controller = (over: Partial<ConstructorParameters<typeof SearchController>[0]> = {}) =>
    new SearchController({
      db,
      managed,
      postgres: pg,
      // The query comparison reads Postgres on both sides unless a test says otherwise.
      elastic: { withIndex: () => pg } as unknown as ElasticBackend,
      legacyIndex: LEGACY,
      issues,
      searchEs: false,
      thresholds: DEFAULT_THRESHOLDS,
      startedAt: new Date(),
      log: () => {},
      ...over,
    })
  const managedIndexes = () => [...es.indexes.keys()].filter((n) => n.startsWith('placeproto-events-')).sort()

  beforeEach(async () => {
    db?.end()
    db = await freshDb('controller_test')
    es = memoryElastic('unused')
    pg = new PostgresBackend(db)
    managed = new ManagedIndex(db, es, pg, () => {})
    opened = []
    for (let n = 1; n <= 5; n++) await pg.put(doc(n))
    await db.query(`update search_control set pg_verified = true, first_fill_done = true, fill_generation = search_generation_now()`)
  })
  after(() => db.end())

  it('builds a managed index with every current row at its Postgres version, keeping today\'s index for its hour', async () => {
    es.indexes.set(LEGACY, new Map())
    es.indexes.set('placeproto-events-7', new Map())
    await managed.setInUse(true)
    const index = await managed.build(LEGACY)
    assert.equal(index, 'placeproto-events-1')
    assert.deepEqual(managedIndexes(), ['placeproto-events-1', LEGACY], 'the leftover went, today\'s index stays')
    const versions = es.versions(index)
    const current = await db.query<{ uri: string; version: string }>('select uri, version::text from event_docs where closed_version is null')
    assert.equal(versions.size, 5)
    for (const r of current.rows) assert.equal(String(versions.get(r.uri)), r.version)

    await managed.markCurrent(index)
    await db.query(`update search_control set legacy_drop_after = now() - interval '1 second'`)
    await managed.dropLegacyWhenDue(LEGACY)
    assert.deepEqual(managedIndexes(), ['placeproto-events-1'])
  })

  it('ends a build current even when writes and deletes race the copy', async () => {
    await managed.setInUse(true)
    const realBulk = es.bulk.bind(es)
    let raced = false
    ;(es as unknown as { bulk: typeof es.bulk }).bulk = (async (p: never) => {
      if (!raced) {
        raced = true
        // Before the copy lands: one event edited and one deleted, each
        // written to Postgres and then to the new index with newer versions.
        const index = (await readControl(db)).elasticIndex!
        const v2 = await pg.put(doc(2, 'edited'))
        await putManaged(es, index, doc(2, 'edited'), v2)
        const v3 = await pg.remove(uriOf(3))
        await es.delete({ index, id: uriOf(3), version: Number(v3), version_type: 'external' } as never, { ignore: [404] } as never)
      }
      return realBulk(p)
    }) as never
    const index = await managed.build(LEGACY)
    assert.equal((es.indexes.get(index)!.get(uriOf(2))!.doc as EventDoc).view.event.name, 'edited')
    assert.equal(es.versions(index).has(uriOf(3)), false, 'the older copy did not bring back a deleted event')
    assert.equal(await managed.reconcile(index), 0)
  })

  it('reconciles: a missing, older or extra document gets an es_pending row, and the replay fixes it', async () => {
    await managed.setInUse(true)
    const index = await managed.build(LEGACY)
    await managed.markCurrent(index)
    const docs = es.indexes.get(index)!
    docs.delete(uriOf(1))
    docs.set(uriOf(2), { doc: doc(2, 'stale'), version: 1 })
    docs.set(uriOf(99), { doc: doc(99), version: 1 })
    assert.equal(await managed.reconcile(index), 3)
    await new Promise((r) => setTimeout(r, 2_100))
    assert.equal((await readControl(db)).pendingStale, true, 'the index may not serve until replayed')
    assert.equal(await managed.replay(), 0)
    assert.equal((await readControl(db)).pendingStale, false)
    assert.equal(await managed.reconcile(index), 0)
    assert.equal(es.versions(index).has(uriOf(99)), false)
  })

  it('ends use only once elastic_cursors_until has passed, then sends nothing more', async () => {
    await managed.setInUse(true)
    const index = await managed.build(LEGACY)
    await managed.markCurrent(index)
    await db.query(`update search_control set elastic_cursors_until = now() + interval '5 minutes'`)
    assert.equal(await managed.endUse(LEGACY), false)
    assert.equal((await readControl(db)).esInUse, true)
    await db.query(`update search_control set elastic_cursors_until = now() - interval '1 second'`)
    assert.equal(await managed.endUse(LEGACY), true)
    const r = await readControl(db)
    assert.deepEqual([r.esInUse, r.esCurrent, r.elasticIndex], [false, false, null])
    assert.equal(es.indexes.has(index), false)
    const calls = es.calls
    const v = await pg.put(doc(4, 'after'))
    assert.ok(v > 0n)
    await managed.replay()
    await controller().keep()
    assert.equal(es.calls, calls)
    assert.equal(await managed.pendingCount(), 0)
  })

  it('with SEARCH_ES on, puts Elasticsearch in use and builds a current managed index; with it off, ends use once allowed', async () => {
    await controller({ searchEs: true }).tick()
    let r = await readControl(db)
    assert.deepEqual([r.esInUse, r.esCurrent, r.elasticIndex, r.state], [true, true, 'placeproto-events-1', 'off'])
    await db.query(`update search_control set elastic_cursors_until = now() - interval '1 second'`)
    await controller({ searchEs: false }).tick()
    r = await readControl(db)
    assert.deepEqual([r.esInUse, r.esCurrent], [false, false])
    assert.deepEqual(managedIndexes(), [])
  })

  it('keeps Elasticsearch in use, and opens an issue, while Postgres is not verified', async () => {
    await controller({ searchEs: true }).tick()
    await db.query(`update search_control set pg_verified = false, elastic_cursors_until = now() - interval '1 second'`)
    await controller().tick()
    assert.equal((await readControl(db)).esInUse, true)
    assert.ok(opened.includes('unverified'))
  })

  it('switches on at 500,000 events: off, ready, starting, backfilling, verifying, on; then drains on repeated fallbacks and turns off', async () => {
    const c = controller({ thresholds: { ...DEFAULT_THRESHOLDS, onEvents: 5 } })
    await c.tick()
    let r = await readControl(db)
    assert.equal(r.state, 'starting')
    assert.equal(r.esInUse, true)
    assert.ok(opened.includes('switch-on'))
    await c.tick()
    assert.equal((await readControl(db)).state, 'backfilling')
    await c.tick()
    r = await readControl(db)
    assert.equal(r.state, 'verifying')
    assert.equal(r.esCurrent, false, 'a candidate until verified')
    await c.tick()
    r = await readControl(db)
    assert.equal(r.state, 'on', r.controllerNote ?? '')
    assert.equal(r.esCurrent, true)
    assert.equal(route(undefined, r)?.backend, 'elastic')

    await db.query(`update search_control set es_seen_at = now()`)
    await db.query(`insert into search_fallbacks (at) select now() from generate_series(1, 21)`)
    await c.tick()
    r = await readControl(db)
    assert.equal(r.state, 'draining')
    assert.equal(route(undefined, r)?.backend, 'postgres')
    assert.equal(r.esInUse, true, 'in use until the cursors\' hour has passed')
    await db.query(`update search_control set elastic_cursors_until = now() - interval '1 second'`)
    await c.tick()
    r = await readControl(db)
    assert.deepEqual([r.state, r.esInUse], ['off', false])
    assert.deepEqual(managedIndexes(), [])
  })

  it('switches on at most once in 30 days', async () => {
    await db.query(`update search_control set last_switch_on = now() - interval '29 days'`)
    const c = controller({ thresholds: { ...DEFAULT_THRESHOLDS, onEvents: 5 } })
    await c.tick()
    assert.equal((await readControl(db)).state, 'ready')
    await db.query(`update search_control set last_switch_on = now() - interval '31 days'`)
    await c.tick()
    assert.equal((await readControl(db)).state, 'starting')
  })

  it('turns off again on a failed check, deletes the candidate, and disables itself after two failures in 30 days', async () => {
    const wrong = new PostgresBackend(db)
    const disagreeing = {
      withIndex: () => ({ ...wrong, name: 'elastic', openSnapshot: () => wrong.openSnapshot(), closeSnapshot: async () => {}, page: async () => ({ hits: [], snapshot: '0' }) }),
    } as unknown as ElasticBackend
    const c = controller({ thresholds: { ...DEFAULT_THRESHOLDS, onEvents: 5 }, elastic: disagreeing })
    for (let i = 0; i < 4; i++) await c.tick()
    let r = await readControl(db)
    assert.equal(r.state, 'off')
    assert.equal(r.disabled, false)
    assert.ok(opened.includes('failure'))
    assert.deepEqual(managedIndexes(), [], 'the candidate no session used is gone')

    await db.query(`update search_control set last_switch_on = now() - interval '31 days', elastic_cursors_until = now() - interval '1 second'`)
    for (let i = 0; i < 4; i++) await c.tick()
    r = await readControl(db)
    assert.equal(r.state, 'off')
    assert.equal(r.disabled, true)
    await db.query(`update search_control set last_switch_on = now() - interval '31 days'`)
    await c.tick()
    assert.equal((await readControl(db)).state, 'off', 'disabled: does nothing until a person clears it')
  })

  it('switches on after 3 slow days with enough calls, and not on 2', async () => {
    const hist = (ms: number) => {
      const a = new Array(BUCKETS_MS.length + 1).fill(0)
      a[bucketOf(ms)] = 600
      return a
    }
    const day = (n: number) => new Date(Date.now() - n * 86_400_000).toISOString().slice(0, 10)
    for (const n of [1, 2]) await addStats(db, { day: day(n), calls: 600, pg: hist(260), es: [], pgTimeouts: 0, shapes: [] })
    const c = controller()
    assert.equal(await c.onReason(5), undefined)
    await addStats(db, { day: day(3), calls: 499, pg: hist(260), es: [], pgTimeouts: 0, shapes: [] })
    assert.equal(await c.onReason(5), undefined, 'a day under 500 calls does not count')
    await addStats(db, { day: day(3), calls: 1, pg: [], es: [], pgTimeouts: 0, shapes: [] })
    assert.match((await c.onReason(5))!, /3 consecutive days/)
  })

  it('switches on when over 1% of Postgres searches time out', async () => {
    const day = (n: number) => new Date(Date.now() - n * 86_400_000).toISOString().slice(0, 10)
    const hist = new Array(BUCKETS_MS.length + 1).fill(0)
    hist[bucketOf(20)] = 980
    hist[bucketOf(2_000)] = 20
    for (const n of [1, 2, 3]) await addStats(db, { day: day(n), calls: 1000, pg: hist, es: [], pgTimeouts: 11, shapes: [] })
    assert.match((await controller().onReason(5))!, /timeouts/)
  })

  it('drains after 30 days on and 14 quiet days', async () => {
    const c = controller()
    const day = (n: number) => new Date(Date.now() - n * 86_400_000).toISOString().slice(0, 10)
    const fast = new Array(BUCKETS_MS.length + 1).fill(0)
    fast[bucketOf(50)] = 600
    for (let n = 1; n <= 14; n++) {
      await addStats(db, { day: day(n), calls: 600, pg: fast, es: [], pgTimeouts: 0, shapes: [] })
      await db.query('update search_stats set events = 5 where day = $1', [day(n)])
    }
    await db.query(`update search_control set state = 'on', since = now() - interval '29 days', es_seen_at = now()`)
    assert.equal(await c.drainReason(await readControl(db), 5), undefined, 'not before 30 days on')
    await db.query(`update search_control set since = now() - interval '31 days'`)
    assert.match((await c.drainReason(await readControl(db), 5))!.why, /14 consecutive days/)
    assert.equal(await c.drainReason(await readControl(db), 300_000), undefined, 'not with too many events')
  })
})
