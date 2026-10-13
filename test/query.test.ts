import { after, before, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { randomBytes } from 'node:crypto'
import type { Db } from '../src/db.js'
import { assemble, type EventDoc } from '../src/index/assemble.js'
import { connectElastic, ensureIndex, putDoc, type Elastic } from '../src/index/elastic.js'
import { applyChange, setIndexState } from '../src/ingest/store.js'
import { CursorError } from '../src/query/cursor.js'
import { parseEventQuery } from '../src/query/params.js'
import { XrpcError, getEvent, queryEvents, type QueryDeps } from '../src/query/service.js'
import { CID_A, CID_B, EVENT, OTHER, SIDECAR, STEWARD, eventValue, geo, row, sidecarValue } from './fixtures.js'
import { databaseUrl, elasticUrl, freshDb, switchableDb } from './support.js'

const INDEX = `test-events-${Date.now()}`
const PEACE_PLAZA = 'latitude=37.785054&longitude=-122.429827'

interface Spec {
  key: string
  did?: string
  location: Record<string, unknown>
  startsAt?: string
  endsAt?: string
}

// Distances from Peace Plaza grow with the index: a (0 m) < b < c < d. e and f
// share one point (an equal-distance tie). u has an address only (unresolved).
const SPECS: Spec[] = [
  { key: 'a', location: geo('37.785054', '-122.429827'), startsAt: '2026-10-15T18:00:00Z', endsAt: '2026-10-15T20:00:00Z' },
  { key: 'b', location: geo('37.7865506', '-122.4308591'), startsAt: '2026-10-16T01:30:00Z' },
  { key: 'c', location: geo('37.7886501', '-122.4267882'), startsAt: '2026-10-24T23:00:00Z', endsAt: '2026-10-25T01:00:00Z' },
  { key: 'd', location: geo('37.80', '-122.40'), did: OTHER },
  { key: 'e', location: geo('37.79', '-122.43'), startsAt: '2026-10-15T20:00:00Z', endsAt: '2026-10-15T22:00:00Z' },
  { key: 'f', location: geo('37.79', '-122.43'), startsAt: '2026-10-14T00:00:00Z', endsAt: '2026-10-15T18:00:00Z' },
  { key: 'u', location: { $type: 'community.lexicon.location.address', country: 'US', locality: 'San Francisco' }, startsAt: '2026-10-17T17:00:00Z' },
  { key: 'x', location: geo('-16.5', '179.9'), startsAt: '2026-10-20T00:00:00Z' },
]

const uriOf = (key: string, did = STEWARD) => `at://${did}/${SIDECAR}/3k${key.repeat(11)}`
const keyOf = (uri: string) => uri.split('/').pop()!.slice(2, 3)

describe('query API', { skip: (!databaseUrl || !elasticUrl) && 'TEST_DATABASE_URL and TEST_ELASTICSEARCH_URL not set' }, () => {
  let db: Db
  let es: Elastic
  let deps: QueryDeps

  async function q(params: string, opts: { cursor?: string } = {}): Promise<{ keys: string[]; cursor?: string }> {
    const sp = new URLSearchParams(params)
    if (opts.cursor) sp.set('cursor', opts.cursor)
    const res = await queryEvents(deps, parseEventQuery(sp))
    return { keys: res.events.map((e) => keyOf(e.uri)), cursor: res.cursor }
  }

  before(async () => {
    db = await freshDb('query_test')
    es = connectElastic(elasticUrl!)
    await ensureIndex(es, INDEX)
    deps = { db, es, index: INDEX, serviceId: 'test', cursorKey: randomBytes(32) }
    for (const s of SPECS) {
      const did = s.did ?? STEWARD
      const rkey = `3e${s.key.repeat(11)}`
      const event = row(did, EVENT, rkey, eventValue({ name: s.key, startsAt: s.startsAt, endsAt: s.endsAt }))
      if (!s.startsAt) delete event.value!.startsAt
      if (!s.endsAt) delete event.value!.endsAt
      const sidecar = { ...row(did, SIDECAR, uriOf(s.key, did).split('/').pop()!, sidecarValue(did, rkey, s.location), CID_B) }
      const result = assemble(sidecar, event, true)
      assert.ok(result.complete, s.key)
      await putDoc(es, INDEX, result.doc as EventDoc)
      await setIndexState(db, sidecar.uri, did, true, null)
      await applyChange(db, { did, collection: SIDECAR, rkey: sidecar.rkey, cid: CID_B, value: sidecar.value! })
      await applyChange(db, { did, collection: EVENT, rkey, cid: CID_A, value: event.value! })
    }
    await es.indices.refresh({ index: INDEX })
  })
  after(async () => {
    await es.indices.delete({ index: INDEX }).catch(() => {})
    await db.end()
  })

  it('orders by distance, breaks equal distances by sidecar URI, and puts unresolved locations last', async () => {
    const { keys } = await q(`${PEACE_PLAZA}&limit=100`)
    assert.deepEqual(keys, ['a', 'b', 'c', 'e', 'f', 'd', 'x', 'u'])
  })

  it('filters by radius and excludes unresolved locations from any geographic bound', async () => {
    assert.deepEqual((await q(`${PEACE_PLAZA}&radiusMeters=600`)).keys, ['a', 'b', 'c', 'e', 'f'])
    assert.deepEqual((await q(`${PEACE_PLAZA}&radiusMeters=1`)).keys, ['a'])
  })

  it('filters by a bounding box, including one across the antimeridian', async () => {
    assert.deepEqual((await q('west=-122.431&south=37.784&east=-122.426&north=37.789')).keys.sort(), ['a', 'b', 'c'])
    assert.deepEqual((await q('west=179&south=-20&east=-179&north=-10')).keys, ['x'])
  })

  it('matches time windows by overlap: underway events in, an end at windowStart out, no start out', async () => {
    const win = 'windowStart=2026-10-15T18:00:00Z&windowEnd=2026-10-16T00:00:00Z'
    // a overlaps, e overlaps, f ends exactly at windowStart (excluded), b starts after windowEnd, d has no start.
    assert.deepEqual((await q(`${win}&sort=startsAt`)).keys, ['a', 'e'])
    // A point event matches when windowStart <= startsAt < windowEnd.
    assert.deepEqual((await q('windowStart=2026-10-16T01:30:00Z&windowEnd=2026-10-16T01:31:00Z')).keys, ['b'])
    assert.deepEqual((await q('windowStart=2026-10-16T01:29:00Z&windowEnd=2026-10-16T01:30:00Z')).keys, [])
    assert.deepEqual((await q('windowStart=2026-10-24T00:00:00Z&sort=startsAt')).keys, ['c'])
  })

  it('sorts by start time with undated events last, and filters by author and eventUri', async () => {
    assert.deepEqual((await q('sort=startsAt&limit=100')).keys, ['f', 'a', 'e', 'b', 'u', 'x', 'c', 'd'])
    assert.deepEqual((await q(`author=${OTHER}`)).keys, ['d'])
    assert.deepEqual((await q(`eventUri=at://${STEWARD}/${EVENT}/3e${'b'.repeat(11)}`)).keys, ['b'])
  })

  it('pages through a snapshot with cursors bound to the query', async () => {
    const seen: string[] = []
    let page = await q(`${PEACE_PLAZA}&limit=3`)
    seen.push(...page.keys)
    while (page.cursor) {
      page = await q(`${PEACE_PLAZA}&limit=3`, { cursor: page.cursor })
      seen.push(...page.keys)
    }
    assert.deepEqual(seen, ['a', 'b', 'c', 'e', 'f', 'd', 'x', 'u'])

    const first = await q(`${PEACE_PLAZA}&limit=2`)
    await assert.rejects(q(`${PEACE_PLAZA}&sort=startsAt&limit=2`, { cursor: first.cursor }), (err: unknown) => {
      return err instanceof CursorError && err.error === 'InvalidCursor'
    })
  })

  it('drops results that stopped being complete mid-session, moving the cursor past them', async () => {
    const first = await q(`${PEACE_PLAZA}&limit=2`)
    assert.deepEqual(first.keys, ['a', 'b'])
    await setIndexState(db, uriOf('c'), STEWARD, false, 'base-deleted')
    await setIndexState(db, uriOf('e'), STEWARD, false, 'base-deleted')
    const second = await q(`${PEACE_PLAZA}&limit=2`, { cursor: first.cursor })
    assert.deepEqual(second.keys, [])
    assert.ok(second.cursor)
    const third = await q(`${PEACE_PLAZA}&limit=2`, { cursor: second.cursor })
    assert.deepEqual(third.keys, ['f', 'd'])
    await setIndexState(db, uriOf('c'), STEWARD, true, null)
    await setIndexState(db, uriOf('e'), STEWARD, true, null)
  })

  it('getEvent returns the complete view with versions, or a named 404', async () => {
    const res = await getEvent(deps, uriOf('b'))
    assert.equal(res.value.uri, uriOf('b'))
    assert.equal((res.value.event as { name: string }).name, 'b')
    assert.deepEqual(res.version, { eventCid: CID_A, sidecarCid: CID_B })

    await assert.rejects(getEvent(deps, uriOf('z')), (e: unknown) => e instanceof XrpcError && e.error === 'SidecarNotFound')
    await applyChange(db, { did: STEWARD, collection: SIDECAR, rkey: '3korphan00001', cid: CID_B, value: sidecarValue(STEWARD, '3emissing0001', geo('1', '1')) })
    await assert.rejects(getEvent(deps, `at://${STEWARD}/${SIDECAR}/3korphan00001`), (e: unknown) => e instanceof XrpcError && e.error === 'EventNotFound')
  })

  it('answers 503 ServiceUnavailable, not an empty page, when the index is down', async () => {
    const down = { ...deps, es: connectElastic('http://127.0.0.1:1') }
    await assert.rejects(queryEvents(down, parseEventQuery(new URLSearchParams(PEACE_PLAZA))), (e: unknown) => e instanceof XrpcError && e.status === 503)
  })

  it('answers 503 ServiceUnavailable when Postgres is down, for queryEvents and getEvent', async () => {
    const pg = switchableDb(db)
    pg.down = true
    const down = { ...deps, db: pg }
    const unavailable = (e: unknown) => e instanceof XrpcError && e.status === 503 && e.error === 'ServiceUnavailable'
    await assert.rejects(queryEvents(down, parseEventQuery(new URLSearchParams(PEACE_PLAZA))), unavailable)
    await assert.rejects(getEvent(down, uriOf('z')), unavailable)
  })

  it('counts a paging session only when the answer returns a cursor', async () => {
    let opened = 0
    const counted = { ...deps, sessions: { wait: () => 0, opened: () => void opened++ } }
    const ask = (params: string, cursor?: string) => {
      const sp = new URLSearchParams(params)
      if (cursor) sp.set('cursor', cursor)
      return queryEvents(counted, parseEventQuery(sp))
    }
    await ask(`${PEACE_PLAZA}&limit=100`)
    assert.equal(opened, 0)
    const first = await ask(`${PEACE_PLAZA}&limit=2`)
    assert.equal(opened, 1)
    await ask(`${PEACE_PLAZA}&limit=2`, first.cursor)
    assert.equal(opened, 1)

    const full = { ...deps, sessions: { wait: () => 120, opened: () => {} } }
    await assert.rejects(queryEvents(full, parseEventQuery(new URLSearchParams(PEACE_PLAZA))), (e: unknown) => {
      return e instanceof XrpcError && e.status === 429 && e.retryAfter === 120
    })
  })
})
