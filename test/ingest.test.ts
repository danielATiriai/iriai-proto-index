import { after, before, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import type { Db } from '../src/db.js'
import { Handler, type JetstreamEvent } from '../src/ingest/handler.js'
import { Projector } from '../src/ingest/projector.js'
import { getRecord } from '../src/ingest/store.js'
import { CID_A, CID_B, EVENT, SIDECAR, STEWARD, eventValue, geo, sidecarValue } from './fixtures.js'
import { databaseUrl, fakeNetwork, freshDb, memoryElastic, type FakeNetwork } from './support.js'

const AUTHOR = 'did:plc:ewvi7nxzyoun6zhxrhs64oiz'
let t = 1_760_000_000_000_000
let rev = 0
const nextRev = () => `3m${String(++rev).padStart(11, '2')}`

function commit(did: string, collection: string, rkey: string, value?: Record<string, unknown>, opts: { rev?: string; cid?: string } = {}): JetstreamEvent {
  return {
    did,
    time_us: t++,
    kind: 'commit',
    commit: {
      rev: opts.rev ?? nextRev(),
      operation: value ? 'update' : 'delete',
      collection,
      rkey,
      ...(value ? { record: value, cid: opts.cid ?? CID_A } : {}),
    },
  }
}

describe('ingestion', { skip: !databaseUrl && 'TEST_DATABASE_URL not set' }, () => {
  let db: Db
  let es: ReturnType<typeof memoryElastic>
  let net: FakeNetwork
  let projector: Projector
  let handler: Handler
  const eventUri = `at://${AUTHOR}/${EVENT}/3kevent000001`
  const sidecarUri = `at://${AUTHOR}/${SIDECAR}/3ksidecar0001`

  before(async () => {
    db = await freshDb('ingest_test')
    es = memoryElastic()
    net = await fakeNetwork()
    projector = new Projector({ db, es, index: 'events', plcUrl: net.url, log: () => {} })
    handler = new Handler(db, projector, () => {})
  })
  after(async () => {
    await db.end()
    await net.close()
  })

  it('pairs an event and its sidecar written by another app, in either order', async () => {
    await handler.handle(commit(AUTHOR, SIDECAR, '3ksidecar0001', sidecarValue(AUTHOR, '3kevent000001', geo('37.785', '-122.43')), { cid: CID_B }))
    // The PDS has no event yet and the stream has not delivered it: pending, then an orphan tombstone.
    assert.equal(es.docs.size, 0)
    await handler.handle(commit(AUTHOR, EVENT, '3kevent000001', eventValue({ name: 'Walk' })))
    assert.equal(es.docs.size, 1)
    const doc = es.docs.get(sidecarUri) as { view: { event: { name: string } }; eventCid: string }
    assert.equal(doc.view.event.name, 'Walk')
  })

  it('updates the indexed view when the base event is edited, and ignores an older revision', async () => {
    const newer = nextRev()
    const older = nextRev().replace(/.$/, '2').replace('3m', '3l')
    await handler.handle(commit(AUTHOR, EVENT, '3kevent000001', eventValue({ name: 'Walk, edited' }), { rev: newer }))
    await handler.handle(commit(AUTHOR, EVENT, '3kevent000001', eventValue({ name: 'Walk, stale' }), { rev: older }))
    assert.equal((es.docs.get(sidecarUri) as { view: { event: { name: string } } }).view.event.name, 'Walk, edited')
  })

  it('removes the event from search when the base event is deleted, and restores it when re-created', async () => {
    await handler.handle(commit(AUTHOR, EVENT, '3kevent000001'))
    assert.equal(es.docs.has(sidecarUri), false)
    const state = await db.query('select complete, reason from index_state where uri = $1', [sidecarUri])
    assert.deepEqual(state.rows[0], { complete: false, reason: 'base-deleted' })
    await handler.handle(commit(AUTHOR, EVENT, '3kevent000001', eventValue({ name: 'Walk again' })))
    assert.equal(es.docs.has(sidecarUri), true)
  })

  it('suppresses an inactive account at once and rebuilds it from its PDS when it returns', async () => {
    await handler.handle({ did: AUTHOR, time_us: t++, kind: 'account', account: { did: AUTHOR, active: false, status: 'deactivated' } })
    assert.equal(es.docs.has(sidecarUri), false)
    // While away, the author edited the event; the rebuild picks that up from the PDS listing.
    net.records.set(eventUri, { cid: CID_A, value: eventValue({ name: 'Walk, after return' }) })
    net.records.set(sidecarUri, { cid: CID_B, value: sidecarValue(AUTHOR, '3kevent000001', geo('37.785', '-122.43')) })
    await handler.handle({ did: AUTHOR, time_us: t++, kind: 'account', account: { did: AUTHOR, active: true } })
    assert.equal((es.docs.get(sidecarUri) as { view: { event: { name: string } } }).view.event.name, 'Walk, after return')
  })

  it('fetches a base event that predates the sidecar from the author PDS', async () => {
    const e = `at://${AUTHOR}/${EVENT}/3kevent000002`
    net.records.set(e, { cid: CID_A, value: eventValue({ name: 'Older event' }) })
    await handler.handle(commit(AUTHOR, SIDECAR, '3ksidecar0002', sidecarValue(AUTHOR, '3kevent000002', geo('37.78', '-122.42'))))
    assert.equal((es.docs.get(`at://${AUTHOR}/${SIDECAR}/3ksidecar0002`) as { view: { event: { name: string } } }).view.event.name, 'Older event')
  })

  it('keeps a sidecar pending, not deleted, while the PDS is unreachable', async () => {
    net.down = true
    await handler.handle(commit(AUTHOR, SIDECAR, '3ksidecar0003', sidecarValue(AUTHOR, '3kevent000003', geo('37.78', '-122.42'))))
    net.down = false
    const state = await db.query('select reason from index_state where uri = $1', [`at://${AUTHOR}/${SIDECAR}/3ksidecar0003`])
    assert.equal(state.rows[0].reason, 'pending-base')
    assert.equal(await getRecord(db, `at://${AUTHOR}/${EVENT}/3kevent000003`), undefined)
  })

  it('stores an invalid sidecar with its issues and never indexes it', async () => {
    await handler.handle(commit(AUTHOR, SIDECAR, '3ksidecar0004', { $type: SIDECAR, eventUri: 'not a uri', location: geo('1', '2') }))
    const state = await db.query('select reason, issues from index_state where uri = $1', [`at://${AUTHOR}/${SIDECAR}/3ksidecar0004`])
    assert.equal(state.rows[0].reason, 'invalid-sidecar')
    assert.ok(state.rows[0].issues.length > 0)
  })

  it('backfills an approved account by listing its records', async () => {
    const e = `at://${STEWARD}/${EVENT}/3mxrq3w4c2k2y`
    const s = `at://${STEWARD}/${SIDECAR}/3mxrq3w4c2l2y`
    net.records.set(e, { cid: CID_A, value: eventValue() })
    net.records.set(s, { cid: CID_B, value: sidecarValue(STEWARD, '3mxrq3w4c2k2y', geo('37.7865506', '-122.4308591', 'JCCCNC')) })
    assert.equal(await projector.rebuildAccount(STEWARD), 2)
    assert.ok(es.docs.has(s))
  })

  it('ignores collections it does not index', async () => {
    await handler.handle(commit(AUTHOR, 'app.bsky.feed.post', '3kpost', { $type: 'app.bsky.feed.post', text: 'hi', createdAt: '2026-10-13T00:00:00Z' }))
    assert.equal(await getRecord(db, `at://${AUTHOR}/app.bsky.feed.post/3kpost`), undefined)
  })
})
