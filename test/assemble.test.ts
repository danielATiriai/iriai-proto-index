import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { latLngToCell } from 'h3-js'
import { assemble } from '../src/index/assemble.js'
import { CID_B, EVENT, OTHER, SIDECAR, STEWARD, eventValue, geo, row, sidecarValue } from './fixtures.js'

const jcccnc = geo('37.7865506', '-122.4308591', 'JCCCNC')
const event = row(STEWARD, EVENT, '3mxrq3w4c2k2y', eventValue())
const sidecar = row(STEWARD, SIDECAR, '3mxrq3w4c2l2y', sidecarValue(STEWARD, '3mxrq3w4c2k2y', jcccnc), CID_B)

describe('assembling complete events', () => {
  it('indexes a valid pair with its point, times and both CIDs', () => {
    const r = assemble(sidecar, event, true, new Date('2026-10-13T20:00:00Z'))
    assert.ok(r.complete)
    assert.deepEqual(r.doc.point, { lat: 37.7865506, lon: -122.4308591 })
    assert.equal(r.doc.uri, sidecar.uri)
    assert.equal(r.doc.eventUri, event.uri)
    assert.equal(r.doc.eventCid, event.cid)
    assert.equal(r.doc.sidecarCid, CID_B)
    assert.equal(r.doc.startsAt, '2026-10-16T01:30:00.000Z')
    assert.equal(r.doc.timeValid, true)
    assert.deepEqual(r.doc.view, { uri: sidecar.uri, eventUri: event.uri, event: event.value, location: jcccnc })
  })

  it('uses the H3 cell centre for an hthree location', () => {
    const cell = latLngToCell(37.785054, -122.429827, 9)
    const s = row(STEWARD, SIDECAR, '3aaa', sidecarValue(STEWARD, '3mxrq3w4c2k2y', { $type: 'community.lexicon.location.hthree', value: cell }))
    const r = assemble(s, event, true)
    assert.ok(r.complete && r.doc.point && r.doc.resolution.status === 'resolved' && r.doc.resolution.via === 'hthree')
  })

  it('keeps an address location complete but unresolved, with no invented point', () => {
    const address = { $type: 'community.lexicon.location.address', country: 'US', locality: 'San Francisco', street: '1840 Sutter St' }
    const r = assemble(row(STEWARD, SIDECAR, '3aab', sidecarValue(STEWARD, '3mxrq3w4c2k2y', address)), event, true)
    assert.ok(r.complete)
    assert.equal(r.doc.point, undefined)
    assert.equal(r.doc.resolved, false)
  })

  it('leaves out deleted, invalid, cross-repository and inactive-account sidecars', () => {
    assert.deepEqual(assemble(undefined, event, true), { complete: false, reason: 'sidecar-missing' })
    assert.equal(assemble({ ...sidecar, deleted: true, value: null }, event, true).complete, false)
    assert.deepEqual(assemble(sidecar, event, false), { complete: false, reason: 'account-inactive' })
    const cross = row(OTHER, SIDECAR, '3aac', sidecarValue(STEWARD, '3mxrq3w4c2k2y', jcccnc))
    const r = assemble(cross, event, true)
    assert.ok(!r.complete && r.reason === 'invalid-sidecar' && r.issues?.some((i) => /same repository/.test(i)))
    const badGeo = row(STEWARD, SIDECAR, '3aad', sidecarValue(STEWARD, '3mxrq3w4c2k2y', geo('91', '0')))
    assert.equal((assemble(badGeo, event, true) as { reason: string }).reason, 'invalid-sidecar')
  })

  it('is pending, and asks for the base event, when the event has not been seen', () => {
    assert.deepEqual(assemble(sidecar, undefined, true), { complete: false, reason: 'pending-base', hydrate: event.uri })
  })

  it('is an orphan when the base event is deleted, and left out when it is invalid', () => {
    assert.equal((assemble(sidecar, { ...event, deleted: true, value: null }, true) as { reason: string }).reason, 'base-deleted')
    const invalid = row(STEWARD, EVENT, '3mxrq3w4c2k2y', eventValue({ name: 42 }))
    assert.equal((assemble(sidecar, invalid, true) as { reason: string }).reason, 'invalid-base')
  })

  it('marks an event that ends before it starts so time filters skip it', () => {
    const backwards = row(STEWARD, EVENT, '3mxrq3w4c2k2y', eventValue({ endsAt: '2026-10-15T00:00:00.000Z' }))
    const r = assemble(sidecar, backwards, true)
    assert.ok(r.complete && r.doc.timeValid === false)
  })
})
