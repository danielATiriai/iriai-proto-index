import type { RecordRow } from '../src/ingest/store.js'

export const STEWARD = 'did:plc:kd5s7zyp7nje43ievnqhgzrr'
export const OTHER = 'did:plc:abcdefghijklmnopqrstuvwx'
export const EVENT = 'community.lexicon.calendar.event'
export const SIDECAR = 'app.iriai.proto.event'

/** A community event like the steward's seeds (gtm/sample-event-record.md). */
export function eventValue(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    $type: EVENT,
    name: 'Kabuki Theatre Appreciation Class with JETAANC',
    createdAt: '2026-10-13T19:06:40.937Z',
    startsAt: '2026-10-16T01:30:00.000Z',
    endsAt: '2026-10-16T03:30:00.000Z',
    mode: `${EVENT}#inperson`,
    status: `${EVENT}#scheduled`,
    ...over,
  }
}

export function geo(lat: string, lon: string, name?: string): Record<string, unknown> {
  return { $type: 'community.lexicon.location.geo', latitude: lat, longitude: lon, ...(name ? { name } : {}) }
}

export function sidecarValue(did: string, eventRkey: string, location: Record<string, unknown>): Record<string, unknown> {
  return { $type: SIDECAR, eventUri: `at://${did}/${EVENT}/${eventRkey}`, location }
}

export const CID_A = 'bafyreibjytuinufvu3gimfxyqjbptuws7g3sdekx2igq5dts4ar7jgxjbq'
export const CID_B = 'bafyreiboelgtqvzt67s2qsh3yuuyemchj6yzzukd5hmlkny4xf5m3x74tu'

export function row(did: string, collection: string, rkey: string, value: Record<string, unknown> | null, cid = CID_A): RecordRow {
  return {
    uri: `at://${did}/${collection}/${rkey}`,
    did,
    collection,
    rkey,
    cid: value ? cid : null,
    rev: null,
    value,
    deleted: value === null,
    event_uri: collection === SIDECAR && value ? (value.eventUri as string) : null,
  }
}
