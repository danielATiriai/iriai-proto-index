import { config, lexicons } from '../contracts/contracts.js'
import { resolveLocation, type ResolvedLocation } from '../contracts/location.js'
import { validateEventSidecar } from '../contracts/sidecar.js'
import type { RecordRow } from '../ingest/store.js'

/** One complete PlaceProto event, as indexed: one document per sidecar (PlaceProto §4). */
export interface EventDoc {
  uri: string
  eventUri: string
  did: string
  sidecarCid: string
  eventCid: string
  /** The complete view, returned as stored (app.iriai.proto.defs#eventView, without version). */
  view: { uri: string; eventUri: string; event: Record<string, unknown>; location: Record<string, unknown> }
  point?: { lat: number; lon: number }
  resolved: boolean
  resolution: ResolvedLocation
  startsAt?: string
  endsAt?: string
  /** False when an externally written event ends before it starts; such events skip time-filtered queries. */
  timeValid: boolean
  indexedAt: string
}

export type Assembly =
  | { complete: true; doc: EventDoc }
  | { complete: false; reason: Incomplete; issues?: string[]; hydrate?: string }

export type Incomplete =
  | 'sidecar-missing'
  | 'sidecar-deleted'
  | 'invalid-sidecar'
  | 'account-inactive'
  | 'pending-base'
  | 'base-deleted'
  | 'invalid-base'

/**
 * Decides whether a sidecar makes a complete PlaceProto event and builds its
 * index document. Authored records are never repaired: an invalid sidecar or
 * base event is left out, a sidecar whose base event has not been seen yet is
 * pending (and its base is fetched), and derived coordinates only ever go in
 * the index, never back into a record.
 */
export function assemble(
  sidecar: RecordRow | undefined,
  event: RecordRow | undefined,
  accountActive: boolean,
  now = new Date(),
): Assembly {
  if (!sidecar) return { complete: false, reason: 'sidecar-missing' }
  if (sidecar.deleted || !sidecar.value || !sidecar.cid) return { complete: false, reason: 'sidecar-deleted' }
  if (!accountActive) return { complete: false, reason: 'account-inactive' }

  const checked = validateEventSidecar(sidecar.value, { repo: sidecar.did })
  if (!checked.ok) return { complete: false, reason: 'invalid-sidecar', issues: checked.issues }
  const eventUri = checked.value.eventUri

  if (!event) return { complete: false, reason: 'pending-base', hydrate: eventUri }
  if (event.deleted || !event.value || !event.cid) return { complete: false, reason: 'base-deleted' }
  try {
    lexicons.assertValidRecord(config.communityEventCollection, event.value)
  } catch (err) {
    return { complete: false, reason: 'invalid-base', issues: [(err as Error).message] }
  }

  const location = checked.value.location
  const resolution = resolveLocation(location)
  const startsAt = isoOrUndefined(event.value.startsAt)
  const endsAt = isoOrUndefined(event.value.endsAt)
  return {
    complete: true,
    doc: {
      uri: sidecar.uri,
      eventUri,
      did: sidecar.did,
      sidecarCid: sidecar.cid,
      eventCid: event.cid,
      view: { uri: sidecar.uri, eventUri, event: event.value, location },
      ...(resolution.status === 'resolved' ? { point: { lat: resolution.latitude, lon: resolution.longitude } } : {}),
      resolved: resolution.status === 'resolved',
      resolution,
      ...(startsAt ? { startsAt } : {}),
      ...(endsAt ? { endsAt } : {}),
      timeValid: !(startsAt && endsAt && endsAt <= startsAt),
      indexedAt: now.toISOString(),
    },
  }
}

function isoOrUndefined(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? undefined : date.toISOString()
}
