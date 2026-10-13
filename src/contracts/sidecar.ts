import { AtUri, isValidDid, isValidRecordKey } from '@atproto/syntax'
import { config, eventSidecarNsid, lexicons } from './contracts.js'
import { checkLocation } from './location.js'

export interface EventSidecar {
  $type: string
  eventUri: string
  location: { $type: string; [key: string]: unknown }
  [key: string]: unknown
}

export type SidecarValidation =
  | { ok: true; value: EventSidecar }
  | { ok: false; error: 'InvalidRequest'; issues: string[] }

/**
 * Validates a PlaceProto event sidecar as a writer must (PlaceProto section 2):
 * the record schema, then the association (eventUri targets the community
 * event collection in the same repository, by DID), then the Location checks.
 * `repo` is the DID of the repository the sidecar is stored in. Any failure is
 * 400 InvalidRequest; indexers quarantine such records and never repair them.
 */
export function validateEventSidecar(record: unknown, opts: { repo: string }): SidecarValidation {
  const issues: string[] = []
  try {
    lexicons.assertValidRecord(eventSidecarNsid, record)
  } catch (err) {
    issues.push((err as Error).message)
    return { ok: false, error: 'InvalidRequest', issues }
  }
  const sidecar = record as EventSidecar
  issues.push(...checkEventUri(sidecar.eventUri, opts.repo))
  issues.push(...checkLocation(sidecar.location))
  return issues.length ? { ok: false, error: 'InvalidRequest', issues } : { ok: true, value: sidecar }
}

export function checkEventUri(eventUri: string, repo: string): string[] {
  let uri: AtUri
  try {
    uri = new AtUri(eventUri)
  } catch {
    return ['eventUri is not a valid AT-URI']
  }
  const issues: string[] = []
  if (!isValidDid(uri.host)) issues.push('eventUri must use the repository DID, not a handle')
  else if (uri.host !== repo) issues.push('eventUri must point into the same repository as the sidecar')
  if (uri.collection !== config.communityEventCollection)
    issues.push(`eventUri must target the ${config.communityEventCollection} collection`)
  if (!uri.rkey || !isValidRecordKey(uri.rkey)) issues.push('eventUri must include a valid record key')
  if (uri.hash || uri.search) issues.push('eventUri must not carry a query or fragment')
  return issues
}
