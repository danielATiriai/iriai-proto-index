import { AtUri } from '@atproto/syntax'
import type { Db } from '../db.js'
import { config, eventSidecarNsid } from '../contracts/contracts.js'
import { assemble } from '../index/assemble.js'
import { putDoc, removeDoc, type Elastic } from '../index/elastic.js'
import { NotFound, fetchRecord, listRecords, resolvePds } from './pds.js'
import {
  applyChange,
  getRecord,
  isAccountActive,
  replaceAccountRecords,
  setIndexState,
  sidecarsFor,
  sidecarsOfAccount,
} from './store.js'

export const COLLECTIONS = [config.communityEventCollection, eventSidecarNsid]

export interface ProjectorDeps {
  db: Db
  es: Elastic
  index: string
  plcUrl: string
  log?: (msg: string) => void
}

/**
 * Keeps the index in step with stored records: after any record changes, the
 * sidecars it affects are re-assembled and either indexed as complete events
 * or removed, with the outcome kept in index_state for suppression and health.
 */
export class Projector {
  constructor(private deps: ProjectorDeps) {}

  /** Re-assembles everything a changed record affects. */
  async recordChanged(uri: string): Promise<void> {
    const collection = new AtUri(uri).collection
    if (collection === eventSidecarNsid) await this.projectSidecar(uri)
    else if (collection === config.communityEventCollection) {
      for (const sidecar of await sidecarsFor(this.deps.db, uri)) await this.projectSidecar(sidecar)
    }
  }

  async projectAccount(did: string): Promise<void> {
    for (const sidecar of await sidecarsOfAccount(this.deps.db, did)) await this.projectSidecar(sidecar)
  }

  async projectSidecar(uri: string, { hydrate = true } = {}): Promise<void> {
    const { db, es, index } = this.deps
    const sidecar = await getRecord(db, uri)
    const event = sidecar?.event_uri ? await getRecord(db, sidecar.event_uri) : undefined
    const active = sidecar ? await isAccountActive(db, sidecar.did) : true
    const result = assemble(sidecar, event, active)
    const did = sidecar?.did ?? new AtUri(uri).host

    if (result.complete) {
      await putDoc(es, index, result.doc)
      await setIndexState(db, uri, did, true, null)
      return
    }
    await removeDoc(es, index, uri)
    await setIndexState(db, uri, did, false, result.reason, result.issues ?? null)
    if (result.hydrate && hydrate && (await this.hydrate(result.hydrate))) {
      await this.projectSidecar(uri, { hydrate: false })
    }
  }

  /**
   * Fetches a base event the stream has not delivered (it predates the
   * sidecar, or was written before ingestion started). Returns true when the
   * event was stored, or confirmed absent: the PDS saying it does not exist
   * stores a tombstone, so the sidecar is an orphan until the event appears.
   * A timeout or PDS error leaves the sidecar pending for a retry, and is
   * never treated as a deletion.
   */
  async hydrate(eventUri: string): Promise<boolean> {
    const at = new AtUri(eventUri)
    try {
      const pds = await resolvePds(this.deps.db, this.deps.plcUrl, at.host)
      const record = await fetchRecord(pds, at.host, at.collection, at.rkey)
      return applyChange(this.deps.db, { did: at.host, collection: at.collection, rkey: at.rkey, cid: record.cid, value: record.value })
    } catch (err) {
      if (err instanceof NotFound) {
        return applyChange(this.deps.db, { did: at.host, collection: at.collection, rkey: at.rkey })
      }
      this.deps.log?.(`hydrate ${eventUri}: ${(err as Error).message}`)
      return false
    }
  }

  /** Lists an account's PlaceProto records from its PDS and rebuilds its index entries. */
  async rebuildAccount(did: string): Promise<number> {
    const pds = await resolvePds(this.deps.db, this.deps.plcUrl, did)
    const listed = []
    for (const collection of COLLECTIONS) {
      for await (const record of listRecords(pds, did, collection)) {
        listed.push({ collection, rkey: new AtUri(record.uri).rkey, cid: record.cid, value: record.value })
      }
    }
    await replaceAccountRecords(this.deps.db, did, COLLECTIONS, listed)
    await this.projectAccount(did)
    return listed.length
  }
}
