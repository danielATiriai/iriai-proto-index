import { AtUri } from '@atproto/syntax'
import type { Db } from '../db.js'
import { config, eventSidecarNsid } from '../contracts/contracts.js'
import { assemble } from '../index/assemble.js'
import { ElasticBackend, type Elastic } from '../index/elastic.js'
import { PostgresBackend } from '../search/postgres.js'
import { NotFound, fetchRecord, listRecords, resolvePds } from './pds.js'
import {
  applyChange,
  getRecord,
  isAccountServable,
  markRebuilt,
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
 * Each change goes to Postgres first and then to today's Elasticsearch index
 * exactly as before; an Elasticsearch failure fails the event, which is
 * replayed (spec 5.1, PR 1).
 */
export class Projector {
  readonly postgres: PostgresBackend
  readonly elastic: ElasticBackend

  constructor(private deps: ProjectorDeps) {
    this.postgres = new PostgresBackend(deps.db)
    this.elastic = new ElasticBackend(deps.es, deps.index)
  }

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
    const { db } = this.deps
    const { result, did } = await this.assembleStored(uri)

    if (result.complete) {
      await this.postgres.put(result.doc)
      await this.elastic.put(result.doc)
      await setIndexState(db, uri, did, true, null)
      return
    }
    await this.postgres.remove(uri)
    await this.elastic.remove(uri)
    await setIndexState(db, uri, did, false, result.reason, result.issues ?? null)
    if (result.hydrate && hydrate && (await this.hydrate(result.hydrate))) {
      await this.projectSidecar(uri, { hydrate: false })
    }
  }

  /**
   * Writes one sidecar's current state to Postgres only, from stored records
   * and with no fetch: the first fill (spec 5.4). Today's index already holds
   * the same state, and index_state is left as it is.
   */
  async fillPostgres(uri: string): Promise<void> {
    const { result } = await this.assembleStored(uri)
    if (result.complete) await this.postgres.put(result.doc)
    else await this.postgres.remove(uri)
  }

  private async assembleStored(uri: string) {
    const { db } = this.deps
    const sidecar = await getRecord(db, uri)
    const event = sidecar?.event_uri ? await getRecord(db, sidecar.event_uri) : undefined
    const servable = sidecar ? await isAccountServable(db, sidecar.did) : true
    return { result: assemble(sidecar, event, servable), did: sidecar?.did ?? new AtUri(uri).host }
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

  /**
   * Lists an account's PlaceProto records from its PDS, replaces the stored
   * ones, clears any rebuild the account owed, and rebuilds its index entries.
   * Throws, changing nothing, when the PDS cannot be listed.
   */
  async rebuildAccount(did: string): Promise<number> {
    const pds = await resolvePds(this.deps.db, this.deps.plcUrl, did)
    const listed = []
    for (const collection of COLLECTIONS) {
      for await (const record of listRecords(pds, did, collection)) {
        listed.push({ collection, rkey: new AtUri(record.uri).rkey, cid: record.cid, value: record.value })
      }
    }
    await replaceAccountRecords(this.deps.db, did, COLLECTIONS, listed)
    await markRebuilt(this.deps.db, did)
    await this.projectAccount(did)
    return listed.length
  }
}
