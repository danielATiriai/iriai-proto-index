import { AtUri } from '@atproto/syntax'
import type { Db } from '../db.js'
import { config, eventSidecarNsid } from '../contracts/contracts.js'
import { assemble, type EventDoc } from '../index/assemble.js'
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
  plcUrl: string
  log?: (msg: string) => void
}

/**
 * Keeps the index in step with stored records: after any record changes, the
 * sidecars it affects are re-assembled and either indexed as complete events
 * or removed, with the outcome kept in index_state for suppression and health.
 * From PR 2 each change goes to Postgres only: Elasticsearch is not written
 * (spec 5.1).
 */
export class Projector {
  readonly postgres: PostgresBackend

  constructor(private deps: ProjectorDeps) {
    this.postgres = new PostgresBackend(deps.db)
  }

  /** Re-assembles everything a changed record affects. */
  async recordChanged(uri: string): Promise<void> {
    const collection = new AtUri(uri).collection
    if (collection === eventSidecarNsid) await this.projectSidecars([uri])
    else if (collection === config.communityEventCollection) {
      await this.projectSidecars(await sidecarsFor(this.deps.db, uri))
    }
  }

  async projectAccount(did: string): Promise<void> {
    await this.projectSidecars(await sidecarsOfAccount(this.deps.db, did))
  }

  async projectSidecar(uri: string, { hydrate = true } = {}): Promise<void> {
    await this.projectSidecars([uri], { hydrate })
  }

  /** Postgres and index_state for every sidecar, then any hydration. */
  async projectSidecars(uris: string[], { hydrate = true } = {}): Promise<void> {
    const { db } = this.deps
    const done: { uri: string; doc?: EventDoc; hydrate?: string }[] = []
    for (const uri of uris) {
      const { result, did } = await this.projectPostgres(uri)
      if (result.complete) {
        await setIndexState(db, uri, did, true, null)
        done.push({ uri, doc: result.doc })
      } else {
        await setIndexState(db, uri, did, false, result.reason, result.issues ?? null)
        done.push({ uri, hydrate: result.hydrate })
      }
    }
    for (const d of done) {
      if (d.hydrate && hydrate && (await this.hydrate(d.hydrate))) await this.projectSidecars([d.uri], { hydrate: false })
    }
  }

  /**
   * Writes one sidecar's current state, its event_docs row and its
   * index_state row, from stored records and with no fetch: the fill (spec
   * 5.4). A sidecar still waiting for its base event is left pending, and the
   * ingester's retry hydrates it.
   */
  async fillSidecar(uri: string): Promise<void> {
    await this.projectSidecars([uri], { hydrate: false })
  }

  /** Assembles uri from stored records and writes it to Postgres, as one step under the write lock. */
  private async projectPostgres(uri: string) {
    return this.postgres.project(uri, async (q) => {
      const assembled = await this.assembleStored(uri, q)
      return { ...assembled, doc: assembled.result.complete ? assembled.result.doc : undefined }
    })
  }

  /**
   * One sidecar as stored records assemble it now, through `db` (a snapshot
   * for the comparison), or undefined when incomplete. Writes nothing.
   */
  async assembled(uri: string, db: Db = this.deps.db): Promise<EventDoc | undefined> {
    const { result } = await this.assembleStored(uri, db)
    return result.complete ? result.doc : undefined
  }

  private async assembleStored(uri: string, db: Db = this.deps.db) {
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
