import type { Db } from '../db.js'
import { COLLECTIONS, type Projector } from './projector.js'
import { forgetPds } from './pds.js'
import { applyChange, needsRebuild, recordUri, setAccountStatus } from './store.js'

/** The Jetstream event shapes this service reads (commit, identity, account). */
export type JetstreamEvent =
  | {
      did: string
      time_us: number
      kind: 'commit'
      commit: {
        rev: string
        operation: 'create' | 'update' | 'delete'
        collection: string
        rkey: string
        record?: Record<string, unknown>
        cid?: string
      }
    }
  | { did: string; time_us: number; kind: 'identity'; identity: { did: string; handle?: string } }
  | { did: string; time_us: number; kind: 'account'; account: { did: string; active: boolean; status?: string } }

/**
 * Handles one upstream event durably before the caller may checkpoint past it.
 * Commits to the two PlaceProto collections are applied in revision order and
 * re-project what they affect. Account and identity events matter only for
 * accounts that hold PlaceProto records: an inactive account's events leave
 * search at once, and a returning account's events come back only once it
 * has been rebuilt from its PDS.
 */
export class Handler {
  /** Accounts holding PlaceProto records; account and identity events for others are skipped. */
  private known = new Set<string>()

  constructor(
    private db: Db,
    private projector: Projector,
    private log: (msg: string) => void = console.log,
  ) {}

  remember(dids: Iterable<string>): void {
    for (const did of dids) this.known.add(did)
  }

  async handle(evt: JetstreamEvent): Promise<void> {
    switch (evt.kind) {
      case 'commit': {
        const c = evt.commit
        if (!COLLECTIONS.includes(c.collection)) return
        const isDelete = c.operation === 'delete'
        if (!isDelete && (!c.record || !c.cid)) return
        const applied = await applyChange(this.db, {
          did: evt.did,
          collection: c.collection,
          rkey: c.rkey,
          rev: c.rev,
          ...(isDelete ? {} : { cid: c.cid, value: c.record }),
        })
        this.known.add(evt.did)
        if (applied) await this.projector.recordChanged(recordUri(evt.did, c.collection, c.rkey))
        return
      }
      case 'account': {
        // Handled in full every time, so a replay redoes whatever failed.
        const a = evt.account
        if (!this.known.has(a.did)) return
        const changed = await setAccountStatus(this.db, a.did, a.active, a.status ?? null)
        if (a.active && (await needsRebuild(this.db, a.did))) {
          this.log(`account ${a.did} is active again; rebuilding before serving its events`)
          try {
            await this.projector.rebuildAccount(a.did)
            return
          } catch (err) {
            // Its events stay suppressed; the ingester retries the rebuild.
            this.log(`rebuild ${a.did} failed, its events stay suppressed until a retry succeeds: ${(err as Error).message}`)
          }
        } else if (changed && !a.active) {
          this.log(`account ${a.did} is ${a.status ?? 'inactive'}; suppressing its events`)
        }
        await this.projector.projectAccount(a.did)
        return
      }
      case 'identity': {
        if (this.known.has(evt.identity.did)) await forgetPds(this.db, evt.identity.did, evt.identity.handle)
        return
      }
    }
  }
}
