import { checkout, type Db } from '../db.js'
import { eventSidecarNsid } from '../contracts/contracts.js'
import type { EventDoc } from '../index/assemble.js'
import type { Projector } from '../ingest/projector.js'
import { readControl } from './control.js'
import type { Issues } from './issues.js'

const COMPARE_EVERY_MS = 60 * 60_000
/**
 * The version of the fill-and-compare rule. A database whose recorded
 * version is lower (PR 1 verified it against today's index, enumerating only
 * index_state) is filled and compared again once at start, keeping its
 * verification while the fill runs, since the fill only writes what stored
 * records assemble to.
 */
export const CHECK_VERSION = 2
const LISTED = 5
/** Held by the ingester running upkeep. */
export const UPKEEP_LOCK = 7140016
/** How long a fill or comparison waits for transactions open when it read the generation. */
export const SETTLE_MS = 30_000

/**
 * The ingester's search upkeep for PR 1 (spec 4.2, 5.2, 5.4), run at start-up
 * and every 10 minutes, on one ingester at a time. Writes may land while it
 * runs (this ingester's events wait, another's do not): each projection
 * reads stored records under the write lock, and a write from code that does
 * not keep event_docs moves the generation, which voids the run. Closed rows
 * past 61 minutes are deleted; the first fill runs until it finishes at an
 * unmoved generation; then the comparison runs right after it. A comparison
 * that finds a difference voids the fill, so the next upkeep fills again
 * (which repairs it) and compares; one that could not run is retried hourly.
 * Verification holds until a comparison finds a difference or the generation
 * moves.
 */
export class SearchUpkeep {
  settleMs = SETTLE_MS
  /**
   * Where the fill and the comparison record their result: during run(), the
   * connection holding the upkeep lock, so a run whose lock session has
   * ended (and another ingester may now run upkeep) can record nothing.
   */
  private publisher?: Db

  constructor(
    private db: Db,
    private projector: Projector,
    private log: (msg: string) => void = console.log,
    private now: () => number = Date.now,
    /** From PR 2 each failed comparison opens an issue (spec 5.2). */
    private issues?: Issues,
  ) {}

  /**
   * One upkeep at a time across ingesters (two run while a deploy overlaps):
   * a session lock on a dedicated connection; an ingester that does not get
   * it skips this run.
   */
  async run(): Promise<void> {
    let client
    try {
      client = await checkout(this.db)
    } catch (err) {
      this.log(`search upkeep: ${(err as Error).message}`)
      return
    }
    // A connection that may still hold the lock is destroyed, not pooled,
    // which ends its session and so releases the lock.
    let holds = false
    try {
      holds = (await client.query<{ ok: boolean }>('select pg_try_advisory_lock($1) as ok', [UPKEEP_LOCK])).rows[0].ok
      if (!holds) return
      try {
        this.publisher = client as unknown as Db
        await this.runLocked()
      } finally {
        this.publisher = undefined
        holds = !(await client.query<{ ok: boolean }>('select pg_advisory_unlock($1) as ok', [UPKEEP_LOCK])).rows[0].ok
      }
    } catch (err) {
      holds = true
      this.log(`search upkeep: ${(err as Error).message}`)
    } finally {
      client.release(holds ? new Error('upkeep lock not confirmed released') : undefined)
    }
  }

  private async runLocked(): Promise<void> {
    await this.projector.postgres.pruneClosed().catch((err) => this.log(`pruning closed search rows failed: ${(err as Error).message}`))
    let row
    try {
      row = await readControl(this.db)
    } catch (err) {
      this.log(`search upkeep: ${(err as Error).message}`)
      return
    }
    if (!row.firstFillDone) {
      try {
        const n = await this.firstFill()
        this.log(`search: first fill done, ${n} sidecars projected into Postgres`)
      } catch (err) {
        this.log(`search: first fill failed, retrying in 10 minutes: ${(err as Error).message}`)
        return
      }
      // The fill cleared verification and the last comparison: compare now,
      // not by the row read before the fill.
    } else if (row.checkedBy < CHECK_VERSION) {
      try {
        const n = await this.fill()
        this.log(`search: filled ${n} sidecars again for the version ${CHECK_VERSION} check`)
      } catch (err) {
        this.log(`search: the version ${CHECK_VERSION} fill failed, retrying in 10 minutes: ${(err as Error).message}`)
        return
      }
    } else {
      if (row.pgVerified) return
      if (row.comparedAt && this.now() - row.comparedAt.getTime() < COMPARE_EVERY_MS) return
    }
    const result = await this.compare()
    this.log(`search: comparison with stored records ${result.ok ? 'passed; Postgres is verified' : `did not pass: ${result.note}`}`)
  }

  /**
   * Projects every sidecar into Postgres, then records that the first fill is
   * done, unless code that does not write event_docs changed an ingestion
   * table meanwhile (the generation moved): then it runs again. The sidecars
   * are every one with a stored record, in index_state, or in event_docs, so
   * a sidecar whose index_state row was never written is not missed. It reads
   * nothing until every write that moved the generation to the value it read
   * is visible (settle). The comparison needs no such wait: it publishes only
   * at the fill's generation, unmoved.
   */
  async firstFill(): Promise<number> {
    const { generation } = await readControl(this.db)
    await this.settle()
    const n = await this.fill()
    const res = await (this.publisher ?? this.db).query(
      `update search_control set first_fill_done = true, fill_generation = $1, pg_verified = false, compared_at = null, note = null, updated_at = now()
       where id = 1 and search_generation_now() = $1`,
      [generation],
    )
    if (res.rowCount === 0) throw new Error('an ingestion table changed during the fill')
    return n
  }

  /** Projects every sidecar, its event_docs row and its index_state row, from stored records. */
  private async fill(): Promise<number> {
    const uris = await expectedSidecars(this.db)
    for (const uri of uris) await this.projector.fillSidecar(uri)
    return uris.length
  }

  /**
   * Compares Postgres's current documents by document id and content (not
   * versions, and not indexedAt) with every sidecar assembled again from the
   * stored records, both read in one snapshot. From PR 2 today's index is not
   * written, so it is no longer the copy. The sidecars are enumerated from
   * records, index_state and event_docs together, and index_state's
   * suppression is checked too. When anything differs the whole comparison
   * runs again in a fresh snapshot, and only that second run counts, so a
   * write that landed between the tables does not fail the check and a
   * partial re-read cannot pass it. The result is recorded
   * only if the fill is done and the generation has not moved since the
   * comparison began: a pass sets pg_verified and checked_by; a comparison
   * that finds a difference clears pg_verified and first_fill_done; one that
   * could not run changes nothing. The note
   * (public in health and in the issue) has counts only; the uris go to the
   * ingester log.
   */
  async compare(): Promise<{ ok: boolean; note: string }> {
    let ok = false
    let ran = false
    let note: string
    const { generation } = await readControl(this.db)
    try {
      // A difference may be a write landing between tables mid-read: the
      // whole comparison runs once more in a fresh snapshot, and only a clean
      // second run passes.
      let found = await this.differences()
      if (found.total > 0) found = await this.differences()
      ran = true
      ok = found.total === 0
      note = ok
        ? `matched stored records: ${found.size} documents`
        : [
            found.missing.length && `${found.missing.length} only in stored records`,
            found.extra.length && `${found.extra.length} only in Postgres`,
            found.differ.length && `${found.differ.length} with different content`,
            found.state.length && `${found.state.length} with a stale suppression state`,
          ]
            .filter(Boolean)
            .join('; ')
      if (!ok) {
        this.log(
          `search: comparison differences: only in stored records ${found.missing.slice(0, LISTED).join(', ')}; ` +
            `only in Postgres ${found.extra.slice(0, LISTED).join(', ')}; different ${found.differ.slice(0, LISTED).join(', ')}; ` +
            `stale state ${found.state.slice(0, LISTED).join(', ')}`,
        )
      }
    } catch (err) {
      // Health is public, so the note says what happened, not the error text.
      this.log(`search: comparison could not run: ${(err as Error).message}`)
      note = 'comparison could not run: Postgres did not answer; retrying at the next upkeep'
    }
    const res = await (this.publisher ?? this.db).query(
      `update search_control
         set pg_verified = case when $4 then $1 else pg_verified end,
             compared_at = case when $4 then now() else compared_at end, note = $2, updated_at = now(),
             checked_by = case when $4 and $1 then greatest(checked_by, ${CHECK_VERSION}) else checked_by end,
             -- A difference voids the fill: the next upkeep fills again, which repairs it.
             first_fill_done = case when $4 and not $1 then false else first_fill_done end
       where id = 1 and first_fill_done and fill_generation = $3 and search_generation_now() = $3`,
      [ok, note, generation, ran],
    )
    if (res.rowCount === 0) return { ok: false, note: 'an ingestion table changed during the comparison; filling again' }
    if (ran && !ok) {
      await this.issues
        ?.open('comparison', 'Search: the Postgres comparison did not pass', `Postgres is not verified, so search answers 503; the next upkeep (within 10 minutes) fills from stored records again and compares. ${note}.`)
        .catch((err) => this.log(`search: could not open the comparison issue: ${(err as Error).message}`))
    }
    return { ok, note }
  }

  /**
   * Postgres's documents and suppression state against stored records, in
   * one read-only snapshot. A sidecar with a stored record needs an
   * index_state row whose `complete` matches its assembly; one without needs
   * none, or one that suppresses.
   */
  private async differences() {
    const client = await checkout(this.db)
    try {
      await client.query('begin isolation level repeatable read read only')
      const q = client as unknown as Db
      const rows = (
        await q.query<{ uri: string; doc: EventDoc }>(
          `select uri, doc from event_docs where closed_version is null`,
        )
      ).rows
      const pg = new Map(rows.map((r) => [r.uri, r.doc]))
      const states = new Map(
        (await q.query<{ uri: string; complete: boolean }>('select uri, complete from index_state')).rows.map((r) => [r.uri, r.complete]),
      )
      const recorded = new Set(
        (await q.query<{ uri: string }>('select uri from records where collection = $1', [eventSidecarNsid])).rows.map((r) => r.uri),
      )
      const stored = new Map<string, EventDoc>()
      const state: string[] = []
      for (const uri of await expectedSidecars(q)) {
        const doc = await this.projector.assembled(uri, q)
        if (doc) stored.set(uri, doc)
        const s = states.get(uri)
        if (recorded.has(uri) ? s !== !!doc : s === true) state.push(uri)
      }
      await client.query('commit')
      const missing = [...stored.keys()].filter((uri) => !pg.has(uri))
      const extra = [...pg.keys()].filter((uri) => !stored.has(uri))
      const differ = [...pg.keys()].filter((uri) => stored.has(uri) && comparable(pg.get(uri)!) !== comparable(stored.get(uri)!))
      return { size: pg.size, missing, extra, differ, state, total: missing.length + extra.length + differ.length + state.length }
    } catch (err) {
      await client.query('rollback').catch(() => {})
      throw err
    } finally {
      client.release()
    }
  }

  /**
   * Waits until every transaction open just after the generation was read
   * has ended. The generation moves when old code writes, but its rows become
   * visible only when it commits, so a run that read them sooner could count
   * an old write as seen. After this wait, every write that moved the
   * generation up to the value read is visible, and any later one moves it
   * again, which voids the run. A transaction that stays open longer than
   * settleMs (any transaction in the cluster that holds an id) fails the run,
   * which retries at the next upkeep.
   */
  private async settle(): Promise<void> {
    // A fresh transaction id is newer than every transaction holding one now
    // (a snapshot's xmax is not: it can equal a running id).
    const { mark } = (await this.db.query<{ mark: string }>('select pg_current_xact_id()::text as mark')).rows[0]
    const until = Date.now() + this.settleMs
    for (;;) {
      const { done } = (await this.db.query<{ done: boolean }>('select pg_snapshot_xmin(pg_current_snapshot()) > $1::xid8 as done', [mark])).rows[0]
      if (done) return
      if (Date.now() >= until) throw new Error(`a transaction open when the generation was read did not end within ${this.settleMs} ms`)
      await new Promise((r) => setTimeout(r, 100))
    }
  }
}

/** A document as compared: indexedAt left out, keys in a fixed order. */
export function comparable(doc: EventDoc): string {
  const { indexedAt: _indexedAt, ...rest } = doc
  return canonical(rest)
}

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`
  if (value && typeof value === 'object') {
    const obj = value as Record<string, unknown>
    return `{${Object.keys(obj)
      .sort()
      .filter((k) => obj[k] !== undefined)
      .map((k) => `${JSON.stringify(k)}:${canonical(obj[k])}`)
      .join(',')}}`
  }
  return JSON.stringify(value)
}

/** Every sidecar Postgres should account for: stored as a record, in index_state, or current in event_docs. */
export async function expectedSidecars(db: Db): Promise<string[]> {
  const res = await db.query<{ uri: string }>(
    `select uri collate "C" as uri from records where collection = $1
     union select uri collate "C" from index_state
     union select uri collate "C" from event_docs where closed_version is null
     order by 1`,
    [eventSidecarNsid],
  )
  return res.rows.map((r) => r.uri)
}
