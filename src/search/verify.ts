import { checkout, type Db } from '../db.js'
import type { EventDoc } from '../index/assemble.js'
import type { Projector } from '../ingest/projector.js'
import { readControl } from './control.js'
import type { Issues } from './issues.js'

const COMPARE_EVERY_MS = 60 * 60_000
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
 * unmoved generation; then the comparison runs right after it and hourly
 * until it passes. Verification holds until a comparison finds a difference
 * or the generation moves.
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
    }
    if (row.pgVerified) return
    if (row.comparedAt && this.now() - row.comparedAt.getTime() < COMPARE_EVERY_MS) return
    const result = await this.compare()
    this.log(`search: comparison ${result.ok ? 'passed; Postgres is verified' : `did not pass: ${result.note}`}`)
  }

  /**
   * Projects every sidecar in index_state into Postgres, then records that
   * the first fill is done, unless code that does not write event_docs
   * changed an ingestion table meanwhile (the generation moved): then it runs
   * again. It reads nothing until every write that moved the generation to
   * the value it read is visible (settle). The comparison needs no such wait:
   * it publishes only at the fill's generation, unmoved.
   */
  async firstFill(): Promise<number> {
    const { generation } = await readControl(this.db)
    await this.settle()
    const uris = (await this.db.query<{ uri: string }>('select uri from index_state order by uri')).rows.map((r) => r.uri)
    for (const uri of uris) await this.projector.fillPostgres(uri)
    const res = await (this.publisher ?? this.db).query(
      `update search_control set first_fill_done = true, fill_generation = $1, pg_verified = false, compared_at = null, note = null, updated_at = now()
       where id = 1 and search_generation_now() = $1`,
      [generation],
    )
    if (res.rowCount === 0) throw new Error('an ingestion table changed during the fill')
    return uris.length
  }

  /**
   * Compares Postgres's current documents by document id and content (not
   * versions, and not indexedAt, which records when each copy was written)
   * with every sidecar assembled again from stored records. From PR 2 today's
   * index is no longer written, and a managed index is itself filled from
   * Postgres, so it is never the copy: comparing with it could not fail. The result is recorded only if the fill is done and
   * the generation has not moved since the comparison began: a pass sets
   * pg_verified and a comparison that finds a difference clears it; one that
   * could not run changes nothing. Upkeep runs on one ingester at a time.
   */
  async compare(): Promise<{ ok: boolean; note: string }> {
    let ok = false
    let ran = false
    let note: string
    const against = 'stored records assembled again'
    const { generation } = await readControl(this.db)
    try {
      const [pg, es] = [await this.projector.postgres.currentDocs(), await this.reassembled()]
      const missing = [...es.keys()].filter((uri) => !pg.has(uri))
      const extra = [...pg.keys()].filter((uri) => !es.has(uri))
      const differ = [...pg.keys()].filter((uri) => es.has(uri) && comparable(pg.get(uri)!) !== comparable(es.get(uri)!))
      ran = true
      ok = missing.length === 0 && extra.length === 0 && differ.length === 0
      note = ok
        ? `matched ${against}: ${pg.size} documents`
        : [
            missing.length && `${missing.length} only in ${against} (${missing.slice(0, LISTED).join(', ')})`,
            extra.length && `${extra.length} only in Postgres (${extra.slice(0, LISTED).join(', ')})`,
            differ.length && `${differ.length} with different content (${differ.slice(0, LISTED).join(', ')})`,
          ]
            .filter(Boolean)
            .join('; ')
    } catch (err) {
      // Health is public, so the note says what happened, not the error text.
      this.log(`search: comparison could not run: ${(err as Error).message}`)
      note = `comparison with ${against} could not run; retrying at the next upkeep`
    }
    const res = await (this.publisher ?? this.db).query(
      `update search_control
         set pg_verified = case when $4 then $1 else pg_verified end,
             compared_at = case when $4 then now() else compared_at end, note = $2, updated_at = now()
       where id = 1 and first_fill_done and fill_generation = $3 and search_generation_now() = $3`,
      [ok, note, generation, ran],
    )
    if (res.rowCount === 0) return { ok: false, note: 'an ingestion table changed during the comparison; filling again' }
    if (ran && !ok) {
      await this.issues
        ?.open(
          'comparison',
          'Search: the Postgres comparison did not pass',
          `Postgres is not verified, so it may not serve new sessions; upkeep compares again hourly. ${note}`,
        )
        .catch((err) => this.log(`search: could not open the comparison issue: ${(err as Error).message}`))
    }
    return { ok, note }
  }

  /** Every complete sidecar in index_state as stored records assemble it now. */
  private async reassembled(): Promise<Map<string, EventDoc>> {
    const out = new Map<string, EventDoc>()
    const uris = (await this.db.query<{ uri: string }>('select uri from index_state order by uri')).rows.map((r) => r.uri)
    for (const uri of uris) {
      const doc = await this.projector.assembled(uri)
      if (doc) out.set(uri, doc)
    }
    return out
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
