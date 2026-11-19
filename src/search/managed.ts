import { checkout, type Db } from '../db.js'
import type { EventDoc } from '../index/assemble.js'
import {
  MANAGED_PREFIX,
  bulkManaged,
  clusterStatus,
  createManagedIndex,
  deleteIndex,
  listEventIndexes,
  managedVersions,
  putManaged,
  removeManaged,
  type BulkOp,
  type Elastic,
} from '../index/elastic.js'
import { WRITE_LOCK, type PostgresBackend, type WriteResult } from './postgres.js'

/** An unconfirmed write older than this takes the managed index out of new sessions (spec 5.2). */
export const PENDING_MS = 2_000
const BATCH = 500

export interface ManagedState {
  esInUse: boolean
  esCurrent: boolean
  elasticIndex: string | null
  elasticCursorsUntil: Date
  legacyDropAfter: Date | null
}

/**
 * The managed Elasticsearch index (spec 5.1, 5.2, 5.4, 6.4), on the ingester:
 * the write that follows each Postgres commit, the replay of unconfirmed
 * writes, the build, the reconcile and the end of use. Nothing here sends
 * anything to Elasticsearch while it is not in use, except deleting the index
 * when use ends.
 */
export class ManagedIndex {
  constructor(
    private db: Db,
    private es: Elastic,
    private postgres: PostgresBackend,
    private log: (msg: string) => void = console.log,
  ) {}

  async state(): Promise<ManagedState> {
    const r = (
      await this.db.query<{
        es_in_use: boolean
        es_current: boolean
        elastic_index: string | null
        elastic_cursors_until: Date
        legacy_drop_after: Date | null
      }>('select es_in_use, es_current, elastic_index, elastic_cursors_until, legacy_drop_after from search_control where id = 1')
    ).rows[0]
    return {
      esInUse: r.es_in_use,
      esCurrent: r.es_current,
      elasticIndex: r.elastic_index,
      elasticCursorsUntil: r.elastic_cursors_until,
      legacyDropAfter: r.legacy_drop_after,
    }
  }

  /**
   * The managed-index write after a Postgres commit (spec 4.2), outside the
   * lock, with a 2-second limit. Success deletes the write's es_pending row;
   * failure leaves it for the replay and never fails the event.
   */
  async afterWrite(uri: string, write: WriteResult, doc: EventDoc | undefined): Promise<void> {
    const index = write.es?.index
    if (!index) return
    try {
      if (doc) await putManaged(this.es, index, doc, write.version)
      else await removeManaged(this.es, index, uri, write.version)
      await this.db.query('delete from es_pending where uri = $1 and version = $2', [uri, String(write.version)])
    } catch (err) {
      this.log(`elasticsearch: write of ${uri} left for the replay: ${(err as Error).message}`)
    }
  }

  /**
   * Sets or clears es_in_use under the write lock, so no write is between
   * reading it and committing. Clearing also clears es_current and every
   * es_pending row: from then on nothing is sent.
   */
  async setInUse(inUse: boolean): Promise<void> {
    await this.underWriteLock(async (c) => {
      if (inUse) await c.query('update search_control set es_in_use = true, updated_at = now() where id = 1')
      else {
        await c.query('update search_control set es_in_use = false, es_current = false, updated_at = now() where id = 1')
        await c.query('delete from es_pending')
      }
    })
  }

  /** Records a keep-awake answer (spec 5.2): the cluster's status and when it answered. */
  async keepAwake(): Promise<'green' | 'yellow' | 'red'> {
    const status = await clusterStatus(this.es)
    await this.db.query('update search_control set es_seen_at = now(), es_status = $1 where id = 1', [status])
    return status
  }

  /**
   * Replays es_pending rows older than `olderThanMs` into the managed index:
   * each uri's current row with its version, or, with no current row, a
   * delete with the newest pending number. The batch is written, the index
   * refreshed, and only then are the rows it covered deleted, so a replayed
   * write is searchable before the index may serve again (spec 5.1). Returns
   * the rows still pending.
   */
  async replay(olderThanMs = PENDING_MS): Promise<number> {
    const { esInUse, elasticIndex: index } = await this.state()
    if (!esInUse || !index) return this.pendingCount()
    for (;;) {
      const due = (
        await this.db.query<{ uri: string; version: string }>(
          `select uri, max(version)::text as version from es_pending
           where at < clock_timestamp() - make_interval(secs => $1::float8 / 1000)
           group by uri order by min(at) limit $2`,
          [olderThanMs, BATCH],
        )
      ).rows
      if (due.length === 0) return this.pendingCount()
      const current = await this.currentRows(due.map((d) => d.uri))
      const ops: BulkOp[] = due.map((d) => {
        const row = current.get(d.uri)
        return row ? { uri: d.uri, version: row.version, doc: row.doc } : { uri: d.uri, version: BigInt(d.version) }
      })
      const done = await bulkManaged(this.es, index, ops)
      await this.es.indices.refresh({ index })
      const covered = ops.filter((op) => done.has(op.uri))
      if (covered.length) {
        await this.db.query(
          `delete from es_pending p using unnest($1::text[], $2::bigint[]) as c(uri, version)
           where p.uri = c.uri and p.version <= c.version`,
          [covered.map((c) => c.uri), covered.map((c) => String(c.version))],
        )
      }
      if (covered.length < ops.length) {
        this.log(`elasticsearch: replay left ${ops.length - covered.length} writes pending`)
        return this.pendingCount()
      }
    }
  }

  /**
   * Builds a fresh managed index (spec 5.4, 6.4 backfilling): deletes
   * leftovers no session can reach, creates placeproto-events-<n>, names it in
   * search_control so new writes reach it, copies every current row with its
   * version, then reconciles and replays. es_current is left to the caller.
   * A failure deletes the candidate, which no session has used.
   */
  async build(legacyIndex: string): Promise<string> {
    await this.deleteLeftovers(legacyIndex)
    const n = (
      await this.db.query<{ n: number }>('update search_control set index_counter = index_counter + 1 where id = 1 returning index_counter as n')
    ).rows[0].n
    const index = `${MANAGED_PREFIX}${n}`
    try {
      await createManagedIndex(this.es, index)
      await this.underWriteLock((c) => c.query('update search_control set elastic_index = $1, es_current = false, updated_at = now() where id = 1', [index]))
      const copied = await this.copyAll(index)
      const differences = await this.reconcile(index)
      const left = await this.replay(0)
      if (left > 0) throw new Error(`${left} writes still pending after the build's replay`)
      this.log(`elasticsearch: built ${index}: ${copied} documents copied, ${differences} differences reconciled`)
      return index
    } catch (err) {
      await this.dropCandidate(index)
      throw err
    }
  }

  /** Marks the managed index current: it holds every current row (spec 5.2). */
  async markCurrent(index: string): Promise<void> {
    await this.db.query('update search_control set es_current = true, updated_at = now() where id = 1 and elastic_index = $1', [index])
  }

  /** Deletes a candidate index no session used, and forgets it. */
  async dropCandidate(index: string): Promise<void> {
    await this.underWriteLock((c) =>
      c.query('update search_control set elastic_index = null, es_current = false where id = 1 and elastic_index = $1', [index]),
    ).catch(() => {})
    await deleteIndex(this.es, index).catch((err) => this.log(`elasticsearch: could not delete ${index}: ${(err as Error).message}`))
  }

  /**
   * Compares every document id and version in the managed index with the
   * current rows (spec 6.4). Each document missing or older, and each with no
   * current row, gets an es_pending row, so a difference takes the index out
   * of new sessions until replayed. Returns the number of differences.
   */
  async reconcile(index: string): Promise<number> {
    const inEs = await managedVersions(this.es, index)
    const current = (await this.db.query<{ uri: string; version: string }>('select uri, version::text from event_docs where closed_version is null')).rows
    let differences = 0
    const behind: { uri: string; version: string }[] = []
    for (const r of current) {
      const v = inEs.get(r.uri)
      if (v === undefined || v < BigInt(r.version)) behind.push(r)
      inEs.delete(r.uri)
    }
    for (let i = 0; i < behind.length; i += BATCH) {
      const part = behind.slice(i, i + BATCH)
      await this.db.query(
        `insert into es_pending (uri, version) select * from unnest($1::text[], $2::bigint[]) on conflict do nothing`,
        [part.map((p) => p.uri), part.map((p) => p.version)],
      )
    }
    differences += behind.length
    // Left in inEs: documents with no current row when read. Each is checked
    // again under the write lock and, if still without one, gets a fresh
    // number, which the replay deletes with.
    for (const uri of inEs.keys()) {
      await this.underWriteLock(async (c) => {
        const has = await c.query('select 1 from event_docs where uri = $1 and closed_version is null', [uri])
        if (has.rowCount) return
        await c.query(`insert into es_pending (uri, version) values ($1, nextval('event_doc_version'))`, [uri])
        differences++
      })
    }
    await this.db.query('update search_control set reconciled_at = now() where id = 1')
    return differences
  }

  /**
   * Ends use (spec 5.2), only once elastic_cursors_until has passed: clears
   * es_in_use, es_current and es_pending under the write lock, then deletes
   * the managed index and, once its hour is over, today's index. Returns false
   * when the cursors' hour has not passed yet.
   */
  async endUse(legacyIndex: string): Promise<boolean> {
    const st = await this.state()
    if (st.elasticCursorsUntil.getTime() > Date.now()) return false
    await this.setInUse(false)
    for (const index of [st.elasticIndex, legacyGone(st) ? legacyIndex : undefined]) {
      if (!index) continue
      await deleteIndex(this.es, index).catch((err) => this.log(`elasticsearch: could not delete ${index}; it stays until use next begins: ${(err as Error).message}`))
    }
    await this.db.query('update search_control set elastic_index = null where id = 1')
    return true
  }

  /**
   * Deletes today's index once its hour after the PR 2 deploy has passed.
   * Only while in use.
   */
  async dropLegacyWhenDue(legacyIndex: string): Promise<void> {
    const st = await this.state()
    if (!st.esInUse || !legacyGone(st)) return
    const exists = await this.es.indices.exists({ index: legacyIndex }, { requestTimeout: 10_000 })
    if (exists) {
      await deleteIndex(this.es, legacyIndex)
      this.log(`elasticsearch: deleted today's index ${legacyIndex}`)
    }
  }

  /**
   * Deletes every placeproto-events-* index no session can reach: all but
   * the current managed index, and today's index until its hour is over.
   */
  private async deleteLeftovers(legacyIndex: string): Promise<void> {
    const st = await this.state()
    for (const index of await listEventIndexes(this.es)) {
      if (st.esCurrent && index === st.elasticIndex) continue
      if (index === legacyIndex && !legacyGone(st)) continue
      await deleteIndex(this.es, index)
      this.log(`elasticsearch: deleted leftover index ${index}`)
    }
  }

  private async copyAll(index: string): Promise<number> {
    let after = ''
    let copied = 0
    for (;;) {
      const rows = (
        await this.db.query<{ uri: string; version: string; doc: EventDoc }>(
          `select uri, version::text, doc from event_docs where closed_version is null and uri > $1 order by uri limit $2`,
          [after, BATCH],
        )
      ).rows
      if (rows.length === 0) return copied
      const ops = rows.map((r) => ({ uri: r.uri, version: BigInt(r.version), doc: r.doc }))
      const done = await bulkManaged(this.es, index, ops)
      // A copy Elasticsearch refused is recorded and replayed with the rest.
      const missed = ops.filter((o) => !done.has(o.uri))
      if (missed.length) {
        await this.db.query(`insert into es_pending (uri, version) select * from unnest($1::text[], $2::bigint[]) on conflict do nothing`, [
          missed.map((m) => m.uri),
          missed.map((m) => String(m.version)),
        ])
      }
      copied += done.size
      after = rows[rows.length - 1].uri
    }
  }

  private async currentRows(uris: string[]): Promise<Map<string, { version: bigint; doc: EventDoc }>> {
    const res = await this.db.query<{ uri: string; version: string; doc: EventDoc }>(
      'select uri, version::text, doc from event_docs where closed_version is null and uri = any($1)',
      [uris],
    )
    return new Map(res.rows.map((r) => [r.uri, { version: BigInt(r.version), doc: r.doc }]))
  }

  async pendingCount(): Promise<number> {
    return Number((await this.db.query<{ n: string }>('select count(*) as n from es_pending')).rows[0].n)
  }

  private async underWriteLock<R>(work: (c: Db) => Promise<R>): Promise<R> {
    const client = await checkout(this.db)
    try {
      await client.query('begin')
      await client.query('select pg_advisory_xact_lock($1)', [WRITE_LOCK])
      const out = await work(client as unknown as Db)
      await client.query('commit')
      return out
    } catch (err) {
      await client.query('rollback').catch(() => {})
      throw err
    } finally {
      client.release()
    }
  }
}

/**
 * Today's index may be deleted: legacy_drop_after (the first PR 2 start plus
 * 75 minutes: the hour of sessions, plus the deploy's overlap, during which a
 * PR 1 replica could still open one) has passed. From PR 2 no session opens
 * on it.
 */
function legacyGone(st: ManagedState): boolean {
  return st.legacyDropAfter !== null && st.legacyDropAfter.getTime() <= Date.now()
}
