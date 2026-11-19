import pg from 'pg'
import { checkout, type Db } from '../db.js'
import type { BackendName } from './backend.js'

export interface ControlRow {
  /** Moved on by every change to an ingestion table from code that does not write event_docs. */
  generation: string
  elasticCursorsUntil: Date
  firstFillDone: boolean
  pgVerified: boolean
  comparedAt: Date | null
  note: string | null
  /** The fill-and-compare version that last passed (CHECK_VERSION in verify.ts). */
  checkedBy: number
}

const READ_EVERY_MS = 30_000
/**
 * Every read or write of search_control by the query service gives up after
 * this long. Routing then counts Postgres as unverified, health reports
 * search_control unavailable, and a step that needs the row (covering or
 * checking an Elasticsearch session) fails, so the request is refused or
 * falls back rather than hanging.
 */
export const CONTROL_MS = 500

/**
 * The query service's own small pool for search_control: two connections,
 * each statement and each connection wait cut off at 500 ms, so a stalled
 * read is stopped in the database too, not only no longer awaited.
 */
export function controlPool(databaseUrl: string, log: (msg: string) => void = console.error): Db {
  const pool = new pg.Pool({ connectionString: databaseUrl, max: 2, statement_timeout: CONTROL_MS, connectionTimeoutMillis: CONTROL_MS, keepAlive: true })
  pool.on('error', (err) => log(`postgres (search control): idle client lost: ${err.message}`))
  return pool
}

export async function readControl(db: Db): Promise<ControlRow> {
  // The fill, the comparison and verification count only at the generation
  // the fill finished at; once it moves, they read as not done.
  const res = await db.query<{
    generation: string
    elastic_cursors_until: Date
    first_fill_done: boolean
    pg_verified: boolean
    compared_at: Date | null
    note: string | null
    stale: boolean
    checked_by: number
  }>(
    `select g::text as generation, elastic_cursors_until,
            first_fill_done and fill_generation = g as first_fill_done,
            pg_verified and fill_generation = g as pg_verified,
            case when fill_generation = g then compared_at end as compared_at,
            note, first_fill_done and fill_generation <> g as stale, checked_by
     from search_control, search_generation_now() as g where id = 1`,
  )
  const r = res.rows[0]
  return {
    generation: r.generation,
    elasticCursorsUntil: r.elastic_cursors_until,
    firstFillDone: r.first_fill_done,
    pgVerified: r.pg_verified,
    comparedAt: r.compared_at,
    checkedBy: r.checked_by,
    note: r.stale ? 'ingestion tables were changed by code that does not write event_docs; filling again' : r.note,
  }
}

/**
 * The query service's view of search_control: read at most every 30 seconds
 * for routing (spec 6), and fresh where an answer depends on it.
 */
export class SearchControl {
  private row?: ControlRow
  private readAt = 0

  constructor(
    private db: Db,
    private now: () => number = Date.now,
  ) {}

  async current(): Promise<ControlRow> {
    if (!this.row || this.now() - this.readAt >= READ_EVERY_MS) await this.refresh()
    return this.row!
  }

  async refresh(): Promise<ControlRow> {
    this.row = await this.bounded((q) => readControl(q))
    this.readAt = this.now()
    return this.row
  }

  /**
   * Runs `work` on a connection of its own, bounded to CONTROL_MS end to end.
   * A connection that does not answer in time is destroyed, not returned to
   * the pool, so a hung connection cannot hold one of its two slots; one that
   * arrives after the wait gave up goes straight back.
   */
  private async bounded<R>(work: (q: Db) => Promise<R>): Promise<R> {
    const deadline = Date.now() + CONTROL_MS
    const pool = this.db as Partial<pg.Pool>
    if (typeof pool.connect !== 'function') return within(work(this.db), deadline)
    let gaveUp = false
    const connecting = checkout(this.db as pg.Pool)
    connecting.then(
      (c) => gaveUp && c.release(),
      () => {},
    )
    let client: pg.PoolClient
    try {
      client = await within(connecting, deadline)
    } catch (err) {
      gaveUp = true
      throw err
    }
    try {
      const out = await within(work(client as unknown as Db), deadline)
      client.release()
      return out
    } catch (err) {
      client.release(err as Error)
      throw err
    }
  }
}

/** The promise's result, or a rejection once the deadline (one for the whole call) has passed. */
async function within<T>(work: Promise<T>, deadline: number): Promise<T> {
  let timer: NodeJS.Timeout | undefined
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`search_control did not answer within ${CONTROL_MS} ms`)), Math.max(0, deadline - Date.now()))
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

export interface Route {
  backend: BackendName
  reason: string
}

/**
 * The rule of spec 5.2 for a new session from PR 2: Postgres if verified,
 * else nothing. Elasticsearch is not written, so it may not serve.
 */
export function route(row: Pick<ControlRow, 'pgVerified'>): Route | undefined {
  return row.pgVerified ? { backend: 'postgres', reason: 'Postgres verified; Elasticsearch is off' } : undefined
}
