import pg from 'pg'
import type { Db } from '../db.js'
import type { BackendName } from './backend.js'

export interface ControlRow {
  /** Moved on by every change to an ingestion table from code that does not write event_docs. */
  generation: string
  elasticCursorsUntil: Date
  firstFillDone: boolean
  pgVerified: boolean
  comparedAt: Date | null
  note: string | null
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
/** Every Elasticsearch first page leaves at least this long for its session (spec 3). */
const COVER_MS = 61 * 60_000
/** Extending when under 60 minutes remain is enough: a session never outlives 60 of the 61 minutes. */
const EXTEND_BELOW_MS = 60 * 60_000

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
  }>(
    `select g::text as generation, elastic_cursors_until,
            first_fill_done and fill_generation = g as first_fill_done,
            pg_verified and fill_generation = g as pg_verified,
            case when fill_generation = g then compared_at end as compared_at,
            note, first_fill_done and fill_generation <> g as stale
     from search_control, search_generation_now() as g where id = 1`,
  )
  const r = res.rows[0]
  return {
    generation: r.generation,
    elasticCursorsUntil: r.elastic_cursors_until,
    firstFillDone: r.first_fill_done,
    pgVerified: r.pg_verified,
    comparedAt: r.compared_at,
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
   * Whether an Elasticsearch cursor may still be sent to Elasticsearch. A
   * stale "no" is checked against the table before it is given, since another
   * replica may have moved the time on.
   */
  async elasticCursorsOpen(): Promise<boolean> {
    if ((await this.current()).elasticCursorsUntil.getTime() > this.now()) return true
    return (await this.refresh()).elasticCursorsUntil.getTime() > this.now()
  }

  /**
   * Called before an Elasticsearch first page returns: moves
   * elastic_cursors_until to at least now + 61 minutes, writing the row only
   * when less than 60 minutes remain.
   */
  async coverElasticSession(): Promise<void> {
    const row = await this.current()
    if (row.elasticCursorsUntil.getTime() >= this.now() + EXTEND_BELOW_MS) return
    const until = new Date(this.now() + COVER_MS)
    const res = await this.bounded((q) =>
      q.query<{ elastic_cursors_until: Date }>(
        `update search_control set elastic_cursors_until = greatest(elastic_cursors_until, $1), updated_at = now()
         where id = 1 returning elastic_cursors_until`,
        [until],
      ),
    )
    row.elasticCursorsUntil = res.rows[0].elastic_cursors_until
  }

  /**
   * Runs `work` on a connection of its own, bounded to CONTROL_MS end to end.
   * A connection that does not answer in time is destroyed, not returned to
   * the pool, so a hung connection cannot hold one of its two slots; one that
   * arrives after the wait gave up goes straight back.
   */
  private async bounded<R>(work: (q: Db) => Promise<R>): Promise<R> {
    const pool = this.db as Partial<pg.Pool>
    if (typeof pool.connect !== 'function') return within(work(this.db))
    let gaveUp = false
    const connecting: Promise<pg.PoolClient> = (this.db as pg.Pool).connect()
    connecting.then(
      (c: pg.PoolClient) => gaveUp && c.release(),
      () => {},
    )
    let client: pg.PoolClient
    try {
      client = await within(connecting)
    } catch (err) {
      gaveUp = true
      throw err
    }
    try {
      const out = await within(work(client as unknown as Db))
      client.release()
      return out
    } catch (err) {
      client.release(err as Error)
      throw err
    }
  }
}

/** The promise's result, or a rejection once CONTROL_MS has passed. */
async function within<T>(work: Promise<T>): Promise<T> {
  let timer: NodeJS.Timeout | undefined
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`search_control did not answer within ${CONTROL_MS} ms`)), CONTROL_MS)
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
 * The rule of spec 5.2 for a new session, as PR 1 has it: the chosen backend
 * (SEARCH_BACKEND_FORCE, default elastic) if it may serve, else Postgres if
 * verified, else nothing. Today's index may serve throughout PR 1, since it is
 * written as before; Postgres may serve once verified.
 */
export function route(force: BackendName, row: Pick<ControlRow, 'pgVerified'>): Route | undefined {
  if (force === 'elastic') return { backend: 'elastic', reason: 'SEARCH_BACKEND_FORCE=elastic (default)' }
  if (row.pgVerified) return { backend: 'postgres', reason: 'SEARCH_BACKEND_FORCE=postgres, Postgres verified' }
  return undefined
}
