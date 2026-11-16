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
/** Every Elasticsearch first page leaves at least this long for its session (spec 3). */
const COVER_MS = 61 * 60_000
/** Extending once a minute is enough: a session never outlives 60 of the 61 minutes. */
const EXTEND_BELOW_MS = 60 * 60_000

export async function readControl(db: Db): Promise<ControlRow> {
  const res = await db.query<{
    generation: string
    elastic_cursors_until: Date
    first_fill_done: boolean
    pg_verified: boolean
    compared_at: Date | null
    note: string | null
  }>('select generation::text, elastic_cursors_until, first_fill_done, pg_verified, compared_at, note from search_control where id = 1')
  const r = res.rows[0]
  return {
    generation: r.generation,
    elasticCursorsUntil: r.elastic_cursors_until,
    firstFillDone: r.first_fill_done,
    pgVerified: r.pg_verified,
    comparedAt: r.compared_at,
    note: r.note,
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
    this.row = await readControl(this.db)
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
   * elastic_cursors_until to at least now + 61 minutes, with one row update at
   * most once a minute.
   */
  async coverElasticSession(): Promise<void> {
    const row = await this.current()
    if (row.elasticCursorsUntil.getTime() >= this.now() + EXTEND_BELOW_MS) return
    const until = new Date(this.now() + COVER_MS)
    const res = await this.db.query<{ elastic_cursors_until: Date }>(
      `update search_control set elastic_cursors_until = greatest(elastic_cursors_until, $1), updated_at = now()
       where id = 1 returning elastic_cursors_until`,
      [until],
    )
    row.elasticCursorsUntil = res.rows[0].elastic_cursors_until
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
