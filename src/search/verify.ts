import type { Db } from '../db.js'
import type { EventDoc } from '../index/assemble.js'
import type { Projector } from '../ingest/projector.js'
import { readControl } from './control.js'

const COMPARE_EVERY_MS = 60 * 60_000
const LISTED = 5

/**
 * The ingester's search upkeep for PR 1 (spec 4.2, 5.2, 5.4), run at start-up
 * and every 10 minutes in order with event handling, so no write lands in the
 * middle of it: closed rows past 61 minutes are deleted; the first fill runs
 * until it has finished once; then the comparison with today's index runs
 * right after the fill and hourly until it passes once, which verifies
 * Postgres for good.
 */
export class SearchUpkeep {
  constructor(
    private db: Db,
    private projector: Projector,
    private log: (msg: string) => void = console.log,
    private now: () => number = Date.now,
  ) {}

  async run(): Promise<void> {
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
    this.log(`search: comparison with today's index ${result.ok ? 'passed; Postgres is verified' : `did not pass: ${result.note}`}`)
  }

  /**
   * Projects every sidecar in index_state into Postgres, then records that
   * the first fill is done, unless code that does not write event_docs
   * changed index_state meanwhile (the generation moved): then it runs again.
   */
  async firstFill(): Promise<number> {
    const { generation } = await readControl(this.db)
    const uris = (await this.db.query<{ uri: string }>('select uri from index_state order by uri')).rows.map((r) => r.uri)
    for (const uri of uris) await this.projector.fillPostgres(uri)
    const res = await this.db.query('update search_control set first_fill_done = true, updated_at = now() where id = 1 and generation = $1', [generation])
    if (res.rowCount === 0) throw new Error('an ingestion table changed during the fill')
    return uris.length
  }

  /**
   * Compares Postgres's current documents with today's index by document id
   * and content (not versions, and not indexedAt, which records when each
   * copy was written). A pass sets pg_verified only if the fill is done and
   * the generation has not moved since the comparison began; only a change
   * from code that does not write event_docs clears it.
   */
  async compare(): Promise<{ ok: boolean; note: string }> {
    let ok = false
    let note: string
    const { generation } = await readControl(this.db)
    try {
      const [pg, es] = [await this.projector.postgres.currentDocs(), await this.projector.elastic.allDocs()]
      const missing = [...es.keys()].filter((uri) => !pg.has(uri))
      const extra = [...pg.keys()].filter((uri) => !es.has(uri))
      const differ = [...pg.keys()].filter((uri) => es.has(uri) && comparable(pg.get(uri)!) !== comparable(es.get(uri)!))
      ok = missing.length === 0 && extra.length === 0 && differ.length === 0
      note = ok
        ? `matched today's index: ${pg.size} documents`
        : [
            missing.length && `${missing.length} only in Elasticsearch (${missing.slice(0, LISTED).join(', ')})`,
            extra.length && `${extra.length} only in Postgres (${extra.slice(0, LISTED).join(', ')})`,
            differ.length && `${differ.length} with different content (${differ.slice(0, LISTED).join(', ')})`,
          ]
            .filter(Boolean)
            .join('; ')
    } catch (err) {
      // Health is public, so the note says what happened, not the error text.
      this.log(`search: comparison could not run: ${(err as Error).message}`)
      note = 'comparison could not run: Elasticsearch or Postgres did not answer; retrying in an hour'
    }
    const res = await this.db.query(
      `update search_control set pg_verified = pg_verified or $1, compared_at = now(), note = $2, updated_at = now()
       where id = 1 and generation = $3 and first_fill_done`,
      [ok, note, generation],
    )
    if (res.rowCount === 0) return { ok: false, note: 'an ingestion table changed during the comparison; filling again' }
    return { ok, note }
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
