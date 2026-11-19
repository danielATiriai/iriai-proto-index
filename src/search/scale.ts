import type { Db } from '../db.js'
import type { Thresholds } from '../settings.js'
import type { Issues } from './issues.js'
import { readControl } from './control.js'
import { countEvents, lastDays, pruneStats, recordEvents } from './stats.js'

const ON_DAYS = 3
/** Postgres unverified for this many checks in a row (30 minutes) means search has answered 503 that long. */
const UNVERIFIED_CHECKS = 3

/**
 * The scale check (spec 6.1, 6.2), in the ingester every 10 minutes. It
 * records the event count, keeps 90 days of stats, and opens an issue when
 * the switch-on condition of D2 A holds. Switching Elasticsearch on is not
 * automatic in PR 2 (amendment to D2 A and D3 A): it is a later PR, written
 * when this issue first opens. Nothing here contacts Elasticsearch.
 */
export class ScaleCheck {
  constructor(
    private db: Db,
    private thresholds: Thresholds,
    private issues: Issues,
    private log: (msg: string) => void = console.log,
  ) {}

  /** The last result, for health. */
  last?: { at: string; events: number; trigger: string | null }
  private unverified = 0

  async tick(): Promise<void> {
    try {
      await pruneStats(this.db)
      const events = await countEvents(this.db)
      await recordEvents(this.db, events)
      const why = await this.onReason(events)
      this.last = { at: new Date().toISOString(), events, trigger: why ?? null }
      await this.db.query(
        `insert into search_scale (id, checked_at, events, trigger_reason) values (1, now(), $1, $2)
         on conflict (id) do update set checked_at = excluded.checked_at, events = excluded.events, trigger_reason = excluded.trigger_reason`,
        [events, why ?? null],
      )
      this.unverified = (await readControl(this.db)).pgVerified ? 0 : this.unverified + 1
      if (this.unverified >= UNVERIFIED_CHECKS) {
        await this.issues.open(
          'unverified',
          'Search: Postgres has not been verified for 30 minutes, so search answers 503',
          'Upkeep fills Postgres again when an ingestion table changed, and compares it with stored records hourly; health shows search.comparison.',
        )
      }
      if (why) {
        await this.issues.open(
          'scale',
          'Search: the scale trigger for Elasticsearch is met',
          `${why}. Search runs on Postgres; switching Elasticsearch on is the follow-up PR described in docs/search-without-elasticsearch/spec.md (6.4).`,
        )
      }
    } catch (err) {
      this.log(`search scale check failed: ${(err as Error).message}`)
    }
  }

  /** Why Elasticsearch should switch on (spec 6.2, D2 A), or undefined. */
  async onReason(events: number): Promise<string | undefined> {
    const t = this.thresholds
    if (events >= t.onEvents) return `${events} events, at least ${t.onEvents}`
    const days = await lastDays(this.db, ON_DAYS)
    const slow = days.every((d) => d.calls >= t.onMinCalls && !!d.pgP95 && (d.pgP95.lower >= t.onP95Ms || d.pgTimeoutRate > t.onTimeoutRate))
    if (slow) return `on ${ON_DAYS} consecutive days with at least ${t.onMinCalls} calls, Postgres p95 at least ${t.onP95Ms} ms or over ${t.onTimeoutRate * 100}% timeouts`
    return undefined
  }
}

/** The last scale check any ingester recorded, for health; null before the first. */
export async function lastScaleCheck(db: Db): Promise<{ at: string; events: number; trigger: string | null } | null> {
  const r = (await db.query<{ checked_at: Date; events: string; trigger_reason: string | null }>('select checked_at, events, trigger_reason from search_scale where id = 1')).rows[0]
  return r ? { at: r.checked_at.toISOString(), events: Number(r.events), trigger: r.trigger_reason } : null
}
