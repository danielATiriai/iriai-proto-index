import type { Db } from '../db.js'

/**
 * Histogram bucket edges in milliseconds: bucket i counts searches that took
 * at least edge i-1 and under edge i; the last bucket counts 2,000 ms and
 * over (a search stopped by the 2 s limit counts as 2,000 ms, spec 6.1).
 * 10 ms steps to 500 ms and 50 ms steps to 2,000 ms, so the 250 ms threshold
 * of 6.2 falls on an edge and is read exactly.
 */
export const BUCKETS_MS: number[] = [
  ...Array.from({ length: 50 }, (_, i) => (i + 1) * 10),
  ...Array.from({ length: 30 }, (_, i) => 550 + i * 50),
]
const SIZE = BUCKETS_MS.length + 1
const FLUSH_MS = 60_000

export function bucketOf(ms: number): number {
  const i = BUCKETS_MS.findIndex((edge) => ms < edge)
  return i === -1 ? SIZE - 1 : i
}

/**
 * The p95 of a histogram as the bounds of its bucket: the true value is at
 * least `lower` and under `upper` (Infinity in the overflow bucket). The scale
 * check reads `lower`, so it never reports a value the bucket cannot prove.
 */
export function p95(hist: readonly (number | string)[]): { lower: number; upper: number; samples: number } | undefined {
  const counts = hist.map(Number)
  const samples = counts.reduce((a, b) => a + b, 0)
  if (samples === 0) return undefined
  const rank = Math.ceil(samples * 0.95)
  let seen = 0
  for (let i = 0; i < counts.length; i++) {
    seen += counts[i]
    if (seen >= rank) return { lower: i === 0 ? 0 : BUCKETS_MS[i - 1], upper: i < BUCKETS_MS.length ? BUCKETS_MS[i] : Infinity, samples }
  }
  return undefined
}

/** Whether a failed Postgres search ran out of time: the 2 s statement limit, or 2 s waiting for one of the search connections. */
export function isTimeout(err: unknown): boolean {
  const e = err as { code?: string; message?: string }
  return e?.code === '57014' || /timeout exceeded when trying to connect/i.test(e?.message ?? '')
}

/**
 * The query service's daily search stats (spec 6.1), counted in memory and
 * added to the day's row every minute, so several replicas add up.
 */
export class SearchStats {
  private day = utcDay()
  private calls = 0
  private pg = new Array<number>(SIZE).fill(0)
  private pgTimeouts = 0
  private timer?: NodeJS.Timeout

  constructor(
    private db: Db,
    private log: (msg: string) => void = console.error,
  ) {}

  start(): void {
    this.timer = setInterval(() => void this.flush().catch((err) => this.log(`search stats: ${(err as Error).message}`)), FLUSH_MS)
    this.timer.unref()
  }

  stop(): void {
    clearInterval(this.timer)
  }

  /** One queryEvents call. */
  call(): void {
    this.roll()
    this.calls++
  }

  /** One Postgres search step's time; one that ran out of time counts as 2,000 ms and a timeout. */
  search(ms: number, timedOut = false): void {
    this.roll()
    this.pg[bucketOf(timedOut ? 2_000 : ms)]++
    if (timedOut) this.pgTimeouts++
  }

  async flush(): Promise<void> {
    const out = { day: this.day, calls: this.calls, pg: this.pg, pgTimeouts: this.pgTimeouts }
    if (out.calls === 0 && out.pg.every((n) => n === 0)) return
    this.reset(this.day)
    try {
      await addStats(this.db, out)
    } catch (err) {
      // Put the counts back for the next flush.
      if (out.day === this.day) {
        this.calls += out.calls
        out.pg.forEach((n, i) => (this.pg[i] += n))
        this.pgTimeouts += out.pgTimeouts
      }
      throw err
    }
  }

  private roll(): void {
    const today = utcDay()
    if (today === this.day) return
    void this.flush().catch((err) => this.log(`search stats: ${(err as Error).message}`))
    this.reset(today)
  }

  private reset(day: string): void {
    this.day = day
    this.calls = 0
    this.pg = new Array<number>(SIZE).fill(0)
    this.pgTimeouts = 0
  }
}

export function utcDay(t = Date.now()): string {
  return new Date(t).toISOString().slice(0, 10)
}

/** Adds counts to a day's row, histograms element by element. */
export async function addStats(db: Db, s: { day: string; calls: number; pg: number[]; pgTimeouts: number }): Promise<void> {
  await db.query(
    `insert into search_stats as t (day, calls, pg_hist, pg_timeouts) values ($1, $2, $3, $4)
     on conflict (day) do update set
       calls = t.calls + excluded.calls,
       pg_hist = add_hist(t.pg_hist, excluded.pg_hist),
       pg_timeouts = t.pg_timeouts + excluded.pg_timeouts`,
    [s.day, s.calls, s.pg, s.pgTimeouts],
  )
}

/** The ingester records the current event count on today's row (spec 6.1): the latest count, not a maximum. */
export async function recordEvents(db: Db, events: number): Promise<void> {
  await db.query(
    `insert into search_stats (day, events) values ((now() at time zone 'utc')::date, $1)
     on conflict (day) do update set events = excluded.events`,
    [events],
  )
}

/** Current events, by an ordinary count that takes no lock. */
export async function countEvents(db: Db): Promise<number> {
  return Number((await db.query<{ n: string }>('select count(*) as n from event_docs where closed_version is null')).rows[0].n)
}

export interface DayStats {
  day: string
  calls: number
  pgP95?: { lower: number; upper: number; samples: number }
  pgTimeoutRate: number
  events: number | null
}

/** The last `n` UTC days before today, newest first; a day with no row is returned with no calls. */
export async function lastDays(db: Db, n: number): Promise<DayStats[]> {
  const res = await db.query<{ day: string; calls: string; pg_hist: string[]; pg_timeouts: string; events: string | null }>(
    `select d::date::text as day, coalesce(s.calls, 0)::text as calls, coalesce(s.pg_hist, '{}') as pg_hist,
            coalesce(s.pg_timeouts, 0)::text as pg_timeouts, s.events::text as events
     from generate_series((now() at time zone 'utc')::date - $1::int, (now() at time zone 'utc')::date - 1, interval '1 day') as d
     left join search_stats s on s.day = d::date
     order by d desc`,
    [n],
  )
  return res.rows.map((r) => {
    const p = p95(r.pg_hist)
    return {
      day: r.day,
      calls: Number(r.calls),
      pgP95: p,
      pgTimeoutRate: p ? Number(r.pg_timeouts) / p.samples : 0,
      events: r.events === null ? null : Number(r.events),
    }
  })
}

/** Today's Postgres p95 so far (the upper bound of its bucket), for health. */
export async function todayPgP95(db: Db): Promise<number | null> {
  const res = await db.query<{ pg_hist: string[] }>(`select pg_hist from search_stats where day = (now() at time zone 'utc')::date`)
  const p = res.rows[0] ? p95(res.rows[0].pg_hist) : undefined
  return p ? p.upper : null
}

/** Keeps rows 90 days (spec 6.1). */
export async function pruneStats(db: Db): Promise<void> {
  await db.query(`delete from search_stats where day < (now() at time zone 'utc')::date - 90`)
}
