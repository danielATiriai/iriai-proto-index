import type { Db } from '../db.js'
import type { EventQuery } from '../query/params.js'
import type { BackendName } from './backend.js'

/**
 * Histogram bucket edges in milliseconds: bucket i counts searches that took
 * at least edge i-1 and under edge i; the last bucket counts 2,000 ms and
 * over (a search stopped by the 2 s limit counts as 2,000 ms, spec 6.1).
 * 10 ms steps to 500 ms and 50 ms steps to 2,000 ms, so the thresholds of
 * 6.2 (250 ms on, 100 ms off) fall on edges and are read exactly.
 */
export const BUCKETS_MS: number[] = [
  ...Array.from({ length: 50 }, (_, i) => (i + 1) * 10),
  ...Array.from({ length: 30 }, (_, i) => 550 + i * 50),
]
const SIZE = BUCKETS_MS.length + 1
export const MAX_SHAPES = 200
const FLUSH_MS = 60_000
/** Shadow queries: 1 in 10 Elasticsearch first pages, at most one a second (spec 6.1, D2 A). */
const SHADOW_EVERY = 10
const SHADOW_GAP_MS = 1_000

export function bucketOf(ms: number): number {
  const i = BUCKETS_MS.findIndex((edge) => ms < edge)
  return i === -1 ? SIZE - 1 : i
}

/**
 * The p95 of a histogram as the bounds of its bucket: the true value is at
 * least `lower` and under `upper` (Infinity in the overflow bucket). Switching
 * on reads `lower` (at least 250 ms), switching off reads `upper` (at most
 * 100 ms), so neither switches on a value the bucket cannot prove.
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

/** A query's shape: its normalized parameters with coordinates rounded to 3 decimals (about 100 m), and no caller. */
export function shapeOf(q: EventQuery): string {
  const r = (n: number) => Math.round(n * 1000) / 1000
  const { cursor: _cursor, limit: _limit, ...rest } = q
  const shape: Record<string, unknown> = { ...rest }
  if (rest.latitude !== undefined) shape.latitude = r(rest.latitude)
  if (rest.longitude !== undefined) shape.longitude = r(rest.longitude)
  if (rest.box) shape.box = { west: r(rest.box.west), south: r(rest.box.south), east: r(rest.box.east), north: r(rest.box.north) }
  return JSON.stringify(Object.fromEntries(Object.keys(shape).sort().map((k) => [k, shape[k]])))
}

/** A stored shape as a query again, for verifying (spec 6.4). */
export function queryOfShape(shape: string, limit: number): EventQuery {
  return { ...(JSON.parse(shape) as Omit<EventQuery, 'limit'>), limit }
}

/**
 * The query service's daily search stats (spec 6.1), counted in memory and
 * added to the day's row every minute, so several replicas add up.
 */
export class SearchStats {
  private day = utcDay()
  private calls = 0
  private pg = new Array<number>(SIZE).fill(0)
  private es = new Array<number>(SIZE).fill(0)
  private pgTimeouts = 0
  private shapes = new Set<string>()
  private firstPages = 0
  private lastShadow = 0
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

  /** One queryEvents call; its shape is kept while the day has room. */
  call(q: EventQuery): void {
    this.roll()
    this.calls++
    if (this.shapes.size < MAX_SHAPES) this.shapes.add(shapeOf(q))
  }

  /** One search step's time on a backend; a Postgres search stopped by the 2 s limit counts as 2,000 ms and a timeout. */
  search(backend: BackendName, ms: number, timedOut = false): void {
    this.roll()
    const hist = backend === 'postgres' ? this.pg : this.es
    hist[bucketOf(timedOut ? 2_000 : ms)]++
    if (timedOut && backend === 'postgres') this.pgTimeouts++
  }

  /** Whether this Elasticsearch first page should also run on Postgres, after its answer is sent. */
  shadowDue(now = Date.now()): boolean {
    this.firstPages++
    if (this.firstPages % SHADOW_EVERY !== 0 || now - this.lastShadow < SHADOW_GAP_MS) return false
    this.lastShadow = now
    return true
  }

  async flush(): Promise<void> {
    const out = { day: this.day, calls: this.calls, pg: this.pg, es: this.es, pgTimeouts: this.pgTimeouts, shapes: [...this.shapes] }
    if (out.calls === 0 && out.pg.every((n) => n === 0) && out.es.every((n) => n === 0)) return
    this.reset(this.day)
    try {
      await addStats(this.db, out)
    } catch (err) {
      // Put the counts back for the next flush.
      this.merge(out)
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
    this.es = new Array<number>(SIZE).fill(0)
    this.pgTimeouts = 0
    this.shapes = new Set()
  }

  private merge(out: { day: string; calls: number; pg: number[]; es: number[]; pgTimeouts: number; shapes: string[] }): void {
    if (out.day !== this.day) return
    this.calls += out.calls
    out.pg.forEach((n, i) => (this.pg[i] += n))
    out.es.forEach((n, i) => (this.es[i] += n))
    this.pgTimeouts += out.pgTimeouts
    for (const s of out.shapes) if (this.shapes.size < MAX_SHAPES) this.shapes.add(s)
  }
}

export function utcDay(t = Date.now()): string {
  return new Date(t).toISOString().slice(0, 10)
}

/** Adds counts to a day's row: histograms element by element, shapes up to 200 distinct. */
export async function addStats(
  db: Db,
  s: { day: string; calls: number; pg: number[]; es: number[]; pgTimeouts: number; shapes: string[] },
): Promise<void> {
  await db.query(
    `insert into search_stats as t (day, calls, pg_hist, es_hist, pg_timeouts, shapes)
     values ($1, $2, $3, $4, $5, $6)
     on conflict (day) do update set
       calls = t.calls + excluded.calls,
       pg_hist = add_hist(t.pg_hist, excluded.pg_hist),
       es_hist = add_hist(t.es_hist, excluded.es_hist),
       pg_timeouts = t.pg_timeouts + excluded.pg_timeouts,
       shapes = (select coalesce(jsonb_agg(x), '[]') from (
                  select x from (select x, min(o) as o from (
                    select x, 0 as o from jsonb_array_elements(t.shapes) as x
                    union all select x, 1 from jsonb_array_elements(excluded.shapes) as x) a group by x) b
                  order by o, x limit ${MAX_SHAPES}) c)`,
    [s.day, s.calls, s.pg, s.es, s.pgTimeouts, JSON.stringify(s.shapes)],
  )
}

/** The ingester records the current event count on today's row (spec 6.1). */
export async function recordEvents(db: Db, events: number): Promise<void> {
  await db.query(
    `insert into search_stats (day, events) values ((now() at time zone 'utc')::date, $1)
     on conflict (day) do update set events = greatest(coalesce(search_stats.events, 0), excluded.events)`,
    [events],
  )
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

/** Today's Postgres p95 so far, for health. */
export async function todayPgP95(db: Db): Promise<number | null> {
  const res = await db.query<{ pg_hist: string[] }>(`select pg_hist from search_stats where day = (now() at time zone 'utc')::date`)
  const p = res.rows[0] ? p95(res.rows[0].pg_hist) : undefined
  return p ? p.upper : null
}

/** Keeps rows 90 days and query shapes 7 (spec 6.1). */
export async function pruneStats(db: Db): Promise<void> {
  await db.query(`delete from search_stats where day < (now() at time zone 'utc')::date - 90`)
  await db.query(`update search_stats set shapes = '[]' where day < (now() at time zone 'utc')::date - 7 and shapes <> '[]'`)
  await db.query(`delete from search_fallbacks where at < now() - interval '1 day'`)
}

/** Stored query shapes from the last 7 days, most recent days first, at most `limit`. */
export async function storedShapes(db: Db, limit: number): Promise<string[]> {
  const res = await db.query<{ shapes: string[] }>(`select shapes from search_stats where day >= (now() at time zone 'utc')::date - 7 order by day desc`)
  const out = new Set<string>()
  for (const r of res.rows) for (const s of r.shapes) if (out.size < limit) out.add(typeof s === 'string' ? s : JSON.stringify(s))
  return [...out]
}
