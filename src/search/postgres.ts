import pg from 'pg'
import type { Db } from '../db.js'
import type { EventDoc } from '../index/assemble.js'
import type { EventQuery } from '../query/params.js'
import type { Hit, SearchBackend } from './backend.js'

/** Taken exclusively by every write and shared by every snapshot (spec 4.2, 4.3). */
export const WRITE_LOCK = 7140015
/** Closed rows outlive any paging session (one hour) by a minute, then go. */
export const CLOSED_KEEP_MINUTES = 61
/** Meters per degree of latitude on the mean radius: a cheap prefilter before the exact distance. */
const METERS_PER_DEGREE = 111195
const SEARCH_TIMEOUT_MS = 2_000
const SEARCH_POOL_SIZE = 5

/**
 * The pool searches run on: five connections and a 2-second statement
 * timeout, so a slow search cannot take ingestion's connections (spec 4.4).
 */
export function searchPool(databaseUrl: string, log: (msg: string) => void = console.error): Db {
  const pool = new pg.Pool({ connectionString: databaseUrl, max: SEARCH_POOL_SIZE, statement_timeout: SEARCH_TIMEOUT_MS })
  pool.on('error', (err) => log(`postgres (search): idle client lost: ${err.message}`))
  return pool
}

/**
 * Event search on Postgres (spec 4). Writes run on `db`, reads on `reads`
 * (the search pool in the query service). Every write keeps the earlier row,
 * closed, so a snapshot number gives each paging session a fixed view.
 */
export class PostgresBackend implements SearchBackend {
  readonly name = 'postgres' as const

  constructor(
    private db: Db,
    private reads: Db = db,
  ) {}

  /** Replaces the current row for doc.uri; returns the write's version. */
  async put(doc: EventDoc): Promise<bigint> {
    return this.write(doc.uri, async (client, version) => {
      await client.query(
        `insert into event_docs (uri, version, did, event_uri, doc, lat, lon, starts_at, ends_at, time_valid)
         values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        [
          doc.uri,
          version,
          doc.did,
          doc.eventUri,
          JSON.stringify(doc),
          doc.point?.lat ?? null,
          doc.point?.lon ?? null,
          doc.startsAt ?? null,
          doc.endsAt ?? null,
          doc.timeValid,
        ],
      )
    })
  }

  /** Closes the current row for uri, if any; returns the write's version. */
  async remove(uri: string): Promise<bigint> {
    return this.write(uri, async () => {})
  }

  /**
   * One transaction: the write lock first, then a new number (a number taken
   * before the lock could be lower than a snapshot read in between), then the
   * current row is closed with it and `insert` adds the new one.
   */
  private async write(uri: string, insert: (client: pg.PoolClient, version: string) => Promise<void>): Promise<bigint> {
    const client = await this.db.connect()
    try {
      await client.query('begin')
      await client.query('select pg_advisory_xact_lock($1)', [WRITE_LOCK])
      const version = (await client.query<{ v: string }>(`select nextval('event_doc_version')::text as v`)).rows[0].v
      await client.query(
        `update event_docs set closed_version = $2, closed_at = clock_timestamp()
         where uri = $1 and closed_version is null`,
        [uri, version],
      )
      await insert(client, version)
      await client.query('commit')
      return BigInt(version)
    } catch (err) {
      await client.query('rollback').catch(() => {})
      throw err
    } finally {
      client.release()
    }
  }

  async get(uri: string): Promise<EventDoc | undefined> {
    const res = await this.reads.query<{ doc: EventDoc }>(
      'select doc from event_docs where uri = $1 and closed_version is null',
      [uri],
    )
    return res.rows[0]?.doc
  }

  /**
   * The snapshot number W: the shared lock waits for any write in flight, so
   * every write that finished has a number at most W and every later one a
   * number above it. No clock is involved.
   */
  async openSnapshot(): Promise<string> {
    const client = await this.reads.connect()
    try {
      await client.query('begin')
      await client.query('select pg_advisory_xact_lock_shared($1)', [WRITE_LOCK])
      const res = await client.query<{ w: string }>(
        `select (case when is_called then last_value else 0 end)::text as w from event_doc_version`,
      )
      await client.query('commit')
      return res.rows[0].w
    } catch (err) {
      await client.query('rollback').catch(() => {})
      throw err
    } finally {
      client.release()
    }
  }

  async page(snapshot: string, q: EventQuery, after: unknown[] | undefined, size: number): Promise<{ hits: Hit[]; snapshot: string }> {
    if (!/^\d+$/.test(snapshot)) throw new Error('not a Postgres snapshot')
    const { text, values } = pageSql(snapshot, q, after, size)
    const res = await this.reads.query<{ doc: EventDoc; k: number | Date | null; uri: string }>(text, values)
    const hits = res.rows.map((r) => ({
      doc: r.doc,
      sort: q.sort === 'uri' ? [r.uri] : [r.k instanceof Date ? r.k.toISOString() : r.k, r.uri],
    }))
    return { hits, snapshot }
  }

  async closeSnapshot(): Promise<void> {}

  async count(): Promise<number> {
    const res = await this.reads.query<{ n: string }>('select count(*) as n from event_docs where closed_version is null')
    return Number(res.rows[0].n)
  }

  /** Deletes rows closed more than 61 minutes ago; no session can still read them. */
  async pruneClosed(): Promise<number> {
    const res = await this.db.query(
      `delete from event_docs where closed_version is not null and closed_at < now() - make_interval(mins => $1)`,
      [CLOSED_KEEP_MINUTES],
    )
    return res.rowCount ?? 0
  }

  /** Every current document, for the verifying comparison. */
  async currentDocs(): Promise<Map<string, EventDoc>> {
    const res = await this.db.query<{ uri: string; doc: EventDoc }>('select uri, doc from event_docs where closed_version is null')
    return new Map(res.rows.map((r) => [r.uri, r.doc]))
  }
}

/**
 * The page query (spec 4.3, 4.4): rows visible at snapshot W, from a branch
 * over current rows and one over closed rows so each uses its partial
 * indexes, after the cursor's position, in the requested order.
 */
export function pageSql(snapshot: string, q: EventQuery, after: unknown[] | undefined, size: number): { text: string; values: unknown[] } {
  const values: unknown[] = []
  const p = (v: unknown, cast = '') => {
    values.push(v)
    return `$${values.length}${cast}`
  }
  const w = p(snapshot, '::bigint')

  const filters: string[] = []
  const origin = q.latitude !== undefined ? { lat: p(q.latitude, '::float8'), lon: p(q.longitude, '::float8') } : undefined
  const distance = origin ? `arc_m(${origin.lat}, ${origin.lon}, lat, lon)` : undefined

  if (q.radiusMeters !== undefined && origin) {
    const r = p(q.radiusMeters, '::float8')
    const lat = q.latitude!
    filters.push(
      `lat between ${p(lat - q.radiusMeters / METERS_PER_DEGREE, '::float8')} and ${p(lat + q.radiusMeters / METERS_PER_DEGREE, '::float8')}`,
      `${distance} <= ${r}`,
    )
  }
  if (q.box) {
    const { west, south, east, north } = q.box
    filters.push(`lat between ${p(south, '::float8')} and ${p(north, '::float8')}`)
    filters.push(
      west > east
        ? `(lon >= ${p(west, '::float8')} or lon <= ${p(east, '::float8')})`
        : `(lon between ${p(west, '::float8')} and ${p(east, '::float8')})`,
    )
  }
  const windowed = Boolean(q.windowStart || q.windowEnd)
  if (windowed) {
    filters.push(
      `time_valid and event_span(starts_at, ends_at) && tstzrange(${p(q.windowStart ?? null, '::timestamptz')}, ${p(q.windowEnd ?? null, '::timestamptz')}, '[)')`,
    )
  }
  if (q.author) filters.push(`did = any(${p(q.author, '::text[]')})`)
  if (q.eventUri) filters.push(`event_uri = ${p(q.eventUri)}`)

  const key = q.sort === 'distance' ? distance! : q.sort === 'startsAt' ? 'starts_at' : undefined
  if (after) {
    const afterUri = p(String(after[after.length - 1]))
    if (!key) filters.push(`uri > ${afterUri}`)
    else if (after[0] === null || after[0] === undefined) filters.push(`(${key} is null and uri > ${afterUri})`)
    else {
      const k = p(after[0], q.sort === 'distance' ? '::float8' : '::timestamptz')
      filters.push(`(${key} > ${k} or (${key} = ${k} and uri > ${afterUri}) or ${key} is null)`)
    }
  }

  const select = `select uri, doc, ${key ?? 'null'} as k from event_docs`
  const where = (visible: string) => [visible, ...filters].join(' and ')
  const current = `closed_version is null and version <= ${w}`
  const closed = `closed_version is not null and version <= ${w} and closed_version > ${w}`
  const order = key ? 'order by k asc nulls last, uri asc' : 'order by uri asc'
  const limit = `limit ${p(size, '::int')}`

  // A time window gathers its matches first: sorting first would walk the
  // start-time index from the oldest event (spec 4.4).
  const text = windowed
    ? `with matched as materialized (${select} where ${where(current)} union all ${select} where ${where(closed)})
       select uri, doc, k from matched ${order} ${limit}`
    : `select uri, doc, k from ((${select} where ${where(current)} ${order} ${limit})
       union all (${select} where ${where(closed)} ${order} ${limit})) as matched ${order} ${limit}`
  return { text, values }
}
