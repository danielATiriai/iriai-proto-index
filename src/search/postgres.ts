import pg from 'pg'
import { checkout, type Db } from '../db.js'
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
  // A search waits at most 2 s for a connection as well, so requests cannot
  // pile up behind five slow searches.
  const pool = new pg.Pool({
    connectionString: databaseUrl,
    max: SEARCH_POOL_SIZE,
    statement_timeout: SEARCH_TIMEOUT_MS,
    connectionTimeoutMillis: SEARCH_TIMEOUT_MS,
  })
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

  /**
   * Writes uri's projection as `assemble` finds it inside the write's
   * transaction, under the write lock: what it reads is the stored records as
   * of that moment, so a projection that started earlier can never be written
   * over a later one, whichever ingester made each (two run at once while a
   * deploy overlaps). Returns what was assembled.
   */
  async project<T extends { doc?: EventDoc }>(uri: string, assemble: (q: Db) => Promise<T>): Promise<T> {
    let out: T | undefined
    await this.write(
      uri,
      async (client, version) => {
        if (out!.doc) await insertDoc(client, version, out!.doc)
      },
      // Read before event_docs is touched, so the transaction locks tables in
      // the migration's order (records, accounts, then event_docs).
      async (client) => {
        out = await assemble(client as unknown as Db)
      },
    )
    return out!
  }

  /** Replaces the current row for doc.uri; returns the write's version. */
  async put(doc: EventDoc): Promise<bigint> {
    return this.write(doc.uri, (client, version) => insertDoc(client, version, doc))
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
  private async write(
    uri: string,
    insert: (client: pg.PoolClient, version: string) => Promise<void>,
    prepare?: (client: pg.PoolClient) => Promise<void>,
  ): Promise<bigint> {
    const client = await checkout(this.db)
    try {
      await client.query('begin')
      await client.query('select pg_advisory_xact_lock($1)', [WRITE_LOCK])
      await prepare?.(client)
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
    const client = await checkout(this.reads)
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
      sort: q.sort === 'uri' ? [r.uri] : [r.k === null ? null : Number(r.k), r.uri],
    }))
    return { hits, snapshot }
  }

  async closeSnapshot(): Promise<void> {}

  /** Current documents, after the same shared lock a first page's snapshot waits for. */
  async count(): Promise<number> {
    const client = await checkout(this.reads)
    try {
      await client.query('begin')
      await client.query('select pg_advisory_xact_lock_shared($1)', [WRITE_LOCK])
      const res = await client.query<{ n: string }>('select count(*) as n from event_docs where closed_version is null')
      await client.query('commit')
      return Number(res.rows[0].n)
    } catch (err) {
      await client.query('rollback').catch(() => {})
      throw err
    } finally {
      client.release()
    }
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

async function insertDoc(client: pg.PoolClient, version: string, doc: EventDoc): Promise<void> {
  await client.query(
    `insert into event_docs (uri, version, did, event_uri, doc, lat, lon, starts_at, ends_at, time_valid)
     values ($1, $2, $3, $4, $5, $6, $7, ${FROM_MS('$8')}, ${FROM_MS('$9')}, $10)`,
    [
      doc.uri,
      version,
      doc.did,
      doc.eventUri,
      JSON.stringify(doc),
      doc.point?.lat ?? null,
      doc.point?.lon ?? null,
      ms(doc.startsAt),
      ms(doc.endsAt),
      doc.timeValid,
    ],
  )
}

/** An ISO time as epoch milliseconds, for binding; undefined stays null. */
function ms(iso: string | undefined): string | null {
  if (iso === undefined) return null
  const t = Date.parse(iso)
  if (Number.isNaN(t)) throw new Error(`not a time: ${iso}`)
  return String(t)
}

/** SQL for a timestamptz from a bound epoch-milliseconds parameter (exact integer arithmetic). */
const FROM_MS = (param: string) => `(timestamptz 'epoch' + ${param}::bigint * interval '1 millisecond')`

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
  // The coordinate is bound only when a radius or distance order uses it:
  // Postgres refuses a parameter the statement never mentions.
  const usesOrigin = q.latitude !== undefined && (q.radiusMeters !== undefined || q.sort === 'distance')
  const distance = usesOrigin ? `arc_m(${p(q.latitude, '::float8')}, ${p(q.longitude, '::float8')}, lat, lon)` : undefined

  if (q.radiusMeters !== undefined && distance) {
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
      `time_valid and event_span(starts_at, ends_at) && tstzrange(${FROM_MS(p(ms(q.windowStart)))}, ${FROM_MS(p(ms(q.windowEnd)))}, '[)')`,
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
      const k = q.sort === 'distance' ? p(after[0], '::float8') : FROM_MS(p(after[0]))
      filters.push(`(${key} > ${k} or (${key} = ${k} and uri > ${afterUri}) or ${key} is null)`)
    }
  }

  // Start times travel as epoch milliseconds: exact, and valid for any year
  // the API accepts, including 0000, which timestamptz text input refuses.
  const keyOut = q.sort === 'startsAt' ? `(extract(epoch from starts_at) * 1000)::bigint` : key
  const select = `select uri, doc, ${keyOut ?? 'null'} as k from event_docs`
  const where = (visible: string) => [visible, ...filters].join(' and ')
  const current = `closed_version is null and version <= ${w}`
  const closed = `closed_version is not null and version <= ${w} and closed_version > ${w}`
  // Each branch orders by the column itself, so the current-rows branch can
  // walk the (starts_at, uri) index (closed rows have no start-time index and
  // are few); the merged rows are then ordered by the returned key.
  const branchOrder = key ? `order by ${key} asc nulls last, uri asc` : 'order by uri asc'
  const order = key ? 'order by k asc nulls last, uri asc' : 'order by uri asc'
  const limit = `limit ${p(size, '::int')}`

  // A time window gathers its matches first: sorting first would walk the
  // start-time index from the oldest event (spec 4.4).
  const text = windowed
    ? `with matched as materialized (${select} where ${where(current)} union all ${select} where ${where(closed)})
       select uri, doc, k from matched ${order} ${limit}`
    : `select uri, doc, k from ((${select} where ${where(current)} ${branchOrder} ${limit})
       union all (${select} where ${where(closed)} ${branchOrder} ${limit})) as matched ${order} ${limit}`
  return { text, values }
}
