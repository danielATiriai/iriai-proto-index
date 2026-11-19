import type { Db } from '../db.js'
import type { EventDoc } from '../index/assemble.js'
import type { EventQuery } from '../query/params.js'
import type { SearchBackend } from './backend.js'
import { queryOfShape, storedShapes } from './stats.js'

/** First results compared per shape (spec 6.4, verifying). */
export const COMPARED = 50
/** Fetched per backend, so a tie group that crosses position 50 can be compared whole. */
const FETCHED = 200
/** Distances closer than this count as tied; events this close to a radius or box edge are ignored. */
const TIE_M = 1
const RETRIES = 3
const BUILT_IN = 50
const R = 6371008.7714
const M_PER_DEG = 111195

function arc(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const rad = Math.PI / 180
  const h = Math.sin(((lat2 - lat1) * rad) / 2) ** 2 + Math.cos(lat1 * rad) * Math.cos(lat2 * rad) * Math.sin(((lon2 - lon1) * rad) / 2) ** 2
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(h)))
}

/** Whether an event lies within 1 m of the query's radius or box edge, where the backends may disagree (spec 4.4). */
function nearEdge(q: EventQuery, doc: EventDoc): boolean {
  const p = doc.point
  if (!p) return false
  if (q.radiusMeters !== undefined && q.latitude !== undefined) {
    if (Math.abs(arc(q.latitude, q.longitude!, p.lat, p.lon) - q.radiusMeters) < TIE_M) return true
  }
  if (q.box) {
    const lonM = M_PER_DEG * Math.cos((p.lat * Math.PI) / 180)
    const dLon = (a: number, b: number) => Math.min(Math.abs(a - b), 360 - Math.abs(a - b))
    if (
      Math.abs(p.lat - q.box.south) * M_PER_DEG < TIE_M ||
      Math.abs(p.lat - q.box.north) * M_PER_DEG < TIE_M ||
      dLon(p.lon, q.box.west) * lonM < TIE_M ||
      dLon(p.lon, q.box.east) * lonM < TIE_M
    ) {
      return true
    }
  }
  return false
}

/**
 * Compares the first 50 results of two backends for one query: the same
 * events in the same order, where results whose distances differ by under
 * 1 m count as tied and may be in either order, a tie group that crosses
 * position 50 is compared whole as a set, and events within 1 m of a radius
 * or box edge are ignored. Returns the uris involved in any difference.
 */
export async function differences(a: SearchBackend, b: SearchBackend, q: EventQuery): Promise<string[]> {
  const [left, right] = [await firstResults(a, q), await firstResults(b, q)]
  const keep = (docs: EventDoc[]) => docs.filter((d) => !nearEdge(q, d))
  const l = keep(left)
  const r = keep(right)
  const key = (d: EventDoc): number | null =>
    q.sort === 'distance' && d.point ? arc(q.latitude!, q.longitude!, d.point.lat, d.point.lon) : null
  const differ = new Set<string>()
  let i = 0
  while (i < Math.min(COMPARED, Math.max(l.length, r.length))) {
    // A group: from position i, every following result tied with the previous one.
    let j = i + 1
    if (q.sort === 'distance') {
      const k = (n: number) => (n < l.length ? key(l[n]) : null)
      if (k(i) === null) j = l.length
      else while (j < l.length && k(j) !== null && Math.abs(k(j)! - k(j - 1)!) < TIE_M) j++
    }
    const capped = j >= FETCHED && Math.max(left.length, right.length) >= FETCHED
    const ls = new Set(l.slice(i, j).map((d) => d.uri))
    const rs = new Set(r.slice(i, j).map((d) => d.uri))
    for (const u of ls) if (!rs.has(u)) differ.add(u)
    for (const u of rs) if (!ls.has(u)) differ.add(u)
    // A tie group longer than what was fetched cannot be shown equal.
    if (capped) for (const u of ls) differ.add(u)
    i = j
  }
  return [...differ]
}

async function firstResults(backend: SearchBackend, q: EventQuery): Promise<EventDoc[]> {
  const snap = await backend.openSnapshot()
  try {
    return (await backend.page(snap, { ...q, limit: FETCHED }, undefined, FETCHED)).hits.map((h) => h.doc)
  } finally {
    void backend.closeSnapshot(snap).catch(() => {})
  }
}

/**
 * The verifying check's query comparison (spec 6.4): for each stored query
 * shape, or 50 built-in shapes when fewer are stored, the first 50 results of
 * both backends must agree. A difference involving an event that changed
 * during the check is retried, up to 3 times; any other is a failure.
 */
export async function backendsAgree(
  db: Db,
  postgres: SearchBackend,
  elastic: SearchBackend,
): Promise<{ ok: true; shapes: number } | { ok: false; note: string }> {
  const stored = await storedShapes(db, 200)
  const shapes = stored.length >= BUILT_IN ? stored.map((s) => queryOfShape(s, COMPARED)) : [...stored.map((s) => queryOfShape(s, COMPARED)), ...(await builtInShapes(db))]
  for (const q of shapes) {
    for (let attempt = 0; ; attempt++) {
      const w = await sequenceNow(db)
      const differ = await differences(postgres, elastic, q)
      if (differ.length === 0) break
      const changed = await changedSince(db, differ, w)
      if (changed && attempt < RETRIES) continue
      return { ok: false, note: `the backends disagree on ${JSON.stringify(q)}: ${differ.slice(0, 5).join(', ')}` }
    }
  }
  return { ok: true, shapes: shapes.length }
}

async function sequenceNow(db: Db): Promise<string> {
  return (await db.query<{ w: string }>(`select (case when is_called then last_value else 0 end)::text as w from event_doc_version`)).rows[0].w
}

async function changedSince(db: Db, uris: string[], w: string): Promise<boolean> {
  const res = await db.query(
    `select 1 from event_docs where uri = any($1) and (version > $2::bigint or closed_version > $2::bigint)
     union all select 1 from es_pending where uri = any($1) and version > $2::bigint limit 1`,
    [uris, w],
  )
  return (res.rowCount ?? 0) > 0
}

/** 50 shapes around up to 10 events spread through the index: distance, radius, radius and a week, box by start, start from a time. */
async function builtInShapes(db: Db): Promise<EventQuery[]> {
  const res = await db.query<{ lat: number | null; lon: number | null; starts_at: Date | null }>(
    `select lat, lon, starts_at from (
       select lat, lon, starts_at, row_number() over (order by uri) as n, count(*) over () as total
       from event_docs where closed_version is null) e
     where (n - 1) % greatest(1, total / 10) = 0 order by n limit 10`,
  )
  const out: EventQuery[] = [{ sort: 'uri', limit: COMPARED }]
  for (const r of res.rows) {
    const start = r.starts_at ?? new Date('2026-01-01T00:00:00Z')
    const weekEnd = new Date(start.getTime() + 7 * 86_400_000).toISOString()
    out.push({ sort: 'startsAt', windowStart: start.toISOString(), limit: COMPARED })
    if (r.lat === null || r.lon === null) continue
    const point = { latitude: r.lat, longitude: r.lon }
    out.push(
      { ...point, sort: 'distance', limit: COMPARED },
      { ...point, radiusMeters: 2_000, sort: 'distance', limit: COMPARED },
      { ...point, radiusMeters: 10_000, windowStart: start.toISOString(), windowEnd: weekEnd, sort: 'distance', limit: COMPARED },
      {
        box: { west: Math.max(-180, r.lon - 0.05), south: Math.max(-90, r.lat - 0.05), east: Math.min(180, r.lon + 0.05), north: Math.min(90, r.lat + 0.05) },
        sort: 'startsAt',
        limit: COMPARED,
      },
    )
  }
  return out.slice(0, BUILT_IN)
}
