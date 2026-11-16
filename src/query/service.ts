import { errors } from '@elastic/elasticsearch'
import { AtUri, isValidDid } from '@atproto/syntax'
import type { Db } from '../db.js'
import { eventSidecarNsid } from '../contracts/contracts.js'
import type { EventDoc } from '../index/assemble.js'
import { getRecord, incompleteAmong } from '../ingest/store.js'
import { CursorError, bindingHash, decodeCursor, encodeCursor } from './cursor.js'
import { InvalidRequest, bindingOf, type EventQuery } from './params.js'
import type { BackendName, CallOptions, Hit, SearchBackend } from '../search/backend.js'
import { route, type ControlRow, type Route, type SearchControl } from '../search/control.js'

export class XrpcError extends Error {
  constructor(
    readonly status: number,
    readonly error: string,
    message: string,
    readonly retryAfter?: number,
  ) {
    super(message)
  }
}

export interface EventView {
  uri: string
  eventUri: string
  event: Record<string, unknown>
  location: Record<string, unknown>
  version: { eventCid: string; sidecarCid: string }
}

export interface QueryDeps {
  db: Db
  search: Search
  serviceId: string
  cursorKey: Buffer
  /** Paging-session limit for this caller: `wait` before a first page, `opened` when it returns a cursor. */
  sessions?: { wait: () => number; opened: () => void }
}

/** Both backends and what routes between them (spec 5.2). */
export interface Search {
  elastic: SearchBackend
  postgres: SearchBackend
  control: SearchControl
  /** SEARCH_BACKEND_FORCE; elastic unless set. */
  force: BackendName
  /** The last backend chosen for a new session or getEvent, and why, for health. */
  last?: Route & { at: string }
}

/** An Elasticsearch first page (or getEvent) that has not answered in this long falls back (spec 5.2). */
export const ELASTIC_FIRST_MS = 2_000

function view(doc: EventDoc): EventView {
  return { ...doc.view, version: { eventCid: doc.eventCid, sidecarCid: doc.sidecarCid } }
}

/**
 * Routes a new session or a getEvent: the chosen backend if it may serve,
 * else Postgres if verified, else ServiceUnavailable. Once Postgres is
 * verified, Elasticsearch failing within 2 seconds counts as not serving and
 * Postgres takes the request; until then Elasticsearch behaves exactly as
 * today, with its own timeouts. If search_control cannot be read, Postgres
 * counts as unverified, so Elasticsearch still serves as before.
 */
async function routed<T>(search: Search, run: (backend: SearchBackend, opts: CallOptions) => Promise<T>): Promise<T> {
  // Read fresh every time (one row by key): a cleared verification takes effect at once.
  const row = await verification(search)
  const chosen = route(search.force, row)
  if (!chosen) throw new XrpcError(503, 'ServiceUnavailable', 'the event index is unavailable: Postgres is not verified yet')
  if (chosen.backend === 'postgres') {
    search.last = { ...chosen, at: new Date().toISOString() }
    return unavailableOnFailure(() => run(search.postgres, {}))
  }
  try {
    const result = await run(search.elastic, row.pgVerified ? { signal: AbortSignal.timeout(ELASTIC_FIRST_MS) } : {})
    search.last = { ...chosen, at: new Date().toISOString() }
    return result
  } catch (err) {
    if (!row.pgVerified) throw toUnavailable(err)
    search.last = { backend: 'postgres', reason: 'Elasticsearch failed or took over 2 s; Postgres verified', at: new Date().toISOString() }
    return unavailableOnFailure(() => run(search.postgres, {}))
  }
}

/** A control read that stalls is cut off after this long and counts as unverified. */
export const CONTROL_READ_MS = 500

/**
 * search_control read fresh, bounded to 500 ms: a read that fails or stalls
 * counts as Postgres unverified, so Elasticsearch still serves as before.
 */
export async function verification(search: Pick<Search, 'control'>): Promise<Pick<ControlRow, 'pgVerified'>> {
  let timer: NodeJS.Timeout | undefined
  const unverified = { pgVerified: false }
  try {
    return await Promise.race([
      search.control.refresh().catch(() => unverified),
      new Promise<typeof unverified>((resolve) => {
        timer = setTimeout(() => resolve(unverified), CONTROL_READ_MS)
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

/** app.iriai.proto.getEvent: one complete event by its sidecar AT-URI. */
export async function getEvent(deps: QueryDeps, uriParam: string | null): Promise<{ value: EventView; version: EventView['version'] }> {
  if (!uriParam) throw new InvalidRequest('uri is required')
  let uri: AtUri
  try {
    uri = new AtUri(uriParam)
  } catch {
    throw new InvalidRequest('uri must be an AT-URI')
  }
  if (!isValidDid(uri.host)) throw new InvalidRequest('uri must use a DID, not a handle')
  if (uri.collection !== eventSidecarNsid || !uri.rkey) throw new InvalidRequest(`uri must name a ${eventSidecarNsid} record`)

  const doc = await routed(deps.search, (backend, opts) => backend.get(uri.toString(), opts))
  if (doc) {
    const v = view(doc)
    return { value: v, version: v.version }
  }
  const sidecar = await unavailableOnFailure(() => getRecord(deps.db, uri.toString()))
  if (!sidecar || sidecar.deleted) throw new XrpcError(404, 'SidecarNotFound', 'no indexed sidecar has this URI')
  throw new XrpcError(404, 'EventNotFound', 'the sidecar has no usable community event')
}

/**
 * app.iriai.proto.queryEvents. The first page opens a snapshot on the backend
 * the session is routed to; each cursor carries that backend, the snapshot
 * and the sort values of the last examined candidate, so new records and
 * edits appear only in a fresh query. Candidates that stopped being complete
 * since the snapshot (deleted, orphaned, suppressed) are dropped, and the
 * cursor still moves past them, so a page can be short or empty while a
 * cursor is returned.
 */
export async function queryEvents(deps: QueryDeps, q: EventQuery, now = Date.now()): Promise<{ events: EventView[]; cursor?: string }> {
  const expect = { svc: deps.serviceId, bind: bindingHash(bindingOf(q)) }
  const { search } = deps
  let backend: SearchBackend
  let created: number
  let hits: Hit[]
  let snapshot: string

  if (q.cursor) {
    const state = decodeCursor(q.cursor, deps.cursorKey, expect, now)
    // Cursors from before the backend field are Elasticsearch's.
    backend = (state.be ?? 'elastic') === 'postgres' ? search.postgres : search.elastic
    created = state.created
    if (backend.name === 'elastic' && !(await unavailableOnFailure(() => search.control.elasticCursorsOpen()))) {
      throw new CursorError('CursorExpired', 'the paging snapshot has expired; restart without a cursor')
    }
    try {
      ;({ hits, snapshot } = await backend.page(state.pit, q, state.after, q.limit + 1))
    } catch (err) {
      if (isMissingSnapshot(err)) throw new CursorError('CursorExpired', 'the paging snapshot has expired; restart without a cursor')
      throw toUnavailable(err)
    }
  } else {
    const wait = deps.sessions?.wait() ?? 0
    if (wait > 0) throw new XrpcError(429, 'RateLimitExceeded', 'too many paging sessions from this caller', wait)
    created = now
    ;({ backend, hits, snapshot } = await routed(search, async (b, opts) => {
      if (b.name === 'elastic') await search.control.coverElasticSession()
      const snap = await b.openSnapshot(opts)
      try {
        return { backend: b, ...(await b.page(snap, q, undefined, q.limit + 1, opts)) }
      } catch (err) {
        // Not awaited: closing must not hold up the fallback (spec 5.2).
        release(b, snap)
        throw err
      }
    }))
  }

  const examined = hits.slice(0, q.limit)
  const more = hits.length > q.limit
  let suppressed: Set<string>
  try {
    suppressed = await incompleteAmong(
      deps.db,
      examined.map((h) => h.doc.uri),
    )
  } catch (err) {
    if (!q.cursor) release(backend, snapshot)
    throw toUnavailable(err)
  }
  const events = examined.filter((h) => !suppressed.has(h.doc.uri)).map((h) => view(h.doc))

  if (!more) {
    release(backend, snapshot)
    return { events }
  }
  const last = examined[examined.length - 1]
  const cursor = encodeCursor(
    { ...expect, be: backend.name, pit: snapshot, after: last.sort, created, issued: now },
    deps.cursorKey,
  )
  if (!q.cursor) deps.sessions?.opened()
  return { events, cursor }
}

/**
 * Closes a snapshot without waiting: closing never holds up an answer, and a
 * snapshot that fails to close expires on its own (Elasticsearch's keep-alive).
 */
function release(backend: SearchBackend, snapshot: string): void {
  void backend.closeSnapshot(snapshot).catch(() => {})
}

function isMissingSnapshot(err: unknown): boolean {
  if (!(err instanceof errors.ResponseError)) return false
  const text = JSON.stringify(err.body ?? '')
  return err.statusCode === 404 || text.includes('search_context_missing_exception') || text.includes('No search context found')
}

/** An Elasticsearch 4xx other than 429: a fault in the request, not the index being down. */
function isClientError(err: unknown): boolean {
  return err instanceof errors.ResponseError && !!err.statusCode && err.statusCode < 500 && err.statusCode !== 429
}

/** Elasticsearch or Postgres failing is the index being unavailable; an Elasticsearch 4xx stays an error. */
function toUnavailable(err: unknown): unknown {
  if (err instanceof XrpcError) return err
  // A missing index (a fresh environment before the ingester's first write) is the index being unavailable.
  if (isClientError(err) && !JSON.stringify((err as errors.ResponseError).body ?? '').includes('index_not_found_exception')) return err
  return new XrpcError(503, 'ServiceUnavailable', 'the event index is unavailable')
}

async function unavailableOnFailure<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn()
  } catch (err) {
    throw toUnavailable(err)
  }
}
