import { AtUri, isValidDid } from '@atproto/syntax'
import type { Db } from '../db.js'
import { eventSidecarNsid } from '../contracts/contracts.js'
import type { EventDoc } from '../index/assemble.js'
import { getRecord, incompleteAmong } from '../ingest/store.js'
import { CursorError, bindingHash, decodeCursor, encodeCursor } from './cursor.js'
import { InvalidRequest, bindingOf, type EventQuery } from './params.js'
import type { CallOptions, Hit, SearchBackend } from '../search/backend.js'
import { route, type ControlRow, type Route, type SearchControl } from '../search/control.js'
import { isTimeout, type SearchStats } from '../search/stats.js'

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

/**
 * The search backend and what decides whether it may serve (spec 5.2). From
 * PR 2 Elasticsearch is not written, so it never serves: every new session
 * and getEvent goes to a verified Postgres, and nothing is sent to
 * Elasticsearch, so it can sleep.
 */
export interface Search {
  postgres: SearchBackend
  control: SearchControl
  /** Daily stats for the scale check (spec 6.1); absent in tests that do not need them. */
  stats?: SearchStats
  /** The last backend chosen for a new session or getEvent, and why, for health. */
  last?: Route & { at: string }
}

function view(doc: EventDoc): EventView {
  return { ...doc.view, version: { eventCid: doc.eventCid, sidecarCid: doc.sidecarCid } }
}

/**
 * Routes a new session or a getEvent (spec 5.2): Postgres if verified, else
 * ServiceUnavailable. If search_control cannot be read, Postgres counts as
 * unverified.
 */
export async function routed<T>(search: Search, run: (backend: SearchBackend, opts: CallOptions) => Promise<T>, record = true): Promise<T> {
  // Read fresh every time (one row by key): a cleared verification takes effect at once.
  const chosen = route(await verification(search))
  if (!chosen) throw new XrpcError(503, 'ServiceUnavailable', 'the event index is unavailable: Postgres is not verified yet')
  if (record) search.last = { ...chosen, at: new Date().toISOString() }
  return unavailableOnFailure(() => run(search.postgres, {}))
}

/** search_control read fresh (SearchControl bounds every read to 500 ms): a read that fails or stalls counts as unverified. */
export async function verification(search: Pick<Search, 'control'>): Promise<Pick<ControlRow, 'pgVerified'>> {
  return search.control.refresh().catch(() => ({ pgVerified: false }))
}

/** What health reports as the index: a first page's routing, with a count in place of the search. */
export async function probeNewSession(search: Search): Promise<{ ok: boolean; backend?: string; events?: number }> {
  try {
    return await routed(search, async (b, opts) => ({ ok: true, backend: b.name, events: await b.count(opts) }), false)
  } catch {
    return { ok: false }
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
  search.stats?.call()

  if (q.cursor) {
    const state = decodeCursor(q.cursor, deps.cursorKey, expect, now)
    // Cursors from before the backend field are Elasticsearch's. Elasticsearch
    // has issued none since Nov 17 (SEARCH_BACKEND_FORCE=postgres), so every
    // one is past its hour; it is never sent there, which would wake it.
    if ((state.be ?? 'elastic') !== 'postgres') throw new CursorError('CursorExpired', 'the paging snapshot has expired; restart without a cursor')
    backend = search.postgres
    created = state.created
    try {
      ;({ hits, snapshot } = await timed(search, () => backend.page(state.pit, q, state.after, q.limit + 1)))
    } catch (err) {
      throw toUnavailable(err)
    }
  } else {
    const wait = deps.sessions?.wait() ?? 0
    if (wait > 0) throw new XrpcError(429, 'RateLimitExceeded', 'too many paging sessions from this caller', wait)
    created = now
    // Timed whole: opening the snapshot waits for a search connection.
    ;({ backend, hits, snapshot } = await routed(search, (b, opts) =>
      timed(search, async () => {
        const snap = await b.openSnapshot(opts)
        try {
          return { backend: b, ...(await b.page(snap, q, undefined, q.limit + 1, opts)) }
        } catch (err) {
          release(b, snap)
          throw err
        }
      }),
    ))
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

/** Times a search step for the daily stats (spec 6.1); one that ran out of time counts as 2 s and a timeout. */
async function timed<T>(search: Search, step: () => Promise<T>): Promise<T> {
  const started = performance.now()
  try {
    const out = await step()
    search.stats?.search(performance.now() - started)
    return out
  } catch (err) {
    if (isTimeout(err)) search.stats?.search(2_000, true)
    throw err
  }
}

/**
 * Closes a snapshot without waiting: closing never holds up an answer.
 */
function release(backend: SearchBackend, snapshot: string): void {
  void backend.closeSnapshot(snapshot).catch(() => {})
}

/** Postgres failing is the index being unavailable. */
function toUnavailable(err: unknown): unknown {
  if (err instanceof XrpcError) return err
  return new XrpcError(503, 'ServiceUnavailable', 'the event index is unavailable')
}

async function unavailableOnFailure<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn()
  } catch (err) {
    throw toUnavailable(err)
  }
}
