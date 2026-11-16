import { errors } from '@elastic/elasticsearch'
import { AtUri, isValidDid } from '@atproto/syntax'
import type { Db } from '../db.js'
import { eventSidecarNsid } from '../contracts/contracts.js'
import type { EventDoc } from '../index/assemble.js'
import { getDoc, type Elastic } from '../index/elastic.js'
import { getRecord, incompleteAmong } from '../ingest/store.js'
import { CursorError, bindingHash, decodeCursor, encodeCursor } from './cursor.js'
import { InvalidRequest, bindingOf, type EventQuery } from './params.js'
import { buildSearch } from './search.js'

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
  es: Elastic
  index: string
  serviceId: string
  cursorKey: Buffer
  /** Paging-session limit for this caller: `wait` before a first page, `opened` when it returns a cursor. */
  sessions?: { wait: () => number; opened: () => void }
}

const KEEP_ALIVE = '15m'

function view(doc: EventDoc): EventView {
  return { ...doc.view, version: { eventCid: doc.eventCid, sidecarCid: doc.sidecarCid } }
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

  const doc = await unavailableOnFailure(() => getDoc(deps.es, deps.index, uri.toString()))
  if (doc) {
    const v = view(doc)
    return { value: v, version: v.version }
  }
  const sidecar = await unavailableOnFailure(() => getRecord(deps.db, uri.toString()))
  if (!sidecar || sidecar.deleted) throw new XrpcError(404, 'SidecarNotFound', 'no indexed sidecar has this URI')
  throw new XrpcError(404, 'EventNotFound', 'the sidecar has no usable community event')
}

/**
 * app.iriai.proto.queryEvents. The first page opens a point-in-time snapshot;
 * each cursor carries it with the sort values of the last examined candidate,
 * so new records and edits appear only in a fresh query. Candidates that
 * stopped being complete since the snapshot (deleted, orphaned, suppressed)
 * are dropped, and the cursor still moves past them, so a page can be short
 * or empty while a cursor is returned.
 */
export async function queryEvents(deps: QueryDeps, q: EventQuery, now = Date.now()): Promise<{ events: EventView[]; cursor?: string }> {
  const expect = { svc: deps.serviceId, bind: bindingHash(bindingOf(q)) }
  let pit: string
  let after: unknown[] | undefined
  let created: number
  if (q.cursor) {
    const state = decodeCursor(q.cursor, deps.cursorKey, expect, now)
    pit = state.pit
    after = state.after
    created = state.created
  } else {
    const wait = deps.sessions?.wait() ?? 0
    if (wait > 0) throw new XrpcError(429, 'RateLimitExceeded', 'too many paging sessions from this caller', wait)
    pit = (await unavailableOnFailure(() => deps.es.openPointInTime({ index: deps.index, keep_alive: KEEP_ALIVE }))).id
    created = now
  }

  const { query, sort } = buildSearch(q)
  let res
  try {
    res = await deps.es.search<EventDoc>({
      pit: { id: pit, keep_alive: KEEP_ALIVE },
      query,
      sort: sort as never,
      size: q.limit + 1,
      ...(after ? { search_after: after as never } : {}),
      track_total_hits: false,
    })
  } catch (err) {
    if (q.cursor && isMissingSnapshot(err)) {
      throw new CursorError('CursorExpired', 'the paging snapshot has expired; restart without a cursor')
    }
    throw toUnavailable(err)
  }

  const hits = res.hits.hits
  const examined = hits.slice(0, q.limit)
  const more = hits.length > q.limit
  const snapshot = res.pit_id ?? pit
  let suppressed: Set<string>
  try {
    suppressed = await incompleteAmong(
      deps.db,
      examined.map((h) => h._source!.uri),
    )
  } catch (err) {
    if (!q.cursor) await deps.es.closePointInTime({ id: snapshot }).catch(() => {})
    throw toUnavailable(err)
  }
  const events = examined.filter((h) => !suppressed.has(h._source!.uri)).map((h) => view(h._source!))

  if (!more) {
    await deps.es.closePointInTime({ id: snapshot }).catch(() => {})
    return { events }
  }
  const last = examined[examined.length - 1]
  const cursor = encodeCursor(
    { ...expect, pit: snapshot, after: last.sort ?? [], created, issued: now },
    deps.cursorKey,
  )
  if (!q.cursor) deps.sessions?.opened()
  return { events, cursor }
}

function isMissingSnapshot(err: unknown): boolean {
  if (!(err instanceof errors.ResponseError)) return false
  const text = JSON.stringify(err.body ?? '')
  return err.statusCode === 404 || text.includes('search_context_missing_exception') || text.includes('No search context found')
}

/** Elasticsearch or Postgres failing is the index being unavailable; an Elasticsearch 4xx stays an error. */
function toUnavailable(err: unknown): unknown {
  if (err instanceof errors.ResponseError && err.statusCode && err.statusCode < 500 && err.statusCode !== 429) return err
  return new XrpcError(503, 'ServiceUnavailable', 'the event index is unavailable')
}

async function unavailableOnFailure<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn()
  } catch (err) {
    throw toUnavailable(err)
  }
}
