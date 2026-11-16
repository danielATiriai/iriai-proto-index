import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import { readFileSync } from 'node:fs'
import { config, contractsRoot } from '../contracts/contracts.js'
import type { Db } from '../db.js'
import { route } from '../search/control.js'
import { CHECKPOINT } from '../ingest/ingester.js'
import { readCheckpoint } from '../ingest/store.js'
import { CursorError } from './cursor.js'
import { Limits } from './limits.js'
import { InvalidRequest, parseEventQuery } from './params.js'
import type { CallOptions, SearchBackend } from '../search/backend.js'
import { ELASTIC_FIRST_MS, XrpcError, getEvent, queryEvents, type QueryDeps, type Search } from './service.js'

const contracts = JSON.parse(readFileSync(`${contractsRoot}SOURCE.json`, 'utf8')) as { tag: string; sha256: string }
const ACCESS = 'Interim: open access with no key and per-caller rate limits (D-14). A project key and higher limits come with MVP.'

export interface ServerDeps {
  db: Db
  search: Search
  serviceId: string
  cursorKey: Buffer
  rateLimitPerMinute: number
  pagingSessionsPerHour: number
  /** Extra health fields, such as the ingester's live status when it runs in this process. */
  health?: () => Record<string, unknown>
}

export function createQueryServer(deps: ServerDeps) {
  const limits = new Limits(deps.rateLimitPerMinute, deps.pagingSessionsPerHour)
  const ns = config.namespace

  return createServer(async (req, res) => {
    const url = new URL(req.url ?? '/', 'http://localhost')
    const caller = callerOf(req)
    try {
      if (req.method === 'OPTIONS') return send(res, 204, null)
      if (req.method !== 'GET' && req.method !== 'HEAD') throw new XrpcError(405, 'MethodNotSupported', 'use GET')
      switch (url.pathname) {
        case '/':
          return send(res, 200, {
            service: 'Iriai PlaceProto query service',
            methods: [`${ns}.getEvent`, `${ns}.queryEvents`],
            contracts: { repository: 'danielATiriai/iriai-proto', ...contracts },
            access: ACCESS,
            health: '/xrpc/_health',
          })
        case '/health':
        case '/xrpc/_health':
          return send(res, 200, await health(deps))
      }

      const wait = limits.request(caller)
      if (wait > 0) throw new XrpcError(429, 'RateLimitExceeded', 'too many requests; slow down', wait)
      const q: QueryDeps = {
        ...deps,
        sessions: { wait: () => limits.sessionWait(caller), opened: () => limits.sessionOpened(caller) },
      }
      switch (url.pathname) {
        case `/xrpc/${ns}.getEvent`:
          return send(res, 200, await getEvent(q, url.searchParams.get('uri')))
        case `/xrpc/${ns}.queryEvents`:
          return send(res, 200, await queryEvents(q, parseEventQuery(url.searchParams)))
        default:
          throw new XrpcError(404, 'MethodNotImplemented', `no method at ${url.pathname}`)
      }
    } catch (err) {
      if (err instanceof XrpcError) {
        if (err.retryAfter) res.setHeader('retry-after', String(err.retryAfter))
        return send(res, err.status, { error: err.error, message: err.message })
      }
      if (err instanceof InvalidRequest || err instanceof CursorError) {
        return send(res, 400, { error: err.error, message: err.message })
      }
      console.error(`${req.method} ${url.pathname} failed:`, err)
      return send(res, 500, { error: 'InternalServerError', message: 'unexpected error' })
    }
  })
}

/** Service health with ingestion freshness (PlaceProto §4); never fails, reports what it can reach. */
async function health(deps: ServerDeps): Promise<Record<string, unknown>> {
  const out: Record<string, unknown> = {
    contracts: contracts.tag,
    namespace: config.namespace,
    collections: [config.communityEventCollection, `${config.namespace}.event`],
    access: ACCESS,
  }
  try {
    const cp = await readCheckpoint(deps.db, CHECKPOINT)
    const counts = await deps.db.query<{ complete: boolean; reason: string | null; n: string }>(
      'select complete, reason, count(*) as n from index_state group by complete, reason',
    )
    out.ingestion = cp
      ? {
          lastAppliedAt: new Date(cp.cursor / 1000).toISOString(),
          lagSeconds: Math.max(0, Math.round((Date.now() - cp.cursor / 1000) / 1000)),
          checkpointedAt: cp.updatedAt.toISOString(),
        }
      : { lastAppliedAt: null }
    out.sidecars = Object.fromEntries(counts.rows.map((r) => [r.complete ? 'complete' : r.reason ?? 'incomplete', Number(r.n)]))
    const accounts = await deps.db.query<{ inactive: string; awaiting_rebuild: string }>(
      `select count(*) filter (where not active) as inactive,
              count(*) filter (where active and needs_rebuild) as awaiting_rebuild
       from accounts`,
    )
    out.accounts = { inactive: Number(accounts.rows[0].inactive), awaitingRebuild: Number(accounts.rows[0].awaiting_rebuild) }
    const skipped = await deps.db.query<{ n: string }>('select count(*) as n from skipped_events')
    out.skippedEvents = Number(skipped.rows[0].n)
  } catch {
    out.database = 'unavailable'
  }
  // index: the backend new sessions go to, by the rule of spec 5.2; search:
  // the state behind that choice (spec 5.3, as PR 1 has it).
  const { search } = deps
  let backend: string | undefined
  let pgVerified = false
  try {
    const row = await search.control.refresh()
    const chosen = route(search.force, row)
    backend = chosen?.backend
    pgVerified = row.pgVerified
    out.search = {
      force: search.force,
      chosen: chosen ?? null,
      lastServed: search.last ?? null,
      esInUse: true,
      pgFirstFillDone: row.firstFillDone,
      pgVerified: row.pgVerified,
      comparedAt: row.comparedAt?.toISOString() ?? null,
      comparison: row.note,
      elasticCursorsUntil: row.elasticCursorsUntil.toISOString(),
    }
  } catch {
    out.search = { unavailable: true }
  }
  // Elasticsearch not answering within 2 seconds hands new sessions to a
  // verified Postgres, so health reports Postgres then, as routing does.
  if (!backend) out.index = { ok: false }
  else if (backend === 'postgres') out.index = await countOn(search.postgres)
  else {
    out.index = await countOn(search.elastic, { signal: AbortSignal.timeout(ELASTIC_FIRST_MS) })
    if (!(out.index as { ok: boolean }).ok && pgVerified) {
      out.index = { ...(await countOn(search.postgres)), fallback: 'Elasticsearch did not answer; new sessions go to Postgres' }
    }
  }
  return { ...out, ...(deps.health?.() ?? {}) }
}

async function countOn(backend: SearchBackend, opts?: CallOptions): Promise<Record<string, unknown>> {
  try {
    return { ok: true, backend: backend.name, events: await backend.count(opts) }
  } catch {
    return { ok: false, backend: backend.name }
  }
}

/**
 * The caller's address as Railway's edge reports it in X-Real-IP, else the
 * socket address. Not X-Forwarded-For, whose first hop a client can write
 * itself. query.iriai.app is DNS-only on Cloudflare, so the edge sees callers
 * directly; proxying it through Cloudflare would make every caller one of
 * Cloudflare's addresses here.
 */
function callerOf(req: IncomingMessage): string {
  const real = req.headers['x-real-ip']
  return (Array.isArray(real) ? real[0] : real)?.trim() || req.socket.remoteAddress || 'unknown'
}

function send(res: ServerResponse, status: number, body: unknown): void {
  res.statusCode = status
  res.setHeader('access-control-allow-origin', '*')
  res.setHeader('access-control-allow-methods', 'GET, HEAD, OPTIONS')
  res.setHeader('cache-control', 'no-store')
  if (body === null) return void res.end()
  res.setHeader('content-type', 'application/json; charset=utf-8')
  res.end(JSON.stringify(body))
}
