import { BUCKETS_MS } from './search/stats.js'
/** events.iriai.app, the steward account that publishes the seed events (BUILD-006). */
export const STEWARD_DID = 'did:plc:kd5s7zyp7nje43ievnqhgzrr'

export type Role = 'ingester' | 'query' | 'all'

export interface Settings {
  role: Role
  port: number
  databaseUrl: string
  jetstreamUrl: string
  /** Jetstream time_us to start from when no checkpoint is stored; live when unset. */
  jetstreamStartCursor?: number
  /** Accounts listed in full at start (BUILD-006): the steward, plus any added later. */
  approvedAccounts: string[]
  plcUrl: string
  /** Identifies this service in cursors, so a cursor from another deployment is refused. */
  serviceId: string
  /** 32 bytes, base64; encrypts and authenticates query cursors. */
  cursorKey: Buffer
  rateLimitPerMinute: number
  pagingSessionsPerHour: number
  /** GITHUB_ALERT_TOKEN (ingester): opens search issues (spec 6.4); without it they are only logged. */
  githubAlertToken?: string
  githubAlertRepo: string
  /** The scale trigger of spec 6.2 (D2 A); each a setting. */
  thresholds: Thresholds
}

export interface Thresholds {
  /** SEARCH_ON_MIN_CALLS: a day counts only with at least this many queryEvents calls. */
  onMinCalls: number
  /** SEARCH_ON_P95_MS */
  onP95Ms: number
  /** SEARCH_ON_TIMEOUT_RATE: the share of Postgres searches that ran out of time. */
  onTimeoutRate: number
  /** SEARCH_ON_EVENTS */
  onEvents: number
}

export const DEFAULT_THRESHOLDS: Thresholds = { onMinCalls: 500, onP95Ms: 250, onTimeoutRate: 0.01, onEvents: 500_000 }

export function loadSettings(env: NodeJS.ProcessEnv = process.env): Settings {
  const role = (env.ROLE ?? 'all') as Role
  if (!['ingester', 'query', 'all'].includes(role)) throw new Error(`ROLE must be ingester, query or all, not ${role}`)
  const needsCursorKey = role !== 'ingester'
  const cursorKey = Buffer.from(env.CURSOR_KEY ?? '', 'base64')
  if (needsCursorKey && cursorKey.length !== 32) throw new Error('CURSOR_KEY must be 32 bytes, base64-encoded')
  return {
    role,
    port: Number(env.PORT ?? 8080),
    databaseUrl: required(env, 'DATABASE_URL'),
    jetstreamUrl: env.JETSTREAM_URL ?? 'wss://jetstream2.us-east.bsky.network/subscribe',
    jetstreamStartCursor: env.JETSTREAM_START_CURSOR ? Number(env.JETSTREAM_START_CURSOR) : undefined,
    approvedAccounts: (env.APPROVED_ACCOUNTS ?? STEWARD_DID).split(',').map((s) => s.trim()).filter(Boolean),
    plcUrl: env.PLC_URL ?? 'https://plc.directory',
    serviceId: env.SERVICE_ID ?? 'query.iriai.app',
    cursorKey,
    rateLimitPerMinute: Number(env.RATE_LIMIT_PER_MINUTE ?? 120),
    pagingSessionsPerHour: Number(env.PAGING_SESSIONS_PER_HOUR ?? 60),
    githubAlertToken: env.GITHUB_ALERT_TOKEN || undefined,
    githubAlertRepo: env.GITHUB_ALERT_REPO ?? 'danielATiriai/iriai-proto-index',
    thresholds: {
      onMinCalls: num(env, 'SEARCH_ON_MIN_CALLS', DEFAULT_THRESHOLDS.onMinCalls),
      onP95Ms: edge(num(env, 'SEARCH_ON_P95_MS', DEFAULT_THRESHOLDS.onP95Ms)),
      onTimeoutRate: num(env, 'SEARCH_ON_TIMEOUT_RATE', DEFAULT_THRESHOLDS.onTimeoutRate),
      onEvents: num(env, 'SEARCH_ON_EVENTS', DEFAULT_THRESHOLDS.onEvents),
    },
  }
}

/** The p95 threshold must be a histogram bucket edge, so the check reads it exactly. */
function edge(ms: number): number {
  if (!BUCKETS_MS.includes(ms)) throw new Error(`SEARCH_ON_P95_MS must be a histogram bucket edge (a multiple of 10 up to 500, or of 50 up to 2000), not ${ms}`)
  return ms
}

function num(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = env[name]
  if (raw === undefined || raw === '') return fallback
  const n = Number(raw)
  if (!Number.isFinite(n) || n < 0) throw new Error(`${name} must be a non-negative number, not ${raw}`)
  return n
}

function required(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name]
  if (!value) throw new Error(`${name} is not set`)
  return value
}
