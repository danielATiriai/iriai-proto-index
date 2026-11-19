/** events.iriai.app, the steward account that publishes the seed events (BUILD-006). */
export const STEWARD_DID = 'did:plc:kd5s7zyp7nje43ievnqhgzrr'

export type Role = 'ingester' | 'query' | 'all'

export interface Settings {
  role: Role
  port: number
  databaseUrl: string
  elasticUrl: string
  elasticIndex: string
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
  /**
   * SEARCH_BACKEND_FORCE: the backend new sessions are routed to when it may
   * serve, overriding the controller's state (spec 5.2, 6.4); unset follows
   * the state.
   */
  searchBackendForce?: 'elastic' | 'postgres'
  /** SEARCH_ES on the ingester: Elasticsearch in use whatever the controller's state (spec 5.2); on unless set. */
  searchEs: boolean
  /** GITHUB_ALERT_TOKEN: opens issues in danielATiriai/iriai-proto-index (spec 9); issues are only logged without it. */
  githubAlertToken?: string
  githubAlertRepo: string
  thresholds: Thresholds
}

/** The six numbers of spec 6.2 (D2 A), each a setting. */
export interface Thresholds {
  /** SEARCH_ON_MIN_CALLS: a day counts toward switching on only with at least this many queryEvents calls. */
  onMinCalls: number
  /** SEARCH_ON_P95_MS: Postgres p95 at or above this on 3 consecutive days switches on. */
  onP95Ms: number
  /** SEARCH_ON_TIMEOUT_RATE: or more than this share of Postgres searches stopped by the 2 s limit. */
  onTimeoutRate: number
  /** SEARCH_ON_EVENTS: or this many current events on any day. */
  onEvents: number
  /** SEARCH_OFF_P95_MS: off after 14 days with the Postgres shadow p95 at most this... */
  offP95Ms: number
  /** SEARCH_OFF_EVENTS: ...and at most this many events. */
  offEvents: number
}

export function loadSettings(env: NodeJS.ProcessEnv = process.env): Settings {
  const role = (env.ROLE ?? 'all') as Role
  if (!['ingester', 'query', 'all'].includes(role)) throw new Error(`ROLE must be ingester, query or all, not ${role}`)
  const needsCursorKey = role !== 'ingester'
  const force = env.SEARCH_BACKEND_FORCE || undefined
  if (force !== undefined && force !== 'elastic' && force !== 'postgres') throw new Error(`SEARCH_BACKEND_FORCE must be elastic, postgres or unset, not ${force}`)
  const searchEs = env.SEARCH_ES || 'on'
  if (searchEs !== 'on' && searchEs !== 'off') throw new Error(`SEARCH_ES must be on or off, not ${searchEs}`)
  const cursorKey = Buffer.from(env.CURSOR_KEY ?? '', 'base64')
  if (needsCursorKey && cursorKey.length !== 32) throw new Error('CURSOR_KEY must be 32 bytes, base64-encoded')
  return {
    role,
    port: Number(env.PORT ?? 8080),
    databaseUrl: required(env, 'DATABASE_URL'),
    elasticUrl: required(env, 'ELASTICSEARCH_URL'),
    elasticIndex: env.ELASTICSEARCH_INDEX ?? 'placeproto-events-v1',
    jetstreamUrl: env.JETSTREAM_URL ?? 'wss://jetstream2.us-east.bsky.network/subscribe',
    jetstreamStartCursor: env.JETSTREAM_START_CURSOR ? Number(env.JETSTREAM_START_CURSOR) : undefined,
    approvedAccounts: (env.APPROVED_ACCOUNTS ?? STEWARD_DID).split(',').map((s) => s.trim()).filter(Boolean),
    plcUrl: env.PLC_URL ?? 'https://plc.directory',
    serviceId: env.SERVICE_ID ?? 'query.iriai.app',
    cursorKey,
    rateLimitPerMinute: Number(env.RATE_LIMIT_PER_MINUTE ?? 120),
    pagingSessionsPerHour: Number(env.PAGING_SESSIONS_PER_HOUR ?? 60),
    searchBackendForce: force,
    searchEs: searchEs === 'on',
    githubAlertToken: env.GITHUB_ALERT_TOKEN || undefined,
    githubAlertRepo: env.GITHUB_ALERT_REPO || 'danielATiriai/iriai-proto-index',
    thresholds: {
      onMinCalls: num(env, 'SEARCH_ON_MIN_CALLS', 500),
      onP95Ms: num(env, 'SEARCH_ON_P95_MS', 250),
      onTimeoutRate: num(env, 'SEARCH_ON_TIMEOUT_RATE', 0.01),
      onEvents: num(env, 'SEARCH_ON_EVENTS', 500_000),
      offP95Ms: num(env, 'SEARCH_OFF_P95_MS', 100),
      offEvents: num(env, 'SEARCH_OFF_EVENTS', 250_000),
    },
  }
}

export const DEFAULT_THRESHOLDS: Thresholds = { onMinCalls: 500, onP95Ms: 250, onTimeoutRate: 0.01, onEvents: 500_000, offP95Ms: 100, offEvents: 250_000 }

function num(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = env[name]
  if (raw === undefined || raw === '') return fallback
  const n = Number(raw)
  if (!Number.isFinite(n) || n < 0) throw new Error(`${name} must be a number of at least 0, not ${raw}`)
  return n
}

function required(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name]
  if (!value) throw new Error(`${name} is not set`)
  return value
}
