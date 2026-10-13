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
}

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
  }
}

function required(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name]
  if (!value) throw new Error(`${name} is not set`)
  return value
}
