import { errors } from '@elastic/elasticsearch'

const NETWORK_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'EHOSTUNREACH', 'ENETUNREACH', 'ENOTFOUND', 'EAI_AGAIN'])
/** SQLSTATEs that mean Postgres is unreachable, restarting, overloaded or contended, not that the data is bad. */
const POSTGRES_STATES = new Set(['57P01', '57P02', '57P03', '57014', '40001', '40P01', '55P03'])
const POSTGRES_CLASSES = ['08', '53']
const POSTGRES_MESSAGES = ['Connection terminated', 'timeout exceeded when trying to connect', 'is not queryable']

/**
 * Whether a failure comes from an outage (Postgres, Elasticsearch or the
 * network) that a later retry can get past, as opposed to something about
 * the event itself that will fail the same way every time.
 */
export function isTransient(err: unknown): boolean {
  if (
    err instanceof errors.ConnectionError ||
    err instanceof errors.TimeoutError ||
    err instanceof errors.NoLivingConnectionsError ||
    err instanceof errors.RequestAbortedError
  ) {
    return true
  }
  if (err instanceof errors.ResponseError) {
    const status = err.statusCode ?? 0
    return status >= 500 || status === 429 || JSON.stringify(err.body ?? '').includes('cluster_block_exception')
  }
  if (!(err instanceof Error)) return false
  const code = (err as { code?: unknown }).code
  if (typeof code === 'string') {
    if (NETWORK_CODES.has(code) || POSTGRES_STATES.has(code)) return true
    if (/^[0-9A-Z]{5}$/.test(code) && POSTGRES_CLASSES.includes(code.slice(0, 2))) return true
  }
  return POSTGRES_MESSAGES.some((m) => err.message.includes(m))
}
