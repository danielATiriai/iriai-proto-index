import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto'

/** Idle and absolute limits for a paging session (PlaceProto §5). */
export const IDLE_MS = 15 * 60 * 1000
export const ABSOLUTE_MS = 60 * 60 * 1000

export interface CursorState {
  /** Issuing service. */
  svc: string
  /** Hash of the normalized filters, coordinate and sort. */
  bind: string
  /** Elasticsearch point-in-time id: the session's snapshot. */
  pit: string
  /** Sort values of the last examined candidate. */
  after: unknown[]
  /** Snapshot creation and this cursor's issue time, ms since epoch. */
  created: number
  issued: number
}

export class CursorError extends Error {
  constructor(
    readonly error: 'InvalidCursor' | 'CursorExpired',
    message: string,
  ) {
    super(message)
  }
}

export function bindingHash(binding: string): string {
  return createHash('sha256').update(binding).digest('base64url').slice(0, 22)
}

/** An opaque, URL-safe, authenticated-encrypted cursor (AES-256-GCM). */
export function encodeCursor(state: CursorState, key: Buffer): string {
  const iv = randomBytes(12)
  const cipher = createCipheriv('aes-256-gcm', key, iv)
  const body = Buffer.concat([cipher.update(JSON.stringify(state), 'utf8'), cipher.final()])
  return Buffer.concat([iv, cipher.getAuthTag(), body]).toString('base64url')
}

/**
 * Opens a cursor and checks it belongs to this service and these filters and
 * has not expired. A malformed or mismatched cursor is InvalidCursor; an
 * expired one is CursorExpired. Neither ever restarts the session silently.
 */
export function decodeCursor(token: string, key: Buffer, expect: { svc: string; bind: string }, now = Date.now()): CursorState {
  let state: CursorState
  try {
    const raw = Buffer.from(token, 'base64url')
    if (raw.length < 29) throw new Error('short')
    const decipher = createDecipheriv('aes-256-gcm', key, raw.subarray(0, 12))
    decipher.setAuthTag(raw.subarray(12, 28))
    state = JSON.parse(Buffer.concat([decipher.update(raw.subarray(28)), decipher.final()]).toString('utf8'))
  } catch {
    throw new CursorError('InvalidCursor', 'cursor is malformed or was not issued by this service')
  }
  if (state.svc !== expect.svc) throw new CursorError('InvalidCursor', 'cursor was issued by another service')
  if (state.bind !== expect.bind) {
    throw new CursorError('InvalidCursor', 'cursor was issued for different query parameters; repeat them unchanged')
  }
  if (now - state.created > ABSOLUTE_MS) throw new CursorError('CursorExpired', 'paging session is over an hour old')
  if (now - state.issued > IDLE_MS) throw new CursorError('CursorExpired', 'paging session was idle for over 15 minutes')
  return state
}
