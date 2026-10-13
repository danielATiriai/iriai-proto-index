import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { randomBytes } from 'node:crypto'
import { ABSOLUTE_MS, IDLE_MS, CursorError, decodeCursor, encodeCursor } from '../src/query/cursor.js'

const key = randomBytes(32)
const expect = { svc: 'query.iriai.app', bind: 'b1' }
const t0 = 1_800_000_000_000
const state = { ...expect, pit: 'pit-id', after: [12.5, 'at://x'], created: t0, issued: t0 }

function code(fn: () => unknown): string {
  try {
    fn()
  } catch (err) {
    if (err instanceof CursorError) return err.error
    throw err
  }
  return 'ok'
}

describe('cursors', () => {
  it('round-trip and are URL-safe', () => {
    const token = encodeCursor(state, key)
    assert.match(token, /^[A-Za-z0-9_-]+$/)
    assert.deepEqual(decodeCursor(token, key, expect, t0 + 1000), state)
  })

  it('are InvalidCursor when tampered, foreign or bound to other parameters', () => {
    const token = encodeCursor(state, key)
    const flipped = token.slice(0, -2) + (token.at(-2) === 'A' ? 'B' : 'A') + token.at(-1)
    assert.equal(code(() => decodeCursor(flipped, key, expect, t0)), 'InvalidCursor')
    assert.equal(code(() => decodeCursor('not-a-cursor', key, expect, t0)), 'InvalidCursor')
    assert.equal(code(() => decodeCursor(token, randomBytes(32), expect, t0)), 'InvalidCursor')
    assert.equal(code(() => decodeCursor(token, key, { ...expect, bind: 'b2' }, t0)), 'InvalidCursor')
    assert.equal(code(() => decodeCursor(token, key, { ...expect, svc: 'other' }, t0)), 'InvalidCursor')
  })

  it('expire after 15 idle minutes or one hour, whichever is first', () => {
    assert.equal(code(() => decodeCursor(encodeCursor(state, key), key, expect, t0 + IDLE_MS + 1)), 'CursorExpired')
    const renewed = { ...state, issued: t0 + ABSOLUTE_MS - 1000 }
    assert.equal(code(() => decodeCursor(encodeCursor(renewed, key), key, expect, t0 + ABSOLUTE_MS - 500)), 'ok')
    assert.equal(code(() => decodeCursor(encodeCursor(renewed, key), key, expect, t0 + ABSOLUTE_MS + 1)), 'CursorExpired')
  })
})
