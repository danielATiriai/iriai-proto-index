import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { Limits } from '../src/query/limits.js'

describe('per-caller limits', () => {
  it('allow N requests a minute, then say how long to wait', () => {
    const l = new Limits(3, 2)
    const t = 1_800_000_000_000 + 10_000
    assert.deepEqual([l.request('a', t), l.request('a', t), l.request('a', t)], [0, 0, 0])
    assert.equal(l.request('a', t), 50)
    assert.equal(l.request('b', t), 0)
    assert.equal(l.request('a', t + 50_000), 0)
  })

  it('cap new paging sessions per hour', () => {
    const l = new Limits(100, 2)
    const t = 1_800_000_000_000
    assert.equal(l.openSession('a', t), 0)
    assert.equal(l.openSession('a', t + 1000), 0)
    assert.equal(l.openSession('a', t + 2000), 3598)
    assert.equal(l.openSession('a', t + 3_600_001), 0)
  })
})
