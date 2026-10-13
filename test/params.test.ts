import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { bindingOf, parseEventQuery } from '../src/query/params.js'

const p = (s: string) => parseEventQuery(new URLSearchParams(s))

describe('queryEvents parameters', () => {
  it('defaults: uri order, limit 50', () => {
    assert.deepEqual(p(''), { sort: 'uri', limit: 50 })
  })

  it('a coordinate makes distance the default sort; startsAt may be chosen', () => {
    assert.equal(p('latitude=37.785054&longitude=-122.429827').sort, 'distance')
    assert.equal(p('latitude=37.785054&longitude=-122.429827&sort=startsAt').sort, 'startsAt')
    assert.equal(p('sort=startsAt').sort, 'startsAt')
  })

  it('rejects malformed or out-of-range coordinates', () => {
    for (const bad of ['1e5', ' 37.7', '37.', '+37', 'NaN', 'Infinity', '91', '-90.0001', '3'.repeat(33)]) {
      assert.throws(() => p(`latitude=${encodeURIComponent(bad)}&longitude=0`), /latitude/, bad)
    }
    assert.throws(() => p('latitude=0&longitude=180.5'), /longitude/)
  })

  it('needs latitude and longitude together, and a coordinate for radius or distance sort', () => {
    assert.throws(() => p('latitude=37'), /together/)
    assert.throws(() => p('radiusMeters=100'), /requires latitude/)
    assert.throws(() => p('sort=distance'), /requires latitude/)
  })

  it('bounds radius to 1..20,100,000 integer meters', () => {
    assert.equal(p('latitude=0&longitude=0&radiusMeters=20100000').radiusMeters, 20100000)
    for (const bad of ['0', '20100001', '1.5', '-1']) assert.throws(() => p(`latitude=0&longitude=0&radiusMeters=${bad}`), /radius/)
  })

  it('needs all four box edges, south < north, west != east; west > east crosses the antimeridian', () => {
    assert.throws(() => p('west=1&south=2&east=3'), /bounding box/)
    assert.throws(() => p('west=1&south=5&east=3&north=5'), /south/)
    assert.throws(() => p('west=3&south=1&east=3&north=5'), /differ/)
    assert.deepEqual(p('west=170&south=-10&east=-170&north=10').box, { west: 170, south: -10, east: -170, north: 10 })
  })

  it('checks the time window and normalizes it to UTC', () => {
    assert.deepEqual(
      [p('windowStart=2026-10-15T00:00:00-07:00').windowStart, p('windowEnd=2026-10-16T00:00:00Z').windowEnd],
      ['2026-10-15T07:00:00.000Z', '2026-10-16T00:00:00.000Z'],
    )
    assert.throws(() => p('windowStart=2026-10-16T00:00:00Z&windowEnd=2026-10-16T00:00:00Z'), /before/)
    assert.throws(() => p('windowStart=tomorrow'), /datetime/)
  })

  it('takes up to 25 author DIDs and a DID-based community eventUri', () => {
    assert.deepEqual(p('author=did:plc:b&author=did:plc:a&author=did:plc:a').author, ['did:plc:a', 'did:plc:b'])
    assert.throws(() => p('author=alice.bsky.social'), /not a DID/)
    assert.throws(() => p(Array.from({ length: 26 }, (_, i) => `author=did:plc:${i}`).join('&')), /25/)
    assert.throws(() => p('eventUri=at://alice.bsky.social/community.lexicon.calendar.event/3abc'), /DID/)
    assert.throws(() => p('eventUri=at://did:plc:a/app.bsky.feed.post/3abc'), /community.lexicon.calendar.event/)
  })

  it('bounds limit to 1..100', () => {
    assert.equal(p('limit=100').limit, 100)
    assert.throws(() => p('limit=0'), /limit/)
    assert.throws(() => p('limit=101'), /limit/)
  })

  it('binds cursors to normalized filters, not limit or cursor', () => {
    const a = bindingOf(p('latitude=37.50&longitude=-122.4&limit=5&cursor=x'))
    const b = bindingOf(p('longitude=-122.40&latitude=37.5&limit=9'))
    assert.equal(a, b)
    assert.notEqual(a, bindingOf(p('latitude=37.5&longitude=-122.4&sort=startsAt')))
    assert.notEqual(bindingOf(p('west=1&south=1&east=2&north=2')), bindingOf(p('west=1&south=1&east=3&north=2')))
  })
})
