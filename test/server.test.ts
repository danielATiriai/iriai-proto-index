import { after, before, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import type { AddressInfo } from 'node:net'
import type { Server } from 'node:http'
import { randomBytes } from 'node:crypto'
import { createQueryServer } from '../src/query/server.js'

describe('query server', () => {
  let server: Server
  let base: string

  before(async () => {
    // Requests here fail validation before reaching Postgres or Elasticsearch.
    server = createQueryServer({
      db: undefined as never,
      es: undefined as never,
      index: 'events',
      serviceId: 'test',
      cursorKey: randomBytes(32),
      rateLimitPerMinute: 1,
      pagingSessionsPerHour: 1,
    })
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
    base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
  })
  after(() => new Promise<void>((r) => server.close(() => r())))

  it("limits callers by the address Railway's edge reports, not one the client puts in X-Forwarded-For", async () => {
    const get = (realIp: string, forwarded: string) =>
      fetch(`${base}/xrpc/app.iriai.proto.getEvent`, { headers: { 'x-real-ip': realIp, 'x-forwarded-for': forwarded } })
    assert.equal((await get('203.0.113.7', '198.51.100.1')).status, 400)
    const limited = await get('203.0.113.7', '198.51.100.2')
    assert.equal(limited.status, 429)
    assert.equal(((await limited.json()) as { error: string }).error, 'RateLimitExceeded')
    assert.ok(Number(limited.headers.get('retry-after')) > 0)
    assert.equal((await get('203.0.113.8', '198.51.100.2')).status, 400)
  })
})
