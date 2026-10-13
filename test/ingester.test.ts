import { after, before, beforeEach, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import type { Db } from '../src/db.js'
import { CHECKPOINT, Ingester, type Socket } from '../src/ingest/ingester.js'
import { Projector } from '../src/ingest/projector.js'
import { readCheckpoint, writeCheckpoint } from '../src/ingest/store.js'
import type { Settings } from '../src/settings.js'
import { EVENT, SIDECAR, eventValue, geo, sidecarValue } from './fixtures.js'
import { databaseUrl, fakeNetwork, freshDb, memoryElastic, switchableDb, type FakeNetwork } from './support.js'

const AUTHOR = 'did:plc:ingesterauthor0000000000'

/** Stands in for one Jetstream connection; the test opens it and sends events. */
class FakeSocket implements Socket {
  private listeners = new Map<string, ((msg: { data: unknown }) => void)[]>()
  closed = false
  constructor(readonly url: URL) {}
  addEventListener(type: string, listener: (msg: { data: unknown }) => void): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener])
  }
  private fire(type: string, msg: { data: unknown } = { data: undefined }): void {
    for (const l of this.listeners.get(type) ?? []) l(msg)
  }
  open(): void {
    this.fire('open')
  }
  send(evt: unknown): void {
    this.fire('message', { data: JSON.stringify(evt) })
  }
  close(): void {
    if (this.closed) return
    this.closed = true
    setImmediate(() => this.fire('close'))
  }
}

async function until(cond: () => boolean | Promise<boolean>, what: string, ms = 5_000): Promise<void> {
  const end = Date.now() + ms
  while (!(await cond())) {
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`)
    await new Promise((r) => setTimeout(r, 5))
  }
}

describe('ingester', { skip: !databaseUrl && 'TEST_DATABASE_URL not set' }, () => {
  let base: Db
  let db: ReturnType<typeof switchableDb>
  let es: ReturnType<typeof memoryElastic>
  let net: FakeNetwork
  let sockets: FakeSocket[]
  let ingester: Ingester
  let t = 1_760_000_000_000_000
  let rev = 0

  const settings = { jetstreamUrl: 'wss://jetstream.test/subscribe', approvedAccounts: [] } as unknown as Settings
  const commit = (rkey: string, collection: string, record: Record<string, unknown>) => ({
    did: AUTHOR,
    time_us: t++,
    kind: 'commit',
    commit: { rev: `3m${String(++rev).padStart(11, '2')}`, operation: 'create', collection, rkey, record, cid: 'bafyreibjytuinufvu3gimfxyqjbptuws7g3sdekx2igq5dts4ar7jgxjbq' },
  })
  const latest = () => sockets[sockets.length - 1]
  async function next(count: number): Promise<FakeSocket> {
    await until(() => sockets.length >= count, `connection ${count}`)
    const s = sockets[count - 1]
    s.open()
    return s
  }

  before(async () => {
    base = await freshDb('ingester_test')
    net = await fakeNetwork()
  })
  after(async () => {
    await base.end()
    await net.close()
  })
  beforeEach(async () => {
    await ingester?.stop()
    db = switchableDb(base)
    es = memoryElastic()
    sockets = []
    const projector = new Projector({ db, es, index: 'events', plcUrl: net.url, log: () => {} })
    ingester = new Ingester(settings, db, projector, process.env.DEBUG_INGESTER ? console.log : () => {}, {
      openSocket: (url) => (sockets.push(new FakeSocket(new URL(url))), latest()),
      backoffMs: 5,
    })
  })
  after(async () => ingester?.stop())

  it('keeps trying to reconnect through a database outage, then resumes from the checkpoint', async () => {
    await writeCheckpoint(base, CHECKPOINT, t)
    await ingester.start()
    const first = await next(1)
    assert.equal(first.url.searchParams.get('cursor'), String(t - 5_000_000))

    // Postgres goes away and Jetstream drops the connection: reading the checkpoint fails, more than once.
    db.down = true
    first.close()
    await new Promise((r) => setTimeout(r, 100))
    assert.equal(sockets.length, 1)
    assert.equal(ingester.status.connected, false)

    db.down = false
    const second = await next(2)
    assert.equal(second.url.searchParams.get('cursor'), String(t - 5_000_000))
    assert.equal(ingester.status.connected, true)
  })

  it('retries an event through an Elasticsearch outage for as long as it lasts, never skipping it', async () => {
    await ingester.start()
    const sidecar = commit('3ksidecar0030', SIDECAR, sidecarValue(AUTHOR, '3kevent000030', geo('37.78', '-122.43')))
    const event = commit('3kevent000030', EVENT, eventValue({ name: 'Survives an outage' }))
    net.records.set(`at://${AUTHOR}/${EVENT}/3kevent000030`, { cid: 'bafyreibjytuinufvu3gimfxyqjbptuws7g3sdekx2igq5dts4ar7jgxjbq', value: event.commit.record })

    es.down = true
    for (let n = 1; n <= 5; n++) {
      const s = await next(n)
      s.send(event)
      s.send(sidecar)
      await until(() => s.closed, `failure ${n}`)
    }
    assert.equal(ingester.status.skipped, 0)

    es.down = false
    const s = await next(6)
    s.send(event)
    s.send(sidecar)
    await until(() => ingester.status.lastEventUs === sidecar.time_us, 'the sidecar to be handled')
    assert.ok(es.docs.has(`at://${AUTHOR}/${SIDECAR}/3ksidecar0030`))
    const skipped = await base.query('select count(*)::int as n from skipped_events')
    assert.equal(skipped.rows[0].n, 0)
    // The checkpoint moves only once both are handled.
    await until(async () => (await readCheckpoint(base, CHECKPOINT))?.cursor === sidecar.time_us, 'the checkpoint', 3_000)
  })

  it('skips and records an event that fails the same way every time, then carries on', async () => {
    await ingester.start()
    // Postgres refuses a \\u0000 in jsonb: no retry can store this record.
    const poisoned = commit('3kevent000040', EVENT, eventValue({ name: 'bad\u0000name' }))
    const fine = commit('3kevent000041', EVENT, eventValue({ name: 'Next one' }))
    for (let n = 1; n <= 3; n++) {
      const s = await next(n)
      s.send(poisoned)
      await until(() => s.closed, `failure ${n}`)
    }
    const s = await next(4)
    s.send(poisoned)
    s.send(fine)
    await until(() => ingester.status.lastEventUs === fine.time_us, 'the next event')
    assert.equal(ingester.status.skipped, 1)
    const skipped = await base.query('select time_us, attempts, error from skipped_events')
    assert.equal(Number(skipped.rows[0].time_us), poisoned.time_us)
    assert.equal(skipped.rows[0].attempts, 3)
    assert.match(skipped.rows[0].error, /Unicode|unsupported/i)
    assert.ok(await base.query('select 1 from records where uri = $1', [`at://${AUTHOR}/${EVENT}/3kevent000041`]).then((r) => r.rowCount === 1))
  })
})
