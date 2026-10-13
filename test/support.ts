import { createServer, type Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import { errors } from '@elastic/elasticsearch'
import pg from 'pg'
import { connect, migrate, type Db } from '../src/db.js'
import type { Elastic } from '../src/index/elastic.js'

export const databaseUrl = process.env.TEST_DATABASE_URL
export const elasticUrl = process.env.TEST_ELASTICSEARCH_URL

/** A fresh database per test file, so files can run in any order. */
export async function freshDb(name: string): Promise<Db> {
  const admin = new pg.Client({ connectionString: databaseUrl })
  await admin.connect()
  await admin.query(`drop database if exists ${name} with (force)`)
  await admin.query(`create database ${name}`)
  await admin.end()
  const url = new URL(databaseUrl!)
  url.pathname = `/${name}`
  const db = connect(url.toString())
  await migrate(db)
  return db
}

/** Just enough of the Elasticsearch client for the projector, kept in memory. Writes fail while `down` is set. */
export function memoryElastic(): Elastic & { docs: Map<string, unknown>; down: boolean } {
  const docs = new Map<string, unknown>()
  const es = {
    docs,
    down: false,
    index: async ({ id, document }: { id: string; document: unknown }) => {
      if (es.down) throw new errors.ConnectionError('connect ECONNREFUSED elasticsearch')
      docs.set(id, document)
    },
    delete: async ({ id }: { id: string }) => {
      if (es.down) throw new errors.ConnectionError('connect ECONNREFUSED elasticsearch')
      docs.delete(id)
    },
    get: async ({ id }: { id: string }) => (docs.has(id) ? { found: true, _source: docs.get(id) } : { found: false }),
  }
  return es as never
}

/** The pool, with every query failing like an unreachable Postgres while `down` is set. */
export function switchableDb(db: Db): Db & { down: boolean } {
  const state = { down: false }
  return new Proxy(db, {
    get(target, prop) {
      if (prop === 'down') return state.down
      if (prop === 'query' && state.down) {
        return async () => {
          throw Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:5432'), { code: 'ECONNREFUSED' })
        }
      }
      const value = Reflect.get(target, prop)
      return typeof value === 'function' ? value.bind(target) : value
    },
    set(target, prop, value) {
      if (prop === 'down') return (state.down = value), true
      return Reflect.set(target, prop, value)
    },
  }) as Db & { down: boolean }
}

export interface FakeNetwork {
  url: string
  /** Records by AT-URI, as each PDS would serve them. */
  records: Map<string, { cid: string; value: Record<string, unknown> }>
  /** Fail every request while set, like a PDS outage. */
  down: boolean
  close(): Promise<void>
}

/** One server playing the PLC directory and every account's PDS. */
export async function fakeNetwork(): Promise<FakeNetwork> {
  const net = { records: new Map(), down: false } as FakeNetwork
  const server: Server = createServer((req, res) => {
    const url = new URL(req.url ?? '/', 'http://x')
    const json = (status: number, body: unknown) => {
      res.writeHead(status, { 'content-type': 'application/json' })
      res.end(JSON.stringify(body))
    }
    if (net.down) return json(502, { error: 'BadGateway' })
    if (url.pathname.startsWith('/did:plc:')) {
      return json(200, { id: url.pathname.slice(1), service: [{ id: '#atproto_pds', type: 'AtprotoPersonalDataServer', serviceEndpoint: net.url }] })
    }
    const q = url.searchParams
    if (url.pathname === '/xrpc/com.atproto.repo.getRecord') {
      const uri = `at://${q.get('repo')}/${q.get('collection')}/${q.get('rkey')}`
      const r = net.records.get(uri)
      return r ? json(200, { uri, ...r }) : json(400, { error: 'RecordNotFound' })
    }
    if (url.pathname === '/xrpc/com.atproto.repo.listRecords') {
      const prefix = `at://${q.get('repo')}/${q.get('collection')}/`
      const records = [...net.records].filter(([u]) => u.startsWith(prefix)).map(([uri, r]) => ({ uri, ...r }))
      return json(200, { records })
    }
    json(404, { error: 'NotFound' })
  })
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
  net.url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
  net.close = () => new Promise((r) => server.close(() => r()))
  return net
}
