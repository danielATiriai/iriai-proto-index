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

type Stored = { doc?: unknown; version: number }

/**
 * Just enough of the Elasticsearch client for the projector and the managed
 * index, kept in memory: indexes, write aliases (require_alias), external
 * versions with delete tombstones, bulk, refresh and cluster health. Every
 * call fails while `down` is set. `docs` shows the documents of `index`
 * (default "events").
 */
export function memoryElastic(index = 'events'): Elastic & {
  docs: Map<string, unknown>
  down: boolean
  versions(name?: string): Map<string, number>
  indexes: Map<string, Map<string, Stored>>
  calls: number
} {
  const indexes = new Map<string, Map<string, Stored>>()
  const aliases = new Map<string, string>()
  const fail = () => {
    es.calls++
    if (es.down) throw new errors.ConnectionError('connect ECONNREFUSED elasticsearch')
  }
  const responseError = (statusCode: number, type: string) =>
    new errors.ResponseError({ statusCode, body: { error: { type }, status: statusCode }, headers: {}, warnings: null, meta: {} as never } as never)
  const target = (name: string, requireAlias = false) => {
    if (aliases.has(name)) return indexes.get(aliases.get(name)!)!
    if (requireAlias) throw responseError(404, 'index_not_found_exception')
    let docs = indexes.get(name)
    // Auto-creation, as Elasticsearch does for a write to a missing index.
    if (!docs) indexes.set(name, (docs = new Map()))
    return docs
  }
  /** Writes one operation; returns the status Elasticsearch would. */
  const write = (name: string, id: string, doc: unknown | undefined, version: number | undefined, requireAlias: boolean): number => {
    if (doc === undefined && !aliases.has(name) && !indexes.has(name)) return 404
    const docs = target(name, requireAlias)
    const prior = docs.get(id)
    if (version !== undefined && prior && prior.version >= version) return 409
    if (doc === undefined) {
      if (!prior?.doc && version === undefined) return 404
      docs.set(id, { version: version ?? (prior?.version ?? 0) + 1 })
      return prior?.doc ? 200 : 404
    }
    docs.set(id, { doc, version: version ?? (prior?.version ?? 0) + 1 })
    return 200
  }
  const live = (name: string) => [...(indexes.get(aliases.get(name) ?? name) ?? new Map<string, Stored>())].filter(([, s]) => s.doc !== undefined)
  const es = {
    down: false,
    calls: 0,
    indexes,
    get docs() {
      return new Map(live(index).map(([id, s]) => [id, s.doc]))
    },
    versions: (name = index) => new Map(live(name).map(([id, s]) => [id, s.version])),
    index: async (
      p: { index: string; id: string; document: unknown; version?: number; require_alias?: boolean },
      opts: { ignore?: number[] } = {},
    ) => {
      fail()
      const status = write(p.index, p.id, p.document, p.version, !!p.require_alias)
      if (status >= 300 && !opts.ignore?.includes(status)) throw responseError(status, 'version_conflict_engine_exception')
      return { result: status === 200 ? 'updated' : 'noop' }
    },
    delete: async (p: { index: string; id: string; version?: number }, opts: { ignore?: number[] } = {}) => {
      fail()
      if (!indexes.has(aliases.get(p.index) ?? p.index)) {
        if (opts.ignore?.includes(404)) return { error: { type: 'index_not_found_exception' } }
        throw responseError(404, 'index_not_found_exception')
      }
      const status = write(p.index, p.id, undefined, p.version, false)
      if (status >= 300 && !opts.ignore?.includes(status)) throw responseError(status, 'version_conflict_engine_exception')
      return { result: status === 200 ? 'deleted' : 'not_found' }
    },
    bulk: async ({ operations }: { operations: Record<string, unknown>[] }) => {
      fail()
      const items = []
      for (let i = 0; i < operations.length; i++) {
        const op = operations[i]
        if (op.index) {
          const m = op.index as { _index: string; _id: string; version?: number; require_alias?: boolean }
          let status: number
          try {
            status = write(m._index, m._id, operations[++i], m.version, !!m.require_alias)
          } catch {
            status = 404
          }
          items.push({ index: { status, ...(status === 404 ? { error: { type: 'index_not_found_exception' } } : {}) } })
        } else {
          const m = op.delete as { _index: string; _id: string; version?: number }
          const exists = indexes.has(aliases.get(m._index) ?? m._index)
          const status = exists ? write(m._index, m._id, undefined, m.version, false) : 404
          items.push({ delete: { status, ...(exists ? {} : { error: { type: 'index_not_found_exception' } }) } })
        }
      }
      return { items }
    },
    get: async ({ index: name, id }: { index: string; id: string }) => {
      fail()
      const s = indexes.get(aliases.get(name) ?? name)?.get(id)
      return s?.doc ? { found: true, _source: s.doc } : { found: false }
    },
    cluster: {
      health: async () => {
        fail()
        return { status: 'green' }
      },
    },
    indices: {
      exists: async ({ index: name }: { index: string }) => {
        fail()
        return index === name || indexes.has(name)
      },
      create: async ({ index: name, aliases: al }: { index: string; aliases?: Record<string, unknown> }) => {
        fail()
        if (indexes.has(name)) throw responseError(400, 'resource_already_exists_exception')
        indexes.set(name, new Map())
        for (const a of Object.keys(al ?? {})) aliases.set(a, name)
      },
      get: async ({ index: pattern }: { index: string }) => {
        fail()
        const prefix = pattern.replace(/\*$/, '')
        return Object.fromEntries([...indexes.keys()].filter((n) => n.startsWith(prefix)).map((n) => [n, {}]))
      },
      delete: async ({ index: name }: { index: string }) => {
        fail()
        indexes.delete(name)
        for (const [a, n] of aliases) if (n === name) aliases.delete(a)
      },
      refresh: async () => {
        fail()
      },
    },
    search: async (p: { index?: string; size?: number; search_after?: unknown[] }) => {
      fail()
      const all = live(p.index ?? index).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      const after = p.search_after?.[0] as string | undefined
      const page = all.filter(([id]) => after === undefined || id > after).slice(0, p.size ?? 10)
      return { hits: { hits: page.map(([id, s]) => ({ _id: id, _source: s.doc, _version: s.version, sort: [id] })) } }
    },
  }
  // The default index exists and takes writes through its alias, as a managed index does.
  indexes.set(index, new Map())
  aliases.set(`${index}-write`, index)
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
