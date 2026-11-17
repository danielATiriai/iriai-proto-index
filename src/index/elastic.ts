import { Client, errors } from '@elastic/elasticsearch'
import type { EventQuery } from '../query/params.js'
import { buildSearch } from '../query/search.js'
import type { CallOptions, Hit, SearchBackend } from '../search/backend.js'
import type { EventDoc } from './assemble.js'

export type Elastic = Client

export function connectElastic(url: string): Elastic {
  return new Client({ node: url, requestTimeout: 10_000, maxRetries: 1 })
}

/** Mapping for the event index; `view` and `resolution` are stored, not searched. */
export const EVENT_MAPPING = {
  dynamic: 'strict' as const,
  properties: {
    uri: { type: 'keyword' as const },
    eventUri: { type: 'keyword' as const },
    did: { type: 'keyword' as const },
    sidecarCid: { type: 'keyword' as const, index: false },
    eventCid: { type: 'keyword' as const, index: false },
    view: { type: 'object' as const, enabled: false },
    point: { type: 'geo_point' as const },
    resolved: { type: 'boolean' as const },
    resolution: { type: 'object' as const, enabled: false },
    startsAt: { type: 'date' as const },
    endsAt: { type: 'date' as const },
    timeValid: { type: 'boolean' as const },
    indexedAt: { type: 'date' as const },
  },
}

export async function ensureIndex(es: Elastic, index: string): Promise<void> {
  if (await es.indices.exists({ index })) return
  try {
    await es.indices.create({ index, mappings: EVENT_MAPPING, settings: { number_of_shards: 1, number_of_replicas: 0 } })
  } catch (err) {
    if (!(err instanceof errors.ResponseError && err.body?.error?.type === 'resource_already_exists_exception')) throw err
  }
}

export async function putDoc(es: Elastic, index: string, doc: EventDoc): Promise<void> {
  await es.index({ index, id: doc.uri, document: doc })
}

export async function removeDoc(es: Elastic, index: string, uri: string): Promise<void> {
  await es.delete({ index, id: uri }, { ignore: [404] })
}

export async function getDoc(es: Elastic, index: string, uri: string): Promise<EventDoc | undefined> {
  const res = await es.get<EventDoc>({ index, id: uri }, { ignore: [404] })
  return res.found ? res._source : undefined
}

const KEEP_ALIVE = '15m'

/**
 * Today's Elasticsearch index behind the search interface (spec 3). Writes
 * use Elasticsearch's own versions, exactly as before (spec 5.1, PR 1). The
 * index is created on the first write rather than at start-up, so neither role
 * waits for Elasticsearch to start (spec 5.2).
 */
export class ElasticBackend implements SearchBackend {
  readonly name = 'elastic' as const
  private ensured = false

  constructor(
    readonly es: Elastic,
    readonly index: string,
  ) {}

  private async ensure(): Promise<void> {
    if (this.ensured) return
    await ensureIndex(this.es, this.index)
    this.ensured = true
  }

  async put(doc: EventDoc): Promise<void> {
    await this.ensure()
    await putDoc(this.es, this.index, doc)
  }

  async remove(uri: string): Promise<void> {
    await this.ensure()
    await removeDoc(this.es, this.index, uri)
  }

  async get(uri: string, opts: CallOptions = {}): Promise<EventDoc | undefined> {
    const res = await this.es.get<EventDoc>({ index: this.index, id: uri }, { ignore: [404], ...opts })
    // A 404 for a missing index is the index being unavailable, not "no such
    // event": throwing lets routing fall back to a verified Postgres.
    const error = (res as unknown as { error?: { type?: string } }).error
    if (error) throw new Error(`Elasticsearch: ${error.type ?? 'error'} on get`)
    return res.found ? res._source : undefined
  }

  async openSnapshot(opts: CallOptions = {}): Promise<string> {
    return (await this.es.openPointInTime({ index: this.index, keep_alive: KEEP_ALIVE }, opts)).id
  }

  async page(snapshot: string, q: EventQuery, after: unknown[] | undefined, size: number, opts: CallOptions = {}): Promise<{ hits: Hit[]; snapshot: string }> {
    const { query, sort } = buildSearch(q)
    const res = await this.es.search<EventDoc>(
      {
        pit: { id: snapshot, keep_alive: KEEP_ALIVE },
        query,
        sort: sort as never,
        size,
        ...(after ? { search_after: after as never } : {}),
        track_total_hits: false,
      },
      opts,
    )
    return { hits: res.hits.hits.map((h) => ({ doc: h._source!, sort: h.sort ?? [] })), snapshot: res.pit_id ?? snapshot }
  }

  async closeSnapshot(snapshot: string): Promise<void> {
    await this.es.closePointInTime({ id: snapshot })
  }

  async count(opts: CallOptions = {}): Promise<number> {
    return (await this.es.count({ index: this.index }, opts)).count
  }

  /** Every document in the index, for the verifying comparison; refreshes first so recent writes are seen. */
  async allDocs(): Promise<Map<string, EventDoc>> {
    await this.es.indices.refresh({ index: this.index })
    const out = new Map<string, EventDoc>()
    let after: unknown[] | undefined
    for (;;) {
      const res = await this.es.search<EventDoc>({
        index: this.index,
        size: 1000,
        sort: [{ uri: { order: 'asc' } }] as never,
        ...(after ? { search_after: after as never } : {}),
        track_total_hits: false,
      })
      const hits = res.hits.hits
      for (const h of hits) out.set(h._id!, h._source!)
      if (hits.length < 1000) return out
      after = hits[hits.length - 1].sort
    }
  }
}
