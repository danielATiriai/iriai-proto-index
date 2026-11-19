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

/** Managed indexes are placeproto-events-<n> (spec 5.4); today's index is placeproto-events-v1. */
export const MANAGED_PREFIX = 'placeproto-events-'
/** Writes go through this alias with require_alias, so a write to a deleted index fails rather than creating one. */
export const writeAlias = (index: string) => `${index}-write`
/** A managed-index write, and each replay request, gives up after this long (spec 4.2). */
export const WRITE_MS = 2_000
const WRITE_OPTS = { requestTimeout: WRITE_MS, maxRetries: 0 }

/**
 * Keeps Elasticsearch from creating a managed index or its write alias by
 * itself: a delete with an external version re-creates a missing index, so
 * after use ends or a candidate is dropped, a late delete would leave a stray
 * index on the volume. Today's index (placeproto-events-v1) and every other
 * name are left as they were, so PR 1's code still works after a revert.
 */
export const AUTO_CREATE = `+${MANAGED_PREFIX}v*,-${MANAGED_PREFIX}*,+*`

/** Creates a managed index: today's mapping, deletes remembered 2 hours (spec 5.4), and its write alias. */
export async function createManagedIndex(es: Elastic, index: string): Promise<void> {
  await es.cluster.putSettings({ persistent: { 'action.auto_create_index': AUTO_CREATE } }, { requestTimeout: 10_000 })
  await es.indices.create({
    index,
    mappings: EVENT_MAPPING,
    settings: { number_of_shards: 1, number_of_replicas: 0, 'index.gc_deletes': '2h' },
    aliases: { [writeAlias(index)]: {} },
  })
}

/** Every index whose name starts with placeproto-events-, today's included. */
export async function listEventIndexes(es: Elastic): Promise<string[]> {
  const res = await es.indices.get({ index: `${MANAGED_PREFIX}*`, expand_wildcards: 'all', allow_no_indices: true }, { requestTimeout: 10_000 })
  return Object.keys(res).sort()
}

/** Deletes one index by name; one already gone counts as deleted. */
export async function deleteIndex(es: Elastic, index: string): Promise<void> {
  await es.indices.delete({ index }, { ignore: [404], requestTimeout: 30_000 })
}

/**
 * Puts a document into a managed index with an external version (spec 5.1):
 * an older version never replaces a newer one, and a conflict (409) means
 * Elasticsearch already holds this version or a newer one, so it counts as
 * success.
 */
export async function putManaged(es: Elastic, index: string, doc: EventDoc, version: bigint): Promise<void> {
  await es.index(
    { index: writeAlias(index), id: doc.uri, document: doc, version: Number(version), version_type: 'external', require_alias: true },
    { ...WRITE_OPTS, ignore: [409] },
  )
}

/** Deletes a document with an external version; a conflict or a missing document counts as success. */
export async function removeManaged(es: Elastic, index: string, uri: string, version: bigint): Promise<void> {
  const res = await es.delete({ index, id: uri, version: Number(version), version_type: 'external' }, { ...WRITE_OPTS, ignore: [404, 409] })
  // A missing index (deleted under us) is not a missing document.
  const error = (res as unknown as { error?: { type?: string } }).error
  if (error?.type === 'index_not_found_exception') throw new Error(`Elasticsearch: index ${index} does not exist`)
}

export type BulkOp = { uri: string; version: bigint; doc?: EventDoc }

/**
 * Writes a batch with external versions (backfill and replay, spec 5.1, 6.4)
 * and returns the uris Elasticsearch now holds at that version or newer
 * (success, a version conflict, or a delete of a missing document).
 */
export async function bulkManaged(es: Elastic, index: string, ops: BulkOp[], opts: { requestTimeout?: number } = {}): Promise<Set<string>> {
  const done = new Set<string>()
  if (ops.length === 0) return done
  const operations: unknown[] = []
  for (const op of ops) {
    if (op.doc) {
      operations.push({ index: { _index: writeAlias(index), _id: op.uri, version: Number(op.version), version_type: 'external', require_alias: true } }, op.doc)
    } else {
      operations.push({ delete: { _index: index, _id: op.uri, version: Number(op.version), version_type: 'external' } })
    }
  }
  const res = await es.bulk({ operations }, { requestTimeout: opts.requestTimeout ?? 30_000, maxRetries: 0 })
  res.items.forEach((item, i) => {
    const r = item.index ?? item.delete
    const status = r?.status ?? 0
    const notFound = item.delete && status === 404 && (r?.error === undefined || (r.error as { type?: string }).type !== 'index_not_found_exception')
    if ((status >= 200 && status < 300) || status === 409 || notFound) done.add(ops[i].uri)
  })
  return done
}

/** Every document id and version in a managed index, after a refresh (reconcile, spec 6.4). */
export async function managedVersions(es: Elastic, index: string): Promise<Map<string, bigint>> {
  await es.indices.refresh({ index })
  const out = new Map<string, bigint>()
  let after: unknown[] | undefined
  for (;;) {
    const res = await es.search({
      index,
      size: 1000,
      _source: false,
      version: true,
      sort: [{ uri: { order: 'asc' } }] as never,
      ...(after ? { search_after: after as never } : {}),
      track_total_hits: false,
    })
    const hits = res.hits.hits
    for (const h of hits) out.set(h._id!, BigInt(h._version!))
    if (hits.length < 1000) return out
    after = hits[hits.length - 1].sort
  }
}

/** The cluster's health status; the keep-awake request (spec 5.2). */
export async function clusterStatus(es: Elastic): Promise<'green' | 'yellow' | 'red'> {
  const res = await es.cluster.health({}, { requestTimeout: 10_000, maxRetries: 0 })
  return res.status.toLowerCase() as 'green' | 'yellow' | 'red'
}

/**
 * An Elasticsearch index behind the search interface (spec 3). From PR 2 the
 * query service reads only the managed index search_control names (withIndex);
 * put and remove keep today's index's PR 1 behavior for tests and tools, and
 * the ingester no longer calls them (spec 5.1).
 */
export class ElasticBackend implements SearchBackend {
  readonly name = 'elastic' as const
  private ensured = false

  constructor(
    readonly es: Elastic,
    readonly index: string,
  ) {}

  /** The same client on another index: the managed index search_control names. */
  withIndex(index: string): ElasticBackend {
    return index === this.index ? this : new ElasticBackend(this.es, index)
  }

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
