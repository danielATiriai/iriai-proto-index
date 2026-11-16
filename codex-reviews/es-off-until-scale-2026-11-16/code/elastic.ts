import { Client, errors } from '@elastic/elasticsearch'
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
