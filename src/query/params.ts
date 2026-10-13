import { AtUri, isValidDatetime, isValidDid } from '@atproto/syntax'
import { config } from '../contracts/contracts.js'

export type Sort = 'distance' | 'startsAt' | 'uri'

/** queryEvents parameters after validation and normalization (PlaceProto §5). */
export interface EventQuery {
  latitude?: number
  longitude?: number
  radiusMeters?: number
  box?: { west: number; south: number; east: number; north: number }
  windowStart?: string
  windowEnd?: string
  eventUri?: string
  author?: string[]
  sort: Sort
  limit: number
  cursor?: string
}

export class InvalidRequest extends Error {
  readonly error = 'InvalidRequest'
}

const COORDINATE = /^-?\d+(\.\d+)?$/
const MAX_RADIUS = 20_100_000

function coordinate(params: URLSearchParams, name: string, min: number, max: number): number | undefined {
  const raw = single(params, name)
  if (raw === undefined) return undefined
  if (raw.length > 32 || !COORDINATE.test(raw)) throw new InvalidRequest(`${name} must be a decimal-degree string`)
  const n = Number(raw)
  if (!Number.isFinite(n) || n < min || n > max) throw new InvalidRequest(`${name} must be between ${min} and ${max}`)
  return n
}

function single(params: URLSearchParams, name: string): string | undefined {
  const all = params.getAll(name)
  if (all.length > 1) throw new InvalidRequest(`${name} may be given once`)
  return all[0]
}

function datetime(params: URLSearchParams, name: string): string | undefined {
  const raw = single(params, name)
  if (raw === undefined) return undefined
  if (!isValidDatetime(raw)) throw new InvalidRequest(`${name} must be a datetime`)
  return new Date(raw).toISOString()
}

function integer(params: URLSearchParams, name: string, min: number, max: number): number | undefined {
  const raw = single(params, name)
  if (raw === undefined) return undefined
  if (!/^\d+$/.test(raw)) throw new InvalidRequest(`${name} must be an integer`)
  const n = Number(raw)
  if (n < min || n > max) throw new InvalidRequest(`${name} must be between ${min} and ${max}`)
  return n
}

export function parseEventQuery(params: URLSearchParams): EventQuery {
  const latitude = coordinate(params, 'latitude', -90, 90)
  const longitude = coordinate(params, 'longitude', -180, 180)
  if ((latitude === undefined) !== (longitude === undefined)) {
    throw new InvalidRequest('latitude and longitude must be given together')
  }
  const hasPoint = latitude !== undefined

  const radiusMeters = integer(params, 'radiusMeters', 1, MAX_RADIUS)
  if (radiusMeters !== undefined && !hasPoint) throw new InvalidRequest('radiusMeters requires latitude and longitude')

  const edges = {
    west: coordinate(params, 'west', -180, 180),
    south: coordinate(params, 'south', -90, 90),
    east: coordinate(params, 'east', -180, 180),
    north: coordinate(params, 'north', -90, 90),
  }
  const given = Object.values(edges).filter((v) => v !== undefined).length
  if (given !== 0 && given !== 4) throw new InvalidRequest('a bounding box needs west, south, east and north')
  let box: EventQuery['box']
  if (given === 4) {
    const b = edges as { west: number; south: number; east: number; north: number }
    if (!(b.south < b.north)) throw new InvalidRequest('south must be less than north')
    if (b.west === b.east) throw new InvalidRequest('west and east must differ')
    box = b
  }

  const windowStart = datetime(params, 'windowStart')
  const windowEnd = datetime(params, 'windowEnd')
  if (windowStart && windowEnd && !(windowStart < windowEnd)) {
    throw new InvalidRequest('windowStart must be before windowEnd')
  }

  const eventUri = single(params, 'eventUri')
  if (eventUri !== undefined) {
    let uri: AtUri
    try {
      uri = new AtUri(eventUri)
    } catch {
      throw new InvalidRequest('eventUri must be an AT-URI')
    }
    if (!isValidDid(uri.host)) throw new InvalidRequest('eventUri must use a DID, not a handle')
    if (uri.collection !== config.communityEventCollection || !uri.rkey) {
      throw new InvalidRequest(`eventUri must name a ${config.communityEventCollection} record`)
    }
  }

  const authors = params.getAll('author')
  if (authors.length > 25) throw new InvalidRequest('author accepts at most 25 DIDs')
  for (const did of authors) if (!isValidDid(did)) throw new InvalidRequest(`author ${did} is not a DID`)

  const sortParam = single(params, 'sort')
  if (sortParam !== undefined && sortParam !== 'distance' && sortParam !== 'startsAt') {
    throw new InvalidRequest('sort must be distance or startsAt')
  }
  if (sortParam === 'distance' && !hasPoint) throw new InvalidRequest('sort=distance requires latitude and longitude')
  const sort: Sort = sortParam ?? (hasPoint ? 'distance' : 'uri')

  return {
    ...(hasPoint ? { latitude, longitude } : {}),
    ...(radiusMeters !== undefined ? { radiusMeters } : {}),
    ...(box ? { box } : {}),
    ...(windowStart ? { windowStart } : {}),
    ...(windowEnd ? { windowEnd } : {}),
    ...(eventUri ? { eventUri } : {}),
    ...(authors.length ? { author: [...new Set(authors)].sort() } : {}),
    sort,
    limit: integer(params, 'limit', 1, 100) ?? 50,
    ...(single(params, 'cursor') ? { cursor: single(params, 'cursor') } : {}),
  }
}

/** Everything a cursor is bound to: the normalized filters, coordinate and sort, not limit or cursor. */
export function bindingOf(q: EventQuery): string {
  const { limit: _limit, cursor: _cursor, ...binding } = q
  return canonical(binding)
}

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`
  if (value && typeof value === 'object') {
    const obj = value as Record<string, unknown>
    return `{${Object.keys(obj).sort().map((k) => `${JSON.stringify(k)}:${canonical(obj[k])}`).join(',')}}`
  }
  return JSON.stringify(value)
}
