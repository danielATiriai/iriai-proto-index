import { cellToLatLng, getResolution, isValidCell } from 'h3-js'

export const GEO = 'community.lexicon.location.geo'
export const ADDRESS = 'community.lexicon.location.address'
export const FSQ = 'community.lexicon.location.fsq'
export const HTHREE = 'community.lexicon.location.hthree'

/** The initial variants of the open Location union (PlaceProto spec section 2). */
export const LOCATION_TYPES = [GEO, ADDRESS, FSQ, HTHREE] as const
export type KnownLocationType = (typeof LOCATION_TYPES)[number]

export function isKnownLocationType(type: unknown): type is KnownLocationType {
  return (LOCATION_TYPES as readonly unknown[]).includes(type)
}

const DECIMAL = /^-?\d+(\.\d+)?$/

function parseCoordinate(value: unknown, min: number, max: number): number | undefined {
  if (typeof value !== 'string' || !DECIMAL.test(value)) return undefined
  const n = Number(value)
  return Number.isFinite(n) && n >= min && n <= max ? n : undefined
}

/**
 * Writer checks beyond the community schemas (PlaceProto section 2): geo
 * coordinates must be numeric and in range, H3 cells valid, Foursquare ids
 * non-empty. Unknown union members with a $type pass untouched.
 */
export function checkLocation(location: unknown, path = 'location'): string[] {
  if (!location || typeof location !== 'object' || Array.isArray(location)) {
    return [`${path} must be a single Location object`]
  }
  const loc = location as Record<string, unknown>
  const issues: string[] = []
  switch (loc.$type) {
    case GEO:
      if (parseCoordinate(loc.latitude, -90, 90) === undefined)
        issues.push(`${path}.latitude must be a decimal string between -90 and 90`)
      if (parseCoordinate(loc.longitude, -180, 180) === undefined)
        issues.push(`${path}.longitude must be a decimal string between -180 and 180`)
      if (loc.altitude !== undefined && parseCoordinate(loc.altitude, -1e6, 1e6) === undefined)
        issues.push(`${path}.altitude must be a decimal string`)
      break
    case HTHREE:
      if (typeof loc.value !== 'string' || !isValidCell(loc.value))
        issues.push(`${path}.value must be a valid H3 cell`)
      break
    case FSQ:
      if (typeof loc.fsq_place_id !== 'string' || loc.fsq_place_id.trim() === '')
        issues.push(`${path}.fsq_place_id must not be empty`)
      if (loc.latitude !== undefined || loc.longitude !== undefined) {
        if (parseCoordinate(loc.latitude, -90, 90) === undefined || parseCoordinate(loc.longitude, -180, 180) === undefined)
          issues.push(`${path} Foursquare coordinates must both be decimal strings in range`)
      }
      break
    case ADDRESS:
      break
    default:
      if (typeof loc.$type !== 'string' || loc.$type === '') issues.push(`${path}.$type is required`)
  }
  return issues
}

export type ResolvedLocation =
  | { status: 'resolved'; latitude: number; longitude: number; via: 'geo' | 'hthree' | 'fsq'; h3Resolution?: number }
  | { status: 'unresolved'; reason: 'needs-provider' | 'unknown-variant' | 'invalid'; type?: string }

/**
 * Resolves a Location to a point where that can be done locally: geo
 * coordinates, H3 cell centres and Foursquare places that carry coordinates.
 * Addresses and bare Foursquare ids need a provider lookup, which comes after
 * both YC journeys. Unknown variants stay unresolved and are never guessed.
 */
export function resolveLocation(location: unknown): ResolvedLocation {
  if (checkLocation(location).length > 0) return { status: 'unresolved', reason: 'invalid' }
  const loc = location as Record<string, unknown>
  switch (loc.$type) {
    case GEO:
      return { status: 'resolved', via: 'geo', latitude: Number(loc.latitude), longitude: Number(loc.longitude) }
    case HTHREE: {
      const [latitude, longitude] = cellToLatLng(loc.value as string)
      return { status: 'resolved', via: 'hthree', latitude, longitude, h3Resolution: getResolution(loc.value as string) }
    }
    case FSQ:
      return loc.latitude !== undefined
        ? { status: 'resolved', via: 'fsq', latitude: Number(loc.latitude), longitude: Number(loc.longitude) }
        : { status: 'unresolved', reason: 'needs-provider', type: FSQ }
    case ADDRESS:
      return { status: 'unresolved', reason: 'needs-provider', type: ADDRESS }
    default:
      return { status: 'unresolved', reason: 'unknown-variant', type: String(loc.$type) }
  }
}

/**
 * The community event's `locations` array is a compatibility copy written by
 * PlaceProto from the sidecar location. Only variants the community event
 * union knows are copied.
 */
export function compatibilityLocations(location: unknown): unknown[] {
  const type = (location as Record<string, unknown> | null)?.$type
  return isKnownLocationType(type) ? [structuredClone(location)] : []
}
