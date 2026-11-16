import type { EventQuery } from './params.js'

/**
 * Builds the Elasticsearch filter and sort for a queryEvents request
 * (PlaceProto §5). All filters combine with AND. Explicit geographic bounds
 * need a resolved point, so they exclude unresolved locations; distance order
 * alone keeps them, after every resolved one. Ties break on the sidecar
 * AT-URI, ascending by bytes (keyword order).
 */
export function buildSearch(q: EventQuery): { query: object; sort: object[] } {
  const filter: object[] = []
  const point = q.latitude !== undefined ? { lat: q.latitude, lon: q.longitude! } : undefined

  if (q.radiusMeters !== undefined && point) {
    filter.push({ geo_distance: { distance: `${q.radiusMeters}m`, distance_type: 'arc', point } })
  }
  if (q.box) {
    // west > east crosses the antimeridian; Elasticsearch handles that box as given.
    filter.push({
      geo_bounding_box: {
        point: { top_left: { lat: q.box.north, lon: q.box.west }, bottom_right: { lat: q.box.south, lon: q.box.east } },
      },
    })
  }
  if (q.windowStart || q.windowEnd) filter.push(timeWindow(q.windowStart, q.windowEnd))
  if (q.author) filter.push({ terms: { did: q.author } })
  if (q.eventUri) filter.push({ term: { eventUri: q.eventUri } })

  const byUri = { uri: { order: 'asc' } }
  let sort: object[]
  switch (q.sort) {
    case 'distance':
      // Great-circle (arc) distance in meters, unrounded; documents with no
      // point sort after all others.
      sort = [{ _geo_distance: { point, order: 'asc', unit: 'm', distance_type: 'arc', mode: 'min', ignore_unmapped: true } }, byUri]
      break
    case 'startsAt':
      sort = [{ startsAt: { order: 'asc', missing: '_last' } }, byUri]
      break
    default:
      sort = [byUri]
  }
  return { query: { bool: { filter } }, sort }
}

/**
 * Interval overlap with inclusive starts and exclusive ends. With both
 * timestamps an event matches when startsAt < windowEnd and endsAt >
 * windowStart, so events underway at windowStart match. With startsAt only,
 * it is a point: windowStart <= startsAt < windowEnd. Without startsAt, or
 * with an end before its start, it never matches a time filter.
 */
export function timeWindow(windowStart?: string, windowEnd?: string): object {
  const withEnd: object[] = [{ exists: { field: 'endsAt' } }]
  const pointOnly: object[] = []
  if (windowEnd) {
    withEnd.push({ range: { startsAt: { lt: windowEnd } } })
    pointOnly.push({ range: { startsAt: { lt: windowEnd } } })
  }
  if (windowStart) {
    withEnd.push({ range: { endsAt: { gt: windowStart } } })
    pointOnly.push({ range: { startsAt: { gte: windowStart } } })
  }
  return {
    bool: {
      filter: [{ exists: { field: 'startsAt' } }, { term: { timeValid: true } }],
      should: [
        { bool: { filter: withEnd } },
        { bool: { filter: pointOnly, must_not: [{ exists: { field: 'endsAt' } }] } },
      ],
      minimum_should_match: 1,
    },
  }
}
