#!/usr/bin/env node
// Live acceptance check for a deployed query service (BUILD-005, BUILD-006).
// Lists every sidecar the steward account has published, straight from its
// PDS, then asks the query service for the steward's events near Peace Plaza,
// by distance and by start time, a few per page so the cursor is exercised,
// and checks that each listing returns exactly those events in order. Ends
// with one getEvent and the health report.
//
//   node scripts/acceptance.mjs [https://query.iriai.app]
const BASE = (process.argv[2] ?? process.env.QUERY_URL ?? 'https://query.iriai.app').replace(/\/$/, '')
const STEWARD = process.env.STEWARD_DID ?? 'did:plc:kd5s7zyp7nje43ievnqhgzrr'
const NS = 'app.iriai.proto'
const PEACE_PLAZA = { latitude: '37.785054', longitude: '-122.429827' }
const PAGE = 4

let failures = 0
function check(ok, message) {
  console.log(`${ok ? '✓' : '✗'} ${message}`)
  if (!ok) failures++
}

async function json(url) {
  const res = await fetch(url, { signal: AbortSignal.timeout(15_000) })
  const body = await res.json().catch(() => ({}))
  if (!res.ok) throw new Error(`${res.status} ${body.error ?? ''} ${body.message ?? ''} from ${url}`)
  return body
}

async function publishedSidecars() {
  const doc = await json(`${process.env.PLC_URL ?? 'https://plc.directory'}/${STEWARD}`)
  const pds = doc.service.find((s) => s.id === '#atproto_pds').serviceEndpoint
  const uris = []
  let cursor
  do {
    const q = new URLSearchParams({ repo: STEWARD, collection: `${NS}.event`, limit: '100', ...(cursor ? { cursor } : {}) })
    const page = await json(`${pds}/xrpc/com.atproto.repo.listRecords?${q}`)
    uris.push(...page.records.map((r) => r.uri))
    cursor = page.cursor
  } while (cursor)
  return uris
}

async function listAll(params) {
  const events = []
  let cursor
  let pages = 0
  do {
    const q = new URLSearchParams({ ...params, author: STEWARD, limit: String(PAGE), ...(cursor ? { cursor } : {}) })
    const page = await json(`${BASE}/xrpc/${NS}.queryEvents?${q}`)
    events.push(...page.events)
    cursor = page.cursor
    pages++
  } while (cursor && pages < 50)
  return { events, pages }
}

function metres(location) {
  if (location?.$type !== 'community.lexicon.location.geo') return Infinity
  const rad = (d) => (Number(d) * Math.PI) / 180
  const [a, b] = [rad(PEACE_PLAZA.latitude), rad(location.latitude)]
  const dLat = b - a
  const dLon = rad(location.longitude) - rad(PEACE_PLAZA.longitude)
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(a) * Math.cos(b) * Math.sin(dLon / 2) ** 2
  return 2 * 6371008.7714 * Math.asin(Math.sqrt(h))
}

function sameSet(got, want) {
  return got.length === want.length && new Set(got).size === got.length && want.every((u) => got.includes(u))
}

const expected = await publishedSidecars()
console.log(`Steward ${STEWARD} has ${expected.length} published sidecars. Querying ${BASE}.\n`)

const byDistance = await listAll({ ...PEACE_PLAZA, sort: 'distance' })
const d = byDistance.events.map((e) => metres(e.location))
console.log(`By distance from Peace Plaza (${byDistance.pages} pages of ${PAGE}):`)
byDistance.events.forEach((e, i) => console.log(`  ${String(i + 1).padStart(2)}. ${String(Math.round(d[i])).padStart(5)} m  ${e.event.startsAt}  ${e.event.name}`))
check(sameSet(byDistance.events.map((e) => e.uri), expected), `returns every published steward event once (${byDistance.events.length} of ${expected.length})`)
check(d.every((m, i) => i === 0 || m + 1 >= d[i - 1]), 'nearest first')

const byStart = await listAll({ ...PEACE_PLAZA, sort: 'startsAt' })
const s = byStart.events.map((e) => Date.parse(e.event.startsAt))
console.log(`\nBy start time (${byStart.pages} pages of ${PAGE}):`)
byStart.events.forEach((e, i) => console.log(`  ${String(i + 1).padStart(2)}. ${e.event.startsAt}  ${e.event.name}`))
check(sameSet(byStart.events.map((e) => e.uri), expected), `returns every published steward event once (${byStart.events.length} of ${expected.length})`)
check(s.every((t, i) => i === 0 || t >= s[i - 1]), 'earliest first')

const first = byDistance.events[0]
if (first) {
  const one = await json(`${BASE}/xrpc/${NS}.getEvent?${new URLSearchParams({ uri: first.uri })}`)
  console.log(`\ngetEvent ${first.uri}: ${one.value.event.name}`)
  check(JSON.stringify(one.value) === JSON.stringify(first), 'getEvent returns the same view as queryEvents')
}

const health = await json(`${BASE}/xrpc/_health`)
console.log(`\nHealth: ${JSON.stringify(health)}`)
check(health.index?.ok === true, 'index reachable')

console.log(failures ? `\n${failures} check(s) failed` : '\nAll checks passed')
process.exit(failures ? 1 : 0)
