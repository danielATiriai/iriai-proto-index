#!/usr/bin/env node
// The PlaceProto change stream recipe (BUILD-012, D-13): follow PlaceProto
// events as they are written anywhere on the network, from Bluesky's public
// Jetstream, and resume after a stop without losing or double-handling any.
//
//   node scripts/stream-recipe.mjs                 # live from now, or resume
//   node scripts/stream-recipe.mjs --since 24h     # first run: start 24 hours back
//   node scripts/stream-recipe.mjs --for 60s       # stop after a minute
//
// Node 22 or later, no dependencies. The cursor is kept in .placeproto-cursor
// (CURSOR_FILE). Jetstream replays the last 36 hours from a saved cursor with
// no key; a cursor older than that cannot resume (see docs/stream-recipe.md).
import { existsSync, readFileSync, writeFileSync } from 'node:fs'

const JETSTREAM = process.env.JETSTREAM ?? 'wss://jetstream.us-east.bsky.network'
const QUERY = process.env.QUERY ?? 'https://query.iriai.app'
const CURSOR_FILE = process.env.CURSOR_FILE ?? '.placeproto-cursor'
const COLLECTIONS = ['community.lexicon.calendar.event', 'app.iriai.proto.event']
const LOOKBACK_US = 36 * 3_600_000_000

const arg = (name) => { const i = process.argv.indexOf(name); return i > 0 ? process.argv[i + 1] : undefined }
const duration = (s) => s && Number(s.slice(0, -1)) * { s: 1e3, m: 6e4, h: 3.6e6 }[s.at(-1)]

// 1. Where to start: the saved cursor, else --since, else live.
const saved = existsSync(CURSOR_FILE) ? JSON.parse(readFileSync(CURSOR_FILE, 'utf8')) : undefined
let cursor = saved?.cursor
if (saved && Date.now() * 1000 - saved.timeUs > LOOKBACK_US) {
  console.error(`The saved cursor is from ${new Date(saved.timeUs / 1000).toISOString()}, older than Jetstream's 36-hour lookback.`)
  console.error('Resync what you need from the query API (queryEvents), delete the cursor file, and start again.')
  process.exit(1)
}
if (cursor === undefined && arg('--since')) cursor = (Date.now() - duration(arg('--since'))) * 1000 // a time in µs is accepted as a cursor

// 2. Subscribe to the two PlaceProto record types only.
const url = new URL(`${JETSTREAM}/xrpc/network.bsky.jetstream.subscribeEvents`)
for (const c of COLLECTIONS) url.searchParams.append('collections', c)
if (cursor !== undefined) url.searchParams.set('cursor', String(cursor))
console.log(`Following ${COLLECTIONS.join(' and ')} ${cursor === undefined ? 'live' : `from cursor ${cursor}`}`)
const ws = new WebSocket(url, ['xrpc.v1.json'])

// 3. Handle each event once, in order. The cursor is inclusive and delivery is
// at least once, so anything at or before the last handled position (the saved
// cursor, after a restart) is skipped. Positions only grow.
let last = saved
let queue = Promise.resolve()
ws.addEventListener('message', (m) => { queue = queue.then(() => onMessage(m.data)) })
async function onMessage(data) {
  const msg = JSON.parse(String(data))
  const e = msg.payload ?? msg
  const kind = e.kind ?? e.$type?.split('#')[1]
  const seq = Number(e.seq ?? e.cursor ?? e.time_us)
  if (last && seq <= last.cursor) return
  const timeUs = e.time_us ?? (e.time ? Date.parse(e.time) * 1000 : Date.now() * 1000)
  if (kind === 'commit') {
    const c = e.commit ?? e
    await handle(c.operation, c.collection, `at://${e.did}/${c.collection}/${c.rkey}`, c.record)
  } else if (kind === 'account' && (e.account ?? e).active === false) {
    console.log(`account ${e.did} is ${(e.account ?? e).status ?? 'inactive'}: hide its events`)
  }
  last = { cursor: seq, timeUs }
}

// 4. Pair a sidecar with its event. A sidecar names its event by eventUri in
// the same repository; the query API returns the complete pair (or 404 until
// both records exist and the index has caught up, usually seconds).
async function handle(operation, collection, uri, record) {
  const when = new Date().toISOString().slice(11, 19)
  if (operation === 'delete') return console.log(`${when} deleted ${uri}`)
  if (collection === 'community.lexicon.calendar.event') return console.log(`${when} ${operation} event "${record?.name}" ${uri}`)
  console.log(`${when} ${operation} sidecar ${uri} for ${record?.eventUri} at ${record?.location?.name ?? record?.location?.$type}`)
  try {
    const res = await fetch(`${QUERY}/xrpc/app.iriai.proto.getEvent?uri=${encodeURIComponent(uri)}`, { signal: AbortSignal.timeout(10_000) })
    const body = await res.json()
    console.log(res.ok ? `         paired: "${body.event?.record?.name ?? body.event?.name ?? '?'}"` : `         not paired yet: ${body.error}`)
  } catch (err) {
    console.log(`         query API unreachable: ${err.message}`)
  }
}

// 5. Save the cursor every second, and on the way out. Resuming from it
// replays from that event on, which step 3 makes harmless.
const save = () => last && last !== saved && writeFileSync(CURSOR_FILE, JSON.stringify(last))
const timer = setInterval(save, 1000)
const stop = () => { clearInterval(timer); ws.close() }
process.on('SIGINT', stop)
if (arg('--for')) setTimeout(stop, duration(arg('--for')))
ws.addEventListener('close', async (e) => { clearInterval(timer); await queue; save(); console.log(`closed (${e.code}); cursor saved to ${CURSOR_FILE}`) })
ws.addEventListener('error', (e) => console.error(`connection failed: ${e.message ?? 'see above'}`))
