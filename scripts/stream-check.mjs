#!/usr/bin/env node
// Spike 3 (BUILD-012, D-12): stop, restart and replay on Bluesky's public
// Jetstream, the stream the ingester and the developer recipe read.
//
// For one Jetstream host it checks four things and writes a report:
//   1. Lookback: a cursor 35 hours back starts near where it asked, and one
//      48 hours back starts at the host's floor (or is refused).
//   2. Stop and restart: a reader that stops for a while and resumes from its
//      saved cursor ends up with exactly the events a reader that never
//      stopped saw over the same stretch: none missing, none extra.
//   3. Replay: reading the same stretch twice from the same cursor gives the
//      same events, for a busy collection and for the PlaceProto collections
//      over the last 30 hours.
//   4. Order: positions never go backwards within a connection.
//
//   node scripts/stream-check.mjs [wss://jetstream.us-east.bsky.network]
//
// It uses only Node 22's built-in WebSocket. It cannot run from a cloud
// thread (no route to the Bluesky network); the Stream check workflow runs it.
import { appendFileSync, writeFileSync } from 'node:fs'

const HOST = (process.argv[2] ?? process.env.JETSTREAM_HOST ?? 'wss://jetstream.us-east.bsky.network').replace(/\/$/, '')
// A collection busy enough that a few minutes always carry thousands of
// commits, so the stop/restart comparison is never vacuous.
const BUSY = process.env.BUSY_COLLECTION ?? 'app.bsky.graph.follow'
const PLACEPROTO = ['community.lexicon.calendar.event', 'app.iriai.proto.event']
const LEAD_MS = Number(process.env.LEAD_MS ?? 120_000) // both readers start this far in the past
const RUN_MS = Number(process.env.RUN_MS ?? 60_000) // the subject reads this long before stopping
const STOP_MS = Number(process.env.STOP_MS ?? 90_000) // and stays stopped this long
const AFTER_MS = Number(process.env.AFTER_MS ?? 60_000) // then reads this long after restarting
// How the restart resumes: `seq` (the recipe: the last handled position) or
// `time` (the ingester: the last handled event's time_us minus 5 s, as
// src/ingest/ingester.ts does).
const RESUME_BY = process.env.RESUME_BY ?? 'seq'
const INGESTER_REPLAY_US = 5_000_000
const PLACEPROTO_HOURS = Number(process.env.PLACEPROTO_HOURS ?? 30)
const REPORT = process.env.REPORT ?? 'stream-check-report.md'
const HOUR_US = 3_600_000_000

const v2 = !/\/subscribe$/.test(HOST) && !/jetstream\d\./.test(HOST)
const results = []
let failures = 0
function check(ok, message) {
  console.log(`${ok ? '✓' : '✗'} ${message}`)
  results.push(`- ${ok ? '✓' : '✗'} ${message}`)
  if (!ok) failures++
}
const note = (message) => (console.log(`  ${message}`), results.push(`  - ${message}`))
const iso = (us) => (us ? new Date(us / 1000).toISOString() : 'n/a')
const nowUs = () => Date.now() * 1000

/** Builds the subscribe URL for v1 (`/subscribe`, `wantedCollections`) or v2 (XRPC, `collections`). */
function subscribeUrl(collections, cursor) {
  const url = v2
    ? new URL(`${HOST}/xrpc/network.bsky.jetstream.subscribeEvents`)
    : new URL(HOST.endsWith('/subscribe') ? HOST : `${HOST}/subscribe`)
  for (const c of collections) url.searchParams.append(v2 ? 'collections' : 'wantedCollections', c)
  if (v2) url.searchParams.set('kinds', 'commit')
  if (cursor !== undefined) url.searchParams.set('cursor', String(cursor))
  return url.toString()
}

/**
 * Reads one message into a common shape. v1 sends `{did, time_us, kind,
 * commit: {...}}`; v2 wraps `{did, seq, time, operation, ...}` in
 * `{$type: "message", payload}`. `pos` is what a resume cursor is made of
 * (v2 `seq`, v1 `time_us`); `timeUs` is when the host saw the event.
 */
export function normalize(text) {
  const msg = JSON.parse(text)
  const p = msg.payload ?? msg
  const kind = p.kind ?? (typeof p.$type === 'string' ? p.$type.split('#')[1] : undefined)
  const c = p.commit ?? p
  const pos = Number(p.seq ?? p.cursor ?? p.time_us)
  const t = p.time_us ?? p.time ?? p.timestamp
  const timeUs = typeof t === 'number' ? (t > 1e15 ? t : t * 1000) : t ? Date.parse(t) * 1000 : pos > 1e15 ? pos : undefined
  const key = [pos, p.did, kind, c.collection, c.rkey, c.rev, c.operation].join('|')
  return { pos, timeUs, did: p.did, kind, collection: c.collection, operation: c.operation, key }
}

/**
 * Opens one connection and collects events until `stop()` or `until(evt)`.
 * Resolves with what it read, the last position and why it ended.
 */
function reader(collections, cursor, { until, maxMs = 600_000, idleMs, label } = {}) {
  const events = []
  let last
  let backwards = 0
  let sample
  let ws
  let finish
  const done = new Promise((resolve) => {
    let settled = false
    finish = (reason) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      clearTimeout(idle)
      try { ws?.close() } catch {}
      resolve({ events, last, backwards, sample, reason, label })
    }
    const timer = setTimeout(() => finish('time limit'), maxMs)
    // A sparse filter may have nothing after the target; going quiet this long
    // once connected means the replay has caught up.
    let idle
    const quiet = () => {
      if (!idleMs) return
      clearTimeout(idle)
      idle = setTimeout(() => finish(`caught up (quiet for ${idleMs / 1000} s)`), idleMs)
    }
    ws = new WebSocket(subscribeUrl(collections, cursor), v2 ? ['xrpc.v1.json'] : undefined)
    ws.addEventListener('open', quiet)
    ws.addEventListener('message', (m) => {
      quiet()
      let evt
      try {
        sample ??= String(m.data)
        evt = normalize(String(m.data))
      } catch {
        return
      }
      if (!Number.isFinite(evt.pos)) return
      if (last !== undefined && evt.pos < last) backwards++
      last = evt.pos
      events.push(evt)
      if (until?.(evt)) finish('reached target')
    })
    ws.addEventListener('close', (e) => finish(`closed (${e.code}${e.reason ? ` ${e.reason}` : ''})`))
    ws.addEventListener('error', (e) => finish(`error (${e.message ?? 'connection failed'})`))
  })
  return { done, stop: () => finish('stopped') }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

function compare(a, b, lo, hi) {
  const inRange = (e) => e.pos >= lo && e.pos <= hi
  const A = new Set(a.filter(inRange).map((e) => e.key))
  const B = new Set(b.filter(inRange).map((e) => e.key))
  let missing = 0
  let extra = 0
  for (const k of A) if (!B.has(k)) missing++
  for (const k of B) if (!A.has(k)) extra++
  return { a: A.size, b: B.size, missing, extra }
}

async function lookback() {
  for (const hours of [35, 48]) {
    const asked = nowUs() - hours * HOUR_US
    const r = await reader([BUSY], asked, { until: () => true, maxMs: 30_000, label: `${hours}h` }).done
    const first = r.events[0]
    if (!first) {
      note(`cursor ${hours} h back (${iso(asked)}): no event, ${r.reason}`)
      if (hours === 35) check(false, `a cursor 35 hours back is accepted`)
      continue
    }
    const offsetMin = ((first.timeUs ?? 0) - asked) / 60e6
    note(`cursor ${hours} h back (${iso(asked)}): first event seen at ${iso(first.timeUs)}, ${offsetMin.toFixed(1)} min after the asked time`)
    if (hours === 35) check(Math.abs(offsetMin) < 30, `a cursor 35 hours back resumes where it asked (within 30 min)`)
    else note(`so the oldest point this host replays live is about ${((nowUs() - first.timeUs) / HOUR_US).toFixed(1)} h back`)
  }
}

async function stopRestart() {
  const start = nowUs() - LEAD_MS * 1000
  const deadline = LEAD_MS + RUN_MS + STOP_MS + AFTER_MS + 120_000
  const control = reader([BUSY], start, { maxMs: deadline, label: 'control' })
  const before = reader([BUSY], start, { maxMs: deadline, label: 'subject before stop' })
  await sleep(RUN_MS)
  before.stop()
  const b = await before.done
  const lastHandled = b.events.at(-1)
  const resumeFrom = RESUME_BY === 'time' ? lastHandled.timeUs - INGESTER_REPLAY_US : b.last
  note(`subject read ${b.events.length} events, stopped at ${iso(b.events.at(-1)?.timeUs)}; stopped for ${STOP_MS / 1000} s`)
  await sleep(STOP_MS)
  const after = reader([BUSY], resumeFrom, { maxMs: deadline, label: 'subject after restart' })
  await sleep(AFTER_MS)
  after.stop()
  control.stop()
  const [a, c] = await Promise.all([after.done, control.done])
  check(b.events.length > 0 && a.events.length > 0 && c.events.length > 0, `stop/restart readers all received events (${BUSY})`)
  const subject = [...b.events, ...a.events]
  const hi = Math.min(c.last ?? 0, a.last ?? 0)
  const lo = Math.max(b.events[0]?.pos ?? Infinity, c.events[0]?.pos ?? Infinity)
  const cmp = compare(c.events, subject, lo, hi)
  const overlap = a.events.filter((e) => e.pos <= lastHandled.pos).length
  note(RESUME_BY === 'time'
    ? `resumed as the ingester does, from the last handled event's time minus 5 s (cursor ${resumeFrom}, ${iso(resumeFrom)}); ${overlap} event(s) already handled were delivered again and are dropped as duplicates`
    : `resumed from the last handled position, cursor ${resumeFrom}; ${overlap} event(s) at or before it were delivered again and are dropped as duplicates`)
  note(`compared ${cmp.a} events from the reader that never stopped with ${cmp.b} from the one that stopped and restarted, ${iso(c.events.find((e) => e.pos >= lo)?.timeUs)} to ${iso(c.events.filter((e) => e.pos <= hi).at(-1)?.timeUs)}`)
  check(cmp.missing === 0, `stop and restart lost no events (${cmp.missing} missing)`)
  check(cmp.extra === 0, `stop and restart invented no events (${cmp.extra} extra)`)
  check(b.backwards + a.backwards + c.backwards === 0, `positions never went backwards within a connection`)
  if (c.sample) {
    const m = JSON.parse(c.sample)
    const p = m.payload ?? m
    note(`message fields: ${Object.keys(m).join(', ')}${m.payload ? `; payload: ${Object.keys(p).join(', ')}` : ''}`)
    if (!v2) check(typeof p.time_us === 'number' && p.kind && (p.commit || p.kind !== 'commit'), `v1 messages still carry kind, time_us and commit, which the ingester reads`)
  }
  return c.sample
}

async function replay(collections, fromUs, label) {
  const target = nowUs()
  const read = () => reader(collections, fromUs, { until: (e) => (e.timeUs ?? 0) >= target, maxMs: 300_000, idleMs: 30_000, label }).done
  const first = await read()
  const second = await read()
  const hi = Math.min(first.last ?? 0, second.last ?? 0)
  const cmp = compare(first.events, second.events, -Infinity, hi)
  const kinds = {}
  for (const e of first.events.filter((e) => e.pos <= hi)) kinds[`${e.collection ?? e.kind} ${e.operation ?? ''}`.trim()] = (kinds[`${e.collection ?? e.kind} ${e.operation ?? ''}`.trim()] ?? 0) + 1
  note(`${label}: read from ${iso(fromUs)} twice (${first.reason}; ${second.reason}); ${cmp.a} and ${cmp.b} events`)
  for (const [k, n] of Object.entries(kinds)) note(`${k}: ${n}`)
  check(cmp.missing === 0 && cmp.extra === 0, `${label}: replaying the same stretch gave the same events (${cmp.missing} missing, ${cmp.extra} extra)`)
  return cmp.a
}

const started = new Date()
console.log(`Stream check against ${HOST} (${v2 ? 'v2' : 'v1'})`)
results.push(`### ${HOST} (${v2 ? 'Jetstream v2' : 'Jetstream v1'})`, '', `Started ${started.toISOString()}.`, '')
try {
  results.push('**Lookback**')
  await lookback()
  results.push('', `**Stop and restart** (${BUSY}; resume by ${RESUME_BY}; read ${RUN_MS / 1000} s, stopped ${STOP_MS / 1000} s, read ${AFTER_MS / 1000} s)`)
  const sample = await stopRestart()
  results.push('', '**Replay**')
  await replay([BUSY], nowUs() - 10 * 60_000_000, `${BUSY}, last 10 minutes`)
  const n = await replay(PLACEPROTO, nowUs() - PLACEPROTO_HOURS * HOUR_US, `PlaceProto collections, last ${PLACEPROTO_HOURS} hours`)
  if (n === 0) note('no PlaceProto commits in that window, so this part shows the filter only')
  results.push('', '<details><summary>First raw message</summary>', '', '```json', sample?.slice(0, 1500) ?? '(none)', '```', '</details>')
} catch (err) {
  check(false, `check crashed: ${err.stack ?? err}`)
}
results.push('', `Finished ${new Date().toISOString()}: ${failures === 0 ? 'passed' : `${failures} failure(s)`}.`, '')
writeFileSync(REPORT, results.join('\n') + '\n')
if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, results.join('\n') + '\n')
process.exit(failures === 0 ? 0 : 1)
