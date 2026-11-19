import { after, before, beforeEach, describe, it } from 'node:test'
import assert from 'node:assert/strict'
import type { Db } from '../src/db.js'
import { DEFAULT_THRESHOLDS } from '../src/settings.js'
import { GitHubIssues, type Issues } from '../src/search/issues.js'
import { ScaleCheck } from '../src/search/scale.js'
import { BUCKETS_MS, SearchStats, addStats, bucketOf, isTimeout, lastDays, p95 } from '../src/search/stats.js'
import { databaseUrl, freshDb } from './support.js'

const day = (n: number) => new Date(Date.now() - n * 86_400_000).toISOString().slice(0, 10)
const hist = (ms: number, n: number) => {
  const a = new Array(BUCKETS_MS.length + 1).fill(0)
  a[bucketOf(ms)] = n
  return a
}

describe('search stats (spec 6.1)', { skip: !databaseUrl && 'TEST_DATABASE_URL not set' }, () => {
  let db: Db
  before(async () => {
    db = await freshDb('stats_test')
  })
  after(() => db.end())

  it('reads the 250 ms threshold exactly at a bucket edge', () => {
    assert.equal(bucketOf(249.9) < bucketOf(250), true)
    assert.equal(p95(hist(250, 100))!.lower, 250)
    assert.equal(p95(hist(249, 100))!.lower, 240)
    assert.equal(p95(hist(5_000, 1))!.upper, Infinity)
    assert.equal(p95([0, 0]), undefined)
  })

  it('counts a statement timeout and a wait for a search connection as timeouts', () => {
    assert.equal(isTimeout(Object.assign(new Error('canceling statement due to statement timeout'), { code: '57014' })), true)
    assert.equal(isTimeout(new Error('timeout exceeded when trying to connect')), true)
    assert.equal(isTimeout(new Error('syntax error')), false)
  })

  it('adds counts from several replicas, and a failed flush keeps its counts for the next', async () => {
    await addStats(db, { day: day(1), calls: 300, pg: hist(300, 300), pgTimeouts: 0 })
    await addStats(db, { day: day(1), calls: 300, pg: hist(300, 300), pgTimeouts: 4 })
    const [y] = await lastDays(db, 1)
    assert.deepEqual([y.calls, y.pgP95!.lower, y.pgP95!.samples, y.pgTimeoutRate], [600, 300, 600, 4 / 600])

    let fail = true
    const flaky = { query: (text: string, values: unknown[]) => (fail ? Promise.reject(new Error('down')) : db.query(text, values)) } as unknown as Db
    const stats = new SearchStats(flaky, () => {})
    stats.call()
    stats.search(20)
    stats.search(0, true)
    await assert.rejects(stats.flush())
    fail = false
    await stats.flush()
    const today = await db.query<{ calls: string; pg_timeouts: string }>(`select calls::text, pg_timeouts::text from search_stats where day = (now() at time zone 'utc')::date`)
    assert.deepEqual(today.rows[0], { calls: '1', pg_timeouts: '1' })
  })

  it('keeps a day whose flush failed at midnight, and writes it at the next flush', async () => {
    await db.query('delete from search_stats')
    let fail = true
    const flaky = { query: (text: string, values: unknown[]) => (fail ? Promise.reject(new Error('down')) : db.query(text, values)) } as unknown as Db
    const stats = new SearchStats(flaky, () => {})
    stats.call()
    stats.call()
    // The counts are yesterday's; the next call rolls the day, and that flush fails.
    ;(stats as unknown as { day: string }).day = day(1)
    stats.call()
    await new Promise((r) => setImmediate(r))
    fail = false
    await stats.flush()
    const rows = await db.query<{ day: string; calls: string }>(`select day::text, calls::text from search_stats order by day`)
    assert.deepEqual(rows.rows, [
      { day: day(1), calls: '2' },
      { day: day(0), calls: '1' },
    ])
  })
})

describe('GitHub issues', { skip: !databaseUrl && 'TEST_DATABASE_URL not set' }, () => {
  let db: Db
  beforeEach(async () => {
    await db?.end()
    db = await freshDb('issues_test')
  })
  after(() => db.end())
  const fake = (sent: string[], ok = true) =>
    (async (_url: string, init: { body: string }) => {
      sent.push(JSON.parse(init.body).title)
      return { ok, status: ok ? 201 : 500 }
    }) as unknown as typeof fetch

  it('opens one issue per cause a day', async () => {
    const sent: string[] = []
    const issues = new GitHubIssues(db, 'token', 'o/r', () => {}, fake(sent))
    await issues.open('k', 'first', 'body')
    await issues.open('k', 'again', 'body')
    await issues.open('other', 'second', 'body')
    assert.deepEqual(sent, ['first', 'second'])
  })

  it('without a token only logs, and opens the issue once a token is set', async () => {
    const logs: string[] = []
    await new GitHubIssues(db, undefined, 'o/r', (m) => logs.push(m)).open('k', 'title', 'body')
    assert.match(logs[0], /GITHUB_ALERT_TOKEN not set/)
    const sent: string[] = []
    await new GitHubIssues(db, 'token', 'o/r', () => {}, fake(sent)).open('k', 'title', 'body')
    assert.deepEqual(sent, ['title'], 'the cause was not held for a day by the undelivered one')
  })

  it('tries again at the next call when GitHub refuses', async () => {
    const sent: string[] = []
    await new GitHubIssues(db, 'token', 'o/r', () => {}, fake(sent, false)).open('k', 'title', 'body')
    await new GitHubIssues(db, 'token', 'o/r', () => {}, fake(sent)).open('k', 'title', 'body')
    assert.deepEqual(sent, ['title', 'title'])
  })
})

describe('the scale check (spec 6.2)', { skip: !databaseUrl && 'TEST_DATABASE_URL not set' }, () => {
  let db: Db
  let opened: string[]
  const issues: Issues = { open: async (key) => void opened.push(key) }
  beforeEach(async () => {
    await db?.end()
    db = await freshDb('scale_test')
    opened = []
    await db.query(`update search_control set pg_verified = true, first_fill_done = true, fill_generation = search_generation_now()`)
  })
  after(() => db.end())

  it('opens an issue at the event threshold, records the current count, and contacts nothing else', async () => {
    const check = new ScaleCheck(db, { ...DEFAULT_THRESHOLDS, onEvents: 0 }, issues, () => {})
    await check.tick()
    assert.deepEqual(opened, ['scale'])
    assert.match(check.last!.trigger!, /0 events/)
    const today = await db.query<{ events: string }>(`select events::text from search_stats where day = (now() at time zone 'utc')::date`)
    assert.equal(today.rows[0].events, '0')
    opened = []
    await new ScaleCheck(db, DEFAULT_THRESHOLDS, issues, () => {}).tick()
    assert.deepEqual(opened, [])
  })

  it('needs 3 slow days with at least 500 calls each, or over 1% timeouts', async () => {
    const check = new ScaleCheck(db, DEFAULT_THRESHOLDS, issues, () => {})
    for (const n of [1, 2]) await addStats(db, { day: day(n), calls: 600, pg: hist(260, 600), pgTimeouts: 0 })
    assert.equal(await check.onReason(5), undefined)
    await addStats(db, { day: day(3), calls: 499, pg: hist(260, 499), pgTimeouts: 0 })
    assert.equal(await check.onReason(5), undefined, 'a day under 500 calls does not count')
    await addStats(db, { day: day(3), calls: 1, pg: hist(260, 1), pgTimeouts: 0 })
    assert.match((await check.onReason(5))!, /3 consecutive days/)

    await db.query('truncate search_stats')
    const mixed = hist(20, 980)
    mixed[bucketOf(2_000)] = 20
    for (const n of [1, 2, 3]) await addStats(db, { day: day(n), calls: 1000, pg: mixed, pgTimeouts: 11 })
    assert.match((await check.onReason(5))!, /timeouts/)
  })

  it('opens an issue once Postgres has been unverified for three checks in a row', async () => {
    const check = new ScaleCheck(db, DEFAULT_THRESHOLDS, issues, () => {})
    await db.query('update search_control set pg_verified = false')
    await check.tick()
    await check.tick()
    assert.deepEqual(opened, [])
    await check.tick()
    assert.deepEqual(opened, ['unverified'])
    await db.query('update search_control set pg_verified = true')
    await check.tick()
    await db.query('update search_control set pg_verified = false')
    opened = []
    await check.tick()
    assert.deepEqual(opened, [], 'the count starts again after a verified check')
  })
})
