import { checkout, type Db } from '../db.js'
import type { ElasticBackend } from '../index/elastic.js'
import type { Thresholds } from '../settings.js'
import { backendsAgree } from './agreement.js'
import { readControl, type ControlRow, type ControllerState } from './control.js'
import type { Issues } from './issues.js'
import type { ManagedIndex } from './managed.js'
import type { PostgresBackend } from './postgres.js'
import { lastDays, pruneStats, recordEvents } from './stats.js'

/** One controller at a time across ingesters (spec 6): a session lock on a dedicated connection. */
export const CONTROLLER_LOCK = 7140017
const DAY_MS = 86_400_000
/** States in which Elasticsearch is in use whatever SEARCH_ES says (spec 5.2). */
const IN_USE: ReadonlySet<ControllerState> = new Set(['starting', 'backfilling', 'verifying', 'on', 'draining'])
const START_TIMEOUT_MS = 30 * 60_000
const FALLBACKS_TO_DRAIN = 20
const SWITCH_ON_GAP_DAYS = 30
const FAILURES_TO_DISABLE = 2
const ON_DAYS = 3
const OFF_DAYS = 14
const MIN_ON_DAYS = 30
/** A pending write this old is stuck: an issue, and an Elasticsearch that is on drains. */
const STUCK_MS = 30 * 60_000

export interface ControllerDeps {
  db: Db
  managed: ManagedIndex
  postgres: PostgresBackend
  /** Today's index name (ELASTICSEARCH_INDEX), deleted an hour after PR 2 deploys. */
  legacyIndex: string
  /** Elasticsearch on any managed index. */
  elastic: ElasticBackend
  issues: Issues
  /** SEARCH_ES: Elasticsearch in use whatever the state. */
  searchEs: boolean
  thresholds: Thresholds
  /** When this ingester started, for the 30-minute keep-awake check (spec 6.4, on). */
  startedAt: Date
  log?: (msg: string) => void
}

/**
 * The scale controller (spec 6), in the ingester every 10 minutes: measures,
 * switches Elasticsearch on and off through the states of 6.4, and starts and
 * ends Elasticsearch use (5.2). Under D3 A, "on" is using Elasticsearch,
 * which wakes it; "off" is sending it nothing, so Railway's Serverless puts
 * it to sleep. The keep-awake and replay every 2 minutes are keep().
 */
export class SearchController {
  private log: (msg: string) => void
  private keeping = false

  constructor(private deps: ControllerDeps) {
    this.log = deps.log ?? console.log
  }

  /** One tick under the controller lock; an ingester that does not get it skips this tick. */
  async tick(): Promise<void> {
    let client
    try {
      client = await checkout(this.deps.db)
    } catch (err) {
      this.log(`search controller: ${(err as Error).message}`)
      return
    }
    let holds = false
    try {
      holds = (await client.query<{ ok: boolean }>('select pg_try_advisory_lock($1) as ok', [CONTROLLER_LOCK])).rows[0].ok
      if (!holds) return
      try {
        await this.tickLocked()
      } finally {
        holds = !(await client.query<{ ok: boolean }>('select pg_advisory_unlock($1) as ok', [CONTROLLER_LOCK])).rows[0].ok
      }
    } catch (err) {
      holds = true
      this.log(`search controller: ${(err as Error).message}`)
    } finally {
      client.release(holds ? new Error('controller lock not confirmed released') : undefined)
    }
  }

  /**
   * Every 2 minutes while Elasticsearch is in use, and once at start-up: a
   * cluster-health request (which keeps a Serverless Elasticsearch awake and
   * records es_seen_at) and the replay of unconfirmed writes older than 2 s.
   */
  async keep(): Promise<void> {
    if (this.keeping) return
    this.keeping = true
    try {
      const { esInUse } = await this.deps.managed.state()
      if (!esInUse) return
      await this.deps.managed.keepAwake()
      await this.deps.managed.replay()
    } catch (err) {
      this.log(`elasticsearch keep-awake or replay failed: ${(err as Error).message}`)
    } finally {
      this.keeping = false
    }
  }

  private async tickLocked(): Promise<void> {
    const { db } = this.deps
    await pruneStats(db).catch((err) => this.log(`search stats prune failed: ${(err as Error).message}`))
    const events = await this.deps.postgres.count()
    await recordEvents(db, events)
    await this.step(await readControl(db), events)
    await this.manageUse(await readControl(db))
  }

  /** Moves through the states of spec 6.4, at most one step per tick except off → ready → starting. */
  private async step(row: ControlRow, events: number): Promise<void> {
    const { managed } = this.deps
    switch (row.state) {
      case 'off': {
        if (row.disabled) return
        const why = await this.onReason(events)
        if (!why) return
        await this.setState('ready', why)
        return this.step({ ...row, state: 'ready' }, events)
      }
      case 'ready': {
        if (row.disabled) return this.setState('off', 'disabled after repeated failures; a person must clear search_control.disabled')
        const last = await this.lastSwitchOn()
        if (last && Date.now() - last.getTime() < SWITCH_ON_GAP_DAYS * DAY_MS) return
        const why = await this.onReason(events)
        if (!why) return this.setState('off', 'the switch-on condition no longer holds')
        await this.deps.issues.open('switch-on', 'Search: turning Elasticsearch on', `The scale trigger was met: ${why}. The controller is turning Elasticsearch on, filling and checking it, then moving new searches to it.`)
        await managed.setInUse(true)
        return this.setState('starting', why)
      }
      case 'starting': {
        try {
          const status = await managed.keepAwake()
          if (status === 'green' || status === 'yellow') return this.setState('backfilling', `cluster ${status}`)
        } catch (err) {
          this.log(`search controller: Elasticsearch not answering yet: ${(err as Error).message}`)
        }
        if (Date.now() - row.since.getTime() > START_TIMEOUT_MS) return this.fail(row, 'Elasticsearch did not reach yellow or green within 30 minutes')
        return
      }
      case 'backfilling': {
        try {
          // A managed index SEARCH_ES already built and marked current is used
          // as it is: open sessions may be reading it. A new one stays a
          // candidate, not current, until verified, so no session uses it and
          // a failure deletes it at once.
          if (row.esCurrent && row.elasticIndex) return this.setState('verifying', `checking the current ${row.elasticIndex}`)
          const index = await managed.build(this.deps.legacyIndex)
          return this.setState('verifying', `built ${index}`)
        } catch (err) {
          this.log(`search controller: build failed: ${(err as Error).message}`)
          return this.fail(row, 'the build of the managed index failed (details in the ingester log)')
        }
      }
      case 'verifying': {
        try {
          const note = await this.verify()
          if (note) return this.fail(row, note)
          await managed.markCurrent(row.elasticIndex!)
          // A switch-on is one that reached on (spec 6.4): a failed attempt
          // counts as a failure instead, so two failures can disable.
          await this.deps.db.query('update search_control set last_switch_on = now() where id = 1')
          return this.setState('on', 'verified: the managed index matches Postgres')
        } catch (err) {
          this.log(`search controller: verifying could not run: ${(err as Error).message}`)
          return this.fail(row, 'the verifying check could not run (details in the ingester log)')
        }
      }
      case 'on': {
        const drain = await this.drainReason(row, events)
        if (!drain) return
        if (drain.issue) await this.deps.issues.open('drain', 'Search: Elasticsearch is being drained', drain.why)
        return this.setState('draining', drain.why)
      }
      case 'draining':
        // Use ends in manageUse once elastic_cursors_until has passed.
        return
    }
  }

  /**
   * Starts and ends Elasticsearch use (spec 5.2): in use while SEARCH_ES is
   * on or the state is starting to draining. A use that begins with no
   * current managed index builds one. Use ends only once Postgres is verified
   * and elastic_cursors_until has passed; draining then returns to off.
   */
  private async manageUse(row: ControlRow): Promise<void> {
    const { managed, legacyIndex } = this.deps
    const wanted = this.deps.searchEs || (IN_USE.has(row.state) && row.state !== 'draining')
    if (wanted) {
      if (!row.esInUse) await managed.setInUse(true)
      // With SEARCH_ES on, use does not end; draining has already moved new
      // sessions to Postgres, which off keeps doing.
      if (row.state === 'draining') return this.setState('off', 'drained; SEARCH_ES=on keeps Elasticsearch in use')
      try {
        if (!row.esCurrent && !['starting', 'backfilling', 'verifying'].includes(row.state)) {
          const index = await managed.build(legacyIndex)
          await managed.markCurrent(index)
        }
        await managed.dropLegacyWhenDue(legacyIndex)
        if ((await managed.stalePending(STUCK_MS)) > 0) {
          await this.deps.issues.open(
            'pending',
            'Search: Elasticsearch writes have been pending for over 30 minutes',
            'The managed index may not serve while they wait, so new sessions use Postgres. Details are in the ingester log.',
          )
        }
        const fresh = await readControl(this.deps.db)
        if (fresh.esCurrent && fresh.elasticIndex && (await this.reconcileDue())) {
          const n = await managed.reconcile(fresh.elasticIndex)
          if (n) this.log(`elasticsearch: daily reconcile found ${n} differences; replaying`)
          await managed.replay(0)
        }
      } catch (err) {
        this.log(`elasticsearch: ${(err as Error).message}`)
        await this.deps.issues.open(
          'build',
          'Search: the managed Elasticsearch index could not be built or reconciled',
          'Details are in the ingester log. Postgres keeps serving new sessions; the controller retries every 10 minutes.',
        )
      }
      return
    }
    if (!row.esInUse) {
      if (row.state === 'draining') await this.setState('off', 'Elasticsearch use has ended')
      if (!row.pgVerified) {
        await this.deps.issues.open(
          'unverified-outage',
          'Search: Postgres is not verified and Elasticsearch is not in use, so search answers 503',
          'Upkeep fills and compares Postgres again every 10 minutes. Setting SEARCH_ES=on on the ingester builds a managed index, which SEARCH_BACKEND_FORCE=elastic on the query service can then serve.',
        )
      }
      return
    }
    if (!row.pgVerified) {
      const why = 'Elasticsearch use cannot end before Postgres is verified (spec 5.2); SEARCH_ES=off is ignored until then'
      await this.note(why)
      await this.deps.issues.open('unverified', 'Search: Postgres is not verified, so Elasticsearch stays in use', why)
      return
    }
    if (await managed.endUse(legacyIndex)) {
      this.log('elasticsearch: use ended; nothing more is sent to it')
      if (row.state === 'draining') await this.setState('off', 'Elasticsearch use has ended')
    }
  }

  /** Why Elasticsearch should switch on (spec 6.2, D2 A), or undefined. */
  async onReason(events: number): Promise<string | undefined> {
    const t = this.deps.thresholds
    if (events >= t.onEvents) return `${events} events, at least ${t.onEvents}`
    const days = await lastDays(this.deps.db, ON_DAYS)
    const slow = days.every(
      (d) => d.calls >= t.onMinCalls && !!d.pgP95 && (d.pgP95.lower >= t.onP95Ms || d.pgTimeoutRate > t.onTimeoutRate),
    )
    if (slow) return `on ${ON_DAYS} consecutive days with at least ${t.onMinCalls} calls, Postgres p95 at least ${t.onP95Ms} ms or over ${t.onTimeoutRate * 100}% timeouts`
    return undefined
  }

  /** Why an Elasticsearch that is on should drain (spec 6.2, 6.4), or undefined. */
  async drainReason(row: ControlRow, events: number): Promise<{ why: string; issue: boolean } | undefined> {
    const { db, startedAt } = this.deps
    const health = (
      await db.query<{ fallbacks: string; seen_recent: boolean; silent: boolean; status: string | null }>(
        `select (select count(*) from search_fallbacks where at > now() - interval '10 minutes')::text as fallbacks,
                coalesce(es_seen_at > now() - interval '5 minutes', false) as seen_recent,
                greatest(coalesce(es_seen_at, '-infinity'), $1::timestamptz) < now() - interval '30 minutes' as silent,
                es_status as status
         from search_control where id = 1`,
        [startedAt],
      )
    ).rows[0]
    if (await this.deps.managed.stalePending(STUCK_MS)) {
      return { why: 'a write has been pending for over 30 minutes, so the managed index may not serve', issue: true }
    }
    if (Number(health.fallbacks) > FALLBACKS_TO_DRAIN && health.seen_recent) {
      return { why: `${health.fallbacks} first pages fell back to Postgres within 10 minutes while Elasticsearch answered its keep-awake`, issue: true }
    }
    if (health.status === 'red') return { why: 'the Elasticsearch cluster is red', issue: true }
    if (health.silent) return { why: 'no keep-awake has been answered for 30 minutes', issue: true }
    const t = this.deps.thresholds
    if (Date.now() - row.since.getTime() < MIN_ON_DAYS * DAY_MS) return undefined
    const days = await lastDays(db, OFF_DAYS)
    const quiet = days.every((d) => (d.calls < t.onMinCalls || (!!d.pgP95 && d.pgP95.upper <= t.offP95Ms)) && d.events !== null && d.events <= t.offEvents)
    if (quiet && events <= t.offEvents) {
      return { why: `on ${OFF_DAYS} consecutive days Postgres's p95 was at most ${t.offP95Ms} ms (or fewer than ${t.onMinCalls} calls) with at most ${t.offEvents} events`, issue: false }
    }
    return undefined
  }

  /**
   * The verifying check (spec 6.4): after a replay and a refresh, no pending
   * rows and equal id-and-version sets; then the first 50 results of both
   * backends for each query shape. Returns a failure note, or undefined.
   */
  private async verify(): Promise<string | undefined> {
    const { managed, db } = this.deps
    const row = await readControl(db)
    if (!row.elasticIndex) return 'no managed index to verify'
    for (let attempt = 0; ; attempt++) {
      await managed.replay(0)
      const differences = await managed.reconcile(row.elasticIndex)
      if (differences === 0 && (await managed.stalePending()) === 0) break
      if (attempt >= 3) return `the managed index still differs from Postgres in ${differences} documents after 3 replays`
    }
    const agreed = await backendsAgree(db, this.deps.postgres, this.deps.elastic.withIndex(row.elasticIndex))
    if (!agreed.ok) {
      this.log(`search controller: ${agreed.detail}`)
      return agreed.note
    }
    this.log(`elasticsearch: verified on ${agreed.shapes} query shapes`)
    return undefined
  }

  /**
   * A failure in starting, backfilling or verifying (spec 6.4): back to off,
   * which ends use unless SEARCH_ES is on; a candidate index no session used
   * is deleted at once; the failure is counted, and two in 30 days disable
   * the controller. Opens an issue.
   */
  private async fail(row: ControlRow, why: string): Promise<void> {
    const res = await this.deps.db.query<{ n: number; disabled: boolean }>(
      `update search_control
         set failures = array(select f from unnest(array_append(failures, now())) as f where f > now() - interval '30 days'),
             disabled = disabled or cardinality(array(select f from unnest(array_append(failures, now())) as f where f > now() - interval '30 days')) >= $1
       where id = 1 returning cardinality(failures) as n, disabled`,
      [FAILURES_TO_DISABLE],
    )
    const { n, disabled } = res.rows[0]
    const fresh = await readControl(this.deps.db)
    if (fresh.elasticIndex && !fresh.esCurrent) await this.deps.managed.dropCandidate(fresh.elasticIndex)
    await this.setState('off', `switch-on failed: ${why}`)
    await this.deps.issues.open(
      'failure',
      'Search: turning Elasticsearch on failed',
      `${why}. Search stays on Postgres. Failures in the last 30 days: ${n}.${disabled ? ' The controller is now disabled until a person clears search_control.disabled.' : ''}`,
    )
    this.log(`search controller: ${row.state} failed: ${why}`)
  }

  private async setState(state: ControllerState, note: string): Promise<void> {
    await this.deps.db.query('update search_control set state = $1, since = now(), controller_note = $2, updated_at = now() where id = 1', [state, `${state}: ${note}`])
    this.log(`search controller: ${state} (${note})`)
  }

  private async note(note: string): Promise<void> {
    await this.deps.db.query('update search_control set controller_note = $1, updated_at = now() where id = 1', [note])
  }

  private async lastSwitchOn(): Promise<Date | null> {
    return (await this.deps.db.query<{ t: Date | null }>('select last_switch_on as t from search_control where id = 1')).rows[0].t
  }

  private async reconcileDue(): Promise<boolean> {
    // Daily, and once after today's index is due: PR 1 replicas still running
    // in the deploy's overlap write event_docs without es_pending rows.
    const r = await this.deps.db.query<{ due: boolean }>(
      `select coalesce(reconciled_at < now() - interval '1 day' or (legacy_drop_after <= now() and reconciled_at < legacy_drop_after), true) as due
       from search_control where id = 1`,
    )
    return r.rows[0].due
  }
}
