import type { Db } from '../db.js'
import { DEFAULT_THRESHOLDS, type Settings } from '../settings.js'
import { COLLECTIONS, type Projector } from './projector.js'
import { Handler, type JetstreamEvent } from './handler.js'
import { isTransient } from './transient.js'
import { SearchUpkeep } from '../search/verify.js'
import { SearchController } from '../search/controller.js'
import { GitHubIssues } from '../search/issues.js'
import {
  accountsAwaitingRebuild,
  knownDids,
  pendingSidecars,
  readCheckpoint,
  recordSkippedEvent,
  writeCheckpoint,
} from './store.js'

export const CHECKPOINT = 'jetstream'
/** On reconnect, replay a little before the last handled event; handling is idempotent. */
const REPLAY_US = 5_000_000
const FLUSH_MS = 1_000
const RETRY_MS = 60_000
/** Search upkeep: pruning, the first fill and the hourly comparison (spec 5.4). */
const UPKEEP_MS = 10 * 60_000
/** The scale controller (spec 6). */
const CONTROLLER_MS = 10 * 60_000
/** Keep-awake and replay while Elasticsearch is in use (spec 5.2). */
const KEEP_MS = 2 * 60_000
/** An event failing this many times with an error a retry cannot change is skipped and recorded. */
const MAX_TRIES = 3
const MAX_BACKOFF_MS = 30_000
/** A connection that stays up this long without a failure resets the backoff. */
const STABLE_MS = 60_000
/** Health reports the ingester as down once it has been disconnected this long. */
const STALE_MS = 60_000

/** The part of the WebSocket API the ingester uses, so tests can stand in for Jetstream. */
export interface Socket {
  addEventListener(type: 'open' | 'close' | 'error', listener: () => void): void
  addEventListener(type: 'message', listener: (msg: { data: unknown }) => void): void
  close(): void
}

export interface IngesterOptions {
  openSocket?: (url: string) => Socket
  /** First reconnect delay; doubles per failed attempt up to 30 s. */
  backoffMs?: number
}

export interface IngesterStatus {
  connected: boolean
  /** When the ingester last lost (or had not yet made) its connection. */
  disconnectedSince?: number
  lastEventUs?: number
  handled: number
  errors: number
  skipped: number
}

/**
 * Reads the public Jetstream (PlaceProto §4, D-13) filtered to community
 * calendar events and PlaceProto event sidecars, handles each event in order,
 * and stores the upstream cursor only after the events before it are handled.
 * A failed event closes the connection and is replayed on reconnect, with
 * backoff. Failures from Postgres, Elasticsearch or the network are retried
 * for as long as they last; only an event that keeps failing for another
 * reason is skipped, and recorded. Approved accounts are listed in full before
 * the stream starts. Sidecars waiting for their base event, and returning
 * accounts whose rebuild failed, are retried every minute, in order with the
 * stream.
 */
export class Ingester {
  readonly status: IngesterStatus = { connected: false, disconnectedSince: Date.now(), handled: 0, errors: 0, skipped: 0 }
  private handler: Handler
  readonly upkeep: SearchUpkeep
  readonly controller: SearchController
  private stopped = false
  private ws?: Socket
  private queue: Promise<void> = Promise.resolve()
  private handledCursor?: number
  private flushedCursor?: number
  private timers: NodeJS.Timeout[] = []
  private reconnectTimer?: NodeJS.Timeout
  private attempt = 0
  private failures = new Map<number, { tries: number; error: string }>()
  private openSocket: (url: string) => Socket
  private backoffMs: number

  constructor(
    private settings: Settings,
    private db: Db,
    private projector: Projector,
    private log: (msg: string) => void = console.log,
    options: IngesterOptions = {},
  ) {
    this.handler = new Handler(db, projector, log)
    const issues = new GitHubIssues(db, settings.githubAlertToken, settings.githubAlertRepo, log)
    this.upkeep = new SearchUpkeep(db, projector, log, Date.now, issues)
    this.controller = new SearchController({
      db,
      managed: projector.managed,
      postgres: projector.postgres,
      elastic: projector.elastic,
      legacyIndex: settings.elasticIndex,
      issues,
      searchEs: settings.searchEs,
      thresholds: settings.thresholds ?? DEFAULT_THRESHOLDS,
      startedAt: new Date(),
      log,
    })
    this.openSocket = options.openSocket ?? ((url) => new WebSocket(url))
    this.backoffMs = options.backoffMs ?? 1_000
  }

  /** False once the stream has been disconnected for more than a minute. */
  get healthy(): boolean {
    return this.status.connected || Date.now() - (this.status.disconnectedSince ?? 0) < STALE_MS
  }

  async start(): Promise<void> {
    this.handler.remember(await knownDids(this.db))
    this.handler.remember(this.settings.approvedAccounts)
    this.timers.push(setInterval(() => void this.flush(), FLUSH_MS))
    // The first fill and the comparison run before this ingester writes
    // anything (another ingester, during a deploy, may still write: the
    // generation and the write lock cover that); the retry and upkeep timers
    // start only after.
    await this.upkeep.run()
    // Before the stream, so the stream's replay lands on top of the listing.
    await this.backfill()
    this.timers.push(setInterval(() => this.enqueue(() => this.retry()), RETRY_MS))
    this.timers.push(setInterval(() => this.enqueue(() => this.upkeep.run()), UPKEEP_MS))
    // The controller and the keep-awake run beside event handling, not in
    // its queue: a build copies every event and must not hold up ingestion.
    // Writes landing meanwhile are covered by external versions, es_pending
    // and the reconcile (spec 5.1, 6.4).
    const controller = this.controller
    let ticking: Promise<void> = Promise.resolve()
    const tick = () => (ticking = ticking.then(() => controller.tick()))
    void controller.keep().then(tick)
    this.timers.push(setInterval(() => void tick(), CONTROLLER_MS))
    this.timers.push(setInterval(() => void controller.keep(), KEEP_MS))
    this.ticking = () => ticking
    await this.connect()
  }

  private ticking: () => Promise<void> = () => Promise.resolve()

  async stop(): Promise<void> {
    this.stopped = true
    for (const t of this.timers) clearInterval(t)
    clearTimeout(this.reconnectTimer)
    this.ws?.close()
    await this.queue
    await this.ticking()
    await this.flush()
  }

  /** Lists each approved account (the steward first) so seeds are indexed even if written before start. */
  async backfill(): Promise<void> {
    for (const did of this.settings.approvedAccounts) {
      try {
        const n = await this.projector.rebuildAccount(did)
        this.log(`backfilled ${did}: ${n} records`)
      } catch (err) {
        this.log(`backfill ${did} failed: ${(err as Error).message}`)
      }
    }
  }

  /** Retries returning accounts still owed a rebuild, and sidecars waiting for their base event. */
  async retry(): Promise<void> {
    for (const did of await accountsAwaitingRebuild(this.db, 5).catch(() => [])) {
      try {
        const n = await this.projector.rebuildAccount(did)
        this.log(`rebuilt ${did} on retry: ${n} records`)
      } catch (err) {
        this.log(`rebuild ${did} still failing: ${(err as Error).message}`)
      }
    }
    for (const uri of await pendingSidecars(this.db, 60, 100).catch(() => [])) {
      await this.projector.projectSidecar(uri).catch((err) => this.log(`retry ${uri}: ${(err as Error).message}`))
    }
  }

  /** Runs work in order with event handling, so it never races a change to the same records. */
  private enqueue(task: () => Promise<void>): void {
    this.queue = this.queue.then(task).catch((err) => this.log(`queued task failed: ${(err as Error).message}`))
  }

  private async connect(): Promise<void> {
    if (this.stopped) return
    try {
      const from = this.handledCursor ?? (await readCheckpoint(this.db, CHECKPOINT))?.cursor ?? this.settings.jetstreamStartCursor
      const url = new URL(this.settings.jetstreamUrl)
      for (const c of COLLECTIONS) url.searchParams.append('wantedCollections', c)
      if (from) url.searchParams.set('cursor', String(Math.max(0, from - REPLAY_US)))
      this.log(`connecting to ${url.host}${from ? ` from ${new Date(from / 1000).toISOString()}` : ' live'}`)
      this.listen(this.openSocket(url.toString()))
    } catch (err) {
      this.log(`connect failed: ${(err as Error).message}`)
      this.reconnect()
    }
  }

  private listen(ws: Socket): void {
    this.ws = ws
    // Once an event fails, nothing after it on this connection is handled, so
    // the reconnect starts from the last event that was.
    let broken = false
    let openedAt: number | undefined
    ws.addEventListener('open', () => {
      openedAt = Date.now()
      this.status.connected = true
      this.status.disconnectedSince = undefined
    })
    ws.addEventListener('message', (msg) => {
      let evt: JetstreamEvent
      try {
        evt = JSON.parse(String(msg.data))
      } catch {
        return
      }
      // Handle strictly in order; the cursor only moves past handled events.
      this.queue = this.queue.then(async () => {
        if (broken) return
        try {
          await this.handleOrSkip(evt)
          this.attempt = 0
          this.handledCursor = evt.time_us
          this.status.lastEventUs = evt.time_us
          this.status.handled++
        } catch (err) {
          broken = true
          this.status.errors++
          this.log(`event ${evt.kind} ${evt.did} at ${evt.time_us} failed: ${(err as Error).message}; reconnecting from before it`)
          ws.close()
        }
      })
    })
    ws.addEventListener('close', () => {
      if (this.ws !== ws) return
      this.status.connected = false
      this.status.disconnectedSince ??= Date.now()
      if (!broken && openedAt !== undefined && Date.now() - openedAt >= STABLE_MS) this.attempt = 0
      this.reconnect()
    })
    ws.addEventListener('error', () => {})
  }

  private async handleOrSkip(evt: JetstreamEvent): Promise<void> {
    const failed = this.failures.get(evt.time_us)
    if (failed && failed.tries >= MAX_TRIES) {
      await recordSkippedEvent(this.db, evt, failed.error, failed.tries).catch((err) => {
        // Retry the outage; anything else must not stop the skip itself.
        if (isTransient(err)) throw err
        this.log(`could not record skipped event at ${evt.time_us}: ${(err as Error).message}`)
      })
      this.status.skipped++
      this.log(`skipped event ${evt.kind} ${evt.did} at ${evt.time_us} after ${failed.tries} failures: ${failed.error}`)
    } else {
      try {
        await this.handler.handle(evt)
      } catch (err) {
        // An outage is retried for as long as it lasts; only other failures count toward a skip.
        if (!isTransient(err)) this.failures.set(evt.time_us, { tries: (failed?.tries ?? 0) + 1, error: (err as Error).message })
        throw err
      }
    }
    this.failures.delete(evt.time_us)
  }

  private reconnect(): void {
    if (this.stopped || this.reconnectTimer) return
    const delay = Math.min(MAX_BACKOFF_MS, this.backoffMs * 2 ** this.attempt++)
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined
      // Let in-flight events finish so the reconnect cursor is accurate.
      void this.queue.then(() => this.connect())
    }, delay)
  }

  private async flush(): Promise<void> {
    const cursor = this.handledCursor
    if (cursor === undefined || cursor === this.flushedCursor) return
    try {
      await writeCheckpoint(this.db, CHECKPOINT, cursor)
      this.flushedCursor = cursor
    } catch (err) {
      this.log(`checkpoint failed: ${(err as Error).message}`)
    }
  }
}
