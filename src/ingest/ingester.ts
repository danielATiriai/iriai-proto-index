import type { Db } from '../db.js'
import type { Settings } from '../settings.js'
import { COLLECTIONS, type Projector } from './projector.js'
import { Handler, type JetstreamEvent } from './handler.js'
import { knownDids, pendingSidecars, readCheckpoint, writeCheckpoint } from './store.js'

export const CHECKPOINT = 'jetstream'
/** On reconnect, replay a little before the last handled event; handling is idempotent. */
const REPLAY_US = 5_000_000
const FLUSH_MS = 1_000
const MAX_TRIES = 3

export interface IngesterStatus {
  connected: boolean
  lastEventUs?: number
  handled: number
  errors: number
}

/**
 * Reads the public Jetstream (PlaceProto §4, D-13) filtered to community
 * calendar events and PlaceProto event sidecars, handles each event in order,
 * and stores the upstream cursor only after the events before it are handled.
 * Approved accounts are listed in full at start, and sidecars still waiting
 * for their base event are retried every minute.
 */
export class Ingester {
  readonly status: IngesterStatus = { connected: false, handled: 0, errors: 0 }
  private handler: Handler
  private stopped = false
  private ws?: WebSocket
  private queue: Promise<void> = Promise.resolve()
  private handledCursor?: number
  private flushedCursor?: number
  private timers: NodeJS.Timeout[] = []
  private attempt = 0
  /** An event that keeps failing is skipped after this many tries, so one bad record cannot stop ingestion. */
  private failures = new Map<number, number>()

  constructor(
    private settings: Settings,
    private db: Db,
    private projector: Projector,
    private log: (msg: string) => void = console.log,
  ) {
    this.handler = new Handler(db, projector, log)
  }

  async start(): Promise<void> {
    this.handler.remember(await knownDids(this.db))
    this.handler.remember(this.settings.approvedAccounts)
    this.timers.push(setInterval(() => void this.flush(), FLUSH_MS))
    this.timers.push(setInterval(() => void this.retryPending(), 60_000))
    void this.backfill()
    await this.connect()
  }

  async stop(): Promise<void> {
    this.stopped = true
    for (const t of this.timers) clearInterval(t)
    this.ws?.close()
    await this.queue
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

  private async retryPending(): Promise<void> {
    for (const uri of await pendingSidecars(this.db, 60, 100).catch(() => [])) {
      await this.projector.projectSidecar(uri).catch((err) => this.log(`retry ${uri}: ${(err as Error).message}`))
    }
  }

  private async connect(): Promise<void> {
    const stored = await readCheckpoint(this.db, CHECKPOINT)
    const from = this.handledCursor ?? stored?.cursor ?? this.settings.jetstreamStartCursor
    const url = new URL(this.settings.jetstreamUrl)
    for (const c of COLLECTIONS) url.searchParams.append('wantedCollections', c)
    if (from) url.searchParams.set('cursor', String(Math.max(0, from - REPLAY_US)))
    this.log(`connecting to ${url.host}${from ? ` from ${new Date(from / 1000).toISOString()}` : ' live'}`)

    const ws = new WebSocket(url)
    this.ws = ws
    // Once an event fails, nothing after it on this connection is handled, so
    // the reconnect starts from the last event that was.
    let broken = false
    ws.addEventListener('open', () => {
      this.status.connected = true
      this.attempt = 0
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
          if ((this.failures.get(evt.time_us) ?? 0) >= MAX_TRIES) {
            this.log(`skipping event ${evt.kind} ${evt.did} at ${evt.time_us} after ${MAX_TRIES} failures`)
          } else {
            await this.handler.handle(evt)
          }
          this.failures.delete(evt.time_us)
          this.handledCursor = evt.time_us
          this.status.lastEventUs = evt.time_us
          this.status.handled++
        } catch (err) {
          broken = true
          this.status.errors++
          this.failures.set(evt.time_us, (this.failures.get(evt.time_us) ?? 0) + 1)
          this.log(`event ${evt.kind} ${evt.did} at ${evt.time_us} failed: ${(err as Error).message}; reconnecting from before it`)
          ws.close()
          throw err
        }
      }).catch(() => {})
    })
    ws.addEventListener('close', () => {
      this.status.connected = false
      if (this.stopped) return
      const delay = Math.min(30_000, 1_000 * 2 ** this.attempt++)
      setTimeout(() => {
        // Let in-flight events finish so the reconnect cursor is accurate.
        void this.queue.then(() => this.connect()).catch((err) => this.log(`reconnect failed: ${err.message}`))
      }, delay)
    })
    ws.addEventListener('error', () => {})
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
