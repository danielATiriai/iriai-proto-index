/**
 * Per-caller limits for the open YC query API (D-14: open with rate limits,
 * labelled interim): requests per minute, and new paging sessions per hour,
 * since each holds an Elasticsearch point-in-time snapshot.
 */
export class Limits {
  private requests = new Map<string, { windowStart: number; count: number }>()
  private sessions = new Map<string, number[]>()

  constructor(
    private perMinute: number,
    private sessionsPerHour: number,
  ) {}

  /** Seconds to wait, or 0 when the request may proceed. */
  request(caller: string, now = Date.now()): number {
    const minute = now - (now % 60_000)
    const entry = this.requests.get(caller)
    if (!entry || entry.windowStart !== minute) {
      this.requests.set(caller, { windowStart: minute, count: 1 })
      if (this.requests.size > 50_000) this.prune(minute)
      return 0
    }
    if (entry.count >= this.perMinute) return Math.ceil((minute + 60_000 - now) / 1000)
    entry.count++
    return 0
  }

  /**
   * Seconds to wait before this caller may be given another paging session,
   * or 0. Only answers that return a cursor hold a snapshot open, so only
   * those are counted, by `sessionOpened`.
   */
  sessionWait(caller: string, now = Date.now()): number {
    const recent = (this.sessions.get(caller) ?? []).filter((t) => now - t < 3_600_000)
    this.sessions.set(caller, recent)
    if (recent.length < this.sessionsPerHour) return 0
    return Math.ceil((recent[0] + 3_600_000 - now) / 1000)
  }

  sessionOpened(caller: string, now = Date.now()): void {
    const recent = this.sessions.get(caller) ?? []
    recent.push(now)
    this.sessions.set(caller, recent)
  }

  private prune(minute: number): void {
    for (const [k, v] of this.requests) if (v.windowStart !== minute) this.requests.delete(k)
  }
}
