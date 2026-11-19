import type { Db } from '../db.js'

/** One cause opens at most one issue in 23 hours; meanwhile it is only logged. */
const REOPEN_AFTER = '23 hours'
/** A claim not yet confirmed by GitHub lapses after this, so a process that stopped mid-request does not hold the cause for a day. */
const LEASE = '5 minutes'
/** A token is printable ASCII with no spaces; anything else is refused without being sent or shown. */
const TOKEN = /^[\x21-\x7e]+$/

export interface Issues {
  open(key: string, title: string, body: string): Promise<void>
}

/**
 * Opens GitHub issues in iriai-proto-index (spec 6.4; GitHub emails Daniel)
 * with GITHUB_ALERT_TOKEN. Our code only opens issues. A cause's day starts
 * only once GitHub has accepted its issue: without a token, or when GitHub
 * refuses, the claim is withdrawn and the issue is only logged, so the next
 * call (each cause is checked again every 10 minutes or hourly) tries again.
 */
export class GitHubIssues implements Issues {
  constructor(
    private db: Db,
    private token: string | undefined,
    private repo: string,
    private log: (msg: string) => void = console.log,
    private fetchFn: typeof fetch = fetch,
  ) {}

  async open(key: string, title: string, body: string): Promise<void> {
    const due = await this.db.query(
      // A lease first: opened_at as if REOPEN_AFTER minus LEASE ago. GitHub's
      // acceptance moves it to now.
      `insert into search_alerts as a (key, opened_at) values ($1, now() - interval '${REOPEN_AFTER}' + interval '${LEASE}')
       on conflict (key) do update set opened_at = excluded.opened_at where a.opened_at < now() - interval '${REOPEN_AFTER}'
       returning 1`,
      [key],
    )
    if (!due.rowCount) {
      this.log(`search issue (already opened today): ${title}`)
      return
    }
    if (!this.token || !TOKEN.test(this.token)) {
      await this.db.query('delete from search_alerts where key = $1', [key]).catch(() => {})
      this.log(`search issue (GITHUB_ALERT_TOKEN ${this.token ? 'malformed' : 'not set'}, so not opened): ${title}: ${body}`)
      return
    }
    try {
      const res = await this.fetchFn(`https://api.github.com/repos/${this.repo}/issues`, {
        method: 'POST',
        headers: {
          authorization: `Bearer ${this.token}`,
          accept: 'application/vnd.github+json',
          'x-github-api-version': '2022-11-28',
          'content-type': 'application/json',
          'user-agent': 'iriai-proto-index',
        },
        body: JSON.stringify({ title, body: `${body}\n\n_Opened by the ingester's search checks (spec 6)._` }),
        signal: AbortSignal.timeout(10_000),
      })
      if (!res.ok) throw new GitHubRefused(res.status)
    } catch (err) {
      // Let the next tick try again rather than waiting a day. Only a status
      // or an error kind is logged: a request error may quote a header.
      await this.db.query('delete from search_alerts where key = $1', [key]).catch(() => {})
      const why = err instanceof GitHubRefused ? `GitHub answered ${err.status}` : `request failed: ${(err as Error).name}`
      this.log(`search issue could not be opened (${why}): ${title}`)
      return
    }
    await this.db.query('update search_alerts set opened_at = now() where key = $1', [key]).catch(() => {})
    this.log(`search issue opened: ${title}`)
  }
}

class GitHubRefused extends Error {
  constructor(readonly status: number) {
    super(`GitHub answered ${status}`)
  }
}
