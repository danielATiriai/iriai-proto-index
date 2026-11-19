import type { Db } from '../db.js'

/** One cause opens at most one issue a day; meanwhile it is only logged. */
const REOPEN_AFTER = '23 hours'

export interface Issues {
  open(key: string, title: string, body: string): Promise<void>
}

/**
 * Opens GitHub issues in iriai-proto-index (spec 6.4; GitHub emails Daniel)
 * with GITHUB_ALERT_TOKEN. Our code only opens issues. Without a token, or
 * when GitHub refuses, the issue is logged and health keeps the last note.
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
      `insert into search_alerts as a (key, opened_at) values ($1, now())
       on conflict (key) do update set opened_at = now() where a.opened_at < now() - interval '${REOPEN_AFTER}'
       returning 1`,
      [key],
    )
    if (!due.rowCount) {
      this.log(`search issue (already opened today): ${title}`)
      return
    }
    if (!this.token) {
      this.log(`search issue (GITHUB_ALERT_TOKEN not set, so not opened): ${title}: ${body}`)
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
        body: JSON.stringify({ title, body: `${body}\n\n_Opened by the search controller in the ingester (spec 6.4)._` }),
        signal: AbortSignal.timeout(10_000),
      })
      if (!res.ok) throw new Error(`GitHub answered ${res.status}`)
      this.log(`search issue opened: ${title}`)
    } catch (err) {
      // Let the next tick try again rather than waiting a day.
      await this.db.query('delete from search_alerts where key = $1', [key]).catch(() => {})
      this.log(`search issue could not be opened (${(err as Error).message}): ${title}`)
    }
  }
}
