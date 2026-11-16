import type { EventDoc } from '../index/assemble.js'
import type { EventQuery } from '../query/params.js'

export type BackendName = 'postgres' | 'elastic'

export interface Hit {
  doc: EventDoc
  /** The sort values of this hit; a cursor carries the last examined one's. */
  sort: unknown[]
}

/**
 * What the query service reads from either search backend (spec 3). The
 * Postgres backend (src/search/postgres.ts) and Elasticsearch (src/index/
 * elastic.ts) both implement it, with the same filters, sorts and paging.
 * Writes differ and are not part of it: Postgres gives each write its version
 * (spec 4.2), and in PR 1 today's Elasticsearch index keeps its own.
 */
export interface SearchBackend {
  readonly name: BackendName
  get(uri: string, opts?: CallOptions): Promise<EventDoc | undefined>
  /** Opens the session's snapshot; the returned token goes into the cursor. */
  openSnapshot(opts?: CallOptions): Promise<string>
  page(snapshot: string, q: EventQuery, after: unknown[] | undefined, size: number, opts?: CallOptions): Promise<{ hits: Hit[]; snapshot: string }>
  closeSnapshot(snapshot: string): Promise<void>
  count(opts?: CallOptions): Promise<number>
}

export interface CallOptions {
  /** Aborts the call: the first page of an Elasticsearch session gets 2 seconds (spec 5.2). */
  signal?: AbortSignal
}
