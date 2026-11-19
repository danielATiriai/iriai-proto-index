import type { EventDoc } from '../index/assemble.js'
import type { EventQuery } from '../query/params.js'

export type BackendName = 'postgres' | 'elastic'

export interface Hit {
  doc: EventDoc
  /** The sort values of this hit; a cursor carries the last examined one's. */
  sort: unknown[]
}

/**
 * What the query service reads from a search backend (spec 3). From PR 2
 * only Postgres (src/search/postgres.ts) implements it; an Elasticsearch
 * backend comes back with the switch-on PR (spec 6.4). Writes are not part of
 * it: Postgres gives each write its version (spec 4.2).
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
  /** Aborts the call. */
  signal?: AbortSignal
}
