import type { Db } from '../db.js'
import { eventSidecarNsid } from '../contracts/contracts.js'

export interface RecordRow {
  uri: string
  did: string
  collection: string
  rkey: string
  cid: string | null
  rev: string | null
  value: Record<string, unknown> | null
  deleted: boolean
  event_uri: string | null
}

export interface RecordChange {
  did: string
  collection: string
  rkey: string
  /** Absent for a deletion. */
  cid?: string
  value?: Record<string, unknown>
  /**
   * Repository revision from the stream. Hydrated and listed records carry
   * none: they never replace a change applied from the stream, and any later
   * stream change replaces them.
   */
  rev?: string
}

export function recordUri(did: string, collection: string, rkey: string): string {
  return `at://${did}/${collection}/${rkey}`
}

function eventUriOf(collection: string, value: Record<string, unknown> | undefined): string | null {
  if (collection !== eventSidecarNsid || !value) return null
  return typeof value.eventUri === 'string' ? value.eventUri : null
}

/**
 * Applies one create, update or delete. Returns false when the stored state
 * is already at a newer repository revision (a replay or a late, older
 * change), so nothing changed. Revisions are TIDs and compare as bytes.
 */
export async function applyChange(db: Db, change: RecordChange): Promise<boolean> {
  const uri = recordUri(change.did, change.collection, change.rkey)
  const deleted = change.value === undefined
  const res = await db.query(
    `insert into records (uri, did, collection, rkey, cid, rev, value, deleted, event_uri, updated_at)
     values ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
     on conflict (uri) do update set
       cid = excluded.cid, rev = excluded.rev, value = excluded.value, deleted = excluded.deleted,
       event_uri = excluded.event_uri, updated_at = now()
     where records.rev is null or (excluded.rev is not null and records.rev < excluded.rev)`,
    [
      uri,
      change.did,
      change.collection,
      change.rkey,
      deleted ? null : (change.cid ?? null),
      change.rev ?? null,
      deleted ? null : JSON.stringify(change.value),
      deleted,
      eventUriOf(change.collection, change.value),
    ],
  )
  return (res.rowCount ?? 0) > 0
}

/**
 * Replaces an account's records with a full listing from its PDS (start-up
 * backfill of approved accounts, and rebuild after an account returns). Any
 * stored record missing from the listing is marked deleted. Revisions are
 * cleared so the next stream change for each record applies.
 */
export async function replaceAccountRecords(
  db: Db,
  did: string,
  collections: string[],
  listed: { collection: string; rkey: string; cid: string; value: Record<string, unknown> }[],
): Promise<string[]> {
  const client = await db.connect()
  try {
    await client.query('begin')
    const seen = new Set<string>()
    for (const r of listed) {
      const uri = recordUri(did, r.collection, r.rkey)
      seen.add(uri)
      await client.query(
        `insert into records (uri, did, collection, rkey, cid, rev, value, deleted, event_uri, updated_at)
         values ($1, $2, $3, $4, $5, null, $6, false, $7, now())
         on conflict (uri) do update set cid = excluded.cid, rev = null, value = excluded.value, deleted = false,
           event_uri = excluded.event_uri, updated_at = now()`,
        [uri, did, r.collection, r.rkey, r.cid, JSON.stringify(r.value), eventUriOf(r.collection, r.value)],
      )
    }
    const stored = await client.query<{ uri: string }>(
      'select uri from records where did = $1 and collection = any($2) and not deleted',
      [did, collections],
    )
    for (const { uri } of stored.rows) {
      if (seen.has(uri)) continue
      await client.query(
        'update records set deleted = true, value = null, cid = null, rev = null, updated_at = now() where uri = $1',
        [uri],
      )
    }
    await client.query('commit')
    return [...seen]
  } catch (err) {
    await client.query('rollback').catch(() => {})
    throw err
  } finally {
    client.release()
  }
}

export async function getRecord(db: Db, uri: string): Promise<RecordRow | undefined> {
  const res = await db.query<RecordRow>('select * from records where uri = $1', [uri])
  return res.rows[0]
}

/** Sidecars referencing an event (reverse references, PlaceProto §4). */
export async function sidecarsFor(db: Db, eventUri: string): Promise<string[]> {
  const res = await db.query<{ uri: string }>(
    'select uri from records where event_uri = $1 and collection = $2',
    [eventUri, eventSidecarNsid],
  )
  return res.rows.map((r) => r.uri)
}

export async function sidecarsOfAccount(db: Db, did: string): Promise<string[]> {
  const res = await db.query<{ uri: string }>('select uri from records where did = $1 and collection = $2', [
    did,
    eventSidecarNsid,
  ])
  return res.rows.map((r) => r.uri)
}

export async function knownDids(db: Db): Promise<string[]> {
  const res = await db.query<{ did: string }>('select distinct did from records')
  return res.rows.map((r) => r.did)
}

export async function isAccountActive(db: Db, did: string): Promise<boolean> {
  const res = await db.query<{ active: boolean }>('select active from accounts where did = $1', [did])
  return res.rows[0]?.active ?? true
}

export async function setAccountStatus(db: Db, did: string, active: boolean, status: string | null): Promise<boolean> {
  const before = await isAccountActive(db, did)
  await db.query(
    `insert into accounts (did, active, status, updated_at) values ($1, $2, $3, now())
     on conflict (did) do update set active = excluded.active, status = excluded.status, updated_at = now()`,
    [did, active, status],
  )
  return before !== active
}

export async function setIndexState(
  db: Db,
  uri: string,
  did: string,
  complete: boolean,
  reason: string | null,
  issues: string[] | null = null,
): Promise<void> {
  await db.query(
    `insert into index_state (uri, did, complete, reason, issues, updated_at) values ($1, $2, $3, $4, $5, now())
     on conflict (uri) do update set complete = excluded.complete, reason = excluded.reason, issues = excluded.issues,
       updated_at = now()`,
    [uri, did, complete, reason, issues ? JSON.stringify(issues) : null],
  )
}

/** URIs among `uris` that are no longer complete (deleted, orphaned or suppressed). */
export async function incompleteAmong(db: Db, uris: string[]): Promise<Set<string>> {
  if (uris.length === 0) return new Set()
  const res = await db.query<{ uri: string }>('select uri from index_state where uri = any($1) and not complete', [uris])
  return new Set(res.rows.map((r) => r.uri))
}

export async function pendingSidecars(db: Db, olderThanSeconds: number, limit: number): Promise<string[]> {
  const res = await db.query<{ uri: string }>(
    `select uri from index_state where reason = 'pending-base' and updated_at < now() - make_interval(secs => $1)
     order by updated_at limit $2`,
    [olderThanSeconds, limit],
  )
  return res.rows.map((r) => r.uri)
}

export async function readCheckpoint(db: Db, name: string): Promise<{ cursor: number; updatedAt: Date } | undefined> {
  const res = await db.query<{ cursor: string; updated_at: Date }>(
    'select cursor, updated_at from checkpoints where name = $1',
    [name],
  )
  const row = res.rows[0]
  return row ? { cursor: Number(row.cursor), updatedAt: row.updated_at } : undefined
}

export async function writeCheckpoint(db: Db, name: string, cursor: number): Promise<void> {
  await db.query(
    `insert into checkpoints (name, cursor, updated_at) values ($1, $2, now())
     on conflict (name) do update set cursor = excluded.cursor, updated_at = now()`,
    [name, cursor],
  )
}
