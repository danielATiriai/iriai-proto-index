import type { Db } from '../db.js'

const TIMEOUT_MS = 5_000
const PDS_TTL_MS = 60 * 60 * 1000

export class NotFound extends Error {}

/**
 * Finds an account's PDS from its DID document (did:plc through the PLC
 * directory, did:web from the host), cached for an hour and dropped when an
 * identity event arrives.
 */
export async function resolvePds(db: Db, plcUrl: string, did: string): Promise<string> {
  const cached = await db.query<{ pds: string | null; pds_resolved_at: Date | null }>(
    'select pds, pds_resolved_at from accounts where did = $1',
    [did],
  )
  const row = cached.rows[0]
  if (row?.pds && row.pds_resolved_at && Date.now() - row.pds_resolved_at.getTime() < PDS_TTL_MS) return row.pds

  let docUrl: string
  if (did.startsWith('did:plc:')) docUrl = `${plcUrl}/${did}`
  else if (did.startsWith('did:web:')) docUrl = `https://${decodeURIComponent(did.slice('did:web:'.length))}/.well-known/did.json`
  else throw new Error(`unsupported DID method: ${did}`)
  const doc = (await getJson(docUrl)) as { service?: { id: string; type: string; serviceEndpoint: string }[] }
  const service = doc.service?.find((s) => s.id === '#atproto_pds' || s.id === `${did}#atproto_pds`)
  if (!service || typeof service.serviceEndpoint !== 'string') throw new Error(`${did} has no PDS in its DID document`)
  const pds = service.serviceEndpoint.replace(/\/$/, '')
  await db.query(
    `insert into accounts (did, pds, pds_resolved_at, updated_at) values ($1, $2, now(), now())
     on conflict (did) do update set pds = excluded.pds, pds_resolved_at = now(), updated_at = now()`,
    [did, pds],
  )
  return pds
}

export async function forgetPds(db: Db, did: string, handle?: string): Promise<void> {
  await db.query('update accounts set pds_resolved_at = null, handle = coalesce($2, handle), updated_at = now() where did = $1', [
    did,
    handle ?? null,
  ])
}

export interface FetchedRecord {
  uri: string
  cid: string
  value: Record<string, unknown>
}

/** Reads one record from its PDS; NotFound when the PDS says it does not exist. */
export async function fetchRecord(pds: string, did: string, collection: string, rkey: string): Promise<FetchedRecord> {
  const params = new URLSearchParams({ repo: did, collection, rkey })
  return (await getJson(`${pds}/xrpc/com.atproto.repo.getRecord?${params}`)) as FetchedRecord
}

export async function* listRecords(pds: string, did: string, collection: string): AsyncGenerator<FetchedRecord> {
  let cursor: string | undefined
  do {
    const params = new URLSearchParams({ repo: did, collection, limit: '100' })
    if (cursor) params.set('cursor', cursor)
    const page = (await getJson(`${pds}/xrpc/com.atproto.repo.listRecords?${params}`)) as {
      records: FetchedRecord[]
      cursor?: string
    }
    yield* page.records
    cursor = page.records.length ? page.cursor : undefined
  } while (cursor)
}

async function getJson(url: string): Promise<unknown> {
  const res = await fetch(url, { signal: AbortSignal.timeout(TIMEOUT_MS), headers: { accept: 'application/json' } })
  const body = (await res.json().catch(() => ({}))) as { error?: string; message?: string }
  if (res.status === 404 || (res.status === 400 && body.error === 'RecordNotFound')) {
    throw new NotFound(`${url}: ${body.error ?? res.status}`)
  }
  if (!res.ok) throw new Error(`${url} failed (${res.status}): ${body.error ?? ''} ${body.message ?? ''}`)
  return body
}
