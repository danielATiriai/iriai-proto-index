import pg from 'pg'

export type Db = pg.Pool

export function connect(databaseUrl: string): Db {
  return new pg.Pool({ connectionString: databaseUrl, max: 10 })
}

// Durable ingestion state (PlaceProto §9): every record seen, with the
// repository revision it was applied at, so a replayed or older change never
// replaces newer state; account status; per-sidecar index state, which the
// query service checks to suppress results that stopped being complete during
// a paging session; and the upstream checkpoint.
const MIGRATION = `
create table if not exists records (
  uri text primary key,
  did text not null,
  collection text not null,
  rkey text not null,
  cid text,
  rev text collate "C",
  value jsonb,
  deleted boolean not null default false,
  event_uri text,
  updated_at timestamptz not null default now()
);
create index if not exists records_event_uri on records (event_uri) where event_uri is not null;
create index if not exists records_did on records (did);

create table if not exists accounts (
  did text primary key,
  active boolean not null default true,
  status text,
  handle text,
  pds text,
  pds_resolved_at timestamptz,
  updated_at timestamptz not null default now()
);

create table if not exists index_state (
  uri text primary key,
  did text not null,
  complete boolean not null,
  reason text,
  issues jsonb,
  updated_at timestamptz not null default now()
);
create index if not exists index_state_pending on index_state (updated_at) where reason = 'pending-base';

create table if not exists checkpoints (
  name text primary key,
  cursor bigint not null,
  updated_at timestamptz not null default now()
);
`

export async function migrate(db: Db): Promise<void> {
  const client = await db.connect()
  try {
    await client.query('select pg_advisory_lock(7140014)')
    await client.query(MIGRATION)
  } finally {
    await client.query('select pg_advisory_unlock(7140014)').catch(() => {})
    client.release()
  }
}
