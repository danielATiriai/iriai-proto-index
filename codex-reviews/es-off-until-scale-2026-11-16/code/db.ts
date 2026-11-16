import pg from 'pg'

export type Db = pg.Pool

export function connect(databaseUrl: string, log: (msg: string) => void = console.error): Db {
  const pool = new pg.Pool({ connectionString: databaseUrl, max: 10 })
  // A Postgres restart terminates idle clients and the pool emits 'error';
  // unheard, that would end the process. The pool replaces the client.
  pool.on('error', (err) => log(`postgres: idle client lost: ${err.message}`))
  return pool
}

// Durable ingestion state (PlaceProto §9): every record seen, with the
// repository revision it was applied at, so an older change never replaces
// newer state; account status; per-sidecar index state, which the
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
-- Set when an account goes inactive; its events stay out of search until a
-- rebuild from its PDS succeeds after it returns (PlaceProto §4).
alter table accounts add column if not exists needs_rebuild boolean not null default false;

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

-- Events skipped after repeated failures a retry could not change, kept so
-- they can be inspected and replayed.
create table if not exists skipped_events (
  time_us bigint primary key,
  did text not null,
  kind text not null,
  -- Text, not jsonb: what made the event fail may be what jsonb refuses.
  event text not null,
  error text,
  attempts integer not null,
  skipped_at timestamptz not null default now()
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
