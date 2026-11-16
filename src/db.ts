import pg from 'pg'

export type Db = pg.Pool

export function connect(databaseUrl: string, log: (msg: string) => void = console.error): Db {
  // iriai.search_writer marks this code's connections as ones that keep
  // event_docs in step with index_state; see the trigger in MIGRATION.
  const pool = new pg.Pool({ connectionString: databaseUrl, max: 10, options: '-c iriai.search_writer=1' })
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

-- Search on Postgres (proposal "Search without Elasticsearch" v14, build
-- 5e80472; spec 4.1). One row per version of each complete event: the
-- current row has closed_version null; a replaced or removed row keeps the
-- number of the write that closed it, so a paging session sees the data as of
-- its snapshot number, and is deleted 61 minutes after closing.
create sequence if not exists event_doc_version;
create table if not exists event_docs (
  uri text collate "C" not null,
  version bigint not null,
  did text not null,
  event_uri text not null,
  doc jsonb not null,
  lat double precision,
  lon double precision,
  starts_at timestamptz,
  ends_at timestamptz,
  time_valid boolean not null,
  closed_version bigint,
  closed_at timestamptz
);

-- [s, s] for an event without an end, [s, e) when it ends after it starts,
-- otherwise null: the time-window semantics of the Elasticsearch filter.
-- The GiST indexes below are built from this body: changing it needs a
-- reindex of event_docs_current_span and event_docs_closed_span.
create or replace function event_span(s timestamptz, e timestamptz) returns tstzrange
  language sql immutable parallel safe as $fn$
  select case
    when s is null then null
    when e is null then tstzrange(s, s, '[]')
    when e > s then tstzrange(s, e, '[)')
    else null
  end
$fn$;

-- Haversine meters on the mean radius Elasticsearch uses. Strict, so an
-- unresolved point gives null and sorts last.
create or replace function arc_m(lat1 double precision, lon1 double precision, lat2 double precision, lon2 double precision)
  returns double precision language sql immutable strict parallel safe as $fn$
  select 2 * 6371008.7714 * asin(least(1, sqrt(
    power(sin(radians(lat2 - lat1) / 2), 2)
    + cos(radians(lat1)) * cos(radians(lat2)) * power(sin(radians(lon2 - lon1) / 2), 2)
  )))
$fn$;

create unique index if not exists event_docs_current_uri on event_docs (uri) where closed_version is null;
create index if not exists event_docs_current_start on event_docs (starts_at, uri) where closed_version is null;
create index if not exists event_docs_current_point on event_docs (lat, lon) where closed_version is null and lat is not null;
create index if not exists event_docs_current_span on event_docs using gist (event_span(starts_at, ends_at)) where closed_version is null;
create index if not exists event_docs_current_did on event_docs (did) where closed_version is null;
create index if not exists event_docs_current_event on event_docs (event_uri) where closed_version is null;
create index if not exists event_docs_closed_point on event_docs (lat, lon) where closed_version is not null and lat is not null;
create index if not exists event_docs_closed_span on event_docs using gist (event_span(starts_at, ends_at)) where closed_version is not null;
create index if not exists event_docs_closed_at on event_docs (closed_at) where closed_version is not null;

-- Search routing state, one row (spec 6; PR 2 adds the controller's columns).
-- elastic_cursors_until: Elasticsearch cursors are honored until then (the
-- row and its cover are written after this script; see migrate()).
-- generation: moved on by every change to an ingestion table from code that
-- does not write event_docs.
create table if not exists search_control (
  id integer primary key check (id = 1),
  elastic_cursors_until timestamptz not null,
  first_fill_done boolean not null default false,
  pg_verified boolean not null default false,
  compared_at timestamptz,
  note text,
  updated_at timestamptz not null default now(),
  generation bigint not null default 0
);

-- Code that changes an ingestion table without writing event_docs (this
-- service before search on Postgres, say after a revert) leaves event_docs
-- stale. Any such change clears the first fill and verification, so the
-- next upkeep fills event_docs again and compares before Postgres may serve.
-- Every table the ingester writes is watched, not only index_state: older
-- code changes records or accounts first and may fail before index_state.
create or replace function search_foreign_write() returns trigger language plpgsql as $fn$
begin
  if coalesce(current_setting('iriai.search_writer', true), '') <> '1' then
    update search_control
      set first_fill_done = false, pg_verified = false, compared_at = null, generation = generation + 1,
          updated_at = now(), note = 'ingestion tables were changed by code that does not write event_docs; filling again'
      where id = 1;
  end if;
  return null;
end
$fn$;
-- Created only when missing: replacing a trigger locks its table, and an
-- older writer already inside the trigger (waiting on search_control) would
-- then deadlock with this script.
do $do$
declare t text;
begin
  foreach t in array array['index_state', 'records', 'accounts', 'checkpoints', 'skipped_events'] loop
    if not exists (select 1 from pg_trigger where tgname = t || '_foreign_write' and tgrelid = t::regclass) then
      execute format('create trigger %I after insert or update or delete on %I for each statement execute function search_foreign_write()', t || '_foreign_write', t);
    end if;
  end loop;
end
$do$;
`

/**
 * Whether this pool's connections carry iriai.search_writer=1 as the server
 * sees it. A connection pooler that drops start-up options would make this
 * code's ingestion writes look foreign: each would clear verification, which
 * fails closed, and start-up logs it.
 */
export async function searchWriterSeen(db: Db): Promise<boolean> {
  const res = await db.query<{ v: string | null }>(`select current_setting('iriai.search_writer', true) as v`)
  return res.rows[0].v === '1'
}

export async function migrate(db: Db): Promise<void> {
  const client = await db.connect()
  try {
    await client.query('select pg_advisory_lock(7140014)')
    await client.query(MIGRATION)
    // Last, on its own: every start covers the hour of Elasticsearch cursors
    // a previous deployment (which may not have recorded them) could have
    // issued. Nothing above holds search_control's row while tables are locked.
    await client.query(
      `insert into search_control (id, elastic_cursors_until) values (1, now() + interval '61 minutes')
       on conflict (id) do update set elastic_cursors_until = greatest(search_control.elastic_cursors_until, excluded.elastic_cursors_until)`,
    )
  } finally {
    await client.query('select pg_advisory_unlock(7140014)').catch(() => {})
    client.release()
  }
}
