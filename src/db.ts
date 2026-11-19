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
-- Only when missing: ALTER TABLE locks accounts exclusively even when the
-- column exists, on every start, while ingestion may be running.
do $do$
begin
  if not exists (select 1 from information_schema.columns where table_schema = current_schema() and table_name = 'accounts' and column_name = 'needs_rebuild') then
    alter table accounts add column needs_rebuild boolean not null default false;
  end if;
end
$do$;

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

-- Search routing state, one row (spec 6).
-- elastic_cursors_until: written since PR 1 and no longer read: from PR 2
-- every Elasticsearch cursor has expired (the row is written after this
-- script; see migrate()).
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
  -- The generation (search_generation) the first fill finished at. The fill
  -- and verification count only while the generation has not moved since.
  fill_generation bigint not null default 0
);
-- PR 2: the fill-and-compare version that last passed (CHECK_VERSION in
-- src/search/verify.ts); a lower one is filled and compared again once.
-- Only when missing, as for accounts.needs_rebuild.
do $do$
begin
  if not exists (select 1 from information_schema.columns where table_schema = current_schema() and table_name = 'search_control' and column_name = 'checked_by') then
    alter table search_control add column checked_by integer not null default 0;
  end if;
end
$do$;
-- Moved on by every change to an ingestion table from code that does not
-- write event_docs. A sequence, not a row: it takes no lock (so the trigger
-- cannot deadlock writers with one another) and does not roll back.
create sequence if not exists search_generation;
create or replace function search_generation_now() returns bigint language sql volatile as $fn$
  select case when is_called then last_value else 0 end from search_generation
$fn$;

-- Code that changes an ingestion table without writing event_docs (this
-- service before search on Postgres, say after a revert) leaves event_docs
-- stale. Any such change moves the generation on, which voids the first
-- fill and verification, so the next upkeep fills event_docs again and
-- compares before Postgres may serve.
-- Every table event_docs is built from is watched, not only index_state:
-- older code changes records or accounts first and may fail before
-- index_state. checkpoints and skipped_events still carry PR 1's triggers
-- but no longer move it: neither feeds event_docs, and since PR 2 a moved
-- generation means 503s until upkeep fills again, so clearing skipped
-- events by hand must not cause one. (A function change takes no table lock.)
create or replace function search_foreign_write() returns trigger language plpgsql as $fn$
begin
  if TG_TABLE_NAME not in ('checkpoints', 'skipped_events')
     and coalesce(current_setting('iriai.search_writer', true), '') <> '1' then
    perform nextval('search_generation');
  end if;
  return null;
end
$fn$;
create or replace function search_truncate() returns trigger language plpgsql as $fn$
begin
  perform nextval('search_generation');
  return null;
end
$fn$;
-- Created only when missing: replacing a trigger locks its table
-- exclusively, which an older writer mid-transaction would have to wait for. Tables are taken in the order the
-- ingester writes them (records, accounts, index_state), as everywhere in
-- this script.
do $do$
declare t text;
begin
  foreach t in array array['records', 'accounts', 'index_state', 'checkpoints', 'skipped_events'] loop
    if not exists (select 1 from pg_trigger where tgname = t || '_foreign_write' and tgrelid = t::regclass) then
      execute format('create trigger %I after insert or update or delete on %I for each statement execute function search_foreign_write()', t || '_foreign_write', t);
    end if;
  end loop;
  -- PR 2: TRUNCATE moves the generation too, from any connection, this code's
  -- included (nothing here truncates). Only when missing, as above.
  foreach t in array array['records', 'accounts', 'index_state'] loop
    if not exists (select 1 from pg_trigger where tgname = t || '_foreign_truncate' and tgrelid = t::regclass) then
      execute format('create trigger %I after truncate on %I for each statement execute function search_truncate()', t || '_foreign_truncate', t);
    end if;
  end loop;
end
$do$;

-- PR 2 (spec 6.1, 6.2): one row per UTC day of queryEvents calls, the
-- Postgres search step's timings as a histogram (bucket i counts searches
-- under SEARCH_BUCKETS_MS[i], the last bucket 2 s and over), Postgres
-- timeouts, and the current event count. Kept 90 days.
create table if not exists search_stats (
  day date primary key,
  calls bigint not null default 0,
  pg_hist bigint[] not null default '{}',
  pg_timeouts bigint not null default 0,
  events bigint
);

-- Adds two histograms element by element; a shorter one counts as zeros.
create or replace function add_hist(a bigint[], b bigint[]) returns bigint[] language sql immutable parallel safe as $fn$
  select coalesce(array_agg(coalesce(x, 0) + coalesce(y, 0) order by i), '{}') from unnest(a, b) with ordinality as t(x, y, i)
$fn$;

-- When each kind of GitHub issue was last delivered, so one cause opens at
-- most one issue a day.
-- The ingester's last scale check, for the query service's health.
create table if not exists search_scale (
  id integer primary key check (id = 1),
  checked_at timestamptz not null,
  events bigint not null,
  trigger_reason text
);

create table if not exists search_alerts (
  key text primary key,
  opened_at timestamptz not null
);
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

/**
 * Checks a client out of a pool for code that holds it across queries. A
 * checked-out client has no pool listener, so a socket error would end the
 * process: this one is heard, and a client that errored is destroyed on
 * release rather than pooled again.
 */
export async function checkout(pool: pg.Pool): Promise<pg.PoolClient> {
  const client = await pool.connect()
  let failed: Error | undefined
  const onError = (err: Error) => (failed = err)
  client.on('error', onError)
  const release = client.release.bind(client)
  client.release = (err?: Error | boolean) => {
    client.off('error', onError)
    return release(err ?? failed)
  }
  return client
}

export async function migrate(db: Db): Promise<void> {
  const client = await checkout(db)
  // A connection that may still hold the migration lock is destroyed, not
  // pooled, which ends its session and so releases the lock.
  try {
    await client.query('select pg_advisory_lock(7140014)')
    await client.query(MIGRATION)
    // Last, on its own: creates search_control's row. elastic_cursors_until is
    // kept so a revert to PR 1 finds it; nothing here reads it. Nothing above
    // holds search_control's row while tables are locked.
    await client.query(
      `insert into search_control (id, elastic_cursors_until) values (1, now() + interval '61 minutes')
       on conflict (id) do update set elastic_cursors_until = greatest(search_control.elastic_cursors_until, excluded.elastic_cursors_until)`,
    )
  } finally {
    const unlocked = await client.query<{ ok: boolean }>('select pg_advisory_unlock(7140014) as ok').then((r) => r.rows[0].ok, () => false)
    client.release(unlocked ? undefined : new Error('migration lock not confirmed released'))
  }
}
