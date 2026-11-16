# Event search on Postgres until scale: technical specification

Draft v8, 2026-11-16 (v1 to v7 reviewed; v7 also by Codex Astra the same day; the triages list every change). Repo: danielATiriai/iriai-proto-index, against main d9ab983. Thread "Move event search off Elasticsearch until scale". The packet of the same name is the plain-language version; Daniel approves the packet.

## 1. Goal and scope

query.iriai.app (`app.iriai.proto.queryEvents` and `app.iriai.proto.getEvent`) is served from PostgreSQL 16, which already runs in the Railway project placeproto. Elasticsearch 8.19 stays defined in the project and costs almost nothing while off. A controller in the ingester measures scale and, when a threshold is passed, turns Elasticsearch on, fills it, checks it and moves new searches to it; when use falls well below, it moves searches back and turns Elasticsearch off.

Out of scope: text search (the index has none and adds none), a new Railway project, changes to lexicons or to request and response shapes, the interim heap change offered in the cost thread (that thread owns it), a staging copy of the index.

## 2. What Elasticsearch does today

Read from src/query/search.ts, src/query/service.ts, src/index/elastic.ts and src/main.ts at d9ab983:

- Filters, combined with AND: geo radius (`geo_distance`, arc), geo box (`geo_bounding_box`; antimeridian when west > east), time window (interval overlap, inclusive start, exclusive end; an event without an end is a point; `timeValid` false never matches a time filter), `author` (up to 25 DIDs), `eventUri`.
- Sorts: distance (arc, meters, unresolved points last), `startsAt` (missing last), then sidecar AT-URI ascending by bytes; with no sort, AT-URI only.
- Paging: a point-in-time snapshot opened on the first page, `search_after`, sealed cursors (AES-256-GCM), idle limit 15 minutes, absolute limit 1 hour. Results that stopped being complete since the snapshot are dropped after a Postgres check (`incompleteAmong`).
- `getEvent` reads the document by id, with no completeness check.
- Start-up: both roles wait for Elasticsearch (`waitFor('Elasticsearch', ensureIndex)`, forever) before serving. Elasticsearch connection errors count as transient in the ingester, so it retries the event and holds its checkpoint for as long as they last. `/xrpc/_health` reports `index.ok` from an Elasticsearch count, and the acceptance script requires `index.ok === true`.
- Not used: text analysis, scoring, aggregations, highlighting.

## 3. The search interface and the cursor

New file src/search/backend.ts. The query service and the projector depend on this interface; src/index/elastic.ts and a new src/search/postgres.ts implement it.

```ts
export interface SearchBackend {
  readonly name: 'postgres' | 'elastic'
  put(doc: EventDoc, version: bigint): Promise<void>
  remove(uri: string, version: bigint): Promise<void>
  get(uri: string): Promise<EventDoc | undefined>
  openSnapshot(): Promise<string>
  page(snapshot: string, q: EventQuery, after: unknown[] | undefined, size: number):
    Promise<{ hits: { doc: EventDoc; sort: unknown[] }[]; snapshot: string }>
  closeSnapshot(snapshot: string): Promise<void>
  count(): Promise<number>
}
```

The cursor gains one sealed field, `be` (`'postgres'` or `'elastic'`); cursors without it are read as `'elastic'`. A Postgres cursor is always accepted. An Elasticsearch cursor is sent to Elasticsearch only while `es_current` is set (5.2) or before `elastic_cursors_until`; otherwise, or when Elasticsearch cannot be reached and `es_current` is clear, it answers `CursorExpired`. `elastic_cursors_until` is set to now + 61 minutes whenever new sessions stop going to Elasticsearch, by the controller or by the query service when it starts with reads forced away from Elasticsearch. `CursorExpired` is an error clients already handle by searching again.

`getEvent` follows the same rule as `queryEvents` (5.2): it reads the backend that would serve a new session.

## 4. The Postgres backend

### 4.1 Table

```sql
create sequence event_doc_version;
create table event_docs (
  uri text collate "C" not null,     -- sidecar AT-URI; "C" = byte order, as an Elasticsearch keyword
  version bigint not null,           -- from event_doc_version; the Elasticsearch external version
  did text not null,
  event_uri text not null,
  doc jsonb not null,                -- the EventDoc as served today
  lat double precision, lon double precision,
  starts_at timestamptz, ends_at timestamptz,
  time_valid boolean not null,
  closed_version bigint,             -- null = current (D1 A only)
  closed_at timestamptz              -- D1 A only
);
```

Indexes, each partial on current rows: unique `(uri)`, `(starts_at, uri)`, `(lat, lon)` where lat is not null, GiST `span(starts_at, ends_at)`, `(did)`, `(event_uri)`; under D1 A also the same lat/lon and span indexes on closed rows, and `(closed_at)`.

`span(s, e)`: null without a start; `[s, s]` without an end; `[s, e)` when e > s; null otherwise. `arc_m(...)`: haversine with radius 6,371,008.7714 m, the mean radius Elasticsearch uses.

### 4.2 Writes

Every `put` and every `remove` runs in one transaction that first takes `pg_advisory_xact_lock(7140015)` and only then takes a new number from `event_doc_version`. The order matters: a number taken before the lock could be lower than a snapshot read in between. While Elasticsearch is in use, the transaction also makes the Elasticsearch write (2 s timeout) before it commits, or raises `dirty_gen` when that write fails, so the lock covers both.

<!-- dp D1 -->
`put` closes the current row (`closed_version` = the new number, `closed_at = clock_timestamp()` read after the lock) and inserts the new row with `version` = the new number. `remove` closes the current row the same way and inserts nothing. Closed rows are deleted once `closed_at` is more than 61 minutes old (every 10 minutes, by the ingester): no paging session lasts longer than an hour.
<!-- opt B -->
`put` upserts the current row by `uri` and sets `version` to the new number; `remove` deletes the row. No earlier versions are kept; `closed_version`, `closed_at` and the closed-row indexes are left out.
<!-- /dp -->

### 4.3 Snapshots and paging

<!-- dp D1 -->
`openSnapshot` runs, in its own transaction, `pg_advisory_xact_lock_shared(7140015)` and then reads the sequence's last value W (0 while the sequence has never been used, `is_called` false). Taking the shared lock waits until no write is in flight; a write that finished before has a number at most W, and a write that starts after it needs the exclusive lock and gets a number above W. So a page that reads rows with `version <= W and (closed_version is null or closed_version > W)` sees exactly the data as of W, on every page of the session, without depending on any clock. Writes take milliseconds, so a first page waits at most that long. The read is a union of a current-rows branch and a closed-rows branch, so the partial indexes apply. `closeSnapshot` does nothing.
<!-- opt B -->
There is no snapshot. `openSnapshot` returns a constant and every page reads the current rows. The cursor carries the last sort values, so an event that does not change between pages is never shown twice or skipped. An event edited between pages can move: behind the cursor it is not shown in this session; ahead of it, it can be shown twice. The README and PlaceProto §5 wording "bound to a point-in-time snapshot" becomes "pages follow the live index; an event edited while you page can be missed or repeated".
<!-- /dp -->

Keyset paging: `(sortkey, uri) > ($after_key, $after_uri)`, with nulls last handled as two branches. The `incompleteAmong` check is unchanged.

### 4.4 Filters and sorts in SQL

- Radius: `lat between c - r/111195 and c + r/111195 and arc_m(c, o, lat, lon) <= r`.
- Box: `lat between south and north and (lon between west and east)`, or `(lon >= west or lon <= east)` when west > east, always in parentheses.
- Time window [ws, we): `time_valid and span(starts_at, ends_at) && tstzrange(ws, we, '[)')`, open on a missing side. Matching rows are gathered first (a materialized CTE), then sorted; otherwise the planner walks the start-time index from the oldest event (1.5 s instead of about 70 ms at a million events in the spike).
- `author`: `did = any($1)`; `eventUri`: `event_uri = $1`.
- Sort by distance: `arc_m(...) nulls last, uri`; by start: `starts_at nulls last, uri`; default `uri`.

Distances will not match Elasticsearch to the millimetre: Elasticsearch stores each point rounded to about 1 cm and uses an approximate haversine. Results are the same set; two results whose distances differ by less than about 5 cm can appear in either order, and an event within about 5 cm of a radius edge can fall on either side.

Every search runs with `statement_timeout` 2 s on its own pool of 5 connections, so a slow search cannot take ingestion's connections; it still shares Postgres's CPU and disk. A search that hits the limit answers `ServiceUnavailable`: near the trigger, the default search (a coordinate and no radius, which sorts every event) can fail this way rather than only slow down. Timeouts count toward the trigger (6.1).

### 4.5 Measured speed (spike, 2026-11-16)

Scratch PostgreSQL 16.15 in the thread's container: 4 vCPU, 15 GB of memory, the whole table cached. Synthetic events clustered in 40 metro areas, about 1.1 KB each, every query in the D1 A form (both branches), every returned document read in full (the timed query filters on `length(doc::text)`, so the planner cannot drop it), at a snapshot 20 minutes back with 1% of events edited in the last 30 minutes. Milliseconds, p50 / p95 over 40 runs (20 for the two slowest), re-run Nov 16 after review. Scripts: spike/spike2-*.sql; raw output: spike/spike3-results.txt (spike2-results.txt was timed without reading the documents and is superseded).

| Query | 10,000 | 100,000 | 1,000,000 |
|---|---|---|---|
| 2 km radius, by distance | 0.6 / 0.8 | 1.3 / 1.8 | 13 / 19 |
| 10 km radius and a week, by distance | 0.7 / 1.4 | 2.8 / 3.7 | 49 / 74 |
| A week, by start | 0.8 / 1.0 | 4.2 / 6.0 | 91 / 103 |
| A month, by start | 1.7 / 2.1 | 21 / 29 | 315 / 370 |
| Box and a month, by start | 0.4 / 0.5 | 2.3 / 2.6 | 41 / 65 |
| Everything by distance from a point (the default with a coordinate and no radius) | 7.4 / 9.3 | 35 / 44 | 304 / 359 |
| By start, from a later position (keyset, as a cursor page reads) | 0.4 / 0.6 | 0.7 / 1.0 | 5.4 / 6.5 |

Table and indexes: 14 MB at 10,000 events, 133 MB at 100,000, 1.33 GB at 1,000,000. Railway's Postgres uses 74 MB of memory today; at a million events the table will not all be cached, and Railway's shared CPU is likely slower, so production will be slower than this table. An earlier run (spike/spike-*) without the snapshot form and without fetching documents was faster and is superseded; Run order, per size: spike-schema.sql, then `psql -v n=<events> -f spike2-load.sql` (it includes spike-load.sql), then spike2-bench.sql. The spike tables mark versions with timestamps (`valid_from`, `valid_to`) rather than the sequence numbers of 4.1, and have no lat/lon index on closed rows; the query shapes and index use are the same. Today the index holds 9 events (acceptance run, Oct 13).

## 5. Writes and start-up

### 5.1 Projector

Every `put` and `remove` goes to Postgres first, which gives it its version. It goes to Elasticsearch as well only while Elasticsearch is in use (5.2), with `version_type: external` and that version, so an older write never replaces a newer one; a version conflict (409) means Elasticsearch already holds a newer version and counts as success. Every index Elasticsearch serves from is built from Postgres with these versions (5.4); Elasticsearch's own version numbers are never compared.

While Postgres is not verified (5.2), a failed Elasticsearch write fails the event as today, so it is replayed; Elasticsearch lags by the replay time, as today, and stays current. Once Postgres is verified, a failed Elasticsearch write never fails the event: the projector raises `search_control.dirty_gen` by one and moves on, Elasticsearch stops being current, and the repair (6.4) restores it. From then on an Elasticsearch outage no longer stalls ingestion.

### 5.2 Elasticsearch is optional

The Elasticsearch client connects lazily; neither role waits for it at start-up. One rule governs every read, whoever chose the backend: **a backend serves only while it may serve, and Postgres, once verified, always may.**

- Postgres may serve once it is verified: its first fill (5.4) is recorded and the ingester's own comparison (the verifying check of 6.4) has passed once. Verified is a latch: nothing clears it. Every write reaches Postgres first (5.1), so it cannot fall behind. The ingester runs the comparison after the first fill and hourly until it passes, and opens an issue on each failure; no sentence or command is needed.
- Elasticsearch may serve while `es_current` is set and no write to it has failed since the last repair (`dirty_gen` = `repaired_gen`). It is in use while `SEARCH_ES=on` on the ingester (default `on`) or, after PR 2, the controller is in `starting` to `draining`. While in use, the ingester writes to it (5.1), sends a cluster-health request every 2 minutes (recording `es_seen_at`), and every 10 minutes repairs it if dirty (reconcile, 6.4). When use begins and `es_current` is clear, the ingester builds a fresh index and then sets `es_current`; when use ends, it clears `es_current` and sends nothing more. Use cannot end before Postgres is verified: until then the ingester ignores `SEARCH_ES=off`, opens an issue and says so in health.
- The may-serve check for Elasticsearch is made at each first page, under the shared lock of 4.3 as the snapshot opens, not from the 30-second copy of the control row; with the write lock of 4.2 this means no new session opens on an Elasticsearch that has missed a committed write.
- A new session goes to the chosen backend (the state, or `SEARCH_BACKEND_FORCE`) if it may serve, else to Postgres if verified, else answers `ServiceUnavailable`. A first page Elasticsearch fails within 2 seconds is answered from Postgres if verified. Health reports which backend served and why.

So after verification, Postgres always serves when Elasticsearch cannot; before it, Elasticsearch is the only backend, as today. Later comparisons (verifying, 6.4) only decide whether the controller switches to Elasticsearch; they never change what may serve. Cost of the rule: a switch to Elasticsearch waits for a full fill; a dirty Elasticsearch hands new sessions to Postgres until a repair succeeds (every 10 minutes while it answers, so through any outage); while Elasticsearch is in use, each write waits for it, up to 2 s; until the first comparison passes, Elasticsearch stays on at today's cost, its failures answer `ServiceUnavailable` and its outages stall ingestion, as today.

### 5.3 Health

`/xrpc/_health` reports `index: { ok, backend, events }` from the backend serving new sessions, plus `search: { state, esInUse, esCurrent, pgVerified, p95Ms, events }`; the weekly check of 6.3 reads `esInUse` here. The acceptance script keeps checking `index.ok` and also prints the backend.

### 5.4 First fill and rebuild

On first start after deploy, the ingester projects every sidecar in `index_state` into `event_docs` (9 today) and records that the first fill is done. Today's index is not trusted, because its version numbers are Elasticsearch's own: the ingester then builds a fresh index from Postgres with external versions (backfill and reconcile, 6.4), sets `es_current` and deletes the old index. Until both are done, `queryEvents` and `getEvent` answer `ServiceUnavailable` with Retry-After, a few seconds at today's size. Whenever a fresh index becomes current, the previous one is deleted, so the volume holds at most two.

## 6. The scale controller

Runs in the ingester every 10 minutes, under a session advisory lock (7140016) taken and released on one dedicated client, so only one runs. State is one row in `search_control` (state, since, elastic_index, elastic_cursors_until, last_switch_on, failures, disabled, dirty_gen, repaired_gen, first_fill_done, note). The query service reads it every 30 seconds for routing; the may-serve check of 5.2 is read fresh at each first page.

### 6.1 What is measured

The query service writes one row per UTC day to `search_stats`: queryEvents calls, p50 and p95 of the search step per backend (a search stopped by the 2 s limit counts as 2,000 ms), timeouts, the current event count, and up to 200 query shapes (normalized parameters with coordinates rounded to 3 decimals, about 100 m; no caller address). Rows are kept 90 days, so the 14- and 30-day conditions of 6.2 survive restarts; the query shapes in them are cleared after 7 days.

<!-- dp D2 -->
While Elasticsearch serves, 1 in 10 first pages also runs against Postgres after the answer is sent (at most one a second), so Postgres's own p95 stays known.
<!-- opt B -->
No shadow queries: only the event count decides, so Postgres timings are recorded only while Postgres serves.
<!-- /dp -->

### 6.2 Thresholds

<!-- dp D2 -->
On when either holds: on 3 consecutive UTC days, with at least 500 queryEvents calls each, the Postgres p95 is at least 250 ms or more than 1% of searches hit the 2 s limit; or on any day the current events number 500,000 or more. Back when Elasticsearch has served at least 30 days and, on 14 consecutive days, the Postgres shadow p95 is at most 100 ms and the events number 250,000 or fewer. All six numbers are settings.
<!-- opt B -->
On when the current events number 500,000 or more on any day. Timeouts are recorded but do not count. Back when Elasticsearch has served at least 30 days and the events have numbered 250,000 or fewer on 14 consecutive days. Both numbers are settings.
<!-- /dp -->

### 6.3 Turning Elasticsearch on and off

<!-- dp D3 -->
Railway Serverless is turned on for the Elasticsearch service once (Daniel's sentence). From then on, turning Elasticsearch on, and the $13 to $20 a month it costs while on, happens without his word, as he asked. Railway puts a Serverless service to sleep 5 to 10 minutes after it last sent any outbound packet, and bills nothing for compute while it sleeps; a request to it wakes it [docs: Railway Serverless; that a request over private networking wakes it, and that Elasticsearch sends nothing on its own when idle, are checked in gate 2, with `ingest.geoip.downloader.enabled=false` set]. "On" is Elasticsearch in use (5.2): its answers to the health request every 2 minutes keep it awake. "Off" is nothing in use, so nothing is sent. No Railway credential is held by our code, so our code cannot see whether Elasticsearch is asleep: the check is a weekly scheduled routine that this thread creates at gate 2: it reads Elasticsearch's memory on Railway (read-only, Railway connector) and opens an issue when it is non-zero while Elasticsearch is not in use. Gate 2 also checks that a sleeping service with a volume stops cleanly and keeps its data. While it sleeps, its image and its old index stay as they were; the backfill always builds a fresh index.
<!-- opt B -->
The ingester holds a Railway project token for placeproto / production. It deploys the Elasticsearch service with `serviceInstanceDeployV2` and removes its active deployment with `deploymentRemove`. Our code calls only the service named in `SEARCH_ES_SERVICE_ID`, but the token itself can read and change every service, variable and volume in that environment, including Postgres and its password. This replaces Daniel's sentence (Eng directions §3) for these two actions with a standing authorization.
<!-- opt C -->
The controller opens a GitHub issue and waits. Daniel types "Turn on Elasticsearch for query.iriai.app" (or "Turn off …") in a thread, which deploys the service or removes its deployment. The controller sees the result and goes on by itself. No Railway credential is held by our code.
<!-- /dp -->

### 6.4 States

off → ready → starting → backfilling → verifying → on → draining → off.

- ready → starting: the controller opens a GitHub issue in danielATiriai/iriai-proto-index (GitHub emails Daniel) and turns Elasticsearch on (6.3).
- starting: waits for cluster health yellow or green, up to 30 minutes.
- backfilling: creates a fresh index `placeproto-events-<yyyymmdd>` with today's mapping and `index.gc_deletes` 2 hours; copies every current row, 500 per bulk request, with external versions; then reconciles.
- Reconcile: notes `dirty_gen` at its start, reads every document id and version from Elasticsearch and compares with the current rows: re-puts missing or older documents; for a document with no current row, takes a fresh number and re-checks Postgres under the write lock, then, after commit and only if there was still no current row, deletes in Elasticsearch with that number. At its end it sets `repaired_gen` to the generation it noted, so a write that failed during the pass leaves `dirty_gen` above `repaired_gen` and is repaired next tick. Runs at the end of every fill, every 10 minutes while dirty, and once a day while in use (5.2).
- verifying: after a refresh, the id-and-version sets must be equal; then for each stored query shape (or 50 built-in shapes when fewer are stored) positions 1 to 45 of a 51-result first page from both backends must hold the same events, in the same order apart from results whose distances differ by under 5 cm, ignoring events within 5 cm of a radius edge. A difference involving an event that changed during the check is retried, up to 3 times. Any other difference is a failure.
- on: new sessions use Elasticsearch, within the rule of 5.2. A later page of an Elasticsearch session that fails answers `ServiceUnavailable`, as today. If more than 20 first pages fall back within 10 minutes while `es_seen_at` is under 5 minutes old, or the cluster is red or no keep-awake has been answered for 30 minutes since the ingester started (compared in SQL), the controller moves to draining and opens an issue.
- draining: new sessions use Postgres; `elastic_cursors_until` = now + 61 minutes; after that, use ends (5.2) and Elasticsearch is turned off (6.3).
- Failure: any failure in starting, backfilling or verifying turns Elasticsearch off again (6.3), returns to `off`, counts a failure and opens an issue.

<!-- dp D3 -->
Turning off under Serverless is ending use; the weekly check (6.3) confirms the sleep.
<!-- opt B -->
Turning off is removing the deployment; the controller confirms through Railway's API that none is active, retries every tick until then, and raises an issue each hour it fails.
<!-- opt C -->
Turning off is an issue asking Daniel for "Turn off Elasticsearch for query.iriai.app"; the controller re-asks each day until the cluster stops answering.
<!-- /dp -->

After 2 failures in 30 days the controller sets `disabled` and does nothing until a person clears it. At most one switch-on in 30 days.
- `SEARCH_BACKEND_FORCE=postgres|elastic` on the query service overrides the state for new sessions, within the rule of 5.2.

## 7. Switching today's service over

1. PR 1: interface, Postgres backend, `event_docs`, Elasticsearch optional at start-up, the rule of 5.2 with `SEARCH_ES=on`, cursor `be` and `elastic_cursors_until`, `getEvent` under the same rule, health, `SEARCH_BACKEND_FORCE` (default `elastic`). Deploy; the first fill and the fresh index (5.4) run.
2. The ingester's comparison (5.2) passes and health shows `pgVerified: true`; the thread checks this before steps 3 and 4. With 9 events in one neighborhood this exercises little; the double-run CI tests (10) are the real guard.
3. Daniel's sentence: set `SEARCH_BACKEND_FORCE=postgres` on the query service.
4. After 61 minutes, Daniel's sentences: set `SEARCH_ES=off` on the ingester; then, under D3 A, turn on Serverless for Elasticsearch (and watch it sleep within 10 minutes on Railway's metrics), or under D3 B or C, remove the Elasticsearch deployment. Set Elasticsearch's limits to 2 GB of memory and 1 vCPU.
5. PR 2: stats, controller, GitHub issues. Daniel's sentence: clear `SEARCH_BACKEND_FORCE` and set the controller's variables on the ingester.

Undo, before or after PR 2: under D3 B or C redeploy Elasticsearch; set `SEARCH_ES=on` on the ingester and `SEARCH_BACKEND_FORCE=elastic` on the query service together. Reads move once the ingester has rebuilt the index (5.2), minutes at today's size; until then Postgres serves.

## 8. Costs (Railway Pro)

Prices: $10 per GB of memory a month and $20 per vCPU a month [docs]; volumes about $0.15 per GB a month [unverified].

- Today: Elasticsearch about $13 a month (1.28 GB held around the clock, near-zero CPU, 0.25 GB volume).
- Below the trigger, today and at ten times today's events: Elasticsearch $0.04 a month (volume only); Postgres adds $0.10 to $0.50 a month.
- At the trigger (about 500,000 events): Postgres about 0.7 GB of table and up to 1 GB of memory, $5 to $10 a month; Elasticsearch, while on, $13 to $20 a month.
- Stop point: Elasticsearch's service limits of 2 GB memory and 1 vCPU (Daniel's sentence) cap its compute at about $40 a month; realistic $13 to $20. Its disk is billed on top: at most two indexes (5.4), about 1 to 2 GB at 500,000 events, under $0.50 a month [price unverified]. At the memory limit Railway restarts it; first pages fall back to Postgres, later pages of its sessions get `ServiceUnavailable`, and repeated fallbacks drain it (6.4).
- Postgres has no stop point. Exposure estimate: about $10 a month at 500,000 events (1 GB memory at $10), which nothing enforces: the workspace alert at $55 only warns. For Daniel to accept by name.
- GitHub: the issues API on the free plan bills nothing.

## 9. Secrets and settings

- `GITHUB_ALERT_TOKEN` (ingester): fine-grained token of Daniel's, iriai-proto-index only, Issues read and write, stored with the secrets helper. That permission can also edit, close and lock any issue in that repository, not only open one; our code only opens and comments.
- `SEARCH_ES`, `SEARCH_BACKEND_FORCE`, threshold settings.

<!-- dp D3 -->
- No Railway credential.
<!-- opt B -->
- `RAILWAY_PROJECT_TOKEN`, `SEARCH_ES_SERVICE_ID`, `SEARCH_ENV_ID` (ingester), the token created by Daniel and stored with the secrets helper.
<!-- opt C -->
- No Railway credential.
<!-- /dp -->

## 10. Tests

CI runs every filter, sort, paging and suppression test against both backends with the same fixtures, including antimeridian boxes, unresolved points, ties and radius edges. New tests: snapshot exactness with concurrent writes (D1 A); versions on put, remove and upsert; backfill racing writes and deletes; the reconcile; start-up with Elasticsearch unreachable (both roles serve, ingestion continues); cursor `be` and `elastic_cursors_until`; the controller's states with a fake clock and a fake Railway; threshold edges; first-page fallback.
