# iriai-proto-index

Iriai's PlaceProto ingestion, index and query API for YC (slices 1.4 and 1.5; BUILD-003, 004, 005). It reads community calendar events and `app.iriai.proto.event` sidecars from Bluesky's public Jetstream, keeps every complete event-plus-sidecar pair in PostgreSQL, searches them there, and serves them through `app.iriai.proto.getEvent` and `app.iriai.proto.queryEvents`. The contracts are [iriai-proto v0.1.0](https://github.com/danielATiriai/iriai-proto/releases/tag/v0.1.0); the behaviour follows the PlaceProto specification §§4, 5 and 9.

Access is **interim** (D-14): open, with no key, and per-caller rate limits.

## Query API

Base URL: `https://query.iriai.app` (Railway service `query` in the `placeproto` project).

```
GET /xrpc/app.iriai.proto.queryEvents?latitude=37.785054&longitude=-122.429827
GET /xrpc/app.iriai.proto.queryEvents?sort=startsAt&windowStart=2026-10-15T00:00:00-07:00
GET /xrpc/app.iriai.proto.getEvent?uri=at://did:plc:kd5s7zyp7nje43ievnqhgzrr/app.iriai.proto.event/3mxrq3w4c2l2y
GET /xrpc/_health
```

`queryEvents` takes `latitude` and `longitude` (decimal-degree strings), `radiusMeters`, a box (`west`, `south`, `east`, `north`; west > east crosses the antimeridian), `windowStart` and `windowEnd`, `author` (up to 25 DIDs), `eventUri`, `sort` (`distance`, the default with a coordinate, or `startsAt`), `limit` (1 to 100, default 50) and `cursor`. It returns `{ events, cursor? }`. Each event is an `app.iriai.proto.defs#eventView` with its `version` (both record CIDs).

- **Distance** is great-circle (arc) distance in meters. Resolved locations come first, nearest first. Ties go by sidecar AT-URI. Unresolved locations (addresses and bare Foursquare ids, until provider lookup in slice 4.3) come last. Any geographic bound excludes them.
- **Time** filtering is interval overlap with inclusive starts and exclusive ends. An event with no end is a point in time. An event with no start never matches a time filter.
- **Cursors** are opaque and encrypted. Each one is bound to the query's parameters and to a point-in-time snapshot. A cursor expires after 15 idle minutes or one hour (`CursorExpired`), and one used with different parameters gets `InvalidCursor`. Results that were deleted or orphaned after the snapshot are dropped, so a page can be short while still returning a cursor.
- **Errors:** `InvalidRequest`, `InvalidCursor` and `CursorExpired` (400). `SidecarNotFound` and `EventNotFound` (404). `RateLimitExceeded` (429, with `Retry-After`). `ServiceUnavailable` (503, never an empty page when the index is down).

The method lexicons (`getEvent`, `queryEvents`) are published in contracts v0.2.0 (BUILD-013). This service answers the shapes in the specification.

## Change stream

Iriai runs no stream of its own in YC (D-13). To follow PlaceProto events as they are written, read Bluesky's public Jetstream filtered to the two PlaceProto collections: the [stream recipe](docs/stream-recipe.md) explains how, with a runnable script (`node scripts/stream-recipe.mjs`) that resumes from a saved cursor inside Jetstream's 36-hour lookback. The planned Iriai-hosted feed is described, with no numbers committed, in [Iriai change feed (coming soon)](docs/change-feed.md). The **Stream check** workflow (spike 3) proves stop, restart and replay on the public stream.

## How it works

- **Ingester** (`ROLE=ingester`) subscribes to Jetstream for `community.lexicon.calendar.event` and `app.iriai.proto.event` only. Posts are fetched on demand later, never streamed (D-12). Events are handled in order, and the cursor is checkpointed only after handling. A record is never replaced by an older repository revision, and a replay of the same revision is applied and projected again, so whatever failed the first time is redone. Each change is re-assembled into complete pairs:
  - A sidecar whose base event hasn't been seen fetches that event from the author's PDS. A timeout keeps it pending, never deleted.
  - Invalid records are kept out of the index and never repaired.
  - An inactive account's events leave search at once. A returning account's events come back only after a rebuild from its PDS succeeds; while its PDS can't be reached they stay out of search, and the rebuild is retried every minute.
  - Approved accounts (`APPROVED_ACCOUNTS`, by default the steward events.iriai.app) are listed in full at start, before the stream, so seeds written before ingestion began are indexed.
  - A failed event closes the connection and is replayed on reconnect, with backoff up to 30 s. Postgres and network outages are retried for as long as they last. An event that fails three times for any other reason is skipped and recorded in `skipped_events`.
  - The ingester's `/health` answers 503 once Jetstream has been disconnected for a minute.
  - Locations resolve locally: geo coordinates, and H3 cell centres.
- **Search on Postgres** (proposal "Search without Elasticsearch" v14, build 5e80472, with the packet amendment to D2 and D3 that Daniel chose on Nov 19: turn Elasticsearch off only; spec, approval and amendment in [docs/search-without-elasticsearch](docs/search-without-elasticsearch/)). Every index change goes to Postgres's `event_docs` only; nothing writes to, reads from or connects to Elasticsearch. On its first start, and again whenever code that does not keep `event_docs` changes an ingestion table, the ingester fills `event_docs` from every stored sidecar, then compares it with the sidecars assembled again from the stored records, hourly until they match once; health shows the result (`search.pgVerified`). The comparison is assembled from the same stored rows as the projection, so it catches missed or stale rows, not an assembly bug.
  - **Writing ingestion tables by hand takes search down.** Any change to `records`, `accounts` or `index_state` from a connection without `iriai.search_writer=1` (psql, say) clears verification, and search answers 503 until the next upkeep fills and compares again, about 10 minutes. `checkpoints` and `skipped_events` can be changed safely.
  - **Scale check.** The query service records each day's calls and Postgres search timings, and every 10 minutes the ingester records the event count (`search_stats`, kept 90 days). It opens one GitHub issue (at most one a day per cause) when the scale trigger is met: `SEARCH_ON_EVENTS` events, or 3 days that each have at least `SEARCH_ON_MIN_CALLS` calls and a p95 of at least `SEARCH_ON_P95_MS` or a timeout rate over `SEARCH_ON_TIMEOUT_RATE`. It also opens one after a failed comparison and after 3 checks in a row find Postgres unverified. Switching a search engine back on is a later, separate change; this check only says when it is due.
- **Query** (`ROLE=query`) serves the API from Postgres once it is verified; otherwise the answer is `ServiceUnavailable`. Every cursor Elasticsearch issued answers `CursorExpired`. It checks PostgreSQL to suppress results that stopped being complete. Rate limits are per caller, by the `X-Real-IP` address Railway's edge sets. `query.iriai.app` stays DNS-only on Cloudflare, since proxying it would make every caller a Cloudflare address. A paging session counts against the hourly limit only when an answer returns a cursor.
- **Health** (`/xrpc/_health`) reports ingestion lag, sidecar counts by state, inactive accounts and accounts awaiting a rebuild, skipped events, the backend serving new sessions with its size (`index`), and the search state behind that choice (`search`).

## Configuration

| Variable | Used by | Default |
|---|---|---|
| `DATABASE_URL` | both | required |
| `ROLE` | both | `all` (both in one process) |
| `CURSOR_KEY` | query | required: 32 random bytes, base64 |
| `JETSTREAM_URL` | ingester | `wss://jetstream2.us-east.bsky.network/subscribe` |
| `JETSTREAM_START_CURSOR` | ingester | live, when no checkpoint is stored |
| `APPROVED_ACCOUNTS` | ingester | the steward, `did:plc:kd5s7zyp7nje43ievnqhgzrr` |
| `RATE_LIMIT_PER_MINUTE`, `PAGING_SESSIONS_PER_HOUR` | query | 120, 60 |
| `SERVICE_ID` | query | `query.iriai.app` |
| `GITHUB_ALERT_TOKEN` | ingester | none: search issues are only logged |
| `GITHUB_ALERT_REPO` | ingester | `danielATiriai/iriai-proto-index` |
| `SEARCH_ON_EVENTS`, `SEARCH_ON_MIN_CALLS`, `SEARCH_ON_P95_MS`, `SEARCH_ON_TIMEOUT_RATE` | ingester | 500000, 500, 250, 0.01 |

`SEARCH_BACKEND_FORCE` and `ELASTICSEARCH_URL` are no longer read, but keep `SEARCH_BACKEND_FORCE=postgres` on the query service, and the Elasticsearch service, for as long as a revert to the PR 1 code may be wanted: that code serves from Elasticsearch without the first, fails events when Elasticsearch cannot be written, and Elasticsearch has had no writes since this version deployed.

## Development

```
npm ci
npm test                                   # unit tests
TEST_DATABASE_URL=postgres://… npm test   # everything, as CI runs it
npm run check-contracts                    # vendored contracts match the v0.1.0 release
node scripts/acceptance.mjs [base URL]     # live check of a deployed query service
node scripts/stream-check.mjs [host]       # spike 3: stop, restart and replay on public Jetstream
```

The **Acceptance** workflow runs the live check from GitHub against `https://query.iriai.app` (or another URL): it lists every sidecar the steward has published from its PDS, then checks that queryEvents near Peace Plaza returns exactly those events, nearest first and earliest first, a few per page, and that getEvent and the health report answer.

`contracts/` is vendored from the v0.1.0 release bundle (SHA-256 in `contracts/SOURCE.json`). `src/contracts/location.ts` and `sidecar.ts` are copies of the release's source files. CI checks both against the release. To move to a new contracts release, replace them and update `SOURCE.json`.
