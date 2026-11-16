# Adversarial review: "Event search without Elasticsearch until scale" v1 (build cb43cea, spec draft v1)

Reviewer: independent Claude reviewer (fallback for Codex Astra), Nov 16 2026. Read: BRIEF.md, facts.md, packet.src.html (plus the option and top-of-page renders), spec.md, eng-directions s16/s18/s3, code/*, spike-*.sql, spike-results.txt, files/CHECKS.txt and MODEL.json, and src/main.ts, src/ingest/transient.ts, src/ingest/ingester.ts and scripts/acceptance.mjs in /home/user/iriai-proto-index (d9ab983).

STAKES: DISAGREE: two lines are wrong. (1) "How it is undone" says the change is undone by setting the read switch back and redeploying Elasticsearch. The files show this undo serves a stale index. Once the deployment is removed, no writes reach Elasticsearch. The spec's "rebuilt by the backfill path" exists only inside the PR 2 controller states (spec 5.1, 6.3, 7). So the undo as written returns missing or deleted events to apps. (2) "Worst realistic failure" leaves out two harms. First, an outage of query.iriai.app and a stall of ingestion. Both roles block at startup until Elasticsearch answers (src/main.ts:18 `waitFor('Elasticsearch', () => ensureIndex(...))`), and the spec never removes that wait. Second, a Railway token that can reach every service and variable in placeproto/production, including Postgres, its secrets and the two placeproto-site services, not just "more reach than stated" for search. Severities below are rated at these corrected stakes.

## Blockers

### B1. Elasticsearch stays a hard startup and health dependency, so removing it takes query.iriai.app and ingestion down
- Where: spec 7 steps 3 and 4 and spec 5.1. Packet part 7, `/xrpc/_health` row ("The acceptance script still passes"), and part 8 rollout. Nothing in the spec covers it.
- What is wrong:
  - `src/main.ts:17-18` runs `await waitFor('Elasticsearch', () => ensureIndex(es, settings.elasticIndex))` for every role. `waitFor` retries forever. After step 3 (deployment removed), any restart or redeploy of `query` or `ingester` never reaches `server.listen`. That includes the PR 2 deploy in step 4, the Railway redeploy that a variable change triggers, a crash and a platform restart.
  - `src/ingest/transient.ts` classes every Elasticsearch connection error as transient. `ingester.ts` retries those "for as long as they last" and does not advance the checkpoint.
  - Spec 5.1 gates Elasticsearch writes on the controller state, but that table and the controller arrive only in PR 2. In PR 1 ("writes to both", spec 7.1) nothing says the Elasticsearch write is skipped or swallowed. Between step 3 and PR 2, every projection's Elasticsearch write fails, so ingestion stalls.
  - `server.ts` health reports `index: { ok: false }` once Elasticsearch is gone. `scripts/acceptance.mjs:97` checks `health.index?.ok === true`. The packet's "index counts the backend in use … acceptance still passes" appears nowhere in the spec.
- What happens and to whom: every app calling query.iriai.app gets no answer, or keeps the old deployment while the new one never goes healthy, depending on the Railway healthcheck setting [unverified]. New and edited events stop reaching search. The Acceptance workflow fails. This is the main job failing in normal use.
- How to confirm: read src/main.ts:17-18, transient.ts:15-27 and acceptance.mjs:97. Unset ELASTICSEARCH_URL's target locally and start `ROLE=query`: it logs "Elasticsearch not ready" forever.
- Fix: the spec needs a section that makes Elasticsearch optional at startup (connect lazily, and run `ensureIndex` only when the state needs Elasticsearch). It must define the PR 1 write gate, and swallow or queue Elasticsearch write failures before step 3. It must specify the health change that the packet already promises.

### B2. The Railway project token's reach is stated wrongly, and the packet calls it "narrow"
- Where: spec 6.3, D3 A ("It can act on no other service"). Packet D1 table D3 A "Built" row ("limited to the one Elasticsearch service"), the D3 "Why A" text, the 4e caption ("Two new credentials … both narrow"), part 8 Security ("a stolen Railway token could change the placeproto project"), and part 12 ("a narrow Railway token").
- What is wrong: a Railway project token is scoped to a project environment, not to a service. Within placeproto/production it can read and set every service's variables, deploy and remove any service, and act on volumes. That covers Postgres (DATABASE_URL with its password), CURSOR_KEY, the GitHub token, the token itself, and placeproto-site and placeproto-site-staging [docs: Railway public API, project tokens; confirm]. Only our code limits the calls to `SEARCH_ES_SERVICE_ID`. The token does not. The token also sits in the ingester, the process that parses untrusted Jetstream and PDS input.
- What happens and to whom: Daniel signs an accepted risk worded as "could change the placeproto project". The real exposure is disclosure of every production secret and possible deletion of the Postgres service or volume. This is the named worst failure in the stakes ("a Railway token with more reach than stated").
- How to confirm: Railway docs on project tokens. Or, with a throwaway project token, query `variables(projectId, environmentId, serviceId: <Postgres>)` through the public GraphQL API.
- Fix: state the true reach in plain words in D3 A's risk row and in part 8. Drop "narrow" and "can act on no other service". Consider whether D3 A should stay the recommendation, or whether D3 B (no token) should be, given that the cost saved by automation is about $13 a month.

### B3. Dual-write and reconcile lose updates, and the version scheme breaks under D1 B
- Where: spec 5.1, spec 6.3 (backfilling and on), spec 3 (`remove(uri, version)`), spec 4.2 option B.
- What is wrong:
  - Spec 5.1 says a failed Elasticsearch write "is retried by the next reconcile pass (6.3)". Spec 6.3 defines that pass only as "deletes any document whose uri is not current in Postgres". A failed `put` is never redone: a new event never appears, and an edit (new time or place) is never applied. Queries do not catch this, because `incompleteAmong` drops only incomplete events, not stale versions. `getEvent` on Elasticsearch has no suppression check at all (service.ts:133-137). Failed writes are realistic: spec 8 names Elasticsearch restarting at its 2 GB memory limit as the stop-point behaviour.
  - `remove` creates no new `event_docs` row, so no version is defined for it. If it reuses the closed row's id, an Elasticsearch external-version delete with an equal version is rejected as a conflict, and the document stays.
  - Under D1 B, "`put` upserts the current row by uri". `ON CONFLICT DO UPDATE` keeps the `bigserial` id, so every edit carries the same external version and Elasticsearch rejects it. During a backfill under B, a copy that lands first blocks the live edit. Neither the D1 B rows nor its "What B changes" list mention any of this.
  - Elasticsearch keeps delete tombstones only for `index.gc_deletes`, 60 s by default. A backfill bulk that lands more than 60 s after a concurrent delete resurrects the document. Only the end-of-backfill reconcile catches it, and later failures are not caught at all.
- What happens and to whom: once Elasticsearch is on at scale, apps get missing new events and stale event data with no alarm. That is the stakes' worst failure, "different or missing search results".
- How to confirm: read spec 5.1 against 6.3. In Postgres, `insert … on conflict (uri) do update set doc = excluded.doc returning id` returns the same id. In Elasticsearch, `PUT idx/_doc/x?version=5&version_type=external` twice gives a 409 the second time.
- Fix: allocate a fresh `nextval` for every put and every remove under both D1 options. Make reconcile compare each document's `_version` with the current `event_docs.id` and re-put stale or missing documents, not only delete extras. Run reconcile while state is `on` whenever the dirty flag is set. Raise `gc_deletes` on the new index above the backfill duration.

## Should fix

### S1. The snapshot in spec 4.3 (D1 A) is not exact as written
- Where: spec 4.2 and 4.3 (`valid_to = now()`, `valid_from = now()`, `openSnapshot` returns `now()`). Packet part 4 ("a short lock makes that moment exact") and the D1 A text.
- What is wrong: in Postgres, `now()` is the transaction start time, not the time the lock was taken or the commit time. Example:
  1. Writer W runs `BEGIN` at t1, so its `now()` is t1.
  2. Snapshot S begins at t2 > t1 and takes the shared lock before W asks for the exclusive one. S returns T = t2.
  3. W then locks and writes `valid_from = t1 < T`, and commits after S.
  4. Page 1 (read before W commits) sees the old row. Page 2 sees the new row and not the old one.
  The edited event is skipped or shown twice, which is exactly what D1 A promises cannot happen. Separately, the read uses `valid_from <= T`, while the argument relies on a strict `<`.
- How to confirm: run two psql sessions in the order above.
- Fix: take `clock_timestamp()` after acquiring the lock, on both sides. Better, snapshot on a sequence watermark: store the creating and closing version ids, and have the snapshot read `max(id)` under the lock. That also removes any dependence on the clock.

### S2. The verification can never pass as specified, and the packet misstates why the backends differ
- Where: spec 4.4 (last paragraph) and 6.3 verifying ("distances compared to the millimetre"). Spec 7.2 uses the same comparison. Packet part 4 says "Same answers … Two events at exactly the same spot can trade places … in the last digit".
- What is wrong:
  - Elasticsearch sorts and filters on geo_point doc values, which are quantized to 32 bits per axis: about 4.7 mm in latitude and 9.3 mm × cos(lat) in longitude.
  - Lucene's haversine (`SloppyMath.haversinMeters`) is an approximation [docs, unverified version].
  - Postgres computes on exact doubles. So per-event distances differ by up to a few millimetres, and millimetre rounding mismatches on most distance-sorted shapes.
  - Two events "at exactly the same spot" have equal distances in both backends and tie-break identically. The real reorder risk is events within a few millimetres of each other's distance, plus events near a radius edge.
  - Counts and first pages are also compared while writes continue, and Elasticsearch's refresh adds about a 1 s lag.
- What happens and to whom: step 7.2 fails on production, or someone loosens the tolerance by hand. At the trigger, verifying fails, two failures disable the controller, and search stays on slow Postgres. Daniel is told the backends give "the same answers".
- How to confirm: index one point at 37.785054, -122.429827 and compare the `_geo_distance` sort value with `arc_m` in Postgres.
- Fix: compare order with a centimetre-scale tolerance and treat near-ties as equal. Optionally store quantized coordinates in Postgres. Freeze writes, or compare at a version watermark after a refresh. Correct the packet sentence.

### S3. Spike numbers are reported selectively, and some are not backed by the pack
- Where: spec 4.6 table, spec 4.4 ("1.5 s instead of 45 to 95 ms"). Packet part 4 "Fast enough, measured" (tagged [code]) and the D1 and D2 "Why" texts.
- What is wrong:
  - "A week, by start (with the range index) 44 to 97" and "A month … 260 (1 run)" have no script or output in the pack. spike-schema.sql creates no range (gist) index and no materialized-CTE query.
  - The pack's own outputs for that query at 1M are 189 / 211 ms (plain form) and 1,470 / 1,568 ms (the union form that D1 A requires).
  - The table marks 10k and 100k as "n/m", although they were measured (2.4 / 3.2 and 21.7 / 25.3).
  - The table takes the better of two runs per row: 2 km p95 13 (other run 18.1); whole world 247 / 272 from the plain form, where the union form D1 A will run measured 290 / 343.
  - "Later page (keyset)" mixes forms: plain at 10k and 100k, union at 1M, where plain measured 201 / 228. The query is a first page with a lower bound, not a later page.
  - The data was in memory on 4 vCPU. Railway Postgres holds 74 MB today, against 1.24 GB of table at 1M. The packet mentions only CPU.
  - The bench wraps queries in `count(*)`, so the 1.1 KB `doc` is never fetched.
  - The packet's "under 2 ms at today's size" and "D1 A … the versioned table stays fast" rest on these numbers.
- What happens: Daniel picks D1 A and D2 on rosier numbers. At 1M the default search (290 to 343 ms in the union form, on faster hardware) already exceeds the 250 ms trigger. On Railway the speed trigger may fire well below 750,000, which moves the cost forecast.
- Fix: put every reported number's script and output in the pack. Report the D1 A (union) form throughout, one run per row, with the plain form only as a comparison. Add the memory caveat.

### S4. Statements that a non-recommended option makes false are not marked as decision points
- Where (packet.src.html, all outside any `data-dp`, confirmed in option-D3-B.png and option-D1-hold.png):
  - Lede (line 260): "comes back by itself only when the numbers say Postgres is too slow". False under D2 B (size only) and D3 B (Daniel's word). Under D1 hold nothing is stopped at all.
  - Part 1 callout: "You approve moving search to Postgres, stopping Elasticsearch, and the trigger". False under D1 hold. Under D2 hold or D3 hold, no trigger is approved.
  - Part 2 goal: "Elasticsearch returning by itself". False under D3 B.
  - Part 3 step 6: "Elasticsearch finishes the open sessions for an hour, then stops". Under D3 B it stops only on Daniel's sentence.
  - 4e caption: "Two new credentials". 4f: "the tokens above". Under D3 B there is one.
  - Part 9 risk 4 mitigation: "a monthly dry run that calls Railway's read-only API". There is no token under D3 B. The dry run is also not in the spec.
  - Part 10 gate 3: "Railway calls tried on a throwaway service" (none under D3 B) and "controller" (not built under D2 hold, by the packet's own D2 hold text).
  - Part 12: "Consequence of A: a narrow Railway token" (see B2).
- How to confirm: open option-D3-B.png and option-D1-hold.png. The lede and callout are unchanged.
- Fix: put each of these in a decision point with option text, or word them so they hold under every selection. This is s16 part 1 (Nov 13): "Under any selection the page states nothing that the selection makes false."

### S5. D3 A silently overrides standing directions s3 and s16 on Railway sentences
- Where: D3 A text, part 1 ("It does not start any Railway change"), and part 8 "Earlier decisions this departs from" (names only the Oct 13 stack) and "Sentences … Nothing when the trigger fires".
- What is wrong: s3 says Railway service changes wait for Daniel's typed sentence, which authorizes only the action it names. s16 says "proposal approval never replaces the sentence". Under D3 A the code deploys and removes a Railway service with no sentence, indefinitely. Daniel's ask ("triggering elastic search automatically") supports the outcome, but the departure must be named as a reversal under agent rules section 9, with the s3 and s16 references. As written, part 1 tells him approval starts no Railway change.
- Fix: add a row in part 8 naming s3 and s16. Make D3 A explicitly "a standing authorization, replacing the sentence for these two actions". State it in part 1.

### S6. Controller failure paths leave Elasticsearch running and billing, and the stop-point behaviour is misstated
- Where: spec 6.3 ("Any failure before `on` goes back to `off`"), draining, spec 8 stop point. Packet part 3 step 7, part 8 Running cost ("users see slower search, not an outage").
- What is wrong:
  - A failure in starting, backfilling or verifying returns to `off` without removing the deployment the controller started. After 2 failures the controller is `disabled`, with Elasticsearch still running at about $13 to $20 a month.
  - A failed `deploymentRemove` in draining is undefined, and the state can say `off` while the service runs.
  - While on, Elasticsearch restarting at the 2 GB limit is not a fallback for everyone. Later pages of Elasticsearch sessions answer 503 (spec 6.3). `getEvent` has no Postgres fallback specified. Writes fail and are not redone (B3). No rule drains a crash-looping Elasticsearch.
  - The packet's failure list omits all of this.
- Fix: every failure exit removes what the controller started, then confirms it is stopped. A draining failure retries and raises an issue. Add a "crash-looping while on" rule that drains. Give `getEvent` a fallback. Correct the users-see text.

### S7. The Running cost row does not meet s18 item 5
- Where: packet part 8 "Running cost (Railway)", spec 8.
- What is wrong:
  - The stop point (a 2 GB memory limit) bounds Elasticsearch memory only, not vCPU ($20 per vCPU) and not replicas. Calling it a cap at "$20 a month" overstates it.
  - Prices are tagged [docs], but facts.md says they come "from memory of Railway's pricing page; not re-read today", so they are [unverified].
  - "$16 a month" for the project (lede, part 2) has no source in facts.md or the spec.
  - The row gives point values where s16 asks for ranges for today, after launch and at 10 times the use.
  - GitHub is now touched (an issues token) but gets no one-line "cannot add cost" (s18 item 1).
  - The new Postgres exposure estimate is placed "inside the $70 you accepted". It is not offered for Daniel to accept by name (s18 item 2).
  - Under D3 A, spend starts without his word. s18 item 4 does not strictly cover usage, but the row should say so.
- Fix: correct the tags and ranges, add vCPU or say it is unbounded, source or remove the $16, add a GitHub line, and add an acceptance line for the estimate.

### S8. The list of Daniel's sentences is incomplete (s3)
- Where: packet part 8 "Sentences you will type", spec 7 and 9.
- Missing:
  - Clearing `SEARCH_BACKEND_FORCE` (spec 7 step 4, a Railway variable change).
  - Setting the ingester variables `RAILWAY_PROJECT_TOKEN`, `GITHUB_ALERT_TOKEN`, `SEARCH_ES_SERVICE_ID` and `SEARCH_ENV_ID` (Railway variable changes even when made through the secrets helper).
  - The rollback sentences (redeploy Elasticsearch, `SEARCH_BACKEND_FORCE=elastic`).
- s3 asks for one list of everything knowable, grouped by place, with later items named as coming later.

### S9. The rollback serves a stale index
- Where: spec 7 ("Undo … its index is rebuilt by the backfill path"), packet part 8 Rollout ("about 15 minutes"), part 7 ("every step undoes by setting the read switch back").
- What is wrong: after step 3 no writes reach Elasticsearch. The backfill exists only as a controller state (PR 2) and runs only through `ready` → `starting`. Before PR 2 there is no rebuild at all. Redeploying the old volume and forcing reads to Elasticsearch serves events as they were at removal: new ones are missing, edits are stale, and deletions are dropped from queryEvents but still served by getEvent.
- Fix: specify a rebuild command (or a forced controller backfill) usable from PR 1 on. The rollback is then "redeploy, rebuild, verify, switch", with a realistic time.

### S10. Coverage (part 13) and its reverse check are wrong
- Packet claims with no spec source, although part 13 says "every claim in parts 2 to 10 was looked up in the spec":
  - `_health` gains backend, timings and state, and the acceptance script passes.
  - The monthly Railway dry run.
  - The forced dry run with zero thresholds.
  - The 12-month deferral trigger.
  - The staging index.
  - The $16 project cost.
- Spec content the packet leaves out or softens:
  - Elasticsearch write failures no longer block ingestion; they set a dirty flag (5.1). This changes today's behaviour.
  - Later pages of an Elasticsearch session return 503 (6.3).
  - The global write lock delays every first page behind in-flight writes (4.2 and 4.3).
  - The shadow-query cap of one per second (6.1).
  - Spec 6.4 tries the Railway calls in gate 2; the packet says gate 3.
- Every pointer links to `files/spec.md` with no heading anchor, and s16 part 13 requires links to the heading.

### S11. The cursor rule contradicts itself across the switch
- Spec 3: a cursor whose `be` is not "serving paging sessions now" answers CursorExpired. Spec 7.2: "Elasticsearch cursors keep working for their last hour" after `FORCE=postgres`. Draining relies on the second reading.
- Fix: define "serving" as "accepting cursors" (new sessions excluded) and say how `SEARCH_BACKEND_FORCE` affects it.

### S12. The default reading is 2,336 words, against s16's "about 1,500"
- CHECKS.txt shows the build counted it but did not act on it. Fold the part 4 and part 8 detail.

## Nits
- N1. Spec 4.4: the antimeridian box predicate must be parenthesised (`(lon >= west or lon <= east)`), or the OR escapes the AND chain.
- N2. Part 8 "Corgo and the portal call query.iriai.app" is stated as fact. The stakes say this is unverified. "9 events" is the Oct 13 count (facts.md: "No later count was read"), so tag both.
- N3. The spike's closed rows all have `valid_to` two days old, so the union's closed-rows branch is always empty. Its cost under live churn is unmeasured.
- N4. `pg_try_advisory_lock(7140016)` is session-level. With a `pg.Pool` it must be released on the same client, or it stays held by a pooled connection.
- N5. The first-page fallback waits for the Elasticsearch client's 10 s timeout with one retry, so up to about 20 s before Postgres answers.
- N6. Part 6: "Postgres only, no trigger: nobody notices when it gets slow" is unfair, because "Postgres plus an issue when the trigger is met" (D3 hold) is a real, cheaper option. Make it a row.
- N7. The production comparison in 7.2 uses stored query shapes that exist only in PR 2. With 9 events clustered in Japantown, it cannot exercise antimeridian, null, tie or edge cases. Say that CI is the real guard.
- N8. Search load now shares the Postgres instance and the pool of 10 with ingestion, with no `statement_timeout`. Add this to "Effects on other features".

## Answers to the brief's specific questions
- **Does the Postgres design reproduce search.ts and service.ts?**
  - These match: filters (AND, radius, box with antimeridian, interval overlap with point events and `timeValid`), sort nulls-last, byte-order tie-break, suppression and error mapping.
  - Distance differs at the millimetre level (S2).
  - Startup and health do not carry over (B1).
  - `getEvent` fallback during `on` is unspecified (S6).
- **Is the snapshot in 4.3 exact?** No (S1).
- **Are the backfill and dual-write safe against lost or resurrected documents?** No (B3).
- **Are the spike numbers fairly reported?** No (S3).

VERDICT: REQUEST CHANGES. As specified, removing Elasticsearch takes query.iriai.app and ingestion down on the next restart, the dual-write loses updates, and the packet presents a project-wide Railway token as narrow, so Daniel cannot safely approve this version.
