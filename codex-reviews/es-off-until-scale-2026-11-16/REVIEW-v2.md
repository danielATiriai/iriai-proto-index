# Adversarial review, round 2: "Event search without Elasticsearch until scale" v2 (build e12311c, spec draft v2)

Reviewer: independent Claude reviewer (fallback for Codex Astra), Nov 16 2026. Read: BRIEF.md, facts.md, REVIEW-v1.md, CODEX-TRIAGE-v1.md, packet.src.html (plus the option renders for D1 hold and D3 B), spec.md, spec.html (anchors), eng-directions s16/s18/s3, code/* (unchanged from round 1), spike2-*.sql, spike2-results.txt, files/CHECKS.txt and MODEL.json.

STAKES: DISAGREE: the lines themselves now match the files, but two unknowns could change the severity of the D3 A findings (R2-B1, R2-S1), and the stakes do not name them. (a) Whether a request over Railway's private network wakes a sleeping Serverless service, and what happens to the first requests during the wake and the Elasticsearch cold boot (dropped, refused or held). facts.md says this is "not confirmed from the docs". (b) How long the gaps without traffic to Elasticsearch will be while it serves at the trigger, from queries plus ingester writes. Gaps over 5 to 10 minutes put the serving Elasticsearch to sleep. If gaps are common (nights, for example), R2-B1 is a blocker in normal use. If traffic never pauses for 5 minutes, it is a should fix. I rate it a blocker until (b) is shown.

## Round-1 fixes, checked

| # | Verdict on the fix | Notes |
|---|---|---|
| B1 start-up / health | Right in direction; breaks something new | Spec 5.2 and 5.3 remove the start-up wait and define health. But 5.1 now swallows Elasticsearch write failures from PR 1 on, while Elasticsearch still serves reads, and `SEARCH_ES_WRITES` has no stated default. See R2-S2. |
| B2 token reach | Right | D3 B states the full environment reach in its rows, in part 8 Security, 4e and Earlier decisions. Recommendation moved to a credential-free option. |
| B3 dual-write | Mostly right; two races remain | Fresh sequence number per write under both D1 options, version-comparing reconcile, `gc_deletes` 2 h: good. See R2-S3 for a delete race and a dirty-flag race. |
| S1 snapshot | Right in principle; one ordering is not stated | The sequence watermark removes the clock. Exactness needs `nextval` to be taken after the exclusive lock is held, and the spec does not say so. See R2-S4. |
| S2 verification | Right | 5 cm order tolerance, refresh, id-and-version set check, retries. |
| S3 spike | Mostly right | Consistent single run in the D1 A form with the document fetched; numbers in spec 4.5 and part 4 match spike2-results.txt. Remaining issues in R2-N2. |
| S4 option-false statements | Mostly right | Two remain (R2-S6). |
| S5 standing directions | Right | Made a D3 decision point. |
| S6 failure paths | Right for B and C; does not work under the new A | "Turns Elasticsearch off and confirms it is off" cannot be done under Serverless. See R2-S1. |
| S7 cost row | Right | Ranges, sourced prices, $40 bound from memory and vCPU limits, GitHub line, acceptance sentence for the Postgres estimate. |
| S8 sentences | Missing one | No sentence or default for `SEARCH_ES_WRITES=on` (R2-S2). |
| S9 rollback | Right | `npm run search:rebuild-es` in PR 1. |
| S10 coverage | Right | Anchors resolve (checked against spec.html ids s1 to s10); packet-only claims named. |
| S11 cursor rule | Right | `elastic_cursors_until`. |
| S12 length | Not fixed, and the triage's reason is wrong | See R2-N1. |
| N1 to N8 | Right | N4, N5 and N8 checked in spec 6, 6.4 and 4.4. The N3 churn is present in spike2-load.sql. |

## Blockers

### R2-B1. Under D3 A, the serving Elasticsearch falls asleep in quiet periods; the controller then drains it and locks it out for 30 days
- Where: spec 6.3 D3 A ("'On' is the controller sending requests until the cluster answers"), spec 6.4 `on` (drain when more than 20 first pages fall back in 10 minutes; at most one switch-on in 30 days). Packet D3 A rows, part 4 "On and off", part 9 risk 3.
- What is wrong:
  - Railway sleeps a Serverless service 5 to 10 minutes after its last outbound packet (facts.md, Railway docs). Nothing in the spec keeps Elasticsearch awake while it is the serving backend (`starting` through `draining`).
  - The controller ticks every 10 minutes. Reconcile runs only when the dirty flag is set, and daily. So any gap of 5 to 10 minutes in queries and ingester writes puts the serving Elasticsearch to sleep.
  - On the next traffic, all open point-in-time snapshots are gone. Every later page answers `CursorExpired`, or `ServiceUnavailable` during the boot.
  - Every first page waits up to 2 s and then falls back to Postgres for the length of the cold boot. Booting an index of 500,000 events takes tens of seconds, and the behaviour of requests during the wake is unknown (facts.md).
  - A morning burst of more than 20 first pages in 10 minutes moves the controller to `draining`. Elasticsearch is turned off, and "at most one switch-on in 30 days" keeps search on the slow Postgres for a month. Each drain also raises an issue.
  - The packet presents A as "fully automatic". Its only stated risk is the opposite one, Elasticsearch never sleeping.
- What happens and to whom: at the scale where Elasticsearch is needed, apps get slow first pages, broken paging after every quiet spell, and then a month on Postgres. That is the main job (serve from Elasticsearch at scale) failing in normal use, if traffic has gaps (stakes unknown (b)).
- How to confirm: spec 6.3 and 6.4 have no keep-alive. Or on Railway: a Serverless service that receives no requests for 10 minutes shows as asleep on its metrics.
- Fix:
  - While the state is `starting`, `backfilling`, `verifying`, `on` or `draining`, the controller (a timer, not the 10-minute tick) sends Elasticsearch a cheap request every 2 to 3 minutes. Note that responses count as outbound traffic in Railway's rule; confirm in gate 2.
  - Exclude wake-related fallbacks from the drain counter, or wake Elasticsearch before counting.
  - Add the cold-boot behaviour to the D3 A rows and to part 9.

## Should fix

### R2-S1. D3 A cannot "confirm it is off", so the failure and cost paths rely on hope
- Where: spec 6.4 Failure ("turns Elasticsearch off again (6.3) and confirms it is off … a failure to turn it off is retried every tick and raises an issue each hour"); spec 8 stop point; packet 4d caption ("every way back to off turns Elasticsearch off"), part 3 step 7.
- What is wrong:
  - Under A, "off" means "we stop sending requests". The controller holds no Railway credential, so it cannot read whether the service sleeps. Any request it sends to check would wake it.
  - "Confirms it is off" and the hourly retry issue have no mechanism under A.
  - If something keeps Elasticsearch awake after gate 2, nothing in our code notices. Examples: a stray client, an old rebuild run left going, an Elasticsearch feature that phones out, a Railway change to the sleep rules. Elasticsearch then bills about $13 to $20 a month again. Only the $55 workspace alert would show it, and that alert may never trip.
  - The part 9 mitigation ("gate 2 watches it sleep … before the old deployment is relied on") is a one-time check.
- Fix:
  - Say plainly in the D3 A rows, part 8 and part 9 that under A our code cannot confirm Elasticsearch is asleep.
  - Add a recurring check that needs no Railway token. Two options: a read-only Railway metrics read by a thread or by Daniel monthly, or the vendor cost register's monthly review naming the Elasticsearch line. Alternatively, state that this is accepted.
  - Make the spec's failure text depend on D3: A "stops sending traffic", B "removes and confirms", C "asks".

### R2-S2. From PR 1 on, Elasticsearch still serves reads but silently drops failed writes; `SEARCH_ES_WRITES` has no default
- Where: spec 5.1 (failed Elasticsearch write sets `search_control.dirty` and moves on; "the reconcile (6.4) repairs it"), spec 5.2, spec 7 steps 1 to 3, packet part 8 sentences.
- What is wrong:
  - PR 1 ships the swallow-on-failure write path while `SEARCH_BACKEND_FORCE` defaults to `elastic`. Production keeps searching Elasticsearch until Daniel's step-3 sentence.
  - The reconcile is part of the PR 2 controller. Between PR 1 and step 3, a failed Elasticsearch write (a restart, a timeout, the 512 MB heap cut that the cost thread may apply) leaves Elasticsearch stale or missing an event, and nothing repairs it.
  - Today the same failure stalls ingestion and replays the event, so it is repaired. The step-2 comparison is manual and one-off.
  - Spec 7.1 says "writes to both with `SEARCH_ES_WRITES=on`", but neither the spec nor the sentence list gives its default or a sentence to set it. If the default is off, every write after the PR 1 deploy misses the Elasticsearch that is still serving production.
- What happens and to whom: apps calling query.iriai.app get missing or stale events for the days between the PR 1 deploy and step 3.
- How to confirm: read spec 5.1, 5.2 and 7.1 together. Search the spec for a `SEARCH_ES_WRITES` default (there is none).
- Fix:
  - State that the default is `on`, or add the sentence.
  - In PR 1, keep today's behaviour (fail and replay) while Elasticsearch is the read backend. Or run `search:rebuild-es` or the reconcile whenever dirty is set before step 3.
  - Run the step-2 comparison immediately before step 3, not just after deploy.

### R2-S3. Two reconcile races still lose documents
- Where: spec 6.4 Reconcile.
- (a) Delete race. The reconcile reads Postgres and sees no current row for X. X is then re-put at version v. The reconcile next deletes X in Elasticsearch "with a fresh version" w, taken after v, so w > v. The live document is deleted. The write succeeded, so dirty is not set. X stays out of Elasticsearch search until the daily reconcile, up to 24 hours.
  - Fix: take the fresh version before reading Postgres, and delete only if it is below the current version. Or re-check Postgres for X under the write lock after taking the number.
- (b) Dirty-flag race. The reconcile "runs … while `dirty` is set (then clears it)". A write that fails during a reconcile, after the reconcile passed that id, sets dirty, and the reconcile's final clear erases it. The document stays stale until the daily pass.
  - Fix: clear dirty at the start of the pass (or use a counter or generation), not at the end.
- What happens: a missing or stale event at scale for up to a day, with no alarm.
- How to confirm: interleave the steps by hand against spec 6.4.

### R2-S4. The sequence watermark is exact only if `nextval` is taken under the exclusive lock, and the spec does not say so
- Where: spec 4.2 ("takes a new number … inside one transaction that holds `pg_advisory_xact_lock`") and 4.3.
- What is wrong: `nextval` is non-transactional. If a writer calls `nextval` (gets N) before acquiring the exclusive lock, this can happen:
  1. A snapshot takes the shared lock and reads W ≥ N.
  2. The writer then locks and writes version N ≤ W, after the snapshot.
  3. Pages read before that commit miss the row; later pages see it.
  This is the same class of bug as round-1 S1. A second edge: on a fresh sequence (`is_called = false`), `last_value` is 1 before any write, so the first write, numbered 1, lands inside an earlier snapshot. Use `case when is_called then last_value else 0 end`.
- Fix: state "lock first, then `nextval`" in 4.2 and in the D1 A test (spec 10). Handle `is_called`.

### R2-S5. The 2 s statement timeout turns the slow default search into errors, and the spec does not say how timeouts count toward the trigger
- Where: spec 4.4 (`statement_timeout` 2 s), spec 4.5, spec 6.1 and 6.2 ("p95 of the search step"), packet part 8 Performance ("cannot starve ingestion").
- What is wrong:
  - The default search (a coordinate and no radius, which is the README's first example) scans and sorts every current row on every page. At 1M it took 290 / 311 ms with the whole 1.33 GB table cached on 4 vCPU.
  - Railway's Postgres holds 74 MB today. The spec itself says the table "will not all be cached" at scale and that the CPU is slower.
  - Before the size trigger, these searches can pass 2 s and return `ServiceUnavailable`. There is no fallback from Postgres.
  - If timed-out searches are left out of the p95 (as an error rather than a duration), the speed trigger under-reports exactly when users suffer most. Under D2 B nothing reacts at all.
  - "Cannot starve ingestion" is true for the pool only. Five concurrent full scans still compete with ingestion for the same Postgres CPU and I/O.
- Fix: count a timeout as at least 2,000 ms (or as a trigger condition of its own) in 6.1. Say in part 4 or part 9 that the default search can fail rather than slow down before the trigger. Soften the performance row.

### R2-S6. Two statements are false under a non-recommended option and are not decision points
- Part 1 (line 299): "It does not start any Railway change: each one waits for your sentence (part 8)." Under D3 B the approval itself creates a standing authorization for the controller to deploy and remove Elasticsearch without a sentence. The packet's own D3 B Earlier-decisions text says so. Make it a D3 decision point.
- Part 1 callout (line 284): "You approve search on Postgres, Elasticsearch off". Under D1 hold nothing is built and Elasticsearch keeps running (option-D1-hold.png shows the callout unchanged; only the lede carries the "D1 held" badge). Make it a D1 decision point, or word it conditionally.

## Nits
- R2-N1 (S12, with a specific error in the triage record). The triage says the default reading rose to 2,490 words because of "a third D3 option with its rows and decision points". The decision bodies sit inside `<details class="dec">`, folded by default, so they are not part of the folds-closed count. The growth comes from the visible text in parts 4, 7 and 8. Part 8's table could fold under its row headings. s16's "about 1,500" is still missed by 66%.
- R2-N2. spike2-load.sql starts with `\i …/scratchpad/spike-load.sql`, which facts.md calls superseded and left out of this pack. So the pack cannot reproduce the load on its own. The spike also runs the v1 timestamp schema (`valid_from`, `valid_to`), not v2's version and `closed_version` columns, and has no closed-row lat/lon index (spec 4.1). Lastly, the "By start, after a cursor" row is still a first page with a lower bound, not a page reached through a cursor (round-1 S3 sub-point).
- R2-N3. Spec 3 has `getEvent` read Postgres from the PR 1 deploy on. If the query service serves before the ingester's first fill finishes, real events answer `SidecarNotFound` or `EventNotFound` for that window. With 9 events the window is seconds, but it comes before the step-2 comparison. Say the query service waits for the first fill.
- R2-N4. Under D3 B or C, an Elasticsearch cursor accepted inside the `elastic_cursors_until` window after the deployment is removed gets `ServiceUnavailable` (connection refused), not `CursorExpired`. Map connection errors on cursor pages to `CursorExpired` once Elasticsearch is off.
- R2-N5. D3 A: whether Railway Serverless sleeps a service with an attached volume, and whether it stops it gracefully, is not in facts.md. Add both to the gate-2 checks. Also note that under A the Elasticsearch deployment's image and old index stay frozen for as long as it sleeps, possibly a year.
- R2-N6. Part 8 "Earlier decisions" (D3 A): "waking a sleeping service is not a Railway change" is fair under s3. But under A, spending of about $13 to $20 a month begins without Daniel's word. Say so in one line in the D3 A "You" row, so the automatic spend is visible as what he asked for.

## Answers to the brief's specific questions
- **Does the design reproduce search.ts and service.ts?**
  - Filters, sorts, ties, nulls, antimeridian (now parenthesised), keyset paging, suppression and the documented distance tolerance all hold.
  - Errors differ in one respect: Postgres timeouts become 503 at scale (R2-S5).
  - `getEvent` now skips Elasticsearch entirely, which is an improvement.
- **Is the snapshot in 4.3 exact?** Yes, if `nextval` follows the lock (R2-S4).
- **Are the backfill and dual-write safe against lost or resurrected documents?** Much improved, but not yet: R2-S3, and R2-S2 before PR 2.
- **Are the spike numbers fairly reported?** Yes, apart from the provenance and schema gaps in R2-N2.

VERDICT: REQUEST CHANGES. Round 1's blockers are fixed. But the new recommended D3 A has no keep-awake, so a serving Elasticsearch sleeps in quiet periods and the controller drains it for 30 days, and the PR 1 write path can leave the still-serving Elasticsearch stale. Both need a spec change before Daniel can approve.
