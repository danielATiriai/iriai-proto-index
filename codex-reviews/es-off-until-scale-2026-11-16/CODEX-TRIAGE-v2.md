# Triage, round 2: "Event search without Elasticsearch until scale"

Under review: packet v2, build e12311c, spec draft v2 (2026-11-16). Code context iriai-proto-index main d9ab983.
Reviewer: the same independent Claude reviewer as round 1 (fallback; Codex Astra could not be started from this thread, which holds no message from Daniel).
Review: REVIEW-v2.md. Fixes land in packet v3 and spec draft v3.

## Stakes

Given (as corrected after round 1): proposal or plan for production infrastructure; reaches every caller of query.iriai.app; worst failure: wrong or missing results, query.iriai.app or ingestion down after Elasticsearch is turned off, a controller that runs up cost, or a credential with the whole project's reach; undone by turning Elasticsearch on, running the rebuild command and switching reads back; nothing said by Daniel beyond the standing rules.

Reviewer: STAKES: DISAGREE, naming two unknowns that could change R2-B1 and R2-S1: (a) whether a request over Railway's private network wakes a sleeping Serverless service, and what happens to requests during the wake; (b) how long traffic gaps are while Elasticsearch serves.

Thread: (b) is made moot by design: spec 6.3 now sends a cluster-health request every 2 minutes from `starting` to the end of `draining`, so the gap never reaches Railway's 5-minute sleep. (a) stays unknown and is now a named gate-2 check (spec 6.3), with B or C as the fallback if it fails; R2-B1 is rated a blocker and fixed regardless. Stakes lines unchanged otherwise; the unknown is carried into the round-3 brief.

## Findings

| # | Finding | Severity | Decision | Reason / what changed |
|---|---|---|---|---|
| R2-B1 | Under D3 A a serving Elasticsearch sleeps in quiet hours and is drained | blocker | fixed | Spec 6.3: keep-awake health request every 2 minutes while starting to draining. Spec 6.4: the drain counter counts fallbacks only while keep-awake requests are answered. Packet D3 A Built row and part 4 "On and off" say so. |
| R2-S1 | D3 A cannot confirm Elasticsearch is off | should fix | fixed | Spec 6.3 and 6.4: our code cannot see the sleep; a weekly read of Elasticsearch's memory on Railway (read-only, cost-monitoring routine) opens an issue if non-zero while `off`. Packet D3 A Risk row, part 4, part 9 risk say so. |
| R2-S2 | PR 1 drops failed writes while Elasticsearch still serves; `SEARCH_ES_WRITES` has no default | should fix | fixed | Spec 5.1: in PR 1, while Elasticsearch serves, a failed write keeps today's fail-and-replay; swallowing starts only once Postgres serves. `SEARCH_ES_WRITES` defaults to on (spec 5.1, 7 step 1). |
| R2-S3 | Reconcile delete race and dirty-flag race | should fix | fixed | Spec 6.4: `dirty_gen` incremented on every write, `repaired_gen` set to the generation read at the start of a pass, so a write during a pass leaves it dirty; a delete of an extra re-checks under the lock that the event is still absent. |
| R2-S4 | `nextval` must come after the exclusive lock; `is_called` | should fix | fixed | Spec 4.2 and 4.3: `nextval` taken after lock 7140015; watermark is 0 when `is_called` is false. |
| R2-S5 | 2 s timeout turns the slow default search into errors; trigger counting unstated | should fix | fixed | Spec 6.1: a timed-out search counts as 2,000 ms toward p95 and as a timeout. Packet part 3 step 7 says near the trigger a slow search can get "service unavailable". |
| R2-S6 | Two statements false under other options | should fix | fixed | Part 1 "It does not start any Railway change" is now a D3 decision point (A: setting changes wait for a sentence, waking and its cost do not; B: standing authorization after setup; C: unchanged). Callout "Elasticsearch off" is a D1 decision point (Hold: nothing built, Elasticsearch keeps serving). |
| R2-N1 | Triage's word-count reason wrong; fold part 8 | nit | fixed | Correction recorded: decision bodies are folded and do not count; the growth was in parts 4, 7 and 8. Part 8's table is now folded behind a visible one-line cost and sentences summary. |
| R2-N2 | Spike provenance and schema | nit | fixed | spike-load.sql included in the round-3 pack; spec 4.5 states the timestamp schema, the missing closed-row index, and renames the keyset row "from a later position (keyset, as a cursor page reads)". |
| R2-N3 | Query service may serve before the first fill | nit | fixed | Spec 3: the query service answers only after the ingester records the first fill done. |
| R2-N4 | Connection errors on Elasticsearch cursor pages under B or C | nit | fixed | Spec 3: once Elasticsearch is off, a connection error on a cursor page answers `CursorExpired`. |
| R2-N5 | Gate-2 checks for volume sleep and clean stop; frozen image | nit | fixed | Spec 6.3: gate 2 checks a sleeping service with a volume stops cleanly and keeps its data; image and old index stay frozen, and the backfill always builds a fresh index. |
| R2-N6 | D3 A spend begins without Daniel's word | nit | fixed | D3 A "You" row and part 1 say the $13 to $20 a month while on needs no further word, as he asked. |

Rejected: none.

## Length

Round 2 was a fix round on round 1's step back rules not yet applying (they start at round 3). Folding part 8 behind a one-liner removes most of its visible text; the additions (D3 A rows, part 1 decision point, part 3 step 7 sentence) are fixes the reviewer asked for. The build's word count is recorded in CHECKS.txt.
