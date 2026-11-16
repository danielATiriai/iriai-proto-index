# Triage, round 5: "Event search without Elasticsearch until scale"

Under review: packet v5, build 924e716, spec draft v5 (2026-11-16). Code context iriai-proto-index main d9ab983.
Reviewer: the same independent Claude reviewer as rounds 1 to 4 (fallback; Codex Astra could not be started from this thread).
Review: REVIEW-v5.md. Fixes land in packet v6 and spec draft v6.

## Stakes

Given: as in round 4. Reviewer: STAKES: AGREE.

## Step back, third time

What keeps recurring: each fix made one backend's readiness explicit but left a path where the other stops serving first (round-1 B1, R2-B1, R3-S1, R4-S1, now R5-S1 and R5-S2). The rule of spec 5.2 now closes that class from both sides, with the reviewer's suggested shape:
- Nothing ends Elasticsearch use while Postgres is not current: the ingester ignores `SEARCH_ES=off` and a drain until it is, and reports that in health. So at least one backend is always current after the first fill.
- A drain acts only on keep-awake evidence gathered since the ingester started.
- Postgres becomes current only through a check the ingester runs itself (after the first fill, hourly while Elasticsearch is in use), so no manual step or sentence can be forgotten.
- While Postgres is not current, a failed Elasticsearch write fails and replays the event as today, so Elasticsearch never leaves "current" while it is the only backend.

Cost: turning Elasticsearch off waits for a passing comparison (a failing one keeps paying for Elasticsearch and opens an issue); before Postgres is current, an Elasticsearch outage still stalls ingestion, as today.

## Findings

| # | Finding | Severity | Decision | Reason / what changed |
|---|---|---|---|---|
| R5-S1 | Step 4 can leave neither backend current | should fix (protected, repeat) | fixed | Spec 5.2: use never ends while Postgres is not current; spec 7 step 2: the thread checks `pgCurrent: true` in health before steps 3 and 4. Packet part 4 rule says so. |
| R5-S2 | Drain on stale keep-awake after an ingester outage | should fix (repeat) | fixed | Spec 6.4: the drain counts only keep-awake missing for 30 minutes since the ingester started. |
| R5-S3 | Production comparison mechanism unspecified | should fix (protected) | fixed | Spec 5.2: the ingester runs it after the first fill and hourly while Elasticsearch is in use, records it in `search_control`; no sentence or command. Spec 7 step 2 rewritten. |
| R5-N1 | Triage R4-N2 claim wrong | nit | fixed | Correction: the weekly check flags only Elasticsearch awake while not in use; it does not detect an unintended "in use". An unintended `SEARCH_ES` deletion is visible in health (`esInUse`) and on Railway's usage, not alerted. Left as is: deleting the variable is a Railway change that needs Daniel's sentence. |
| R5-N2 | PR 1 fail-and-replay vs "current" | nit | fixed | Spec 5.1 now states it: while Postgres is not current, failed writes replay, Elasticsearch lags by the replay time as today and stays current. The stale "no repair pass exists (PR 1)" wording, contradicted by round 4's move of the repair into PR 1, is gone. |
| R5-N3 | R4-N6 deferral | nit | deferred | Reviewer accepts. |

Rejected: none.

## Length

Spec draft v6: 3,979 words against 3,924 (+55, 1.4%): the two guards and the self-run comparison are the step back's rule text; 5.1 and 7 step 2 shrank. Packet default reading: 2,185 words against 2,152 (+33), which is the review card line for this round; the part 4 clause is folded.
