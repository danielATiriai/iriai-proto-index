# Triage, round 7: "Event search without Elasticsearch until scale"

Under review: packet v7, build 183ae1d, spec draft v7 (2026-11-16). Code context iriai-proto-index main d9ab983.
Reviewer: the same independent Claude reviewer as rounds 1 to 6 (fallback; Codex Astra could not be started from this thread, which holds no message from Daniel).
Review: REVIEW-v7.md. Verdict: APPROVE.

## Stakes

Given: as in round 6. Reviewer: STAKES: AGREE. Step back: not triggered (no repeat, no open blocker or protected finding).

## Findings

| # | Finding | Severity | Decision | Reason |
|---|---|---|---|---|
| R7-N1 | Spec 6's `search_control` column list omits `pg_verified`, `es_current`, `es_seen_at` | nit | deferred | Spec text completeness; the columns are defined in 5.2. Added to the spec in PR 1's spec update, so the reviewed build stays the one Daniel sees. |
| R7-N2 | The latch means later Postgres search changes are never compared in production; CI is the only guard | nit | deferred | True and intended (spec 7 step 2 already names CI as the real guard). The cost line gains this sentence in PR 1's spec update. |
| R7-N3 | Before the latch, an Elasticsearch outage opens an issue every hour | nit | deferred | Only possible in the hours between the PR 1 deploy and the first passing comparison; PR 1 opens one issue per failure streak instead of per run. |

Rejected: none.

## Summary across rounds

Seven rounds, v1 to v7. Blockers: 3 (round 1), 1 (round 2), 1 (round 6), all fixed. Should-fix: 12, 6, 5, 2, 3, 1, 0, all fixed. Nits: 8, 6, 6, 6, 3, 3, 3; all fixed except R4-N6, R5-N3 (same item) and R7-N1 to N3, deferred. The step back ran in rounds 3 to 6 and replaced the readiness patches with one rule (spec 5.2).
