# Triage, round 4: "Event search without Elasticsearch until scale"

Under review: packet v4, build efa5063, spec draft v4 (2026-11-16). Code context iriai-proto-index main d9ab983.
Reviewer: the same independent Claude reviewer as rounds 1 to 3 (fallback; Codex Astra could not be started from this thread).
Review: REVIEW-v4.md. Fixes land in packet v5 and spec draft v5.

## Stakes

Given: as in round 3. Reviewer: STAKES: AGREE.

## Step back, again

Both should-fixes are further cases of "a backend serves while it is not current". Round 3's rule covered only Elasticsearch. It is now one rule for both backends (spec 5.2): **a backend serves only while it is current.** Postgres is current once its first fill and a passing production comparison are recorded. Elasticsearch is current while freshly filled and with no failed write awaiting repair; while in use, the ingester repairs it every 10 minutes whatever the controller state. A new session goes to the chosen backend if current, else the other if current, else `ServiceUnavailable`; a first page Elasticsearch fails falls back only to a current Postgres. `es_seen_at` no longer gates reads (it only feeds the drain condition), so an ingester outage does not move reads.

Cost, stated in spec 5.2: a switch to Elasticsearch waits for a full fill; a dirty Elasticsearch hands new sessions to Postgres for up to 10 minutes; before the production comparison an Elasticsearch failure answers `ServiceUnavailable`, as today.

## Findings

| # | Finding | Severity | Decision | Reason / what changed |
|---|---|---|---|---|
| R4-S1 | Postgres readiness not in the rule: empty pages at deploy, unverified Postgres serving before Daniel's sentence, ingester outage moving reads | should fix (repeat) | fixed | Step back above: Postgres is current only after first fill and a recorded comparison (spec 5.2, 7 step 2); `es_seen_at` no longer gates reads. Packet part 4 rule and part 8 "What reaches production users" say what happens before and after the comparison. |
| R4-S2 | "Current" ignores failed writes; no repair on the rollback path | should fix (repeat) | fixed | Spec 5.2: Elasticsearch is not current while `dirty_gen` > `repaired_gen`; the ingester repairs every 10 minutes while in use, in any controller state (5.2, 6.4). Spec 7 undo wording corrected. |
| R4-N1 | Gate 2 list and effort not updated | nit | fixed | Part 10: gate 2 4 to 5 days with the backend rule and its fill and repair passes; gate 3 2 to 4 days without the repair pass; total unchanged. |
| R4-N2 | Deleting `SEARCH_ES` re-enables Elasticsearch silently | nit | fixed in part | Health now reports `esInUse` and the weekly check reads it (R4-N3), so an unexpected "in use" is seen within a week. The default stays `on` so PR 1's deploy changes nothing for today's serving index; deleting a variable is itself a Railway change that needs Daniel's sentence. |
| R4-N3 | Weekly check cannot learn "in use" | nit | fixed | Spec 5.3: health reports `esInUse`, `esCurrent`, `pgCurrent`; spec 6.3's check reads it. |
| R4-N4 | `es_seen_at` compared on the query service's clock | nit | fixed | `es_seen_at` is used only in the drain condition, compared in SQL (spec 6.4). |
| R4-N5 | First-page fallback only in state `on` | nit | fixed | Spec 5.2: fallback is part of the rule, in every state. |
| R4-N6 | "In use" has two owners | nit | deferred | Intended: `SEARCH_ES` is the manual override (rollback), the controller the automatic owner; either puts it in use, and both are reported in health. The rule makes the overlap safe, since reads depend on "current", not on who set "in use". |

Rejected: none.

## Length

Spec draft v5: 3,924 words against 3,846 (+78, 2%). The added text is the rule's extension to Postgres, the repair cadence outside the controller, the health fields and the stated cost, which the step back requires; the 6.4 "on" bullet shrank. Packet default reading: 2,152 words against 2,129 (+23); the visible additions are one clause in part 8 (what happens before and after the comparison) and the gate-2 list.
