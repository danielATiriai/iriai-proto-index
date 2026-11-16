# Triage, round 3: "Event search without Elasticsearch until scale"

Under review: packet v3, build 0ecfb29, spec draft v3 (2026-11-16). Code context iriai-proto-index main d9ab983.
Reviewer: the same independent Claude reviewer as rounds 1 and 2 (fallback; Codex Astra could not be started from this thread, which holds no message from Daniel).
Review: REVIEW-v3.md. Fixes land in packet v4 and spec draft v4.

## Stakes

Given: as in round 2, with the open unknown (whether a private-network request wakes a sleeping Serverless service) carried as a gate-2 check.
Reviewer: STAKES: AGREE.

## Step back (codex-review 6a)

No blocker was open, but two findings repeated problems earlier fixes addressed:
- Elasticsearch served while stale or asleep: round-1 B3 and S9, round-2 B1 and S2, now R3-S1 (the manual rollback never turned writes back on, had no keep-awake, and a forced read had no guard). Each fix patched one path (controller, PR 1, rollback) separately.
- Statements false under another option: round-1 S4, round-2 S6, now R3-S3.

What replaces the patches, for the first class: one rule that fails closed, spec 5.2, "Elasticsearch serves only while it is current". Whatever puts Elasticsearch in use (the `SEARCH_ES` setting or the controller) makes the ingester write to it and keep it awake, and build a fresh index before marking it current. The query service sends a new session to Elasticsearch only while it is marked current and has answered within 5 minutes; otherwise Postgres serves, whatever the state or the force setting says. This removes `SEARCH_ES_WRITES`, the rebuild command, the separate keep-awake paragraph and the separate drain condition. Cost of the rule: every switch to Elasticsearch, including a rollback, waits for a full fill (minutes today, longer at scale), and a 5-minute gap in its answers moves new sessions to Postgres.

For the second class: the packet now states mechanism details that differ by option only inside decision points or the spec; the step-7 clause that did not was removed rather than marked.

## Findings

| # | Finding | Severity | Decision | Reason / what changed |
|---|---|---|---|---|
| R3-S1 | Rollback serves a stale and, under D3 A, sleeping index; a forced read has no guard | should fix (repeat) | fixed | Step back above. Spec 5.2, 5.4, 6.3, 6.4, 7, 9; packet part 4 (one-rule paragraph), part 7 undo, part 8 Rollout row and all three rollback sentences ("Set SEARCH_ES=on on the ingester and SEARCH_BACKEND_FORCE=elastic …", plus "Redeploy" under B and C). |
| R3-S2 | Weekly sleep check depends on an undeclared routine | should fix | fixed | Spec 6.3: a weekly scheduled routine this thread creates at gate 2 (read-only Railway connector, opens an issue). Packet D3 A Risk row, part 4, part 9 and part 10 (dependency named) say so. |
| R3-S3 | Step 7 clause false under D2 B and hold; D2 B Risk row | should fix (repeat) | fixed | Clause removed from step 7 (the D2 decision point in part 4 carries the trigger rule). D2 B Risk row: "can get slow or fail with 'service unavailable' … and nothing reacts". |
| R3-S4 | Part 8 summary hides the Postgres acceptance; count wrong under B and C | should fix | fixed | Summary now quotes the acceptance sentence and names no count. |
| R3-S5 | Pack lacks renders | should fix | fixed | review-pack-v4/files holds every file under the build's files/. |
| R3-N1 | How the query service knows keep-awake is answered | nit | fixed | `search_control.es_seen_at` (spec 5.2), used by the rule and the drain condition. |
| R3-N2 | Reconcile delete inside the global write lock | nit | fixed | Spec 6.4: number and re-check under the lock, delete after commit with that number. |
| R3-N3 | Spec growth in round 2 unexplained | nit | fixed | Round 2's spec growth (16%) was the fixes asked for (keep-awake, fail-and-replay, generations, lock order, timeouts, gate-2 checks). This round see Length. |
| R3-N4 | First-fill gate holds the whole query service | nit | fixed | Spec 3: only `getEvent` answers 503 with Retry-After until the first fill; the first fill writes Postgres only. |
| R3-N5 | Spike run order | nit | fixed | Spec 4.5 states it; spike2-load.sql uses `\ir`. |
| R3-N6 | Part 1 under D3 hold | nit | fixed | A's text now reads "what happens when the trigger fires is D3", true under hold. |

Rejected: none.

## Length

Spec draft v4 is 3,846 words against 3,787 (+1.6%): the rule of 5.2 and its stated cost are new text the step back requires; it replaced longer passages in 5.2, 5.4, 6.3, 6.4 and 7. Packet default reading: 2,129 words against 2,082 (+47): the visible part 8 summary now quotes the acceptance sentence section 18 requires (R3-S4), and the D2 B Risk row names the failure (R3-S3); the one-rule paragraph is inside a fold.
