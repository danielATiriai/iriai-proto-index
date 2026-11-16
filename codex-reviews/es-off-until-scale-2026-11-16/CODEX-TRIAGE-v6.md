# Triage, round 6: "Event search without Elasticsearch until scale"

Under review: packet v6, build e2b051c, spec draft v6 (2026-11-16). Code context iriai-proto-index main d9ab983.
Reviewer: the same independent Claude reviewer as rounds 1 to 5 (fallback; Codex Astra could not be started from this thread).
Review: REVIEW-v6.md. Fixes land in packet v7 and spec draft v7.

## Stakes

Given: as in round 5. Reviewer: STAKES: AGREE.

## Step back, fourth time

What recurs: a readiness signal that can be cleared by a check of the *other* backend (R6-B1: an hourly comparison against an empty, filling or failing Elasticsearch cleared Postgres's flag). Round 5's claim that one backend is always current after the first fill was false; that record is corrected here.

Replacement, simpler than the flag it replaces, following the reviewer's suggested split:
- Postgres readiness is a latch, `pgVerified`: set by the first passing comparison after the first fill, cleared by nothing. It can be a latch because every write reaches Postgres first (5.1), so Postgres cannot fall behind.
- Later comparisons never change what may serve; they only decide whether the controller switches to Elasticsearch.
- Before the latch, Elasticsearch is the only backend, exactly as today (fail-and-replay, `ServiceUnavailable` on failure), and its use cannot end.
So after verification there is no state in which neither backend may serve.

Cost: until the first comparison passes, Elasticsearch stays on at today's cost (now stated in the Running cost row); a real later divergence in Postgres is caught only by the verifying step and the issue it opens, not by taking Postgres out of serving.

## Findings

| # | Finding | Severity | Decision | Reason / what changed |
|---|---|---|---|---|
| R6-B1 | Hourly comparison clears Postgres readiness when Elasticsearch is the incomplete side; neither backend serves | blocker (repeat) | fixed | Step back above. Spec 5.2 rewritten around the `pgVerified` latch; 5.1, 5.3 and 7 step 2 renamed to it. |
| R6-S1 | Packet presents the comparison as one-time but it was not; cost row silent on failing comparisons | should fix | fixed | It is now one-time by design. Part 4 rule paragraph says so; the Running cost row says Elasticsearch stays on at $13 with an issue if the first check never passes. |
| R6-N1 | Fallbacks during an ingester outage count toward a drain | nit | fixed | Spec 6.4: fallbacks count only while `es_seen_at` is under 5 minutes old. |
| R6-N2 | 5.2 cost line names "the step-2 comparison" | nit | fixed | Rewritten. |
| R6-N3 | Ignored `SEARCH_ES=off` only in health | nit | fixed | Spec 5.2: it also opens an issue. |

Rejected: none.

## Length

Spec draft v7: 4,032 words against 3,979 (+53): the latch and the sentence saying what later comparisons may not do are the rule. Packet: the review card was condensed to one summary of all rounds, offsetting the part 4 and cost-row clauses: default reading 2,104 words against 2,185 (-81).
