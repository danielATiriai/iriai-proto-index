# REVIEW-v7: Event search without Elasticsearch until scale

Reviewer: independent adversarial reviewer (fallback for Codex Astra)
Round: 7. Packet v7, build 183ae1d, spec draft v7.

Files read:
- BRIEF.md (only the round numbers changed) and CODEX-TRIAGE-v6.md.
- The v6-to-v7 diffs of spec.md and packet.src.html.
- files/CHECKS.txt: build 183ae1d, 2,104 words of default reading.
- Spec 3 (cursors, getEvent), 5.4 and 6 re-read for paths that touch the latch.

code/, facts.md, eng-directions-s3.txt and spike2-results.txt are byte-identical to v6. Everything else was read in rounds 1 to 6 against the same code at d9ab983.

STAKES: AGREE

## Step back

No finding this round is another case of a problem an earlier fix addressed. No blocker or protected finding is open.

The latch closes the recurring class, "neither backend may serve" (round-1 B1, R2-B1, R3-S1, R4-S1, R5-S1, R5-S2, R6-B1), structurally rather than by patching paths:

- After verification, the routing rule in 5.2 always has Postgres as the last resort, and nothing can clear it.
- Before verification, Elasticsearch is the only backend. It runs in fail-and-replay mode and cannot leave use, which matches today.

The triage's statement "after verification there is no state in which neither backend may serve" is true as written.

## The rule, path by path

| Path | Who may serve (v7) | Result |
|---|---|---|
| Deploy (PR 1), first fill, before the latch | ES, as today: fail-and-replay keeps it current; `SEARCH_ES=off` is ignored and an issue opened | OK |
| First comparison passes | Latch set; ES chosen (FORCE=elastic), PG as fallback | OK |
| Comparison never passes | ES stays on and alone at $13; an issue opens each hour | OK; the cost is stated in part 8 and spec 5.2 |
| Steps 3 and 4 | Gated on `pgVerified: true` in health and by the ingester | OK |
| Controller `starting` and `backfilling` (ES not current) | PG (latched) | OK (R6-B1 fixed) |
| Controller verifying fails | Back to `off`; PG serves; failure counted | OK |
| `on`, ES dirty, down or at its memory limit | PG for first pages; later ES pages answer ServiceUnavailable or CursorExpired, as stated | OK |
| Drain after an ingester outage | Stale keep-awake ignored before start; fallbacks count only while `es_seen_at` is under 5 minutes old | OK (R5-S2 and R6-N1 fixed) |
| Undo (ES redeployed, rebuilding) | PG (latched) until `es_current` is set | OK; "until then Postgres serves" is now true |
| `SEARCH_BACKEND_FORCE=elastic` while ES may not serve | PG | OK |
| Ingester outage | PG serves; the query service needs nothing from the ingester | OK |

Does it fail closed? Yes on every path. The one place the rule is not failure-aware is a latched Postgres that later turns out wrong. The triage states that cost openly (see R7-N2).

## Round-6 fixes checked

| Finding | Status | Notes |
|---|---|---|
| R6-B1: hourly comparison clears PG readiness | Fixed | Readiness is now a one-time latch. Later comparisons only gate the controller. The basis, "every write reaches Postgres first (5.1)", holds: a failed Postgres write fails the event and replays it, so Postgres can stall but cannot silently skip a write. Spec 5.4 has no Postgres rebuild that could empty a latched table. |
| R6-S1: packet's one-time framing; cost row | Fixed | Part 4 now matches spec 5.2. The Running cost row names the case where the first check never passes. |
| R6-N1: fallbacks after an ingester outage | Fixed | Overload while the cluster still answers health checks still counts, which is intended. |
| R6-N2: stale cost line | Fixed | |
| R6-N3: ignored `SEARCH_ES=off` only shown in health | Fixed | An issue now opens. |
| Length | Spec +53 words, explained; packet -81 words | Acceptable. The review card's totals (2 more blockers and 17 should-fixes in rounds 2 to 6) match REVIEW-v2 to REVIEW-v6. |

## Blockers

None.

## Should fix

None.

## Nits

- **R7-N1 (spec 6, line 144):** the column list for `search_control` was not updated. It still names `first_fill_done` but not the latch (`pg_verified`), `es_current` or `es_seen_at`, which 3, 5.2 and 6.4 rely on. An implementer reading 6 alone would miss them. Add the three names, or say they live elsewhere.
- **R7-N2 (spec 5.2, cost line):** the latch never resets, so a later PR that changes the Postgres projector or query code is never compared against Elasticsearch in production. Below the trigger nothing exists to compare against anyway. The double-run CI tests (10) are the only guard. One clause, "the latch is not re-earned after later code changes; CI is the guard", would make the cost complete. This is not a defect in the rule.
- **R7-N3 (spec 5.2):** before the latch, an Elasticsearch outage makes the hourly comparison fail and open an issue every hour, on top of the outage itself. Consider treating "could not reach Elasticsearch" as no result, as suggested in round 6, so the issues track real disagreements only.

## Brief's questions

- **Does the Postgres design reproduce search.ts and service.ts?** Yes. Unchanged since v5.
- **Is the snapshot in spec 4.3 exact?** Yes. Unchanged.
- **Are the backfill and dual-write safe against lost or resurrected documents?** Yes. The 5.1 mode switch is now keyed on a latch that cannot flip back, which removes R6-B1's flip into stall mode.
- **Are the spike numbers fairly reported?** Yes. Unchanged.
- **Is the stated cost of the rule right?** Yes. It is complete apart from R7-N2.

VERDICT: APPROVE. The `pgVerified` latch closes the recurring "neither backend may serve" class on every path (deploy, controller, force, undo, PR 1, ingester outage), it fails closed, and its stated cost is accurate; what remains is three nits.
