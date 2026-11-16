# Adversarial review, round 4: "Event search without Elasticsearch until scale" v4 (build efa5063, spec draft v4)

Reviewer: independent Claude reviewer (fallback for Codex Astra), Nov 16 2026. Read:
- BRIEF.md, facts.md (unchanged), REVIEW-v1 to v3 and CODEX-TRIAGE-v1 to v3.
- packet.src.html, diffed against v3.
- spec.md, diffed against v3, and spec.html.
- eng-directions s16, s18 and s3; code/* (unchanged since round 1); the spike files (unchanged).
- From files/: CHECKS.txt, PACKET-TESTS.txt, packet-390-first-screen.png; the pack now holds every file the build wrote.

STAKES: AGREE. The lines match the files. The private-network wake unknown is carried in the stakes with a gate-2 fallback. None of the findings below turns on an unknown.

Step-back triggers (BRIEF): no blocker is open. Two findings are new cases of the class the round-3 step back meant to close, "a backend serves while it is not current":
- **R4-S1.** The rule fails closed onto Postgres, but at the PR 1 deploy Postgres may not yet be filled. The round-3 fix R3-N4 removed the only gate that stopped this. This is the same class on the fallback side.
- **R4-S2.** The rule never treats a dirty Elasticsearch as not current. On the rollback path, nothing is specified to repair it.

The rule itself is the right shape. Both gaps sit at its edges and close with small changes to the same rule, not with more patches. Length: spec +1.6% and packet +47 words, both explained in CODEX-TRIAGE-v3 "Length". No length finding.

## The one rule (spec 5.2), checked path by path

| Path that can make Elasticsearch serve | Holds? | Notes |
|---|---|---|
| Controller, `starting` to `draining` | Yes | The ingester writes, pings every 2 min, and sets `es_current` only after backfill and reconcile. Query service: `es_current` and `es_seen_at` < 5 min. |
| `SEARCH_BACKEND_FORCE=elastic` | Yes for the stale-or-asleep case | The force is bounded by the rule. A dirty index still serves (R4-S2). |
| Rollback (`SEARCH_ES=on` plus force) | Partly | Fresh fill and keep-awake: yes. Repair after a failed write while the controller state is `off` is unspecified (R4-S2). |
| PR 1 (migration marks today's index current) | No, at deploy | `es_seen_at` is empty until the ingester's first ping, so new sessions go to a Postgres that may not be filled yet (R4-S1). |
| Fails closed? | Toward Postgres, always | Postgres's own readiness is not part of the rule (R4-S1). |
| Stated cost | Incomplete | It leaves out that an ingester outage, not only an Elasticsearch one, moves reads, and that before step 3 this happens without Daniel's sentence (R4-S1). |

## Round-3 fixes, checked

| # | Verdict | Notes |
|---|---|---|
| R3-S1 rollback | Right | Replaced by the rule. All three rollback sentences now set `SEARCH_ES=on` with the force. Edge cases in R4-S2. |
| R3-S2 weekly check | Right | This thread creates it at gate 2, and it is named in part 10. How the routine learns "not in use" is unstated (R4-N3). |
| R3-S3 step-7 clause; D2 B risk | Right | The clause is removed and the D2 B Risk row names the failures. |
| R3-S4 part 8 summary | Right | It quotes the acceptance sentence and gives no count. |
| R3-S5 pack completeness | Right | All renders and diagrams are present. |
| R3-N1 `es_seen_at` | Right | It compares an ingester-written time against the query service's clock (R4-N4). |
| R3-N2 delete after commit | Right | A number taken and checked under the lock and then deleted after commit is still correct. A later put gets a higher number and wins in either order. |
| R3-N3, N5, N6 | Right | |
| R3-N4 gate only `getEvent` | Right as asked, but it breaks something | Combined with the rule, queryEvents can now read a Postgres that is not yet filled (R4-S1). |

## Blockers

None.

## Should fix

### R4-S1. The rule fails closed onto a Postgres that may not be filled, and moves production reads without Daniel's sentence (another case of the class; step back)
- **Where:** spec 5.2 (the query service uses Postgres "whatever the state or `SEARCH_BACKEND_FORCE` says" when `es_seen_at` is over 5 minutes old); spec 3 ("Until the first fill … `getEvent` answers 503 …; nothing else waits"); spec 5.4; spec 7 steps 1 to 3. Packet: part 4 "One rule", part 8 "What reaches production users and when" ("Reads switch after PR 1 and your sentence"), and the rule's stated cost.
- **What is wrong:**
  - At the PR 1 deploy, both services start from the same image. The migration marks today's index current, but `es_seen_at` stays empty until the ingester has started and answered its first ping.
  - Until then, every new session goes to Postgres. Under the R3-N4 fix, queryEvents does not wait for the first fill, so `event_docs` may be empty or partial. Apps get empty or short pages, not 503, which contradicts the README's "never an empty page when the index is down".
  - Afterwards, until Daniel's step-3 sentence, any ingester outage over 5 minutes moves production reads to Postgres. That includes a redeploy, a crash, or the fail-and-replay backoff if the ping shares the event loop. Postgres is then complete but not yet compared (step 2 is meant to come before step 3). Part 8 tells Daniel reads switch only on his sentence.
  - The stated cost ("a 5-minute gap in its answers") describes Elasticsearch going quiet. In practice the signal is the ingester's ping, so an ingester outage is what moves reads, at scale too.
- **What happens and to whom:** for the seconds to minutes of the first fill at PR 1, apps calling query.iriai.app get missing results. Before step 3, reads can move to an unverified backend with no sentence from Daniel.
- **How to confirm:** read spec 3 ("nothing else waits") together with spec 5.2 (`es_seen_at` < 5 min, otherwise Postgres) and with the start-up order in spec 5.4. Nothing sets `es_seen_at` before the ingester's first ping.
- **Fix:**
  - Make Postgres's readiness part of the same rule: "Postgres serves new sessions only once `first_fill_done` is set". Before that, a new session answers 503 with Retry-After, or uses Elasticsearch if it is current.
  - Have the PR 1 migration also set `es_seen_at` to now.
  - Before step 3, keep forced Elasticsearch reads on Elasticsearch through short ping gaps, or say in part 8 that an ingester outage can move reads before the sentence.
  - Restate the rule's cost as "an ingester or Elasticsearch outage over 5 minutes moves new sessions to Postgres".

### R4-S2. "Current" ignores failed writes, so a dirty Elasticsearch still serves, and on the rollback path nothing repairs it (another case of the class; step back)
- **Where:** spec 5.2 ("serves only while it is current"), spec 5.1 (from PR 2 on, a failed write raises `dirty_gen` and moves on), spec 6.4 Reconcile ("at every tick while `dirty_gen` > `repaired_gen`, and once a day while `on`"), spec 7 undo ("a stale or sleeping Elasticsearch is never served"). Packet part 4 "One rule", part 8 Rollout row.
- **What is wrong:**
  - From PR 2 on, a failed Elasticsearch write leaves the index behind Postgres. `es_current` stays set and the pings stay fresh, so the query service keeps serving it until a reconcile runs. Under the controller that is at most one tick, which is the accepted design. But spec 7's "never served stale" is then false.
  - After PR 2, a rollback (`SEARCH_ES=on` plus force) runs with the controller state still `off`. Spec 6.4 lists the reconcile among the controller's states: the daily pass runs only "while `on`", and nothing says the per-tick dirty pass runs when use comes from `SEARCH_ES` rather than the state.
  - On that path a failed write can stay unrepaired indefinitely while Elasticsearch serves every new session.
- **What happens and to whom:** on the rollback path after PR 2, apps can get a missing or stale event for an unbounded time, which is what the rule was meant to make impossible.
- **How to confirm:** read 5.2, 6.4 Reconcile and 7 undo together, and trace `SEARCH_ES=on` with state `off` and one failed write.
- **Fix (pick one and state it):**
  - Run the reconcile whenever Elasticsearch is in use, by either route (each tick while dirty, plus daily).
  - Or add "and `dirty_gen` = `repaired_gen`, or the dirty generation is under one tick old" to the query service's serve condition.
  - Either way, soften "never served stale" to "stale by at most one repair tick".

## Nits
- **R4-N1.** Part 10 gate 2 lists PR 1 as "interface, Postgres backend, Elasticsearch optional, writes to both, cursor field, read switch, double-run tests", with effort unchanged at 3 to 4 days. Under the rule, PR 1 must now also carry the backfill, the reconcile, the 2-minute ping, `es_current` and `es_seen_at`. The rollback path needs them before PR 2, and the rebuild command that held them is gone. Gate 3 still lists the "repair pass" for PR 2. Move the items and re-estimate.
- **R4-N2.** `SEARCH_ES` defaults to `on`. After step 4 it is explicitly `off`. If the variable is ever deleted rather than set (step 5 itself says "clear" for the force variable), Elasticsearch silently goes back into use: writes, a 2-minute ping, a fresh fill, and under D3 A it never sleeps, at about $13 to $20 a month. The weekly check opens an issue only when Elasticsearch is awake "while … not in use", so it stays silent. After PR 2, default to `off`, or have the controller own use.
- **R4-N3.** Spec 6.3: the weekly routine "opens an issue when it is non-zero while Elasticsearch is not in use". Postgres has no outside access (facts.md: no TCP proxy), so the routine can learn "in use" only from the public `/xrpc/_health`. Say that 5.3 reports in-use, `es_current` and `es_seen_at` there, and that the routine reads it.
- **R4-N4.** The query service compares `es_seen_at`, written with the ingester's or Postgres's clock, against its own clock. Compare in SQL (`now() - es_seen_at < interval '5 minutes'`) so skew between services cannot flip the rule.
- **R4-N5.** The first-page fallback to Postgres within 2 s is specified only for state `on` (6.4). On the rollback path the state is `off` and the force is set, so make the fallback part of the 5.2 rule. Otherwise a first page that Elasticsearch fails answers 503 there.
- **R4-N6.** Spec 5.2 "In use" has two owners, `SEARCH_ES` and the controller state. Say explicitly that use is their OR and ends only when both are off. Otherwise draining under D3 B while `SEARCH_ES=on` would remove a deployment the ingester still pings. The rule then fails closed correctly, but the controller's "confirm turned off" and the issue text would be misleading.

## Answers to the brief's specific questions
- **Does the Postgres design reproduce search.ts and service.ts?** Yes, unchanged from round 3. The remaining error difference is the stated 2 s timeout, which answers 503.
- **Is the snapshot in 4.3 exact?** Yes.
- **Are the backfill and dual-write safe against lost or resurrected documents?** On the controller path, yes. On the rollback path after PR 2, not specified (R4-S2).
- **Are the spike numbers fairly reported?** Yes. The run order is now stated.

VERDICT: REQUEST CHANGES. The one rule is the right fix and closes the stale-or-asleep Elasticsearch paths, but it does not yet cover Postgres's own readiness at the PR 1 deploy or a dirty Elasticsearch on the rollback path. Both are further cases of the class the step back targeted and need the rule extended, not patched.
