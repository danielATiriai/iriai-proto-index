# REVIEW-v5: Event search without Elasticsearch until scale

Reviewer: independent adversarial reviewer (fallback for Codex Astra)
Round: 5. Packet v5, build 924e716, spec draft v5.
Files read: BRIEF.md, CODEX-TRIAGE-v4.md, packet.src.html, spec.md, facts.md, CHECKS.txt, eng-directions txts, code/ and spike files (unchanged from v4), with spot checks against /home/user/iriai-proto-index/ at d9ab983.

STAKES: AGREE

## Step back

Two of the three should-fix findings below (R5-S1 and R5-S2) are new cases of problems an earlier fix addressed:

- R5-S1 is another case of round-1 B1, where users get no search when a backend that is not ready is the only one left.
- R5-S2 is another case of R2-B1, a spurious drain of a serving Elasticsearch.

Under the brief this triggers a step back. Both come from the same gap: the one rule in 5.2 says who may serve, but nothing stops an operator step or controller action that leaves no backend current.

A suggested single invariant for the step back: **"No action may end Elasticsearch use, or drain it, unless Postgres is current. No drain may act on liveness evidence older than the ingester's own uptime."** This replaces patches at each call site.

## The rule, path by path

| Path | Who serves under 5.2 | Result |
|---|---|---|
| Deploy (PR 1), Postgres filling | ES (current); PG not current | OK |
| Step 2 passes and is recorded | ES chosen; PG current as fallback | OK |
| Step 2 never recorded, then steps 3 and 4 | Step 3: FORCE=postgres has no effect (PG not current). Step 4: es_current cleared. | **Neither current: ServiceUnavailable for every new session (R5-S1)** |
| Controller `on`, then drain | ES while current; PG fallback | OK, except after an ingester outage (R5-S2) |
| FORCE=elastic while ES dirty | Dirty means not current, so PG serves; 10-minute repair | OK (R4-S2 fixed) |
| Rollback (undo) | "until then Postgres serves" | OK if PG current; same hole as R5-S1 if not |
| PR 1 fail-and-replay stall | ES counted current while it misses a write | Same as today; not stated (R5-N2) |
| Ingester outage over 30 minutes | First tick may see a stale `es_seen_at` | **Spurious drain and 30-day lockout (R5-S2)** |

## Round-4 fixes checked

| Finding | Status | Notes |
|---|---|---|
| R4-S1: Postgres readiness not in the rule | Fixed | 5.2 "current once first fill and a recorded passing production comparison". This opens R5-S1 and R5-S3. |
| R4-S2: a dirty ES could serve; no repair on rollback | Fixed | Dirty is not current. Repair every 10 minutes while dirty, in any state. |
| R4-N1, N3, N4, N5 | Fixed | |
| R4-N2: an unexpected "in use" goes unseen | Triage says fixed in part; the claim is wrong | See R5-N1 |
| R4-N6 | Deferred | Acceptable for a nit |
| Length | +78 words in the spec, +23 in the packet; explained in the triage | Acceptable |

## Blockers

None.

## Should fix

### R5-S1: Step 4 can take search down when the comparison was never recorded (protected: production behavior; another case of round-1 B1)

- **Where:** spec 5.2 (Postgres current), spec 7 steps 2 to 4, packet Part 8 and the sentence list.
- **What is wrong:** Postgres becomes current only when a passing production comparison is recorded. Nothing makes steps 3 and 4 depend on that record. Say the comparison found a difference, or the record write failed. Step 3 (`SEARCH_BACKEND_FORCE=postgres`) then has no effect, because Postgres is not current and Elasticsearch keeps serving, and nothing reports the problem. Step 4 (`SEARCH_ES=off`) clears `es_current`. Neither backend is current, so every new session answers ServiceUnavailable.
- **What realistically happens, and to whom:** s3 asks Daniel to type all his sentences in one reply, so steps 3 and 4 are likely to run back to back without anyone looking at health in between. Every query.iriai.app user loses search until someone turns Elasticsearch back on. Under D3 A it must also wake from sleep first.
- **Fix:** the ingester refuses to end Elasticsearch use while `pgCurrent` is false. It logs, opens an issue and shows the refusal in health. Also, the thread checks `pgCurrent: true` in health before step 4, and the packet states this as a precondition.
- **How to confirm:** in a test, leave the comparison unrecorded, set FORCE=postgres and then SEARCH_ES=off, and call queryEvents. Today's text gives ServiceUnavailable.

### R5-S2: The drain lost R2-B1's qualifier, so an ingester outage can drain a healthy Elasticsearch for 30 days (another case of R2-B1)

- **Where:** spec 6.4, the `on` bullet.
- **What is wrong:** the drain fires when "`es_seen_at` is over 30 minutes old (compared in SQL)". The v4 qualifier "while keep-awake answered" is gone. Suppose the ingester (which runs both the ping and the controller) is down for more than 30 minutes, for example during a deploy failure or a Railway incident. On restart, the first controller tick can run before the first ping and see a stale `es_seen_at`. It then drains and turns Elasticsearch off, and the 30-day limit on switch-ons blocks turning it back on.
- **A second route, under D3 A:** during the same outage nothing keeps Elasticsearch awake, so a chosen Elasticsearch sleeps in quiet hours. Its first pages time out, fall back to Postgres, and count toward the "more than 20 fallbacks in 10 minutes" drain.
- **What realistically happens, and to whom:** at scale (the only time the controller is `on`), users move to the Postgres path the trigger already judged too slow, for up to 30 days. Nobody hears about it except through the drain log.
- **Fix:** send a ping and wait for its answer before the first controller tick, or ignore `es_seen_at` older than the ingester's start time. Do not count fallbacks while the cluster is waking.
- **How to confirm:** in a test with the controller `on`, set `es_seen_at` to now minus 40 minutes, restart the ingester and watch the first tick.

### R5-S3: How the production comparison is run and recorded is unspecified, and it may need a sentence (protected: s3 and approval rules)

- **Where:** spec 7 step 2, spec 5.2, packet Part 10 and the sentence list.
- **What is wrong:** "a pass is recorded and makes Postgres current" is the gate for the whole cutover, but the spec does not say who runs the comparison, from where, or how the record is written. Postgres has no TCP proxy (facts.md), so a thread cannot reach it from outside. It must exec into a Railway service or trigger a built-in command that writes `pgCurrent` to the production database. The packet does not say whether either needs Daniel's s3 sentence, and the one sentence list does not include it.
- **What realistically happens, and to whom:** either Daniel is asked for a second sentence mid-rollout, which breaks s3's "types once", or a thread writes the production gate without a sentence. The second is an approval-rule breach.
- **Fix:** name the mechanism. A built-in ingester job that runs on deploy and writes its own record needs no new Railway action. If it needs a sentence, add that sentence to the list.
- **How to confirm:** search spec 7 and the sentence list for how the record is written. Neither says.

## Nits

- **R5-N1 (a specific error in the record):** the triage for R4-N2 says an unexpected "in use" is seen within a week via `esInUse`. The weekly check in spec 6.3, which is unchanged, opens an issue only when memory is non-zero while Elasticsearch is *not* in use. A deleted `SEARCH_ES` (default on) means Elasticsearch is in use, so the check never flags it. Either correct the triage, or add "in use while `SEARCH_ES` is unset and the controller is off" to the weekly check.
- **R5-N2:** in PR 1, fail-and-replay does not raise `dirty_gen`. During a replay stall, Elasticsearch therefore counts as current while missing a write. This matches today's behavior and is acceptable, but spec 5.2 should say so in one clause so that readers do not assume "current" means complete in PR 1.
- **R5-N3:** R4-N6 is still deferred. That is fine; it should be listed under open items in the packet.

## Brief's questions

- **Does search.ts and service.ts behavior reproduce?** Yes, including getEvent, which has no completeness check.
- **Is the snapshot exact?** Yes. The advisory lock is taken before nextval on writes, and the shared lock before last_value on reads, so there is no gap.
- **Is dual-write safe on both the controller path and the rollback path?** Yes for data. Availability has the gaps in R5-S1 and R5-S2.
- **Are the spike numbers fairly reported?** Yes. They are unchanged from v4.

VERDICT: REQUEST CHANGES. There is no blocker, but if the comparison is never recorded, step 4 takes query.iriai.app search down, and an ingester outage can drain a healthy Elasticsearch for 30 days. Both repeat problems that earlier fixes addressed, so a step back is needed.
