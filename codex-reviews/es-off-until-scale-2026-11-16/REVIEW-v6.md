# REVIEW-v6: Event search without Elasticsearch until scale

Reviewer: independent adversarial reviewer (fallback for Codex Astra)
Round: 6. Packet v6, build e2b051c, spec draft v6.
Files read: BRIEF.md, CODEX-TRIAGE-v5.md and the v5-to-v6 diffs of spec.md and packet.src.html, files/CHECKS.txt (build e2b051c, 2,185 words of default reading). code/, facts.md, eng-directions-s3.txt and spike2-results.txt are byte-identical to v5. Everything else was read in rounds 1 to 5 against the same code at d9ab983.

STAKES: AGREE

## Step back

R6-B1 is another case of the recurring class: a path where neither backend is current, so query.iriai.app answers ServiceUnavailable. That class covers round-1 B1, R2-B1, R3-S1, R4-S1 and R5-S1.

The new guards close the two routes raised in round 5. The new hourly comparison opens a third route: it can take Postgres out of serving exactly while Elasticsearch is not current. That triggers the step back a fourth time.

The triage's claim "So at least one backend is always current after the first fill" is false as written; see R6-B1.

Suggested shape for the step back: split one flag into two.

- **"May serve":** first fill done, and no failed Postgres write. Once set, it is never cleared by a comparison with Elasticsearch.
- **"Verified":** the last comparison passed. This gates only the end of Elasticsearch use, a drain, and the cutover steps.

A comparison that cannot run cleanly, because Elasticsearch is not current, is unreachable or is dirty, records "no result" and changes neither flag. The comparison then never becomes a way to remove the only working backend.

## The rule, path by path

| Path | Who serves under 5.2 (v6) | Result |
|---|---|---|
| Deploy (PR 1), first fill | ES current; PG not yet current | OK |
| First comparison after the fill (ES is today's index, current) | ES; then PG current | OK |
| Steps 3 and 4 with no pass | `SEARCH_ES=off` is ignored, and health says so | OK (R5-S1 fixed) |
| Controller `ready` to `starting` to `backfilling` (ES in use, `es_current` clear, index empty or partial) | PG serves until the hourly comparison runs against the partial index, fails, and clears `pgCurrent` | **Neither current (R6-B1)** |
| Undo: redeploy ES, `SEARCH_ES=on` (ES in use, rebuilding) | Same as above. The spec's "until then Postgres serves" holds only if no hourly tick lands during the rebuild. | **Neither current (R6-B1)** |
| `on`, then an ES write fails (ES dirty), then the hourly comparison runs before the 10-minute repair, or during an ES outage | ES not current (dirty or down); the comparison fails and clears `pgCurrent` | **Neither current; ingestion also falls back to fail-and-replay and stalls (R6-B1)** |
| `on`, healthy, comparison passes | ES serves; PG is the fallback | OK |
| Drain after an ingester outage | Only keep-awakes answered since the ingester started count | OK (R5-S2's main route fixed) |
| `SEARCH_BACKEND_FORCE` | Within the rule | OK, except as above |
| PR 1 fail-and-replay | Stated in 5.1 | OK (R5-N2 fixed) |

Does it fail closed? For data, yes. For availability, the failure mode is "neither serves" on paths that occur in normal operation, which is the outcome the rule exists to prevent.

## Round-5 fixes checked

| Finding | Status | Notes |
|---|---|---|
| R5-S1: step 4 leaves neither current | Fixed for that path | Use never ends while PG is not current; the thread checks health before steps 3 and 4. The new hourly re-check reopens the class elsewhere (R6-B1). |
| R5-S2: drain on stale keep-awake | Fixed in the main | The second route (fallbacks from a cluster that slept during the outage) is untouched; see R6-N1 |
| R5-S3: comparison mechanism | Fixed | Self-run, recorded in `search_control`, no sentence needed. It broke something: R6-B1. |
| R5-N1 | Fixed | The triage correction is accurate |
| R5-N2 | Fixed | 5.1 is clear, and the stale "no repair pass (PR 1)" text is gone |
| R5-N3 | Deferred | Accepted |
| Length | +55 words in the spec, +33 in the packet, explained | Acceptable |

## Blockers

### R6-B1: The hourly comparison can clear `pgCurrent` while Elasticsearch is not current, so search goes down (production behavior; another case of the recurring class)

- **Where:** spec 5.2, first bullet: "every hour while Elasticsearch is in use ... a failure clears `pgCurrent`". Also spec 6.4 (states and the verifying check), spec 7's undo, and spec 5.1.
- **What is wrong:** the comparison is the verifying check of 6.4. It requires equal id-and-version sets between Postgres and Elasticsearch, and equal first pages. It runs whenever Elasticsearch is in use, and that includes the states where Elasticsearch is in use but not current:
  1. **Controller switch-on.** In `starting` and `backfilling`, the index is absent or partial. `starting` alone may last 30 minutes, and backfill at about 500,000 events adds more, so an hourly tick in that window is likely.
  2. **Undo.** Elasticsearch is redeployed and rebuilding.
  3. **Dirty or down.** In `on`, after a failed write (up to 10 minutes before repair) or during an Elasticsearch outage, a failed or unreachable comparison counts as a failure, and nothing says otherwise.

  In each case the comparison fails because Elasticsearch is incomplete, but the spec blames Postgres: it clears `pgCurrent`. Elasticsearch is not current either, so 5.2 sends every new session to ServiceUnavailable.
- **Why it stays down:** the rule then forbids ending Elasticsearch use and ignores the drain. Under 5.1, Elasticsearch write failures go back to fail-and-replay, so in case 3 ingestion stalls as well. The only way back is a later comparison that passes. In case 3 that waits on Elasticsearch recovering and then the next hourly tick. In case 1, the controller's own verifying state may meanwhile fail and try to turn Elasticsearch off, which the rule now ignores.
- **What realistically happens, and to whom:** this does not need a contrived input. Case 1 occurs on the controller's normal job, the first time the trigger fires at scale. Case 2 occurs on the named rollback procedure. Every app calling query.iriai.app gets ServiceUnavailable for up to an hour or more. At scale, an Elasticsearch outage also stops ingestion: the very thing PR 2 was meant to stop.
- **Fix:** run the hourly comparison only while Elasticsearch is current and has been reachable for at least one keep-awake. Treat "could not compare" as no result. Do not let a comparison failure stop Postgres serving once it has served; let it block only ending use or draining, and open an issue. The step back above gives the two-flag version. Correct the triage's "always current" claim.
- **How to confirm:** in a test, set `pgCurrent` true and the controller to `backfilling` with an empty index, run the hourly comparison, then call queryEvents. As written, it fails, clears `pgCurrent`, and the call answers ServiceUnavailable. Repeat with `dirty_gen` above `repaired_gen` in `on`.

## Should fix

### R6-S1: The packet leaves out that Postgres can stop being current after it was compared (s16: a technical point the packet leaves out)

- **Where:** packet part 4, "One rule", and the "Rollout and rollback" row (around lines 491 and 713).
- **What is wrong:** the packet says Postgres is current "once filled and compared on production by the ingester itself". That reads as a one-time gate. The spec makes it a standing hourly check that can withdraw Postgres. It also says "a failing one keeps paying for Elasticsearch" (triage), and the Running cost row does not mention that exposure.
- **What realistically happens, and to whom:** Daniel approves without seeing the behavior behind R6-B1, or the open-ended Elasticsearch spend while comparisons keep failing. The spend is capped by the stop point at about $40 a month, but this case is not named.
- **Fix:** after the R6-B1 fix, one clause in part 4 and the cost row: what a failing comparison does, and that it keeps Elasticsearch on, capped at the stop point, with an issue opened.
- **How to confirm:** compare packet line 491 with spec 5.2, first bullet.

## Nits

- **R6-N1 (specific error in the R5-S2 record, which is marked "fixed"):** only the stale-`es_seen_at` route was changed. The second route in R5-S2, where Elasticsearch sleeps during an ingester outage under D3 A and its timeouts count toward "more than 20 first pages fall back within 10 minutes", is not addressed. At scale, query traffic probably keeps Elasticsearch awake, so this is a nit. Either do not count fallbacks within one keep-awake interval of an ingester start, or say why traffic keeps it awake.
- **R6-N2:** spec 5.2's cost line still says "before the step-2 comparison". Step 2 is no longer a manual comparison. Say "while Postgres is not current", and add the cost of a failing hourly comparison.
- **R6-N3:** when the ingester ignores `SEARCH_ES=off`, it only "says so in health". Opening an issue would match the other refusals, because Daniel will not be watching health after typing his sentences.

## Brief's questions

- **Does the Postgres design reproduce search.ts and service.ts (filters, sorts, ties, nulls, antimeridian, paging, suppression, errors)?** Yes. Nothing changed since v5.
- **Is the snapshot in spec 4.3 exact?** Yes. Nothing changed.
- **Is the backfill and dual-write safe against lost or resurrected documents?** Yes for data. 5.1's mode switch, keyed on `pgCurrent`, is correct, but R6-B1 can flip it back during an outage.
- **Are the spike numbers fairly reported?** Yes. Nothing changed.
- **Is the stated cost of the rule right?** No. It omits that a comparison failure removes Postgres from serving (R6-B1, R6-N2).

VERDICT: REQUEST CHANGES. The new hourly comparison clears `pgCurrent` whenever it runs against an incomplete, dirty or unreachable Elasticsearch, so a normal controller switch-on, the named undo, or an Elasticsearch outage at scale leaves neither backend current and takes query.iriai.app search down, a fourth case of the class that triggers the step back.
