# Adversarial review, round 3: "Event search without Elasticsearch until scale" v3 (build 0ecfb29, spec draft v3)

Reviewer: independent Claude reviewer (fallback for Codex Astra), Nov 16 2026.

Read:
- BRIEF.md, facts.md
- REVIEW-v1/v2 and CODEX-TRIAGE-v1/v2
- packet.src.html, diffed line by line against v2
- spec.md, diffed against v2, and spec.html
- eng-directions s16/s18/s3
- code/* (byte-identical to rounds 1 and 2)
- spike-schema.sql, spike-load.sql, spike2-*.sql and spike2-results.txt
- files/ (CHECKS.txt, MODEL.json, option renders, packet-390.png)

STAKES: AGREE. The lines match the files. Unknown (a), whether a request over Railway's private network wakes a sleeping service, is carried in the stakes, with B or C as the gate-2 fallback; a failed check costs a re-decision, not production harm. Unknown (b), traffic gaps, is designed away by the keep-awake request every 2 minutes (spec 6.3). Whether the "cost-monitoring routine" named in spec 6.3 exists is open (R3-S2), but it does not change any severity here: the worst case is about $13 to $20 a month of unnoticed Elasticsearch spend, a should fix either way.

Step-back triggers (BRIEF): no blocker is open. Two findings are new cases of problems an earlier fix addressed:
- R3-S1 is another stale or unserviceable rollback (round-1 S9), and its D3 A half is another serving Elasticsearch left without keep-awake (R2-B1).
- R3-S3 is another statement made false by a non-recommended option (round-1 S4 and R2-S6).
Both trigger the step back. Length: the packet's default reading fell from 2,490 to 2,082 words, but spec.md grew from 3,255 to 3,787 words (+16%), and CODEX-TRIAGE-v2 "Length" explains only the packet (R3-N3).

## Round-2 fixes, checked

| # | Verdict | Notes |
|---|---|---|
| R2-B1 serving Elasticsearch sleeps | Right | Spec 6.3: cluster-health request every 2 min on its own timer, from `starting` to the end of `draining`. Replies are outbound packets, which is what Railway's sleep rule counts (facts.md). Spec 6.4: fallbacks count toward a drain only while keep-awake is answered. The keep-awake runs in the ingester and the counter in the query service; how the query service learns "keep-awake is answered" is unstated (R3-N1). |
| R2-S1 cannot confirm off | Right in spec; depends on an undeclared routine | See R3-S2. |
| R2-S2 PR 1 swallowed writes; `SEARCH_ES_WRITES` default | Right | Fail-and-replay while Elasticsearch serves new sessions in PR 1; default `on`. The rollback path now breaks the same promise (R3-S1). |
| R2-S3 reconcile races | Right | Generations close the dirty race. The delete re-checks under the write lock with a fresh number, so it cannot delete a document re-put meanwhile. The Elasticsearch delete now runs inside the Postgres transaction that holds the global write lock (R3-N2). |
| R2-S4 `nextval` order, `is_called` | Right | Spec 4.2 takes the lock first; 4.3 sets W to 0 when `is_called` is false. Snapshot exactness holds as argued. |
| R2-S5 timeouts | Right in spec; the packet text is false under D2 B | See R3-S3. |
| R2-S6 option-false statements | Right | Part 1 is now a D3 decision point; the callout covers D1 hold. |
| R2-N1 to N6 | Right | N3 introduces a new gate (R3-N4). |

## Blockers

None.

## Should fix

### R3-S1. The rollback before PR 2 serves a stale index, and under D3 A a sleeping one (repeat of round-1 S9 and R2-B1: step back)
- Where: spec 7 "Undo before PR 2", packet part 7 "Migration order, undo", part 8 Rollout row, and the "Only to roll back" sentences (packet lines 725 to 727).
- What is wrong:
  - Step 4 sets `SEARCH_ES_WRITES=off` on the ingester. The undo is "turn Elasticsearch on, run `npm run search:rebuild-es`, then set `SEARCH_BACKEND_FORCE=elastic`". No step, and no rollback sentence, sets `SEARCH_ES_WRITES=on` again.
  - After the rollback, Elasticsearch serves every new session from the rebuilt index, but no event written afterwards reaches it. New events are missing, edits go stale, and deletions are dropped from queryEvents only by suppression.
  - Under D3 A, Serverless stays on after the rollback, and PR 1 has no keep-awake (it is a PR 2 controller feature). The production Elasticsearch then sleeps in quiet spells. Every point-in-time snapshot is lost, and later pages answer `CursorExpired`. Until it boots, first pages fail: before PR 2 there is no fallback to Postgres, so they get `ServiceUnavailable` or wait out a cold boot. That is the behaviour R2-B1 fixed for the controller path.
  - After PR 2, spec 6.4 still lets `SEARCH_BACKEND_FORCE=elastic` override the state with no guard. In state `off`, Elasticsearch gets no writes and no keep-awake. The spec's undo says "once the controller has filled it", but nothing enforces that.
- What happens and to whom: on the one day the rollback is needed, every app calling query.iriai.app gets missing or stale results from a backend that may be napping. That is the stakes' "different or missing search results".
- How to confirm: compare spec 7 step 4 with the undo paragraph, and search the packet's three rollback sentence lists for `SEARCH_ES_WRITES`.
- Fix:
  - Make the undo "turn Elasticsearch on; under D3 A, turn Serverless off; set `SEARCH_ES_WRITES=on`; rebuild; compare; set `SEARCH_BACKEND_FORCE=elastic`", with matching sentences for each D3 option.
  - After PR 2, have the query service ignore `SEARCH_BACKEND_FORCE=elastic` unless the state is `on` (or Elasticsearch writes are on and keep-awake runs), and log that it did.

### R3-S2. The only check that Elasticsearch sleeps under D3 A is a routine of another thread that the packet says it does not depend on
- Where: spec 6.3 ("a weekly read of Elasticsearch's memory on Railway … by the cost-monitoring routine through the Railway connector … opens an issue"), spec 6.4 failure (D3 A), packet D3 A Risk row, part 4 "On and off", part 9 risk 3, part 10 ("No dependency on other threads").
- What is wrong:
  - The R2-S1 fix moves the sole off-check outside this feature's code, into a "cost-monitoring routine". facts.md has no such routine, and nothing says who owns it, that it runs weekly, or that it can open a GitHub issue.
  - Part 10 still says "No dependency on other threads".
  - The packet never says who does the weekly read: part 4 says only that "a weekly read … confirms it sleeps".
- What happens: if the routine is never set up or lapses, an awake Elasticsearch bills about $13 to $20 a month unseen. Section 18's stop point and the $55 alert stay silent at that size. Daniel is told a check exists that may not.
- How to confirm: compare spec 6.3 with packet part 10 and facts.md.
- Fix:
  - Name the routine, its owner and its status. List it in part 10 as a dependency, with a fallback: for example, add the Elasticsearch line to the vendor cost register's checked items.
  - Otherwise, state in D3 A's Risk row that the check is a new commitment this thread makes, and how.

### R3-S3. Part 3 step 7 says timeouts count toward the trigger, which is false under D2 B and D2 hold (repeat of round-1 S4 / R2-S6: step back)
- Where: packet line 457 (part 3 "Failure paths", no `data-dp`): "a slow Postgres search can hit its 2 s limit and get 'service unavailable'; each counts as slow toward the trigger." Also the D2 B Risk row ("Searches can get slow below 500,000 … and nothing reacts").
- What is wrong:
  - Spec 6.2 option B says "Timeouts are recorded but do not count". Under D2 hold no controller is built.
  - The step text sits outside any decision point, so a reader with D2 B selected is told something false.
  - The D2 B Risk row still says "slow". The spec now says the default search can fail with `ServiceUnavailable` before the trigger, and under B nothing reacts to that.
- What happens: Daniel picks D2 B believing failing searches still push toward Elasticsearch.
- How to confirm: open option-D2-B.png at part 3 step 7, and read spec 6.2 option B.
- Fix: make the clause a D2 decision point, and change D2 B's Risk row to "can fail with 'service unavailable', and nothing reacts".

### R3-S4. The part 8 summary hides a sentence that section 18 needs Daniel to type
- Where: packet lines 702 and 703 (the new visible summary above the folded part 8 table).
- What is wrong:
  - The summary says "You type five setup sentences, listed in the last row".
  - The Running cost row, now folded, also asks Daniel to type the acceptance "Index search on Postgres: no stop point, exposure estimate $10 a month accepted". Section 18 item 2 requires that he accept the estimate by name.
  - Under D3 C he also types two sentences each time the trigger fires. Under D3 B he creates a token. The summary is not a decision point.
- What happens: with the folds closed, as section 16 sets the default reading, Daniel sees five sentences and does not see the acceptance. The section 18 condition then stays unmet, or is met only by an answer that never names it.
- How to confirm: read lines 702 to 703 against the Running cost row and the D3 B and C sentence templates.
- Fix: put the acceptance sentence in the visible summary, or in the decision surface. Make the sentence count a D3 decision point, or word it so it holds under every option.

### R3-S5. The review pack lacks files the review rule requires
- Where: review-pack-v3/files/. It holds CHECKS.txt, MODEL.json, the option renders and packet-390.png only.
- What is wrong:
  - Section 16 says the pack "holds the packet HTML and its 1440 and 390 renders, the diagram sources". packet-1440.png, the diagram PNGs (CHECKS.txt and the packet link files/diagram-4a.png to 4e.png) and the state renders are missing.
  - The diagram sources are in packet.src.html, but the renders that show what Daniel sees at 390 are not, so the 390-width readability claim in CHECKS.txt could not be checked this round.
- How to confirm: list review-pack-v3/files/.
- Fix: rebuild the pack with every file under files/ that the packet links.

## Nits
- R3-N1. Spec 6.4 counts first-page fallbacks only "while the keep-awake requests are being answered". The keep-awake runs in the ingester and the counter in the query service. State how the query service knows, for example a `keepawake_ok_at` column in `search_control`, which it already reads every 30 s.
- R3-N2. Spec 6.4 reconcile deletes in Elasticsearch inside the Postgres transaction that holds `pg_advisory_xact_lock(7140015)`. Every write, and every first page (`openSnapshot` takes the shared lock), waits for that Elasticsearch call, up to the client's 10 s timeout with one retry. Extras are rare, so this is minor. Take the number and re-check under the lock, then delete after commit, using the number taken under the lock; a later put still gets a higher number.
- R3-N3. Length: spec.md grew 16% in a fix round. CODEX-TRIAGE-v2 "Length" explains only the packet. From this round, section 16 asks the triage to explain any growth of the packet or the spec. Most of the new spec text is the fixes asked for, which is a fine reason, but it should be written down.
- R3-N4. Spec 3 (R2-N3 fix): "the query service answers only after the ingester has recorded that the first fill is done". It does not say what the service answers meanwhile (a 503 with Retry-After, or not listening at all). In PR 1 the first fill also writes Elasticsearch with fail-and-replay (5.1), so an Elasticsearch problem at deploy time would hold all of query.iriai.app down, not just `getEvent`. Gate only `getEvent` (the only Postgres read before step 3) and answer 503 with Retry-After.
- R3-N5. spike2-load.sql now runs `\ir spike-load.sql`, but spike-load.sql needs spike-schema.sql and a `-v n=…` value first. Add one line on the run order. The spike numbers themselves are fairly reported: spike2-results.txt matches spec 4.5 and part 4, and the schema caveat is stated.
- R3-N6. The part 1 decision point (D3) has text for A, B and C only. Under D3 hold it shows A's "once Serverless is on, Elasticsearch waking … need no further word", which does not happen when D3 is held. Check the hold render carries a "held" badge, as the D1 lede does; otherwise add hold text.

## Answers to the brief's specific questions
- **Does the Postgres design reproduce search.ts and service.ts?** Yes: filters, sorts, ties, nulls, antimeridian, keyset paging and suppression, with the documented distance tolerance. One stated difference remains: a 2 s Postgres timeout answers 503.
- **Is the snapshot in 4.3 exact?** Yes, now that the lock is taken before `nextval` and `is_called` is handled.
- **Are the backfill and dual-write safe against lost or resurrected documents?** Yes on the controller path (generations, external versions, re-checked deletes, `gc_deletes` 2 h). No on the manual rollback path, which stops writing (R3-S1).
- **Are the spike numbers fairly reported?** Yes.

VERDICT: REQUEST CHANGES. No blocker remains, but the manual rollback would serve a stale, and under D3 A sleeping, Elasticsearch, and the only check that Elasticsearch sleeps depends on a routine the packet says it does not depend on. Both are repeats that trigger the step back.
