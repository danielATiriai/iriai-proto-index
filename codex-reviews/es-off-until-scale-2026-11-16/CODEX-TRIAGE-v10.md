# Triage of Codex round 3 (review of v9, build d1afd7c, pack commit 66ca641); fixes land in v10

Reviewer: Codex Astra on Daniel's Mac. Review: CODEX-REVIEW-v9.md.

Stakes (unchanged): proposal or plan for production infrastructure (query.iriai.app search and Railway settings); reached by Daniel, and after build by any app calling query.iriai.app; worst realistic failure: apps get wrong or missing events, or Elasticsearch keeps billing; undone by settings, or by redeploying Elasticsearch under D3 B/C/Hold, then settings. Reviewer: **STAKES: AGREE**.

## Step back (codex-review 6a, round 3)

Findings 1, 2, 4, 6 and 7 are protected (production behavior, paging, approval scope, rollback) and each is another case of a problem an earlier fix addressed (C3/D3, D2, C10/D7, C6/D6, C7). Finding 5 repeats C8/D5 and finding 8 repeats C12/D10. What keeps recurring: v7 to v9 had PR 1 replace today's index and track Elasticsearch's freshness in memory. Every patch to that left another path (crash, initial replacement, retention deadline, pre-verification fallback). So the patches are replaced, not extended:

1. **PR 1 leaves today's index alone.** PR 1 writes Postgres first and then today's index exactly as today (Elasticsearch's own versions, fail-and-replay), never replaces it, and compares by document id and content. Elasticsearch is turned off only after PR 2. This removes the initial replacement (finding 2's case), the version-namespace mix (C1), and PR 1's alerts (finding 3). Cost: Elasticsearch is turned off a few days later (after PR 2, not PR 1), about $1 to $2 more. Also, until PR 2 an Elasticsearch outage stalls ingestion, as today.
2. **Unconfirmed writes are saved in Postgres with the write (`es_pending`).** A managed index may serve only while no pending row is older than 2 s, read fresh at each first page. This replaces `dirty_gen`/`repaired_gen` and fails closed for every reason a write goes unconfirmed: a failure, a timeout, or a crash between the two writes. Reconcile writes its differences as pending rows. Cost: one row inserted and deleted per write while Elasticsearch is in use.
3. **Retention rule.** Every Elasticsearch first page extends `elastic_cursors_until` to at least now + 61 min, and PR 1's migration sets it for earlier cursors. A non-current index is deleted only after that time, and use ends only after it. One field now covers every way an index stops serving, with no per-event deadline. Cost: one row update a minute at most while Elasticsearch serves.

## Findings

| # | Finding | Severity | Decision | What changed |
|---|---|---|---|---|
| E1 | Crash between the Postgres commit and the Elasticsearch write leaves Elasticsearch eligible | should fix (protected) | fixed (step back 2) | Spec 4.2, 5.1, 5.2, 6.4; packet part 3 step 7, part 4 "One rule", 4c (new `es_pending` table); test added (spec 10). |
| E2 | Initial index replacement sets no retention deadline | should fix (protected) | fixed (step back 1 and 3) | PR 1 no longer replaces any index (spec 5.4, 7). Spec 3: first pages extend `elastic_cursors_until`, and the migration sets it at deploy. Spec 5.4: a non-current index is deleted only after it. |
| E3 | PR 1 promises issues that only exist from PR 2 | should fix | fixed (step back 1) | Spec 5.2 and 7 step 2: in PR 1 the comparison reports in health and the thread checks it and tells Daniel; issues start with PR 2. Packet cost row says so. The token is still stored before PR 2, which is now when issues begin. |
| E4 | D2 Hold and D3 Hold contradict each other; D3 Hold has no removal sentence | should fix (protected) | fixed | D3 Hold now says nothing turns Elasticsearch on by itself, and if D2 builds a controller, a met trigger only opens an issue. D2 Hold says D3's automatic parts are not built. The sentence row (A text, shown for Hold) gives the Hold removal and redeploy sentences inline. All three variants say "without the variables if D2 is held". |
| E5 | $11 estimate omits Postgres CPU | should fix | fixed | Spec 8: CPU of about 0.3 s per search (the spike's slowest shapes, used as an upper bound) is about $0.70 a month per 10,000 searches a day. Exposure estimate is now $12 at 500,000 events and 10,000 searches a day; today's traffic is marked unmeasured. New acceptance sentence: "Index search: no stop point on Postgres or Elasticsearch's disk, exposure estimate $12 a month accepted". |
| E6 | Failure journey promises fallback before Postgres is verified | should fix (protected) | fixed | Diagram 4b has an `else` branch ("fails before Postgres is checked: service unavailable, as today"). The caption, the in-words list and part 3 step 7 now state the condition. |
| E7 | Rollback summaries omit the redeploy under D3 B/C | should fix (protected) | fixed | Part 7 migration fold and part 8 rollout row: until Elasticsearch is off, one setting. After that, redeploy first under D3 B, C or Hold, then two settings. Spec 7 matches. |
| E8 | Near-tie group can cross the 45/51 cutoff | nit | fixed | Spec 6.4: compare the first 50. A tie group that crosses position 50 is fetched whole from both and compared as a set. |
| E9 | 5 cm is not a worldwide bound | nit | fixed | Spec 4.4 and 6.4, packet part 4: the tolerance is 1 m, above Lucene's documented 40 cm. |

Rejected: none.

## Length

The spec grows by about 220 words (4,493 to about 4,720). The new text is the two write modes of 5.1, the `es_pending` rule and its cost in 5.2, and the CPU line in 8. The step back needs them to replace the removed `dirty_gen` mechanism, and the CPU line is required by E5. The packet's part 7 and part 8 rollback text grows by a clause each for the redeploy, which E7 requires. Part 3 loses the deploy-time "service unavailable" sentence, and part 7 loses a clause from its start-up row.
