# Triage, round 1: "Event search without Elasticsearch until scale"

Under review: packet v1, build cb43cea, spec draft v1 (2026-11-16). Code context iriai-proto-index main d9ab983.
Reviewer: independent Claude reviewer (fallback). Codex Astra was not run: this thread was started by the coordinator and has no message from Daniel, and Remote Control needs one to start on his Mac.
Review: REVIEW-v1.md. Fixes land in packet v2 and spec draft v2.

## Stakes

As given: Kind proposal or plan (for production infrastructure); reaches every caller of query.iriai.app; worst failure: wrong or missing results, or a controller that runs up cost or holds a token with more reach than stated; undone by a new version / read switch and redeploy; nothing said by Daniel beyond the standing rules.

Reviewer: STAKES: DISAGREE. (1) The stated undo serves a stale index, because nothing rebuilds Elasticsearch before PR 2. (2) The worst failure omits an outage of query.iriai.app and an ingestion stall (both roles wait for Elasticsearch at start-up, src/main.ts:18), and a Railway token that reaches every service and secret in placeproto.

Thread: agreed, checked against src/main.ts:17-18, src/ingest/transient.ts and Railway's docs ("Project tokens are scoped to a specific environment within a project"). Stakes corrected upward for round 2: worst failure now includes "query.iriai.app or ingestion down after Elasticsearch is turned off, and a credential with the whole project's reach"; undo now "turn Elasticsearch on, run the rebuild command, switch reads back".

## Findings

| # | Finding | Severity | Decision | Reason / what changed |
|---|---|---|---|---|
| B1 | Elasticsearch stays a start-up and health dependency; removing it takes the API and ingestion down | blocker | fixed | Confirmed in src/main.ts:18. Spec 2 now records the wait; new spec 5.2 makes Elasticsearch optional (lazy client, no start-up wait, no traffic while off), 5.1 says a failed Elasticsearch write never fails an event, 5.3 specifies health and the acceptance script. Packet part 4 and part 7 say so. |
| B2 | Railway project token reach misstated as narrow | blocker | fixed | Confirmed in Railway docs. Recommendation reversed: D3 A is now Railway Serverless (sleep and wake, no credential); the token is D3 B with its real reach stated in its rows, in part 8 Security and as a departure from Eng directions 3 and 16; "your word" is D3 C. |
| B3 | Dual-write and reconcile lose updates; versions break under D1 B | blocker | fixed | Spec 4.2: every put and remove takes a new number from a sequence (both D1 options); spec 6.4: reconcile compares id and version, re-puts missing or older documents, deletes extras with a fresh version, runs at the end of backfill, on every tick while dirty, and daily; `index.gc_deletes` 2 h. |
| S1 | Snapshot not exact (`now()` is transaction start) | should fix | fixed | Spec 4.3 now uses a sequence watermark read under the shared lock; no clock involved. |
| S2 | Verification cannot pass at 1 mm; wrong explanation | should fix | fixed | Spec 4.4 and 6.4: same set, order equal apart from distances under 5 cm, after a refresh, comparing id-and-version sets, retry on events that changed. Packet part 4 sentence corrected. |
| S3 | Spike numbers selective and partly unbacked | should fix | fixed | Re-ran everything in the D1 A form, fetching documents, with recent churn: spike/spike2-*. Spec 4.5 table replaced with one consistent run; memory and CPU caveats; the earlier run marked superseded. Size threshold lowered to 500,000 (on) and 250,000 (off) because the default search passes 250 ms near a million. |
| S4 | Statements false under other options, unmarked | should fix | fixed | Lede, callout, goal, step 6, 4a caption and in-words, 4e, 4f, part 9 risk 4, part 10, part 12 reworded to hold under every selection or made decision points (lede: D1; interface row, Security, Earlier decisions, Sentences: D3). |
| S5 | Token option silently overrides Eng directions 3 and 16 | should fix | fixed | Part 8 "Earlier decisions" is a D3 decision point; under B it names the standing authorization replacing the sentence. Under the new A no Railway change happens without a sentence. |
| S6 | Failure paths leave Elasticsearch billing; stop-point text wrong | should fix | fixed | Spec 6.4: every failure turns Elasticsearch off and confirms it; failure to turn off retried with hourly issues; drain on repeated fallbacks or a red cluster; getEvent always on Postgres. Part 3 step 7 and part 8 Running cost reworded. |
| S7 | Running cost row short of s18 item 5 | should fix | fixed | Ranges for today, ten times and the trigger; memory and vCPU limits (bound about $40); price tags corrected ([docs] memory and vCPU, re-read today; disk [unverified]); $16 sourced to the cost thread; GitHub cost line; acceptance sentence for the Postgres exposure estimate. |
| S8 | Sentence list incomplete | should fix | fixed | Part 8 lists every sentence, numbered and grouped by when, including clearing FORCE, the controller variables, tokens and rollback, per D3 option. |
| S9 | Rollback serves a stale index | should fix | fixed | Spec 5.4 adds `npm run search:rebuild-es` in PR 1; spec 7 undo and part 8 Rollout use it. |
| S10 | Coverage wrong; no heading links | should fix | fixed | Spec built to spec.html with an anchor per heading; part 13 links to them and maps the items the reviewer listed; packet-only claims named in the reverse check. Gate numbering aligned (Railway checks in gate 2 for D3 A's sleep check). |
| S11 | Cursor rule contradicts itself across the switch | should fix | fixed | Spec 3: `elastic_cursors_until`; an Elasticsearch cursor is tried only while Elasticsearch accepts cursors, otherwise CursorExpired without contacting it (which also keeps a sleeping Elasticsearch asleep). |
| S12 | Default reading 2,336 words | should fix | fixed in part | Part 4 detail, part 12, part 13, the interface table and the migration note folded. The count still rose, to 2,490, because the fixes add text the rules require: a third D3 option with its rows and decision points, the full section 18 cost row, the numbered sentence list, and the failure paths. Section 16 asks for detail to be folded rather than cut; what is left open is decision content. |
| N1 | Antimeridian predicate needs parentheses | nit | fixed | Spec 4.4. |
| N2 | Callers and the 9-event count stated as fact | nit | fixed | Tagged unverified / dated in parts 8 and 15. |
| N3 | Spike closed-rows branch always empty | nit | fixed | spike2 adds 1% recent edits. |
| N4 | Session advisory lock with a pool | nit | fixed | Spec 6: one dedicated client. |
| N5 | First-page fallback could wait 20 s | nit | fixed | Spec 6.4: fallback after 2 s. |
| N6 | Part 6 omits "Postgres plus an issue" | nit | fixed | Row added. |
| N7 | Production comparison exercises little | nit | fixed | Spec 7 step 2 says CI is the real guard. |
| N8 | Search shares ingestion's pool, no timeout | nit | fixed | Spec 4.4: own pool of 5, 2 s statement timeout; part 8. |

Rejected: none. Every finding was checked against the code or the docs and held.
