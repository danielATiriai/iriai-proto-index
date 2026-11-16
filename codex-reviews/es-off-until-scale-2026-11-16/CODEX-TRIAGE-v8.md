# Triage of the Codex Astra review of v7 (fixes land in v8)

Under review: packet v7, build 183ae1d, spec draft v7. Pack: iriai-proto-index d9fd017, branch claude/project-thread-0uvnq8, codex-reviews/es-off-until-scale-2026-11-16 (41 checksums OK on the Mac).
Reviewer: Codex Astra (gpt-6-astra) on Daniel's Mac, started at his request ("Do a codex run.", Nov 16 04:58 UTC). Exit 0, first run. Review: CODEX-REVIEW-v7.md.

## Stakes

Reviewer: STAKES: DISAGREE: "How it is undone" still named a manual rebuild command; spec 7 now undoes by redeploying Elasticsearch under D3 B or C, setting `SEARCH_ES=on` and `SEARCH_BACKEND_FORCE=elastic`, and waiting for the automatic rebuild. Reach and harm otherwise agreed.
Thread: agreed; the line is corrected for the next round: "How it is undone: the proposal by a new version; the change by redeploying Elasticsearch (D3 B or C), setting SEARCH_ES=on and SEARCH_BACKEND_FORCE=elastic, and waiting for the ingester's automatic rebuild." Severity unchanged.

## Findings

| # | Finding | Severity | Decision | What changed |
|---|---|---|---|---|
| C1 | PR 1 mixes Elasticsearch's internal versions with the Postgres sequence: verification never passes, later writes get 409 and are skipped | blocker | fixed | Spec 5.4: today's index is not trusted; on first start the ingester fills Postgres, builds a fresh index from it with external versions, sets `es_current`, deletes the old index; a few seconds of `ServiceUnavailable` with Retry-After meanwhile. Spec 5.1: a 409 means Elasticsearch holds a newer version and counts as success; Elasticsearch's own versions are never compared. Packet part 3 step 7 and part 7 start-up row state the few seconds. |
| C2 | `getEvent` bypasses the verification gate | should fix | fixed | Spec 3: `getEvent` follows 5.2, reading the backend that would serve a new session. Packet coverage row updated. |
| C3 | 30-second cache lets a new session open on a dirty Elasticsearch | should fix | fixed | Spec 4.2: while Elasticsearch is in use, a write makes its Elasticsearch write (2 s timeout) or raises `dirty_gen` before commit, under the exclusive lock. Spec 5.2: the may-serve check is made at each first page under the shared lock as the snapshot opens. Cost stated: each write waits for Elasticsearch, up to 2 s, while it is in use. Packet part 4 rule says so. |
| C4 | 7-day stats cannot prove a 14-day condition | should fix | fixed | Spec 6.1: daily rows kept 90 days; only the query shapes are cleared after 7 days. Packet parts 4, 4b list and 8 updated. |
| C5 | Packet omits the 30-day minimum, one switch-on in 30 days, and exact thresholds | should fix | fixed | D2 A and B option text now give every threshold (500 calls, 250 ms or over 1% timeouts, 100 ms and 250,000 back, 30 days on, one switch-on in 30 days); D2 A Risk row names the month-long consequences; part 3 steps 6 and 7. |
| C6 | 5.2 cost understates duration; packet overstates outage independence | should fix | fixed | Spec 5.2 cost: Postgres serves until a repair succeeds, through any outage; before verification Elasticsearch outages stall ingestion as today. Packet start-up row and coverage row say "once Postgres has passed its check". |
| C7 | Spike did not read the documents | should fix | fixed | spike2-bench.sql now filters on `length(doc::text)`; re-run as spike3-results.txt. Numbers changed little (1M: month by start 315/370 ms, default 304/359, week 91/103). Spec 4.5 table and packet part 4 updated; spike2-results.txt marked superseded. |
| C8 | Running cost row treats the compute limit as a total bound; says an alert enforces | should fix | fixed | Spec 8 and packet cost row: limits cap compute at about $40; disk billed on top (at most two indexes, under $0.50 a month, price unverified); Postgres estimate "which nothing enforces (the alert only warns)". |
| C9 | GitHub token can do more than open issues | should fix | fixed | Spec 9 and packet 4e caption, 4e list and Security rows: limited to issues in iriai-proto-index; can open, edit and close them; our code only opens and comments. |
| C10 | Selections leave contradictions (D1 B breaking, D2 held gate 3, D1 held plan) | should fix | fixed | Part 6 recommended row and part 8 "Effects on other features" are D1 decision points with B text. Holds cannot carry text, so part 10's gates say "not built if D1 is held" and "if D2 is held, stats only and no controller". |
| C11 | `closed_at=now()` is transaction start | nit | fixed | Spec 4.2: `clock_timestamp()` read after the lock. |
| C12 | Verification rejects allowed geographic differences | nit | fixed | Spec 6.4: compare positions 1 to 45 of a 51-result page, ignoring events within 5 cm of a radius edge. |

Rejected: none.

## Length

Spec draft v8: grows, because C1, C3, C4 and C8 add mechanism the design lacked (a fresh index at deploy, the lock covering the Elasticsearch write, retention, disk). Packet: D2 option text and the cost row grow because C5 and C8 ask for the exact conditions on the decision surface; the review card was shortened. Counts in CHECKS.txt.
