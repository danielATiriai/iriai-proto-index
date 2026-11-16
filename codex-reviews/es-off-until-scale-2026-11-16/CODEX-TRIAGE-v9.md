# Triage of Codex round 2 (review of v8, build f476432); fixes land in v9

Reviewer: Codex Astra on Daniel's Mac. STAKES: AGREE. Review: CODEX-REVIEW-v8.md.

| # | Finding | Severity | Decision | What changed |
|---|---|---|---|---|
| D1 | Same-day index name collision blocks a rollback | should fix | fixed | Spec 6.4: `placeproto-events-<n>`, n a build counter in `search_control`. |
| D2 | Immediate old-index deletion kills open paging sessions | should fix | fixed | Spec 5.4: the previous index is kept until `elastic_cursors_until` (61 minutes), then deleted; this also applies at the PR 1 deploy. |
| D3 | Write lock held across the Elasticsearch request blocks Postgres first pages | should fix | fixed, by simplifying | The C3 lock-holding is removed (spec 4.2: the Elasticsearch write follows the commit, outside the lock). Spec 5.2: the may-serve check reads the control row fresh at each first page; a session opened within 2 s of a write can miss it, the same kind of delay Elasticsearch's 1 s refresh gives today. Stated as the rule's cost; packet part 4 says so. |
| D4 | Zero-traffic days block the D2 A switch-back | should fix | fixed | Spec 6.2 and D2 A text: a day with fewer than 500 calls counts as fast. |
| D5 | Disk bound unsupported | should fix | fixed | Spec 5.4: every tick deletes any index that is neither current nor the 61-minute previous one. Spec 8 and packet: disk has no stop point, about $0.10 to $0.20 a month at 500,000 to a million events (estimate, price unverified); the acceptance sentence now covers it: "Index search: no stop point on Postgres or Elasticsearch's disk, exposure estimate $11 a month accepted". |
| D6 | Packet part 4 contradicts spec 3 and 5.1 | should fix | fixed | Rewritten: lookups follow the same rule; failed writes stop holding up ingestion only after Postgres's first check, replayed before that as today. |
| D7 | D2 Hold moots D3 although PR 1's switch-off needs it | should fix | fixed | Relation removed; D2 Hold says D3 still decides the rollout switch-off; D3 Hold says the rollout removes the deployment on Daniel's sentence. |
| D8 | D1 B leaves "notice nothing" and "Nothing visible" | should fix | fixed | Both are D1 decision points with B text; the D1 B fold names part 2. |
| D9 | No merge gate or risk rating | should fix | fixed | Part 1 (all D3 texts): each PR merges only on Daniel's separate merge word, risk at least medium. Part 10 gates add "then your merge word". |
| D10 | Positions 1–45 cutoff | nit | fixed | Spec 6.4: each event in positions 1–45 of one backend's 51 must appear in the other's 51; shared events in the same order apart from under-5-cm differences. |
| D11 | Spike `arc_m` not STRICT | nit | fixed | spike-schema.sql declares it `strict`; spec 4.1 requires `STRICT`. Timings unaffected (spike data has no null points in the timed paths). |

Rejected: none.

Recurrence check (codex-review 6a, applies from round 3): D1 to D3 were caused by round-1 fixes; D3 was fixed by removing mechanism, not adding it.

Length: spec grows slightly (generation counter, index retention, quiet-day rule, disk line) and shrinks where the lock-holding was removed. Packet: the merge gate and the D1 B and D2/D3 hold texts are required by the findings.
