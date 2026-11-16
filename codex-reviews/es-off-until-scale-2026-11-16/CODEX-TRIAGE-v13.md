# Triage of Codex round 6 (review of v12, build 4b20e13, pack commit 9d0757d); fixes land in v13

Reviewer: Codex Astra on Daniel's Mac. Review: CODEX-REVIEW-v12.md. Stakes unchanged from CODEX-TRIAGE-v10.md; reviewer: **STAKES: AGREE**. No blocker. Codex again found no lost-document or resurrection path and no filter, sort or paging mismatch. The Running cost row now meets section 18 item 5.

## Step back check (round 6)

The count did not fall (4 and 4), and H1 is a regression caused by G1. So each fix this round removes a mechanism or applies one rule to a whole class, rather than patching the case Codex named:

- H1: writes no longer wait for a refresh. Only replays refresh, once per batch, before they clear their rows.
- H2: one weekly Railway check now covers every D3 option, instead of a confirmation rule for each.
- H3: D3 does not apply when D2 is held (`mootWhen`). The page then marks every D3 passage "does not apply". This replaces the round-5 `notWith` for B and the conditional wording.
- H4: a single retention statement now includes its failure exception.

## Findings

| # | Finding | Severity | Decision | What changed |
|---|---|---|---|---|
| H1 | `refresh=wait_for` on every write throttles serial ingestion | should fix (protected) | fixed, by removing | Spec 5.1: writes do not wait for a refresh, so ingestion is not slowed. Replays write their batch, refresh once, then delete their rows, which keeps G1's guarantee for the stale-after-recovery case. Spec 5.2: a new session can miss a write from the last 3 seconds (the 2 s limit plus the 1 s refresh), the same kind of delay as today. The cost line no longer mentions waiting. |
| H2 | D3 C treats an unanswering cluster as removed | should fix (protected) | fixed | Spec 6.4: whatever D3 picks, the weekly routine reads Elasticsearch's memory on Railway and opens an issue if it is non-zero while not in use. A deployment left running after any switch-off is reported within a week, at a cost of up to a week of compute (about $3). C's text says so. Packet part 10: the weekly Railway check is set up for every option. |
| H3 | D2 Hold + D3 A or C promise controller automation | should fix (protected) | fixed, by a relation | `mootWhen` D3 when D2 is held, with its reason, so every D3 passage and option is marked "does not apply". D2 Hold text: Elasticsearch's deployment is removed at rollout on Daniel's sentence and comes back only by his sentences. The sentence and rollback texts say "if D3 is held or does not apply". Spec 6 opening and 7 say the same for D2 held. The round-5 `notWith` is removed as redundant. |
| H4 | "None while off" contradicts failed-deletion leftovers; 6.3 A still keeps the old index | should fix (protected) | fixed | Spec 5.4: while Elasticsearch is off none remains, except one it could not delete when use ended (an outage then), which stays on its volume until use next begins. Spec 6.3 A: its index was deleted when use ended. |

Rejected: none.

## Length

The spec grows by about 90 words: the weekly-check paragraph (H2), the D2-held line at the top of 6 (H3), and the retention exception (H4). The `refresh=wait_for` text is shorter. The packet adds one relation and the D2 Hold sentence, and drops the B-only relation and one "Not possible with D2 held" clause. Part 11's review summary now points to the header's list of reviews instead of repeating the links.
