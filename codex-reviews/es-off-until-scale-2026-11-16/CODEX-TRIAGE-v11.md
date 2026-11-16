# Triage of Codex round 4 (review of v10, build d62a361, pack commit 7b34fd7); fixes land in v11

Reviewer: Codex Astra on Daniel's Mac. Review: CODEX-REVIEW-v10.md. Stakes unchanged from CODEX-TRIAGE-v10.md; reviewer: **STAKES: AGREE**. No blocker.

## Step back check (round 4)

Codex confirms the round-3 step back works: the durable `es_pending` record closes the crash window, and no lost-document or resurrection path was found. Of the open protected findings, F1 is a regression caused by the step back's retention rule. The fix gives each index its own retirement time, which is simpler than one shared deadline. F2 and F3 are D3 B permission scope; one rule fixes them: the Railway token exists only if D2 builds a controller. F4 and F5 are PR-boundary wording. No mechanism is added.

## Findings

| # | Finding | Severity | Decision | What changed |
|---|---|---|---|---|
| F1 | One shared `elastic_cursors_until` keeps retired indexes alive while any session runs | should fix (protected) | fixed | Spec 5.4: each index records `retired_at` when new sessions stop going to it, and is deleted 61 minutes later; at most two indexes exist. Spec 3: `elastic_cursors_until` now only governs when use may end. Spec 8: disk is "at most two indexes". |
| F2 | D2 Hold + D3 B still installs the Railway token | should fix (protected) | fixed | Spec 6.3 B and 9: the token is created only if D2 builds a controller. Packet D3 B security row: "New, if D2 builds a controller". Sentences row B: no token if D2 is held. |
| F3 | Revoking the token does not turn B into C | should fix (protected) | fixed | D3 B "Later": revoking stops it, you deploy or remove by sentence, and moving to C is a code change. Spec 6.3 B: with the token revoked, each attempt fails and opens an issue. Spec 6.4 B: removal is confirmed through Railway's API or by the cluster not answering for 30 minutes, so a manual removal is recognised. |
| F4 | Failure journey and coverage table apply PR 2 protections to PR 1 | should fix (protected) | fixed | Part 3 step 7: first-page fallback once Postgres is checked; "from PR 2" for the unconfirmed-write rule. Part 13, spec 5 row: "PR 1 writes today's index as today; from PR 2 Elasticsearch writes never fail an event". |
| F5 | Issue on ignored `SEARCH_ES=off` before verification needs PR 2 | should fix | fixed (clarified) | `SEARCH_ES` is not in PR 1 (spec 5.1 and 7 step 1), so this path only exists from PR 2, when issues exist. Spec 5.2 now says so. |
| F6 | CPU estimate presented as measured | should fix | fixed | Spec 8 and the packet cost row label 0.3 s of CPU per search an assumption: the spike timed elapsed time, CPU was not measured [unverified]. The figure stays at $12 and the acceptance sentence is unchanged. |
| F7 | Box-edge rounding outside the tolerance | nit | fixed | Spec 4.4 and 6.4, packet part 4: "radius or box edge". |

Rejected: none.

## Length

The spec grows by about 140 words: the per-index retirement rule (F1), the token condition and revocation path (F2, F3), and the CPU assumption label (F6). The packet grows by a few clauses for the same findings and loses none. All were required by the findings.
