# Triage of Codex round 5 (review of v11, build facd495, pack commit 244e03e); fixes land in v12

Reviewer: Codex Astra on Daniel's Mac. Review: CODEX-REVIEW-v11.md. Stakes unchanged from CODEX-TRIAGE-v10.md; reviewer: **STAKES: AGREE**. No blocker. Codex again found no lost-document or resurrection path and no filter, sort or paging mismatch.

## Step back check (round 5)

All four findings are protected and recur from earlier ones. Each fix below removes mechanism or adds a fail-closed condition; nothing is patched onto the previous rule:

- G1: a write counts as confirmed only once it is searchable (`refresh=wait_for`).
- G2: removal is confirmed only through Railway's API; the timeout shortcut added in v11 is gone.
- G3: D3 B is not possible with D2 held (a `notWith` relation). This replaces the conditional wording v11 added, so the token texts no longer need it.
- G4: `retired_at` is gone. An index is deleted as soon as no session can reach it.

## Findings

| # | Finding | Severity | Decision | What changed |
|---|---|---|---|---|
| G1 | Replay clears pending before the write is searchable | should fix (protected) | fixed | Spec 5.1: managed-index writes and replays use `refresh=wait_for`, and a 409 counts only after a refresh returns. Cost added to 5.2: up to about a second per write while Elasticsearch is in use. |
| G2 | D3 B confirms removal by 30 minutes of silence | should fix (protected) | fixed | Spec 6.4 B: confirmed through Railway's API, and only through it. Spec 6.3 B and the D3 B "Later" row: with the token revoked, an issue opens each hour until the token works or a code change moves to C, while Daniel deploys or removes by sentence. |
| G3 | D2 Hold + D3 B leaves token and authorization text | should fix (protected) | fixed | Relation `notWith`: D3 B is impossible when D2 is held, with the reason shown. The conditional token wording from v11 is removed from spec 6.3 and 9 and from packet parts 8 (security, sentences). |
| G4 | "At most two indexes" not enforced | should fix | fixed | Spec 5.4: a managed index is deleted when use ends (which waits for `elastic_cursors_until`), a failed candidate at once, today's index 61 minutes after PR 2, and leftovers before a new build. That gives at most one index, or two in the hour after PR 2, and none while Elasticsearch is off. Spec 8's "at most two indexes" now holds. |

Rejected: none.

## Length

The spec changes by a few words. The 5.4 rule is about as long as the one it replaces, the `refresh=wait_for` clause and its cost are added, and the conditional token text is removed. The packet adds one relation and one "not possible with D2 held" clause, and drops two conditional clauses.
