# Triage of Codex round 7 (review of v13, build 9e1c3c2, pack commit 531556f); fixes land in v14

Reviewer: Codex Astra on Daniel's Mac. Review: CODEX-REVIEW-v13.md. Stakes unchanged from CODEX-TRIAGE-v10.md; reviewer: **STAKES: AGREE**. No blocker. Codex confirms that H1 and H2 are right. It again found no loss or resurrection path, no filter, sort or paging mismatch beyond one summary line, and that the cost row meets section 18 item 5.

## Step back check (round 7)

All three should-fix findings complete fixes from round 6, and none adds mechanism:

- I1 widens the D2-held scope line and qualifies the controller passages.
- I2 states the retention bound so that it covers the failure case.
- I3 adds one disclosure sentence to the packet.

## Findings

| # | Finding | Severity | Decision | What changed |
|---|---|---|---|---|
| I1 | D2 Hold excluded reconcile and replay, which builds and rollbacks need; controller passages were unqualified | should fix (protected) | fixed | Spec 6 opening: with D2 held, only the thresholds, the automatic on and off, and the states are left out. The stats, build, reconcile, replay, verifying comparison and weekly check are still built. Packet: part 3 steps 6 and 7, part 4's controller paragraph, the 4d caption and the part 5 scope line each say the controller is not built if D2 is held. |
| I2 | Two indexes can be left behind if Elasticsearch fails in the hour after PR 2 | should fix (protected) | fixed | Spec 5.4: at most two indexes exist (today's and one managed, in the hour after PR 2), otherwise at most one. Indexes Elasticsearch could not delete because it was down (at most those two) stay on its volume until use next begins. |
| I3 | Packet omits the 3-second freshness allowance | should fix (protected) | fixed | Packet part 4, "One rule": a new Elasticsearch search can still miss a write from the last 3 seconds, and its later pages keep that view, as Elasticsearch's own refresh already allows today. |
| I4 | Default-sort summary | nit | fixed | Spec 2: with no sort, distance when a coordinate is given, else AT-URI only. |

Rejected: none.

## Length

The spec grows by about 40 words, for the D2-held scope (I1) and the retention bound (I2). The packet grows by five short qualifiers (I1) and one sentence (I3). All are required by the findings.
