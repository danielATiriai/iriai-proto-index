# Amendment to D2 and D3: turn Elasticsearch off only

Chosen by Daniel in the thread "Turn Elasticsearch off", Nov 19, 2026, 22:07 UTC, on a decision card (card cmsg_018w2Yxn469hwrAKCburTJyb6rh8EjTnrpz8R57DEuf9tp, recommended option). Card text, verbatim:

> Should PR 2 only turn Elasticsearch off, and leave the automatic switch-on for later?
>
> **Turn off only (amend D2/D3)**: Writes stop and nothing is built; the controller measures and opens an issue at the scale trigger, and the switch-on comes in a later PR when it is needed.

The other option was "Keep the full controller". Why: every blocker in the review of the full PR 2 (Codex Astra, df6c1b1) sat in the automatic switch-on path, and that path is not needed until the trigger (9 events today; 500,000 is the trigger).

## What changes in spec.md (v14, build 5e80472) for PR 2

- **5.1, 5.2.** Elasticsearch is not written, read or contacted, and there is no managed index or `es_pending`. Postgres serves once verified, else `ServiceUnavailable`. "Until Postgres is verified, Elasticsearch serves alone" no longer applies, so PR 2 deploys only while health shows `search.pgVerified: true`. The verifying comparison checks Postgres against sidecars assembled again from stored records, not today's index.
- **5.3.** Health drops `esInUse`, `esCurrent` and `state`, shows `elastic: "off"`, and adds the scale check's last result.
- **5.4.** Code does not delete today's index. It goes when Daniel removes the Railway Elasticsearch service.
- **6.1, 6.2.** These are measured as specified, with the D2 A switch-on thresholds. The event count is written by the ingester.
- **6.3, 6.4.** No controller, states, automatic switch-on or switch-off, and no weekly Railway check. When the switch-on condition holds, the ingester opens a GitHub issue (at most one a day), and switching a search engine on becomes a later PR written then. The D2 A switch-off thresholds and the 30-day limits wait for that PR.
- **7, step 4 and step 5.** The GitHub token is stored after the merge. Step 5 becomes: Serverless on Elasticsearch, or its removal on Daniel's sentence; remove `SEARCH_BACKEND_FORCE` only once a revert to PR 1 is no longer wanted.
- **10.** CI runs against Postgres only.

D1 (exact snapshots) is unchanged.
