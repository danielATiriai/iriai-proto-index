# Facts the packet relies on (collected Nov 16, 2026)

- Railway project placeproto (production): services Postgres, Elasticsearch, query, ingester, placeproto-site, placeproto-site-staging. Read with the Railway connector.
- Metrics, last 7 days (Railway connector, get-service-metrics): Elasticsearch memory 1.28 GB average (min 1.27, max 1.30), CPU 0.004 vCPU average, disk 0.25 GB. Postgres memory 0.074 GB average, disk 0.25 GB. Query service memory 0.08 GB, CPU near zero, egress near zero.
- No TCP proxy on Postgres (not reachable from outside Railway).
- Acceptance run Oct 13 (GitHub Actions run 38461986239): health showed 9 complete sidecars and 9 index documents. No later count was read.
- Vendor cost register, Railway line (Nov 16): Pro plan since Nov 16, $20 a month including $20 usage; custom email alert at $55; no hard limit; exposure estimate $70 a month accepted by Daniel on Nov 16.
- Railway Pro prices used: $10 per GB of memory a month, $20 per vCPU a month, $0.15 per GB of volume a month [docs, from memory of Railway's pricing page; not re-read today].
- The cost thread offered Daniel an interim heap cut (ES_JAVA_OPTS 512 MB); that thread applies it if he agrees. This proposal leaves it alone.
- Codex Astra could not be reached from this thread (no person's message in the thread to start Remote Control with), so this review is the fallback reviewer.

Added for round 2 (Nov 16):
- Railway docs (read through the Railway connector's doc search): "Rates are $20 per vCPU-month and $10 per GB of memory per month"; "Project tokens are scoped to a specific environment within a project and can only be used to authenticate requests to that environment"; Serverless: "Inactivity is based on the detection of any outbound packets ... Once a service stops sending packets it is considered inactive after 5 minutes ... in practice a service sleeps somewhere between 5 and 10 minutes after its last outbound traffic". The page section "Waking a service up" could not be fetched; that a request over private networking wakes a sleeping service is not confirmed from the docs.
- The spike was re-run (spike2-*): the earlier spike files are superseded and not in this pack.
