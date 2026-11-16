# Approval: "Search without Elasticsearch" v14, build 5e80472

Approved by Daniel in the thread "Move event search off Elasticsearch until scale", Nov 16, 2026, 21:04 UTC (message cmsg_018w2Yxn469hwrAKCburTJyb1VngjvM1sVMtv82PPXQdiS). His words, verbatim:

> Index search: no stop point on Postgres or Elasticsearch's disk, exposure estimate $12 a month accepted
>
> Approve proposal "Search without Elasticsearch" v14 as recommended

Route: answer in words, all recommended and none held, so no selection page is needed (feature-proposal skill, section 5).

## Decisions as recorded

- D1 paging: A, exact snapshots.
- D2 trigger: A, speed or size (on after 3 days with at least 500 calls a day and p95 at least 250 ms or over 1% timeouts, or at 500,000 events; off after at least 30 days on, plus 14 days with p95 at most 100 ms and at most 250,000 events; at most one switch-on in 30 days).
- D3 on and off: A, Railway sleep (Serverless turned on once by Daniel's sentence; weekly Railway check).
- Running cost (Eng directions 18): exposure estimate accepted by name, as quoted above.

## Pinned

- Packet fragment sha256: 5e80472c1949caa087a9c52646e1703c23a376e590ae8a52630c1940654c27b3 (packet.html in this folder).
- packet.src.html sha256: 75bb40200f085623d1946046ab7514459fea0f40c37b8e3678d1f0d10a55f493
- spec.md (draft v14) sha256: 41e815a38883f9171d0cc4c823fb3a6ae56d4eb3190e3b228145707464eb074d
- Review: Codex Astra round 8, APPROVE with no findings (review pack at commit 1b15c00, codex-reviews/es-off-until-scale-2026-11-16/CODEX-REVIEW-v14.md), pack at iriai-proto-index 1b15c00, branch claude/project-thread-0uvnq8. Earlier: Claude reviewer rounds 1 to 7, Codex rounds 1 to 7, triages in codex-v7/ and files/.
- Drive copy: folder "Search without Elasticsearch v14 (5e80472)", https://drive.google.com/drive/folders/1qzCiCL0x31uhbT9vrXlltj4sWUPML-RX (packet.html, packet.pdf, spec.md, CODEX-REVIEW-v14.md, APPROVAL.md).
- Published page: https://claude.ai/artifact/3WKE5dqGbWxqwL4zXTNURv (recording closed after approval).

Every implementation PR links this packet and build hash 5e80472. Each PR merges only on Daniel's separate merge word, risk at least medium.
