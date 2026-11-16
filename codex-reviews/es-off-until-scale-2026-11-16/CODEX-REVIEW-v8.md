# Codex Astra review of v8 (build f476432), Codex round 2

Run on Daniel's Mac, Nov 16: pack at iriai-proto-index 47f7c88 (46 checksums OK), exit 0 on the first run. Source: /Users/danielzhang/src/codex-reviews/es-off-until-scale-2026-11-16-v8/codex-last-message.md (13,268 bytes). Summarised faithfully from the verbatim copy; links shortened.

STAKES: AGREE

9 should-fix findings and 2 nits; no blocker confirmed. Source review; reproductions not executed; PNG renders absent.

Round-1 fixes: C1 resolves the version mismatch but introduces findings 1–2. C2 corrected in the spec, contradicted by the packet's main explanation (6). C3 closes the race but its read-side latency is understated (3). C4 fixed. C5 fixed. C6 corrected in the spec, an unconditional packet claim remains (6). C7 fixed; numbers match spike3-results.txt. C8: compute vs storage distinguished, storage claim unsupported (5). C9 fixed. C10 partly fixed (7–8). C11 fixed. C12 improved, cutoff problem remains (10). Core SQL rules match the code; the watermark argument holds; no other document-loss or resurrection path found.

1. Should fix: rollback cannot create a fresh index twice on the same day (`placeproto-events-<yyyymmdd>`; spec 6.4, 5.4, 7). Give each build a unique generation id.
2. Should fix: deleting the old index immediately invalidates unexpired paging sessions (a PIT does not prevent index deletion). Keep the old index through draining, or disclose.
3. Should fix: holding the exclusive write lock across the Elasticsearch request (C3 fix) also blocks Postgres snapshot opening; 4.3's "milliseconds" promise breaks; read-side cost undisclosed.
4. Should fix: zero-traffic days have no shadow p95, so the D2 A switch-back can never be met and a quiet Elasticsearch stays on, billing. Define unsampled days.
5. Should fix: "at most two indexes, under $0.50 a month" does not follow: failed builds leave candidates; two indexes do not bound size. Specify abandoned-index cleanup; present disk exposure with assumptions and acceptance.
6. Should fix: packet part 4 still says single-event lookups "always read" Postgres and a failed Elasticsearch write "no longer holds up ingestion", contradicting spec 3 and 5.1.
7. Should fix: D2 Hold moots D3, yet PR 1's initial switch-off depends on D3's mechanism.
8. Should fix: D1 B leaves "they should notice nothing" (part 2) and "Nothing visible" (part 8) outside decision points.
9. Should fix: the rollout omits the separate merge gate and a risk rating.
10. Nit: positions 1–45 still rejects an allowed swap at 45/46; match against the buffered results.
11. Nit: spike `arc_m` is not STRICT; `least(1, NULL)` returns 1, so a null point gets the antipodal distance.

REQUEST CHANGES: v8 fixes the original version mismatch, but rebuild recovery, paging preservation, controller empty states, cost claims and the approval packet still need correction.
