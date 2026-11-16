-- v2: as spike-load.sql (in this folder), then recent churn: 1% of events edited 0 to 30 minutes ago (closed row kept), so the closed-rows branch has work
\ir spike-load.sql
create or replace function span(s timestamptz, e timestamptz) returns tstzrange language sql immutable parallel safe as $$
  select case when s is null then null when e is null then tstzrange(s, s, '[]') when e > s then tstzrange(s, e, '[)') else null end $$;
create temp table churn as select id, now() - random() * interval '30 minutes' as t from event_docs where valid_to is null and id % 100 = 7;
insert into event_docs (uri, did, event_uri, doc, lat, lon, starts_at, ends_at, valid_from, valid_to)
  select e.uri, e.did, e.event_uri, e.doc, e.lat, e.lon, e.starts_at, e.ends_at, e.valid_from, c.t from event_docs e join churn c using (id);
update event_docs e set valid_from = c.t, starts_at = e.starts_at + interval '1 hour', ends_at = e.ends_at + interval '1 hour' from churn c where e.id = c.id;
create index event_docs_span on event_docs using gist (span(starts_at, ends_at)) where valid_to is null;
create index event_docs_span_old on event_docs using gist (span(starts_at, ends_at)) where valid_to is not null;
analyze event_docs;
