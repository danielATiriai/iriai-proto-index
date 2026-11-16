-- :n rows; clustered in 40 metros, 20% spread worldwide; 10% have an older closed version
insert into event_docs (uri, did, event_uri, doc, lat, lon, starts_at, ends_at, valid_from, valid_to)
select 'at://did:plc:' || md5((g % 5000)::text) || '/app.iriai.proto.event/' || lpad(g::text, 13, '0'),
       'did:plc:' || md5((g % 5000)::text),
       'at://did:plc:' || md5((g % 5000)::text) || '/community.lexicon.calendar.event/' || g,
       jsonb_build_object('event', jsonb_build_object('name', 'Event ' || g, 'description', repeat('x', 400), 'startsAt', now()), 'location', jsonb_build_object('lat', 0, 'lon', 0)),
       case when random() < 0.95 then lat else null end, lon,
       s, s + make_interval(hours => 1 + (random()*5)::int),
       now() - interval '2 days', null
from (
  select g,
    case when random() < 0.8 then m.lat + (random() - 0.5) * 0.3 else (random() * 140 - 70) end as lat,
    case when random() < 0.8 then m.lon + (random() - 0.5) * 0.3 else (random() * 360 - 180) end as lon,
    now() - interval '180 days' + random() * interval '365 days' as s
  from generate_series(1, :n) g
  cross join lateral (select 37.785 + ((g % 40) - 20) * 0.9 as lat, -122.43 + ((g % 40) - 20) * 1.7 as lon) m
) x;
-- 10% get a superseded older version kept for snapshots
insert into event_docs (uri, did, event_uri, doc, lat, lon, starts_at, ends_at, valid_from, valid_to)
select uri, did, event_uri, doc, lat, lon, starts_at, ends_at, valid_from - interval '1 day', valid_from from event_docs where id % 10 = 0;
create unique index event_docs_current on event_docs (uri) where valid_to is null;
create index event_docs_starts on event_docs (starts_at, uri) where valid_to is null;
create index event_docs_lat on event_docs (lat, lon) where valid_to is null and lat is not null;
create index event_docs_old on event_docs (valid_to) where valid_to is not null;
create index event_docs_did on event_docs (did) where valid_to is null;
analyze event_docs;
