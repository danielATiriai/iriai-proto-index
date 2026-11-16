drop table if exists event_docs;
create table event_docs (
  id bigserial primary key,
  uri text collate "C" not null,
  did text not null,
  event_uri text not null,
  doc jsonb not null,
  lat double precision,
  lon double precision,
  starts_at timestamptz,
  ends_at timestamptz,
  time_valid boolean not null default true,
  valid_from timestamptz not null default now(),
  valid_to timestamptz
);
create or replace function arc_m(lat1 float8, lon1 float8, lat2 float8, lon2 float8) returns float8
language sql immutable strict parallel safe as $$
  select 2 * 6371008.7714 * asin(least(1, sqrt(
    power(sin(radians(lat2 - lat1) / 2), 2) +
    cos(radians(lat1)) * cos(radians(lat2)) * power(sin(radians(lon2 - lon1) / 2), 2))))
$$;
