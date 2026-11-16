-- v2: every query in the snapshot (D1 A) form: current-rows branch union closed-rows branch, at a snapshot 20 minutes ago so both branches matter.
-- The full document (doc, about 1.1 KB) is fetched. Time windows use the range index through a materialized CTE.
create or replace function bench3(shape text, runs int) returns table(query_shape text, p50_ms numeric, p95_ms numeric, rows_avg numeric) language plpgsql as $$
declare t0 timestamptz; ms float8[] := '{}'; r int; tot int := 0; c float8; o float8; T timestamptz := now() - interval '20 minutes'; w text; ord text; sql text; cur text; old text;
begin
  for i in 1..runs loop
    c := 37.785 + ((i % 40) - 20) * 0.9 + (random()-0.5)*0.1; o := -122.43 + ((i % 40) - 20) * 1.7 + (random()-0.5)*0.1;
    w := 'true'; ord := 'uri';
    if shape = 'radius 2 km, by distance' then
      w := format('lat between %s and %s and arc_m(%s,%s,lat,lon) <= 2000', c-2000/111195.0, c+2000/111195.0, c, o); ord := format('arc_m(%s,%s,lat,lon), uri', c, o);
    elsif shape = 'radius 10 km and a week, by distance' then
      w := format('lat between %s and %s and arc_m(%s,%s,lat,lon) <= 10000 and span(starts_at, ends_at) && tstzrange(now(), now() + interval ''7 days'', ''[)'')', c-10000/111195.0, c+10000/111195.0, c, o); ord := format('arc_m(%s,%s,lat,lon), uri', c, o);
    elsif shape = 'a week, by start' then
      w := 'span(starts_at, ends_at) && tstzrange(now(), now() + interval ''7 days'', ''[)'')'; ord := 'starts_at, uri';
    elsif shape = 'a month, by start' then
      w := 'span(starts_at, ends_at) && tstzrange(now(), now() + interval ''30 days'', ''[)'')'; ord := 'starts_at, uri';
    elsif shape = 'box and a month, by start' then
      w := format('lat between %s and %s and lon between %s and %s and span(starts_at, ends_at) && tstzrange(now(), now() + interval ''30 days'', ''[)'')', c-0.1, c+0.1, o-0.15, o+0.15); ord := 'starts_at, uri';
    elsif shape = 'everything by distance (default with a point)' then
      ord := format('arc_m(%s,%s,lat,lon) nulls last, uri', c, o);
    elsif shape = 'later page by start (keyset after day 20)' then
      w := '(starts_at, uri) > (now() - interval ''20 days'', '''')'; ord := 'starts_at, uri';
    end if;
    cur := format('select * from event_docs where valid_to is null and valid_from <= %L and %s', T, w);
    old := format('select * from event_docs where valid_to > %L and valid_from <= %L and %s', T, T, w);
    if w like '%span(%' then
      sql := format('with m as materialized ((%s) union all (%s)) select uri, doc from m order by %s limit 51', cur, old, ord);
    else
      sql := format('select uri, doc from ((%s order by %s limit 51) union all (%s order by %s limit 51)) u order by %s limit 51', cur, ord, old, ord, ord);
    end if;
    t0 := clock_timestamp();
    execute 'select count(*) from (select length(doc::text) from (' || sql || ') q) s' into r;
    ms := ms || extract(epoch from clock_timestamp() - t0) * 1000;
    tot := tot + r;
  end loop;
  return query select shape, round(percentile_cont(0.5) within group (order by m)::numeric, 1), round(percentile_cont(0.95) within group (order by m)::numeric, 1), round(tot::numeric / runs, 1) from unnest(ms) m;
end $$;
select * from bench3('radius 2 km, by distance', 40)
union all select * from bench3('radius 10 km and a week, by distance', 40)
union all select * from bench3('a week, by start', 40)
union all select * from bench3('a month, by start', 20)
union all select * from bench3('box and a month, by start', 40)
union all select * from bench3('everything by distance (default with a point)', 20)
union all select * from bench3('later page by start (keyset after day 20)', 40);
