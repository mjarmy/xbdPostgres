
-- AirFlowSensor
explain analyze
select spec.qname from spec
where
  (spec.inherits_from @> '{"ph.points::AirFlowSensor"}');

-- Sensor
explain analyze
select spec.qname from spec
where
  (spec.inherits_from @> '{"ph::Sensor"}');

-------------------------------------------

-- id
explain analyze
select * from rec where id = 'h:2ca';

-- ids
explain analyze
select * from rec where id in ('a-0000','a-0011','a-0022');

-- ahu
explain analyze
select * from rec
where
  (rec.paths @> '{"ahu"}'::text[]);

-- facets->min
explain analyze
select * from rec
where
  (rec.paths @> '{"facets.min"}'::text[]);

-- chilledWaterRef->chilled
explain analyze
select * from rec
  inner join path_ref p1 on p1.source = rec.id
  inner join rec     r1 on r1.id     = p1.target
where
  (p1.path_ = 'chilledWaterRef') and
  (r1.paths @> '{"chilled"}'::text[]);

-- links->in4->fromRef->meta->inA->flags->linkTarget
explain analyze
select rec.* from rec
  inner join path_ref p1 on p1.source = rec.id
  inner join rec     r1 on r1.id     = p1.target
where
  (p1.path_ = 'links.in4.fromRef') and
  (r1.paths @> '{"meta.inA.flags.linkTarget"}'::text[]);

-- ahu and elec
explain analyze
select * from rec
where
  ((rec.paths @> '{"ahu"}'::text[]) and
  ((rec.paths @> '{"elec"}'::text[])));

-- chilled and pump and sensor and equipRef->siteRef->site
explain analyze
select rec.id from rec
  inner join path_ref p1 on p1.source = rec.id
  inner join rec     r1 on r1.id     = p1.target
  inner join path_ref p2 on p2.source = r1.id
  inner join rec     r2 on r2.id     = p2.target
where
  (rec.paths @> '{"chilled"}'::text[]) and
  (rec.paths @> '{"pump"}'::text[]) and
  (rec.paths @> '{"sensor"}'::text[]) and
  (p1.path_ = 'equipRef') and
  (p2.path_ = 'siteRef') and
  (r2.paths @> '{"site"}'::text[]);

-- dis == 'Alpha Airside AHU-4'
explain analyze
select * from rec
where
  (rec.values_ @> '{"dis":"Alpha Airside AHU-4"}'::jsonb);

-- custom->description == 'Clg_Valve_Cmd'
explain analyze
select * from rec
where
  (rec.values_ @> '{"custom": {"description":"Clg_Valve_Cmd"}}'::jsonb);

-- area == 151455
explain analyze
select * from rec
where
  (rec.values_ @> '{"area":151455}'::jsonb);

-- facets->min == -INF
--explain analyze
--select * from rec
--where
--  (rec.values_ @> '{"facets": {"min":{"val": "-INF", "_kind": "number"}}}'::jsonb);

-- chilled and pump and sensor and equipRef->siteRef->area == 151455
explain (analyze true, verbose true, buffers true)
select * from rec
  inner join path_ref p1 on p1.source = rec.id
  inner join rec     r1 on r1.id     = p1.target
  inner join path_ref p2 on p2.source = r1.id
  inner join rec     r2 on r2.id     = p2.target
where
  (rec.paths @> '{"chilled"}'::text[]) and
  (rec.paths @> '{"pump"}'::text[]) and
  (rec.paths @> '{"sensor"}'::text[]) and
  (p1.path_ = 'equipRef') and
  (p2.path_ = 'siteRef') and
  (r2.nums @> '{"area":151455}'::jsonb);

------------------------------------------------------------------
------------------------------------------------------------------
------------------------------------------------------------------

-- b < 2
explain (analyze true, verbose true, buffers true)
select rec.id from rec
where
  ((rec.paths @> '{"b"}'::text[]) and ((rec.hayson #> '{b}')::int > 2));

explain (analyze true, verbose true, buffers true)
select rec.id from rec
where
  ((rec.paths @> '{"c.d"}'::text[]) and ((rec.hayson #> '{c,d}')::int > 2));

------------------------------------------------------------------
------------------------------------------------------------------
------------------------------------------------------------------

select id, paths, hayson from rec
where (paths @> '{"extra"}'::text[]);
