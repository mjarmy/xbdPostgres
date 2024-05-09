
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
  (rec.paths @> '{"ahu"}');

-- ahu and elec
explain analyze
select * from rec
where
  ((rec.paths @> '{"ahu"}') and
  ((rec.paths @> '{"elec"}')));

-- facets->min
explain analyze
select * from rec
where
  (rec.paths @> '{"facets.min"}');

-- facets->min == -INF
explain analyze
select * from rec
where
  (rec.values_ @> '{"facets.min":{"val": "-INF", "_kind": "number"}}'::jsonb);

-- compName == 'Services'
explain analyze
select * from rec
where
  (rec.values_ @> '{"compName":"Services"}'::jsonb);

-- dis == 'Alpha Airside AHU-4'
explain analyze
select * from rec
where
  (rec.values_ @> '{"dis":"Alpha Airside AHU-4"}'::jsonb);

-- chilledWaterRef->chilled
explain analyze
select * from rec
  inner join pathref p1 on p1.rec_id = rec.id
  inner join rec     r1 on r1.id     = p1.ref_
where
  (p1.path_ = 'chilledWaterRef') and
  (r1.paths @> '{"chilled"}');

-- links->in4->fromRef->meta->inA->flags->linkTarget
explain analyze
select * from rec
  inner join pathref p1 on p1.rec_id = rec.id
  inner join rec     r1 on r1.id     = p1.ref_
where
  (p1.path_ = 'links.in4.fromRef') and
  (r1.paths @> '{"meta.inA.flags.linkTarget"}');

-- chilled and pump and sensor and equipRef->siteRef->area == 151455
explain analyze
select * from rec
  inner join pathref p1 on p1.rec_id = rec.id
  inner join rec     r1 on r1.id     = p1.ref_
  inner join pathref p2 on p2.rec_id = r1.id
  inner join rec     r2 on r2.id     = p2.ref_
where
  (rec.paths @> '{"chilled"}') and
  (rec.paths @> '{"pump"}') and
  (rec.paths @> '{"sensor"}') and
  (p1.path_ = 'equipRef') and
  (p2.path_ = 'siteRef') and
  (r2.values_ @> '{"area":151455}'::jsonb);
