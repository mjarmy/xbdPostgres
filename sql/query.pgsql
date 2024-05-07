
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

explain analyze
select r.hayson from rec as r where (r.hayson ? 'point');

explain analyze
select r.hayson from rec as r where (r.hayson @> '{"compName":"Services"}'::jsonb);

explain analyze
select r.hayson from rec as r where ((r.hayson ? 'links') and (r.hayson ? 'point'));

-------------------------------------------

explain analyze
select r.hayson->'facets' from rec as r where (r.hayson ? 'facets');

select r.hayson->'facets' from rec as r where (r.hayson ? 'facets.min');


explain analyze
select r.hayson->'facets' from rec as r where (r.hayson @? '$.facets.min');


select r.id from rec as r where (r.hayson ? 'facets.min');

explain analyze
select r.hayson->'ntpl_3avendor' from rec as r where (r.hayson ? 'ntpl_3avendor');

-------------------------------------------

explain
select r.hayson from rec as r where (r.hayson @? '$.ntpl_3avendor');

explain
select r.hayson from rec as r where (r.hayson @> '{"ntpl_3avendor":"Tridium"}'::jsonb);

