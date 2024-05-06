
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

explain analyze
select r.hayson from rec as r where (r.hayson ? 'point');
