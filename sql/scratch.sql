  --paths, 
  --refs,
  --strs,
  --nums,
  --units,
  --bools,
  --uris
  --dates,
  --times,
  --dateTimes
select id, nums, units
from rec where (paths @> '{"haven"}'::text[]);

select rec.id from rec
where
  (
    (rec.paths @> '{"haven"}'::text[])
    and
    (rec.strs @> '{"nest.bar":"y"}'::jsonb)
  );

select rec.id from rec
where
  (
    (rec.paths @> '{"haven"}'::text[])
    and
    ((rec.paths @> '{"nest.bar"}'::text[]) and ((rec.strs #> '{nest.bar}')::text < 'y'))
  );

select rec.id from rec
where
  (
    (rec.paths @> '{"haven"}'::text[])
    and
    ((rec.paths @> '{"nest.bar"}'::text[]) and ((rec.strs->'nest.bar')::text < 'y'))
  );

select 
  rec.id, 
  rec.strs->'nest.bar' as val,
  (rec.strs @> '{"nest.bar":"y"}'::jsonb) as eq,
  ((rec.strs->>'nest.bar')::text < 'y') as lt
from rec
where
    (rec.paths @> '{"nest.bar"}'::text[]);

select rec.id, rec.strs from rec
where
  (
    (rec.paths @> '{"haven"}'::text[])
    and
    ((rec.paths @> '{"nest.bar"}'::text[]) and ((rec.strs->>'nest.bar')::text < 'y'))
  );

select rec.id, rec.nums from rec
where
  (
    (rec.paths @> '{"haven"}'::text[])
    and
    ((rec.paths @> '{"nest.bar"}'::text[]) and ((rec.nums->>'nest.bar')::real < 2))
  );

-----------------------------------

-- str eq
explain (analyze true, verbose true, buffers true)
select rec.id from rec
where
    (rec.strs @> '{"links.inA.fromSlot":"out"}'::jsonb);

-- str le
explain (analyze true, verbose true, buffers true)
select rec.id from rec
where
    (rec.paths @> '{"links.inA.fromSlot"}'::text[]) 
    and 
    ((rec.strs->>'links.inA.fromSlot')::text < 'out');

-----------------------------------

-- num eq Niagara, needs units
explain (analyze true, verbose true, buffers true)
select rec.id from rec
where
    (rec.nums @> '{"inA.value":68.0}'::jsonb);

-- num le Niagara, needs units
explain (analyze true, verbose true, buffers true)
select rec.id, rec.nums from rec
where
    (rec.paths @> '{"inA.value"}'::text[]) 
    and 
    ((rec.nums->>'inA.value')::real < 67.0);

-- num eq 
explain (analyze true, verbose true, buffers true)
select rec.id from rec
where
    (rec.paths @> '{"haven"}'::text[])
    and
    ((rec.nums @> '{"num":2}'::jsonb) and (rec.units @> '{"num":null}'::jsonb));

explain (analyze true, verbose true, buffers true)
select rec.id from rec
where
    (rec.paths @> '{"haven"}'::text[])
    and
    ((rec.nums @> '{"num":2}'::jsonb) and (rec.units @> '{"num":"m"}'::jsonb));
