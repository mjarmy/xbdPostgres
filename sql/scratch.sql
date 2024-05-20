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
select id, paths, nums, units
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
select rec.id, rec.nums from rec
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

select rec.brio from rec
where
  (
    ((rec.paths @> @x1::text[]) 
      and ((rec.strs->>@x2)::text < @x3))
  );

---------------------------------------

-- haven and b == `https://project-haystack.org/`"
explain (analyze true, verbose true, buffers true)
select 
  rec.id,
  rec.paths, 
  rec.strs,
  rec.uris
from rec
where
  (
    (rec.paths @> '{"haven"}'::text[])
    and
    (rec.uris @> '{"b":"https://project-haystack.org/"}'::jsonb)
  );

-- haven and b != `https://project-haystack.org/`"
explain (analyze true, verbose true, buffers true)
select 
  rec.id,
  rec.paths, 
  rec.strs,
  rec.uris
from rec
where
  (
    (rec.paths @> '{"haven"}'::text[])
    and
    (
      (
        -- we need to actually have the path
        (rec.paths @> '{"b"}'::text[]) 
        and 
        (
          -- either there is no uri
          (rec.uris is null)
          or
          -- or its the wrong uri
          (not (rec.uris @> '{"b":"https://project-haystack.org/"}'::jsonb))
        )
      )
    )
  );

explain (analyze true, verbose true, buffers true)
select rec.id from rec
where
  (
    (rec.paths @> '{"haven"}'::text[])
    and
    ((rec.paths @> '{"num"}'::text[]) and ((rec.nums->'num')::real < 2.0) and (rec.units @> '{"num":"\u00b0F"}'::jsonb))
  );

---------------------------------------

select rec.id, rec.paths, rec.strs from rec
where
  (
    (rec.paths @> '{"haven"}'::text[])
    and
    ((rec.paths @> '{"str"}'::text[]) and (rec.strs->>'str' > 'y'))
  );

select 
  rec.id, 
  rec.paths, 
  rec.nums,
  rec.units,
  pg_typeof(rec.nums->'num') as a,
  pg_typeof((rec.nums->'num')::real) as b,
  (((rec.nums->'num')::real) < 2) as x // <------------------
from rec
where
  (
    (rec.paths @> '{"haven"}'::text[])
    and
    (rec.paths @> '{"num"}'::text[])
  );


