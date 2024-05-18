select 
  id, 
  --paths, 
  --refs,
  --strs,
  --nums,
  --units,
  --bools,
  --uris
  dates,
  times,
  dateTimes
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

select rec.id from rec
where
  (
    (rec.paths @> '{"haven"}'::text[])
    and
    ((rec.paths @> '{"nest.bar"}'::text[]) and ((rec.strs->>'nest.bar')::text < 'y'))
  );
