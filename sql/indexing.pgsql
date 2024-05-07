drop table if exists foo;
create table foo (
  id text primary key,
  hayson jsonb
);
create index foo_hayson on foo using gin (hayson jsonb_path_ops);

delete from foo;
insert into foo (id, hayson)
select format('%s', i), format('{"foo": %s, "bar": {"quux": %s}}', i, i)::jsonb
from generate_series(1, 100000) i;

explain
select r.hayson from foo as r where (r.hayson @? '$.foo');

select r.hayson from foo as r where (r.hayson @> '{"ntpl_3avendor":"Tridium"}'::jsonb);
