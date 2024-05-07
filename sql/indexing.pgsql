drop table if exists foo;
create table foo (
  id text primary key,
  hayson jsonb
);
create index foo_hayson on foo using gin (hayson jsonb_path_ops);
grant select, insert, update, delete on all tables in schema xbd to xbd;
grant usage, select on all sequences in schema xbd to xbd;

delete from foo;

explain
select r.hayson from foo as r where (r.hayson @? '$.foo');

explain
select r.hayson from foo as r where (r.hayson @> '{"ntpl_3avendor":"Tridium"}'::jsonb);
