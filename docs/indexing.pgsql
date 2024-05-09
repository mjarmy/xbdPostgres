drop table if exists foo;
create table foo (
  id text primary key,
  paths text[],
  obj jsonb
);
create index foo_paths on foo using gin (paths);
create index foo_obj on foo using gin (obj jsonb_path_ops);

grant select, insert, update, delete on all tables in schema xbd to xbd;
grant usage, select on all sequences in schema xbd to xbd;

delete from foo;

--------------------------------------------------------

explain analyze
select obj from foo where (obj ? 'a');

explain analyze
select obj from foo where (obj @? '$.a.a.a.a'::jsonpath);

explain analyze select obj from foo where (obj @> '{"a": {"a": {"a": {"a": 1}}}}'::jsonb);

explain analyze
select obj from foo where (obj @@ '$.a.a.a.a == 1'::jsonpath);

explain analyze select obj from foo where (obj @? '$.a.a.a.a'::jsonpath);

--------------------------------------------------------

explain analyze
select obj from foo where (obj @> '{"a": {"a": {"a": {"a": 1}}}}'::jsonb);

explain analyze
select obj from foo where (paths @> '{"a.a.a.a"}');
