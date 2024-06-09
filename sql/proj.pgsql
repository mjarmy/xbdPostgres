-- create role xbd with login superuser password 's3crkEt';

select schema_name, schema_owner from information_schema.schemata;

select exists (
  select schema_name from information_schema.schemata
  where schema_name = 'test_proj');

select schema_name, schema_owner from information_schema.schemata;

create schema test_proj;

set search_path to test_proj;

select exists (
   select from pg_tables
   where  schemaname = 'test_proj'
   and    tablename  = 'rec');

drop schema test_proj cascade;
drop schema proj1 cascade;
drop schema proj2 cascade;
