-- create role xbd with login superuser password 's3crkEt';

select schema_name, schema_owner from information_schema.schemata;

drop schema test_proj cascade;
drop schema proj1 cascade;
drop schema proj2 cascade;

set search_path to test_proj;
set search_path to proj1;
set search_path to proj2;
