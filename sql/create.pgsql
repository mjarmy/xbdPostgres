create role xbd with password 's3crkEt';
alter role xbd with login;

create schema authorization xbd;
set search_path to xbd;

-- Xeto Specs
create table spec (
  id serial primary key,
  qname text unique,
  inherits_from text[] -- the qname of every spec that this spec inherits from, including itself
);
create index spec_inherits_from on spec using gin (inherits_from);

grant select, insert, update, delete on all tables in schema xbd to xbd;
