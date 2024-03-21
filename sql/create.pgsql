create role xbd with password 's3crkEt';
alter role xbd with login;

create schema authorization xbd;
set search_path to xbd;

-- Xeto Specs
create table spec (
  id serial primary key,
  qname text unique,
  -- inherits_from contains the qname of every spec that this spec inherits
  -- from, including itself
  inherits_from text[]
);
create index spec_inherits_from on spec using gin (inherits_from);

-- Objects
create table obj (
  id text primary key,
  tags text[],
  -- spec_id is nullable, since not every object has a spec
  spec_id int references spec (id)
);
create index obj_tags on obj using gin (tags);

grant select, insert, update, delete on all tables in schema xbd to xbd;
grant usage, select on all sequences in schema xbd to xbd;

