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

-- Recs
create table rec (
  id text primary key,
  hayson jsonb,
  spec_id int references spec (id)
);
create index rec_hayson on rec using gin (hayson);

grant select, insert, update, delete on all tables in schema xbd to xbd;
grant usage, select on all sequences in schema xbd to xbd;

