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
  -- A rec does not necessarilly have a spec
  spec_id int references spec (id)
);
create index rec_hayson on rec using gin (hayson);

-- Arrow is a bridge table for self-joins
create table arrow (
  from_id text not null references rec (id),
  to_path text,
  -- could be dangling...
  -- to_id text not null references rec (id),
  to_id text,
  constraint arrow_pkey primary key (from_id, to_path, to_id)
);
--create index arrow_tag_to on arrow (tag, to_path, to_id);

grant select, insert, update, delete on all tables in schema xbd to xbd;
grant usage, select on all sequences in schema xbd to xbd;

