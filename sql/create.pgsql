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
  paths text[] not null,  -- indexed
  values_ jsonb not null, -- indexed
  refs jsonb not null,
  units jsonb not null,
  spec text -- nullable, no foreign key to spec(id), since it could be dangling
);
create index rec_paths on rec using gin (paths);
create index rec_values_ on rec using gin (values_ jsonb_path_ops);

-- pathref does Ref lookups via self-joins
create table pathref (
  rec_id text not null references rec (id),
  path_ text not null,
  ref_ text not null, -- no foreign key to rec(id), since it could be dangling
  constraint pathref_pkey primary key (rec_id, path_, ref_)
);
create index pathref_path_ref on pathref (path_, ref_);

--------------------------------------------------------------------------

create table foo (
  id text primary key,
  paths text[] not null,
  values_ jsonb not null
);
create index foo_bar on foo using gin (paths);
create index foo_values_ on foo using gin (values_ jsonb_path_ops);

--------------------------------------------------------------------------

grant select, insert, update, delete on all tables in schema xbd to xbd;
grant usage, select on all sequences in schema xbd to xbd;

