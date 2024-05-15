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
  brio bytea,
  paths text[] not null, -- the path to every key
  hayson jsonb not null, -- no markers, numbers have units stripped
  spec text -- nullable, no foreign key to spec(id), since it could be dangling
);
create index rec_paths on rec using gin (paths);
create index rec_hayson on rec using gin (hayson jsonb_path_ops);

-- pathref does Ref lookups via self-joins
create table pathref (
  source text not null references rec (id),
  path_ text not null,
  target text not null, -- no foreign key to rec(id), since it could be dangling
  constraint pathref_pkey primary key (source, path_, target)
);
create index pathref_path_target on pathref (path_, target);

grant select, insert, update, delete on all tables in schema xbd to xbd;
grant usage, select on all sequences in schema xbd to xbd;

