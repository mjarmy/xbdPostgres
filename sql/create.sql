create role xbd with password 's3crkEt';
alter role xbd with login;

create schema authorization xbd;
set search_path to xbd;

-- Recs
create table rec (
  id text primary key,
  brio bytea,            -- the brio encoding of the dict
  paths text[] not null, -- the path to every tag
  refs  jsonb  not null  -- refs
);
create index rec_paths on rec using gin (paths);
create index rec_refs on rec using gin (refs jsonb_path_ops);

-- Ref lookups via self-join
--create table path_ref (
--  source text not null references rec (id),
--  path_  text not null,
--  target text not null, -- no foreign key to rec(id), since it could be dangling
--  constraint path_ref_pkey primary key (source, path_, target)
--);
--create index path_ref_path_target on path_ref (path_, target);

grant select, insert, update, delete on all tables in schema xbd to xbd;
grant usage, select on all sequences in schema xbd to xbd;

