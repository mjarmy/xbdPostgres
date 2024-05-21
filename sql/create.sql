create role xbd with password 's3crkEt';
alter role xbd with login;

create schema authorization xbd;
set search_path to xbd;

-- Recs
create table rec (
  id text primary key,
  brio bytea,
  paths text[] not null,

  refs      jsonb not null,
  strs      jsonb,
  nums      jsonb,
  units     jsonb,
  bools     jsonb,
  uris      jsonb,
  dates     jsonb,
  times     jsonb,
  dateTimes jsonb
);
create index rec_paths on rec using gin (paths jsonb_path_ops);

create index rec_refs      on rec using gin (refs      jsonb_path_ops);
create index rec_strs      on rec using gin (strs      jsonb_path_ops);
create index rec_nums      on rec using gin (nums      jsonb_path_ops);
create index rec_units     on rec using gin (units     jsonb_path_ops);
create index rec_bools     on rec using gin (bools     jsonb_path_ops);
create index rec_uris      on rec using gin (uris      jsonb_path_ops);
create index rec_dates     on rec using gin (dates     jsonb_path_ops);
create index rec_times     on rec using gin (times     jsonb_path_ops);
create index rec_dateTimes on rec using gin (dateTimes jsonb_path_ops);

-- Ref lookups via self-join
create table path_ref (
  source text not null references rec (id),
  path_  text not null,
  target text not null, -- no foreign key to rec(id), since it could be dangling
  constraint path_ref_pkey primary key (source, path_, target)
);
create index path_ref_path_target on path_ref (path_, target);

grant select, insert, update, delete on all tables in schema xbd to xbd;
grant usage, select on all sequences in schema xbd to xbd;

