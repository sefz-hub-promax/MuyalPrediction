-- Family Election Guess — Supabase schema.
-- Run once in the Supabase SQL editor (safe to re-run).

create table if not exists public.parties (
  id         text primary key,
  name       text   not null default '',
  ord        bigint not null default 0,
  prev_2022  int    null check (prev_2022 between 0 and 120),
  prev_note  text   not null default '',
  leader     text   not null default '',
  running    boolean not null default true
);
alter table public.parties add column if not exists leader text not null default '';
alter table public.parties add column if not exists running boolean not null default true;

-- one row per guesser (= one column in the table); vals = {party_id: seats}
create table if not exists public.guessers (
  id    text primary key,
  name  text   not null default '',
  ord   bigint not null default 0,
  vals  jsonb  not null default '{}'::jsonb
);

-- single row 'main'; vals = {party_id: seats}
create table if not exists public.results (
  id    text primary key default 'main',
  vals  jsonb not null default '{}'::jsonb
);
insert into public.results (id) values ('main') on conflict (id) do nothing;

-- ---------- one-cell atomic writes ----------
-- A single UPDATE row-locks and recomputes vals from the latest version,
-- so two people editing different cells of the same column never clobber each other.

create or replace function public.set_guess(g text, p text, v int)
returns void
language plpgsql
security invoker
set search_path = public
as $$
begin
  if v is not null and (v < 0 or v > 120) then
    raise exception 'seats out of range: %', v using errcode = '22003';
  end if;
  update public.guessers
     set vals = case when v is null then vals - p
                     else jsonb_set(vals, array[p], to_jsonb(v), true) end
   where id = g;
  if not found then
    raise exception 'guesser not found: %', g using errcode = 'P0002';
  end if;
end $$;

create or replace function public.set_result(p text, v int)
returns void
language plpgsql
security invoker
set search_path = public
as $$
begin
  if v is not null and (v < 0 or v > 120) then
    raise exception 'seats out of range: %', v using errcode = '22003';
  end if;
  insert into public.results as r (id, vals)
       values ('main', case when v is null then '{}'::jsonb else jsonb_build_object(p, v) end)
  on conflict (id) do update
     set vals = case when v is null then r.vals - p
                     else jsonb_set(r.vals, array[p], to_jsonb(v), true) end;
end $$;

-- ---------- access: intentionally public family game, no login ----------
grant usage on schema public to anon, authenticated;
grant select, insert, update, delete on public.parties, public.guessers, public.results to anon, authenticated;
grant execute on function public.set_guess(text, text, int)  to anon, authenticated;
grant execute on function public.set_result(text, int)       to anon, authenticated;

alter table public.parties  enable row level security;
alter table public.guessers enable row level security;
alter table public.results  enable row level security;

do $$
declare t text;
begin
  foreach t in array array['parties', 'guessers', 'results'] loop
    execute format('drop policy if exists "public select" on public.%I', t);
    execute format('drop policy if exists "public insert" on public.%I', t);
    execute format('drop policy if exists "public update" on public.%I', t);
    execute format('drop policy if exists "public delete" on public.%I', t);
    execute format('create policy "public select" on public.%I for select to anon, authenticated using (true)', t);
    execute format('create policy "public insert" on public.%I for insert to anon, authenticated with check (true)', t);
    execute format('create policy "public update" on public.%I for update to anon, authenticated using (true) with check (true)', t);
    execute format('create policy "public delete" on public.%I for delete to anon, authenticated using (true)', t);
  end loop;
end $$;

-- ---------- realtime ----------
do $$
declare t text;
begin
  foreach t in array array['parties', 'guessers', 'results'] loop
    if not exists (
      select 1 from pg_publication_tables
       where pubname = 'supabase_realtime' and schemaname = 'public' and tablename = t
    ) then
      execute format('alter publication supabase_realtime add table public.%I', t);
    end if;
  end loop;
end $$;
