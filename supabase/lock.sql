-- Lock guesses at 22:00 Israel time on election day (polls close).
-- After the lock: guesses, guessers and parties are read-only; results stay editable.
create or replace function public.lock_at()
returns timestamptz
language sql
stable
as $$ select timestamptz '2026-10-27 22:00:00 Asia/Jerusalem' $$;
grant execute on function public.lock_at() to anon, authenticated;

create or replace function public.set_guess(g text, p text, v int)
returns void
language plpgsql
security invoker
set search_path = public
as $$
begin
  if now() >= public.lock_at() then
    raise exception 'guesses are locked' using errcode = 'P0001', hint = 'locked';
  end if;
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

do $$
declare t text;
begin
  foreach t in array array['parties', 'guessers'] loop
    execute format('drop policy if exists "public insert" on public.%I', t);
    execute format('drop policy if exists "public update" on public.%I', t);
    execute format('drop policy if exists "public delete" on public.%I', t);
    execute format('create policy "public insert" on public.%I for insert to anon, authenticated with check (now() < public.lock_at())', t);
    execute format('create policy "public update" on public.%I for update to anon, authenticated using (now() < public.lock_at()) with check (now() < public.lock_at())', t);
    execute format('create policy "public delete" on public.%I for delete to anon, authenticated using (now() < public.lock_at())', t);
  end loop;
end $$;

notify pgrst, 'reload schema';
