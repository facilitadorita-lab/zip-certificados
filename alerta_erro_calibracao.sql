alter table public.certificados
  add column if not exists erro_acima_1c boolean
  generated always as (
    jsonb_path_exists(coalesce(pontos, '[]'::jsonb), '$[*] ? (@.erro > 1 || @.erro < -1)')
  ) stored;
