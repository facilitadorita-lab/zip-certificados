-- Execute uma vez no Supabase do projeto CERTIFICADOS.
-- O backend usa esta tabela para guardar layouts aprendidos pela IA.
create table if not exists public.parser_layouts_dlh (
  id uuid primary key default gen_random_uuid(),
  assinatura text not null unique,
  nome text not null,
  estrategia text not null default 'colunas_posicionadas',
  config_json jsonb not null default '{}'::jsonb,
  origem text not null default 'IA',
  modelo_ia text,
  ativo boolean not null default true,
  confianca numeric(5,4) not null default 0.8500,
  sucessos integer not null default 0,
  falhas integer not null default 0,
  ultima_utilizacao timestamptz,
  criado_em timestamptz not null default now(),
  atualizado_em timestamptz not null default now()
);

create index if not exists parser_layouts_dlh_ativo_assinatura_idx
  on public.parser_layouts_dlh (ativo, assinatura);

alter table public.parser_layouts_dlh enable row level security;
