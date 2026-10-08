alter table public.qualidade_pareceres add column snapshot jsonb not null default '{}'::jsonb;

-- Corrige a validacao de datas ISO da consulta de movimentacao.
do $$
declare definicao text;
begin
  select pg_get_functiondef('public.obter_certificado_logger_rapido(text,text,text,date,date)'::regprocedure) into definicao;
  definicao := replace(definicao,chr(92)||chr(92)||'d','[0-9]');
  execute definicao;
end $$;
