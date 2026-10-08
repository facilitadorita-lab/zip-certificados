create or replace view public.certificados_atuais_dlt with (security_invoker = true) as
select distinct on (public.normalizar_logger_tag(c.dlt)) c.*
from public.certificados c
where public.normalizar_logger_tag(c.dlt) <> ''
order by public.normalizar_logger_tag(c.dlt), c.data desc nulls last, c.criado_em desc nulls last, c.id collate "C" desc;

create or replace view public.certificados_atuais_dlh with (security_invoker = true) as
select distinct on (public.normalizar_logger_tag(c.dlh)) c.*
from public.certificados_dlh c
where public.normalizar_logger_tag(c.dlh) <> ''
order by public.normalizar_logger_tag(c.dlh), c.data desc nulls last, c.criado_em desc nulls last, c.id collate "C" desc;

create table public.qualidade_pareceres (
  id bigint generated always as identity primary key,
  modulo text not null check (modulo in ('DLT','DLH')),
  certificado_id text not null,
  versao text not null,
  parecer text not null check (length(trim(parecer)) between 5 and 4000),
  responsavel_id uuid not null,
  responsavel_email text not null,
  criado_em timestamptz not null default now()
);
create index on public.qualidade_pareceres (modulo, certificado_id, versao);
alter table public.qualidade_pareceres enable row level security;
revoke all on public.qualidade_pareceres from anon, authenticated;
grant select, insert on public.qualidade_pareceres to service_role;
grant usage on sequence public.qualidade_pareceres_id_seq to service_role;

create or replace view public.qualidade_ocorrencias with (security_invoker = true) as
with registros as (
  select 'DLT'::text modulo, c.id, c.dlt logger, c.certificado, c.nome_original, c.status,
    c.data, c.vencimento, c.divergente, c.motivo_divergencia,
    coalesce(c.pontos,'[]'::jsonb) pontos_temperatura, '[]'::jsonb pontos_umidade
  from public.certificados_atuais_dlt c
  union all
  select 'DLH', c.id, c.dlh, c.certificado, c.nome_original, c.status,
    c.data, c.vencimento, c.divergente, c.motivo_divergencia,
    coalesce(c.pontos_temperatura,'[]'::jsonb), coalesce(c.pontos_umidade,'[]'::jsonb)
  from public.certificados_atuais_dlh c
), motivos as (
  select r.*, array_remove(array[
    case when upper(status) = 'REPROVADO' then 'Certificado reprovado' end,
    case when upper(status) = 'ERRO' then 'Leitura inconsistente' end,
    case when divergente then 'Divergencia cadastral' end,
    case when jsonb_path_exists(pontos_temperatura,'$[*] ? (@.erro > 1 || @.erro < -1)') then 'Erro de temperatura acima de 1 C' end
  ], null) motivos,
  md5(jsonb_build_array(certificado,data,vencimento,status,divergente,motivo_divergencia,pontos_temperatura,pontos_umidade)::text) versao
  from registros r
)
select m.*, exists (
  select 1 from public.qualidade_pareceres p where p.modulo=m.modulo and p.certificado_id=m.id and p.versao=m.versao
) avaliado from motivos m where cardinality(m.motivos)>0;

create table public.processamento_falhas (
  modulo text not null check (modulo in ('DLT','DLH')),
  arquivo_id text not null,
  nome_arquivo text not null,
  categoria text not null,
  mensagem text not null,
  tentativas integer not null default 1,
  primeira_falha timestamptz not null default now(),
  ultima_falha timestamptz not null default now(),
  resolvido_em timestamptz,
  primary key (modulo,arquivo_id)
);
alter table public.processamento_falhas enable row level security;
revoke all on public.processamento_falhas from anon, authenticated;
grant select, insert, update on public.processamento_falhas to service_role;

create function public.registrar_falha_processamento(p_modulo text,p_id text,p_nome text,p_categoria text,p_mensagem text)
returns void language sql security invoker set search_path=public as $$
  insert into public.processamento_falhas(modulo,arquivo_id,nome_arquivo,categoria,mensagem)
  values(p_modulo,p_id,p_nome,p_categoria,left(p_mensagem,1000))
  on conflict(modulo,arquivo_id) do update set
    nome_arquivo=excluded.nome_arquivo,categoria=excluded.categoria,mensagem=excluded.mensagem,
    tentativas=processamento_falhas.tentativas+1,ultima_falha=now(),resolvido_em=null;
$$;
revoke all on function public.registrar_falha_processamento(text,text,text,text,text) from public,anon,authenticated;
grant execute on function public.registrar_falha_processamento(text,text,text,text,text) to service_role;
revoke all on public.certificados_atuais_dlt,public.certificados_atuais_dlh,public.qualidade_ocorrencias from anon,authenticated;
grant select on public.certificados_atuais_dlt,public.certificados_atuais_dlh,public.qualidade_ocorrencias to service_role;

-- A consulta com token usa a mesma selecao que dashboard e listas atuais.
insert into public.app_permissoes(chave,nome,descricao,grupo,ordem) values
('qualidade.visualizar','Ver avaliacao tecnica','Consultar ocorrencias e pareceres de calibracao','Qualidade',1),
('qualidade.avaliar','Registrar parecer tecnico','Concluir avaliacao tecnica com parecer e responsavel','Qualidade',2)
on conflict(chave) do nothing;
do $$
declare definicao text;
begin
  select pg_get_functiondef('public.obter_certificado_logger_rapido(text,text,text,date,date)'::regprocedure) into definicao;
  definicao := replace(definicao,'from public.certificados c','from public.certificados_atuais_dlt c');
  definicao := replace(definicao,'from public.certificados_dlh c','from public.certificados_atuais_dlh c');
  execute definicao;
  select pg_get_functiondef('public.listar_alertas_vencimentos(integer,integer)'::regprocedure) into definicao;
  definicao := replace(definicao,'from public.certificados c','from public.certificados_atuais_dlt c');
  definicao := replace(definicao,'from public.certificados_dlh c','from public.certificados_atuais_dlh c');
  execute definicao;
end $$;

insert into public.processamento_falhas(modulo,arquivo_id,nome_arquivo,categoria,mensagem)
select 'DLH',id,coalesce(nome_original,id),'leitura',left(coalesce(motivo_divergencia,'Falha na leitura do certificado.'),1000)
from public.certificados_dlh where upper(status)='ERRO'
on conflict(modulo,arquivo_id) do nothing;
