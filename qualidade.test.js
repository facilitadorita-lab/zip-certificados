import test from 'node:test';
import assert from 'node:assert/strict';
import { criarQualidade, classificarFalha } from './qualidade.js';

function ambiente(modulo='DLT', respostas=[]) {
  const rotas=new Map(), chamadas=[];
  const app={get:(p,f)=>rotas.set(`GET ${p}`,f),post:(p,f)=>rotas.set(`POST ${p}`,f)};
  const servico=criarQualidade({app,modulo,url:'https://banco.test',headers:()=>({}),
    fetch:async(url,opts)=>{chamadas.push({url,opts});return {ok:true,status:200,json:async()=>respostas.shift() ?? null};},
    tentarNovamente:async id=>chamadas.push({retry:id})});
  async function invocar(rota,req={}) {
    const res={code:200,body:null,status(n){this.code=n;return this;},json(v){this.body=v;return this;},setHeader(){}};
    await rotas.get(rota)({query:{},params:{id:'cert-1'},...req},res);return res;
  }
  return {invocar,chamadas,servico};
}
const auth={user:{id:'usuario-confiavel',email:'tecnico@test.invalid'},perfil:{role:'administrador'}};
test('separa credito, indisponibilidade e leitura',()=>{
  assert.equal(classificarFalha('insufficient_quota'),'credito_ia');
  assert.equal(classificarFalha('HTTP 503'),'indisponibilidade');
  assert.equal(classificarFalha('tempo esgotado'),'tempo_esgotado');
  assert.equal(classificarFalha('tabela incompleta'),'leitura');
});
test('parecer exige autenticacao e permissao de avaliacao',async()=>{
  const a=ambiente();
  assert.equal((await a.invocar('POST /qualidade/:id/parecer',{body:{parecer:'Avaliado'}})).code,401);
  assert.equal((await a.invocar('POST /qualidade/:id/parecer',{auth:{...auth,perfil:{role:'auditor',permissoes:['qualidade.visualizar']}},body:{parecer:'Avaliado'}})).code,403);
  assert.equal(a.chamadas.length,0);
});
test('mudanca no certificado impede parecer em dados antigos',async()=>{
  const a=ambiente('DLT',[[{id:'cert-1',versao:'nova'}]]);
  const res=await a.invocar('POST /qualidade/:id/parecer',{auth,body:{parecer:'Avaliado por tecnico',versao:'antiga'}});
  assert.equal(res.code,409);assert.equal(a.chamadas.length,1);
});
test('responsavel vem da sessao e parecer nao altera status do certificado',async()=>{
  const a=ambiente('DLH',[[{id:'cert-1',versao:'v1'}]]);
  const res=await a.invocar('POST /dlh/qualidade/:id/parecer',{auth,body:{parecer:'Enviar para manutencao.',versao:'v1',responsavel_id:'forjado'}});
  assert.equal(res.code,200);
  const gravacao=a.chamadas[1];assert.match(gravacao.url,/qualidade_pareceres$/);
  const dados=JSON.parse(gravacao.opts.body);assert.equal(dados.responsavel_id,auth.user.id);assert.equal(dados.modulo,'DLH');
  assert.ok(!('status' in dados));
});
test('nova tentativa se restringe ao arquivo e modulo e exige resultado resolvido',async()=>{
  const a=ambiente('DLH',[[{arquivo_id:'arquivo/1'}],[{resolvido_em:'2026-10-08T12:00:00Z'}]]);
  const res=await a.invocar('POST /dlh/processamento/falhas/:id/tentar',{auth});
  assert.equal(res.code,200);assert.equal(a.chamadas[1].retry,'arquivo/1');
  assert.match(a.chamadas[0].url,/modulo=eq.DLH/);
});
test('falha persistente nao informa sucesso',async()=>{
  const a=ambiente('DLT',[[{arquivo_id:'a'}],[{resolvido_em:null,mensagem:'Sem credito'}]]);
  const res=await a.invocar('POST /processamento/falhas/:id/tentar',{auth});
  assert.equal(res.code,422);assert.equal(res.body.erro,'Sem credito');
});
