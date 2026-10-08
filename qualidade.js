export function classificarFalha(mensagem) {
  const texto = String(mensagem || 'Falha desconhecida');
  if (/quota|credit|billing|saldo|insufficient/i.test(texto)) return 'credito_ia';
  if (/timeout|tempo.*esgot|abort/i.test(texto)) return 'tempo_esgotado';
  if (/fetch|network|ECONN|503|502|429|unavailable/i.test(texto)) return 'indisponibilidade';
  if (/banco|supabase|gravar/i.test(texto)) return 'banco';
  return 'leitura';
}

export function criarQualidade({ app, modulo, url, headers, fetch, tentarNovamente }) {
  const prefixo = modulo === 'DLH' ? '/dlh' : '';
  async function banco(recurso, opts = {}) {
    const resposta = await fetch(`${url}/rest/v1/${recurso}`, { ...opts, headers: { ...headers(), ...opts.headers } });
    if (!resposta.ok) throw new Error(`Falha ao acessar avaliacao tecnica (${resposta.status})`);
    return resposta.status === 204 ? null : resposta.json();
  }
  async function registrarFalha(arquivo, erro) {
    try {
      const mensagem = String(erro?.message || erro || 'Falha de leitura')
        .replace(/sk-[a-zA-Z0-9_-]+/g, '[chave omitida]').slice(0, 1000);
      await banco('rpc/registrar_falha_processamento', {
        method: 'POST', body: JSON.stringify({ p_modulo: modulo, p_id: arquivo.id,
          p_nome: arquivo.name, p_categoria: classificarFalha(mensagem), p_mensagem: mensagem })
      });
    } catch (e) { console.warn('Falha ao persistir diagnostico:', e.message); }
  }
  async function resolverFalha(id) {
    try {
      await banco(`processamento_falhas?modulo=eq.${modulo}&arquivo_id=eq.${encodeURIComponent(id)}`, {
        method: 'PATCH', headers: { Prefer: 'return=minimal' }, body: JSON.stringify({ resolvido_em: new Date().toISOString() })
      });
    } catch (e) { console.warn('Falha ao atualizar diagnostico:', e.message); }
  }
  // As rotas exigem usuario ativo mesmo quando o modo legado desativa o middleware global.
  function permitir(req, res, permissao) {
    const perfil = req.auth?.perfil;
    if (!req.auth?.user?.id) { res.status(401).json({ erro: 'Autenticacao obrigatoria' }); return false; }
    if (!['dev','administrador'].includes(perfil?.role) && !perfil?.permissoes?.includes(permissao)) {
      res.status(403).json({ erro: 'Sem permissao para esta operacao' }); return false;
    }
    return true;
  }
  app.get(`${prefixo}/qualidade`, async (req,res) => {
    if (!permitir(req,res,'qualidade.visualizar')) return;
    try {
      const estado = req.query.estado === 'avaliados' ? 'true' : 'false';
      const offset = Math.max(0, Math.min(100000, Number(req.query.offset) || 0));
      const registros = await banco(`qualidade_ocorrencias?modulo=eq.${modulo}&avaliado=eq.${estado}&order=data.desc.nullslast,id.asc&limit=100&offset=${offset}`);
      res.setHeader('Cache-Control','no-store');
      res.json({ registros, offset });
    } catch (e) { res.status(500).json({ erro: e.message }); }
  });
  app.get(`${prefixo}/qualidade/:id/historico`, async (req,res) => {
    if (!permitir(req,res,'qualidade.visualizar')) return;
    try {
      res.json(await banco(`qualidade_pareceres?modulo=eq.${modulo}&certificado_id=eq.${encodeURIComponent(req.params.id)}&order=criado_em.desc&limit=100`));
    } catch(e) { res.status(500).json({ erro:e.message }); }
  });
  app.get(`${prefixo}/qualidade/historico`, async (req,res) => {
    if (!permitir(req,res,'qualidade.visualizar')) return;
    try {
      const offset=Math.max(0,Math.min(100000,Number(req.query.offset)||0));
      res.setHeader('Cache-Control','no-store');
      res.json(await banco(`qualidade_pareceres?modulo=eq.${modulo}&order=criado_em.desc,id.desc&limit=100&offset=${offset}`));
    } catch(e) {res.status(500).json({erro:e.message});}
  });
  app.post(`${prefixo}/qualidade/:id/parecer`, async (req,res) => {
    if (!permitir(req,res,'qualidade.avaliar')) return;
    const parecer = String(req.body?.parecer || '').trim();
    if (parecer.length < 5 || parecer.length > 4000) return res.status(400).json({erro:'Informe um parecer entre 5 e 4000 caracteres.'});
    try {
      const [item] = await banco(`qualidade_ocorrencias?modulo=eq.${modulo}&id=eq.${encodeURIComponent(req.params.id)}&limit=1`);
      if (!item || item.versao !== req.body.versao) return res.status(409).json({erro:'O certificado mudou. Atualize a avaliacao antes de registrar o parecer.'});
      await banco('qualidade_pareceres',{method:'POST',headers:{Prefer:'return=minimal'},body:JSON.stringify({
        modulo,certificado_id:item.id,versao:item.versao,parecer,responsavel_id:req.auth.user.id,
        responsavel_email:req.auth.user.email || req.auth.perfil.email || '',snapshot:item
      })});
      res.json({ok:true});
    } catch(e) { res.status(500).json({erro:e.message}); }
  });
  app.get(`${prefixo}/processamento/falhas`, async(req,res) => {
    if (!permitir(req,res,'processamento.visualizar')) return;
    try {
      const offset=Math.max(0,Math.min(100000,Number(req.query.offset)||0));
      const resolvidas=req.query.estado==='resolvidas' ? 'not.is.null' : 'is.null';
      const registros=await banco(`processamento_falhas?modulo=eq.${modulo}&resolvido_em=${resolvidas}&order=ultima_falha.desc,arquivo_id.asc&limit=100&offset=${offset}`);
      res.setHeader('Cache-Control','no-store');res.json({registros,offset});
    } catch(e) {res.status(500).json({erro:e.message});}
  });
  let repetindo = false;
  app.post(`${prefixo}/processamento/falhas/:id/tentar`,async(req,res) => {
    if (!permitir(req,res,'processamento.executar')) return;
    if (repetindo) return res.status(409).json({erro:'Uma nova tentativa ja esta em andamento.'});
    try {
      const [arquivo]=await banco(`processamento_falhas?modulo=eq.${modulo}&arquivo_id=eq.${encodeURIComponent(req.params.id)}&resolvido_em=is.null&limit=1`);
      if (!arquivo) return res.status(404).json({erro:'Falha pendente nao encontrada.'});
      repetindo=true;
      await tentarNovamente(arquivo.arquivo_id);
      const [resultado]=await banco(`processamento_falhas?modulo=eq.${modulo}&arquivo_id=eq.${encodeURIComponent(arquivo.arquivo_id)}&limit=1`);
      if (!resultado?.resolvido_em) return res.status(422).json({erro:resultado?.mensagem || 'Arquivo continua pendente. Confira a origem.'});
      res.json({ok:true});
    } catch(e) {res.status(500).json({erro:e.message});}
    finally {repetindo=false;}
  });
  return { registrarFalha, resolverFalha };
}
