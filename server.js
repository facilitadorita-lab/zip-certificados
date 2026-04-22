import express from "express";
import fetch from "node-fetch";
import { getDocument } from "pdfjs-dist/legacy/build/pdf.mjs";

const app = express();
app.use(express.json());

// =========================
// CONFIG
// =========================
const SUPABASE_URL = "https://padjfnfysbzaehkqmoyx.supabase.co";
const SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBhZGpmbmZ5c2J6YWVoa3Ftb3l4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY0NTE1OTIsImV4cCI6MjA5MjAyNzU5Mn0.l3xmdwJfu-NDGpoN9MhzQHlW522eO4JX4xgjybRi7vU";
const GOOGLE_API_KEY = "AIzaSyC6KlqA8q9ZUo_4WRC-pIy7P6kg85WMP3s";
const FOLDER_ID = "1SZO18AAITa3-3wI86zcZi2yGR6RXtUZ_";

const LIMITE = 50;

// =========================
// HELPERS
// =========================
function supabaseHeaders() {
  return {
    apikey: SUPABASE_KEY,
    Authorization: `Bearer ${SUPABASE_KEY}`,
    "Content-Type": "application/json"
  };
}

function parseBR(v) {
  return parseFloat(String(v).replace(",", "."));
}

function fmt2(n) {
  return Number(Number(n).toFixed(2));
}

function extrairDados(nome) {
  const partes = nome.replace(".pdf", "").split("_");

  return {
    nome_original: nome,
    dlt: partes[0]?.replace("DLT-", "") || "",
    serie: partes[1] || "",
    data: partes[2]
      ? partes[2].split(".").reverse().join("-")
      : ""
  };
}

function verificarValidade(dataISO) {
  if (!dataISO) return { valido: false, vencimento: null };

  const [ano, mes] = dataISO.split("-").map(Number);

  // último dia do mês + 1 ano
  const vencimentoDate = new Date(ano + 1, mes, 0);

  const hoje = new Date();
  hoje.setHours(0, 0, 0, 0);

  return {
    valido: hoje <= vencimentoDate,
    vencimento: vencimentoDate.toISOString().split("T")[0],
    mes_ano: `${String(mes).padStart(2, "0")}/${ano + 1}`
  };
}

function somenteNumeroBR(texto) {
  return /^-?\d+,\d+$/.test((texto || "").trim());
}

function agruparLinhasPorY(items, tolerancia = 2.2) {
  const ordenados = [...items].sort((a, b) => b.y - a.y);
  const linhas = [];

  for (const item of ordenados) {
    let linha = linhas.find(l => Math.abs(l.y - item.y) <= tolerancia);

    if (!linha) {
      linha = { y: item.y, items: [] };
      linhas.push(linha);
    }

    linha.items.push(item);
  }

  for (const linha of linhas) {
    linha.items.sort((a, b) => a.x - b.x);
    linha.texto = linha.items.map(i => i.text).join(" ");
  }

  return linhas.sort((a, b) => b.y - a.y);
}

function acharCabecalho(items, regex) {
  return items.find(i => regex.test(i.text));
}

function numeroMaisProximoNaColuna(linha, xColuna, faixa = 30) {
  const candidatos = linha.items.filter(i =>
    somenteNumeroBR(i.text) && Math.abs(i.x - xColuna) <= faixa
  );

  if (!candidatos.length) return null;

  candidatos.sort((a, b) => Math.abs(a.x - xColuna) - Math.abs(b.x - xColuna));
  return candidatos[0];
}

// =========================
// LEITURA DA TABELA DO PDF
// =========================
async function extrairTabelaPorColunas(buffer) {
  const pdf = await getDocument({ data: new Uint8Array(buffer) }).promise;
  const page = await pdf.getPage(1);
  const textContent = await page.getTextContent();

  const items = textContent.items
    .map(i => ({
      text: String(i.str || "").trim(),
      x: i.transform[4],
      y: i.transform[5]
    }))
    .filter(i => i.text);

  const linhas = agruparLinhasPorY(items);

  const cabAquecimento = acharCabecalho(items, /^Aquecimento$/i);
  const cabErro = acharCabecalho(items, /^Erro$/i);
  const cabIncerteza = acharCabecalho(items, /Medição Expandida/i);

  if (!cabAquecimento || !cabErro || !cabIncerteza) {
    return {
      ok: false,
      debug: {
        motivo: "Cabeçalhos não encontrados",
        aquecimento: !!cabAquecimento,
        erro: !!cabErro,
        incerteza: !!cabIncerteza
      }
    };
  }

  const xAquecimento = cabAquecimento.x;
  const xErro = cabErro.x;
  const xIncerteza = cabIncerteza.x;
  const yTopoTabela = Math.max(cabAquecimento.y, cabErro.y, cabIncerteza.y);

  const linhasDados = linhas.filter(l => l.y < yTopoTabela - 3);
  const candidatos = [];

  for (const linha of linhasDados) {
    const aq = numeroMaisProximoNaColuna(linha, xAquecimento, 35);
    const er = numeroMaisProximoNaColuna(linha, xErro, 35);
    const inc = numeroMaisProximoNaColuna(linha, xIncerteza, 45);

    if (!aq || !er || !inc) continue;

    const aquecimento = parseBR(aq.text);
    const erro = Math.abs(parseBR(er.text));
    const incerteza = Math.abs(parseBR(inc.text));

    if (Number.isNaN(aquecimento) || Number.isNaN(erro) || Number.isNaN(incerteza)) continue;
    if (aquecimento < -100 || aquecimento > 200) continue;
    if (erro > 2 || incerteza > 2) continue;

    candidatos.push({
      y: linha.y,
      aquecimento: fmt2(aquecimento),
      erro: fmt2(erro),
      incerteza: fmt2(incerteza),
      texto: linha.texto
    });
  }

  const vistos = new Set();
  const unicos = [];

  for (const c of candidatos.sort((a, b) => b.y - a.y)) {
    const chave = c.aquecimento.toFixed(2);
    if (!vistos.has(chave)) {
      vistos.add(chave);
      unicos.push(c);
    }
  }

  const pontosOrdenados = unicos
    .sort((a, b) => a.aquecimento - b.aquecimento)
    .slice(0, 4);

  if (pontosOrdenados.length < 4) {
    return {
      ok: false,
      debug: {
        motivo: "Menos de 4 linhas válidas",
        xAquecimento,
        xErro,
        xIncerteza,
        candidatos: candidatos.map(c => ({
          aquecimento: c.aquecimento,
          erro: c.erro,
          incerteza: c.incerteza,
          texto: c.texto
        }))
      }
    };
  }

  const pontos = pontosOrdenados.map((p, idx) => ({
    ponto: idx + 1,
    aquecimento: p.aquecimento,
    erro: p.erro,
    incerteza: p.incerteza,
    soma: fmt2(p.erro + p.incerteza)
  }));

  return { ok: true, pontos };
}

// =========================
// PROCESSAMENTO PRINCIPAL
// =========================
async function processarPDF(fileId) {
  try {
    const url = `https://drive.google.com/uc?export=download&id=${fileId}`;
    const res = await fetch(url);

    if (!res.ok) {
      return { status: "ERRO", pontos: [] };
    }

    const buffer = Buffer.from(await res.arrayBuffer());
    const tabela = await extrairTabelaPorColunas(buffer);

    if (!tabela.ok) {
      return {
        status: "ERRO",
        pontos: [],
        debug: tabela.debug || {}
      };
    }

    let aprovado = true;

    for (const p of tabela.pontos) {
      if (p.soma > 0.5) aprovado = false;
    }

    return {
      status: aprovado ? "APROVADO" : "REPROVADO",
      pontos: tabela.pontos
    };
  } catch (e) {
    return {
      status: "ERRO",
      pontos: [],
      debug: { erro: e.message }
    };
  }
}

// =========================
// CONTROLE / DRIVE
// =========================
async function atualizarControleSync(payload) {
  await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1`, {
    method: "PATCH",
    headers: supabaseHeaders(),
    body: JSON.stringify(payload)
  });
}

async function buscarControleSync() {
  const r = await fetch(
    `${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1&select=*`,
    { headers: supabaseHeaders() }
  );

  const data = await r.json();
  return data && data.length ? data[0] : null;
}

async function buscarIdsBanco() {
  const r = await fetch(
    `${SUPABASE_URL}/rest/v1/certificados?select=id`,
    { headers: supabaseHeaders() }
  );

  const data = await r.json();
  return new Set((data || []).map(e => e.id));
}

async function buscarArquivosDrive() {
  const arquivos = [];
  let pageToken = null;

  do {
    const url = `https://www.googleapis.com/drive/v3/files?q='${FOLDER_ID}'+in+parents&key=${GOOGLE_API_KEY}&fields=nextPageToken,files(id,name)&pageSize=1000${pageToken ? `&pageToken=${pageToken}` : ""}`;

    const res = await fetch(url);
    const data = await res.json();

    arquivos.push(...(data.files || []));
    pageToken = data.nextPageToken || null;
  } while (pageToken);

  return arquivos;
}

// =========================
// EXECUÇÃO DE SYNC EM BACKGROUND
// =========================
async function executarSyncEmBackground() {
  try {
    await atualizarControleSync({
      em_execucao: true,
      ultima_execucao: new Date().toISOString(),
      total_processados: 0
    });

    const idsBanco = await buscarIdsBanco();
    const arquivosDrive = await buscarArquivosDrive();

    let processados = 0;

    for (const f of arquivosDrive) {
      if (idsBanco.has(f.id)) continue;
      if (processados >= LIMITE) break;

      const base = extrairDados(f.name);
      const proc = await processarPDF(f.id);
      const val = verificarValidade(base.data);

      await fetch(`${SUPABASE_URL}/rest/v1/certificados`, {
        method: "POST",
        headers: supabaseHeaders(),
        body: JSON.stringify({
          id: f.id,
          nome_original: base.nome_original,
          dlt: base.dlt,
          serie: base.serie,
          data: base.data,
          status: proc.status,
          validade: val.valido,
          vencimento: val.vencimento,
          pontos: proc.pontos
        })
      });

      processados++;

      await atualizarControleSync({
        em_execucao: true,
        ultima_execucao: new Date().toISOString(),
        total_processados: processados
      });
    }

    await atualizarControleSync({
      em_execucao: false,
      ultima_execucao: new Date().toISOString(),
      total_processados: processados
    });
  } catch (e) {
    await atualizarControleSync({
      em_execucao: false,
      ultima_execucao: new Date().toISOString()
    });
  }
}

// =========================
// ROTAS
// =========================
app.get("/", (req, res) => {
  res.send("API OK 🚀");
});

// STATUS PARA O LOVABLE
app.get("/status", async (req, res) => {
  try {
    const controle = await buscarControleSync();
    const idsBanco = await buscarIdsBanco();
    const arquivosDrive = await buscarArquivosDrive();

    const totalDrive = arquivosDrive.length;
    const totalBanco = idsBanco.size;
    const faltantes = arquivosDrive.filter(f => !idsBanco.has(f.id)).length;

    res.json({
      id: controle?.id || 1,
      total_processados: controle?.total_processados || 0,
      em_execucao: controle?.em_execucao || false,
      ultima_execucao: controle?.ultima_execucao || null,
      total_drive: totalDrive,
      total_banco: totalBanco,
      faltantes
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/certificados", async (req, res) => {
  try {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=*&order=data.desc`,
      { headers: supabaseHeaders() }
    );

    const data = await r.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

// SYNC RÁPIDO PARA CRON / LOVABLE
app.get("/sync", async (req, res) => {
  try {
    const controle = await buscarControleSync();

    if (controle?.em_execucao) {
      return res.json({
        mensagem: "Processamento já está em execução",
        novos_processados: 0
      });
    }

    // responde imediatamente
    res.json({
      mensagem: "Processamento iniciado",
      novos_processados: 0
    });

    // continua em background
    executarSyncEmBackground();
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/reprocess", async (req, res) => {
  try {
    const limit = Number(req.query.limit || 50);
    const offset = Number(req.query.offset || 0);

    const controle = await buscarControleSync();

    if (controle?.em_execucao) {
      return res.json({
        mensagem: "Processamento já está em execução",
        processados: 0,
        offset,
        proximo_offset: offset
      });
    }

    await atualizarControleSync({
      em_execucao: true,
      ultima_execucao: new Date().toISOString(),
      total_processados: 0
    });

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=id,data&limit=${limit}&offset=${offset}`,
      { headers: supabaseHeaders() }
    );

    const lista = await r.json();
    let total = 0;

    for (const item of lista) {
      const proc = await processarPDF(item.id);

      await fetch(`${SUPABASE_URL}/rest/v1/certificados?id=eq.${item.id}`, {
        method: "PATCH",
        headers: supabaseHeaders(),
        body: JSON.stringify({
          status: proc.status,
          pontos: proc.pontos
        })
      });

      total++;

      await atualizarControleSync({
        em_execucao: true,
        ultima_execucao: new Date().toISOString(),
        total_processados: total
      });
    }

    await atualizarControleSync({
      em_execucao: false,
      ultima_execucao: new Date().toISOString(),
      total_processados: total
    });

    res.json({
      mensagem: "Reprocessamento concluído",
      processados: total,
      offset,
      proximo_offset: offset + total
    });
  } catch (e) {
    await atualizarControleSync({
      em_execucao: false,
      ultima_execucao: new Date().toISOString()
    });

    res.status(500).json({ erro: e.message });
  }
});

app.get("/teste/:id", async (req, res) => {
  const resultado = await processarPDF(req.params.id);
  res.json(resultado);
});

app.listen(3000, () => console.log("Servidor rodando 🚀"));
