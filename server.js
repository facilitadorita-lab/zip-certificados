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

const LIMITE_POR_LOTE = 50;

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

  const d = new Date(dataISO);
  const v = new Date(d);
  v.setFullYear(v.getFullYear() + 1);

  return {
    valido: new Date() <= v,
    vencimento: v.toISOString().split("T")[0]
  };
}

function parseNumeroBR(valor) {
  return parseFloat(valor.replace(",", "."));
}

function format2(n) {
  return Number(Number(n).toFixed(2));
}

function agruparLinhasPorY(items, tolerancia = 2.5) {
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

function pegarNumeroMaisProximoDaColuna(linha, xColuna) {
  const candidatos = linha.items.filter(i => /^-?\d+,\d+$/.test(i.text));
  if (!candidatos.length) return null;

  candidatos.sort((a, b) => Math.abs(a.x - xColuna) - Math.abs(b.x - xColuna));
  return candidatos[0];
}

// =========================
// LEITURA DO PDF POR POSIÇÃO
// =========================
async function extrairTabelaCalibracao(buffer) {
  const pdf = await getDocument({ data: new Uint8Array(buffer) }).promise;
  const page = await pdf.getPage(1);
  const textContent = await page.getTextContent();

  const items = textContent.items
    .map(i => ({
      text: (i.str || "").trim(),
      x: i.transform[4],
      y: i.transform[5]
    }))
    .filter(i => i.text);

  const linhas = agruparLinhasPorY(items);

  // localizar âncoras das colunas visualmente
  const itemAquecimento = items.find(i => /Aquecimento/i.test(i.text));
  const itemErro = items.find(i => /^Erro$/i.test(i.text));
  const itemExpandida = items.find(i => /Expandida/i.test(i.text));

  if (!itemAquecimento || !itemErro || !itemExpandida) {
    return {
      ok: false,
      motivo: "Não encontrou cabeçalhos da tabela",
      debug: {
        cabecalhos: {
          aquecimento: !!itemAquecimento,
          erro: !!itemErro,
          expandida: !!itemExpandida
        }
      }
    };
  }

  const xAquecimento = itemAquecimento.x;
  const xErro = itemErro.x;
  const xIncerteza = itemExpandida.x;

  // pega apenas linhas abaixo do cabeçalho principal
  const linhasDados = linhas.filter(l => l.y < itemErro.y - 5);

  const candidatos = [];

  for (const linha of linhasDados) {
    const aqItem = pegarNumeroMaisProximoDaColuna(linha, xAquecimento);
    const erroItem = pegarNumeroMaisProximoDaColuna(linha, xErro);
    const incItem = pegarNumeroMaisProximoDaColuna(linha, xIncerteza);

    if (!aqItem || !erroItem || !incItem) continue;

    const aquecimento = parseNumeroBR(aqItem.text);
    const erro = Math.abs(parseNumeroBR(erroItem.text));
    const incerteza = Math.abs(parseNumeroBR(incItem.text));

    // filtros defensivos para pegar só a tabela certa
    if (aquecimento < -100 || aquecimento > 200) continue;
    if (erro > 2 || incerteza > 2) continue;

    candidatos.push({
      y: linha.y,
      aquecimento: format2(aquecimento),
      erro: format2(erro),
      incerteza: format2(incerteza),
      texto: linha.texto
    });
  }

  // remove duplicadas por aquecimento
  const unicos = [];
  const vistos = new Set();

  for (const c of candidatos.sort((a, b) => b.y - a.y)) {
    const chave = c.aquecimento.toFixed(2);
    if (!vistos.has(chave)) {
      vistos.add(chave);
      unicos.push(c);
    }
  }

  // queremos só os 4 pontos
  const pontosOrdenados = unicos
    .sort((a, b) => a.aquecimento - b.aquecimento)
    .slice(0, 4);

  if (pontosOrdenados.length < 4) {
    return {
      ok: false,
      motivo: "Não encontrou 4 linhas válidas da tabela",
      debug: {
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

  return {
    ok: true,
    linhas: pontosOrdenados.map((p, idx) => ({
      ponto: idx + 1,
      aquecimento: p.aquecimento,
      erro: p.erro,
      incerteza: p.incerteza,
      soma: format2(p.erro + p.incerteza)
    }))
  };
}

// =========================
// PROCESSAMENTO PRINCIPAL
// =========================
async function processarPDF(fileId) {
  try {
    const url = `https://drive.google.com/uc?export=download&id=${fileId}`;
    const res = await fetch(url);

    if (!res.ok) {
      return {
        status: "ERRO",
        pontos: [],
        debug: { motivo: "Falha ao baixar PDF" }
      };
    }

    const buffer = Buffer.from(await res.arrayBuffer());

    const tabela = await extrairTabelaCalibracao(buffer);

    if (!tabela.ok) {
      return {
        status: "ERRO",
        pontos: [],
        debug: tabela.debug || { motivo: tabela.motivo || "Falha ao ler tabela" }
      };
    }

    let aprovado = true;
    const pontos = tabela.linhas.map(p => {
      if (p.soma > 0.5) aprovado = false;
      return p;
    });

    return {
      status: aprovado ? "APROVADO" : "REPROVADO",
      pontos
    };
  } catch (e) {
    return {
      status: "ERRO",
      pontos: [],
      debug: { motivo: e.message }
    };
  }
}

// =========================
// ROTAS
// =========================
app.get("/", (req, res) => {
  res.send("API OK 🚀");
});

app.get("/status", async (req, res) => {
  try {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1&select=*`,
      {
        headers: {
          apikey: SUPABASE_KEY,
          Authorization: `Bearer ${SUPABASE_KEY}`
        }
      }
    );

    const data = await r.json();
    res.json(data[0] || {});
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/check-novos", async (req, res) => {
  try {
    const existentesRes = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=id`,
      {
        headers: {
          apikey: SUPABASE_KEY,
          Authorization: `Bearer ${SUPABASE_KEY}`
        }
      }
    );

    const existentes = await existentesRes.json();
    const idsExistentes = new Set((existentes || []).map(e => e.id));

    let pageToken = null;
    let totalNovos = 0;

    do {
      const url = `https://www.googleapis.com/drive/v3/files?q='${FOLDER_ID}'+in+parents&key=${GOOGLE_API_KEY}&fields=nextPageToken,files(id,name)&pageSize=1000${pageToken ? `&pageToken=${pageToken}` : ""}`;

      const driveRes = await fetch(url);
      const drive = await driveRes.json();

      const arquivos = drive.files || [];
      const novos = arquivos.filter(f => !idsExistentes.has(f.id));
      totalNovos += novos.length;

      pageToken = drive.nextPageToken || null;
    } while (pageToken);

    res.json({ novos: totalNovos });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/sync", async (req, res) => {
  try {
    const check = await fetch("https://zip-certificados.onrender.com/check-novos");
    const checkData = await check.json();

    if (checkData.novos === 0) {
      await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1`, {
        method: "PATCH",
        headers: supabaseHeaders(),
        body: JSON.stringify({
          em_execucao: false,
          total_processados: 0,
          ultima_execucao: new Date()
        })
      });

      return res.json({
        mensagem: "Nada novo. Sync ignorado.",
        processados: 0
      });
    }

    await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1`, {
      method: "PATCH",
      headers: supabaseHeaders(),
      body: JSON.stringify({
        em_execucao: true,
        ultima_execucao: new Date()
      })
    });

    const existentesRes = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=id`,
      {
        headers: {
          apikey: SUPABASE_KEY,
          Authorization: `Bearer ${SUPABASE_KEY}`
        }
      }
    );

    const existentes = await existentesRes.json();
    const ids = new Set((existentes || []).map(e => e.id));

    let pageToken = null;
    let totalProcessados = 0;
    const processadosNesteCiclo = [];

    do {
      const url = `https://www.googleapis.com/drive/v3/files?q='${FOLDER_ID}'+in+parents&key=${GOOGLE_API_KEY}&fields=nextPageToken,files(id,name)&pageSize=1000${pageToken ? `&pageToken=${pageToken}` : ""}`;

      const driveRes = await fetch(url);
      const drive = await driveRes.json();

      const arquivos = drive.files || [];
      const novos = arquivos.filter(f => !ids.has(f.id));

      for (const f of novos.slice(0, LIMITE_POR_LOTE)) {
        const base = extrairDados(f.name);
        const proc = await processarPDF(f.id);
        const val = verificarValidade(base.data);

        const registro = {
          id: f.id,
          nome_original: base.nome_original,
          dlt: base.dlt,
          serie: base.serie,
          data: base.data,
          status: proc?.status || "ERRO",
          validade: val.valido,
          vencimento: val.vencimento,
          pontos: proc?.pontos || []
        };

        await fetch(`${SUPABASE_URL}/rest/v1/certificados`, {
          method: "POST",
          headers: {
            ...supabaseHeaders(),
            Prefer: "resolution=merge-duplicates"
          },
          body: JSON.stringify(registro)
        });

        totalProcessados++;
        processadosNesteCiclo.push({
          id: f.id,
          nome_original: f.name,
          status: registro.status
        });
      }

      pageToken = drive.nextPageToken || null;

      if (totalProcessados >= LIMITE_POR_LOTE) break;
    } while (pageToken);

    await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1`, {
      method: "PATCH",
      headers: supabaseHeaders(),
      body: JSON.stringify({
        total_processados: totalProcessados,
        em_execucao: false
      })
    });

    res.json({
      mensagem: "Sync concluído",
      processados: totalProcessados,
      itens: processadosNesteCiclo
    });
  } catch (e) {
    await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1`, {
      method: "PATCH",
      headers: supabaseHeaders(),
      body: JSON.stringify({
        em_execucao: false
      })
    });

    res.status(500).json({ erro: e.message });
  }
});

app.get("/reprocess", async (req, res) => {
  try {
    const limit = Number(req.query.limit || LIMITE_POR_LOTE);
    const offset = Number(req.query.offset || 0);

    await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1`, {
      method: "PATCH",
      headers: supabaseHeaders(),
      body: JSON.stringify({
        em_execucao: true,
        ultima_execucao: new Date()
      })
    });

    const existentesRes = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=id,nome_original,dlt,serie,data&order=criado_em.asc&limit=${limit}&offset=${offset}`,
      {
        headers: {
          apikey: SUPABASE_KEY,
          Authorization: `Bearer ${SUPABASE_KEY}`
        }
      }
    );

    const existentes = await existentesRes.json();
    let totalProcessados = 0;

    for (const item of existentes || []) {
      const proc = await processarPDF(item.id);
      const val = verificarValidade(item.data);

      const registro = {
        id: item.id,
        nome_original: item.nome_original,
        dlt: item.dlt,
        serie: item.serie,
        data: item.data,
        status: proc?.status || "ERRO",
        validade: val.valido,
        vencimento: val.vencimento,
        pontos: proc?.pontos || []
      };

      await fetch(`${SUPABASE_URL}/rest/v1/certificados?id=eq.${item.id}`, {
        method: "PATCH",
        headers: supabaseHeaders(),
        body: JSON.stringify(registro)
      });

      totalProcessados++;
    }

    await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1`, {
      method: "PATCH",
      headers: supabaseHeaders(),
      body: JSON.stringify({
        total_processados: totalProcessados,
        em_execucao: false
      })
    });

    res.json({
      mensagem: "Reprocessamento concluído",
      processados: totalProcessados,
      offset,
      proximo_offset: offset + totalProcessados
    });
  } catch (e) {
    await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1`, {
      method: "PATCH",
      headers: supabaseHeaders(),
      body: JSON.stringify({
        em_execucao: false
      })
    });

    res.status(500).json({ erro: e.message });
  }
});

app.get("/certificados", async (req, res) => {
  try {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=*&order=data.desc`,
      {
        headers: {
          apikey: SUPABASE_KEY,
          Authorization: `Bearer ${SUPABASE_KEY}`
        }
      }
    );

    const data = await r.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.listen(3000, () => console.log("Servidor rodando 🚀"));
