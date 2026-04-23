import express from "express";
import { MAPA_LOGGERS, normalizarDLT } from "./mapa-loggers.js";
import fetch from "node-fetch";
import { google } from "googleapis";
import { getDocument } from "pdfjs-dist/legacy/build/pdf.mjs";

const app = express();
app.use(express.json());

// =========================
// CONFIG
// =========================
const SUPABASE_URL = "https://padjfnfysbzaehkqmoyx.supabase.co";
const SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBhZGpmbmZ5c2J6YWVoa3Ftb3l4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY0NTE1OTIsImV4cCI6MjA5MjAyNzU5Mn0.l3xmdwJfu-NDGpoN9MhzQHlW522eO4JX4xgjybRi7vU";
const GOOGLE_API_KEY = "AIzaSyC6KlqA8q9ZUo_4WRC-pIy7P6kg85WMP3s";

const GOOGLE_CLIENT_EMAIL = process.env.GOOGLE_CLIENT_EMAIL || "id-drive-certificados@calcium-bot-493618-e2.iam.gserviceaccount.com";
const GOOGLE_PRIVATE_KEY = (process.env.GOOGLE_PRIVATE_KEY || "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDV5dgC9gPzZ+Va\nELqoquU0YE8BbPptJ2zsUBr+WzGOJUbeWWyrgo9yqeTYwSzcWKeK11GmRgepgKxc\nkQ4ucxceTil9xsH4+AxcciNYiPFquvkKH0i9/UhkK/WCfbR+OsvCXyx4YtAEK7ju\nLkJ7rQabOsftrIv+XIkiah9tZO6ft2qn3nISRuOaRat3VW9xJeeN/Ba1QZN+6FEl\nV6roHubWbLEn4b7I6nbU/uBy/f7Gu0V52CJNXIdTmYIpuwJvc86MV+/IVDqN/233\nJGmVOEZvkx6RP99sTPxd79jjZsuTnUvCI70ggypusOJZWcb7rEKvrscreKuDydYv\njB3NXXdfAgMBAAECggEACXldI5rV+sM262uJeP/b/k5NvlhsKmC9EfJ/LGKWduwi\nKXMSI/HSfL4XS52yz2FPenZzDWEiS1joFk/uet9qJLnj9WHT8aOHy9VAySK3q4Ym\n+Ow0NdLkKluwGI/zNxKC0Ycs2kackOXtRc95IZU8xHj9pgKNTz6C0t1nqvOPhjXU\nbakMNhX5ckWc132esSXVOGOBenTqjsJcIadNuEcUtcPbx17EJT2P0WOFTOkVHffO\nBWycBcD6N6G6p7p457TfCHjcK6be/kNhTtnX5tUmw9Xy+Cpv5bihKfYZXqD7BrEn\nSs/KireqMUYIPx/7JfdMABIXu2Yt2OZ6APA2xlGPAQKBgQDu2du9ARZapf5M2nTa\n6LJCvanjWOcybRYZBQ5a0HsDnFRG+bkHHQ2Lo4zlSWZQghlPv0VrVrw5epePSwzK\nc2g7sx05nU3UChsIli1isPRkbJrqF2CI54ppyS5JIXIyIVYCl041wE7R5LLz2ulL\nPAclJOr8AhMZ/Cs2noJOnnnTQQKBgQDlQVb1WK2hcxL97dS2FWeeDnL76OTs7vU0\nj+E7hyYBWUzOFIkijtI1DGSV/MIChWOgrNSNw5BTlEtMTsuDP5VwTOjibhBcrc2B\nFea7w5y+eMzHiGNWFNE0aW5nX2Xd4EELFYmZx8ruPUgN27mfT9CvQOxg9FBT+7h4\nvmJp0pzCnwKBgFhCZqFTuofqmKqbety9acmhvhpFasFGcAj0xlYmfZ5a8QV9F7Ma\nODwmRlUfp1AOkv3V5vgAB/ORalnH2MUimhydVipJB05YIZ8tpz21t8k4HJJt6v0L\n2ii274SUeFcv3FF+yaaxFi8XPE1B0j07xEQkfTR8K8TJWsqHDg2xH8FBAoGBAKfd\n9EKqsFjr3hg5seuyOLEve1qh6h7jyoC2agIgr9+E+AxeVRwM4Dcf3/dDoPwfmBfq\n9ajobiIFEC3L9JEiWdZlOpGybiCu0y+WTeFnFrsR0UC5yaMakyWBnenrnLeeoYHw\nP1VvSlSwYrZjEcRpuTDapTtJKhiU1Tr0jTNXmJmZAoGAZRcXd+zBm3spGwGmopD5\nVduVSHESwUucfM6g/UDkzpmRkTWjUAOo7gl/jT4ycoM2IGIjQO8/3hOapoCPmI/v\nSoKlQMJsqDMCz2Y8yOCSPes0sI00qpbXijmkes8eegIc6309l7bgPzlqQXdH2dGW\nCKbtjgeGUVDEXl8fD77sazc=\n-----END PRIVATE KEY-----\n").replace(/\\n/g, "\n");

const LIMITE = 50;

// =========================
// GOOGLE DRIVE AUTH
// =========================
const auth = new google.auth.GoogleAuth({
  credentials: {
    client_email: GOOGLE_CLIENT_EMAIL,
    private_key: GOOGLE_PRIVATE_KEY
  },
  scopes: ["https://www.googleapis.com/auth/drive"]
});

const drive = google.drive({ version: "v3", auth });

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

function soDigitos(texto) {
  return String(texto || "").replace(/\D/g, "");
}

function formatarDataBRparaISO(dataBR) {
  const m = String(dataBR || "").match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!m) return "";
  return `${m[3]}-${m[2]}-${m[1]}`;
}

function formatarDataISOParaNome(dataISO) {
  if (!dataISO) return "";
  const [ano, mes, dia] = dataISO.split("-");
  if (!ano || !mes || !dia) return "";
  return `${dia}.${mes}.${ano}`;
}

function montarNomePadrao(dlt, serie, dataISO) {
  const tag = normalizarDLT(dlt);
  const dataFormatada = formatarDataISOParaNome(dataISO);

  if (!tag || !serie || !dataFormatada) return null;
  return `${tag}_${serie}_${dataFormatada}.pdf`;
}

// validade por mês/ano + 1 ano
function verificarValidade(dataISO) {
  if (!dataISO) return { valido: false, vencimento: null, mes_ano: null };

  const [ano, mes] = dataISO.split("-").map(Number);
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

function avaliarDivergencia(dlt, serie) {
  const tag = normalizarDLT(dlt);
  const serieEsperada = tag ? MAPA_LOGGERS[tag] || null : null;

  if (!tag) {
    return {
      divergente: true,
      serie_esperada: null,
      motivo_divergencia: "DLT inválido"
    };
  }

  if (!serieEsperada) {
    return {
      divergente: true,
      serie_esperada: null,
      motivo_divergencia: "DLT não encontrado na base"
    };
  }

  if (String(serie).trim() !== String(serieEsperada).trim()) {
    return {
      divergente: true,
      serie_esperada: serieEsperada,
      motivo_divergencia: "Série divergente"
    };
  }

  return {
    divergente: false,
    serie_esperada: serieEsperada,
    motivo_divergencia: null
  };
}

function execucaoTravada(controle) {
  if (!controle?.em_execucao || !controle?.ultima_execucao) return false;

  const ultima = new Date(controle.ultima_execucao).getTime();
  const agora = Date.now();

  return agora - ultima > 5 * 60 * 1000;
}

// =========================
// GOOGLE DRIVE
// =========================
async function excluirArquivoDrive(fileId) {
  if (!GOOGLE_CLIENT_EMAIL || !GOOGLE_PRIVATE_KEY) {
    throw new Error("GOOGLE_CLIENT_EMAIL ou GOOGLE_PRIVATE_KEY não configurados");
  }

  await drive.files.delete({
    fileId
  });

  return true;
}

// =========================
// PDF / TEXTO
// =========================
async function baixarArquivoDrive(fileId) {
  const url = `https://drive.google.com/uc?export=download&id=${fileId}`;
  const res = await fetch(url);

  if (!res.ok) {
    throw new Error(`Falha ao baixar arquivo do Drive: ${res.status}`);
  }

  return Buffer.from(await res.arrayBuffer());
}

async function extrairTextoELinhasDoPDF(buffer) {
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
  const texto = linhas.map(l => l.texto).join("\n");

  return { texto, items, linhas };
}

function extrairMetadadosDoTexto(texto, nomeOriginal = "") {
  let dlt = "";
  let serie = "";
  let data = "";

  let m = texto.match(/(\d{8})\s+DLT-(\d{4})/i);
  if (m) {
    serie = m[1];
    dlt = m[2];
  }

  if (!dlt || !serie) {
    m = texto.match(/DLT-(\d{4})\s+(\d{8})/i);
    if (m) {
      dlt = m[1];
      serie = m[2];
    }
  }

  let dataMatch = null;
  const idx = texto.search(/Data da Calibração/i);
  if (idx >= 0) {
    const trecho = texto.slice(idx, idx + 400);
    dataMatch = trecho.match(/(\d{2}\/\d{2}\/\d{4})/);
  }

  if (!dataMatch) {
    dataMatch = texto.match(/(\d{2}\/\d{2}\/\d{4})/);
  }

  if (dataMatch) {
    data = formatarDataBRparaISO(dataMatch[1]);
  }

  const baseNome = extrairDados(nomeOriginal);
  if (!dlt && baseNome.dlt) dlt = soDigitos(baseNome.dlt).padStart(4, "0");
  if (!serie && baseNome.serie) serie = soDigitos(baseNome.serie);
  if (!data && baseNome.data) data = baseNome.data;

  return {
    dlt: dlt ? soDigitos(dlt).padStart(4, "0") : "",
    serie: serie ? soDigitos(serie) : "",
    data: data || ""
  };
}

// =========================
// LEITURA DA TABELA DO PDF
// =========================
async function extrairTabelaPorColunas(buffer) {
  const { items, linhas } = await extrairTextoELinhasDoPDF(buffer);

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
    const buffer = await baixarArquivoDrive(fileId);
    const tabela = await extrairTabelaPorColunas(buffer);

    if (!tabela.ok) {
      return {
        status: "ERRO",
        pontos: [],
        debug: tabela.debug || {}
      };
    }

    const aprovado = tabela.pontos.every(p => p.soma <= 0.5);

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
  const ids = new Set();
  const limit = 1000;
  let offset = 0;

  while (true) {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=id&limit=${limit}&offset=${offset}`,
      { headers: supabaseHeaders() }
    );

    const data = await r.json();

    if (!data || data.length === 0) break;

    for (const item of data) {
      ids.add(item.id);
    }

    if (data.length < limit) break;
    offset += limit;
  }

  return ids;
}

async function contarCertificadosBanco() {
  const r = await fetch(
    `${SUPABASE_URL}/rest/v1/certificados?select=id`,
    {
      headers: {
        ...supabaseHeaders(),
        Prefer: "count=exact",
        Range: "0-0"
      }
    }
  );

  const contentRange = r.headers.get("content-range");
  if (!contentRange) return 0;

  const total = contentRange.split("/")[1];
  return Number(total || 0);
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

      try {
        const buffer = await baixarArquivoDrive(f.id);
        const { texto } = await extrairTextoELinhasDoPDF(buffer);
        const meta = extrairMetadadosDoTexto(texto, f.name);

        if (!meta.dlt || !meta.serie || !meta.data) {
          console.log("Arquivo sem metadados suficientes:", f.name, meta);
          continue;
        }

        const tabela = await extrairTabelaPorColunas(buffer);

        if (!tabela.ok) {
          console.log("Arquivo sem tabela válida:", f.name, tabela.debug);
          continue;
        }

        const status = tabela.pontos.every(p => p.soma <= 0.5) ? "APROVADO" : "REPROVADO";
        const val = verificarValidade(meta.data);
        const divergencia = avaliarDivergencia(meta.dlt, meta.serie);
        const nomePadronizado = montarNomePadrao(meta.dlt, meta.serie, meta.data);

        const respInsert = await fetch(`${SUPABASE_URL}/rest/v1/certificados`, {
          method: "POST",
          headers: supabaseHeaders(),
          body: JSON.stringify({
            id: f.id,
            nome_original: f.name,
            nome_download: nomePadronizado,
            dlt: meta.dlt,
            serie: meta.serie,
            data: meta.data,
            status: status,
            validade: val.valido,
            vencimento: val.vencimento,
            mes_ano_validade: val.mes_ano,
            pontos: tabela.pontos,
            divergente: divergencia.divergente,
            serie_esperada: divergencia.serie_esperada,
            motivo_divergencia: divergencia.motivo_divergencia
          })
        });

        if (!respInsert.ok) {
          const erroInsert = await respInsert.text();
          console.log("Erro ao salvar no banco:", f.name, erroInsert);
          continue;
        }

        processados++;

        await atualizarControleSync({
          em_execucao: true,
          ultima_execucao: new Date().toISOString(),
          total_processados: processados
        });
      } catch (e) {
        console.log("Erro ao processar arquivo:", f.name, e.message);
      }
    }

    const idsBancoAtualizado = await buscarIdsBanco();
    const arquivosDriveAtualizados = await buscarArquivosDrive();
    const faltantesRestantes = arquivosDriveAtualizados.filter(f => !idsBancoAtualizado.has(f.id)).length;

    await atualizarControleSync({
      em_execucao: false,
      ultima_execucao: new Date().toISOString(),
      total_processados: processados
    });

    if (faltantesRestantes > 0) {
      setTimeout(() => {
        executarSyncEmBackground();
      }, 3000);
    }
  } catch (e) {
    console.log("Erro geral executarSyncEmBackground:", e.message);

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

app.get("/status", async (req, res) => {
  try {
    const controle = await buscarControleSync();
    const totalBanco = await contarCertificadosBanco();
    const idsBanco = await buscarIdsBanco();
    const arquivosDrive = await buscarArquivosDrive();

    const totalDrive = arquivosDrive.length;
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
    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=*&order=data.desc&limit=${limit}&offset=${offset}`,
      {
        headers: {
          ...supabaseHeaders(),
          Prefer: "count=exact"
        }
      }
    );

    const data = await r.json();
    const contentRange = r.headers.get("content-range");
    const total = contentRange ? Number(contentRange.split("/")[1]) : data.length;

    res.json({
      total,
      limit,
      offset,
      registros: data
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/divergentes", async (req, res) => {
  try {
    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=*&divergente=eq.true&order=data.desc&limit=${limit}&offset=${offset}`,
      {
        headers: {
          ...supabaseHeaders(),
          Prefer: "count=exact"
        }
      }
    );

    const data = await r.json();
    const contentRange = r.headers.get("content-range");
    const total = contentRange ? Number(contentRange.split("/")[1]) : data.length;

    res.json({
      total,
      limit,
      offset,
      registros: data
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/sync", async (req, res) => {
  try {
    const controle = await buscarControleSync();

    if (controle?.em_execucao && !execucaoTravada(controle)) {
      return res.json({
        mensagem: "Processamento já está em execução",
        novos_processados: 0
      });
    }

    if (execucaoTravada(controle)) {
      await atualizarControleSync({
        em_execucao: false,
        total_processados: 0,
        ultima_execucao: new Date().toISOString()
      });
    }

    res.json({
      mensagem: "Processamento iniciado",
      novos_processados: 0
    });

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
      `${SUPABASE_URL}/rest/v1/certificados?select=id,data,dlt,serie&limit=${limit}&offset=${offset}`,
      { headers: supabaseHeaders() }
    );

    const lista = await r.json();
    let total = 0;

    for (const item of lista) {
      const proc = await processarPDF(item.id);
      const val = verificarValidade(item.data);
      const divergencia = avaliarDivergencia(item.dlt, item.serie);
      const nomePadronizado = montarNomePadrao(item.dlt, item.serie, item.data);

      await fetch(`${SUPABASE_URL}/rest/v1/certificados?id=eq.${item.id}`, {
        method: "PATCH",
        headers: supabaseHeaders(),
        body: JSON.stringify({
          status: proc.status,
          pontos: proc.pontos,
          validade: val.valido,
          vencimento: val.vencimento,
          mes_ano_validade: val.mes_ano,
          nome_download: nomePadronizado,
          divergente: divergencia.divergente,
          serie_esperada: divergencia.serie_esperada,
          motivo_divergencia: divergencia.motivo_divergencia
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

app.get("/pendentes", async (req, res) => {
  try {
    const idsBanco = await buscarIdsBanco();
    const arquivosDrive = await buscarArquivosDrive();

    const pendentes = arquivosDrive
      .filter(f => !idsBanco.has(f.id))
      .map(f => ({
        id: f.id,
        nome_original: f.name
      }))
      .sort((a, b) => a.nome_original.localeCompare(b.nome_original, "pt-BR"));

    res.json({
      total: pendentes.length,
      arquivos: pendentes
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/download/:id", async (req, res) => {
  try {
    const id = req.params.id;

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?id=eq.${id}&select=id,nome_download`,
      { headers: supabaseHeaders() }
    );

    const data = await r.json();

    if (!data || data.length === 0) {
      return res.status(404).send("Arquivo não encontrado");
    }

    const nome = data[0].nome_download || `arquivo_${id}.pdf`;
    const buffer = await baixarArquivoDrive(id);

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${nome}"`);
    res.send(buffer);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.listen(3000, () => console.log("Servidor rodando 🚀"));
