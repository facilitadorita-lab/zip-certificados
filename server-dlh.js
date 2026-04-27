import express from "express";
import fetch from "node-fetch";
import { google } from "googleapis";
import { getDocument } from "pdfjs-dist/legacy/build/pdf.mjs";
import { MAPA_LOGGERS_DLH, normalizarDLH } from "./mapa-loggers-dlh.js";

const app = express();
app.use(express.json());

// =========================
// CONFIG DLH
// =========================
const PORT = process.env.PORT || 3001;

const SUPABASE_URL = process.env.SUPABASE_URL || "https://padjfnfysbzaehkqmoyx.supabase.co";
const SUPABASE_KEY = process.env.SUPABASE_KEY || "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBhZGpmbmZ5c2J6YWVoa3Ftb3l4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY0NTE1OTIsImV4cCI6MjA5MjAyNzU5Mn0.l3xmdwJfu-NDGpoN9MhzQHlW522eO4JX4xgjybRi7vU";

const FOLDER_ID_DLH = process.env.FOLDER_ID_DLH || "1PqEsZ5r2z-I9l4BbR5oFPksywC7gcDyl";
const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY || "AIzaSyC6KlqA8q9ZUo_4WRC-pIy7P6kg85WMP3s";

const GOOGLE_CLIENT_EMAIL = process.env.GOOGLE_CLIENT_EMAIL || "id-drive-certificados@calcium-bot-493618-e2.iam.gserviceaccount.com";
const GOOGLE_PRIVATE_KEY = (process.env.GOOGLE_PRIVATE_KEY || "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDV5dgC9gPzZ+Va\nELqoquU0YE8BbPptJ2zsUBr+WzGOJUbeWWyrgo9yqeTYwSzcWKeK11GmRgepgKxc\nkQ4ucxceTil9xsH4+AxcciNYiPFquvkKH0i9/UhkK/WCfbR+OsvCXyx4YtAEK7ju\nLkJ7rQabOsftrIv+XIkiah9tZO6ft2qn3nISRuOaRat3VW9xJeeN/Ba1QZN+6FEl\nV6roHubWbLEn4b7I6nbU/uBy/f7Gu0V52CJNXIdTmYIpuwJvc86MV+/IVDqN/233\nJGmVOEZvkx6RP99sTPxd79jjZsuTnUvCI70ggypusOJZWcb7rEKvrscreKuDydYv\njB3NXXdfAgMBAAECggEACXldI5rV+sM262uJeP/b/k5NvlhsKmC9EfJ/LGKWduwi\nKXMSI/HSfL4XS52yz2FPenZzDWEiS1joFk/uet9qJLnj9WHT8aOHy9VAySK3q4Ym\n+Ow0NdLkKluwGI/zNxKC0Ycs2kackOXtRc95IZU8xHj9pgKNTz6C0t1nqvOPhjXU\nbakMNhX5ckWc132esSXVOGOBenTqjsJcIadNuEcUtcPbx17EJT2P0WOFTOkVHffO\nBWycBcD6N6G6p7p457TfCHjcK6be/kNhTtnX5tUmw9Xy+Cpv5bihKfYZXqD7BrEn\nSs/KireqMUYIPx/7JfdMABIXu2Yt2OZ6APA2xlGPAQKBgQDu2du9ARZapf5M2nTa\n6LJCvanjWOcybRYZBQ5a0HsDnFRG+bkHHQ2Lo4zlSWZQghlPv0VrVrw5epePSwzK\nc2g7sx05nU3UChsIli1isPRkbJrqF2CI54ppyS5JIXIyIVYCl041wE7R5LLz2ulL\nPAclJOr8AhMZ/Cs2noJOnnnTQQKBgQDlQVb1WK2hcxL97dS2FWeeDnL76OTs7vU0\nj+E7hyYBWUzOFIkijtI1DGSV/MIChWOgrNSNw5BTlEtMTsuDP5VwTOjibhBcrc2B\nFea7w5y+eMzHiGNWFNE0aW5nX2Xd4EELFYmZx8ruPUgN27mfT9CvQOxg9FBT+7h4\nvmJp0pzCnwKBgFhCZqFTuofqmKqbety9acmhvhpFasFGcAj0xlYmfZ5a8QV9F7Ma\nODwmRlUfp1AOkv3V5vgAB/ORalnH2MUimhydVipJB05YIZ8tpz21t8k4HJJt6v0L\n2ii274SUeFcv3FF+yaaxFi8XPE1B0j07xEQkfTR8K8TJWsqHDg2xH8FBAoGBAKfd\n9EKqsFjr3hg5seuyOLEve1qh6h7jyoC2agIgr9+E+AxeVRwM4Dcf3/dDoPwfmBfq\n9ajobiIFEC3L9JEiWdZlOpGybiCu0y+WTeFnFrsR0UC5yaMakyWBnenrnLeeoYHw\nP1VvSlSwYrZjEcRpuTDapTtJKhiU1Tr0jTNXmJmZAoGAZRcXd+zBm3spGwGmopD5\nVduVSHESwUucfM6g/UDkzpmRkTWjUAOo7gl/jT4ycoM2IGIjQO8/3hOapoCPmI/v\nSoKlQMJsqDMCz2Y8yOCSPes0sI00qpbXijmkes8eegIc6309l7bgPzlqQXdH2dGW\nCKbtjgeGUVDEXl8fD77sazc=\n-----END PRIVATE KEY-----\n").replace(/\\n/g, "\n").replace(/\\n/g, "\n").replace(/\\n/g, "\n");

const LIMITE = Number(process.env.LIMITE_DLH || 50);


// =========================
// GOOGLE DRIVE AUTH
// =========================
const googleAuth =
  GOOGLE_CLIENT_EMAIL && GOOGLE_PRIVATE_KEY
    ? new google.auth.GoogleAuth({
        credentials: {
          client_email: GOOGLE_CLIENT_EMAIL,
          private_key: GOOGLE_PRIVATE_KEY
        },
        scopes: ["https://www.googleapis.com/auth/drive"]
      })
    : null;

const drive = googleAuth ? google.drive({ version: "v3", auth: googleAuth }) : null;

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
  if (v === null || v === undefined) return NaN;
  return parseFloat(String(v).replace(",", "."));
}

function fmt2(n) {
  return Number(Number(n).toFixed(2));
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
  const [ano, mes, dia] = String(dataISO).split("-");
  if (!ano || !mes || !dia) return "";
  return `${dia}.${mes}.${ano}`;
}

function formatarDataISOParaBR(dataISO) {
  if (!dataISO) return "";
  const [ano, mes, dia] = String(dataISO).split("-");
  if (!ano || !mes || !dia) return String(dataISO);
  return `${dia}/${mes}/${ano}`;
}

function obterHojeISO() {
  const hoje = new Date();
  const ano = hoje.getFullYear();
  const mes = String(hoje.getMonth() + 1).padStart(2, "0");
  const dia = String(hoje.getDate()).padStart(2, "0");
  return `${ano}-${mes}-${dia}`;
}

function mesmaData(dataIsoA, dataIsoB) {
  return String(dataIsoA || "").slice(0, 10) === String(dataIsoB || "").slice(0, 10);
}

function montarNomePadrao(dlh, serie, dataISO) {
  const tag = normalizarDLH(dlh);
  const dataFormatada = formatarDataISOParaNome(dataISO);

  if (!tag || !serie || !dataFormatada) return null;
  return `${tag}_${serie}_${dataFormatada}.pdf`;
}

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

function extrairNumeroCertificado(texto) {
  const match = String(texto || "").match(/LT\s*[-–]?\s*(\d{3})\s*(\d{3})/i);
  if (!match) return "";
  return `LT-${match[1]} ${match[2]}`;
}

function extrairDadosNomeArquivo(nome) {
  const base = String(nome || "").replace(".pdf", "");
  const partes = base.split("_");

  return {
    dlh: partes[0] ? normalizarDLH(partes[0]) : "",
    serie: partes[1] ? soDigitos(partes[1]) : "",
    data: partes[2] ? partes[2].split(".").reverse().join("-") : ""
  };
}

function avaliarDivergencia(dlh, serie) {
  const tag = normalizarDLH(dlh);
  const serieEsperada = tag ? MAPA_LOGGERS_DLH[tag] || null : null;

  if (!tag) {
    return {
      divergente: true,
      serie_esperada: null,
      motivo_divergencia: "DLH inválido"
    };
  }

  if (!serieEsperada) {
    return {
      divergente: true,
      serie_esperada: null,
      motivo_divergencia: "DLH não encontrado na base"
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

function somenteNumeroBR(texto) {
  return /^-?\d+,\d+$|^-?\d+\.\d+$|^-?\d+$/.test(String(texto || "").trim());
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

function numeroNaFaixa(linha, xMin, xMax) {
  const candidatos = linha.items.filter(i => {
    return somenteNumeroBR(i.text) && i.x >= xMin && i.x <= xMax;
  });

  if (!candidatos.length) return null;

  candidatos.sort((a, b) => a.x - b.x);
  return candidatos[0];
}
// =========================
// DRIVE
// =========================
async function buscarArquivosDriveDLH() {
  if (drive) {
    const arquivos = [];
    let pageToken = null;

    do {
      const response = await drive.files.list({
        q: `'${FOLDER_ID_DLH}' in parents and mimeType='application/pdf' and trashed=false`,
        fields: "nextPageToken, files(id, name, mimeType)",
        pageSize: 1000,
        pageToken: pageToken || undefined,
        supportsAllDrives: true,
        includeItemsFromAllDrives: true
      });

      arquivos.push(...(response.data.files || []));
      pageToken = response.data.nextPageToken || null;
    } while (pageToken);

    return arquivos;
  }

  const arquivos = [];
  let pageToken = null;

  do {
    const url =
      `https://www.googleapis.com/drive/v3/files` +
      `?q='${FOLDER_ID_DLH}'+in+parents+and+mimeType='application/pdf'+and+trashed=false` +
      `&key=${GOOGLE_API_KEY}` +
      `&fields=nextPageToken,files(id,name,mimeType)` +
      `&pageSize=1000` +
      `&supportsAllDrives=true` +
      `&includeItemsFromAllDrives=true` +
      `${pageToken ? `&pageToken=${pageToken}` : ""}`;

    const res = await fetch(url);
    const data = await res.json();

    if (data.error) {
      throw new Error(data.error.message || "Erro ao buscar arquivos no Google Drive");
    }

    arquivos.push(...(data.files || []));
    pageToken = data.nextPageToken || null;
  } while (pageToken);

  return arquivos;
}

async function baixarArquivoDrive(fileId) {
  if (drive) {
    const response = await drive.files.get(
      {
        fileId,
        alt: "media",
        supportsAllDrives: true
      },
      {
        responseType: "arraybuffer"
      }
    );

    return Buffer.from(response.data);
  }

  const url = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${GOOGLE_API_KEY}&supportsAllDrives=true`;

  const res = await fetch(url);

  if (!res.ok) {
    throw new Error(`Falha ao baixar arquivo do Drive: ${res.status}`);
  }

  return Buffer.from(await res.arrayBuffer());
}
// =========================
// PDF
// =========================
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

function extrairMetadadosDLH(texto) {
  let dlh = "";
  let serie = "";
  let data = "";
  let certificado = "";

  const textoStr = String(texto || "");

  let m = textoStr.match(/Número de Série:\s*(\d{6,10})/i);
  if (m) {
    serie = m[1];
  }

  if (!serie) {
    m = textoStr.match(/(\d{6,10})\s+DLH[-\s]?(\d{4})/i);
    if (m) {
      serie = m[1];
      dlh = `DLH-${m[2]}`;
    }
  }

  if (!dlh) {
    m = textoStr.match(/DLH[-\s]?(\d{4})/i);
    if (m) {
      dlh = `DLH-${m[1]}`;
    }
  }

  const idxData = textoStr.search(/Data da Calibração/i);

  if (idxData >= 0) {
    const trecho = textoStr.slice(idxData, idxData + 500);
    const dataMatch = trecho.match(/(\d{2}\/\d{2}\/\d{4})/);

    if (dataMatch) {
      data = formatarDataBRparaISO(dataMatch[1]);
    }
  }

  if (!data) {
    const dataMatch = textoStr.match(/(\d{2}\/\d{2}\/\d{4})/);
    if (dataMatch) {
      data = formatarDataBRparaISO(dataMatch[1]);
    }
  }

  certificado = extrairNumeroCertificado(textoStr);

  return {
    dlh: dlh ? soDigitos(dlh).padStart(4, "0") : "",
    serie: serie ? soDigitos(serie) : "",
    data: data || "",
    certificado: certificado || ""
  };
}
// =========================
// EXTRAÇÃO TABELA DLH
// =========================
async function extrairTabelaDLH(buffer) {
  const { linhas } = await extrairTextoELinhasDoPDF(buffer);

  const pontosUmidade = [];
  const pontosTemperatura = [];

  let modo = null; // "UMIDADE" | "TEMPERATURA"

  for (const linha of linhas) {
    const t = (linha.texto || "").toUpperCase();

    // Detecta seção
    if (t.includes("MEDIDOR DE UMIDADE")) {
      modo = "UMIDADE";
      continue;
    }
    if (t.includes("MEDIDOR DE TEMPERATURA") || t.includes("SENSOR IN")) {
      modo = "TEMPERATURA";
      continue;
    }

    const indicado = numeroNaFaixa(linha, 40, 95);
    const padrao = numeroNaFaixa(linha, 150, 225);
    const incerteza = numeroNaFaixa(linha, 400, 455);

    if (!indicado || !padrao || !incerteza) continue;

    const indicadoNum = parseBR(indicado.text);
    const padraoNum = parseBR(padrao.text);
    const incertezaNum = parseBR(incerteza.text);

    if (
      Number.isNaN(indicadoNum) ||
      Number.isNaN(padraoNum) ||
      Number.isNaN(incertezaNum)
    ) {
      continue;
    }

    // =========================
    // UMIDADE
    // =========================
    if (modo === "UMIDADE") {
      const erroUmidade = numeroNaFaixa(linha, 250, 315);
      if (!erroUmidade) continue;

      const erroNum = parseBR(erroUmidade.text);
      if (Number.isNaN(erroNum)) continue;

      if (padraoNum >= 0 && padraoNum <= 100 && pontosUmidade.length < 3) {
        pontosUmidade.push({
          ponto: pontosUmidade.length + 1,
          indicado: fmt2(indicadoNum),
          padrao: fmt2(padraoNum),
          erro: fmt2(erroNum),
          incerteza: fmt2(Math.abs(incertezaNum)),
          soma: fmt2(Math.abs(erroNum) + Math.abs(incertezaNum))
        });
      }
    }

    // =========================
    // TEMPERATURA
    // =========================
    if (modo === "TEMPERATURA") {
      const erroTemperatura = numeroNaFaixa(linha, 330, 390);
      if (!erroTemperatura) continue;

      const erroNum = parseBR(erroTemperatura.text);
      if (Number.isNaN(erroNum)) continue;

      if (padraoNum >= -30 && padraoNum <= 70 && pontosTemperatura.length < 4) {
        pontosTemperatura.push({
          ponto: pontosTemperatura.length + 1,
          indicado: fmt2(indicadoNum),
          padrao: fmt2(padraoNum),
          erro: fmt2(erroNum),
          incerteza: fmt2(Math.abs(incertezaNum)),
          soma: fmt2(Math.abs(erroNum) + Math.abs(incertezaNum))
        });
      }
    }
  }

  if (pontosUmidade.length < 3 || pontosTemperatura.length < 4) {
    return {
      ok: false,
      pontos_umidade: pontosUmidade,
      pontos_temperatura: pontosTemperatura
    };
  }

  return {
    ok: true,
    pontos_umidade: pontosUmidade.slice(0, 3),
    pontos_temperatura: pontosTemperatura.slice(0, 4)
  };
}
// =========================
// PROCESSAMENTO
// =========================
async function processarPDFDLH(fileId, nomeArquivo = "") {
  try {
    const buffer = await baixarArquivoDrive(fileId);
    const { texto } = await extrairTextoELinhasDoPDF(buffer);

    const meta = extrairMetadadosDLH(texto);
    const tabela = await extrairTabelaDLH(buffer);

    if (!tabela.ok) {
      return {
        status: "ERRO",
        pontos_umidade: tabela.pontos_umidade || [],
        pontos_temperatura: tabela.pontos_temperatura || [],
        certificado: meta.certificado || "",
        meta,
        debug: tabela.debug
      };
    }

    const todos = [...tabela.pontos_umidade, ...tabela.pontos_temperatura];
    const aprovado = todos.every(p => p.soma <= 0.5);

    return {
      status: aprovado ? "APROVADO" : "REPROVADO",
      pontos_umidade: tabela.pontos_umidade,
      pontos_temperatura: tabela.pontos_temperatura,
      certificado: meta.certificado || "",
      meta
    };
  } catch (e) {
    return {
      status: "ERRO",
      pontos_umidade: [],
      pontos_temperatura: [],
      certificado: "",
      meta: {},
      debug: { erro: e.message }
    };
  }
}

// =========================
// BANCO
// =========================
async function buscarIdsBancoDLH() {
  const ids = new Set();
  const limit = 1000;
  let offset = 0;

  while (true) {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh?select=id&limit=${limit}&offset=${offset}`,
      { headers: supabaseHeaders() }
    );

    const data = await r.json();

    if (!Array.isArray(data) || data.length === 0) break;

    for (const item of data) {
      ids.add(item.id);
    }

    if (data.length < limit) break;
    offset += limit;
  }

  return ids;
}

async function buscarIdsExcluidosDLH() {
  const ids = new Set();
  const limit = 1000;
  let offset = 0;

  while (true) {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh_excluidos?select=id&limit=${limit}&offset=${offset}`,
      { headers: supabaseHeaders() }
    );

    const data = await r.json();

    if (!Array.isArray(data) || data.length === 0) break;

    for (const item of data) {
      ids.add(item.id);
    }

    if (data.length < limit) break;
    offset += limit;
  }

  return ids;
}

async function contarCertificadosBancoDLH() {
  const r = await fetch(
    `${SUPABASE_URL}/rest/v1/certificados_dlh?select=id`,
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

  return Number(contentRange.split("/")[1] || 0);
}

// =========================
// SYNC
// =========================
async function executarSyncDLH() {
  const idsBanco = await buscarIdsBancoDLH();
  const idsExcluidos = await buscarIdsExcluidosDLH();
  const arquivosDrive = await buscarArquivosDriveDLH();

  let processados = 0;
  const erros = [];

  for (const f of arquivosDrive) {
    if (idsBanco.has(f.id)) continue;
    if (idsExcluidos.has(f.id)) continue;
    if (processados >= LIMITE) break;

    try {
      const proc = await processarPDFDLH(f.id, f.name);
      const meta = proc.meta || {};

      if (!meta.dlh || !meta.serie || !meta.data) {
        erros.push({
          arquivo: f.name,
          motivo: "Metadados insuficientes",
          meta
        });
        continue;
      }

      const val = verificarValidade(meta.data);
      const divergencia = avaliarDivergencia(meta.dlh, meta.serie);
      const nomePadronizado = montarNomePadrao(meta.dlh, meta.serie, meta.data);

      const respInsert = await fetch(`${SUPABASE_URL}/rest/v1/certificados_dlh`, {
        method: "POST",
        headers: supabaseHeaders(),
        body: JSON.stringify({
          id: f.id,
          nome_original: f.name,
          nome_download: nomePadronizado || f.name,
          dlh: meta.dlh,
          serie: meta.serie,
          data: meta.data,
          certificado: proc.certificado || meta.certificado || "",
          status: proc.status,
          validade: val.valido,
          vencimento: val.vencimento,
          mes_ano_validade: val.mes_ano,
          pontos_umidade: proc.pontos_umidade || [],
          pontos_temperatura: proc.pontos_temperatura || [],
          divergente: divergencia.divergente,
          serie_esperada: divergencia.serie_esperada,
          motivo_divergencia: divergencia.motivo_divergencia,
          criado_em: new Date().toISOString()
        })
      });

      if (!respInsert.ok) {
        const erroInsert = await respInsert.text();

        erros.push({
          arquivo: f.name,
          motivo: erroInsert
        });

        continue;
      }

      processados++;
    } catch (e) {
      erros.push({
        arquivo: f.name,
        motivo: e.message
      });
    }
  }

  return {
    sucesso: true,
    processados,
    erros
  };
}

// =========================
// ROTAS
// =========================
app.get("/", (req, res) => {
  res.send("API DLH OK 🚀");
});

app.get("/dlh/status", async (req, res) => {
  try {
    const totalBanco = await contarCertificadosBancoDLH();
    const idsBanco = await buscarIdsBancoDLH();
    const idsExcluidos = await buscarIdsExcluidosDLH();
    const arquivosDrive = await buscarArquivosDriveDLH();

    const totalDrive = arquivosDrive.length;
    const faltantes = arquivosDrive.filter(
      f => !idsBanco.has(f.id) && !idsExcluidos.has(f.id)
    ).length;

    res.json({
      total_drive: totalDrive,
      total_banco: totalBanco,
      faltantes
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/sync", async (req, res) => {
  try {
    const resultado = await executarSyncDLH();
    res.json(resultado);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/certificados", async (req, res) => {
  try {
    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh?select=*&order=data.desc&limit=${limit}&offset=${offset}`,
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

app.get("/dlh/divergentes", async (req, res) => {
  try {
    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh?select=*&divergente=eq.true&order=data.desc&limit=${limit}&offset=${offset}`,
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

app.get("/dlh/pendentes", async (req, res) => {
  try {
    const idsBanco = await buscarIdsBancoDLH();
    const idsExcluidos = await buscarIdsExcluidosDLH();
    const arquivosDrive = await buscarArquivosDriveDLH();

    const pendentes = arquivosDrive
      .filter(f => !idsBanco.has(f.id) && !idsExcluidos.has(f.id))
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

app.get("/dlh/teste/:id", async (req, res) => {
  try {
    const resultado = await processarPDFDLH(req.params.id);
    res.json(resultado);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/download/:id", async (req, res) => {
  try {
    const id = req.params.id;

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh?id=eq.${id}&select=id,nome_download`,
      { headers: supabaseHeaders() }
    );

    const data = await r.json();

    if (!data || data.length === 0) {
      return res.status(404).send("Arquivo não encontrado");
    }

    const nome = data[0].nome_download || `DLH_${id}.pdf`;
    const buffer = await baixarArquivoDrive(id);

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${nome}"`);
    res.send(buffer);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.delete("/dlh/certificados/:id", async (req, res) => {
  try {
    const id = req.params.id;

    const busca = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh?id=eq.${id}&select=*`,
      { headers: supabaseHeaders() }
    );

    const registros = await busca.json();

    if (!registros || registros.length === 0) {
      return res.status(404).json({ erro: "Certificado DLH não encontrado" });
    }

    const certificado = registros[0];

    const podeExcluir =
      certificado.divergente === true ||
      certificado.duplicado === true ||
      !!certificado.motivo_divergencia;

    if (!podeExcluir) {
      return res.status(400).json({
        erro: "Exclusão permitida apenas para certificados divergentes"
      });
    }

    const insereHistorico = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh_excluidos`,
      {
        method: "POST",
        headers: supabaseHeaders(),
        body: JSON.stringify({
          ...certificado,
          motivo_exclusao: `Exclusão manual pelo Lovable - ${certificado.motivo_divergencia || "Divergente"}`,
          excluido_em: new Date().toISOString()
        })
      }
    );

    if (!insereHistorico.ok) {
      const erroHistorico = await insereHistorico.text();

      return res.status(500).json({
        erro: `Falha ao gravar histórico: ${erroHistorico}`
      });
    }

    const del = await fetch(`${SUPABASE_URL}/rest/v1/certificados_dlh?id=eq.${id}`, {
      method: "DELETE",
      headers: supabaseHeaders()
    });

    if (!del.ok) {
      const erroBanco = await del.text();

      return res.status(500).json({
        erro: `Falha ao excluir da base principal: ${erroBanco}`
      });
    }

    res.json({
      sucesso: true,
      mensagem: "Certificado DLH removido da base e registrado no histórico",
      id,
      nome_original: certificado.nome_original
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/relatorio-dia/dados", async (req, res) => {
  try {
    const dataRelatorio = req.query.data || obterHojeISO();

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh?select=*&order=dlh.asc,data.asc,serie.asc`,
      { headers: supabaseHeaders() }
    );

    const todos = await r.json();
    const dados = (Array.isArray(todos) ? todos : []).filter(item =>
      mesmaData(item.criado_em, dataRelatorio)
    );

    res.json({
      data_relatorio: dataRelatorio,
      total: dados.length,
      registros: dados
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/exportar-csv", async (req, res) => {
  try {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh?select=*&order=data.desc`,
      { headers: supabaseHeaders() }
    );

    const data = await r.json();

    const linhas = [
      [
        "DLH",
        "Serie",
        "Data",
        "Validade",
        "Certificado",
        "Status",
        "Divergente"
      ],
      ...(Array.isArray(data) ? data : []).map(d => [
        d.dlh || "",
        d.serie || "",
        formatarDataISOParaBR(d.data),
        d.mes_ano_validade || "",
        d.certificado || "",
        d.status || "",
        d.divergente ? "SIM" : "NÃO"
      ])
    ];

    const csv = linhas
      .map(l => l.map(v => `"${String(v).replace(/"/g, '""')}"`).join(";"))
      .join("\n");

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", "attachment; filename=certificados_dlh.csv");
    res.send("\uFEFF" + csv);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.listen(PORT, () => console.log(`Servidor DLH rodando na porta ${PORT} 🚀`));
