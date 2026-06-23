import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import fetch from "node-fetch";
import { google } from "googleapis";
import { getDocument } from "pdfjs-dist/legacy/build/pdf.mjs";
import ExcelJS from "exceljs";
import os from "os";
import crypto from "crypto";
import zlib from "zlib";
import archiver from "archiver";
import dns from "dns";
import { MAPA_LOGGERS_DLH, normalizarDLH } from "./mapa-loggers-dlh.js";

dns.setDefaultResultOrder("ipv4first");

const app = express();
app.use(express.json({ limit: "2mb" }));
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", process.env.CORS_ORIGIN || "*");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const MODELO_RELATORIO_PATH = path.join(__dirname, "modelo-relatorio.xlsx");


// =========================
// CONFIG DLH
// =========================
const PORT = Number(process.env.PORT || 3001);
const SUPABASE_URL = String(process.env.SUPABASE_URL || "").replace(/\/+$/, "");
const SUPABASE_KEY = process.env.SUPABASE_KEY || "";
const FOLDER_ID_DLH = process.env.FOLDER_ID_DLH || "";
const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY || "";
const DOWNLOADS_FOLDER_ID_DLH =
  process.env.DOWNLOADS_FOLDER_ID_DLH || FOLDER_ID_DLH || "";
const GOOGLE_CLIENT_EMAIL = process.env.GOOGLE_CLIENT_EMAIL || "";
const GOOGLE_PRIVATE_KEY = (process.env.GOOGLE_PRIVATE_KEY || "").replace(/\\n/g, "\n");
const LIMITE = Number(process.env.LIMITE_DLH || 50);
const STATUS_CACHE_MS = Number(process.env.STATUS_CACHE_MS || 30000);
const IDS_CACHE_MS = Number(process.env.IDS_CACHE_MS || 600000);
const AUTO_SYNC_ENABLED = String(process.env.AUTO_SYNC_ENABLED || "true") === "true";
const AUTO_SYNC_START_DELAY_MS = Number(process.env.AUTO_SYNC_START_DELAY_MS || 15000);
const CRITERIOS_CACHE_MS = Number(process.env.CRITERIOS_CACHE_MS || 600000);
const CERTIFICADOS_DLH_LISTA_SELECT = [
  "id", "nome_original", "nome_download", "dlh", "serie", "data",
  "certificado", "status", "validade", "vencimento", "mes_ano_validade",
  "divergente", "duplicado", "serie_esperada", "motivo_divergencia", "criado_em"
].join(",");

let statusCacheDLH = { expiraEm: 0, valor: null };
let idsBancoCacheDLH = { expiraEm: 0, valor: null };
let idsExcluidosCacheDLH = { expiraEm: 0, valor: null };
let syncLocalDLHEmExecucao = false;
let criteriosCacheDLH = { expiraEm: 0, valor: null };


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
  const headers = {
    apikey: SUPABASE_KEY,
    "Content-Type": "application/json"
  };

  // Somente chaves antigas JWT usam Bearer
  if (SUPABASE_KEY.split(".").length === 3) {
    headers.Authorization = `Bearer ${SUPABASE_KEY}`;
  }

  return headers;
}

function validarConfiguracaoBasica() {
  if (!SUPABASE_URL) throw new Error("SUPABASE_URL não configurada no Render");
  if (!SUPABASE_KEY) throw new Error("SUPABASE_KEY não configurada no Render");
}

function validarListaSupabase(response, data, contexto) {
  if (response.ok && Array.isArray(data)) return data;

  const detalhe =
    data?.message ||
    data?.error_description ||
    data?.error ||
    data?.hint ||
    response.statusText ||
    "resposta inesperada";

  throw new Error(`${contexto}: ${detalhe} (HTTP ${response.status})`);
}

function dividirEmLotes(lista, tamanho = 100) {
  const lotes = [];
  for (let i = 0; i < lista.length; i += tamanho) {
    lotes.push(lista.slice(i, i + tamanho));
  }
  return lotes;
}

function invalidarCachesDLH() {
  statusCacheDLH = { expiraEm: 0, valor: null };
}

async function contarTabela(tabela) {
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${tabela}?select=id`, {
    headers: { ...supabaseHeaders(), Prefer: "count=exact", Range: "0-0" }
  });

  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    throw new Error(
      `Supabase ${tabela}: ${data.message || data.error || response.statusText} (HTTP ${response.status})`
    );
  }

  return Number(response.headers.get("content-range")?.split("/")[1] || 0);
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



// =========================
// FILTROS POR PERIODO / DOWNLOAD EM MASSA
// =========================
const downloadJobsDLH = new Map();

function limparNomeArquivo(nome) {
  return String(nome || "certificado.pdf")
    .replace(/[\/:*?"<>|]/g, "-")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 180) || "certificado.pdf";
}

function normalizarListaQuery(valor) {
  if (Array.isArray(valor)) return valor.flatMap(normalizarListaQuery);
  return String(valor || "")
    .split(/[;,\n]/)
    .map(v => v.trim())
    .filter(Boolean);
}

function normalizarDataQuery(valor) {
  const v = String(valor || "").trim();
  if (!v) return "";
  if (/^\d{4}-\d{2}-\d{2}$/.test(v)) return v;
  return formatarDataBRparaISO(v);
}

function montarUrlCertificadosPorPeriodo({ tabela, campoEquipamento, equipamentos, testeInicio, testeFim }) {
  const params = new URLSearchParams();
  params.set("select", CERTIFICADOS_DLH_LISTA_SELECT);
  params.set(campoEquipamento, `in.(${equipamentos.map(v => String(v).replace(/[()"]/g, "")).join(",")})`);
  params.set("data", `lte.${testeFim}`);
  params.set("vencimento", `gte.${testeInicio}`);
  params.append("order", `${campoEquipamento}.asc`);
  params.append("order", "data.asc");
  return `${SUPABASE_URL}/rest/v1/${tabela}?${params.toString()}`;
}

async function buscarCertificadosPorPeriodoEmLotes({
  tabela,
  campoEquipamento,
  equipamentos,
  testeInicio,
  testeFim
}) {
  validarConfiguracaoBasica();

  if (testeInicio > testeFim) {
    throw new Error("A data inicial do teste não pode ser posterior à data final");
  }

  const normalizados = [...new Set(
    equipamentos
      .map(normalizarDLH)
      .filter(Boolean)
  )];

  const resultados = [];
  for (const lote of dividirEmLotes(normalizados, 100)) {
    let inicio = 0;
    const tamanhoPagina = 1000;

    while (true) {
      const response = await fetch(
        montarUrlCertificadosPorPeriodo({
          tabela,
          campoEquipamento,
          equipamentos: lote,
          testeInicio,
          testeFim
        }),
        {
          headers: {
            ...supabaseHeaders(),
            Range: `${inicio}-${inicio + tamanhoPagina - 1}`
          }
        }
      );
      const data = await response.json();
      const pagina = validarListaSupabase(response, data, `Supabase ${tabela}`);
      resultados.push(...pagina);
      if (pagina.length < tamanhoPagina) break;
      inicio += tamanhoPagina;
    }
  }

  return resultados;
}

async function buscarCertificadosPorIdsEmLotes(tabela, campos, ids) {
  validarConfiguracaoBasica();
  const resultados = [];
  const idsUnicos = [...new Set(ids.map(String).filter(Boolean))];

  for (const lote of dividirEmLotes(idsUnicos, 100)) {
    const params = new URLSearchParams();
    params.set("select", campos);
    params.set("id", `in.(${lote.map(v => v.replace(/[()"]/g, "")).join(",")})`);
    const response = await fetch(
      `${SUPABASE_URL}/rest/v1/${tabela}?${params.toString()}`,
      { headers: supabaseHeaders() }
    );
    const data = await response.json();
    resultados.push(...validarListaSupabase(response, data, `Supabase ${tabela}`));
  }

  return resultados;
}

function crc32(buf) {
  let crc = -1;
  for (let i = 0; i < buf.length; i++) {
    crc ^= buf[i];
    for (let j = 0; j < 8; j++) crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
  }
  return (crc ^ -1) >>> 0;
}

function dosDateTime(date = new Date()) {
  const year = Math.max(date.getFullYear(), 1980);
  const dosTime = (date.getHours() << 11) | (date.getMinutes() << 5) | Math.floor(date.getSeconds() / 2);
  const dosDate = ((year - 1980) << 9) | ((date.getMonth() + 1) << 5) | date.getDate();
  return { dosTime, dosDate };
}

function criarZip(entries) {
  const locals = [];
  const centrals = [];
  let offset = 0;
  const { dosTime, dosDate } = dosDateTime();

  for (const entry of entries) {
    const nameBuffer = Buffer.from(limparNomeArquivo(entry.name), "utf8");
    const data = Buffer.isBuffer(entry.data) ? entry.data : Buffer.from(entry.data || "");
    const compressed = zlib.deflateRawSync(data, { level: 1 });
    const crc = crc32(data);

    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0);
    local.writeUInt16LE(20, 4);
    local.writeUInt16LE(0x0800, 6);
    local.writeUInt16LE(8, 8);
    local.writeUInt16LE(dosTime, 10);
    local.writeUInt16LE(dosDate, 12);
    local.writeUInt32LE(crc, 14);
    local.writeUInt32LE(compressed.length, 18);
    local.writeUInt32LE(data.length, 22);
    local.writeUInt16LE(nameBuffer.length, 26);
    local.writeUInt16LE(0, 28);
    locals.push(local, nameBuffer, compressed);

    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50, 0);
    central.writeUInt16LE(20, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt16LE(0x0800, 8);
    central.writeUInt16LE(8, 10);
    central.writeUInt16LE(dosTime, 12);
    central.writeUInt16LE(dosDate, 14);
    central.writeUInt32LE(crc, 16);
    central.writeUInt32LE(compressed.length, 20);
    central.writeUInt32LE(data.length, 24);
    central.writeUInt16LE(nameBuffer.length, 28);
    central.writeUInt16LE(0, 30);
    central.writeUInt16LE(0, 32);
    central.writeUInt16LE(0, 34);
    central.writeUInt16LE(0, 36);
    central.writeUInt32LE(0, 38);
    central.writeUInt32LE(offset, 42);
    centrals.push(central, nameBuffer);

    offset += local.length + nameBuffer.length + compressed.length;
  }

  const centralSize = centrals.reduce((sum, b) => sum + b.length, 0);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(0, 4);
  end.writeUInt16LE(0, 6);
  end.writeUInt16LE(entries.length, 8);
  end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(centralSize, 12);
  end.writeUInt32LE(offset, 16);
  end.writeUInt16LE(0, 20);

  return Buffer.concat([...locals, ...centrals, end]);
}

async function esperar(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function executarGoogleComRetry(operacao, tentativas = 5) {
  let ultimoErro;

  for (let tentativa = 0; tentativa < tentativas; tentativa++) {
    try {
      return await operacao();
    } catch (e) {
      ultimoErro = e;
      const status = Number(e?.response?.status || e?.code || 0);
      const mensagem = String(e?.message || "").toLowerCase();
      const recuperavel =
        [408, 429, 500, 502, 503, 504].includes(status) ||
        mensagem.includes("premature close") ||
        mensagem.includes("socket hang up") ||
        mensagem.includes("econnreset") ||
        mensagem.includes("etimedout");

      if (!recuperavel || tentativa === tentativas - 1) throw e;
      await esperar(Math.min(15000, 1000 * (2 ** tentativa)));
    }
  }

  throw ultimoErro;
}

async function baixarArquivoDriveComRetry(fileId, tentativas = 4) {
  let ultimoErro;
  for (let i = 0; i < tentativas; i++) {
    try {
      return await baixarArquivoDrive(fileId);
    } catch (e) {
      ultimoErro = e;
      const status = Number(e?.response?.status || String(e.message || "").match(/\b(403|429|5\d\d)\b/)?.[1] || 0);
      if (![403, 429, 500, 502, 503, 504].includes(status) && i > 0) break;
      await esperar(Math.min(30000, (2 ** i) * 1000 + Math.floor(Math.random() * 500)));
    }
  }
  throw ultimoErro;
}

async function salvarZipNoDriveDLH(zipPath, nomeArquivo) {
  if (!drive) throw new Error("Credenciais Google Drive não configuradas");
  if (!DOWNLOADS_FOLDER_ID_DLH) throw new Error("DOWNLOADS_FOLDER_ID_DLH ou FOLDER_ID_DLH não configurado");

  const response = await drive.files.create({
    requestBody: {
      name: nomeArquivo,
      parents: [DOWNLOADS_FOLDER_ID_DLH],
      mimeType: "application/zip"
    },
    media: {
      mimeType: "application/zip",
      body: fs.createReadStream(zipPath)
    },
    fields: "id, name, webViewLink, webContentLink",
    supportsAllDrives: true
  });

  return response.data;
}

async function criarZipNoDiscoDLH(registros, zipPath, job) {
  const pastaTemporaria = await fs.promises.mkdtemp(path.join(os.tmpdir(), "certificados-dlh-"));
  const arquivos = [];

  try {
    for (let index = 0; index < registros.length; index++) {
      const item = registros[index];
      try {
        const buffer = await baixarArquivoDriveComRetry(item.id);
        const nome = limparNomeArquivo(
          item.nome_download || item.nome_original || `DLH_${item.id}.pdf`
        );
        const caminho = path.join(pastaTemporaria, `${String(index + 1).padStart(4, "0")}_${nome}`);
        await fs.promises.writeFile(caminho, buffer);
        arquivos.push({ caminho, nome });
        job.processados++;
      } catch (e) {
        job.falhas++;
        job.erros.push({
          id: item.id,
          nome: item.nome_download || item.nome_original,
          erro: e.message
        });
      }
      job.atualizado_em = new Date().toISOString();
    }

    if (!arquivos.length) {
      throw new Error("Nenhum certificado foi baixado com sucesso");
    }

    await new Promise((resolve, reject) => {
      const output = fs.createWriteStream(zipPath);
      const archive = archiver("zip", { zlib: { level: 1 } });
      output.on("close", resolve);
      output.on("error", reject);
      archive.on("warning", reject);
      archive.on("error", reject);
      archive.pipe(output);
      for (const arquivo of arquivos) {
        archive.file(arquivo.caminho, { name: arquivo.nome });
      }
      archive.finalize();
    });
  } finally {
    await fs.promises.rm(pastaTemporaria, { recursive: true, force: true });
  }
}

async function processarDownloadMassaDLH(jobId, registros) {
  const job = downloadJobsDLH.get(jobId);
  job.status = "processando";
  job.total = registros.length;
  job.atualizado_em = new Date().toISOString();

  const nomeArquivo = `CERTIFICADOS_DLH_${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}.zip`;
  const zipPath = path.join(os.tmpdir(), nomeArquivo);
  try {
    await criarZipNoDiscoDLH(registros, zipPath, job);
    const arquivoDrive = await salvarZipNoDriveDLH(zipPath, nomeArquivo);

    job.status = "concluido";
    job.arquivo_zip_nome = nomeArquivo;
    job.arquivo_zip_drive_id = arquivoDrive.id || null;
    job.arquivo_zip_link = arquivoDrive.webViewLink || arquivoDrive.webContentLink || null;
    job.atualizado_em = new Date().toISOString();
  } finally {
    await fs.promises.rm(zipPath, { force: true });
  }
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

function numerosDaLinha(linha) {
  return linha.items
    .filter(i => somenteNumeroBR(i.text))
    .sort((a, b) => a.x - b.x)
    .map(i => ({
      text: i.text,
      valor: parseBR(i.text),
      x: i.x,
      y: i.y
    }))
    .filter(i => !Number.isNaN(i.valor));
}

// =========================
// DRIVE
// =========================
async function buscarArquivosDriveDLH() {
  if (drive) {
    const arquivos = [];
    let pageToken = null;

    do {
      const response = await executarGoogleComRetry(() =>
        drive.files.list({
          q: `'${FOLDER_ID_DLH}' in parents and mimeType='application/pdf' and trashed=false`,
          fields: "nextPageToken, files(id, name,mimeType)",
          pageSize: 1000,
          pageToken: pageToken || undefined,
          supportsAllDrives: true,
          includeItemsFromAllDrives: true
        })
      );

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
  const { texto, linhas } = await extrairTextoELinhasDoPDF(buffer);

  const textoCompleto = String(texto || "");
  const textoLinhas = textoCompleto
    .split(/\n+/)
    .map(l => String(l || "").trim())
    .filter(Boolean);

  const pontosUmidade = [];
  const pontosTemperatura = [];

  const padroesUmidade = [10, 50, 90];
  const padroesTemperatura = [-20, 0, 15, 60];

  function extrairNumeros(textoLinha) {
    return (String(textoLinha || "").match(/-?\d+(?:[,.]\d+)?/g) || [])
      .map(v => parseBR(v))
      .filter(v => !Number.isNaN(v));
  }

  function adicionarUmidade(valores) {
    if (pontosUmidade.length >= 3) return false;
    if (!Array.isArray(valores) || valores.length < 3) return false;

    const padraoNum = padroesUmidade[pontosUmidade.length];
    const indicadoNum = valores[0];

    let erroNum;
    let incertezaNum;

    // Formato do texto extraído pelo pdfjs:
    // indicado | erro | incerteza | k
    // Exemplo: 14,0 4,0 0,4 2,00
    erroNum = valores[1];
    incertezaNum = valores[2];

    const coerenteCurto = Math.abs((indicadoNum - padraoNum) - erroNum) <= 1.5;

    // Formato completo, quando o PDF preserva todas as colunas:
    // indicado | padrão | erro | temperatura ref. | incerteza | k
    // Exemplo: 14,0 10,0 4,0 20 0,4 2,00
    if (!coerenteCurto && valores.length >= 5 && Math.round(valores[1]) === padraoNum) {
      erroNum = valores[2];
      incertezaNum = valores[4];
    }

    const coerente =
      Math.abs((indicadoNum - padraoNum) - erroNum) <= 1.5 &&
      indicadoNum >= 0 &&
      indicadoNum <= 100 &&
      Math.abs(erroNum) <= 20 &&
      Math.abs(incertezaNum) <= 10;

    if (!coerente) return false;

    pontosUmidade.push({
      ponto: pontosUmidade.length + 1,
      indicado: fmt2(indicadoNum),
      padrao: fmt2(padraoNum),
      erro: fmt2(erroNum),
      incerteza: fmt2(Math.abs(incertezaNum)),
      soma: fmt2(Math.abs(erroNum) + Math.abs(incertezaNum))
    });

    return true;
  }

  function adicionarTemperatura(valores) {
    if (pontosTemperatura.length >= 4) return false;
    if (!Array.isArray(valores) || valores.length < 2) return false;

    const padraoNum = padroesTemperatura[pontosTemperatura.length];
    const indicadoNum = valores[0];

    let erroNum;
    let incertezaNum;

    // Formato do texto extraído pelo pdfjs:
    // indicado | incerteza | k
    // Exemplo: -19,9 0,2 2,00
    incertezaNum = valores[1];
    erroNum = fmt2(indicadoNum - padraoNum);

    // Formato completo:
    // indicado | padrão | erro | incerteza | k
    // Exemplo: -19,9 -20,0 0,1 0,2 2,00
    if (valores.length >= 4 && Math.round(valores[1]) === padraoNum) {
      erroNum = valores[2];
      incertezaNum = valores[3];
    }

    const coerente =
      indicadoNum >= -40 &&
      indicadoNum <= 80 &&
      Math.abs(erroNum) <= 5 &&
      Math.abs(incertezaNum) <= 5;

    if (!coerente) return false;

    pontosTemperatura.push({
      ponto: pontosTemperatura.length + 1,
      indicado: fmt2(indicadoNum),
      padrao: fmt2(padraoNum),
      erro: fmt2(erroNum),
      incerteza: fmt2(Math.abs(incertezaNum)),
      soma: fmt2(Math.abs(erroNum) + Math.abs(incertezaNum))
    });

    return true;
  }

  // =====================================================
  // LEITURA PRINCIPAL POR TEXTO EXTRAÍDO
  // Funciona para o modelo Escala:
  //
  // Teste (%u.r.) ...
  // 14,0 4,0 0,4 2,00 ∞
  // 51,0 1,0 0,8 2,00 ∞
  // 86,5 -3,5 1,5 2,00 ∞
  //
  // Teste (ºC) ...
  // -19,9 0,2 2,00 ∞
  // 0,0 0,2 2,00 ∞
  // 14,9 0,2 2,00 ∞
  // 59,7 0,2 2,00 ∞
  // =====================================================

  let modo = "";

  for (const linha of textoLinhas) {
    const upper = linha.toUpperCase();

    if (
      upper.includes("TESTE (%U.R.)") ||
      upper.includes("TESTE (% U.R.)") ||
      upper.includes("TESTE (%UR)") ||
      (upper.includes("TESTE") && upper.includes("U.R"))
    ) {
      modo = "UMIDADE";
      continue;
    }

    if (
      upper.includes("TESTE (ºC)") ||
      upper.includes("TESTE (°C)") ||
      upper.includes("TESTE (OC)") ||
      (upper.includes("TESTE") && (upper.includes("ºC") || upper.includes("°C")))
    ) {
      modo = "TEMPERATURA";
      continue;
    }

    if (
      upper.includes("A INCERTEZA") ||
      upper.includes("OBSERVAÇÕES") ||
      upper.includes("OBSERVACOES") ||
      upper.includes("DATA DA CALIBRAÇÃO") ||
      upper.includes("DATA DA CALIBRACAO")
    ) {
      modo = "";
    }

    const valores = extrairNumeros(linha);

    if (modo === "UMIDADE" && pontosUmidade.length < 3) {
      adicionarUmidade(valores);
      continue;
    }

    if (modo === "TEMPERATURA" && pontosTemperatura.length < 4) {
      adicionarTemperatura(valores);
      continue;
    }
  }

  // =====================================================
  // FALLBACK POR LINHAS AGRUPADAS
  // Caso textoCompleto venha diferente, usa as linhas do pdfjs
  // =====================================================

  if (pontosUmidade.length < 3 || pontosTemperatura.length < 4) {
    const backupUmidade = [];
    const backupTemperatura = [];
    let modoLinha = "";

    function pushBackupUmidade(valores) {
      if (backupUmidade.length >= 3 || valores.length < 3) return false;

      const padraoNum = padroesUmidade[backupUmidade.length];
      const indicadoNum = valores[0];

      let erroNum = valores[1];
      let incertezaNum = valores[2];

      if (Math.abs((indicadoNum - padraoNum) - erroNum) > 1.5 && valores.length >= 5 && Math.round(valores[1]) === padraoNum) {
        erroNum = valores[2];
        incertezaNum = valores[4];
      }

      const ok =
        indicadoNum >= 0 &&
        indicadoNum <= 100 &&
        Math.abs(erroNum) <= 20 &&
        Math.abs(incertezaNum) <= 10 &&
        Math.abs((indicadoNum - padraoNum) - erroNum) <= 1.5;

      if (!ok) return false;

      backupUmidade.push({
        ponto: backupUmidade.length + 1,
        indicado: fmt2(indicadoNum),
        padrao: fmt2(padraoNum),
        erro: fmt2(erroNum),
        incerteza: fmt2(Math.abs(incertezaNum)),
        soma: fmt2(Math.abs(erroNum) + Math.abs(incertezaNum))
      });

      return true;
    }

    function pushBackupTemperatura(valores) {
      if (backupTemperatura.length >= 4 || valores.length < 2) return false;

      const padraoNum = padroesTemperatura[backupTemperatura.length];
      const indicadoNum = valores[0];

      let erroNum = fmt2(indicadoNum - padraoNum);
      let incertezaNum = valores[1];

      if (valores.length >= 4 && Math.round(valores[1]) === padraoNum) {
        erroNum = valores[2];
        incertezaNum = valores[3];
      }

      const ok =
        indicadoNum >= -40 &&
        indicadoNum <= 80 &&
        Math.abs(erroNum) <= 5 &&
        Math.abs(incertezaNum) <= 5;

      if (!ok) return false;

      backupTemperatura.push({
        ponto: backupTemperatura.length + 1,
        indicado: fmt2(indicadoNum),
        padrao: fmt2(padraoNum),
        erro: fmt2(erroNum),
        incerteza: fmt2(Math.abs(incertezaNum)),
        soma: fmt2(Math.abs(erroNum) + Math.abs(incertezaNum))
      });

      return true;
    }

    for (const linha of linhas) {
      const linhaTexto = String(linha.texto || "");
      const upper = linhaTexto.toUpperCase();

      if (
        upper.includes("TESTE (%U.R.)") ||
        upper.includes("TESTE (% U.R.)") ||
        (upper.includes("TESTE") && upper.includes("U.R"))
      ) {
        modoLinha = "UMIDADE";
        continue;
      }

      if (
        upper.includes("TESTE (ºC)") ||
        upper.includes("TESTE (°C)") ||
        (upper.includes("TESTE") && (upper.includes("ºC") || upper.includes("°C")))
      ) {
        modoLinha = "TEMPERATURA";
        continue;
      }

      if (
        upper.includes("A INCERTEZA") ||
        upper.includes("OBSERVAÇÕES") ||
        upper.includes("OBSERVACOES") ||
        upper.includes("DATA DA CALIBRAÇÃO") ||
        upper.includes("DATA DA CALIBRACAO")
      ) {
        modoLinha = "";
      }

      const valores = extrairNumeros(linhaTexto);

      if (modoLinha === "UMIDADE") {
        pushBackupUmidade(valores);
      }

      if (modoLinha === "TEMPERATURA") {
        pushBackupTemperatura(valores);
      }
    }

    if (pontosUmidade.length < 3 && backupUmidade.length >= 3) {
      pontosUmidade.length = 0;
      pontosUmidade.push(...backupUmidade.slice(0, 3));
    }

    if (pontosTemperatura.length < 4 && backupTemperatura.length >= 4) {
      pontosTemperatura.length = 0;
      pontosTemperatura.push(...backupTemperatura.slice(0, 4));
    }
  }

  if (pontosUmidade.length < 3 || pontosTemperatura.length < 4) {
    return {
      ok: false,
      pontos_umidade: pontosUmidade,
      pontos_temperatura: pontosTemperatura,
      debug: {
        motivo: "Quantidade insuficiente de pontos DLH",
        umidade_encontrada: pontosUmidade.length,
        temperatura_encontrada: pontosTemperatura.length,
        texto: textoCompleto,
        linhas: linhas.map(l => l.texto)
      }
    };
  }

  return {
    ok: true,
    pontos_umidade: pontosUmidade,
    pontos_temperatura: pontosTemperatura
  };
}


// =========================
// CRITÉRIOS DE ACEITAÇÃO
// =========================
async function buscarCriteriosCalibracao() {
  if (criteriosCacheDLH.valor && criteriosCacheDLH.expiraEm > Date.now()) {
    return criteriosCacheDLH.valor;
  }

  try {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/criterios_calibracao?id=eq.1&select=id,limite_temperatura,limite_umidade,atualizado_em`,
      { headers: supabaseHeaders() }
    );

    const data = await r.json();
    const registros = validarListaSupabase(r, data, "Supabase critérios DLH");
    const criterios = {
      limite_temperatura: Number(data[0].limite_temperatura ?? 0.5),
      limite_umidade: Number(data[0].limite_umidade ?? 5.0),
      atualizado_em: data[0].atualizado_em || null
    };

    criteriosCacheDLH = {
      valor: criterios,
      expiraEm: Date.now() + CRITERIOS_CACHE_MS
    };
    return criterios;
  } catch (e) {
    console.log("Erro ao buscar critérios de calibração, usando padrão:", e.message);

    return {
      limite_temperatura: 0.5,
      limite_umidade: 5.0
    };
  }
}

function avaliarStatusDLH(pontosUmidade = [], pontosTemperatura = [], criterios = {}) {
  const limiteTemperatura = Number(criterios.limite_temperatura ?? 0.5);
  const limiteUmidade = Number(criterios.limite_umidade ?? 5.0);

  const umidadeOk = (Array.isArray(pontosUmidade) ? pontosUmidade : []).every(
    p => Number(p.soma) <= limiteUmidade
  );

  const temperaturaOk = (Array.isArray(pontosTemperatura) ? pontosTemperatura : []).every(
    p => Number(p.soma) <= limiteTemperatura
  );

  return {
    aprovado: umidadeOk && temperaturaOk,
    umidade_ok: umidadeOk,
    temperatura_ok: temperaturaOk,
    limite_temperatura: limiteTemperatura,
    limite_umidade: limiteUmidade
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
    const fallbackNome = extrairDadosNomeArquivo(nomeArquivo);

    if (!meta.dlh && fallbackNome.dlh) meta.dlh = soDigitos(fallbackNome.dlh).padStart(4, "0");
    if (!meta.serie && fallbackNome.serie) meta.serie = fallbackNome.serie;
    if (!meta.data && fallbackNome.data) meta.data = fallbackNome.data;

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

    const criterios = await buscarCriteriosCalibracao();
    const avaliacao = avaliarStatusDLH(
      tabela.pontos_umidade,
      tabela.pontos_temperatura,
      criterios
    );

    return {
      status: avaliacao.aprovado ? "APROVADO" : "REPROVADO",
      pontos_umidade: tabela.pontos_umidade,
      pontos_temperatura: tabela.pontos_temperatura,
      certificado: meta.certificado || "",
      criterios_aceitacao: {
        limite_temperatura: avaliacao.limite_temperatura,
        limite_umidade: avaliacao.limite_umidade
      },
      avaliacao,
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
  if (idsBancoCacheDLH.valor && idsBancoCacheDLH.expiraEm > Date.now()) {
    return idsBancoCacheDLH.valor;
  }

  const ids = new Set();
  const limit = 1000;
  let offset = 0;

  while (true) {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh?select=id&limit=${limit}&offset=${offset}`,
      { headers: supabaseHeaders() }
    );

    const data = await r.json();
    validarListaSupabase(r, data, "Supabase certificados_dlh");

    if (!Array.isArray(data) || data.length === 0) break;

    for (const item of data) {
      ids.add(item.id);
    }

    if (data.length < limit) break;
    offset += limit;
  }

  idsBancoCacheDLH = { valor: ids, expiraEm: Date.now() + IDS_CACHE_MS };
  return ids;
}

async function buscarIdsExcluidosDLH() {
  if (idsExcluidosCacheDLH.valor && idsExcluidosCacheDLH.expiraEm > Date.now()) {
    return idsExcluidosCacheDLH.valor;
  }

  const ids = new Set();
  const limit = 1000;
  let offset = 0;

  while (true) {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh_excluidos?select=id&limit=${limit}&offset=${offset}`,
      { headers: supabaseHeaders() }
    );

    const data = await r.json();
    validarListaSupabase(r, data, "Supabase certificados_dlh_excluidos");

    if (!Array.isArray(data) || data.length === 0) break;

    for (const item of data) {
      ids.add(item.id);
    }

    if (data.length < limit) break;
    offset += limit;
  }

  idsExcluidosCacheDLH = { valor: ids, expiraEm: Date.now() + IDS_CACHE_MS };
  return ids;
}

async function contarCertificadosBancoDLH() {
  return contarTabela("certificados_dlh");
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

      const certificadoFinal = proc.certificado || meta.certificado || "";

      const val = verificarValidade(meta.data);
      const divergencia = avaliarDivergencia(meta.dlh, meta.serie);
      const nomePadronizado = montarNomePadrao(meta.dlh, meta.serie, meta.data);

      const dupCheck = await fetch(
        `${SUPABASE_URL}/rest/v1/certificados_dlh?select=id&dlh=eq.${encodeURIComponent(meta.dlh)}&serie=eq.${encodeURIComponent(meta.serie)}&data=eq.${encodeURIComponent(meta.data)}&certificado=eq.${encodeURIComponent(certificadoFinal)}`,
        { headers: supabaseHeaders() }
      );

      const duplicados = await dupCheck.json();
      const duplicado = Array.isArray(duplicados) && duplicados.length > 0;

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
          certificado: certificadoFinal,
          status: proc.status,
          validade: val.valido,
          vencimento: val.vencimento,
          mes_ano_validade: val.mes_ano,
          pontos_umidade: proc.pontos_umidade || [],
          pontos_temperatura: proc.pontos_temperatura || [],
          divergente: divergencia.divergente || duplicado,
          duplicado: duplicado,
          serie_esperada: divergencia.serie_esperada,
          motivo_divergencia: duplicado
            ? "Certificado duplicado"
            : divergencia.motivo_divergencia,
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

      idsBanco.add(f.id);
      processados++;
      invalidarCachesDLH();
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

async function executarSyncAutomaticoDLH() {
  if (syncLocalDLHEmExecucao) return;
  syncLocalDLHEmExecucao = true;
  let deveContinuar = false;

  try {
    const resultado = await executarSyncDLH();
    invalidarCachesDLH();
    deveContinuar = resultado.processados > 0;
  } catch (e) {
    console.log("Erro na sincronização automática DLH:", e.message);
  } finally {
    syncLocalDLHEmExecucao = false;
    if (deveContinuar) {
      setTimeout(() => executarSyncAutomaticoDLH(), 3000);
    }
  }
}

// =========================
// REPROCESSAMENTO
// =========================
async function executarReprocessDLH(limit = 50, offset = 0) {
  const r = await fetch(
    `${SUPABASE_URL}/rest/v1/certificados_dlh?select=id,nome_original,dlh,serie,data,certificado&limit=${limit}&offset=${offset}`,
    { headers: supabaseHeaders() }
  );

  const lista = await r.json();
  validarListaSupabase(r, lista, "Supabase certificados_dlh para reprocessamento");
  let processados = 0;
  const erros = [];

  for (const item of lista) {
    try {
      const proc = await processarPDFDLH(item.id, item.nome_original);
      const meta = proc.meta || {};

      const dataFinal = meta.data || item.data;
      const dlhFinal = meta.dlh || item.dlh;
      const serieFinal = meta.serie || item.serie;
      const certificadoFinal = proc.certificado || meta.certificado || item.certificado || "";

      const val = verificarValidade(dataFinal);
      const divergencia = avaliarDivergencia(dlhFinal, serieFinal);
      const nomePadronizado = montarNomePadrao(dlhFinal, serieFinal, dataFinal);

      const dupCheck = await fetch(
        `${SUPABASE_URL}/rest/v1/certificados_dlh?select=id&dlh=eq.${encodeURIComponent(dlhFinal)}&serie=eq.${encodeURIComponent(serieFinal)}&data=eq.${encodeURIComponent(dataFinal)}&certificado=eq.${encodeURIComponent(certificadoFinal)}`,
        { headers: supabaseHeaders() }
      );

      const duplicados = await dupCheck.json();

      const duplicado =
        Array.isArray(duplicados) &&
        duplicados.some(d => d.id !== item.id);

      const update = await fetch(
        `${SUPABASE_URL}/rest/v1/certificados_dlh?id=eq.${item.id}`,
        {
          method: "PATCH",
          headers: supabaseHeaders(),
          body: JSON.stringify({
            nome_download: nomePadronizado,
            dlh: dlhFinal,
            serie: serieFinal,
            data: dataFinal,
            certificado: certificadoFinal,
            status: proc.status,
            validade: val.valido,
            vencimento: val.vencimento,
            mes_ano_validade: val.mes_ano,
            pontos_umidade: proc.pontos_umidade || [],
            pontos_temperatura: proc.pontos_temperatura || [],
            duplicado: duplicado,
            divergente: divergencia.divergente || duplicado,
            serie_esperada: divergencia.serie_esperada,
            motivo_divergencia: duplicado
              ? "Certificado duplicado"
              : divergencia.motivo_divergencia
          })
        }
      );

      if (!update.ok) {
        erros.push({
          arquivo: item.nome_original,
          erro: await update.text()
        });
        continue;
      }

      processados++;
    } catch (e) {
      erros.push({
        arquivo: item.nome_original,
        erro: e.message
      });
    }
  }

  return {
    mensagem: "Reprocessamento DLH concluído",
    processados,
    offset,
    proximo_offset: offset + processados,
    erros
  };
}

// =========================
// ROTAS
// =========================
app.get("/", (req, res) => {
  res.send("API DLH OK 🚀");
});


app.get("/dlh/criterios", async (req, res) => {
  try {
    const criterios = await buscarCriteriosCalibracao();
    res.setHeader("Cache-Control", "private, max-age=300");
    res.json(criterios);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.patch("/dlh/criterios", async (req, res) => {
  try {
    const limiteTemperatura = Number(req.body?.limite_temperatura);
    const limiteUmidade = Number(req.body?.limite_umidade);
    const alteradoPor = String(req.body?.alterado_por || "").trim() || null;

    if (!Number.isFinite(limiteTemperatura) || limiteTemperatura <= 0 || limiteTemperatura > 100) {
      return res.status(400).json({ erro: "limite_temperatura deve ser maior que zero e menor ou igual a 100." });
    }

    if (!Number.isFinite(limiteUmidade) || limiteUmidade <= 0 || limiteUmidade > 100) {
      return res.status(400).json({ erro: "limite_umidade deve ser maior que zero e menor ou igual a 100." });
    }

    const anterior = await buscarCriteriosCalibracao();
    const atualizadoEm = new Date().toISOString();
    const resposta = await fetch(
      `${SUPABASE_URL}/rest/v1/criterios_calibracao?on_conflict=id`,
      {
        method: "POST",
        headers: {
          ...supabaseHeaders(),
          Prefer: "resolution=merge-duplicates,return=representation"
        },
        body: JSON.stringify({
          id: 1,
          limite_temperatura: limiteTemperatura,
          limite_umidade: limiteUmidade,
          atualizado_em: atualizadoEm
        })
      }
    );
    const data = await resposta.json();
    const registros = validarListaSupabase(resposta, data, "Supabase atualização do DMA DLH");

    const historico = await fetch(`${SUPABASE_URL}/rest/v1/criterios_calibracao_historico`, {
      method: "POST",
      headers: supabaseHeaders(),
      body: JSON.stringify({
        modulo: "DLH",
        limite_temperatura_anterior: anterior.limite_temperatura,
        limite_temperatura_novo: limiteTemperatura,
        limite_umidade_anterior: anterior.limite_umidade,
        limite_umidade_novo: limiteUmidade,
        alterado_por: alteradoPor,
        alterado_em: atualizadoEm
      })
    });
    if (!historico.ok) {
      throw new Error(`DMA atualizado, mas houve falha ao gravar histórico: ${await historico.text()}`);
    }

    criteriosCacheDLH = {
      valor: {
        limite_temperatura: limiteTemperatura,
        limite_umidade: limiteUmidade,
        atualizado_em: atualizadoEm
      },
      expiraEm: Date.now() + CRITERIOS_CACHE_MS
    };

    res.json({
      sucesso: true,
      criterios: registros[0],
      reprocessamento_automatico: false
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/criterios/historico", async (req, res) => {
  try {
    const limit = Math.min(100, Math.max(1, Number(req.query.limit || 20)));
    const response = await fetch(
      `${SUPABASE_URL}/rest/v1/criterios_calibracao_historico?modulo=eq.DLH&select=id,limite_temperatura_anterior,limite_temperatura_novo,limite_umidade_anterior,limite_umidade_novo,alterado_por,alterado_em&order=alterado_em.desc&limit=${limit}`,
      { headers: supabaseHeaders() }
    );
    const data = await response.json();
    const registros = validarListaSupabase(response, data, "Supabase histórico DMA DLH");
    res.setHeader("Cache-Control", "private, max-age=300");
    res.json({ total: registros.length, registros });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/status", async (req, res) => {
  try {
    if (statusCacheDLH.valor && statusCacheDLH.expiraEm > Date.now()) {
      res.setHeader("Cache-Control", "public, max-age=15, s-maxage=30, stale-while-revalidate=60");
      return res.json(statusCacheDLH.valor);
    }

    const totalBanco = await contarCertificadosBancoDLH();
    const totalExcluidos = await contarTabela("certificados_dlh_excluidos");
    const arquivosDrive = await buscarArquivosDriveDLH();

    const totalDrive = arquivosDrive.length;
    const faltantes = Math.max(0, totalDrive - totalBanco - totalExcluidos);

    const payload = {
      total_drive: totalDrive,
      total_banco: totalBanco,
      faltantes
    };

    statusCacheDLH = { valor: payload, expiraEm: Date.now() + STATUS_CACHE_MS };
    res.setHeader("Cache-Control", "public, max-age=15, s-maxage=30, stale-while-revalidate=60");
    res.json(payload);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/status/supabase", async (req, res) => {
  try {
    const totalBanco = await contarCertificadosBancoDLH();
    res.json({ ok: true, servico: "supabase", total_banco: totalBanco });
  } catch (e) {
    res.status(500).json({ ok: false, servico: "supabase", erro: e.message });
  }
});

app.get("/dlh/status/google", async (req, res) => {
  try {
    if (!googleAuth || !drive) {
      throw new Error("GOOGLE_CLIENT_EMAIL ou GOOGLE_PRIVATE_KEY não configurado");
    }

    await executarGoogleComRetry(() => googleAuth.getAccessToken());
    const response = await executarGoogleComRetry(() =>
      drive.files.list({
        q: `'${FOLDER_ID_DLH}' in parents and trashed=false`,
        fields: "files(id)",
        pageSize: 1,
        supportsAllDrives: true,
        includeItemsFromAllDrives: true
      })
    );

    res.json({
      ok: true,
      servico: "google_drive",
      pasta_acessivel: true,
      arquivos_encontrados_no_teste: response.data.files?.length || 0
    });
  } catch (e) {
    res.status(500).json({ ok: false, servico: "google_drive", erro: e.message });
  }
});

app.get("/dlh/sync", async (req, res) => {
  try {
    if (syncLocalDLHEmExecucao) {
      return res.json({ mensagem: "Processamento DLH já está em execução" });
    }

    res.status(202).json({ mensagem: "Processamento DLH iniciado" });
    executarSyncAutomaticoDLH();
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/reprocess", async (req, res) => {
  try {
    const limit = Number(req.query.limit || 50);
    const offset = Number(req.query.offset || 0);

    const resultado = await executarReprocessDLH(limit, offset);
    res.json(resultado);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/certificados", async (req, res) => {
  try {
    const listaEquipamentos = normalizarListaQuery(req.query.equipamentos || req.query.dlh || req.query.lista);
    const testeInicio = normalizarDataQuery(req.query.teste_inicio || req.query.data_inicio || req.query.inicio);
    const testeFim = normalizarDataQuery(req.query.teste_fim || req.query.data_fim || req.query.fim);

    if (listaEquipamentos.length && testeInicio && testeFim) {
      const data = await buscarCertificadosPorPeriodoEmLotes({
        tabela: "certificados_dlh",
        campoEquipamento: "dlh",
        equipamentos: listaEquipamentos,
        testeInicio,
        testeFim
      });
      return res.json({ total: data.length, registros: data });
    }

    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh?select=${CERTIFICADOS_DLH_LISTA_SELECT}&order=data.desc&limit=${limit}&offset=${offset}`,
      { headers: { ...supabaseHeaders(), Prefer: "count=exact" } }
    );
    const data = await r.json();
    res.setHeader("Cache-Control", "private, max-age=30");
    res.json({ total: Number(r.headers.get("content-range")?.split("/")[1] || 0), registros: data });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/certificados/:id/detalhes", async (req, res) => {
  try {
    const id = encodeURIComponent(req.params.id);
    const response = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh?id=eq.${id}&select=id,pontos_umidade,pontos_temperatura,status,certificado,data,vencimento`,
      { headers: supabaseHeaders() }
    );
    const data = await response.json();
    const registros = validarListaSupabase(response, data, "Supabase detalhes do certificado DLH");

    if (!registros.length) {
      return res.status(404).json({ erro: "Certificado DLH não encontrado" });
    }

    res.setHeader("Cache-Control", "private, max-age=3600");
    res.json(registros[0]);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/divergentes", async (req, res) => {
  try {
    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh?select=${CERTIFICADOS_DLH_LISTA_SELECT}&or=(divergente.eq.true,duplicado.eq.true,motivo_divergencia.not.is.null,status.eq.ERRO)&order=criado_em.desc&limit=${limit}&offset=${offset}`,
      {
        headers: {
          ...supabaseHeaders(),
          Prefer: "count=exact"
        }
      }
    );

    const data = await r.json();
    const contentRange = r.headers.get("content-range");
    const total = contentRange
      ? Number(contentRange.split("/")[1])
      : Array.isArray(data)
        ? data.length
        : 0;

    res.json({
      total,
      limit,
      offset,
      registros: Array.isArray(data) ? data : []
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


app.post("/dlh/downloads/massa", async (req, res) => {
  try {
    const ids = Array.isArray(req.body?.ids) ? req.body.ids.map(String).filter(Boolean) : [];
    const listaEquipamentos = Array.isArray(req.body?.equipamentos)
      ? req.body.equipamentos.map(String).filter(Boolean)
      : normalizarListaQuery(req.body?.equipamentos || req.body?.dlh || req.body?.lista);
    const testeInicio = normalizarDataQuery(req.body?.teste_inicio || req.body?.data_inicio || req.body?.inicio);
    const testeFim = normalizarDataQuery(req.body?.teste_fim || req.body?.data_fim || req.body?.fim);

    let registros = [];

    if (ids.length) {
      registros = await buscarCertificadosPorIdsEmLotes(
        "certificados_dlh",
        "id,nome_original,nome_download,dlh,serie,data,vencimento",
        ids
      );
    } else if (listaEquipamentos.length && testeInicio && testeFim) {
      registros = await buscarCertificadosPorPeriodoEmLotes({
        tabela: "certificados_dlh",
        campoEquipamento: "dlh",
        equipamentos: listaEquipamentos,
        testeInicio,
        testeFim
      });
    } else {
      return res.status(400).json({ erro: "Informe ids ou equipamentos + teste_inicio + teste_fim" });
    }

    const jobId = crypto.randomUUID();
    downloadJobsDLH.set(jobId, {
      id: jobId,
      tipo: "DLH",
      status: "pendente",
      total: Array.isArray(registros) ? registros.length : 0,
      processados: 0,
      falhas: 0,
      erros: [],
      arquivo_zip_nome: null,
      arquivo_zip_drive_id: null,
      arquivo_zip_link: null,
      criado_em: new Date().toISOString(),
      atualizado_em: new Date().toISOString()
    });

    setTimeout(() => {
      processarDownloadMassaDLH(jobId, Array.isArray(registros) ? registros : []).catch(e => {
        const job = downloadJobsDLH.get(jobId);
        if (job) {
          job.status = "erro";
          job.erro = e.message;
          job.atualizado_em = new Date().toISOString();
        }
      });
    }, 0);

    res.status(202).json({ job_id: jobId, total: registros.length });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/downloads/massa/:jobId", (req, res) => {
  const job = downloadJobsDLH.get(req.params.jobId);
  if (!job) return res.status(404).json({ erro: "Tarefa não encontrada" });
  res.json(job);
});

app.get("/dlh/downloads/massa/:jobId/arquivo", async (req, res) => {
  try {
    const job = downloadJobsDLH.get(req.params.jobId);
    if (!job) return res.status(404).json({ erro: "Tarefa não encontrada" });
    if (job.status !== "concluido" || !job.arquivo_zip_drive_id) {
      return res.status(409).json({ erro: "O arquivo ZIP ainda não está disponível" });
    }
    if (!drive) {
      return res.status(503).json({ erro: "Google Drive não configurado" });
    }

    const arquivo = await executarGoogleComRetry(() =>
      drive.files.get(
        {
          fileId: job.arquivo_zip_drive_id,
          alt: "media",
          supportsAllDrives: true
        },
        { responseType: "stream" }
      )
    );

    res.setHeader("Content-Type", "application/zip");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${job.arquivo_zip_nome || "certificados-dlh.zip"}"`
    );
    arquivo.data.on("error", e => {
      if (!res.headersSent) res.status(500).json({ erro: e.message });
      else res.destroy(e);
    });
    arquivo.data.pipe(res);
  } catch (e) {
    if (!res.headersSent) res.status(500).json({ erro: e.message });
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

    if (!Array.isArray(data) || data.length === 0) {
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

app.get("/dlh/historico-exclusoes", async (req, res) => {
  try {
    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh_excluidos?select=*&order=excluido_em.desc&limit=${limit}&offset=${offset}`,
      {
        headers: {
          ...supabaseHeaders(),
          Prefer: "count=exact"
        }
      }
    );

    const data = await r.json();
    const contentRange = r.headers.get("content-range");
    const total = contentRange ? Number(contentRange.split("/")[1]) : Array.isArray(data) ? data.length : 0;

    res.json({
      total,
      limit,
      offset,
      registros: Array.isArray(data) ? data : []
    });
  } catch (e) {
    res.json({
      total: 0,
      limit: Number(req.query.limit || 100),
      offset: Number(req.query.offset || 0),
      registros: []
    });
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

    if (!Array.isArray(registros) || registros.length === 0) {
      return res.status(404).json({ erro: "Certificado DLH não encontrado" });
    }

    const certificado = registros[0];

    const podeExcluir =
      certificado.divergente === true ||
      certificado.duplicado === true ||
      !!certificado.motivo_divergencia ||
      certificado.motivo_divergencia === "Série divergente" ||
      certificado.motivo_divergencia === "DLH inválido" ||
      certificado.motivo_divergencia === "DLH não encontrado na base";

    if (!podeExcluir) {
      return res.status(400).json({
        erro: "Exclusão permitida apenas para certificados divergentes, duplicados ou com inconsistência de DLH/série."
      });
    }

    let motivoExclusao = "Exclusão manual pelo Lovable";

    if (certificado.duplicado === true) {
      motivoExclusao += " - Certificado duplicado";
    } else if (certificado.motivo_divergencia) {
      motivoExclusao += ` - ${certificado.motivo_divergencia}`;
    } else if (certificado.divergente === true) {
      motivoExclusao += " - Divergente";
    }

    const insereHistorico = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh_excluidos`,
      {
        method: "POST",
        headers: supabaseHeaders(),
        body: JSON.stringify({
          ...certificado,
          motivo_exclusao: motivoExclusao,
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

    const del = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh?id=eq.${id}`,
      {
        method: "DELETE",
        headers: supabaseHeaders()
      }
    );

    if (!del.ok) {
      const erroBanco = await del.text();

      return res.status(500).json({
        erro: `Falha ao excluir da base principal: ${erroBanco}`
      });
    }

    idsBancoCacheDLH.valor?.delete(id);
    idsExcluidosCacheDLH.valor?.add(id);
    invalidarCachesDLH();

    res.json({
      sucesso: true,
      mensagem: "Certificado DLH excluído e registrado no histórico.",
      id,
      nome_original: certificado.nome_original,
      motivo_exclusao: motivoExclusao
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

    const criterios = await buscarCriteriosCalibracao();

    res.json({
      data_relatorio: dataRelatorio,
      total: dados.length,
      criterios_aceitacao: criterios,
      registros: dados
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/relatorio-dia/excel", async (req, res) => {
  try {
    const dataRelatorio = req.query.data || obterHojeISO();

    if (!fs.existsSync(MODELO_RELATORIO_PATH)) {
      throw new Error(
        "Arquivo modelo-relatorio.xlsx não encontrado na raiz do projeto. Adicione a planilha modelo no repositório com este nome."
      );
    }

    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(MODELO_RELATORIO_PATH);

    workbook.creator = "ITA FRIA";
    workbook.lastModifiedBy = "Sistema de Certificados DLH";
    workbook.created = new Date();
    workbook.modified = new Date();

    const sheet = workbook.worksheets[0];

    if (!sheet) {
      throw new Error("A planilha modelo não possui nenhuma aba.");
    }

    sheet.name = "Relatório DLH";

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh?select=*&order=dlh.asc,data.asc,serie.asc`,
      { headers: supabaseHeaders() }
    );

    const todos = await r.json();

    const dados = (Array.isArray(todos) ? todos : []).filter(item =>
      mesmaData(item.criado_em, dataRelatorio)
    );

    const criteriosRelatorio = await buscarCriteriosCalibracao();
    const limiteTemperaturaRelatorio = Number(criteriosRelatorio.limite_temperatura ?? 0.5);
    const limiteUmidadeRelatorio = Number(criteriosRelatorio.limite_umidade ?? 5.0);

    // =========================
    // CONFIGURAÇÃO DE IMPRESSÃO
    // =========================
    sheet.pageSetup = {
      paperSize: 9,
      orientation: "landscape",
      fitToPage: true,
      fitToWidth: 1,
      fitToHeight: 0,
      horizontalCentered: true,
      verticalCentered: false,
      margins: {
        left: 0.2,
        right: 0.2,
        top: 0.3,
        bottom: 0.3,
        header: 0.2,
        footer: 0.2
      }
    };

    sheet.headerFooter = sheet.headerFooter || {};

    const footerOriginal =
      sheet.headerFooter.oddFooter ||
      "&LResp.: ________________________________&CSistema de Gestão da Qualidade ITA FRIA&R Página &P de &N";

    sheet.headerFooter.oddFooter = footerOriginal;
    sheet.headerFooter.evenFooter = footerOriginal;
    sheet.headerFooter.firstFooter = footerOriginal;

    // =========================
    // HELPERS DO RELATÓRIO
    // =========================
    function valorTexto(cell) {
      const v = cell?.value;

      if (v === null || v === undefined) return "";

      if (typeof v === "object") {
        if (v.richText) return v.richText.map(t => t.text || "").join("");
        if (v.text) return String(v.text);
        if (v.result !== undefined) return String(v.result);
        if (v.formula) return String(v.formula);
      }

      return String(v);
    }

    function clonar(obj) {
      return obj ? JSON.parse(JSON.stringify(obj)) : {};
    }

    function aplicarBorda(cell, style = "thin") {
      cell.border = {
        top: { style },
        left: { style },
        bottom: { style },
        right: { style }
      };
    }

    function aplicarPadraoCelula(cell, baseStyle = null) {
      if (baseStyle && Object.keys(baseStyle).length > 0) {
        cell.style = clonar(baseStyle);
      }

      aplicarBorda(cell, "thin");

      cell.alignment = {
        horizontal: "center",
        vertical: "middle",
        wrapText: true
      };

      cell.font = {
        ...(cell.font || {}),
        name: "Arial",
        size: 8
      };
    }

    function numeroOuVazio(v) {
      if (v === null || v === undefined || v === "") return "";
      const n = Number(v);
      return Number.isNaN(n) ? v : n;
    }

    function textoLinha(row) {
      const valores = [];

      row.eachCell({ includeEmpty: true }, cell => {
        valores.push(valorTexto(cell));
      });

      return valores.join(" ").toUpperCase();
    }

    function encontrarLinhaCabecalhoTabela() {
      let melhor = 0;

      sheet.eachRow((row, rowNumber) => {
        const t = textoLinha(row);
        const temSerie = t.includes("SÉRIE") || t.includes("SERIE");
        const temTag = t.includes("TAG") || t.includes("DLH");
        const temCertificado = t.includes("CERTIFICADO");
        const temResultado = t.includes("RESULTADO");

        if (temSerie && temTag && (temCertificado || temResultado)) {
          melhor = rowNumber;
        }
      });

      return melhor || 7;
    }

    function encontrarLinhaAssinatura(aposLinha) {
      let linhaEncontrada = 0;

      sheet.eachRow((row, rowNumber) => {
        if (rowNumber <= aposLinha) return;

        const t = textoLinha(row);

        if (
          t.includes("RESP") ||
          t.includes("ASSIN") ||
          t.includes("ELABORADO") ||
          t.includes("REVISADO") ||
          t.includes("APROVADO")
        ) {
          if (!linhaEncontrada || rowNumber < linhaEncontrada) {
            linhaEncontrada = rowNumber;
          }
        }
      });

      return linhaEncontrada;
    }

    function atualizarCabecalhoModelo() {
      sheet.eachRow(row => {
        row.eachCell(cell => {
          const t = valorTexto(cell).toUpperCase();

          if (t.includes("DATA DO RELATÓRIO")) {
            cell.value = `Data do relatório: ${formatarDataISOParaBR(dataRelatorio)}`;
          }

          if (t.startsWith("DATA:") || t === "DATA") {
            cell.value = `Data: ${formatarDataISOParaBR(dataRelatorio)}`;
          }
        });
      });

      const a1 = valorTexto(sheet.getCell("A1")).toUpperCase();

      if (!a1 || a1.includes("REL")) {
        sheet.getCell("A1").value =
`REL 06GQ09
Versão: 01
Data: ${formatarDataISOParaBR(dataRelatorio)}`;
        sheet.getCell("A1").alignment = {
          horizontal: "left",
          vertical: "middle",
          wrapText: true
        };
        sheet.getCell("A1").font = {
          name: "Arial",
          size: 8,
          bold: true
        };
      }
    }

    function aplicarCorResultado(cell, valor) {
      const texto = String(valor || "").toUpperCase();

      if (texto === "APROVADO" || texto === "VÁLIDO" || texto === "VALIDO") {
        cell.fill = {
          type: "pattern",
          pattern: "solid",
          fgColor: { argb: "C6EFCE" }
        };
        cell.font = {
          ...(cell.font || {}),
          name: "Arial",
          size: 8,
          bold: true,
          color: { argb: "006100" }
        };
      }

      if (texto === "REPROVADO" || texto === "ERRO" || texto === "VENCIDO") {
        cell.fill = {
          type: "pattern",
          pattern: "solid",
          fgColor: { argb: "FFC7CE" }
        };
        cell.font = {
          ...(cell.font || {}),
          name: "Arial",
          size: 8,
          bold: true,
          color: { argb: "9C0006" }
        };
      }
    }

    function aplicarCorSoma(cell, valor, limite) {
      const n = Number(valor);
      const limiteAceitacao = Number(limite);

      if (Number.isNaN(n) || Number.isNaN(limiteAceitacao)) return;

      if (n <= limiteAceitacao) {
        cell.fill = {
          type: "pattern",
          pattern: "solid",
          fgColor: { argb: "C6EFCE" }
        };
        cell.font = {
          ...(cell.font || {}),
          name: "Arial",
          size: 8,
          bold: true,
          color: { argb: "006100" }
        };
      } else {
        cell.fill = {
          type: "pattern",
          pattern: "solid",
          fgColor: { argb: "FFC7CE" }
        };
        cell.font = {
          ...(cell.font || {}),
          name: "Arial",
          size: 8,
          bold: true,
          color: { argb: "9C0006" }
        };
      }
    }

    atualizarCabecalhoModelo();

    const headerRow = encontrarLinhaCabecalhoTabela();
    const dataStartRow = headerRow + 1;
    let footerStartRow = encontrarLinhaAssinatura(headerRow);

    if (!footerStartRow) {
      footerStartRow = Math.max(sheet.rowCount + 3, dataStartRow + 1);
    }

    const linhasDisponiveis = Math.max(footerStartRow - dataStartRow, 0);

    if (dados.length > linhasDisponiveis) {
      const quantidadeInserir = dados.length - linhasDisponiveis;
      sheet.spliceRows(
        footerStartRow,
        0,
        ...Array.from({ length: quantidadeInserir }, () => [])
      );
      footerStartRow += quantidadeInserir;
    }

    const baseStyle = clonar(sheet.getRow(dataStartRow).getCell(1).style);
    const colCount = 27;

    // Limpa somente a área de dados, preservando o cabeçalho e o rodapé/assinaturas do modelo.
    for (let rowNumber = dataStartRow; rowNumber < footerStartRow; rowNumber++) {
      const row = sheet.getRow(rowNumber);
      row.height = 18;

      for (let col = 1; col <= colCount; col++) {
        const cell = row.getCell(col);
        cell.value = "";
        aplicarPadraoCelula(cell, baseStyle);
      }
    }

    dados.forEach((c, index) => {
      const rowNumber = dataStartRow + index;
      const row = sheet.getRow(rowNumber);

      const u = Array.isArray(c.pontos_umidade) ? c.pontos_umidade : [];
      const t = Array.isArray(c.pontos_temperatura) ? c.pontos_temperatura : [];

      const u1 = u[0] || {};
      const u2 = u[1] || {};
      const u3 = u[2] || {};

      const t1 = t[0] || {};
      const t2 = t[1] || {};
      const t3 = t[2] || {};
      const t4 = t[3] || {};

      row.values = [
        "",
        c.serie || "",
        normalizarDLH(c.dlh) || c.dlh || "",
        formatarDataISOParaBR(c.data),
        c.mes_ano_validade || "",
        c.certificado || "",

        numeroOuVazio(u1.erro),
        numeroOuVazio(u1.incerteza),
        numeroOuVazio(u1.soma),

        numeroOuVazio(u2.erro),
        numeroOuVazio(u2.incerteza),
        numeroOuVazio(u2.soma),

        numeroOuVazio(u3.erro),
        numeroOuVazio(u3.incerteza),
        numeroOuVazio(u3.soma),

        numeroOuVazio(t1.erro),
        numeroOuVazio(t1.incerteza),
        numeroOuVazio(t1.soma),

        numeroOuVazio(t2.erro),
        numeroOuVazio(t2.incerteza),
        numeroOuVazio(t2.soma),

        numeroOuVazio(t3.erro),
        numeroOuVazio(t3.incerteza),
        numeroOuVazio(t3.soma),

        numeroOuVazio(t4.erro),
        numeroOuVazio(t4.incerteza),
        numeroOuVazio(t4.soma),

        c.status || ""
      ];

      row.height = 18;

      for (let col = 1; col <= colCount; col++) {
        const cell = row.getCell(col);
        aplicarPadraoCelula(cell, baseStyle);

        // Colunas de soma:
        // Umidade: 8, 11, 14 → limite dinâmico de umidade.
        // Temperatura: 17, 20, 23, 26 → limite dinâmico de temperatura.
        if ([8, 11, 14].includes(col)) {
          aplicarCorSoma(cell, cell.value, limiteUmidadeRelatorio);
        }

        if ([17, 20, 23, 26].includes(col)) {
          aplicarCorSoma(cell, cell.value, limiteTemperaturaRelatorio);
        }

        if (col === 27) {
          aplicarCorResultado(cell, cell.value);
        }
      }
    });

    if (dados.length === 0) {
      const row = sheet.getRow(dataStartRow);
      row.getCell(1).value = "Nenhum certificado DLH processado na data selecionada.";
      row.getCell(1).alignment = {
        horizontal: "center",
        vertical: "middle",
        wrapText: true
      };
      row.getCell(1).font = {
        name: "Arial",
        size: 9,
        italic: true
      };
    }

    // Repetição de cabeçalho e área de impressão.
    sheet.pageSetup.printTitlesRow = `1:${headerRow}`;

    const ultimaLinhaComDados = dataStartRow + Math.max(dados.length, 1) - 1;
    const ultimaLinha = Math.max(footerStartRow + 6, ultimaLinhaComDados + 8, sheet.rowCount);
    sheet.pageSetup.printArea = `A1:AA${ultimaLinha}`;

    sheet.views = [
      {
        state: "frozen",
        ySplit: headerRow
      }
    ];

    const nomeArquivo = `RELATORIO_DIARIO_DLH_${dataRelatorio}.xlsx`;

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );

    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${nomeArquivo}"`
    );

    await workbook.xlsx.write(res);
    return res.end();
  } catch (e) {
    console.error(e);

    return res.status(500).json({
      erro: e.message
    });
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
        "Divergente",
        "Duplicado"
      ],
      ...(Array.isArray(data) ? data : []).map(d => [
        d.dlh || "",
        d.serie || "",
        formatarDataISOParaBR(d.data),
        d.mes_ano_validade || "",
        d.certificado || "",
        d.status || "",
        d.divergente ? "SIM" : "NÃO",
        d.duplicado ? "SIM" : "NÃO"
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

app.listen(PORT, () => {
  console.log(`Servidor DLH rodando na porta ${PORT} 🚀`);

  if (AUTO_SYNC_ENABLED) {
    setTimeout(() => {
      executarSyncAutomaticoDLH();
    }, AUTO_SYNC_START_DELAY_MS);
  }
});
