import express from "express";
import fs from "fs";
import os from "os";
import path from "path";
import { chromium } from "playwright";
import { google } from "googleapis";
import { MAPA_LOGGERS, normalizarDLT } from "./mapa-loggers.js";
import fetch from "node-fetch";
import { getDocument } from "pdfjs-dist/legacy/build/pdf.mjs";
import ExcelJS from "exceljs";
import crypto from "crypto";
import zlib from "zlib";
import archiver from "archiver";
import dns from "dns";

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

// =========================
// CONFIG
// =========================
const PORT = Number(process.env.PORT || 3000);
const SUPABASE_URL = String(process.env.SUPABASE_URL || "").replace(/\/+$/, "");
const SUPABASE_KEY = process.env.SUPABASE_KEY || "";
const FOLDER_ID = process.env.FOLDER_ID || "";
const REPORTS_FOLDER_ID = process.env.REPORTS_FOLDER_ID || "";
const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY || "";
const DOWNLOADS_FOLDER_ID =
  process.env.DOWNLOADS_FOLDER_ID || REPORTS_FOLDER_ID || FOLDER_ID || "";
const GOOGLE_CLIENT_EMAIL = process.env.GOOGLE_CLIENT_EMAIL || "";
const GOOGLE_PRIVATE_KEY = (process.env.GOOGLE_PRIVATE_KEY || "").replace(/\\n/g, "\n");
const LOGO_URL = process.env.LOGO_URL || "";
const LIMITE = Number(process.env.LIMITE || 50);
const STATUS_CACHE_MS = Number(process.env.STATUS_CACHE_MS || 30000);
const IDS_CACHE_MS = Number(process.env.IDS_CACHE_MS || 600000);
const AUTO_SYNC_ENABLED = String(process.env.AUTO_SYNC_ENABLED || "true") === "true";
const AUTO_SYNC_START_DELAY_MS = Number(process.env.AUTO_SYNC_START_DELAY_MS || 15000);
const CRITERIOS_CACHE_MS = Number(process.env.CRITERIOS_CACHE_MS || 600000);
const CERTIFICADOS_LISTA_SELECT = [
  "id",
  "nome_original",
  "nome_download",
  "dlt",
  "serie",
  "data",
  "certificado",
  "status",
  "validade",
  "vencimento",
  "mes_ano_validade",
  "divergente",
  "duplicado",
  "serie_esperada",
  "motivo_divergencia",
  "criado_em"
].join(",");

let statusCache = { expiraEm: 0, valor: null };
let idsBancoCache = { expiraEm: 0, valor: null };
let idsExcluidosCache = { expiraEm: 0, valor: null };
let syncLocalEmExecucao = false;
let criteriosCache = { expiraEm: 0, valor: null };

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

function invalidarCaches() {
  statusCache = { expiraEm: 0, valor: null };
}

async function contarTabela(tabela) {
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${tabela}?select=id`, {
    headers: {
      ...supabaseHeaders(),
      Prefer: "count=exact",
      Range: "0-0"
    }
  });

  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    throw new Error(
      `Supabase ${tabela}: ${data.message || data.error || response.statusText} (HTTP ${response.status})`
    );
  }

  const total = response.headers.get("content-range")?.split("/")[1];
  return Number(total || 0);
}

async function buscarCriteriosCalibracao() {
  if (criteriosCache.valor && criteriosCache.expiraEm > Date.now()) {
    return criteriosCache.valor;
  }

  const response = await fetch(
    `${SUPABASE_URL}/rest/v1/criterios_calibracao?id=eq.1&select=id,limite_dlt,atualizado_em`,
    { headers: supabaseHeaders() }
  );
  const data = await response.json();
  const registros = validarListaSupabase(response, data, "Supabase critérios DLT");
  const criterios = {
    limite_dlt: Number(registros[0]?.limite_dlt ?? 0.5),
    atualizado_em: registros[0]?.atualizado_em || null
  };

  criteriosCache = { valor: criterios, expiraEm: Date.now() + CRITERIOS_CACHE_MS };
  return criterios;
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
    data: partes[2] ? partes[2].split(".").reverse().join("-") : ""
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

function formatarDataISOParaBR(dataISO) {
  if (!dataISO) return "";
  const [ano, mes, dia] = String(dataISO).split("-");
  if (!ano || !mes || !dia) return String(dataISO);
  return `${dia}/${mes}/${ano}`;
}

function formatarDataHoraBR(dataISO) {
  if (!dataISO) return "";
  const d = new Date(dataISO);
  return d.toLocaleString("pt-BR");
}

function montarNomePadrao(dlt, serie, dataISO) {
  const tag = normalizarDLT(dlt);
  const dataFormatada = formatarDataISOParaNome(dataISO);

  if (!tag || !serie || !dataFormatada) return null;
  return `${tag}_${serie}_${dataFormatada}.pdf`;
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

function escaparHtml(valor) {
  return String(valor ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}



// =========================
// FILTROS POR PERIODO / DOWNLOAD EM MASSA
// =========================
const downloadJobs = new Map();

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
  params.set("select", CERTIFICADOS_LISTA_SELECT);
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
      .map(normalizarDLT)
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

async function salvarZipNoDrive(zipPath, nomeArquivo) {
  if (!drive) throw new Error("Credenciais Google Drive não configuradas");
  if (!DOWNLOADS_FOLDER_ID) throw new Error("DOWNLOADS_FOLDER_ID, REPORTS_FOLDER_ID ou FOLDER_ID não configurado");

  const response = await drive.files.create({
    requestBody: {
      name: nomeArquivo,
      parents: [DOWNLOADS_FOLDER_ID],
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

async function criarZipNoDisco(registros, zipPath, job) {
  const pastaTemporaria = await fs.promises.mkdtemp(path.join(os.tmpdir(), "certificados-dlt-"));
  const arquivos = [];

  try {
    for (let index = 0; index < registros.length; index++) {
      const item = registros[index];
      try {
        const buffer = await baixarArquivoDriveComRetry(item.id);
        const nome = limparNomeArquivo(
          item.nome_download || item.nome_original || `DLT_${item.id}.pdf`
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

async function processarDownloadMassa(jobId, registros) {
  const job = downloadJobs.get(jobId);
  job.status = "processando";
  job.total = registros.length;
  job.atualizado_em = new Date().toISOString();

  const nomeArquivo = `CERTIFICADOS_DLT_${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}.zip`;
  const zipPath = path.join(os.tmpdir(), nomeArquivo);
  try {
    await criarZipNoDisco(registros, zipPath, job);
    const arquivoDrive = await salvarZipNoDrive(zipPath, nomeArquivo);

    job.status = "concluido";
    job.arquivo_zip_nome = nomeArquivo;
    job.arquivo_zip_drive_id = arquivoDrive.id || null;
    job.arquivo_zip_link = arquivoDrive.webViewLink || arquivoDrive.webContentLink || null;
    job.atualizado_em = new Date().toISOString();
  } finally {
    await fs.promises.rm(zipPath, { force: true });
  }
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

function somenteNumeroBR(texto) {
  return /^-?\d+,\d+$/.test((texto || "").trim());
}

function extrairNumeroCertificado(texto) {
  if (!texto) return "";

  const match = String(texto).match(/LT\s*[-–]?\s*(\d{3})\s*(\d{3})/i);

  if (!match) return "";

  return `LT-${match[1]} ${match[2]}`;
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
  const candidatos = linha.items.filter(
    i => somenteNumeroBR(i.text) && Math.abs(i.x - xColuna) <= faixa
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

function agruparRegistrosPorDLT(registros) {
  const grupos = new Map();

  for (const item of registros) {
    const chave = item.dlt || "SEM_DLT";
    if (!grupos.has(chave)) grupos.set(chave, []);
    grupos.get(chave).push(item);
  }

  return [...grupos.entries()].sort((a, b) => a[0].localeCompare(b[0], "pt-BR"));
}

function montarLogoHtml() {
  if (LOGO_URL) {
    return `<img src="${escaparHtml(LOGO_URL)}" alt="Logo" style="height:48px; object-fit:contain;" />`;
  }

  return `
    <div style="
      width: 150px;
      height: 48px;
      border: 1px solid #1f4e79;
      display:flex;
      align-items:center;
      justify-content:center;
      color:#1f4e79;
      font-weight:700;
      font-size:20px;
      letter-spacing:1px;
    ">
      ITA FRIA
    </div>
  `;
}

function montarHtmlRelatorioDia(registros, dataRelatorio, dma = 0.5) {
  const grupos = agruparRegistrosPorDLT(registros);

  const total = registros.length;
  const aprovados = registros.filter(r => r.status === "APROVADO").length;
  const reprovados = registros.filter(r => r.status === "REPROVADO").length;
  const erros = registros.filter(r => r.status === "ERRO").length;

  const secoes = grupos
    .map(([dlt, itens]) => {
      const linhas = itens
        .map((c, index) => {
          const pontos = Array.isArray(c.pontos) ? c.pontos : [];
          const p1 = pontos[0] || {};
          const p2 = pontos[1] || {};
          const p3 = pontos[2] || {};
          const p4 = pontos[3] || {};

          return `
            <tr>
              <td>${index + 1}</td>
              <td>${escaparHtml(c.serie)}</td>
              <td>${escaparHtml(normalizarDLT(c.dlt) || c.dlt)}</td>
              <td>${escaparHtml(formatarDataISOParaBR(c.data))}</td>
              <td>${escaparHtml(c.mes_ano_validade || "")}</td>
              <td>${escaparHtml(c.certificado || "")}</td>
              <td>${escaparHtml(String(p1.incerteza ?? "-"))}</td>
              <td>${escaparHtml(String(p1.erro ?? "-"))}</td>
              <td>${escaparHtml(String(p1.soma ?? "-"))}</td>
              <td>${escaparHtml(String(p2.erro ?? "-"))}</td>
              <td>${escaparHtml(String(p2.soma ?? "-"))}</td>
              <td>${escaparHtml(String(p3.erro ?? "-"))}</td>
              <td>${escaparHtml(String(p3.soma ?? "-"))}</td>
              <td>${escaparHtml(String(p4.erro ?? "-"))}</td>
              <td>${escaparHtml(String(p4.soma ?? "-"))}</td>
              <td class="${c.status === "APROVADO" ? "ok" : c.status === "REPROVADO" ? "bad" : "warn"}">
                ${escaparHtml(c.status)}
              </td>
            </tr>
          `;
        })
        .join("");

      return `
        <section class="bloco">
          <div class="subtitulo">DLT ${escaparHtml(dlt)} — ${itens.length} certificado(s)</div>
          <table>
            <thead>
              <tr>
                <th rowspan="2">Nº</th>
                <th rowspan="2">Nº Série</th>
                <th rowspan="2">TAG</th>
                <th rowspan="2">Calibrado em</th>
                <th rowspan="2">Validade</th>
                <th rowspan="2">Certificado</th>
                <th rowspan="2">Incerteza ± U</th>
                <th colspan="2">-20,0°C</th>
                <th colspan="2">0,0°C</th>
                <th colspan="2">15,0°C</th>
                <th colspan="2">60,0°C</th>
                <th rowspan="2">Resultado</th>
              </tr>
              <tr>
                <th>Erro</th>
                <th>Resultado</th>
                <th>Erro</th>
                <th>Resultado</th>
                <th>Erro</th>
                <th>Resultado</th>
                <th>Erro</th>
                <th>Resultado</th>
              </tr>
            </thead>
            <tbody>
              ${linhas}
            </tbody>
          </table>
        </section>
      `;
    })
    .join("");

  return `
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
      <meta charset="UTF-8" />
      <title>Relatório Diário 174T</title>
      <style>
        @page {
          size: A4 landscape;
          margin: 8mm;
        }

        body {
          font-family: Arial, sans-serif;
          font-size: 9px;
          color: #000;
          margin: 0;
        }

        .pagina {
          width: 100%;
        }

        .header-box {
          border: 1px solid #000;
          padding: 4px 6px;
          margin-bottom: 5px;
        }

        .header-top {
          display: grid;
          grid-template-columns: 180px 1fr 180px;
          align-items: center;
          gap: 8px;
          border-bottom: 1px solid #000;
          padding-bottom: 5px;
          margin-bottom: 5px;
        }

        .versao {
          font-size: 9px;
          line-height: 1.35;
        }

        .titulo {
          text-align: center;
          font-weight: 700;
          font-size: 12px;
        }

        .meta-grid {
          display: grid;
          grid-template-columns: 1fr 1fr 1fr 1fr;
          gap: 0;
          border: 1px solid #000;
          margin-top: 5px;
        }

        .meta-grid > div {
          border-right: 1px solid #000;
          padding: 3px 5px;
        }

        .meta-grid > div:last-child {
          border-right: 0;
        }

        .resumo {
          display: grid;
          grid-template-columns: repeat(4, 1fr);
          gap: 5px;
          margin: 6px 0;
        }

        .card {
          border: 1px solid #000;
          padding: 4px;
          text-align: center;
        }

        .card .n {
          font-size: 14px;
          font-weight: 700;
        }

        .subtitulo {
          font-size: 10px;
          font-weight: 700;
          margin: 8px 0 3px;
        }

        table {
          width: 100%;
          border-collapse: collapse;
          table-layout: fixed;
          margin-bottom: 8px;
        }

        th, td {
          border: 1px solid #000;
          padding: 2px 3px;
          text-align: center;
          vertical-align: middle;
          word-wrap: break-word;
        }

        th {
          background: #f0f0f0;
          font-size: 8px;
        }

        td {
          font-size: 8px;
        }

        .ok {
          color: #0a7a1f;
          font-weight: 700;
        }

        .bad {
          color: #b00020;
          font-weight: 700;
        }

        .warn {
          color: #9a6a00;
          font-weight: 700;
        }

        .rodape {
          margin-top: 6px;
          display: flex;
          justify-content: space-between;
          font-size: 8px;
        }

        .bloco {
          page-break-inside: avoid;
        }
      </style>
    </head>
    <body>
      <div class="pagina">
        <div class="header-box">
          <div class="header-top">
            <div class="versao">
              <div><strong>REL 06G009</strong></div>
              <div>Versão: 00</div>
              <div>Data: ${escaparHtml(formatarDataISOParaBR(dataRelatorio))}</div>
            </div>

            <div class="titulo">
              AVALIAÇÃO DOS CERTIFICADOS DE CALIBRAÇÃO - TESTE 174T
            </div>

            <div style="display:flex;justify-content:flex-end;">
              ${montarLogoHtml()}
            </div>
          </div>

          <div class="meta-grid">
            <div><strong>Instrumento:</strong> TESTO</div>
            <div><strong>Modelo:</strong> 174T</div>
            <div><strong>DMA:</strong> ${escaparHtml(String(dma).replace(".", ","))}</div>
            <div><strong>Unidade:</strong> °C</div>
          </div>
        </div>

        <div class="resumo">
          <div class="card">
            <div>Total processados</div>
            <div class="n">${total}</div>
          </div>
          <div class="card">
            <div>Aprovados</div>
            <div class="n">${aprovados}</div>
          </div>
          <div class="card">
            <div>Reprovados</div>
            <div class="n">${reprovados}</div>
          </div>
          <div class="card">
            <div>Erros</div>
            <div class="n">${erros}</div>
          </div>
        </div>

        ${secoes || `<div class="subtitulo">Nenhum processamento encontrado para ${escaparHtml(formatarDataISOParaBR(dataRelatorio))}.</div>`}

        <div class="rodape">
          <div>Sistema de Gestão da Qualidade ITA FRIA</div>
          <div>Emitido em ${escaparHtml(formatarDataHoraBR(new Date().toISOString()))}</div>
        </div>
      </div>
    </body>
    </html>
  `;
}

async function salvarRelatorioNoDrive(pdfPath, nomeArquivo) {
  if (!drive) {
    throw new Error("Credenciais Google Drive não configuradas");
  }

  if (!REPORTS_FOLDER_ID) {
    throw new Error("REPORTS_FOLDER_ID não configurado");
  }

  const response = await drive.files.create({
    requestBody: {
      name: nomeArquivo,
      parents: [REPORTS_FOLDER_ID],
      mimeType: "application/pdf"
    },
    media: {
      mimeType: "application/pdf",
      body: fs.createReadStream(pdfPath)
    },
    fields: "id, webViewLink, webContentLink, name"
  });

  return response.data;
}

// =========================
// PDF / TEXTO
// =========================
async function baixarArquivoDrive(fileId) {
  if (drive) {
    const response = await drive.files.get(
      { fileId, alt: "media", supportsAllDrives: true },
      { responseType: "arraybuffer" }
    );
    return Buffer.from(response.data);
  }

  const url =
    `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}` +
    `?alt=media&key=${encodeURIComponent(GOOGLE_API_KEY)}&supportsAllDrives=true`;
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
  let certificado = "";

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

  certificado = extrairNumeroCertificado(texto);

  const baseNome = extrairDados(nomeOriginal);
  if (!dlt && baseNome.dlt) dlt = soDigitos(baseNome.dlt).padStart(4, "0");
  if (!serie && baseNome.serie) serie = soDigitos(baseNome.serie);
  if (!data && baseNome.data) data = baseNome.data;

  return {
    dlt: dlt ? soDigitos(dlt).padStart(4, "0") : "",
    serie: serie ? soDigitos(serie) : "",
    data: data || "",
    certificado: certificado || ""
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
    const { texto } = await extrairTextoELinhasDoPDF(buffer);
    const meta = extrairMetadadosDoTexto(texto);
    const tabela = await extrairTabelaPorColunas(buffer);

    if (!tabela.ok) {
      return {
        status: "ERRO",
        pontos: [],
        certificado: meta.certificado || "",
        debug: tabela.debug || {}
      };
    }

    const criterios = await buscarCriteriosCalibracao();
    const aprovado = tabela.pontos.every(p => p.soma <= criterios.limite_dlt);

    return {
      status: aprovado ? "APROVADO" : "REPROVADO",
      pontos: tabela.pontos,
      certificado: meta.certificado || "",
      criterios_aceitacao: criterios
    };
  } catch (e) {
    return {
      status: "ERRO",
      pontos: [],
      certificado: "",
      debug: { erro: e.message }
    };
  }
}

// =========================
// CONTROLE / HISTÓRICO
// =========================
async function atualizarControleSync(payload) {
  const response = await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?on_conflict=id`, {
    method: "POST",
    headers: {
      ...supabaseHeaders(),
      Prefer: "resolution=merge-duplicates,return=minimal"
    },
    body: JSON.stringify({ id: 1, ...payload })
  });

  if (!response.ok) {
    const detalhe = await response.text();
    throw new Error(`Falha ao atualizar controle_sync: ${detalhe}`);
  }

  invalidarCaches();
}

async function buscarControleSync() {
  const r = await fetch(
    `${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1&select=*`,
    { headers: supabaseHeaders() }
  );

  const data = await r.json();
  validarListaSupabase(r, data, "Supabase controle_sync");
  return data && data.length ? data[0] : null;
}

async function buscarIdsBanco() {
  if (idsBancoCache.valor && idsBancoCache.expiraEm > Date.now()) {
    return idsBancoCache.valor;
  }

  const ids = new Set();
  const limit = 1000;
  let offset = 0;

  while (true) {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=id&limit=${limit}&offset=${offset}`,
      { headers: supabaseHeaders() }
    );

    const data = await r.json();
    validarListaSupabase(r, data, "Supabase certificados");

    if (!data || data.length === 0) break;

    for (const item of data) {
      ids.add(item.id);
    }

    if (data.length < limit) break;
    offset += limit;
  }

  idsBancoCache = { valor: ids, expiraEm: Date.now() + IDS_CACHE_MS };
  return ids;
}

async function buscarIdsExcluidos() {
  if (idsExcluidosCache.valor && idsExcluidosCache.expiraEm > Date.now()) {
    return idsExcluidosCache.valor;
  }

  const ids = new Set();
  const limit = 1000;
  let offset = 0;

  while (true) {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_excluidos?select=id&limit=${limit}&offset=${offset}`,
      { headers: supabaseHeaders() }
    );

    const data = await r.json();
    validarListaSupabase(r, data, "Supabase certificados_excluidos");

    if (!data || data.length === 0) break;

    for (const item of data) {
      ids.add(item.id);
    }

    if (data.length < limit) break;
    offset += limit;
  }

  idsExcluidosCache = { valor: ids, expiraEm: Date.now() + IDS_CACHE_MS };
  return ids;
}

async function contarCertificadosBanco() {
  return contarTabela("certificados");
}

async function buscarArquivosDrive() {
  if (drive) {
    const arquivos = [];
    let pageToken = null;

    do {
      const response = await executarGoogleComRetry(() =>
        drive.files.list({
          q: `'${FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false`,
          fields: "nextPageToken, files(id, name, mimeType)",
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
    const url = `https://www.googleapis.com/drive/v3/files?q='${FOLDER_ID}'+in+parents+and+mimeType='application/pdf'&key=${GOOGLE_API_KEY}&fields=nextPageToken,files(id,name,mimeType)&pageSize=1000${pageToken ? `&pageToken=${pageToken}` : ""}`;

    const res = await fetch(url);
    const data = await res.json();
    if (!res.ok || data.error) {
      throw new Error(data?.error?.message || `Erro ao listar Google Drive (HTTP ${res.status})`);
    }

    arquivos.push(...(data.files || []));
    pageToken = data.nextPageToken || null;
  } while (pageToken);

  return arquivos;
}

// =========================
// EXECUÇÃO DE SYNC EM BACKGROUND
// =========================
async function executarSyncEmBackground() {
  if (syncLocalEmExecucao) return;
  syncLocalEmExecucao = true;
  let deveContinuar = false;

  try {
    const idsBanco = await buscarIdsBanco();
    const idsExcluidos = await buscarIdsExcluidos();
    const arquivosDrive = await buscarArquivosDrive();
    const criterios = await buscarCriteriosCalibracao();

    await atualizarControleSync({
      em_execucao: true,
      ultima_execucao: new Date().toISOString(),
      total_processados: idsBanco.size
    });

    let processados = 0;

    for (const f of arquivosDrive) {
      if (idsBanco.has(f.id)) continue;
      if (idsExcluidos.has(f.id)) continue;
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

        const status = tabela.pontos.every(p => p.soma <= criterios.limite_dlt)
          ? "APROVADO"
          : "REPROVADO";
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
            certificado: meta.certificado || "",
            status,
            validade: val.valido,
            vencimento: val.vencimento,
            mes_ano_validade: val.mes_ano,
            pontos: tabela.pontos,
            divergente: divergencia.divergente,
            serie_esperada: divergencia.serie_esperada,
            motivo_divergencia: divergencia.motivo_divergencia,
            criado_em: new Date().toISOString()
          })
        });

        if (!respInsert.ok) {
          const erroInsert = await respInsert.text();
          console.log("Erro ao salvar no banco:", f.name, erroInsert);
          continue;
        }

        idsBanco.add(f.id);
        processados++;
        invalidarCaches();

        await atualizarControleSync({
          em_execucao: true,
          ultima_execucao: new Date().toISOString(),
          total_processados: idsBanco.size
        });
      } catch (e) {
        console.log("Erro ao processar arquivo:", f.name, e.message);
      }
    }

    const idsBancoAtualizado = await buscarIdsBanco();
    const idsExcluidosAtualizado = await buscarIdsExcluidos();
    const arquivosDriveAtualizados = await buscarArquivosDrive();
    const faltantesRestantes = arquivosDriveAtualizados.filter(
      f => !idsBancoAtualizado.has(f.id) && !idsExcluidosAtualizado.has(f.id)
    ).length;

    await atualizarControleSync({
      em_execucao: false,
      ultima_execucao: new Date().toISOString(),
      total_processados: idsBancoAtualizado.size
    });

    deveContinuar = faltantesRestantes > 0 && processados > 0;
  } catch (e) {
    console.log("Erro geral executarSyncEmBackground:", e.message);

    try {
      await atualizarControleSync({
        em_execucao: false,
        ultima_execucao: new Date().toISOString()
      });
    } catch (controleErro) {
      console.log("Erro ao finalizar controle_sync:", controleErro.message);
    }
  } finally {
    syncLocalEmExecucao = false;

    if (deveContinuar) {
      setTimeout(() => executarSyncEmBackground(), 3000);
    }
  }
}

// =========================
// ROTAS
// =========================
app.get("/", (req, res) => {
  res.send("API OK 🚀");
});

app.get("/criterios", async (req, res) => {
  try {
    const criterios = await buscarCriteriosCalibracao();
    res.setHeader("Cache-Control", "private, max-age=300");
    res.json(criterios);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.patch("/criterios", async (req, res) => {
  try {
    const limiteDlt = Number(req.body?.limite_dlt);
    const alteradoPor = String(req.body?.alterado_por || "").trim() || null;

    if (!Number.isFinite(limiteDlt) || limiteDlt <= 0 || limiteDlt > 100) {
      return res.status(400).json({ erro: "limite_dlt deve ser maior que zero e menor ou igual a 100." });
    }

    const anterior = await buscarCriteriosCalibracao();
    const atualizadoEm = new Date().toISOString();
    const response = await fetch(
      `${SUPABASE_URL}/rest/v1/criterios_calibracao?on_conflict=id`,
      {
        method: "POST",
        headers: {
          ...supabaseHeaders(),
          Prefer: "resolution=merge-duplicates,return=representation"
        },
        body: JSON.stringify({
          id: 1,
          limite_dlt: limiteDlt,
          atualizado_em: atualizadoEm
        })
      }
    );
    const data = await response.json();
    const registros = validarListaSupabase(response, data, "Supabase atualização do DMA DLT");

    const historico = await fetch(`${SUPABASE_URL}/rest/v1/criterios_calibracao_historico`, {
      method: "POST",
      headers: supabaseHeaders(),
      body: JSON.stringify({
        modulo: "DLT",
        limite_dlt_anterior: anterior.limite_dlt,
        limite_dlt_novo: limiteDlt,
        alterado_por: alteradoPor,
        alterado_em: atualizadoEm
      })
    });
    if (!historico.ok) {
      throw new Error(`DMA atualizado, mas houve falha ao gravar histórico: ${await historico.text()}`);
    }

    criteriosCache = {
      valor: { limite_dlt: limiteDlt, atualizado_em: atualizadoEm },
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

app.get("/criterios/historico", async (req, res) => {
  try {
    const limit = Math.min(100, Math.max(1, Number(req.query.limit || 20)));
    const response = await fetch(
      `${SUPABASE_URL}/rest/v1/criterios_calibracao_historico?modulo=eq.DLT&select=id,limite_dlt_anterior,limite_dlt_novo,alterado_por,alterado_em&order=alterado_em.desc&limit=${limit}`,
      { headers: supabaseHeaders() }
    );
    const data = await response.json();
    const registros = validarListaSupabase(response, data, "Supabase histórico DMA DLT");
    res.setHeader("Cache-Control", "private, max-age=300");
    res.json({ total: registros.length, registros });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/status", async (req, res) => {
  try {
    if (statusCache.valor && statusCache.expiraEm > Date.now()) {
      res.setHeader("Cache-Control", "public, max-age=15, s-maxage=30, stale-while-revalidate=60");
      return res.json(statusCache.valor);
    }

    const controle = await buscarControleSync();
    const totalBanco = await contarCertificadosBanco();
    const totalExcluidos = await contarTabela("certificados_excluidos");
    const arquivosDrive = await buscarArquivosDrive();

    const totalDrive = arquivosDrive.length;
    const faltantes = Math.max(0, totalDrive - totalBanco - totalExcluidos);

    const payload = {
      id: controle?.id || 1,
      total_processados: controle?.total_processados || 0,
      em_execucao: controle?.em_execucao || false,
      ultima_execucao: controle?.ultima_execucao || null,
      total_drive: totalDrive,
      total_banco: totalBanco,
      faltantes
    };

    statusCache = { valor: payload, expiraEm: Date.now() + STATUS_CACHE_MS };
    res.setHeader("Cache-Control", "public, max-age=15, s-maxage=30, stale-while-revalidate=60");
    res.json(payload);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/status/supabase", async (req, res) => {
  try {
    const controle = await buscarControleSync();
    const totalBanco = await contarCertificadosBanco();
    res.json({ ok: true, servico: "supabase", controle, total_banco: totalBanco });
  } catch (e) {
    res.status(500).json({ ok: false, servico: "supabase", erro: e.message });
  }
});

app.get("/status/google", async (req, res) => {
  try {
    if (!googleAuth || !drive) {
      throw new Error("GOOGLE_CLIENT_EMAIL ou GOOGLE_PRIVATE_KEY não configurado");
    }

    await executarGoogleComRetry(() => googleAuth.getAccessToken());
    const response = await executarGoogleComRetry(() =>
      drive.files.list({
        q: `'${FOLDER_ID}' in parents and trashed=false`,
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

app.get("/certificados", async (req, res) => {
  try {
    const listaEquipamentos = normalizarListaQuery(req.query.equipamentos || req.query.dlt || req.query.lista);
    const testeInicio = normalizarDataQuery(req.query.teste_inicio || req.query.data_inicio || req.query.inicio);
    const testeFim = normalizarDataQuery(req.query.teste_fim || req.query.data_fim || req.query.fim);

    if (listaEquipamentos.length && testeInicio && testeFim) {
      const data = await buscarCertificadosPorPeriodoEmLotes({
        tabela: "certificados",
        campoEquipamento: "dlt",
        equipamentos: listaEquipamentos,
        testeInicio,
        testeFim
      });
      return res.json({ total: data.length, registros: data });
    }

    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=${CERTIFICADOS_LISTA_SELECT}&order=data.desc&limit=${limit}&offset=${offset}`,
      { headers: { ...supabaseHeaders(), Prefer: "count=exact" } }
    );
    const data = await r.json();
    res.setHeader("Cache-Control", "private, max-age=30");
    res.json({ total: Number(r.headers.get("content-range")?.split("/")[1] || 0), registros: data });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/certificados/:id/detalhes", async (req, res) => {
  try {
    const id = encodeURIComponent(req.params.id);
    const response = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?id=eq.${id}&select=id,pontos,status,certificado,data,vencimento`,
      { headers: supabaseHeaders() }
    );
    const data = await response.json();
    const registros = validarListaSupabase(response, data, "Supabase detalhes do certificado");

    if (!registros.length) {
      return res.status(404).json({ erro: "Certificado não encontrado" });
    }

    res.setHeader("Cache-Control", "private, max-age=3600");
    res.json(registros[0]);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/divergentes", async (req, res) => {
  try {
    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=${CERTIFICADOS_LISTA_SELECT}&divergente=eq.true&order=data.desc&limit=${limit}&offset=${offset}`,
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

app.get("/historico-exclusoes", async (req, res) => {
  try {
    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_excluidos?select=*&order=excluido_em.desc&limit=${limit}&offset=${offset}`,
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

app.get("/relatorios-diarios", async (req, res) => {
  try {
    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/relatorios_diarios?select=*&order=criado_em.desc&limit=${limit}&offset=${offset}`,
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
    validarListaSupabase(r, lista, "Supabase certificados para reprocessamento");
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
          certificado: proc.certificado || "",
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
    const idsExcluidos = await buscarIdsExcluidos();
    const arquivosDrive = await buscarArquivosDrive();

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


app.post("/downloads/massa", async (req, res) => {
  try {
    const ids = Array.isArray(req.body?.ids) ? req.body.ids.map(String).filter(Boolean) : [];
    const listaEquipamentos = Array.isArray(req.body?.equipamentos)
      ? req.body.equipamentos.map(String).filter(Boolean)
      : normalizarListaQuery(req.body?.equipamentos || req.body?.dlt || req.body?.lista);
    const testeInicio = normalizarDataQuery(req.body?.teste_inicio || req.body?.data_inicio || req.body?.inicio);
    const testeFim = normalizarDataQuery(req.body?.teste_fim || req.body?.data_fim || req.body?.fim);

    let registros = [];

    if (ids.length) {
      registros = await buscarCertificadosPorIdsEmLotes(
        "certificados",
        "id,nome_original,nome_download,dlt,serie,data,vencimento",
        ids
      );
    } else if (listaEquipamentos.length && testeInicio && testeFim) {
      registros = await buscarCertificadosPorPeriodoEmLotes({
        tabela: "certificados",
        campoEquipamento: "dlt",
        equipamentos: listaEquipamentos,
        testeInicio,
        testeFim
      });
    } else {
      return res.status(400).json({ erro: "Informe ids ou equipamentos + teste_inicio + teste_fim" });
    }

    const jobId = crypto.randomUUID();
    downloadJobs.set(jobId, {
      id: jobId,
      tipo: "DLT",
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
      processarDownloadMassa(jobId, Array.isArray(registros) ? registros : []).catch(e => {
        const job = downloadJobs.get(jobId);
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

app.get("/downloads/massa/:jobId", (req, res) => {
  const job = downloadJobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ erro: "Tarefa não encontrada" });
  res.json(job);
});

app.get("/downloads/massa/:jobId/arquivo", async (req, res) => {
  try {
    const job = downloadJobs.get(req.params.jobId);
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
      `attachment; filename="${job.arquivo_zip_nome || "certificados-dlt.zip"}"`
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

app.delete("/certificados/:id", async (req, res) => {
  try {
    const id = req.params.id;

    const busca = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?id=eq.${id}&select=*`,
      { headers: supabaseHeaders() }
    );

    const registros = await busca.json();

    if (!registros || registros.length === 0) {
      return res.status(404).json({ erro: "Certificado não encontrado" });
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
      `${SUPABASE_URL}/rest/v1/certificados_excluidos`,
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

    const del = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?id=eq.${id}`,
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

    idsBancoCache.valor?.delete(id);
    idsExcluidosCache.valor?.add(id);
    invalidarCaches();

    return res.json({
      sucesso: true,
      mensagem: "Certificado removido da base e registrado no histórico",
      id,
      nome_original: certificado.nome_original
    });
  } catch (e) {
    return res.status(500).json({ erro: e.message });
  }
});

app.get("/relatorio-dia/dados", async (req, res) => {
  try {
    const dataRelatorio = req.query.data || obterHojeISO();

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=*&order=dlt.asc,data.asc,serie.asc`,
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

app.get("/relatorio-dia/html", async (req, res) => {
  try {
    const dataRelatorio = req.query.data || obterHojeISO();

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=*&order=dlt.asc,data.asc,serie.asc`,
      { headers: supabaseHeaders() }
    );

    const todos = await r.json();
    const dados = (Array.isArray(todos) ? todos : []).filter(item =>
      mesmaData(item.criado_em, dataRelatorio)
    );

    const criterios = await buscarCriteriosCalibracao();
    const html = montarHtmlRelatorioDia(dados, dataRelatorio, criterios.limite_dlt);
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.send(html);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/relatorio-dia/pdf", async (req, res) => {
  let browser;

  try {
    const dataRelatorio = req.query.data || obterHojeISO();
    const salvar = String(req.query.salvar || "0") === "1";

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=*&order=dlt.asc,data.asc,serie.asc`,
      { headers: supabaseHeaders() }
    );

    const todos = await r.json();
    const dados = (Array.isArray(todos) ? todos : []).filter(item =>
      mesmaData(item.criado_em, dataRelatorio)
    );

    const criterios = await buscarCriteriosCalibracao();
    const html = montarHtmlRelatorioDia(dados, dataRelatorio, criterios.limite_dlt);
    const nomeArquivo = `RELATORIO_DIARIO_174T_${dataRelatorio}.pdf`;

    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    await page.setContent(html, { waitUntil: "networkidle" });

    const pdfPath = path.join(os.tmpdir(), nomeArquivo);

    await page.pdf({
      path: pdfPath,
      format: "A4",
      landscape: true,
      printBackground: true,
      margin: {
        top: "8mm",
        right: "8mm",
        bottom: "8mm",
        left: "8mm"
      }
    });

    await browser.close();
    browser = null;

    if (salvar) {
      const arquivoDrive = await salvarRelatorioNoDrive(pdfPath, nomeArquivo);

      await fetch(`${SUPABASE_URL}/rest/v1/relatorios_diarios`, {
        method: "POST",
        headers: supabaseHeaders(),
        body: JSON.stringify({
          data_relatorio: dataRelatorio,
          nome_arquivo: nomeArquivo,
          drive_file_id: arquivoDrive.id || null,
          drive_link: arquivoDrive.webViewLink || arquivoDrive.webContentLink || null,
          total_registros: dados.length
        })
      });
    }

    const buffer = fs.readFileSync(pdfPath);
    fs.unlinkSync(pdfPath);

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${nomeArquivo}"`);
    return res.send(buffer);
  } catch (e) {
    if (browser) {
      await browser.close();
    }
    res.status(500).json({ erro: e.message });
  }
});

app.get("/relatorio-dia/excel", async (req, res) => {
  try {
    const dataRelatorio = req.query.data || obterHojeISO();

    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=*&order=dlt.asc,data.asc,serie.asc`,
      {
        headers: supabaseHeaders()
      }
    );

    const todos = await r.json();

    const dados = (Array.isArray(todos) ? todos : []).filter(item =>
      mesmaData(item.criado_em, dataRelatorio)
    );

    const workbook = new ExcelJS.Workbook();

    workbook.creator = "ITA FRIA";
    workbook.lastModifiedBy = "Sistema";
    workbook.created = new Date();
    workbook.modified = new Date();

    const sheet = workbook.addWorksheet("Relatório DLT");

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
        header: 0.1,
        footer: 0.1
      }
    };

    sheet.properties.defaultRowHeight = 20;

    // =========================
    // CABEÇALHO
    // =========================

    sheet.mergeCells("A1:C3");

    sheet.getCell("A1").value =
`REL 06GQ09
Versão: 00
Data: ${formatarDataISOParaBR(dataRelatorio)}`;

    sheet.getCell("A1").alignment = {
      vertical: "middle",
      horizontal: "left",
      wrapText: true
    };

    sheet.getCell("A1").font = {
      bold: true,
      size: 9,
      name: "Arial"
    };

    sheet.mergeCells("D1:O3");

    sheet.getCell("D1").value =
"AVALIAÇÃO DOS CERTIFICADOS DE CALIBRAÇÃO - TESTO 174T";

    sheet.getCell("D1").alignment = {
      vertical: "middle",
      horizontal: "center"
    };

    sheet.getCell("D1").font = {
      bold: true,
      size: 14,
      name: "Arial"
    };

    // =========================
    // BLOCO INSTRUMENTO
    // =========================

    sheet.mergeCells("A4:C4");
    sheet.getCell("A4").value = "INSTRUMENTO";

    sheet.mergeCells("D4:O4");
    sheet.getCell("D4").value =
"ESPECIFICAÇÕES TEMPERATURA / CRITÉRIOS DE ACEITAÇÃO";

    ["A4", "D4"].forEach(c => {
      sheet.getCell(c).font = {
        bold: true,
        size: 10,
        name: "Arial"
      };

      sheet.getCell(c).alignment = {
        horizontal: "center",
        vertical: "middle"
      };

      sheet.getCell(c).fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "D9D9D9" }
      };
    });

    sheet.getCell("A5").value = "Marca: TESTO";
    sheet.getCell("B5").value = "Modelo: 174T";
    sheet.getCell("C5").value = "Resolução: 0,1";
    const criterios = await buscarCriteriosCalibracao();
    sheet.getCell("D5").value = `DMA: ${String(criterios.limite_dlt).replace(".", ",")}`;
    sheet.getCell("E5").value = "Unidade: °C";

    // =========================
    // CABEÇALHO TABELA
    // =========================

    const headerRow = 7;

    sheet.getRow(headerRow).values = [
      "N° Série",
      "TAG",
      "Calibrado em",
      "Validade",
      "Certificado",
      "Incerteza",
      "-20°C Erro",
      "-20°C Resultado",
      "0°C Erro",
      "0°C Resultado",
      "15°C Erro",
      "15°C Resultado",
      "60°C Erro",
      "60°C Resultado",
      "RESULTADO"
    ];

    sheet.getRow(headerRow).height = 35;

    sheet.getRow(headerRow).font = {
      bold: true,
      size: 9,
      name: "Arial"
    };

    sheet.getRow(headerRow).alignment = {
      horizontal: "center",
      vertical: "middle",
      wrapText: true
    };

    sheet.getRow(headerRow).fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "BFBFBF" }
    };

    // =========================
    // DADOS
    // =========================

    dados.forEach(c => {
      const pontos = Array.isArray(c.pontos) ? c.pontos : [];

      const p1 = pontos[0] || {};
      const p2 = pontos[1] || {};
      const p3 = pontos[2] || {};
      const p4 = pontos[3] || {};

      const row = sheet.addRow([
        c.serie || "",
        normalizarDLT(c.dlt) || "",
        formatarDataISOParaBR(c.data),
        c.mes_ano_validade || "",
        c.certificado || "",
        p1.incerteza ?? "",
        p1.erro ?? "",
        p1.soma ?? "",
        p2.erro ?? "",
        p2.soma ?? "",
        p3.erro ?? "",
        p3.soma ?? "",
        p4.erro ?? "",
        p4.soma ?? "",
        c.status || ""
      ]);

      row.height = 22;

      if (String(c.status || "").toUpperCase() === "APROVADO") {
        row.getCell(15).fill = {
          type: "pattern",
          pattern: "solid",
          fgColor: { argb: "C6EFCE" }
        };
      }

      if (String(c.status || "").toUpperCase() === "REPROVADO") {
        row.getCell(15).fill = {
          type: "pattern",
          pattern: "solid",
          fgColor: { argb: "FFC7CE" }
        };
      }
    });

    // =========================
    // LARGURA COLUNAS
    // =========================

    const larguras = [
      16,
      14,
      16,
      16,
      18,
      12,
      12,
      12,
      12,
      12,
      12,
      12,
      12,
      12,
      14
    ];

    larguras.forEach((w, i) => {
      sheet.getColumn(i + 1).width = w;
    });

    // =========================
    // ESTILO GERAL
    // =========================

    sheet.eachRow((row, rowNumber) => {
      row.eachCell(cell => {
        cell.border = {
          top: { style: "thin" },
          left: { style: "thin" },
          bottom: { style: "thin" },
          right: { style: "thin" }
        };

        cell.alignment = {
          horizontal: "center",
          vertical: "middle",
          wrapText: true
        };

        if (rowNumber > headerRow) {
          cell.font = {
            name: "Arial",
            size: 8
          };
        }
      });
    });

    // =========================
    // CONGELAR CABEÇALHO
    // =========================

    sheet.views = [
      {
        state: "frozen",
        ySplit: 7
      }
    ];

    // =========================
    // RODAPÉ
    // =========================

    sheet.headerFooter.oddFooter =
"&LResp:&C Sistema de Gestão da Qualidade ITA FRIA&R Página &P de &N";

    // =========================
    // NOME ARQUIVO
    // =========================

    const nomeArquivo =
`RELATORIO_DIARIO_174T_${dataRelatorio}.xlsx`;

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

app.get("/relatorio-dia", async (req, res) => {
  res.redirect("/relatorio-dia/html");
});

app.listen(PORT, () => {
  console.log(`Servidor DLT rodando na porta ${PORT} 🚀`);

  if (AUTO_SYNC_ENABLED) {
    setTimeout(() => {
      executarSyncEmBackground().catch(e => {
        console.log("Erro ao iniciar sincronização automática:", e.message);
      });
    }, AUTO_SYNC_START_DELAY_MS);
  }
});
