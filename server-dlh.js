import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import fetchNative from "node-fetch";
import { google } from "googleapis";
import { getDocument } from "pdfjs-dist/legacy/build/pdf.mjs";
import ExcelJS from "exceljs";
import os from "os";
import crypto from "crypto";
import zlib from "zlib";
import archiver from "archiver";
import dns from "dns";
import { createClient } from "@supabase/supabase-js";
import { MAPA_LOGGERS_DLH, normalizarDLH } from "./mapa-loggers-dlh.js";
import { gerarPdfDLH } from "./relatorios-pdf.js";

dns.setDefaultResultOrder("ipv4first");

const app = express();
app.use(express.json({ limit: "2mb" }));

function origemCorsPermitida(origem, regras) {
  return regras.some(regra => {
    if (regra === "*") return true;
    if (!regra.includes("*")) return regra === origem;
    const expressao = regra
      .replace(/[.+?^${}()|[\]\\]/g, "\\$&")
      .replace(/\*/g, "[^/]+");
    return new RegExp(`^${expressao}$`, "i").test(origem);
  });
}

app.use((req, res, next) => {
  const origem = String(req.headers.origin || "");
  const origensPermitidas = String(process.env.CORS_ORIGIN || "*")
    .split(",")
    .map(item => item.trim().replace(/\/+$/, ""))
    .filter(Boolean);
  const origemNormalizada = origem.replace(/\/+$/, "");
  if (origensPermitidas.includes("*")) {
    res.setHeader("Access-Control-Allow-Origin", "*");
  } else if (origemCorsPermitida(origemNormalizada, origensPermitidas)) {
    res.setHeader("Access-Control-Allow-Origin", origem);
    res.setHeader("Vary", "Origin");
  }
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});
app.use((req, _res, next) => {
  if (req.url.startsWith("/dlh/certificados/downloads/massa")) {
    req.url = req.url.replace(
      "/dlh/certificados/downloads/massa",
      "/dlh/downloads/massa"
    );
  }
  next();
});

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const MODELO_RELATORIO_PATH = path.join(__dirname, "modelo-relatorio-dlh.xlsx");


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
const METRICS_ENABLED = String(process.env.METRICS_ENABLED || "true") === "true";
const METRICS_TIMEZONE = process.env.METRICS_TIMEZONE || "America/Sao_Paulo";
const METRICS_START_TIME = process.env.METRICS_START_TIME || "07:30";
const METRICS_END_TIME = process.env.METRICS_END_TIME || "20:00";
const METRICS_INTERVAL_MINUTES = Number(process.env.METRICS_INTERVAL_MINUTES || 120);
const METRICS_RETENTION_DAYS = Number(process.env.METRICS_RETENTION_DAYS || 90);
const AUTH_ENABLED = String(process.env.AUTH_ENABLED || "false") === "true";
const AUTH_CACHE_MS = Number(process.env.AUTH_CACHE_MS || 300000);
const PROFILE_CACHE_MS = Number(process.env.PROFILE_CACHE_MS || 300000);
const AUTOMATION_SECRET = process.env.AUTOMATION_SECRET || "";
const BACKEND_VERSION = "assistente-backend-2026-07-23";
const SUPPORT_TO_EMAIL = process.env.SUPPORT_TO_EMAIL || "contato@calibraflow.com";
const SUPPORT_FROM_EMAIL =
  process.env.SUPPORT_FROM_EMAIL || "CalibraFlow <contato@calibraflow.com>";
const SUPPORT_RESEND_API_KEY = process.env.SUPPORT_RESEND_API_KEY || process.env.RESEND_API_KEY || "";
const limparConfiguracao = (valor) => String(valor || "")
  .trim()
  .replace(/^['"]|['"]$/g, "");
const TELEGRAM_BOT_TOKEN = limparConfiguracao(process.env.TELEGRAM_BOT_TOKEN);
const TELEGRAM_CHAT_ID = limparConfiguracao(process.env.TELEGRAM_CHAT_ID);
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
let metricasFlushEmExecucao = false;
let ultimoSlotMetricas = "";
let ultimaLimpezaMetricas = "";
const authCache = new Map();
const profileCache = new Map();

const supabaseAdmin =
  SUPABASE_URL && SUPABASE_KEY
    ? createClient(SUPABASE_URL, SUPABASE_KEY, {
        auth: { persistSession: false, autoRefreshToken: false }
      })
    : null;

function novasMetricas() {
  return {
    periodo_inicio: new Date().toISOString(),
    requisicoes: 0,
    respostas_bytes: 0,
    requisicoes_externas: 0,
    supabase_requisicoes: 0,
    supabase_bytes: 0,
    google_requisicoes: 0,
    google_bytes: 0,
    erros: 0,
    tempo_total_ms: 0,
    rotas: {}
  };
}

let metricas = novasMetricas();

function classificarDestino(url) {
  const valor = String(url || "");
  if (SUPABASE_URL && valor.startsWith(SUPABASE_URL)) return "supabase";
  if (valor.includes("googleapis.com") || valor.includes("google.com")) return "google";
  return "outro";
}

async function fetch(url, options) {
  const response = await fetchNative(url, options);
  if (!METRICS_ENABLED) return response;

  const destino = classificarDestino(typeof url === "string" ? url : url?.url);
  const bytes = Number(response.headers.get("content-length") || 0);
  metricas.requisicoes_externas++;

  if (destino === "supabase") {
    metricas.supabase_requisicoes++;
    metricas.supabase_bytes += bytes;
  } else if (destino === "google") {
    metricas.google_requisicoes++;
    metricas.google_bytes += bytes;
  }

  return response;
}

function normalizarRotaMetrica(req) {
  return String(req.route?.path || req.path || "/")
    .replace(/[0-9a-f]{8}-[0-9a-f-]{27,}/gi, ":id")
    .replace(/\/[A-Za-z0-9_-]{20,}(?=\/|$)/g, "/:id");
}

app.use((req, res, next) => {
  if (!METRICS_ENABLED) return next();

  const inicio = Date.now();
  let bytes = 0;
  const writeOriginal = res.write.bind(res);
  const endOriginal = res.end.bind(res);
  const tamanhoChunk = chunk => {
    if (typeof chunk === "string" || Buffer.isBuffer(chunk) || ArrayBuffer.isView(chunk)) {
      return Buffer.byteLength(chunk);
    }
    return 0;
  };

  res.write = (chunk, ...args) => {
    bytes += tamanhoChunk(chunk);
    return writeOriginal(chunk, ...args);
  };

  res.end = (chunk, ...args) => {
    bytes += tamanhoChunk(chunk);
    return endOriginal(chunk, ...args);
  };

  res.on("finish", () => {
    const rota = `${req.method} ${normalizarRotaMetrica(req)}`;
    const duracao = Date.now() - inicio;
    const erro = res.statusCode >= 400 ? 1 : 0;
    const atual = metricas.rotas[rota] || {
      requisicoes: 0,
      respostas_bytes: 0,
      erros: 0,
      tempo_total_ms: 0
    };

    atual.requisicoes++;
    atual.respostas_bytes += bytes;
    atual.erros += erro;
    atual.tempo_total_ms += duracao;
    metricas.rotas[rota] = atual;
    metricas.requisicoes++;
    metricas.respostas_bytes += bytes;
    metricas.erros += erro;
    metricas.tempo_total_ms += duracao;
  });

  next();
});


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

function escaparHtmlSuporte(valor) {
  return String(valor ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function limitarTextoSuporte(valor, max = 2000) {
  return String(valor ?? "")
    .replace(/\r\n/g, "\n")
    .replace(/\n{4,}/g, "\n\n\n")
    .trim()
    .slice(0, max);
}

function gerarTicketIdSuporte(modulo) {
  const agora = new Date().toISOString().replace(/\D/g, "").slice(0, 14);
  return `TKT-${modulo}-${agora}-${crypto.randomBytes(3).toString("hex").toUpperCase()}`;
}

function montarTicketSuporte(req, modulo) {
  const mensagem = limitarTextoSuporte(req.body?.mensagem, 5000);
  if (!mensagem) {
    const erro = new Error("Informe a mensagem do chamado");
    erro.statusCode = 400;
    throw erro;
  }

  const assunto = limitarTextoSuporte(req.body?.assunto, 160) || "Solicitacao de suporte";
  const categoria = limitarTextoSuporte(req.body?.categoria, 80) || "Suporte";
  const prioridade = limitarTextoSuporte(req.body?.prioridade, 40) || "Normal";
  const pagina = limitarTextoSuporte(req.body?.pagina, 300);
  const navegador = limitarTextoSuporte(req.headers["user-agent"], 500);
  const ip = limitarTextoSuporte(String(req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "").split(",")[0], 80);

  return {
    id: gerarTicketIdSuporte(modulo),
    modulo,
    assunto,
    categoria,
    prioridade,
    mensagem,
    pagina,
    navegador,
    ip,
    criado_em: new Date().toISOString(),
    usuario: {
      id: req.auth?.user?.id || null,
      email: req.auth?.user?.email || req.auth?.perfil?.email || "",
      nome: req.auth?.perfil?.nome || req.auth?.user?.email || "",
      role: req.auth?.perfil?.role || ""
    }
  };
}

function montarHtmlTicketSuporte(ticket) {
  const campo = (label, valor) => `
    <tr>
      <td style="padding:8px 12px;border-bottom:1px solid #e2e8f0;color:#64748b;width:180px">${escaparHtmlSuporte(label)}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #e2e8f0">${escaparHtmlSuporte(valor || "-")}</td>
    </tr>`;

  return `
<!doctype html>
<html lang="pt-BR">
<body style="margin:0;background:#f4f7fb;font-family:Arial,sans-serif;color:#102a43">
  <div style="max-width:760px;margin:24px auto;background:#ffffff;border:1px solid #dbe3ec">
    <div style="background:#12355b;padding:22px">
      <h1 style="margin:0;color:#ffffff;font-size:22px">Calibra<span style="color:#2dd4bf">Flow</span></h1>
      <p style="margin:8px 0 0;color:#dbeafe">Novo chamado aberto pelo sistema</p>
    </div>
    <div style="padding:22px">
      <h2 style="margin:0 0 12px;font-size:18px">${escaparHtmlSuporte(ticket.assunto)}</h2>
      <table style="width:100%;border-collapse:collapse;font-size:14px;margin-bottom:18px">
        ${campo("Ticket", ticket.id)}
        ${campo("Modulo", ticket.modulo)}
        ${campo("Categoria", ticket.categoria)}
        ${campo("Prioridade", ticket.prioridade)}
        ${campo("Usuario", `${ticket.usuario.nome || "-"} <${ticket.usuario.email || "-"}>`)}
        ${campo("Pagina", ticket.pagina)}
        ${campo("Criado em", ticket.criado_em)}
      </table>
      <div style="background:#f8fafc;border-left:4px solid #2dd4bf;padding:16px;white-space:pre-wrap">${escaparHtmlSuporte(ticket.mensagem)}</div>
    </div>
    <div style="padding:14px;text-align:center;background:#f1f5f9;color:#64748b;font-size:12px">
      CalibraFlow - Gestao de Certificados
    </div>
  </div>
</body>
</html>`;
}

function montarTextoTelegramSuporte(ticket) {
  return [
    "<b>Novo chamado CalibraFlow</b>",
    `<b>Ticket:</b> ${escaparHtmlSuporte(ticket.id)}`,
    `<b>Modulo:</b> ${escaparHtmlSuporte(ticket.modulo)}`,
    `<b>Prioridade:</b> ${escaparHtmlSuporte(ticket.prioridade)}`,
    `<b>Categoria:</b> ${escaparHtmlSuporte(ticket.categoria)}`,
    `<b>Usuario:</b> ${escaparHtmlSuporte(ticket.usuario.nome || "-")} (${escaparHtmlSuporte(ticket.usuario.email || "-")})`,
    `<b>Assunto:</b> ${escaparHtmlSuporte(ticket.assunto)}`,
    `<b>Pagina:</b> ${escaparHtmlSuporte(ticket.pagina || "-")}`,
    "",
    escaparHtmlSuporte(ticket.mensagem).slice(0, 2500),
    "",
    "<i>Para responder ao usuario, use a funcao Responder nesta mensagem.</i>"
  ].join("\n");
}

async function gravarTicketSuporte(ticket) {
  if (!SUPABASE_URL || !SUPABASE_KEY) return { ok: false, aviso: "Supabase nao configurado" };
  try {
    const response = await fetch(`${SUPABASE_URL}/rest/v1/suporte_tickets`, {
      method: "POST",
      headers: { ...supabaseHeaders(), Prefer: "return=minimal" },
      body: JSON.stringify({
        id: ticket.id,
        modulo: ticket.modulo,
        assunto: ticket.assunto,
        categoria: ticket.categoria,
        prioridade: ticket.prioridade,
        mensagem: ticket.mensagem,
        pagina: ticket.pagina || null,
        status: "aberto",
        usuario_id: ticket.usuario.id,
        usuario_email: ticket.usuario.email || null,
        usuario_nome: ticket.usuario.nome || null,
        criado_em: ticket.criado_em
      })
    });
    if (!response.ok) return { ok: false, aviso: await response.text() };
    return { ok: true };
  } catch (e) {
    return { ok: false, aviso: e.message };
  }
}

async function buscarTicketSuporteDLH(ticketId) {
  const params = new URLSearchParams({
    select: "id,modulo,assunto,categoria,prioridade,mensagem,pagina,status,usuario_id,usuario_email,usuario_nome,criado_em",
    id: `eq.${ticketId}`,
    limit: "1"
  });
  const response = await fetch(`${SUPABASE_URL}/rest/v1/suporte_tickets?${params}`, {
    headers: supabaseHeaders()
  });
  const data = await response.json();
  const tickets = validarListaSupabase(response, data, "Supabase ticket de suporte");
  return tickets[0] || null;
}

async function listarTicketsSuporteDoUsuarioDLH(usuarioId, modulo) {
  const params = new URLSearchParams({
    select: "id,modulo,assunto,categoria,prioridade,mensagem,pagina,status,usuario_id,usuario_email,usuario_nome,criado_em",
    usuario_id: `eq.${usuarioId}`,
    order: "criado_em.desc",
    limit: "100"
  });
  if (modulo) params.set("modulo", `eq.${modulo}`);

  const response = await fetch(`${SUPABASE_URL}/rest/v1/suporte_tickets?${params}`, {
    headers: supabaseHeaders()
  });
  const data = await response.json();
  return validarListaSupabase(response, data, "Supabase lista de tickets");
}

async function listarMensagensTicketSuporteDLH(ticketId, usuarioId) {
  const ticket = await buscarTicketSuporteDLH(ticketId);
  if (!ticket || ticket.usuario_id !== usuarioId) return null;

  const params = new URLSearchParams({
    select: "id,ticket_id,autor_tipo,autor_nome,autor_email,mensagem,canal,criado_em",
    ticket_id: `eq.${ticketId}`,
    order: "criado_em.asc",
    limit: "200"
  });
  const response = await fetch(`${SUPABASE_URL}/rest/v1/suporte_ticket_mensagens?${params}`, {
    headers: supabaseHeaders()
  });
  const data = await response.json();
  return { ticket, mensagens: validarListaSupabase(response, data, "Supabase mensagens do ticket") };
}

async function enviarEmailTicketSuporte(ticket) {
  if (!SUPPORT_RESEND_API_KEY) return { ok: false, aviso: "RESEND_API_KEY nao configurada" };
  try {
    const response = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${SUPPORT_RESEND_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        from: SUPPORT_FROM_EMAIL,
        to: [SUPPORT_TO_EMAIL],
        subject: `[${ticket.id}] ${ticket.assunto}`,
        html: montarHtmlTicketSuporte(ticket),
        reply_to: ticket.usuario.email || SUPPORT_TO_EMAIL
      })
    });
    if (!response.ok) return { ok: false, aviso: await response.text() };
    const data = await response.json().catch(() => ({}));
    return { ok: true, id: data.id || null };
  } catch (e) {
    return { ok: false, aviso: e.message };
  }
}

async function enviarTelegramTicketSuporte(ticket) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    return { ok: false, aviso: "Telegram nao configurado no backend" };
  }

  const payload = {
    chat_id: TELEGRAM_CHAT_ID,
    text: montarTextoTelegramSuporte(ticket),
    parse_mode: "HTML",
    disable_web_page_preview: true
  };

  for (let tentativa = 1; tentativa <= 2; tentativa += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 12000);
    try {
      const response = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: controller.signal
      });
      const data = await response.json().catch(() => ({}));
      if (response.ok && data?.ok === true) return { ok: true, message_id: data?.result?.message_id || null };

      const status = Number(response.status || 0);
      console.error("Falha ao enviar novo ticket DLH para o Telegram", {
        tentativa,
        status,
        codigo: data?.error_code || status || null,
        descricao: String(data?.description || "resposta invalida da API do Telegram").replace(/[\r\n]+/g, " ").slice(0, 240)
      });
      if ((status === 429 || status >= 500) && tentativa < 2) {
        await new Promise((resolve) => setTimeout(resolve, 700));
        continue;
      }
      const aviso = status === 401
        ? "Token do Telegram invalido ou expirado"
        : status === 400
          ? "Chat do Telegram invalido ou mensagem rejeitada"
          : status === 403
            ? "Bot do Telegram sem permissao neste chat"
            : status === 429
              ? "Limite temporario do Telegram atingido"
              : status >= 500
                ? "Telegram indisponivel temporariamente"
                : `Telegram recusou o envio (${status || data?.error_code || "erro"})`;
      return { ok: false, aviso };
    } catch (e) {
      if (tentativa < 2) {
        await new Promise((resolve) => setTimeout(resolve, 700));
        continue;
      }
      return { ok: false, aviso: e?.name === "AbortError" ? "Tempo esgotado ao enviar para o Telegram" : "Falha de comunicacao com o Telegram" };
    } finally {
      clearTimeout(timeout);
    }
  }

  return { ok: false, aviso: "Falha de comunicacao com o Telegram" };
}

async function abrirTicketSuporte(req, res, modulo) {
  try {
    const ticket = montarTicketSuporte(req, modulo);
    // Persistir primeiro evita avisos no Telegram/e-mail apontando para tickets
    // que nao existem no banco e, portanto, nao podem ser abertos ou respondidos.
    const banco = await gravarTicketSuporte(ticket);
    if (!banco.ok) {
      console.error("Falha ao gravar ticket DLH:", banco.aviso || "erro desconhecido");
      return res.status(503).json({
        erro: "O chamado nao foi registrado. Tente novamente em instantes.",
        ticket_id: ticket.id,
        banco_gravado: false
      });
    }

    const [email, telegram] = await Promise.all([
      enviarEmailTicketSuporte(ticket),
      enviarTelegramTicketSuporte(ticket)
    ]);

    res.status(201).json({
      ok: true,
      ticket_id: ticket.id,
      mensagem: "Chamado registrado",
      banco_gravado: banco.ok,
      email_enviado: email.ok,
      telegram_enviado: telegram.ok,
      telegram_aviso: telegram.ok ? null : telegram.aviso || "Telegram indisponivel",
      avisos: [banco, email, telegram]
        .filter(item => !item.ok && item.aviso)
        .map(item => item.aviso)
    });
  } catch (e) {
    res.status(e.statusCode || 500).json({
      erro: e.statusCode === 400 ? e.message : "Nao foi possivel abrir o chamado"
    });
  }
}

function limparCachesAuth() {
  const agora = Date.now();
  for (const [chave, item] of authCache) {
    if (item.expiraEm <= agora) authCache.delete(chave);
  }
  for (const [chave, item] of profileCache) {
    if (item.expiraEm <= agora) profileCache.delete(chave);
  }
}

async function buscarPerfilUsuario(user) {
  const cache = profileCache.get(user.id);
  if (cache?.expiraEm > Date.now()) return cache.valor;

  const response = await fetch(
    `${SUPABASE_URL}/rest/v1/profiles?id=eq.${encodeURIComponent(user.id)}&select=id,email,nome,role,ativo,aprovado`,
    { headers: supabaseHeaders() }
  );
  const data = await response.json();
  const registros = validarListaSupabase(response, data, "Supabase perfil");
  const perfil = registros[0] || null;

  if (!perfil) throw new Error("Perfil de acesso não encontrado");
  profileCache.set(user.id, { valor: perfil, expiraEm: Date.now() + PROFILE_CACHE_MS });
  return perfil;
}

async function autenticarToken(token) {
  limparCachesAuth();
  const cache = authCache.get(token);
  if (cache?.expiraEm > Date.now()) return cache.valor;
  if (!supabaseAdmin) throw new Error("Supabase Auth não configurado");

  const { data, error } = await supabaseAdmin.auth.getUser(token);
  if (error || !data?.user) throw new Error("Sessão inválida ou expirada");

  const perfil = await buscarPerfilUsuario(data.user);
  if (!perfil.ativo) throw new Error("Usuário desativado");

  if (!perfil.aprovado) throw new Error("Usuario aguardando aprovacao");

  const auth = { user: data.user, perfil };
  authCache.set(token, { valor: auth, expiraEm: Date.now() + AUTH_CACHE_MS });
  return auth;
}

function papeisPermitidos(req) {
  const rota = req.path;
  const metodo = req.method;

  if (rota.startsWith("/dlh/metricas")) return ["dev"];
  if (
    metodo === "DELETE" ||
    rota === "/dlh/sync" ||
    rota === "/dlh/reprocess" ||
    (rota === "/dlh/criterios" && metodo === "PATCH")
  ) {
    return ["dev", "administrador"];
  }
  if (metodo === "POST" && rota.startsWith("/dlh/downloads/")) {
    return ["dev", "administrador", "usuario"];
  }

  return ["dev", "administrador", "usuario", "auditor"];
}

function identificarAcaoAuditoriaDLH(metodo, rota) {
  if (rota === "/dlh/sync") return ["SINCRONIZAR", "Sincronização DLH iniciada"];
  if (rota === "/dlh/reprocess") return ["REPROCESSAR", "Reprocessamento DLH iniciado"];
  if (metodo === "PATCH" && rota === "/dlh/criterios") return ["ALTERAR_DMA", "Critérios DLH alterados"];
  if (metodo === "DELETE" && rota.startsWith("/dlh/certificados/")) return ["EXCLUIR_CERTIFICADO", "Certificado DLH excluído"];
  if (metodo === "POST" && rota === "/dlh/downloads/massa") return ["DOWNLOAD_MASSA", "Download em massa DLH solicitado"];
  if (metodo === "POST" && rota.startsWith("/dlh/downloads/massa/") && rota.endsWith("/link")) return ["BAIXAR_ZIP", "Link temporário do ZIP DLH gerado"];
  if (metodo === "GET" && rota.startsWith("/dlh/downloads/massa/") && rota.endsWith("/arquivo")) return ["BAIXAR_ZIP", "Arquivo ZIP DLH baixado"];
  if (metodo === "GET" && rota.startsWith("/dlh/download/")) return ["DOWNLOAD_CERTIFICADO", "Certificado DLH baixado"];
  if (metodo === "POST" && rota === "/dlh/suporte/tickets") return ["ABRIR_TICKET", "Ticket de suporte DLH aberto"];
  return null;
}

function entidadeAuditoriaDLH(rota) {
  const partes = String(rota || "").split("/").filter(Boolean);
  const candidato = partes.at(-1);
  if (!candidato || ["sync", "reprocess", "massa", "arquivo", "criterios", "tickets"].includes(candidato)) return null;
  return candidato.slice(0, 160);
}

async function registrarAuditoriaDLH(req, statusCode, acao, descricao) {
  if (!SUPABASE_URL || !SUPABASE_KEY || !req.auth?.user?.id) return;
  try {
    const response = await fetch(`${SUPABASE_URL}/rest/v1/audit_logs`, {
      method: "POST",
      headers: { ...supabaseHeaders(), Prefer: "return=minimal" },
      body: JSON.stringify({
        user_id: req.auth.user.id,
        user_email: req.auth.user.email || req.auth.perfil?.email || "",
        action: acao,
        module: "DLH",
        entity: entidadeAuditoriaDLH(req.path),
        description: descricao,
        request_path: req.path,
        request_method: req.method,
        status_code: statusCode
      })
    });
    if (!response.ok) console.warn("Falha ao registrar auditoria DLH:", response.status);
  } catch (e) {
    console.warn("Falha ao registrar auditoria DLH:", e.message);
  }
}

app.use(async (req, res, next) => {
  const possuiTicketDownload =
    req.method === "GET" &&
    /^\/dlh\/downloads\/massa\/[^/]+\/arquivo$/.test(req.path) &&
    Boolean(req.query?.ticket);
  if (
    !AUTH_ENABLED ||
    req.method === "OPTIONS" ||
    req.path === "/" ||
    req.path === "/dlh/versao" ||
    req.path === "/dlh/status-publico" ||
    req.path.startsWith("/dlh/automacao/") ||
    possuiTicketDownload
  ) {
    return next();
  }

  try {
    const authorization = String(req.headers.authorization || "");
    const token = authorization.startsWith("Bearer ") ? authorization.slice(7).trim() : "";
    if (!token) return res.status(401).json({ erro: "Autenticação obrigatória" });

    req.auth = await autenticarToken(token);
    if (!papeisPermitidos(req).includes(req.auth.perfil.role)) {
      return res.status(403).json({ erro: "Você não possui permissão para esta ação" });
    }
    next();
  } catch (e) {
    res.status(401).json({ erro: e.message });
  }
});

app.use((req, res, next) => {
  const evento = req.auth ? identificarAcaoAuditoriaDLH(req.method, req.path) : null;
  if (evento) {
    res.once("finish", () => {
      if (res.statusCode >= 200 && res.statusCode < 400) {
        registrarAuditoriaDLH(req, res.statusCode, evento[0], evento[1]).catch(() => {});
      }
    });
  }
  next();
});

function validarSegredoAutomacao(req, res) {
  const recebido = String(req.headers["x-automation-secret"] || "");
  if (!AUTOMATION_SECRET || !recebido) {
    res.status(401).json({ erro: "Automação não autorizada" });
    return false;
  }

  const esperadoBuffer = Buffer.from(AUTOMATION_SECRET);
  const recebidoBuffer = Buffer.from(recebido);
  const valido =
    esperadoBuffer.length === recebidoBuffer.length &&
    crypto.timingSafeEqual(esperadoBuffer, recebidoBuffer);

  if (!valido) {
    res.status(401).json({ erro: "Automação não autorizada" });
    return false;
  }

  return true;
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
const downloadTicketsDLH = new Map();
const DOWNLOAD_JOB_PROGRESS_STEP = Number(process.env.DOWNLOAD_JOB_PROGRESS_STEP || 25);
const DOWNLOAD_TICKET_TTL_MS = Number(process.env.DOWNLOAD_TICKET_TTL_MS || 300000);

function criarDownloadTicketDLH(jobId) {
  const ticket = crypto.randomBytes(32).toString("hex");
  const expiraEm = Date.now() + DOWNLOAD_TICKET_TTL_MS;
  downloadTicketsDLH.set(ticket, { jobId, expiraEm });
  return { ticket, expiraEm };
}

function validarDownloadTicketDLH(jobId, ticket) {
  const agora = Date.now();
  for (const [chave, valor] of downloadTicketsDLH) {
    if (valor.expiraEm <= agora) downloadTicketsDLH.delete(chave);
  }
  const registro = downloadTicketsDLH.get(String(ticket || ""));
  return Boolean(registro && registro.jobId === jobId && registro.expiraEm > agora);
}

function serializarDownloadJobDLH(job) {
  return {
    id: job.id,
    modulo: job.tipo || "DLH",
    status: job.status,
    total: Number(job.total || 0),
    processados: Number(job.processados || 0),
    falhas: Number(job.falhas || 0),
    erros: Array.isArray(job.erros) ? job.erros.slice(-50) : [],
    erro: job.erro || null,
    aviso_drive: job.aviso_drive || null,
    arquivo_zip_nome: job.arquivo_zip_nome || null,
    arquivo_zip_drive_id: job.arquivo_zip_drive_id || null,
    arquivo_zip_link: job.arquivo_zip_link || null,
    solicitado_por: job.solicitado_por || null,
    solicitado_email: job.solicitado_email || null,
    parametros: job.parametros || {},
    criado_em: job.criado_em,
    atualizado_em: job.atualizado_em || new Date().toISOString(),
    expira_em: job.expira_em || null
  };
}

function hidratarDownloadJobDLH(row) {
  if (!row) return null;
  return {
    id: row.id,
    tipo: row.modulo || "DLH",
    status: row.status,
    total: Number(row.total || 0),
    processados: Number(row.processados || 0),
    falhas: Number(row.falhas || 0),
    erros: Array.isArray(row.erros) ? row.erros : [],
    erro: row.erro || null,
    aviso_drive: row.aviso_drive || null,
    arquivo_zip_nome: row.arquivo_zip_nome || null,
    arquivo_zip_drive_id: row.arquivo_zip_drive_id || null,
    arquivo_zip_link: row.arquivo_zip_link || null,
    solicitado_por: row.solicitado_por || null,
    solicitado_email: row.solicitado_email || null,
    parametros: row.parametros || {},
    criado_em: row.criado_em,
    atualizado_em: row.atualizado_em,
    expira_em: row.expira_em
  };
}

async function salvarDownloadJobDLH(job) {
  if (!SUPABASE_URL || !SUPABASE_KEY || !job?.id) return;
  try {
    const response = await fetch(`${SUPABASE_URL}/rest/v1/download_jobs?on_conflict=id`, {
      method: "POST",
      headers: { ...supabaseHeaders(), Prefer: "resolution=merge-duplicates,return=minimal" },
      body: JSON.stringify(serializarDownloadJobDLH(job))
    });
    if (!response.ok) {
      const detalhe = await response.text().catch(() => "");
      console.warn("Falha ao salvar download_jobs:", detalhe || response.status);
    }
  } catch (e) {
    console.warn("Falha ao salvar download_jobs:", e.message);
  }
}

async function buscarDownloadJobPersistidoDLH(jobId, modulo = "DLH") {
  if (!SUPABASE_URL || !SUPABASE_KEY || !jobId) return null;
  const response = await fetch(
    `${SUPABASE_URL}/rest/v1/download_jobs?id=eq.${encodeURIComponent(jobId)}&modulo=eq.${modulo}&select=*`,
    { headers: supabaseHeaders() }
  );
  const data = await response.json().catch(() => []);
  if (!response.ok || !Array.isArray(data)) return null;
  return hidratarDownloadJobDLH(data[0]);
}

async function listarDownloadJobsPersistidosDLH(modulo = "DLH", limit = 50, solicitadoPor = null) {
  if (!SUPABASE_URL || !SUPABASE_KEY) return [];
  const filtroUsuario = solicitadoPor
    ? `&solicitado_por=eq.${encodeURIComponent(solicitadoPor)}`
    : "";
  const response = await fetch(
    `${SUPABASE_URL}/rest/v1/download_jobs?modulo=eq.${modulo}${filtroUsuario}&select=*&order=criado_em.desc&limit=${limit}`,
    { headers: supabaseHeaders() }
  );
  const data = await response.json().catch(() => []);
  if (!response.ok || !Array.isArray(data)) return [];
  return data.map(hidratarDownloadJobDLH);
}

function podeAcessarDownloadJobDLH(req, job) {
  const role = req.auth?.perfil?.role;
  if (role === "dev" || role === "administrador") return true;
  return Boolean(job?.solicitado_por) && job.solicitado_por === req.auth?.user?.id;
}

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
    .split(/[;,\s\n]+/)
    .map(v => v.trim())
    .filter(Boolean);
}

function montarFiltroBuscaCertificado(termos, normalizador, campoEquipamento) {
  const loggers = new Set();
  for (const termo of termos) {
    const normalizado = normalizador(termo);
    if (normalizado) loggers.add(normalizado);

    // Bases antigas podem guardar o mesmo logger como "0163" ou "DLH-0163".
    const numero = String(termo || "").replace(/\D/g, "");
    if (numero) {
      const codigo = numero.padStart(4, "0");
      loggers.add(codigo);
      loggers.add(`DLH-${codigo}`);
    }
  }
  const series = [...new Set(
    termos
      .map((valor) => String(valor || "").replace(/\D/g, ""))
      .filter(Boolean)
  )];
  const partes = [];
  if (loggers.size) partes.push(`${campoEquipamento}.in.(${[...loggers].join(",")})`);
  if (series.length) partes.push(`serie.in.(${series.join(",")})`);
  return partes.length ? `(${partes.join(",")})` : "";
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
  if (testeInicio && testeFim) {
    params.set("data", `lte.${testeFim}`);
    params.set("vencimento", `gte.${testeInicio}`);
  }
  params.append("order", `${campoEquipamento}.asc`);
  params.append("order", "data.desc");
  params.append("order", "id.asc");
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

  if ((testeInicio && !testeFim) || (!testeInicio && testeFim)) {
    throw new Error("Informe data inicial e data final do teste, ou deixe as duas em branco");
  }

  if (testeInicio && testeFim && testeInicio > testeFim) {
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

function montarResultadoBuscaLista(registros, equipamentosInformados, campoEquipamento, normalizador) {
  const solicitados = [...new Set(equipamentosInformados.map(normalizador).filter(Boolean))];
  const encontradosSet = new Set(
    registros.map(item => normalizador(item[campoEquipamento])).filter(Boolean)
  );

  return {
    total_equipamentos_informados: solicitados.length,
    total_equipamentos_encontrados: encontradosSet.size,
    total_certificados_encontrados: registros.length,
    equipamentos_encontrados: solicitados.filter(item => encontradosSet.has(item)),
    equipamentos_nao_encontrados: solicitados.filter(item => !encontradosSet.has(item)),
    registros
  };
}

function hojeIso() {
  return new Date().toISOString().slice(0, 10);
}

function adicionarDiasIso(dataIso, dias) {
  const data = new Date(`${dataIso}T00:00:00.000Z`);
  data.setUTCDate(data.getUTCDate() + Number(dias || 0));
  return data.toISOString().slice(0, 10);
}

function limitarNumero(valor, padrao, minimo, maximo) {
  const numero = Number(valor);
  if (!Number.isFinite(numero)) return padrao;
  return Math.min(maximo, Math.max(minimo, Math.trunc(numero)));
}

async function contarRegistrosSupabase(tabela, filtros = []) {
  validarConfiguracaoBasica();
  const params = new URLSearchParams();
  params.set("select", "id");
  for (const [chave, valor] of filtros) params.append(chave, valor);

  const response = await fetch(`${SUPABASE_URL}/rest/v1/${tabela}?${params.toString()}`, {
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

  return Number(response.headers.get("content-range")?.split("/")[1] || 0);
}

async function buscarRegistrosAssistente(tabela, select, filtros = [], opcoes = {}) {
  validarConfiguracaoBasica();
  const params = new URLSearchParams();
  params.set("select", select);
  for (const [chave, valor] of filtros) params.append(chave, valor);
  if (opcoes.order) params.append("order", opcoes.order);
  params.set("limit", String(limitarNumero(opcoes.limit, 100, 1, 1000)));
  params.set("offset", String(limitarNumero(opcoes.offset, 0, 0, 100000)));

  const headers = { ...supabaseHeaders() };
  if (opcoes.count) headers.Prefer = "count=exact";
  const response = await fetch(`${SUPABASE_URL}/rest/v1/${tabela}?${params.toString()}`, {
    headers
  });
  const data = await response.json();
  const registros = validarListaSupabase(response, data, `Supabase ${tabela}`);
  return {
    total: opcoes.count
      ? Number(response.headers.get("content-range")?.split("/")[1] || registros.length)
      : registros.length,
    registros
  };
}

function explicarDivergenciaCertificadoDLH(certificado) {
  const motivo = String(certificado.motivo_divergencia || "").trim();
  const status = String(certificado.status || "").toUpperCase();
  const serie = String(certificado.serie || "").trim();
  const serieEsperada = String(certificado.serie_esperada || "").trim();
  const vencimento = String(certificado.vencimento || "").slice(0, 10);
  const vencido = Boolean(vencimento && vencimento < hojeIso());

  if (status === "REPROVADO") {
    return {
      tipo: "certificado_reprovado",
      severidade: "alta",
      resumo: "Certificado reprovado nos criterios de aceitacao.",
      detalhe: "Ha ponto de temperatura ou umidade fora do limite configurado.",
      acao_recomendada: "Bloquear uso do logger neste ensaio e solicitar avaliacao da empresa de calibracao.",
      encaminhar_calibracao: true
    };
  }

  if (status === "ERRO") {
    return {
      tipo: "erro_processamento",
      severidade: "alta",
      resumo: "O backend nao conseguiu interpretar o certificado com seguranca.",
      detalhe: motivo || "O arquivo pode estar fora do padrao esperado, protegido ou incompleto.",
      acao_recomendada: "Conferir o PDF original e solicitar reemissao caso o documento esteja fora do padrao.",
      encaminhar_calibracao: true
    };
  }

  if (certificado.duplicado) {
    return {
      tipo: "certificado_duplicado",
      severidade: "media",
      resumo: "Existe indicio de certificado duplicado para o mesmo DLH.",
      detalhe: motivo || "Pode haver mais de um arquivo representando o mesmo certificado.",
      acao_recomendada: "Manter apenas o registro correto e revisar o historico antes de emitir relatorio.",
      encaminhar_calibracao: false
    };
  }

  if (certificado.divergente && serie && serieEsperada && serie !== serieEsperada) {
    return {
      tipo: "serie_divergente",
      severidade: "media",
      resumo: "A serie extraida do certificado nao bate com a serie esperada para o logger.",
      detalhe: `Serie no certificado: ${serie}. Serie esperada: ${serieEsperada}.`,
      acao_recomendada: "Confirmar se o certificado pertence ao equipamento correto antes de aprovar.",
      encaminhar_calibracao: true
    };
  }

  if (certificado.divergente || motivo) {
    return {
      tipo: "divergencia",
      severidade: "media",
      resumo: "Existe uma divergencia cadastrada para este certificado.",
      detalhe: motivo || "Divergencia sem motivo detalhado registrado.",
      acao_recomendada: "Abrir o certificado e validar os dados principais antes de liberar para uso.",
      encaminhar_calibracao: true
    };
  }

  if (vencido) {
    return {
      tipo: "vencido",
      severidade: "alta",
      resumo: "O certificado esta vencido.",
      detalhe: `Vencimento registrado: ${vencimento}.`,
      acao_recomendada: "Remover o logger da selecao de ensaio ate existir certificado vigente.",
      encaminhar_calibracao: false
    };
  }

  return {
    tipo: "sem_divergencia",
    severidade: "baixa",
    resumo: "Nao foi identificada divergencia automatica relevante.",
    detalhe: "O certificado passou pelas regras conhecidas do backend.",
    acao_recomendada: "Manter rastreabilidade normal.",
    encaminhar_calibracao: false
  };
}

function resumirCertificadosAssistente(registros) {
  const hoje = hojeIso();
  const em30 = adicionarDiasIso(hoje, 30);
  const em60 = adicionarDiasIso(hoje, 60);
  const total = registros.length;
  const aprovados = registros.filter(c => String(c.status || "").toUpperCase() === "APROVADO").length;
  const reprovados = registros.filter(c => String(c.status || "").toUpperCase() === "REPROVADO").length;
  const erros = registros.filter(c => String(c.status || "").toUpperCase() === "ERRO").length;
  const divergentes = registros.filter(c => c.divergente || c.duplicado || c.motivo_divergencia).length;
  const vencidos = registros.filter(c => c.vencimento && String(c.vencimento).slice(0, 10) < hoje).length;
  const vence30 = registros.filter(c => {
    const vencimento = String(c.vencimento || "").slice(0, 10);
    return vencimento >= hoje && vencimento <= em30;
  }).length;
  const vence60 = registros.filter(c => {
    const vencimento = String(c.vencimento || "").slice(0, 10);
    return vencimento > em30 && vencimento <= em60;
  }).length;

  return { total, aprovados, reprovados, erros, divergentes, vencidos, vence30, vence60 };
}

async function montarResumoModuloDLH() {
  const hoje = hojeIso();
  const em30 = adicionarDiasIso(hoje, 30);
  const em60 = adicionarDiasIso(hoje, 60);
  const [
    total,
    aprovados,
    reprovados,
    erros,
    divergentes,
    vencidos,
    vence30,
    vence60
  ] = await Promise.all([
    contarRegistrosSupabase("certificados_dlh"),
    contarRegistrosSupabase("certificados_dlh", [["status", "eq.APROVADO"]]),
    contarRegistrosSupabase("certificados_dlh", [["status", "eq.REPROVADO"]]),
    contarRegistrosSupabase("certificados_dlh", [["status", "eq.ERRO"]]),
    contarRegistrosSupabase("certificados_dlh", [["or", "(divergente.eq.true,duplicado.eq.true,motivo_divergencia.not.is.null)"]]),
    contarRegistrosSupabase("certificados_dlh", [["vencimento", `lt.${hoje}`]]),
    contarRegistrosSupabase("certificados_dlh", [["vencimento", `gte.${hoje}`], ["vencimento", `lte.${em30}`]]),
    contarRegistrosSupabase("certificados_dlh", [["vencimento", `gt.${em30}`], ["vencimento", `lte.${em60}`]])
  ]);

  return {
    modulo: "DLH",
    gerado_em: new Date().toISOString(),
    total,
    aprovados,
    reprovados,
    erros,
    divergentes,
    vencidos,
    ate_30_dias: vence30,
    de_31_a_60_dias: vence60
  };
}

function montarRiscosAssistente(resumo) {
  const riscos = [];
  if (resumo.vencidos > 0) {
    riscos.push({
      prioridade: "alta",
      titulo: "Certificados vencidos",
      detalhe: `${resumo.vencidos} certificado(s) vencido(s) podem bloquear auditoria ou uso em teste.`,
      acao: "Recalibrar ou substituir os loggers antes de novos ensaios."
    });
  }
  if (resumo.reprovados > 0 || resumo.erros > 0) {
    riscos.push({
      prioridade: "alta",
      titulo: "Certificados reprovados ou com erro",
      detalhe: `${resumo.reprovados} reprovado(s) e ${resumo.erros} com erro de leitura.`,
      acao: "Encaminhar para avaliacao da empresa de calibracao."
    });
  }
  if (resumo.divergentes > 0) {
    riscos.push({
      prioridade: "media",
      titulo: "Divergencias de rastreabilidade",
      detalhe: `${resumo.divergentes} certificado(s) com divergencia de dados ou duplicidade.`,
      acao: "Conferir serie, TAG e numero do certificado antes de liberar relatorios."
    });
  }
  if (resumo.ate_30_dias > 0) {
    riscos.push({
      prioridade: "media",
      titulo: "Vencimentos proximos",
      detalhe: `${resumo.ate_30_dias} certificado(s) vencem em ate 30 dias.`,
      acao: "Planejar calibracao para evitar parada operacional."
    });
  }
  if (!riscos.length) {
    riscos.push({
      prioridade: "baixa",
      titulo: "Sem risco critico identificado",
      detalhe: "Nao ha vencidos, reprovados ou divergencias relevantes no resumo atual.",
      acao: "Manter monitoramento semanal."
    });
  }
  return riscos;
}

function montarMensagemExecutiva(resumo, riscos) {
  const principal = riscos[0];
  return {
    titulo: "Resumo executivo DLH",
    resumo: `Base DLH com ${resumo.total} certificado(s), ${resumo.vencidos} vencido(s), ${resumo.divergentes} divergente(s) e ${resumo.reprovados} reprovado(s).`,
    leitura_gerencial: principal?.prioridade === "baixa"
      ? "A operacao esta controlada no momento."
      : `Atencao para ${principal.titulo.toLowerCase()}: ${principal.detalhe}`,
    proxima_acao: principal?.acao || "Manter rotina de acompanhamento."
  };
}

async function buscarCertificadoAssistenteDLH(id) {
  const response = await fetch(
    `${SUPABASE_URL}/rest/v1/certificados_dlh?id=eq.${encodeURIComponent(id)}&select=${CERTIFICADOS_DLH_LISTA_SELECT},pontos_umidade,pontos_temperatura&limit=1`,
    { headers: supabaseHeaders() }
  );
  const data = await response.json();
  const registros = validarListaSupabase(response, data, "Supabase certificado DLH");
  return registros[0] || null;
}

function normalizarTextoOperacional(valor) {
  return String(valor || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function codigoCurtoLogger(codigo) {
  const digitos = String(codigo || "").replace(/\D/g, "");
  return digitos ? digitos.padStart(4, "0") : "";
}

function sanitizarValorIn(valor) {
  return String(valor || "").replace(/[()"]/g, "");
}

function localEhAreaTecnica(local) {
  const texto = normalizarTextoOperacional(local);
  return !texto || texto.includes("area tecnica") || texto.includes("conferido") || texto === "disponivel";
}

function localEhCliente(local) {
  return normalizarTextoOperacional(local).includes("cliente");
}

function localEhManutencao(local) {
  const texto = normalizarTextoOperacional(local);
  return texto.includes("manut") || texto.includes("calib");
}

function montarBaseLoggersDLH() {
  return Object.entries(MAPA_LOGGERS_DLH).map(([codigo, serie]) => ({
    modulo: "DLH",
    logger_codigo: normalizarDLH(codigo),
    logger: codigoCurtoLogger(codigo),
    serie_esperada: serie
  }));
}

async function buscarStatusAtualDLHOperacional(codigos = []) {
  const params = new URLSearchParams();
  params.set(
    "select",
    "modulo,logger_codigo,local_atual,cliente,responsavel,observacao,data_movimentacao,ultima_movimentacao_id,usuario_email,atualizado_em"
  );
  params.set("modulo", "eq.DLH");
  params.append("order", "data_movimentacao.desc");
  params.set("limit", "10000");
  if (codigos.length) {
    const variantes = new Set();
    for (const codigo of codigos) {
      const normalizado = normalizarDLH(codigo);
      if (normalizado) variantes.add(normalizado);
      const curto = codigoCurtoLogger(codigo);
      if (curto) variantes.add(curto);
    }
    params.set("logger_codigo", `in.(${[...variantes].map(sanitizarValorIn).join(",")})`);
  }

  const response = await fetch(`${SUPABASE_URL}/rest/v1/logger_status_atual?${params.toString()}`, {
    headers: supabaseHeaders()
  });
  const data = await response.json().catch(() => []);
  if (!response.ok) {
    return {
      registros: [],
      aviso: "Tabela logger_status_atual indisponivel; disponibilidade assumida como Area Tecnica."
    };
  }
  return { registros: Array.isArray(data) ? data : [], aviso: null };
}

function combinarDisponibilidadeDLH(statusRegistros, opcoes = {}) {
  const statusPorLogger = new Map();
  for (const item of statusRegistros) {
    const chave = normalizarDLH(item.logger_codigo);
    if (chave && !statusPorLogger.has(chave)) statusPorLogger.set(chave, item);
  }

  const base = montarBaseLoggersDLH().map(item => {
    const status = statusPorLogger.get(item.logger_codigo) || {};
    const localAtual = status.local_atual || "Conferido - Area Tecnica";
    return {
      ...item,
      local_atual: localAtual,
      cliente: status.cliente || null,
      responsavel: status.responsavel || null,
      observacao: status.observacao || null,
      data_movimentacao: status.data_movimentacao || null,
      ultima_movimentacao_id: status.ultima_movimentacao_id || null,
      fora_empresa: !localEhAreaTecnica(localAtual),
      em_cliente: localEhCliente(localAtual),
      manutencao_calibracao: localEhManutencao(localAtual)
    };
  });

  const busca = normalizarTextoOperacional(opcoes.busca);
  const lista = new Set((opcoes.lista || []).map(normalizarDLH).filter(Boolean));
  const localFiltro = normalizarTextoOperacional(opcoes.local);

  const filtrados = base.filter(item => {
    if (lista.size && !lista.has(item.logger_codigo)) return false;
    if (localFiltro && localFiltro !== "todos" && !normalizarTextoOperacional(item.local_atual).includes(localFiltro)) {
      return false;
    }
    if (!busca) return true;
    return [
      item.logger_codigo,
      item.logger,
      item.serie_esperada,
      item.local_atual,
      item.cliente,
      item.responsavel
    ].some(valor => normalizarTextoOperacional(valor).includes(busca));
  });

  const resumoBase = lista.size || busca || localFiltro ? filtrados : base;
  const clientes = new Set(resumoBase.filter(item => item.em_cliente && item.cliente).map(item => item.cliente));
  const resumo = {
    total: resumoBase.length,
    area_tecnica: resumoBase.filter(item => localEhAreaTecnica(item.local_atual)).length,
    em_cliente: resumoBase.filter(item => item.em_cliente).length,
    manutencao_calibracao: resumoBase.filter(item => item.manutencao_calibracao).length,
    fora_empresa: resumoBase.filter(item => item.fora_empresa).length,
    clientes: clientes.size
  };

  return { resumo, registros: filtrados };
}

function avaliarChecklistPreTesteDLH({ equipamento, disponibilidade, certificados, testeInicio, testeFim }) {
  const problemas = [];
  const avisos = [];
  const certificadosOrdenados = certificados
    .slice()
    .sort((a, b) => String(b.data || "").localeCompare(String(a.data || "")));

  if (!disponibilidade) {
    problemas.push("Logger nao cadastrado no mapa DLH.");
  } else if (!localEhAreaTecnica(disponibilidade.local_atual)) {
    problemas.push(`Logger fora da Area Tecnica: ${disponibilidade.local_atual}.`);
  }

  if (!certificadosOrdenados.length) {
    problemas.push("Nenhum certificado encontrado para o periodo informado.");
  }

  const bloqueados = certificadosOrdenados.filter(c =>
    ["REPROVADO", "ERRO"].includes(String(c.status || "").toUpperCase()) ||
    c.divergente ||
    c.duplicado ||
    c.motivo_divergencia
  );
  if (bloqueados.length) {
    problemas.push(`${bloqueados.length} certificado(s) com reprovacao, erro, duplicidade ou divergencia.`);
  }

  if (certificadosOrdenados.length > 1) {
    avisos.push("Ha mais de um certificado cobrindo o periodo; manter todos no dossie para rastreabilidade.");
  }

  return {
    equipamento,
    logger: codigoCurtoLogger(equipamento),
    local_atual: disponibilidade?.local_atual || "Nao cadastrado",
    cliente: disponibilidade?.cliente || null,
    certificados_encontrados: certificadosOrdenados.length,
    certificado_mais_recente: certificadosOrdenados[0] || null,
    certificados: certificadosOrdenados.slice(0, 20),
    teste_inicio: testeInicio || null,
    teste_fim: testeFim || null,
    pronto: problemas.length === 0,
    problemas,
    avisos
  };
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

function agendarRemocaoArquivoTemporarioDLH(caminho, minutos = 60) {
  const timer = setTimeout(() => {
    fs.promises.rm(caminho, { force: true }).catch(() => {});
  }, minutos * 60 * 1000);
  if (typeof timer.unref === "function") timer.unref();
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
      if ((index + 1) % DOWNLOAD_JOB_PROGRESS_STEP === 0 || index === registros.length - 1) {
        await salvarDownloadJobDLH(job);
      }
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
  await salvarDownloadJobDLH(job);

  const nomeArquivo = `CERTIFICADOS_DLH_${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}.zip`;
  const zipPath = path.join(os.tmpdir(), nomeArquivo);
  try {
    await criarZipNoDiscoDLH(registros, zipPath, job);
    let arquivoDrive = {};
    try {
      arquivoDrive = await salvarZipNoDriveDLH(zipPath, nomeArquivo);
    } catch (e) {
      job.aviso_drive = `ZIP criado localmente, mas nao foi salvo no Drive: ${e.message}`;
    }

    job.status = "concluido";
    job.arquivo_zip_nome = nomeArquivo;
    job.arquivo_zip_local_path = zipPath;
    job.arquivo_zip_drive_id = arquivoDrive.id || null;
    job.arquivo_zip_link = arquivoDrive.webViewLink || arquivoDrive.webContentLink || null;
    job.atualizado_em = new Date().toISOString();
    await salvarDownloadJobDLH(job);
    agendarRemocaoArquivoTemporarioDLH(zipPath, 60);
  } catch (e) {
    await fs.promises.rm(zipPath, { force: true }).catch(() => {});
    throw e;
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

function obterIntervaloRelatorio(query = {}) {
  const dataUnica = String(query.data || "").trim();
  const dataInicio = String(query.data_inicio || query.inicio || dataUnica || obterHojeISO()).trim();
  const dataFim = String(query.data_fim || query.fim || dataUnica || dataInicio).trim();
  const formatoISO = /^\d{4}-\d{2}-\d{2}$/;

  if (!formatoISO.test(dataInicio) || !formatoISO.test(dataFim)) {
    const erro = new Error("Informe data_inicio e data_fim no formato AAAA-MM-DD.");
    erro.statusCode = 400;
    throw erro;
  }

  if (dataInicio > dataFim) {
    const erro = new Error("data_inicio não pode ser posterior a data_fim.");
    erro.statusCode = 400;
    throw erro;
  }

  const periodoFormatado = dataInicio === dataFim
    ? formatarDataISOParaBR(dataInicio)
    : `${formatarDataISOParaBR(dataInicio)} a ${formatarDataISOParaBR(dataFim)}`;

  return {
    dataInicio,
    dataFim,
    periodoFormatado,
    sufixoArquivo: dataInicio === dataFim ? dataInicio : `${dataInicio}_a_${dataFim}`
  };
}

function montarUrlRelatorio(tabela, dataInicio, dataFim, ordenacao) {
  return `${SUPABASE_URL}/rest/v1/${tabela}?select=*&data=gte.${dataInicio}&data=lte.${dataFim}&order=${ordenacao}`;
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

function montarHtmlRelatorioPdfDLH(registros, periodoRelatorio, limiteTemperatura = 0.5, limiteUmidade = 5) {
  const html = valor => String(valor ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
  const numero = valor => {
    const n = Number(valor);
    return Number.isFinite(n) ? n.toFixed(2).replace(".", ",") : "-";
  };
  const buscarPonto = (pontos, alvo, fallbackIndex) => {
    const lista = Array.isArray(pontos) ? pontos : [];
    const candidatos = lista
      .map(ponto => ({ ponto, referencia: Number(ponto?.padrao ?? ponto?.referencia) }))
      .filter(item => Number.isFinite(item.referencia))
      .sort((a, b) => Math.abs(a.referencia - alvo) - Math.abs(b.referencia - alvo));
    return candidatos[0] && Math.abs(candidatos[0].referencia - alvo) <= 3
      ? candidatos[0].ponto
      : lista[fallbackIndex] || {};
  };
  const resultadoPonto = ponto => {
    const soma = Number(ponto?.soma);
    if (Number.isFinite(soma)) return soma;
    const erro = Number(ponto?.erro);
    const incerteza = Number(ponto?.incerteza);
    return Number.isFinite(erro) && Number.isFinite(incerteza)
      ? Math.abs(erro) + Math.abs(incerteza)
      : null;
  };
  const resultadoGrupo = (pontos, limite, quantidade) => {
    const resultados = pontos.map(resultadoPonto).filter(Number.isFinite);
    if (resultados.length < quantidade) return "INDETERMINADO";
    return resultados.every(valor => valor <= Number(limite)) ? "APROVADO" : "REPROVADO";
  };
  const classeSoma = (valor, limite) => Number.isFinite(valor) && valor <= Number(limite) ? "ok" : "bad";
  const classeStatus = status => status === "APROVADO" ? "ok status" : status === "REPROVADO" ? "bad status" : "warn status";

  const linhas = registros.map((c, index) => {
    const temperatura = Array.isArray(c.pontos_temperatura) ? c.pontos_temperatura : [];
    const umidade = Array.isArray(c.pontos_umidade) ? c.pontos_umidade : [];
    const t20 = buscarPonto(temperatura, -20, 0);
    const t0 = buscarPonto(temperatura, 0, 1);
    const t15 = buscarPonto(temperatura, 15, 2);
    const t60 = buscarPonto(temperatura, 60, 3);
    const u10 = buscarPonto(umidade, 10, 0);
    const u50 = buscarPonto(umidade, 50, 1);
    const u90 = buscarPonto(umidade, 90, 2);
    const pontosT = [t20, t0, t15, t60];
    const pontosU = [u10, u50, u90];
    const resultadoT = resultadoGrupo(temperatura, limiteTemperatura, 4);
    const resultadoU = resultadoGrupo(umidade, limiteUmidade, 3);
    const geral = resultadoT === "REPROVADO" || resultadoU === "REPROVADO"
      ? "REPROVADO"
      : resultadoT === "APROVADO" && resultadoU === "APROVADO"
        ? "APROVADO"
        : String(c.status || "INDETERMINADO").toUpperCase();
    const incertezaT = temperatura.map(p => Number(p?.incerteza)).find(Number.isFinite);

    return `<tr class="${index % 2 ? "alt" : ""}">
      <td>${html(c.serie || "")}</td><td>${html(normalizarDLH(c.dlh) || c.dlh || "")}</td>
      <td>${html(formatarDataISOParaBR(c.data))}</td><td>${html(c.mes_ano_validade || "")}</td><td>${html(c.certificado || "")}</td>
      <td>${numero(incertezaT)}</td>
      ${pontosT.map(p => { const soma = resultadoPonto(p); return `<td>${numero(p?.erro)}</td><td class="${classeSoma(soma, limiteTemperatura)}">${numero(soma)}</td>`; }).join("")}
      <td class="${classeStatus(resultadoT)}">${resultadoT}</td>
      ${pontosU.map(p => { const soma = resultadoPonto(p); return `<td>${numero(p?.incerteza)}</td><td>${numero(p?.erro)}</td><td class="${classeSoma(soma, limiteUmidade)}">${numero(soma)}</td>`; }).join("")}
      <td class="${classeStatus(resultadoU)}">${resultadoU}</td><td class="${classeStatus(geral)}">${geral}</td>
    </tr>`;
  }).join("");

  return `<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><style>
    @page { size: A3 landscape; margin: 7mm; }
    * { box-sizing: border-box; }
    body { margin: 0; color: #10233f; font-family: Arial, sans-serif; font-size: 6.4px; }
    .cabecalho { border: 1px solid #0b2855; margin-bottom: 5px; }
    .topo { display: grid; grid-template-columns: 180px 1fr 180px; align-items: center; min-height: 46px; border-bottom: 1px solid #0b2855; }
    .codigo { padding: 6px; line-height: 1.45; }
    .titulo { color: #0b2855; font-size: 12px; font-weight: 700; text-align: center; }
    .marca { padding-right: 10px; color: #0b2855; font-size: 20px; font-weight: 700; text-align: right; }
    .marca span { color: #27d3ae; }
    .meta { display: grid; grid-template-columns: repeat(5, 1fr); background: #ddf7f1; }
    .meta div { padding: 4px 6px; border-right: 1px solid #0b2855; }
    .meta div:last-child { border-right: 0; }
    table { width: 100%; border-collapse: collapse; table-layout: fixed; }
    thead { display: table-header-group; }
    tr { break-inside: avoid; }
    th, td { border: 1px solid #8aa2b8; padding: 2.5px 1.5px; text-align: center; vertical-align: middle; overflow-wrap: anywhere; }
    th { color: white; background: #0b2855; font-weight: 700; }
    th.temp { background: #147b82; } th.umid { background: #198f78; }
    tr.alt td { background: #f0fbf8; }
    td.ok { color: #087f5b; background: #d7f5e9 !important; font-weight: 700; }
    td.bad { color: #b42318; background: #fee4e2 !important; font-weight: 700; }
    td.warn { color: #8a5b00; background: #fff3cd !important; font-weight: 700; }
    td.status { font-size: 5.7px; }
    .legenda { margin-top: 4px; display: flex; justify-content: space-between; color: #52677d; font-size: 6px; }
  </style></head><body>
    <div class="cabecalho"><div class="topo">
      <div class="codigo"><strong>REL 06GQ10</strong><br>Vers&atilde;o: 00<br>Per&iacute;odo: ${html(periodoRelatorio)}</div>
      <div class="titulo">AVALIA&Ccedil;&Atilde;O DOS CERTIFICADOS DE CALIBRA&Ccedil;&Atilde;O - TESTO 174H</div>
      <div class="marca">Calibra<span>Flow</span></div>
    </div><div class="meta">
      <div><strong>Instrumento:</strong> TESTO</div><div><strong>Modelo:</strong> 174H</div>
      <div><strong>DMA temperatura:</strong> ${numero(limiteTemperatura)} &deg;C</div>
      <div><strong>DMA umidade:</strong> ${numero(limiteUmidade)} %UR</div><div><strong>Total:</strong> ${registros.length}</div>
    </div></div>
    <table><thead><tr>
      <th rowspan="2">N&deg; S&eacute;rie</th><th rowspan="2">TAG</th><th rowspan="2">Calibrado em</th><th rowspan="2">Validade</th><th rowspan="2">Certificado</th>
      <th class="temp" rowspan="2">Incerteza T</th><th class="temp" colspan="2">-20&deg;C</th><th class="temp" colspan="2">0&deg;C</th><th class="temp" colspan="2">15&deg;C</th><th class="temp" colspan="2">60&deg;C</th><th class="temp" rowspan="2">Resultado T</th>
      <th class="umid" colspan="3">10% UR</th><th class="umid" colspan="3">50% UR</th><th class="umid" colspan="3">90% UR</th><th class="umid" rowspan="2">Resultado UR</th><th rowspan="2">RESULTADO</th>
    </tr><tr>${Array.from({ length: 4 }, () => "<th>Erro</th><th>Soma</th>").join("")}${Array.from({ length: 3 }, () => "<th>Inc.</th><th>Erro</th><th>Soma</th>").join("")}</tr></thead><tbody>${linhas}</tbody></table>
    <div class="legenda"><span>Soma = |erro| + incerteza. Resultado comparado ao DMA configurado.</span><span>CalibraFlow - Gest&atilde;o de Certificados</span></div>
  </body></html>`;
}

function minutosDoHorario(valor) {
  const [hora, minuto] = String(valor).split(":").map(Number);
  return hora * 60 + minuto;
}

function horarioLocalMetricas() {
  const partes = new Intl.DateTimeFormat("en-CA", {
    timeZone: METRICS_TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23"
  }).formatToParts(new Date());
  const dados = Object.fromEntries(partes.map(p => [p.type, p.value]));

  return {
    data: `${dados.year}-${dados.month}-${dados.day}`,
    minutos: Number(dados.hour) * 60 + Number(dados.minute)
  };
}

function slotAtualMetricas() {
  const agora = horarioLocalMetricas();
  const inicio = minutosDoHorario(METRICS_START_TIME);
  const fim = minutosDoHorario(METRICS_END_TIME);
  if (agora.minutos < inicio || agora.minutos >= fim) return null;

  const indice = Math.floor((agora.minutos - inicio) / METRICS_INTERVAL_MINUTES);
  const slotMinutos = inicio + indice * METRICS_INTERVAL_MINUTES;
  return `${agora.data}-${slotMinutos}`;
}

async function gravarMetricasConsolidadas() {
  if (!METRICS_ENABLED || metricasFlushEmExecucao || metricas.requisicoes === 0) return;
  metricasFlushEmExecucao = true;
  const snapshot = metricas;
  metricas = novasMetricas();

  try {
    const response = await fetchNative(`${SUPABASE_URL}/rest/v1/metricas_consumo`, {
      method: "POST",
      headers: supabaseHeaders(),
      body: JSON.stringify({
        servico: "DLH",
        periodo_inicio: snapshot.periodo_inicio,
        periodo_fim: new Date().toISOString(),
        requisicoes: snapshot.requisicoes,
        respostas_bytes: snapshot.respostas_bytes,
        requisicoes_externas: snapshot.requisicoes_externas,
        supabase_requisicoes: snapshot.supabase_requisicoes,
        supabase_bytes: snapshot.supabase_bytes,
        google_requisicoes: snapshot.google_requisicoes,
        google_bytes: snapshot.google_bytes,
        erros: snapshot.erros,
        tempo_total_ms: snapshot.tempo_total_ms,
        rotas: snapshot.rotas
      })
    });

    if (!response.ok) throw new Error(await response.text());

    const dataLocal = horarioLocalMetricas().data;
    if (ultimaLimpezaMetricas !== dataLocal) {
      ultimaLimpezaMetricas = dataLocal;
      const limite = new Date(Date.now() - METRICS_RETENTION_DAYS * 86400000).toISOString();
      await fetchNative(
        `${SUPABASE_URL}/rest/v1/metricas_consumo?periodo_fim=lt.${encodeURIComponent(limite)}`,
        { method: "DELETE", headers: supabaseHeaders() }
      );
    }
  } catch (e) {
    metricas.requisicoes += snapshot.requisicoes;
    metricas.respostas_bytes += snapshot.respostas_bytes;
    metricas.requisicoes_externas += snapshot.requisicoes_externas;
    metricas.supabase_requisicoes += snapshot.supabase_requisicoes;
    metricas.supabase_bytes += snapshot.supabase_bytes;
    metricas.google_requisicoes += snapshot.google_requisicoes;
    metricas.google_bytes += snapshot.google_bytes;
    metricas.erros += snapshot.erros;
    metricas.tempo_total_ms += snapshot.tempo_total_ms;
    for (const [rota, valores] of Object.entries(snapshot.rotas)) {
      const atual = metricas.rotas[rota] || {
        requisicoes: 0, respostas_bytes: 0, erros: 0, tempo_total_ms: 0
      };
      for (const campo of Object.keys(atual)) atual[campo] += valores[campo] || 0;
      metricas.rotas[rota] = atual;
    }
    console.log("Falha ao consolidar métricas DLH:", e.message);
  } finally {
    metricasFlushEmExecucao = false;
  }
}

async function verificarAgendaMetricas() {
  const slot = slotAtualMetricas();
  if (!slot) return;
  if (!ultimoSlotMetricas) {
    ultimoSlotMetricas = slot;
    return;
  }
  if (slot === ultimoSlotMetricas) return;

  ultimoSlotMetricas = slot;
  await gravarMetricasConsolidadas();
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
    const pontosUmidadeComResultado = (tabela.pontos_umidade || []).map(p => {
      const aprovadoPonto = Number(p.soma) <= avaliacao.limite_umidade;
      return {
        ...p,
        limite: avaliacao.limite_umidade,
        resultado: aprovadoPonto ? "APROVADO" : "REPROVADO"
      };
    });
    const pontosTemperaturaComResultado = (tabela.pontos_temperatura || []).map(p => {
      const aprovadoPonto = Number(p.soma) <= avaliacao.limite_temperatura;
      return {
        ...p,
        limite: avaliacao.limite_temperatura,
        resultado: aprovadoPonto ? "APROVADO" : "REPROVADO"
      };
    });

    return {
      status: avaliacao.aprovado ? "APROVADO" : "REPROVADO",
      pontos_umidade: pontosUmidadeComResultado,
      pontos_temperatura: pontosTemperaturaComResultado,
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

async function atualizarControleSyncDLH(payload) {
  const response = await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?on_conflict=id`, {
    method: "POST",
    headers: {
      ...supabaseHeaders(),
      Prefer: "resolution=merge-duplicates,return=minimal"
    },
    body: JSON.stringify({ id: 2, ...payload })
  });

  if (!response.ok) {
    throw new Error(`Falha ao atualizar controle_sync DLH: ${await response.text()}`);
  }

  invalidarCachesDLH();
}

async function buscarControleSyncDLH() {
  const response = await fetch(
    `${SUPABASE_URL}/rest/v1/controle_sync?id=eq.2&select=id,total_processados,ultima_execucao,em_execucao`,
    { headers: supabaseHeaders() }
  );
  const data = await response.json();
  const registros = validarListaSupabase(response, data, "Supabase controle_sync DLH");
  return registros[0] || null;
}

function execucaoTravadaDLH(controle) {
  if (!controle?.em_execucao || !controle?.ultima_execucao) return false;
  return Date.now() - new Date(controle.ultima_execucao).getTime() > 5 * 60 * 1000;
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

      await atualizarControleSyncDLH({
        em_execucao: true,
        ultima_execucao: new Date().toISOString(),
        total_processados: idsBanco.size
      });
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
    const totalInicial = await contarCertificadosBancoDLH();
    await atualizarControleSyncDLH({
      em_execucao: true,
      ultima_execucao: new Date().toISOString(),
      total_processados: totalInicial
    });

    const resultado = await executarSyncDLH();
    invalidarCachesDLH();
    deveContinuar = resultado.processados > 0;

    const totalAtual = await contarCertificadosBancoDLH();
    await atualizarControleSyncDLH({
      em_execucao: false,
      ultima_execucao: new Date().toISOString(),
      total_processados: totalAtual
    });
  } catch (e) {
    console.log("Erro na sincronização automática DLH:", e.message);
    try {
      await atualizarControleSyncDLH({
        em_execucao: false,
        ultima_execucao: new Date().toISOString()
      });
    } catch (controleErro) {
      console.log("Erro ao finalizar controle_sync DLH:", controleErro.message);
    }
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


// Diagnóstico público mínimo: não consulta banco/Drive e nunca expõe segredos.
app.get("/dlh/status-publico", (_req, res) => {
  res.setHeader("Cache-Control", "no-store");
  res.json({
    ok: true,
    modulo: "DLH",
    telegram_token_configurado: Boolean(TELEGRAM_BOT_TOKEN),
    telegram_chat_configurado: Boolean(TELEGRAM_CHAT_ID),
    telegram_configurado: Boolean(TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID),
    verificado_em: new Date().toISOString()
  });
});

// Keep-alive econômico: consulta somente uma linha para evitar pausa por inatividade.
// Reutiliza o segredo da automação e nunca retorna dados de certificados.
app.get("/dlh/keepalive", async (req, res) => {
  if (!validarSegredoAutomacao(req, res)) return;

  const inicio = Date.now();
  try {
    if (!SUPABASE_URL || !SUPABASE_KEY) {
      return res.status(503).json({ ok: false, erro: "Supabase não configurado" });
    }

    const resposta = await fetch(
      `${SUPABASE_URL}/rest/v1/controle_sync?select=id&limit=1`,
      {
        method: "GET",
        headers: supabaseHeaders(),
        signal: AbortSignal.timeout(5000)
      }
    );

    if (!resposta.ok) {
      return res.status(502).json({ ok: false, erro: "Banco indisponível" });
    }

    res.setHeader("Cache-Control", "no-store");
    return res.json({ ok: true, modulo: "DLH", banco: "ativo", tempo_ms: Date.now() - inicio });
  } catch {
    return res.status(503).json({ ok: false, erro: "Banco indisponível" });
  }
});

app.get("/dlh/versao", (_req, res) => {
  res.setHeader("Cache-Control", "no-store");
  res.json({
    ok: true,
    modulo: "DLH",
    versao: BACKEND_VERSION,
    commit_render: process.env.RENDER_GIT_COMMIT || null,
    servico_render: process.env.RENDER_SERVICE_NAME || null,
    verificado_em: new Date().toISOString(),
    rotas_assistente: [
      "/dlh/assistente/resumo",
      "/dlh/assistente/riscos",
      "/dlh/assistente/divergencias",
      "/dlh/assistente/resumo-lote",
      "/dlh/assistente/conferencia-relatorio",
      "/dlh/assistente/relatorio-executivo",
      "/dlh/assistente/chamado-calibracao",
      "/dlh/assistente/perguntar",
      "/dlh/loggers/disponibilidade",
      "/dlh/loggers/checklist-pre-teste",
      "/dlh/suporte/tickets"
    ]
  });
});

app.post("/dlh/suporte/tickets", async (req, res) => {
  await abrirTicketSuporte(req, res, "DLH");
});

app.get("/dlh/suporte/tickets", async (req, res) => {
  try {
    const tickets = await listarTicketsSuporteDoUsuarioDLH(req.auth.user.id, "DLH");
    res.setHeader("Cache-Control", "private, max-age=15");
    res.json({ ok: true, tickets });
  } catch (e) {
    res.status(500).json({ erro: "Nao foi possivel carregar os tickets" });
  }
});

app.get("/dlh/suporte/tickets/:ticketId", async (req, res) => {
  try {
    const resultado = await listarMensagensTicketSuporteDLH(req.params.ticketId, req.auth.user.id);
    if (!resultado) return res.status(404).json({ erro: "Ticket nao encontrado" });
    res.setHeader("Cache-Control", "private, max-age=10");
    res.json({ ok: true, ...resultado });
  } catch (e) {
    res.status(500).json({ erro: "Nao foi possivel carregar o ticket" });
  }
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

app.get("/dlh/automacao/status", async (req, res) => {
  if (!validarSegredoAutomacao(req, res)) return;

  try {
    let controle = await buscarControleSyncDLH();
    if (execucaoTravadaDLH(controle)) {
      await atualizarControleSyncDLH({
        em_execucao: false,
        ultima_execucao: controle?.ultima_execucao || new Date().toISOString()
      });
      controle = { ...controle, em_execucao: false };
    }

    const [totalBanco, totalExcluidos, arquivosDrive] = await Promise.all([
      contarCertificadosBancoDLH(),
      contarTabela("certificados_dlh_excluidos"),
      buscarArquivosDriveDLH()
    ]);
    const totalDrive = arquivosDrive.length;

    res.json({
      modulo: "DLH",
      em_execucao: controle?.em_execucao || syncLocalDLHEmExecucao,
      ultima_execucao: controle?.ultima_execucao || null,
      total_drive: totalDrive,
      total_banco: totalBanco,
      faltantes: Math.max(0, totalDrive - totalBanco - totalExcluidos)
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.post("/dlh/automacao/sincronizar", async (req, res) => {
  if (!validarSegredoAutomacao(req, res)) return;

  try {
    const controle = await buscarControleSyncDLH();
    if (
      syncLocalDLHEmExecucao ||
      (controle?.em_execucao && !execucaoTravadaDLH(controle))
    ) {
      return res.json({ iniciado: false, modulo: "DLH", mensagem: "DLH já está processando" });
    }

    res.status(202).json({ iniciado: true, modulo: "DLH", mensagem: "Sincronização DLH iniciada" });
    executarSyncAutomaticoDLH();
  } catch (e) {
    if (!res.headersSent) res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/metricas/atual", (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  res.json({
    servico: "DLH",
    consolidado: false,
    ...metricas
  });
});

app.get("/dlh/metricas", async (req, res) => {
  try {
    const dias = Math.min(
      METRICS_RETENTION_DAYS,
      Math.max(1, Number(req.query.dias || 30))
    );
    const desde = new Date(Date.now() - dias * 86400000).toISOString();
    const response = await fetch(
      `${SUPABASE_URL}/rest/v1/metricas_consumo?servico=eq.DLH&periodo_fim=gte.${encodeURIComponent(desde)}&select=id,servico,periodo_inicio,periodo_fim,requisicoes,respostas_bytes,requisicoes_externas,supabase_requisicoes,supabase_bytes,google_requisicoes,google_bytes,erros,tempo_total_ms,rotas&order=periodo_fim.desc&limit=500`,
      { headers: supabaseHeaders() }
    );
    const data = await response.json();
    const registros = validarListaSupabase(response, data, "Supabase métricas DLH");
    res.setHeader("Cache-Control", "private, max-age=300");
    res.json({ estimativa: true, dias, total: registros.length, registros });
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

    let controle = await buscarControleSyncDLH();
    if (execucaoTravadaDLH(controle)) {
      await atualizarControleSyncDLH({
        em_execucao: false,
        ultima_execucao: controle?.ultima_execucao || new Date().toISOString()
      });
      controle = { ...controle, em_execucao: false };
    }

    const totalBanco = await contarCertificadosBancoDLH();
    const totalExcluidos = await contarTabela("certificados_dlh_excluidos");
    const arquivosDrive = await buscarArquivosDriveDLH();

    const totalDrive = arquivosDrive.length;
    const faltantes = Math.max(0, totalDrive - totalBanco - totalExcluidos);

    const payload = {
      id: 2,
      total_processados: controle?.total_processados ?? totalBanco,
      em_execucao: controle?.em_execucao || false,
      ultima_execucao: controle?.ultima_execucao || null,
      total_drive: totalDrive,
      total_banco: totalBanco,
      faltantes,
      telegram_configurado: Boolean(TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID)
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

app.get("/dlh/status/pdf", (req, res) => {
  return res.json({ ok: true, servico: "pdf", motor: "pdfkit" });
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
    // `equipamentos`/`lista` representam a busca explícita em lote.
    // `dlh` é a busca normal da tela e também aceita vários loggers.
    const listaEquipamentos = normalizarListaQuery(req.query.equipamentos || req.query.lista);
    const buscaDlh = normalizarListaQuery(req.query.dlh);
    const testeInicio = normalizarDataQuery(req.query.teste_inicio || req.query.data_inicio || req.query.inicio);
    const testeFim = normalizarDataQuery(req.query.teste_fim || req.query.data_fim || req.query.fim);
    const limit = limitarNumero(req.query.limit, 100, 1, 1000);
    const offset = limitarNumero(req.query.offset, 0, 0, 1000000);

    if (listaEquipamentos.length) {
      const data = await buscarCertificadosPorPeriodoEmLotes({
        tabela: "certificados_dlh",
        campoEquipamento: "dlh",
        equipamentos: listaEquipamentos,
        testeInicio,
        testeFim
      });
      return res.json({
        total: data.length,
        teste_inicio: testeInicio || null,
        teste_fim: testeFim || null,
        registros: data
      });
    }

    const filtros = [];
    if (buscaDlh.length) {
      const filtroBusca = montarFiltroBuscaCertificado(buscaDlh, normalizarDLH, "dlh");
      if (filtroBusca) filtros.push(["or", filtroBusca]);
    }
    if (testeInicio && testeFim) {
      filtros.push(["data", `lte.${testeFim}`], ["vencimento", `gte.${testeInicio}`]);
    } else if (testeInicio || testeFim) {
      return res.status(400).json({ erro: "Informe data inicial e data final do teste, ou deixe as duas em branco" });
    }

    const params = new URLSearchParams();
    params.set("select", CERTIFICADOS_DLH_LISTA_SELECT);
    for (const [chave, valor] of filtros) params.append(chave, valor);
    params.append("order", "data.desc");
    params.append("order", "id.asc");
    params.set("limit", String(limit));
    params.set("offset", String(offset));
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_dlh?${params.toString()}`,
      { headers: { ...supabaseHeaders(), Prefer: "count=exact" } }
    );
    const data = validarListaSupabase(r, await r.json(), "Supabase certificados_dlh");
    res.setHeader("Cache-Control", "private, max-age=30");
    res.json({ total: Number(r.headers.get("content-range")?.split("/")[1] || data.length), limit, offset, registros: data });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.post("/dlh/certificados/busca-lista", async (req, res) => {
  try {
    const equipamentos = Array.isArray(req.body?.equipamentos)
      ? req.body.equipamentos.map(String).filter(Boolean)
      : normalizarListaQuery(req.body?.equipamentos || req.body?.lista);
    const testeInicio = normalizarDataQuery(
      req.body?.teste_inicio || req.body?.data_inicio || req.body?.inicio
    );
    const testeFim = normalizarDataQuery(
      req.body?.teste_fim || req.body?.data_fim || req.body?.fim
    );

    const unicos = [...new Set(equipamentos.map(normalizarDLH).filter(Boolean))];
    if (!unicos.length) {
      return res.status(400).json({ erro: "Informe ao menos um equipamento DLH" });
    }
    if (unicos.length > 500) {
      return res.status(400).json({
        erro: "O limite é de 500 equipamentos por busca",
        total_informado: unicos.length
      });
    }
    const registros = await buscarCertificadosPorPeriodoEmLotes({
      tabela: "certificados_dlh",
      campoEquipamento: "dlh",
      equipamentos: unicos,
      testeInicio,
      testeFim
    });

    res.setHeader("Cache-Control", "private, max-age=300");
    res.json({
      teste_inicio: testeInicio || null,
      teste_fim: testeFim || null,
      ...montarResultadoBuscaLista(registros, unicos, "dlh", normalizarDLH)
    });
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

app.get("/dlh/assistente/resumo", async (_req, res) => {
  try {
    const resumo = await montarResumoModuloDLH();
    const riscos = montarRiscosAssistente(resumo);
    res.setHeader("Cache-Control", "private, max-age=300");
    res.json({
      ...resumo,
      riscos,
      executivo: montarMensagemExecutiva(resumo, riscos)
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/assistente/riscos", async (_req, res) => {
  try {
    const resumo = await montarResumoModuloDLH();
    res.setHeader("Cache-Control", "private, max-age=300");
    res.json({
      modulo: "DLH",
      gerado_em: resumo.gerado_em,
      riscos: montarRiscosAssistente(resumo)
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/assistente/divergencias", async (req, res) => {
  try {
    const limit = limitarNumero(req.query.limit, 50, 1, 200);
    const offset = limitarNumero(req.query.offset, 0, 0, 100000);
    const { total, registros } = await buscarRegistrosAssistente(
      "certificados_dlh",
      CERTIFICADOS_DLH_LISTA_SELECT,
      [["or", "(divergente.eq.true,duplicado.eq.true,motivo_divergencia.not.is.null,status.eq.REPROVADO,status.eq.ERRO)"]],
      { order: "criado_em.desc", limit, offset, count: true }
    );

    res.setHeader("Cache-Control", "private, max-age=120");
    res.json({
      modulo: "DLH",
      total,
      limit,
      offset,
      registros: registros.map(item => ({
        ...item,
        analise: explicarDivergenciaCertificadoDLH(item)
      }))
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/assistente/resumo-lote", async (req, res) => {
  try {
    const equipamentos = normalizarListaQuery(req.query.equipamentos || req.query.dlh || req.query.lista);
    const testeInicio = normalizarDataQuery(req.query.teste_inicio || req.query.data_inicio || req.query.inicio);
    const testeFim = normalizarDataQuery(req.query.teste_fim || req.query.data_fim || req.query.fim);
    const unicos = [...new Set(equipamentos.map(normalizarDLH).filter(Boolean))];

    if (!unicos.length) {
      return res.status(400).json({ erro: "Informe ao menos um equipamento DLH" });
    }
    if (unicos.length > 500) {
      return res.status(400).json({ erro: "O limite e de 500 equipamentos por resumo", total_informado: unicos.length });
    }

    const registros = await buscarCertificadosPorPeriodoEmLotes({
      tabela: "certificados_dlh",
      campoEquipamento: "dlh",
      equipamentos: unicos,
      testeInicio,
      testeFim
    });
    const resumo = resumirCertificadosAssistente(registros);
    const riscos = montarRiscosAssistente({ ...resumo, modulo: "DLH" });

    res.setHeader("Cache-Control", "private, max-age=300");
    res.json({
      modulo: "DLH",
      teste_inicio: testeInicio || null,
      teste_fim: testeFim || null,
      equipamentos: unicos.length,
      ...montarResultadoBuscaLista(registros, unicos, "dlh", normalizarDLH),
      resumo,
      riscos,
      executivo: montarMensagemExecutiva({ ...resumo, modulo: "DLH" }, riscos)
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/assistente/conferencia-relatorio", async (req, res) => {
  try {
    const dataInicio = normalizarDataQuery(req.query.data_inicio || req.query.inicio);
    const dataFim = normalizarDataQuery(req.query.data_fim || req.query.fim);
    if ((dataInicio && !dataFim) || (!dataInicio && dataFim)) {
      return res.status(400).json({ erro: "Informe data inicial e final, ou deixe ambas em branco" });
    }
    if (dataInicio && dataFim && dataInicio > dataFim) {
      return res.status(400).json({ erro: "A data inicial nao pode ser posterior a data final" });
    }

    const filtros = [];
    if (dataInicio) filtros.push(["data", `gte.${dataInicio}`], ["data", `lte.${dataFim}`]);
    const { total, registros } = await buscarRegistrosAssistente(
      "certificados_dlh",
      CERTIFICADOS_DLH_LISTA_SELECT,
      filtros,
      { order: "data.desc", limit: limitarNumero(req.query.limit, 1000, 1, 1000), count: true }
    );
    const resumo = resumirCertificadosAssistente(registros);
    const bloqueios = registros
      .map(item => ({ item, analise: explicarDivergenciaCertificadoDLH(item) }))
      .filter(item => ["alta", "media"].includes(item.analise.severidade));

    res.setHeader("Cache-Control", "private, max-age=120");
    res.json({
      modulo: "DLH",
      total_periodo: total,
      avaliados_na_amostra: registros.length,
      data_inicio: dataInicio || null,
      data_fim: dataFim || null,
      pronto_para_emitir: bloqueios.length === 0,
      resumo,
      bloqueios: bloqueios.slice(0, 200)
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/assistente/relatorio-executivo", async (_req, res) => {
  try {
    const resumo = await montarResumoModuloDLH();
    const riscos = montarRiscosAssistente(resumo);
    res.setHeader("Cache-Control", "private, max-age=300");
    res.json({
      modulo: "DLH",
      gerado_em: resumo.gerado_em,
      resumo,
      riscos,
      executivo: montarMensagemExecutiva(resumo, riscos)
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.post("/dlh/assistente/chamado-calibracao", async (req, res) => {
  try {
    const id = req.body?.id;
    if (!id) return res.status(400).json({ erro: "Informe o id do certificado" });
    const certificado = await buscarCertificadoAssistenteDLH(id);
    if (!certificado) return res.status(404).json({ erro: "Certificado DLH nao encontrado" });

    const analise = explicarDivergenciaCertificadoDLH(certificado);
    const assunto = `Divergencia certificado DLH ${certificado.dlh || ""} - ${certificado.certificado || ""}`.trim();
    const corpo =
      `Prezados,\n\n` +
      `Solicitamos apoio na avaliacao do certificado abaixo:\n\n` +
      `Modulo: DLH\n` +
      `Logger: ${certificado.dlh || "-"}\n` +
      `Serie: ${certificado.serie || "-"}\n` +
      `Serie esperada: ${certificado.serie_esperada || "-"}\n` +
      `Certificado: ${certificado.certificado || "-"}\n` +
      `Status: ${certificado.status || "-"}\n` +
      `Motivo: ${certificado.motivo_divergencia || analise.resumo}\n\n` +
      `Analise automatica: ${analise.detalhe}\n` +
      `Acao recomendada: ${analise.acao_recomendada}\n\n` +
      `Atenciosamente,\nCalibraFlow`;

    res.json({
      modulo: "DLH",
      modo: "rascunho",
      certificado,
      analise,
      chamado: {
        para: req.body?.destinatario || process.env.CALIBRACAO_EMAIL || "",
        assunto,
        corpo
      }
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.post("/dlh/assistente/perguntar", async (req, res) => {
  try {
    const pergunta = String(req.body?.pergunta || req.body?.mensagem || "").trim();
    const lista = normalizarListaQuery(pergunta).map(normalizarDLH).filter(Boolean);
    const resumo = await montarResumoModuloDLH();
    const riscos = montarRiscosAssistente(resumo);

    if (lista.length) {
      return res.json({
        tipo: "busca_equipamentos",
        resposta: `Encontrei ${lista.length} codigo(s) de logger na pergunta. Use a busca por lista para trazer todos os certificados relacionados.`,
        filtros_sugeridos: { dlh: lista },
        endpoint_sugerido: `/dlh/certificados?equipamentos=${lista.join(",")}`
      });
    }

    res.json({
      tipo: "resumo_operacional",
      resposta: montarMensagemExecutiva(resumo, riscos).leitura_gerencial,
      resumo,
      riscos
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/loggers/disponibilidade", async (req, res) => {
  try {
    const lista = normalizarListaQuery(req.query.lista || req.query.equipamentos || req.query.dlh);
    const limit = limitarNumero(req.query.limit, 100, 1, 500);
    const offset = limitarNumero(req.query.offset, 0, 0, 100000);
    const status = await buscarStatusAtualDLHOperacional(lista);
    const combinado = combinarDisponibilidadeDLH(status.registros, {
      busca: req.query.busca,
      local: req.query.local,
      lista
    });
    const registros = combinado.registros.slice(offset, offset + limit);

    res.setHeader("Cache-Control", "private, max-age=120");
    res.json({
      modulo: "DLH",
      aviso: status.aviso,
      resumo: combinado.resumo,
      total: combinado.registros.length,
      limit,
      offset,
      registros
    });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.post("/dlh/loggers/checklist-pre-teste", async (req, res) => {
  try {
    const equipamentos = Array.isArray(req.body?.equipamentos)
      ? req.body.equipamentos.map(String).filter(Boolean)
      : normalizarListaQuery(req.body?.equipamentos || req.body?.lista || req.body?.dlh);
    const testeInicio = normalizarDataQuery(req.body?.teste_inicio || req.body?.data_inicio || req.body?.inicio);
    const testeFim = normalizarDataQuery(req.body?.teste_fim || req.body?.data_fim || req.body?.fim);
    const unicos = [...new Set(equipamentos.map(normalizarDLH).filter(Boolean))];

    if (!unicos.length) {
      return res.status(400).json({ erro: "Informe ao menos um equipamento DLH" });
    }
    if (unicos.length > 500) {
      return res.status(400).json({ erro: "O limite e de 500 equipamentos por checklist", total_informado: unicos.length });
    }
    if (!testeInicio || !testeFim) {
      return res.status(400).json({ erro: "Informe data inicial e final do teste" });
    }

    const [status, certificados] = await Promise.all([
      buscarStatusAtualDLHOperacional(unicos),
      buscarCertificadosPorPeriodoEmLotes({
        tabela: "certificados_dlh",
        campoEquipamento: "dlh",
        equipamentos: unicos,
        testeInicio,
        testeFim
      })
    ]);
    const disponibilidade = combinarDisponibilidadeDLH(status.registros, { lista: unicos }).registros;
    const disponibilidadePorLogger = new Map(disponibilidade.map(item => [item.logger_codigo, item]));
    const certificadosPorLogger = new Map();
    for (const certificado of certificados) {
      const chave = normalizarDLH(certificado.dlh);
      if (!chave) continue;
      const listaCertificados = certificadosPorLogger.get(chave) || [];
      listaCertificados.push(certificado);
      certificadosPorLogger.set(chave, listaCertificados);
    }

    const itens = unicos.map(equipamento => avaliarChecklistPreTesteDLH({
      equipamento,
      disponibilidade: disponibilidadePorLogger.get(equipamento),
      certificados: certificadosPorLogger.get(equipamento) || [],
      testeInicio,
      testeFim
    }));
    const resumo = {
      total_loggers: itens.length,
      prontos: itens.filter(item => item.pronto).length,
      bloqueados: itens.filter(item => !item.pronto).length,
      fora_area_tecnica: itens.filter(item => !localEhAreaTecnica(item.local_atual)).length,
      sem_certificado_periodo: itens.filter(item => item.certificados_encontrados === 0).length,
      com_divergencia: itens.filter(item => item.problemas.some(p => p.includes("divergencia") || p.includes("reprovacao") || p.includes("erro"))).length
    };

    res.setHeader("Cache-Control", "private, max-age=300");
    res.json({
      modulo: "DLH",
      aviso: status.aviso,
      teste_inicio: testeInicio,
      teste_fim: testeFim,
      pronto_para_teste: resumo.bloqueados === 0,
      resumo,
      itens
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
    const agoraJob = new Date();
    const job = {
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
      solicitado_por: req.auth?.user?.id || null,
      solicitado_email: req.auth?.user?.email || req.auth?.perfil?.email || null,
      parametros: {
        modo: ids.length ? "ids" : "periodo",
        total_ids: ids.length,
        total_equipamentos: listaEquipamentos.length,
        teste_inicio: testeInicio || null,
        teste_fim: testeFim || null
      },
      criado_em: agoraJob.toISOString(),
      atualizado_em: agoraJob.toISOString(),
      expira_em: new Date(agoraJob.getTime() + 24 * 60 * 60 * 1000).toISOString()
    };
    downloadJobsDLH.set(jobId, job);
    await salvarDownloadJobDLH(job);

    setTimeout(() => {
      processarDownloadMassaDLH(jobId, Array.isArray(registros) ? registros : []).catch(e => {
        const job = downloadJobsDLH.get(jobId);
        if (job) {
          job.status = "erro";
          job.erro = e.message;
          job.atualizado_em = new Date().toISOString();
          salvarDownloadJobDLH(job).catch(() => {});
        }
      });
    }, 0);

    res.status(202).json({ job_id: jobId, total: registros.length });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/downloads/massa/historico", async (req, res) => {
  try {
    const limit = Math.min(100, Math.max(1, Number(req.query.limit || 50)));
    const role = req.auth?.perfil?.role;
    const solicitadoPor = role === "dev" || role === "administrador"
      ? null
      : req.auth?.user?.id;
    const jobs = await listarDownloadJobsPersistidosDLH("DLH", limit, solicitadoPor);
    res.json({ jobs });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/dlh/downloads/massa/:jobId", async (req, res) => {
  const job = downloadJobsDLH.get(req.params.jobId) || await buscarDownloadJobPersistidoDLH(req.params.jobId, "DLH");
  if (!job) return res.status(404).json({ erro: "Tarefa não encontrada" });
  if (!podeAcessarDownloadJobDLH(req, job)) return res.status(403).json({ erro: "Acesso negado a esta tarefa" });
  res.json(job);
});

app.post("/dlh/downloads/massa/:jobId/link", async (req, res) => {
  const job = downloadJobsDLH.get(req.params.jobId) || await buscarDownloadJobPersistidoDLH(req.params.jobId, "DLH");
  if (!job) return res.status(404).json({ erro: "Tarefa não encontrada" });
  if (!podeAcessarDownloadJobDLH(req, job)) return res.status(403).json({ erro: "Acesso negado a esta tarefa" });
  const temArquivoLocal = job.arquivo_zip_local_path && fs.existsSync(job.arquivo_zip_local_path);
  if (job.status !== "concluido" || (!temArquivoLocal && !job.arquivo_zip_drive_id)) {
    return res.status(409).json({ erro: "O arquivo ZIP ainda não está disponível" });
  }
  const { ticket, expiraEm } = criarDownloadTicketDLH(job.id);
  res.setHeader("Cache-Control", "no-store");
  res.json({
    download_url: `/dlh/downloads/massa/${encodeURIComponent(job.id)}/arquivo?ticket=${ticket}`,
    expira_em: new Date(expiraEm).toISOString()
  });
});

app.get("/dlh/downloads/massa/:jobId/arquivo", async (req, res) => {
  try {
    const job = downloadJobsDLH.get(req.params.jobId) || await buscarDownloadJobPersistidoDLH(req.params.jobId, "DLH");
    if (!job) return res.status(404).json({ erro: "Tarefa não encontrada" });
    const ticketValido = validarDownloadTicketDLH(job.id, req.query?.ticket);
    if (!ticketValido && !podeAcessarDownloadJobDLH(req, job)) {
      return res.status(403).json({ erro: "Link de download inválido ou expirado" });
    }
    const temArquivoLocal = job.arquivo_zip_local_path && fs.existsSync(job.arquivo_zip_local_path);
    if (job.status !== "concluido" || (!temArquivoLocal && !job.arquivo_zip_drive_id)) {
      return res.status(409).json({ erro: "O arquivo ZIP ainda não está disponível" });
    }
    if (temArquivoLocal) {
      res.setHeader("Content-Type", "application/zip");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="${job.arquivo_zip_nome || "certificados-dlh.zip"}"`
      );
      return fs.createReadStream(job.arquivo_zip_local_path).pipe(res);
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
    const { dataInicio, dataFim, periodoFormatado } = obterIntervaloRelatorio(req.query);

    const r = await fetch(
      montarUrlRelatorio("certificados_dlh", dataInicio, dataFim, "dlh.asc,data.asc,serie.asc"),
      { headers: supabaseHeaders() }
    );

    const todos = await r.json();
    const dados = Array.isArray(todos) ? todos : [];

    const criterios = await buscarCriteriosCalibracao();

    res.json({
      data_relatorio: dataInicio === dataFim ? dataInicio : null,
      data_inicio: dataInicio,
      data_fim: dataFim,
      periodo: periodoFormatado,
      total: dados.length,
      criterios_aceitacao: criterios,
      registros: dados
    });
  } catch (e) {
    res.status(e.statusCode || 500).json({ erro: e.message });
  }
});

app.get("/dlh/relatorio-dia/pdf", async (req, res) => {
  try {
    const { dataInicio, dataFim, periodoFormatado, sufixoArquivo } = obterIntervaloRelatorio(req.query);
    const r = await fetch(
      montarUrlRelatorio("certificados_dlh", dataInicio, dataFim, "dlh.asc,data.asc,serie.asc"),
      { headers: supabaseHeaders() }
    );
    const todos = await r.json();
    if (!r.ok) {
      const erro = new Error(todos?.message || todos?.error || "Falha ao consultar certificados DLH.");
      erro.statusCode = 502;
      throw erro;
    }
    const dados = Array.isArray(todos) ? todos : [];
    if (dados.length === 0) {
      return res.status(404).json({ erro: "Nenhum certificado DLH encontrado no periodo informado." });
    }

    const criterios = await buscarCriteriosCalibracao();
    const pdf = await gerarPdfDLH(
      dados,
      periodoFormatado,
      Number(criterios.limite_temperatura ?? 0.5),
      Number(criterios.limite_umidade ?? 5)
    );
    const nomeArquivo = `RELATORIO_DLH_${sufixoArquivo}.pdf`;

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${nomeArquivo}"`);
    return res.send(pdf);
  } catch (e) {
    return res.status(e.statusCode || 500).json({ erro: e.message });
  }
});

app.get("/dlh/relatorio-dia/excel", async (req, res) => {
  try {
    const { dataInicio, dataFim, periodoFormatado, sufixoArquivo } = obterIntervaloRelatorio(req.query);

    if (!fs.existsSync(MODELO_RELATORIO_PATH)) {
      throw new Error(
        "Arquivo modelo-relatorio-dlh.xlsx não encontrado na raiz do projeto."
      );
    }

    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(MODELO_RELATORIO_PATH);

    workbook.creator = "ITA FRIA";
    workbook.lastModifiedBy = "CalibraFlow";
    workbook.created = new Date();
    workbook.modified = new Date();

    const sheet = workbook.worksheets[0];

    if (!sheet) {
      throw new Error("A planilha modelo não possui nenhuma aba.");
    }

    sheet.name = "Relatorio DLH";

    const r = await fetch(
      montarUrlRelatorio("certificados_dlh", dataInicio, dataFim, "dlh.asc,data.asc,serie.asc"),
      { headers: supabaseHeaders() }
    );

    const todos = await r.json();

    if (!r.ok) {
      const erro = new Error(todos?.message || todos?.error || "Falha ao consultar certificados DLH.");
      erro.statusCode = 502;
      throw erro;
    }

    const dados = Array.isArray(todos) ? todos : [];

    if (dados.length === 0) {
      return res.status(404).json({
        erro: "Nenhum certificado DLH encontrado no período informado."
      });
    }

    const criteriosRelatorio = await buscarCriteriosCalibracao();
    const limiteTemperaturaRelatorio = Number(criteriosRelatorio.limite_temperatura ?? 0.5);
    const limiteUmidadeRelatorio = Number(criteriosRelatorio.limite_umidade ?? 5.0);

    sheet.getCell("A1").value = "REL 06GQ10";
    sheet.getCell("A2").value = `Versao: 00   Periodo: ${periodoFormatado}`;
    sheet.getCell("D1").value = "AVALIACAO DOS CERTIFICADOS DE CALIBRACAO - TESTO 174H";
    sheet.getCell("I5").value = `DMA temperatura: ${String(limiteTemperaturaRelatorio).replace(".", ",")} °C`;
    sheet.getCell("S5").value = `DMA umidade: ${String(limiteUmidadeRelatorio).replace(".", ",")} %UR`;

    // =========================
    // CONFIGURAÇÃO DE IMPRESSÃO
    // =========================
    sheet.pageSetup = {
      paperSize: 9,
      orientation: "landscape",
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
            cell.value = `Periodo do relatorio: ${periodoFormatado}`;
          }

          if (t.startsWith("DATA:") || t === "DATA") {
            cell.value = `Periodo: ${periodoFormatado}`;
          }
        });
      });

      const a1 = valorTexto(sheet.getCell("A1")).toUpperCase();

      if (!a1 || a1.includes("REL")) {
        sheet.getCell("A1").value = "REL 06GQ10";
        sheet.getCell("A2").value = `Versao: 00   Periodo: ${periodoFormatado}`;
        sheet.getCell("A1").alignment = {
          horizontal: "left",
          vertical: "middle",
          wrapText: false
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

    const colCount = 26;
    const estilosBase = Array.from({ length: colCount }, (_, index) =>
      clonar(sheet.getRow(dataStartRow).getCell(index + 1).style)
    );

    // Limpa somente a área de dados, preservando o cabeçalho e o rodapé/assinaturas do modelo.
    for (let rowNumber = dataStartRow; rowNumber < footerStartRow; rowNumber++) {
      const row = sheet.getRow(rowNumber);
      row.height = 18;

      for (let col = 1; col <= colCount; col++) {
        const cell = row.getCell(col);
        cell.value = "";
        aplicarPadraoCelula(cell, estilosBase[col - 1]);
      }
    }

    function buscarPonto(pontos, alvo, fallbackIndex) {
      const candidatos = (Array.isArray(pontos) ? pontos : [])
        .map((ponto, index) => ({
          ponto,
          index,
          referencia: Number(ponto?.padrao ?? ponto?.referencia)
        }))
        .filter(item => Number.isFinite(item.referencia))
        .sort((a, b) => Math.abs(a.referencia - alvo) - Math.abs(b.referencia - alvo));

      if (candidatos[0] && Math.abs(candidatos[0].referencia - alvo) <= 3) {
        return candidatos[0].ponto;
      }

      return (Array.isArray(pontos) ? pontos : [])[fallbackIndex] || {};
    }

    function resultadoPonto(ponto) {
      const soma = numeroOuVazio(ponto?.soma);
      if (soma !== "") return soma;

      const erro = numeroOuVazio(ponto?.erro);
      const incerteza = numeroOuVazio(ponto?.incerteza);
      if (erro === "" || incerteza === "") return "";

      return Number((Math.abs(erro) + Math.abs(incerteza)).toFixed(2));
    }

    function resultadoGrupo(pontos, limite, quantidadeEsperada) {
      const resultados = pontos.map(resultadoPonto).filter(valor => valor !== "");
      if (resultados.length < quantidadeEsperada) return "INDETERMINADO";
      return resultados.every(valor => Number(valor) <= Number(limite))
        ? "APROVADO"
        : "REPROVADO";
    }

    function dataExcel(valor) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(String(valor || ""))) return "";
      const [ano, mes, dia] = String(valor).split("-").map(Number);
      return new Date(ano, mes - 1, dia, 12, 0, 0);
    }

    dados.forEach((c, index) => {
      const rowNumber = dataStartRow + index;
      const row = sheet.getRow(rowNumber);

      const u = Array.isArray(c.pontos_umidade) ? c.pontos_umidade : [];
      const t = Array.isArray(c.pontos_temperatura) ? c.pontos_temperatura : [];

      const u10 = buscarPonto(u, 10, 0);
      const u50 = buscarPonto(u, 50, 1);
      const u90 = buscarPonto(u, 90, 2);
      const tMenos20 = buscarPonto(t, -20, 0);
      const tZero = buscarPonto(t, 0, 1);
      const tQuinze = buscarPonto(t, 15, 2);
      const tSessenta = buscarPonto(t, 60, 3);
      const incertezaTemperatura = t
        .map(ponto => numeroOuVazio(ponto?.incerteza))
        .find(valor => valor !== "") ?? "";
      const resultadoTemperatura = resultadoGrupo(t, limiteTemperaturaRelatorio, 4);
      const resultadoUmidade = resultadoGrupo(u, limiteUmidadeRelatorio, 3);
      const resultadoGeral = resultadoTemperatura === "REPROVADO" || resultadoUmidade === "REPROVADO"
        ? "REPROVADO"
        : resultadoTemperatura === "APROVADO" && resultadoUmidade === "APROVADO"
          ? "APROVADO"
          : String(c.status || "INDETERMINADO").toUpperCase();

      row.values = [
        c.serie || "",
        normalizarDLH(c.dlh) || c.dlh || "",
        dataExcel(c.data),
        c.mes_ano_validade || "",
        c.certificado || "",
        incertezaTemperatura,
        numeroOuVazio(tMenos20.erro),
        resultadoPonto(tMenos20),
        numeroOuVazio(tZero.erro),
        resultadoPonto(tZero),
        numeroOuVazio(tQuinze.erro),
        resultadoPonto(tQuinze),
        numeroOuVazio(tSessenta.erro),
        resultadoPonto(tSessenta),
        resultadoTemperatura,
        numeroOuVazio(u10.incerteza),
        numeroOuVazio(u10.erro),
        resultadoPonto(u10),
        numeroOuVazio(u50.incerteza),
        numeroOuVazio(u50.erro),
        resultadoPonto(u50),
        numeroOuVazio(u90.incerteza),
        numeroOuVazio(u90.erro),
        resultadoPonto(u90),
        resultadoUmidade,
        resultadoGeral
      ];

      row.height = 18;

      for (let col = 1; col <= colCount; col++) {
        const cell = row.getCell(col);
        aplicarPadraoCelula(cell, estilosBase[col - 1]);

        // Colunas de soma:
        // Temperatura: H, J, L, N. Umidade: R, U, X.
        if ([8, 10, 12, 14].includes(col)) {
          aplicarCorSoma(cell, cell.value, limiteTemperaturaRelatorio);
        }

        if ([18, 21, 24].includes(col)) {
          aplicarCorSoma(cell, cell.value, limiteUmidadeRelatorio);
        }

        if ([15, 25, 26].includes(col)) {
          aplicarCorResultado(cell, cell.value);
        }
      }

      if (index % 2 === 1) {
        for (let col = 1; col <= colCount; col++) {
          if (![8, 10, 12, 14, 15, 18, 21, 24, 25, 26].includes(col)) {
            row.getCell(col).fill = {
              type: "pattern",
              pattern: "solid",
              fgColor: { argb: "F0FBF8" }
            };
          }
        }
      }

      row.getCell(3).numFmt = "dd/mm/yyyy";
      for (let col = 6; col <= 24; col++) row.getCell(col).numFmt = "0.00";
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
    sheet.pageSetup.printArea = `A1:Z${ultimaLinha}`;
    sheet.autoFilter = {
      from: `A${headerRow}`,
      to: `Z${ultimaLinhaComDados}`
    };

    sheet.views = [
      {
        state: "frozen",
        ySplit: headerRow,
        topLeftCell: `A${dataStartRow}`,
        activeCell: `A${dataStartRow}`
      }
    ];

    const nomeArquivo = `RELATORIO_DLH_${sufixoArquivo}.xlsx`;

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

    return res.status(e.statusCode || 500).json({
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

  if (METRICS_ENABLED) {
    verificarAgendaMetricas();
    setInterval(() => {
      verificarAgendaMetricas().catch(e => {
        console.log("Erro na agenda de métricas DLH:", e.message);
      });
    }, 60000);
  }
});
