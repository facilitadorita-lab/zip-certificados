import express from "express";
import fs from "fs";
import os from "os";
import path from "path";
import { chromium } from "playwright";
import { google } from "googleapis";
import { MAPA_LOGGERS, normalizarDLT } from "./mapa-loggers.js";
import fetch from "node-fetch";
import { getDocument } from "pdfjs-dist/legacy/build/pdf.mjs";

const app = express();
app.use(express.json());

// =========================
// CONFIG
// =========================
const SUPABASE_URL = "https://padjfnfysbzaehkqmoyx.supabase.co";
const SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBhZGpmbmZ5c2J6YWVoa3Ftb3l4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY0NTE1OTIsImV4cCI6MjA5MjAyNzU5Mn0.l3xmdwJfu-NDGpoN9MhzQHlW522eO4JX4xgjybRi7vU";
const FOLDER_ID = process.env.FOLDER_ID || "1SZO18AAITa3-3wI86zcZi2yGR6RXtUZ_";
const REPORTS_FOLDER_ID = process.env.REPORTS_FOLDER_ID || "1K0F4EdL2i5y9A1kQF-NlsdPv4zijjHMV";
const GOOGLE_API_KEY = "AIzaSyC6KlqA8q9ZUo_4WRC-pIy7P6kg85WMP3s";

const GOOGLE_CLIENT_EMAIL = process.env.GOOGLE_CLIENT_EMAIL || "id-drive-certificados@calcium-bot-493618-e2.iam.gserviceaccount.com";
const GOOGLE_PRIVATE_KEY = (process.env.GOOGLE_PRIVATE_KEY || "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDV5dgC9gPzZ+Va\nELqoquU0YE8BbPptJ2zsUBr+WzGOJUbeWWyrgo9yqeTYwSzcWKeK11GmRgepgKxc\nkQ4ucxceTil9xsH4+AxcciNYiPFquvkKH0i9/UhkK/WCfbR+OsvCXyx4YtAEK7ju\nLkJ7rQabOsftrIv+XIkiah9tZO6ft2qn3nISRuOaRat3VW9xJeeN/Ba1QZN+6FEl\nV6roHubWbLEn4b7I6nbU/uBy/f7Gu0V52CJNXIdTmYIpuwJvc86MV+/IVDqN/233\nJGmVOEZvkx6RP99sTPxd79jjZsuTnUvCI70ggypusOJZWcb7rEKvrscreKuDydYv\njB3NXXdfAgMBAAECggEACXldI5rV+sM262uJeP/b/k5NvlhsKmC9EfJ/LGKWduwi\nKXMSI/HSfL4XS52yz2FPenZzDWEiS1joFk/uet9qJLnj9WHT8aOHy9VAySK3q4Ym\n+Ow0NdLkKluwGI/zNxKC0Ycs2kackOXtRc95IZU8xHj9pgKNTz6C0t1nqvOPhjXU\nbakMNhX5ckWc132esSXVOGOBenTqjsJcIadNuEcUtcPbx17EJT2P0WOFTOkVHffO\nBWycBcD6N6G6p7p457TfCHjcK6be/kNhTtnX5tUmw9Xy+Cpv5bihKfYZXqD7BrEn\nSs/KireqMUYIPx/7JfdMABIXu2Yt2OZ6APA2xlGPAQKBgQDu2du9ARZapf5M2nTa\n6LJCvanjWOcybRYZBQ5a0HsDnFRG+bkHHQ2Lo4zlSWZQghlPv0VrVrw5epePSwzK\nc2g7sx05nU3UChsIli1isPRkbJrqF2CI54ppyS5JIXIyIVYCl041wE7R5LLz2ulL\nPAclJOr8AhMZ/Cs2noJOnnnTQQKBgQDlQVb1WK2hcxL97dS2FWeeDnL76OTs7vU0\nj+E7hyYBWUzOFIkijtI1DGSV/MIChWOgrNSNw5BTlEtMTsuDP5VwTOjibhBcrc2B\nFea7w5y+eMzHiGNWFNE0aW5nX2Xd4EELFYmZx8ruPUgN27mfT9CvQOxg9FBT+7h4\nvmJp0pzCnwKBgFhCZqFTuofqmKqbety9acmhvhpFasFGcAj0xlYmfZ5a8QV9F7Ma\nODwmRlUfp1AOkv3V5vgAB/ORalnH2MUimhydVipJB05YIZ8tpz21t8k4HJJt6v0L\n2ii274SUeFcv3FF+yaaxFi8XPE1B0j07xEQkfTR8K8TJWsqHDg2xH8FBAoGBAKfd\n9EKqsFjr3hg5seuyOLEve1qh6h7jyoC2agIgr9+E+AxeVRwM4Dcf3/dDoPwfmBfq\n9ajobiIFEC3L9JEiWdZlOpGybiCu0y+WTeFnFrsR0UC5yaMakyWBnenrnLeeoYHw\nP1VvSlSwYrZjEcRpuTDapTtJKhiU1Tr0jTNXmJmZAoGAZRcXd+zBm3spGwGmopD5\nVduVSHESwUucfM6g/UDkzpmRkTWjUAOo7gl/jT4ycoM2IGIjQO8/3hOapoCPmI/v\nSoKlQMJsqDMCz2Y8yOCSPes0sI00qpbXijmkes8eegIc6309l7bgPzlqQXdH2dGW\nCKbtjgeGUVDEXl8fD77sazc=\n-----END PRIVATE KEY-----\n").replace(/\\n/g, "\n").replace(/\\n/g, "\n");
const LOGO_URL = process.env.LOGO_URL || "https://drive.google.com/file/d/1RFnwmMsi1e-x8ktTzb-2IZXTRuXEng9x/view?usp=drive_link";

const LIMITE = 50;

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

function montarHtmlRelatorioDia(registros, dataRelatorio) {
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
            <div><strong>DMA:</strong> 0,5</div>
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
// CONTROLE / HISTÓRICO
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

async function buscarIdsExcluidos() {
  const ids = new Set();
  const limit = 1000;
  let offset = 0;

  while (true) {
    const r = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_excluidos?select=id&limit=${limit}&offset=${offset}`,
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
    const url = `https://www.googleapis.com/drive/v3/files?q='${FOLDER_ID}'+in+parents+and+mimeType='application/pdf'&key=${GOOGLE_API_KEY}&fields=nextPageToken,files(id,name,mimeType)&pageSize=1000${pageToken ? `&pageToken=${pageToken}` : ""}`;

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
    const idsExcluidos = await buscarIdsExcluidos();
    const arquivosDrive = await buscarArquivosDrive();

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
    const idsExcluidosAtualizado = await buscarIdsExcluidos();
    const arquivosDriveAtualizados = await buscarArquivosDrive();
    const faltantesRestantes = arquivosDriveAtualizados.filter(
      f => !idsBancoAtualizado.has(f.id) && !idsExcluidosAtualizado.has(f.id)
    ).length;

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
    const idsExcluidos = await buscarIdsExcluidos();
    const arquivosDrive = await buscarArquivosDrive();

    const totalDrive = arquivosDrive.length;
    const faltantes = arquivosDrive.filter(
      f => !idsBanco.has(f.id) && !idsExcluidos.has(f.id)
    ).length;

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

    const ehDuplicado =
      certificado.duplicado === true ||
      certificado.motivo_divergencia === "Duplicidade (DLT + Data)";

    if (!ehDuplicado) {
      return res.status(400).json({
        erro: "Exclusão permitida apenas para certificados duplicados"
      });
    }

    const insereHistorico = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados_excluidos`,
      {
        method: "POST",
        headers: supabaseHeaders(),
        body: JSON.stringify({
          ...certificado,
          motivo_exclusao: "Exclusão manual pelo Lovable - duplicidade",
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

    const html = montarHtmlRelatorioDia(dados, dataRelatorio);
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

    const html = montarHtmlRelatorioDia(dados, dataRelatorio);
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

app.get("/relatorio-dia", async (req, res) => {
  res.redirect("/relatorio-dia/html");
});

app.listen(3000, () => console.log("Servidor rodando 🚀"));
