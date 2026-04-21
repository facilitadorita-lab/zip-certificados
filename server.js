import express from "express";
import fetch from "node-fetch";
import pdf from "pdf-parse";

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
  return parseFloat(v.replace(",", "."));
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
  if (!dataISO) return { valido: false };

  const d = new Date(dataISO);
  const v = new Date(d);
  v.setFullYear(v.getFullYear() + 1);

  return {
    valido: new Date() <= v,
    vencimento: v.toISOString().split("T")[0]
  };
}

// =========================
// PARSER PDF
// =========================
function extrairAquecimentos(texto) {
  const linhas = texto.split("\n");
  const lista = [];
  let capturar = false;

  for (const l of linhas) {
    if (l.trim().toLowerCase() === "aquecimento") {
      capturar = true;
      continue;
    }

    if (!capturar) continue;

    const m = l.match(/-?\d+,\d+/);
    if (m) lista.push(parseBR(m[0]));

    if (lista.length === 4) break;
  }

  return lista;
}

function extrairIncertezas(texto) {
  const linhas = texto.split("\n");
  const lista = [];
  let capturar = false;

  for (const l of linhas) {
    if (l.toLowerCase().includes("incerteza da")) {
      capturar = true;
      continue;
    }

    if (!capturar) continue;

    const m = l.match(/^\s*(-?\d+,\d+)\s*$/);
    if (m) lista.push(Math.abs(parseBR(m[1])));

    if (lista.length === 4) break;
  }

  return lista;
}

function extrairErros(texto) {
  const linhas = texto.split("\n");
  const erros = [];

  for (const linha of linhas) {
    if (!linha.includes("2,00")) continue;

    const nums = linha.match(/-?\d+,\d+/g);
    if (!nums) continue;

    const idx = nums.findIndex(n => n === "2,00");
    if (idx <= 0) continue;

    erros.push(Math.abs(parseBR(nums[idx - 1])));

    if (erros.length === 4) break;
  }

  return erros;
}

async function processarPDF(fileId) {
  try {
    const url = `https://drive.google.com/uc?export=download&id=${fileId}`;
    const res = await fetch(url);

    const buffer = Buffer.from(await res.arrayBuffer());
    const data = await pdf(buffer);

    const texto = data.text;

    const aq = extrairAquecimentos(texto);
    const er = extrairErros(texto);
    const inc = extrairIncertezas(texto);

    if (aq.length < 4 || er.length < 4 || inc.length < 4) {
      return { status: "ERRO", pontos: [] };
    }

    let aprovado = true;

    const pontos = aq.map((a, i) => {
      const soma = +(Math.abs(er[i]) + Math.abs(inc[i])).toFixed(2);
      if (soma > 0.5) aprovado = false;

      return {
        ponto: i + 1,
        aquecimento: +a.toFixed(2),
        erro: +Math.abs(er[i]).toFixed(2),
        incerteza: +Math.abs(inc[i]).toFixed(2),
        soma
      };
    });

    return {
      status: aprovado ? "APROVADO" : "REPROVADO",
      pontos
    };

  } catch {
    return { status: "ERRO", pontos: [] };
  }
}

// =========================
// ROTAS
// =========================

app.get("/", (req, res) => {
  res.send("API OK 🚀");
});

app.get("/sync", async (req, res) => {
  try {
    const existentesRes = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=id`,
      { headers: supabaseHeaders() }
    );

    const existentes = await existentesRes.json();
    const ids = new Set(existentes.map(e => e.id));

    const url = `https://www.googleapis.com/drive/v3/files?q='${FOLDER_ID}'+in+parents&key=${GOOGLE_API_KEY}&fields=files(id,name)&pageSize=1000`;

    const driveRes = await fetch(url);
    const drive = await driveRes.json();

    let processados = 0;

    for (const f of drive.files || []) {
      if (ids.has(f.id)) continue;
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
    }

    res.json({ novos_processados: processados });

  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.get("/reprocess", async (req, res) => {
  const limit = Number(req.query.limit || 50);
  const offset = Number(req.query.offset || 0);

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
  }

  res.json({
    mensagem: "Reprocessamento concluído",
    processados: total,
    offset,
    proximo_offset: offset + total
  });
});

app.listen(3000, () => console.log("Servidor rodando 🚀"));
