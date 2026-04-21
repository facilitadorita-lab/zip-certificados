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

function parseBR(v) {
  return parseFloat(v.replace(",", "."));
}

// =========================
// EXTRAÇÕES DO PDF
// =========================

function extrairAquecimentos(texto) {
  const linhas = texto.split("\n");
  const lista = [];
  let capturar = false;

  for (const l of linhas) {
    const line = l.trim().toLowerCase();

    if (line === "aquecimento") {
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
    const line = l.toLowerCase();

    if (line.includes("incerteza da")) {
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

    const erro = Math.abs(parseBR(nums[idx - 1]));
    erros.push(erro);

    if (erros.length === 4) break;
  }

  return erros;
}

// =========================
// PROCESSAMENTO
// =========================
async function processarPDF(fileId) {
  try {
    const url = `https://drive.google.com/uc?export=download&id=${fileId}`;
    const res = await fetch(url);

    if (!res.ok) {
      return { status: "ERRO", pontos: [] };
    }

    const buffer = Buffer.from(await res.arrayBuffer());
    const data = await pdf(buffer);

    const texto = data.text;

    const aquecimentos = extrairAquecimentos(texto);
    const erros = extrairErros(texto);
    const incertezas = extrairIncertezas(texto);

    if (aquecimentos.length < 4 || erros.length < 4 || incertezas.length < 4) {
      return {
        status: "ERRO",
        pontos: [],
        debug: { aquecimentos, erros, incertezas }
      };
    }

    let aprovado = true;

    const pontos = aquecimentos.map((a, i) => {
      const erro = Math.abs(erros[i]);
      const inc = Math.abs(incertezas[i]);
      const soma = +(erro + inc).toFixed(2);

      if (soma > 0.5) aprovado = false;

      return {
        ponto: i + 1,
        aquecimento: +a.toFixed(2),
        erro: +erro.toFixed(2),
        incerteza: +inc.toFixed(2),
        soma
      };
    });

    return {
      status: aprovado ? "APROVADO" : "REPROVADO",
      pontos
    };

  } catch (e) {
    return { status: "ERRO", pontos: [] };
  }
}

// =========================
// ROTAS
// =========================

app.get("/", (req, res) => {
  res.send("API OK 🚀");
});

app.get("/certificados", async (req, res) => {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/certificados?select=*&order=data.desc`, {
    headers: supabaseHeaders()
  });

  const data = await r.json();
  res.json(data);
});

app.get("/reprocess", async (req, res) => {
  const limit = Number(req.query.limit || 50);
  const offset = Number(req.query.offset || 0);

  const existentesRes = await fetch(
    `${SUPABASE_URL}/rest/v1/certificados?select=id,nome_original,dlt,serie,data&limit=${limit}&offset=${offset}`,
    { headers: supabaseHeaders() }
  );

  const existentes = await existentesRes.json();

  let total = 0;

  for (const item of existentes) {
    const proc = await processarPDF(item.id);
    const val = verificarValidade(item.data);

    await fetch(`${SUPABASE_URL}/rest/v1/certificados?id=eq.${item.id}`, {
      method: "PATCH",
      headers: supabaseHeaders(),
      body: JSON.stringify({
        status: proc.status,
        pontos: proc.pontos,
        validade: val.valido,
        vencimento: val.vencimento
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
