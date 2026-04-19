import express from "express";
import fetch from "node-fetch";
import pdf from "pdf-parse";

const app = express();
app.use(express.json());

// 🔹 CONFIG
const SUPABASE_URL = "https://padjfnfysbzaehkqmoyx.supabase.co";
const SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBhZGpmbmZ5c2J6YWVoa3Ftb3l4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY0NTE1OTIsImV4cCI6MjA5MjAyNzU5Mn0.l3xmdwJfu-NDGpoN9MhzQHlW522eO4JX4xgjybRi7vU";

const GOOGLE_API_KEY = "AIzaSyC6KlqA8q9ZUo_4WRC-pIy7P6kg85WMP3s";
const FOLDER_ID = "1SZO18AAITa3-3wI86zcZi2yGR6RXtUZ_";

const LIMITE_POR_LOTE = 20; // ⚠️ controle de carga

// 🔹 EXTRAIR DADOS DO NOME
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

// 🔹 VALIDAR DATA
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

// 🔹 PROCESSAR PDF
async function processarPDF(fileId) {
  try {
    const url = `https://drive.google.com/uc?export=download&id=${fileId}`;
    const res = await fetch(url);

    if (!res.ok) return null;

    const buffer = Buffer.from(await res.arrayBuffer());
    const data = await pdf(buffer);

    const linhas = data.text.split("\n");

    const erros = [];
    const incertezas = [];

    for (let l of linhas) {
      const m = l.match(/-?\d+,\d+\s+(-?\d+,\d+)/);
      if (m) erros.push(Math.abs(parseFloat(m[1].replace(",", "."))));
    }

    let capturar = false;
    for (let l of linhas) {
      if (l.includes("Incerteza")) capturar = true;
      if (capturar) {
        const m = l.match(/\d+,\d+/);
        if (m) incertezas.push(parseFloat(m[0].replace(",", ".")));
      }
    }

    let aprovado = true;
    const pontos = [];

    for (let i = 0; i < 4; i++) {
      const soma = (erros[i] || 0) + (incertezas[i] || 0);
      if (soma > 0.5) aprovado = false;

      pontos.push({ ponto: i + 1, soma });
    }

    return {
      status: aprovado ? "APROVADO" : "REPROVADO",
      pontos
    };

  } catch (e) {
    return null;
  }
}

// 🚀 ROTA TESTE
app.get("/", (req, res) => {
  res.send("API OK 🚀");
});

// 🚀 STATUS DO PROCESSAMENTO
app.get("/status", async (req, res) => {
  try {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/controle_sync`, {
      headers: { apikey: SUPABASE_KEY }
    });

    const data = await r.json();
    res.json(data[0] || {});
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

// 🚀 SYNC AUTOMÁTICO COM CONTROLE
app.get("/sync", async (req, res) => {
  try {

    // 🔹 marcar como rodando
    await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1`, {
      method: "PATCH",
      headers: {
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        em_execucao: true,
        ultima_execucao: new Date()
      })
    });

    let pageToken = null;
    let totalProcessados = 0;

    do {
      const url = `https://www.googleapis.com/drive/v3/files?q='${FOLDER_ID}'+in+parents&key=${GOOGLE_API_KEY}&fields=nextPageToken,files(id,name)&pageSize=1000${pageToken ? `&pageToken=${pageToken}` : ""}`;

      const driveRes = await fetch(url);
      const drive = await driveRes.json();

      const arquivos = drive.files || [];

      // 🔹 buscar existentes
      const existentesRes = await fetch(`${SUPABASE_URL}/rest/v1/certificados`, {
        headers: {
          apikey: SUPABASE_KEY,
          Authorization: `Bearer ${SUPABASE_KEY}`
        }
      });

      const existentes = await existentesRes.json();
      const ids = new Set(existentes.map(e => e.id));

      const novos = arquivos.filter(f => !ids.has(f.id));

      // 🔥 processar em lote controlado
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
            apikey: SUPABASE_KEY,
            Authorization: `Bearer ${SUPABASE_KEY}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(registro)
        });

        totalProcessados++;
      }

      pageToken = drive.nextPageToken;

    } while (pageToken);

    // 🔹 finalizar
    await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1`, {
      method: "PATCH",
      headers: {
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        total_processados: totalProcessados,
        em_execucao: false
      })
    });

    res.json({
      processados: totalProcessados
    });

  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

// 🚀 LISTAR CERTIFICADOS
app.get("/certificados", async (req, res) => {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/certificados`, {
    headers: { apikey: SUPABASE_KEY }
  });

  const data = await r.json();
  res.json(data);
});

app.listen(3000, () => console.log("Servidor rodando"));
