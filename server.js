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

function normalizarTexto(texto) {
  return (texto || "")
    .replace(/\r/g, "")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{2,}/g, "\n")
    .trim();
}

function parseNumeroBR(valor) {
  return Math.abs(parseFloat(valor.replace(",", ".")));
}

// =========================
// PARSER DO PDF
// =========================
function extrairErrosELinhas(texto) {
  const linhas = normalizarTexto(texto).split("\n");
  const erros = [];
  const linhasCapturadas = [];

  for (const linha of linhas) {
    // exemplo esperado:
    // 120 -20,1 -0,08 2,00 ∞
    // 120 -0,1 -0,07 2,00 ∞
    // 120 15,0 0,02 2,00 ∞
    // 120 60,2 0,22 2,00 ∞

    const temK = linha.includes("2,00");
    const numeros = linha.match(/-?\d+,\d+/g);

    if (!temK || !numeros || numeros.length < 3) continue;

    // Queremos linhas da tabela de medição, não qualquer linha do PDF.
    // Nessas linhas, o erro costuma ser o PENÚLTIMO número antes do 2,00.
    // Exemplo:
    // ["-20,1", "-0,08", "2,00"] => erro = "-0,08"
    const idxK = numeros.findIndex(n => n === "2,00");
    if (idxK < 1) continue;

    const erroRaw = numeros[idxK - 1];
    const erro = parseNumeroBR(erroRaw);

    // filtro defensivo: erro de certificado desse tipo não deveria ser enorme
    if (erro <= 2) {
      erros.push(erro);
      linhasCapturadas.push(linha);
    }
  }

  return { erros, linhasCapturadas };
}

function extrairIncertezas(texto) {
  const linhas = normalizarTexto(texto).split("\n");
  const incertezas = [];

  let capturar = false;

  for (const linha of linhas) {
    const l = linha.toLowerCase();

    if (l.includes("incerteza da") || l.includes("incerteza expandida")) {
      capturar = true;
      continue;
    }

    if (!capturar) continue;

    // queremos linhas simples com número isolado, ex:
    // 0,20
    const m = linha.match(/^\s*(-?\d+,\d+)\s*$/);
    if (m) {
      const valor = parseNumeroBR(m[1]);

      if (valor <= 2) {
        incertezas.push(valor);
      }

      // padrão do seu certificado: 4 pontos
      if (incertezas.length === 4) break;
    }
  }

  return incertezas;
}

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
    const data = await pdf(buffer);

    const texto = normalizarTexto(data.text || "");

    if (!texto) {
      return {
        status: "ERRO",
        pontos: [],
        debug: { motivo: "PDF sem texto legível" }
      };
    }

    const { erros, linhasCapturadas } = extrairErrosELinhas(texto);
    const incertezas = extrairIncertezas(texto);

    if (erros.length < 4 || incertezas.length < 4) {
      return {
        status: "ERRO",
        pontos: [],
        debug: {
          motivo: "Não encontrou 4 erros e 4 incertezas",
          erros_encontrados: erros,
          incertezas_encontradas: incertezas,
          linhas_erros: linhasCapturadas
        }
      };
    }

    let aprovado = true;
    const pontos = [];

    for (let i = 0; i < 4; i++) {
      const erro = Math.abs(erros[i]);
      const incerteza = Math.abs(incertezas[i]);
      const soma = erro + incerteza;

      if (soma > 0.5) aprovado = false;

      pontos.push({
        ponto: i + 1,
        erro: Number(erro.toFixed(2)),
        incerteza: Number(incerteza.toFixed(2)),
        soma: Number(soma.toFixed(2))
      });
    }

    return {
      status: aprovado ? "APROVADO" : "REPROVADO",
      pontos,
      debug: {
        erros_encontrados: erros,
        incertezas_encontradas: incertezas,
        linhas_erros: linhasCapturadas
      }
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

// sync só processa novos
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
    let processadosNesteCiclo = [];

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

// reprocessa registros existentes com parser novo
app.get("/reprocess", async (req, res) => {
  try {
    const limit = Number(req.query.limit || LIMITE_POR_LOTE);

    await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1`, {
      method: "PATCH",
      headers: supabaseHeaders(),
      body: JSON.stringify({
        em_execucao: true,
        ultima_execucao: new Date()
      })
    });

    const existentesRes = await fetch(
      `${SUPABASE_URL}/rest/v1/certificados?select=id,nome_original,dlt,serie,data&limit=${limit}`,
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
      processados: totalProcessados
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
