import express from "express";
import fetch from "node-fetch";
import pdf from "pdf-parse";

const app = express();
app.use(express.json());

// 🔹 CONFIG
const SUPABASE_URL = "https://padjfnfysbzaehkqmoyx.supabase.co";
const SUPABASE_KEY = "minha chave";           // ← substitua
const GOOGLE_API_KEY = "minha chave";         // ← substitua
const FOLDER_ID = "minha chave";              // ← substitua
const LIMITE_POR_LOTE = 20;

// 🔹 EXTRAIR DADOS DO NOME DO ARQUIVO
function extrairDados(nome) {
  const partes = nome.replace(".pdf", "").split("_");
  return {
    nome_original: nome,
    dlt: partes[0]?.replace("DLT-", "") || "",
    serie: partes[1] || "",
    data: partes[2] ? partes[2].split(".").reverse().join("-") : ""
  };
}

// 🔹 VALIDAR VALIDADE (1 ano)
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

// 🔥 PROCESSAR PDF - VERSÃO MELHORADA PARA O CERTIFICADO DA ESCALA
async function processarPDF(fileId) {
  try {
    const url = `https://drive.google.com/uc?export=download&id=${fileId}`;
    const res = await fetch(url);
    if (!res.ok) return null;

    const buffer = Buffer.from(await res.arrayBuffer());
    const data = await pdf(buffer);
    const texto = data.text || "";
    const linhas = texto.split("\n");

    const erros = [];
    const incertezas = [];
    let capturar = false;

    for (let linha of linhas) {
      const l = linha.toLowerCase().trim();

      // 🔥 Detecta início da tabela
      if (l.includes("erro") && l.includes("incerteza")) {
        capturar = true;
        continue;
      }

      // 🔥 Para quando a tabela acabar (linha em branco)
      if (capturar && linha.trim() === "") {
        break;
      }

      if (capturar) {
        const matches = linha.match(/-?\d+,\d+/g);
        if (matches && matches.length >= 3) {
          // Pega os 3 últimos números da linha (erro, incerteza, k)
          const ultimos = matches.slice(-3).map(m => 
            Math.abs(parseFloat(m.replace(",", ".")))
          );

          const erro = ultimos[0];
          const incerteza = ultimos[1];
          const k = ultimos[2];

          // Confirma que o último número é o fator k ≈ 2,00
          if (Math.abs(k - 2) < 0.5) {
            erros.push(erro);
            incertezas.push(incerteza);
          }
        }
      }
    }

    // 🔥 VALIDAÇÃO
    if (erros.length === 0) {
      console.log("❌ Não encontrou tabela de erro/incerteza");
      return { status: "ERRO", pontos: [] };
    }

    let aprovado = true;
    const pontos = [];

    for (let i = 0; i < erros.length; i++) {
      const erro = erros[i];
      const inc = incertezas[i];
      const soma = erro + inc;

      // ✅ Aprovado se soma <= 0,50 (exatamente como você pediu)
      if (soma > 0.50) aprovado = false;

      pontos.push({
        ponto: i + 1,
        erro: Number(erro.toFixed(2)),
        incerteza: Number(inc.toFixed(2)),
        soma: Number(soma.toFixed(2))
      });
    }

    return {
      status: aprovado ? "APROVADO" : "REPROVADO",
      pontos
    };

  } catch (e) {
    console.log("Erro ao processar PDF:", e.message);
    return null;
  }
}

// 🚀 TESTE
app.get("/", (req, res) => res.send("API OK 🚀"));

// 🚀 STATUS
app.get("/status", async (req, res) => {
  try {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1&select=*`, {
      headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` }
    });
    const data = await r.json();
    res.json(data[0] || {});
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

// 🚀 VERIFICAR NOVOS
app.get("/check-novos", async (req, res) => {
  try {
    const existentesRes = await fetch(`${SUPABASE_URL}/rest/v1/certificados?select=id`, {
      headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` }
    });
    const existentes = await existentesRes.json();
    const idsExistentes = new Set(existentes.map(e => e.id));

    let pageToken = null;
    let totalNovos = 0;

    do {
      const url = `https://www.googleapis.com/drive/v3/files?q='${FOLDER_ID}'+in+parents&key=${GOOGLE_API_KEY}&fields=nextPageToken,files(id,name)&pageSize=1000${pageToken ? `&pageToken=${pageToken}` : ""}`;
      const driveRes = await fetch(url);
      const drive = await driveRes.json();
      const arquivos = drive.files || [];
      const novos = arquivos.filter(f => !idsExistentes.has(f.id));
      totalNovos += novos.length;
      pageToken = drive.nextPageToken;
    } while (pageToken);

    res.json({ novos: totalNovos });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

// 🚀 SYNC INTELIGENTE
app.get("/sync", async (req, res) => {
  try {
    const check = await fetch("https://zip-certificados.onrender.com/check-novos");
    const checkData = await check.json();
    if (checkData.novos === 0) {
      return res.json({ mensagem: "Nada novo. Sync ignorado.", processados: 0 });
    }

    // Marca execução
    await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1`, {
      method: "PATCH",
      headers: {
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ em_execucao: true, ultima_execucao: new Date() })
    });

    const existentesRes = await fetch(`${SUPABASE_URL}/rest/v1/certificados?select=id`, {
      headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` }
    });
    const existentes = await existentesRes.json();
    const ids = new Set(existentes.map(e => e.id));

    let pageToken = null;
    let totalProcessados = 0;

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

    // Finaliza execução
    await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1`, {
      method: "PATCH",
      headers: {
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ total_processados: totalProcessados, em_execucao: false })
    });

    res.json({ mensagem: "Sync concluído", processados: totalProcessados });
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

// 🚀 LISTAR CERTIFICADOS
app.get("/certificados", async (req, res) => {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/certificados`, {
    headers: { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` }
  });
  const data = await r.json();
  res.json(data);
});

app.listen(3000, () => console.log("Servidor rodando 🚀"));
