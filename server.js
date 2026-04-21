import express from "express";
import fetch from "node-fetch";
import pdf from "pdf-parse";

const app = express();
app.use(express.json());

// ====================== CONFIG ======================
const SUPABASE_URL = "https://padjfnfysbzaehkqmoyx.supabase.co";   // ← sua URL
const SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBhZGpmbmZ5c2J6YWVoa3Ftb3l4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY0NTE1OTIsImV4cCI6MjA5MjAyNzU5Mn0.l3xmdwJfu-NDGpoN9MhzQHlW522eO4JX4xgjybRi7vU";                 // ← chave Service Role
const GOOGLE_API_KEY = "AIzaSyC6KlqA8q9ZUo_4WRC-pIy7P6kg85WMP3s";                 // ← API Key do Google
const FOLDER_ID = "1SZO18AAITa3-3wI86zcZi2yGR6RXtUZ_";                    // ← ID da pasta
const LIMITE_POR_LOTE = 20;                                       // máximo de PDFs por execução do /sync

// ====================== FUNÇÕES AUXILIARES ======================

// Extrai dados do nome do arquivo
function extrairDados(nome) {
  const partes = nome.replace(".pdf", "").split("_");

  return {
    nome_original: nome,
    dlt: partes[0]?.replace("DLT-", "") || "",
    serie: partes[1] || "",
    data: partes[2] ? partes[2].split(".").reverse().join("-") : ""
  };
}

// Valida validade do certificado (1 ano)
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

// Processa o PDF (versão melhorada)
async function processarPDF(fileId) {
  try {
    const url = `https://drive.google.com/uc?export=download&id=${fileId}`;
    const res = await fetch(url);

    if (!res.ok) {
      console.log(`❌ Falha ao baixar PDF ${fileId}`);
      return { status: "ERRO", pontos: [] };
    }

    const buffer = Buffer.from(await res.arrayBuffer());
    const data = await pdf(buffer);
    const texto = data.text || "";
    const linhas = texto.split("\n");

    const erros = [];
    const incertezas = [];

    for (let linha of linhas) {
      // Captura números com vírgula OU ponto
      const matches = linha.match(/-?\d+[.,]\d+/g);
      
      if (matches && matches.length >= 3) {
        // Pega os 3 últimos números da linha (Erro | Incerteza | k)
        const ultimos = matches.slice(-3).map(m => 
          Math.abs(parseFloat(m.replace(",", ".")))
        );

        const [erro, incerteza, k] = ultimos;

        // Confirma que é linha válida (terceiro valor ≈ 2,00)
        if (Math.abs(k - 2) < 0.5) {
          erros.push(erro);
          incertezas.push(incerteza);
        }
      }
    }

    if (erros.length === 0) {
      console.log(`❌ Não encontrou pontos de calibração no PDF ${fileId}`);
      return { status: "ERRO", pontos: [] };
    }

    let aprovado = true;
    const pontos = [];

    for (let i = 0; i < erros.length; i++) {
      const erro = erros[i];
      const inc = incertezas[i] || 0;
      const soma = erro + inc;

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
    console.error("Erro ao processar PDF:", e.message);
    return { status: "ERRO", pontos: [] };
  }
}

// ====================== ROTAS ======================

app.get("/", (req, res) => {
  res.send("API de Processamento de Certificados - OK 🚀");
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
    res.json(data[0] || { mensagem: "Nenhum registro de controle encontrado" });
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

app.get("/sync", async (req, res) => {
  try {
    // Verifica se há novos arquivos
    const check = await fetch(`http://localhost:3000/check-novos`);
    const checkData = await check.json();

    if (checkData.novos === 0) {
      return res.json({
        mensagem: "Nada novo. Sync ignorado.",
        processados: 0
      });
    }

    // Marca início da execução
    await fetch(`${SUPABASE_URL}/rest/v1/controle_sync?id=eq.1`, {
      method: "PATCH",
      headers: {
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        em_execucao: true,
        ultima_execucao: new Date().toISOString()
      })
    });

    let pageToken = null;
    let totalProcessados = 0;

    // Busca IDs já existentes
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
    const ids = new Set(existentes.map(e => e.id));

    do {
      const url = `https://www.googleapis.com/drive/v3/files?q='${FOLDER_ID}'+in+parents&key=${GOOGLE_API_KEY}&fields=nextPageToken,files(id,name)&pageSize=1000${pageToken ? `&pageToken=${pageToken}` : ""}`;

      const driveRes = await fetch(url);
      const drive = await driveRes.json();
      const arquivos = drive.files || [];

      const novos = arquivos.filter(f => !ids.has(f.id));

      // Processa em lote limitado
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
        console.log(`✅ Processado: ${f.name} → ${proc?.status || "ERRO"}`);
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
      body: JSON.stringify({
        total_processados: totalProcessados,
        em_execucao: false
      })
    });

    res.json({
      mensagem: "Sync concluído com sucesso",
      processados: totalProcessados
    });

  } catch (e) {
    console.error("Erro no sync:", e);
    res.status(500).json({ erro: e.message });
  }
});

app.get("/certificados", async (req, res) => {
  try {
    const r = await fetch(`${SUPABASE_URL}/rest/v1/certificados?order=data.desc`, {
      headers: {
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`
      }
    });

    const data = await r.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

app.listen(3000, () => {
  console.log("🚀 Servidor rodando na porta 3000");
});
