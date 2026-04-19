import express from "express";
import archiver from "archiver";
import fetch from "node-fetch";

const app = express();
app.use(express.json());

// 🔥 CORS
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "*");
  res.header("Access-Control-Allow-Methods", "POST, GET, OPTIONS");

  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

// 🔹 CONFIG (PREENCHER)
const GOOGLE_API_KEY = "AIzaSyC6KlqA8q9ZUo_4WRC-pIy7P6kg85WMP3s";
const FOLDER_ID = "1SZO18AAITa3-3wI86zcZi2yGR6RXtUZ_";

// 🔹 EXTRAI DADOS DO NOME
function extrairDados(nome) {
  const partes = nome.replace(".pdf", "").split("_");

  return {
    nome,
    dlt: partes[0]?.replace("DLT-", "") || "",
    serie: partes[1] || "",
    data: partes[2]
      ? partes[2].split(".").reverse().join("-")
      : ""
  };
}

// 🚀 LISTAR ARQUIVOS
app.get("/arquivos", async (req, res) => {
  try {
    const url = `https://www.googleapis.com/drive/v3/files?q='${FOLDER_ID}'+in+parents&key=${GOOGLE_API_KEY}&fields=files(id,name)`;

    const response = await fetch(url);
    const data = await response.json();

    // 🔥 DEBUG MELHORADO
    if (!data.files) {
      return res.status(500).json({
        erro: "Erro ao buscar arquivos no Google Drive",
        detalhe: data
      });
    }

    const arquivos = data.files.map(f => ({
      id: f.id,
      ...extrairDados(f.name)
    }));

    res.json(arquivos);

  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

// 🔹 FORMATADORES
function formatarData(dataISO) {
  if (!dataISO) return "sem-data";
  const [ano, mes, dia] = dataISO.split("-");
  return `${dia}.${mes}.${ano}`;
}

function formatarDLT(dlt) {
  const numero = dlt.toString().replace(/\D/g, "");
  return `DLT-${numero.padStart(4, "0")}`;
}

// 🔹 DOWNLOAD DO DRIVE (SEM API)
async function baixarArquivoDrive(fileId) {
  const url = `https://drive.google.com/uc?export=download&id=${fileId}`;
  const response = await fetch(url);

  if (!response.ok) return null;

  const buffer = await response.arrayBuffer();
  return Buffer.from(buffer);
}

// 🚀 GERAR ZIP
app.post("/zip", async (req, res) => {
  try {
    const { arquivos } = req.body;

    if (!arquivos || arquivos.length === 0) {
      return res.status(400).json({ erro: "Nenhum arquivo enviado" });
    }

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", "attachment; filename=certificados.zip");

    const archive = archiver("zip");
    archive.pipe(res);

    for (const arq of arquivos) {
      const buffer = await baixarArquivoDrive(arq.id);
      if (!buffer) continue;

      const nome = `${formatarDLT(arq.dlt)}_${arq.serie}_${formatarData(arq.data)}.pdf`;

      archive.append(buffer, { name: nome });
    }

    await archive.finalize();

  } catch (e) {
    res.status(500).json({ erro: e.message });
  }
});

// 🧪 TESTE
app.get("/", (req, res) => {
  res.send("API OK 🚀");
});

app.listen(3000, () => console.log("Servidor rodando"));
