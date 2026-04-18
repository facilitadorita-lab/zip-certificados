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

// 🔥 DOWNLOAD ROBUSTO DO DRIVE
async function baixarArquivoDrive(fileId) {
  const url = `https://drive.google.com/uc?export=download&id=${fileId}`;

  const response = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0"
    }
  });

  if (!response.ok) return null;

  const contentType = response.headers.get("content-type");

  // ⚠️ se vier HTML → erro
  if (contentType && contentType.includes("text/html")) {
    console.log("Recebeu HTML em vez de PDF:", fileId);
    return null;
  }

  const buffer = await response.arrayBuffer();
  return Buffer.from(buffer);
}

// 🚀 ROTA ZIP
app.post("/zip", async (req, res) => {
  try {
    const { arquivos } = req.body;

    if (!arquivos || arquivos.length === 0) {
      return res.status(400).json({ erro: "Nenhum arquivo enviado" });
    }

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", "attachment; filename=certificados.zip");

    const archive = archiver("zip", { zlib: { level: 9 } });
    archive.pipe(res);

    let adicionados = 0;

    for (const arq of arquivos) {
      try {
        const buffer = await baixarArquivoDrive(arq.id);

        if (!buffer || buffer.length < 5000) {
          console.log("Arquivo ignorado:", arq.id);
          continue;
        }

        const nome = `${formatarDLT(arq.dlt)}_${arq.serie}_${formatarData(arq.data)}.pdf`;

        archive.append(buffer, { name: nome });
        adicionados++;

      } catch (e) {
        console.log("Erro no arquivo:", arq.id);
      }
    }

    if (adicionados === 0) {
      console.log("⚠️ Nenhum arquivo válido encontrado");
    }

    await archive.finalize();

  } catch (e) {
    console.error(e);
    res.status(500).json({ erro: "Erro ao gerar ZIP" });
  }
});

// TESTE
app.get("/", (req, res) => {
  res.send("API ZIP OK 🚀");
});

app.listen(3000, () => console.log("Servidor rodando"));
