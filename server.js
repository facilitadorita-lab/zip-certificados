import express from "express";
import archiver from "archiver";
import fetch from "node-fetch";

const app = express();
app.use(express.json());

// 🔥 LIBERAR CORS (resolve erro no Lovable)
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "*");
  res.header("Access-Control-Allow-Methods", "POST, GET, OPTIONS");

  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }

  next();
});

// 🔹 CONFIG
const GOOGLE_API_KEY = "COLE_SUA_API_KEY_AQUI";

// 🔹 FORMATA DATA (YYYY-MM-DD → DD.MM.YYYY)
function formatarData(dataISO) {
  if (!dataISO) return "sem-data";

  const partes = dataISO.split("-");
  if (partes.length !== 3) return dataISO;

  const [ano, mes, dia] = partes;
  return `${dia}.${mes}.${ano}`;
}

// 🔹 FORMATA DLT (ex: 9 → DLT-0009)
function formatarDLT(dlt) {
  if (!dlt) return "DLT-0000";

  const numero = dlt.toString().replace(/\D/g, "");
  return `DLT-${numero.padStart(4, "0")}`;
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

    for (const arq of arquivos) {
      try {
        const url = `https://www.googleapis.com/drive/v3/files/${arq.id}?alt=media&key=${GOOGLE_API_KEY}`;

        const response = await fetch(url);

        if (!response.ok) {
          console.log("Erro ao baixar arquivo:", arq.id);
          continue;
        }

        const buffer = await response.arrayBuffer();

        const nomeArquivo = `${formatarDLT(arq.dlt)}_${arq.serie}_${formatarData(arq.data)}.pdf`
          .replace(/\s+/g, "_");

        archive.append(Buffer.from(buffer), { name: nomeArquivo });

      } catch (e) {
        console.log("Erro no arquivo:", arq.id, e.message);
      }
    }

    await archive.finalize();

  } catch (e) {
    console.error("Erro geral:", e.message);
    res.status(500).json({ erro: "Erro ao gerar ZIP" });
  }
});

// 🧪 TESTE
app.get("/", (req, res) => {
  res.send("API ZIP funcionando 🚀");
});

app.listen(3000, () => console.log("Servidor rodando"));
