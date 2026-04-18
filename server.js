import express from "express";
import archiver from "archiver";
import fetch from "node-fetch";

const app = express();
app.use(express.json());

// 🔥 LIBERAR CORS
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "*");
  res.header("Access-Control-Allow-Methods", "POST, GET, OPTIONS");

  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }

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
        const url = `https://drive.google.com/uc?id=${arq.id}`;
        const response = await fetch(url);

        if (!response.ok) continue;

        const buffer = await response.arrayBuffer();

        const nome = `${formatarDLT(arq.dlt)}_${arq.serie}_${formatarData(arq.data)}.pdf`;

        archive.append(Buffer.from(buffer), { name: nome });

      } catch (e) {
        console.log("Erro arquivo:", arq.id);
      }
    }

    await archive.finalize();

  } catch (e) {
    res.status(500).json({ erro: "Erro ao gerar ZIP" });
  }
});

// teste
app.get("/", (req, res) => {
  res.send("API ZIP OK");
});

app.listen(3000, () => console.log("Rodando"));
