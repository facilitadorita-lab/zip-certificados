import express from "express";
import archiver from "archiver";
import fetch from "node-fetch";

const app = express();
app.use(express.json());

app.post("/zip", async (req, res) => {
  const { arquivos } = req.body;

  res.setHeader("Content-Type", "application/zip");
  res.setHeader("Content-Disposition", "attachment; filename=certificados.zip");

  const archive = archiver("zip", { zlib: { level: 9 } });
  archive.pipe(res);

  for (const arq of arquivos) {
    try {
      const url = `https://drive.google.com/uc?id=${arq.id}`;
      const response = await fetch(url);
      const buffer = await response.arrayBuffer();

      const nome = `${arq.dlt}_${arq.serie}_${arq.data}.pdf`
        .replace(/\s+/g, "_");

      archive.append(Buffer.from(buffer), { name: nome });

    } catch (e) {
      console.log("Erro no arquivo:", arq.id);
    }
  }

  await archive.finalize();
});

app.listen(3000, () => console.log("API rodando"));
