import PDFDocument from "pdfkit";
import { existsSync } from "fs";
import { fileURLToPath } from "url";

const LOGO_ITAFRIA = fileURLToPath(new URL("./logo-itafria.png", import.meta.url));
const RODAPE_ALTURA = 52;

const CORES = {
  navy: "#0B2855",
  teal: "#147B82",
  green: "#198F78",
  mint: "#DDF7F1",
  alternate: "#F0FBF8",
  border: "#8AA2B8",
  text: "#10233F",
  muted: "#52677D",
  okBg: "#D7F5E9",
  okText: "#087F5B",
  badBg: "#FEE4E2",
  badText: "#B42318",
  warnBg: "#FFF3CD",
  warnText: "#8A5B00",
  white: "#FFFFFF"
};

function texto(valor) {
  return String(valor ?? "");
}

function numero(valor) {
  const n = Number(valor);
  return Number.isFinite(n) ? n.toFixed(2).replace(".", ",") : "-";
}

function formatarData(valor) {
  const partes = texto(valor).split("-");
  return partes.length === 3 ? `${partes[2]}/${partes[1]}/${partes[0]}` : texto(valor);
}

function normalizarTag(valor) {
  const digitos = texto(valor).replace(/\D/g, "");
  return digitos ? digitos.padStart(4, "0") : texto(valor);
}

function buscarPonto(pontos, alvo, fallbackIndex, camposReferencia) {
  const lista = Array.isArray(pontos) ? pontos : [];
  const candidatos = lista
    .map(ponto => {
      const referencia = camposReferencia
        .map(campo => Number(ponto?.[campo]))
        .find(Number.isFinite);
      return { ponto, referencia };
    })
    .filter(item => Number.isFinite(item.referencia))
    .sort((a, b) => Math.abs(a.referencia - alvo) - Math.abs(b.referencia - alvo));
  return candidatos[0] && Math.abs(candidatos[0].referencia - alvo) <= 3
    ? candidatos[0].ponto
    : lista[fallbackIndex] || {};
}

function resultadoPonto(ponto) {
  const soma = Number(ponto?.soma);
  if (Number.isFinite(soma)) return soma;
  const erro = Number(ponto?.erro);
  const incerteza = Number(ponto?.incerteza);
  return Number.isFinite(erro) && Number.isFinite(incerteza)
    ? Math.abs(erro) + Math.abs(incerteza)
    : null;
}

function resultadoGrupo(pontos, limite, quantidade) {
  const resultados = pontos.map(resultadoPonto).filter(Number.isFinite);
  if (resultados.length < quantidade) return "INDETERMINADO";
  return resultados.every(valor => valor <= Number(limite)) ? "APROVADO" : "REPROVADO";
}

function estiloResultado(status) {
  const valor = texto(status).toUpperCase();
  if (valor === "APROVADO") return { fill: CORES.okBg, color: CORES.okText };
  if (valor === "REPROVADO" || valor === "ERRO") return { fill: CORES.badBg, color: CORES.badText };
  return { fill: CORES.warnBg, color: CORES.warnText };
}

function desenharCelula(doc, valor, x, y, largura, altura, opcoes = {}) {
  const fill = opcoes.fill || CORES.white;
  const color = opcoes.color || CORES.text;
  doc.save();
  doc.lineWidth(opcoes.lineWidth || 0.45);
  doc.rect(x, y, largura, altura).fillAndStroke(fill, opcoes.border || CORES.border);
  doc.fillColor(color).font(opcoes.bold ? "Helvetica-Bold" : "Helvetica").fontSize(opcoes.fontSize || 6.2);
  const padding = opcoes.padding ?? 2;
  const alturaTexto = doc.heightOfString(texto(valor), {
    width: Math.max(largura - padding * 2, 1),
    align: opcoes.align || "center",
    lineBreak: false
  });
  doc.text(texto(valor), x + padding, y + Math.max((altura - alturaTexto) / 2, 1), {
    width: Math.max(largura - padding * 2, 1),
    height: Math.max(altura - 2, 1),
    align: opcoes.align || "center",
    lineBreak: false,
    ellipsis: true
  });
  doc.restore();
}

function largurasAjustadas(doc, bases) {
  const disponivel = doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const total = bases.reduce((soma, largura) => soma + largura, 0);
  return bases.map(largura => largura * disponivel / total);
}

function desenharCabecalho(doc, configuracao) {
  const x = doc.page.margins.left;
  const largura = doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const topo = doc.page.margins.top;
  const alturaTopo = 42;
  const codigoLargura = largura * 0.18;
  const marcaLargura = largura * 0.16;

  desenharCelula(doc, `${configuracao.codigo}\nVersao: 00\nPeriodo: ${configuracao.periodo}`, x, topo, codigoLargura, alturaTopo, {
    align: "left", fontSize: 6.5, bold: true, border: CORES.navy
  });
  desenharCelula(doc, configuracao.titulo, x + codigoLargura, topo, largura - codigoLargura - marcaLargura, alturaTopo, {
    fontSize: 11, bold: true, color: CORES.navy, border: CORES.navy
  });
  const marcaX = x + largura - marcaLargura;
  desenharCelula(doc, "", marcaX, topo, marcaLargura, alturaTopo, { border: CORES.navy });
  if (existsSync(LOGO_ITAFRIA)) {
    doc.image(LOGO_ITAFRIA, marcaX + 5, topo + 6, {
      fit: [marcaLargura - 10, alturaTopo - 12],
      align: "center",
      valign: "center"
    });
  } else {
    doc.fillColor(CORES.navy).font("Helvetica-Bold").fontSize(11);
    doc.text("ITA FRIA", marcaX + 4, topo + 15, {
      width: marcaLargura - 8,
      align: "center",
      lineBreak: false
    });
  }

  const metaY = topo + alturaTopo;
  const metaLargura = largura / configuracao.meta.length;
  configuracao.meta.forEach((item, index) => {
    desenharCelula(doc, item, x + index * metaLargura, metaY, metaLargura, 18, {
      align: "left", fontSize: 6.5, fill: CORES.mint, border: CORES.navy
    });
  });
  return metaY + 24;
}

function finalizarDocumento(doc, resolve, reject, desenharRodape) {
  try {
    const paginas = doc.bufferedPageRange();
    for (let i = paginas.start; i < paginas.start + paginas.count; i++) {
      doc.switchToPage(i);
      desenharRodape(doc, i + 1, paginas.count);
    }
    doc.end();
  } catch (e) {
    reject(e);
  }
}

function criarDocumento(opcoes, montar) {
  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({
      ...opcoes,
      bufferPages: true,
      autoFirstPage: false,
      info: { Title: opcoes.title, Author: "CalibraFlow", Creator: "CalibraFlow" }
    });
    const partes = [];
    doc.on("data", parte => partes.push(parte));
    doc.on("end", () => resolve(Buffer.concat(partes)));
    doc.on("error", reject);
    montar(doc, (documento, pagina, total) => {
      const esquerda = documento.page.margins.left;
      const direita = documento.page.width - documento.page.margins.right;
      const largura = direita - esquerda;
      const topo = documento.page.height - RODAPE_ALTURA;
      const assinaturaX = esquerda;
      const paginaLargura = 82;
      const assinaturaLargura = Math.max(largura - paginaLargura - 20, 180);
      const margemInferiorOriginal = documento.page.margins.bottom;

      documento.save();
      // O PDFKit tenta criar uma nova página ao escrever dentro da margem inferior.
      // O conteúdo já respeitou a margem reservada; zeramos apenas durante o rodapé.
      documento.page.margins.bottom = 0;
      documento.lineWidth(0.6).strokeColor(CORES.border);
      documento.moveTo(esquerda, topo).lineTo(direita, topo).stroke();

      documento.fillColor(CORES.muted).font("Helvetica").fontSize(6.2);
      documento.text("Responsável pela avaliação:", assinaturaX, topo + 7, {
        width: assinaturaLargura,
        lineBreak: false
      });
      documento.moveTo(assinaturaX, topo + 22).lineTo(assinaturaX + assinaturaLargura, topo + 22).stroke();
      documento.text("Assinatura", assinaturaX, topo + 25, { width: assinaturaLargura, lineBreak: false });

      documento.fillColor(CORES.muted).font("Helvetica").fontSize(6);
      documento.text(`Página ${pagina} de ${total}`, direita - paginaLargura, topo + 24, {
        width: paginaLargura,
        align: "right",
        lineBreak: false
      });
      documento.page.margins.bottom = margemInferiorOriginal;
      documento.restore();
    }, resolve, reject);
  });
}

export function gerarPdfDLT(registros, periodo, dma = 0.5) {
  return criarDocumento({ size: "A4", layout: "landscape", margins: { top: 20, right: 20, bottom: 60, left: 20 }, title: "Relatorio DLT" }, (doc, rodape, resolve, reject) => {
    const bases = [58, 42, 55, 48, 66, 45, 36, 40, 36, 40, 36, 40, 36, 40, 62];
    let larguras;
    let y;

    const novaPagina = () => {
      doc.addPage();
      larguras = largurasAjustadas(doc, bases);
      y = desenharCabecalho(doc, {
        codigo: "REL 06GQ09",
        periodo,
        titulo: "AVALIACAO DOS CERTIFICADOS DE CALIBRACAO - TESTO 174T",
        meta: ["Instrumento: TESTO", "Modelo: 174T", `DMA: ${numero(dma)} C`, `Total: ${registros.length}`]
      });
      const h = 16;
      let x = doc.page.margins.left;
      const fixos = ["N Serie", "TAG", "Calibrado em", "Validade", "Certificado", "Incerteza (+/-U)"];
      fixos.forEach((titulo, i) => {
        desenharCelula(doc, titulo, x, y, larguras[i], h * 2, { fill: CORES.navy, color: CORES.white, bold: true, fontSize: 6 });
        x += larguras[i];
      });
      ["-20,0 C", "0,0 C", "15,0 C", "60,0 C"].forEach((titulo, grupo) => {
        const indice = 6 + grupo * 2;
        const larguraGrupo = larguras[indice] + larguras[indice + 1];
        desenharCelula(doc, titulo, x, y, larguraGrupo, h, { fill: CORES.teal, color: CORES.white, bold: true });
        desenharCelula(doc, "Erro", x, y + h, larguras[indice], h, { fill: CORES.navy, color: CORES.white, bold: true });
        desenharCelula(doc, "Soma", x + larguras[indice], y + h, larguras[indice + 1], h, { fill: CORES.navy, color: CORES.white, bold: true });
        x += larguraGrupo;
      });
      desenharCelula(doc, "RESULTADO", x, y, larguras[14], h * 2, { fill: CORES.navy, color: CORES.white, bold: true, fontSize: 5.8 });
      y += h * 2;
    };

    novaPagina();
    registros.forEach((registro, index) => {
      const altura = 14;
      if (y + altura > doc.page.height - doc.page.margins.bottom - 8) novaPagina();
      const pontosOriginais = Array.isArray(registro.pontos) ? registro.pontos : [];
      const pontos = [
        buscarPonto(pontosOriginais, -20, 0, ["aquecimento", "referencia", "padrao"]),
        buscarPonto(pontosOriginais, 0, 1, ["aquecimento", "referencia", "padrao"]),
        buscarPonto(pontosOriginais, 15, 2, ["aquecimento", "referencia", "padrao"]),
        buscarPonto(pontosOriginais, 60, 3, ["aquecimento", "referencia", "padrao"])
      ];
      const resultados = pontos.map(resultadoPonto);
      const status = resultados.filter(Number.isFinite).length === 4
        ? (resultados.every(valor => valor <= Number(dma)) ? "APROVADO" : "REPROVADO")
        : texto(registro.status || "INDETERMINADO").toUpperCase();
      const incerteza = pontosOriginais.map(ponto => Number(ponto?.incerteza)).find(Number.isFinite);
      const valores = [
        registro.serie, normalizarTag(registro.dlt), formatarData(registro.data), registro.mes_ano_validade,
        registro.certificado, numero(incerteza),
        ...pontos.flatMap((ponto, i) => [numero(ponto?.erro), numero(resultados[i])]), status
      ];
      let x = doc.page.margins.left;
      valores.forEach((valor, coluna) => {
        let estilo = { fill: index % 2 ? CORES.alternate : CORES.white };
        if ([7, 9, 11, 13].includes(coluna)) {
          const resultado = Number(pontos[(coluna - 7) / 2]?.soma);
          const calculado = Number.isFinite(resultado) ? resultado : resultados[(coluna - 7) / 2];
          estilo = Number.isFinite(calculado) && calculado <= Number(dma)
            ? { fill: CORES.okBg, color: CORES.okText, bold: true }
            : { fill: CORES.badBg, color: CORES.badText, bold: true };
        }
        if (coluna === 14) estilo = { ...estiloResultado(status), bold: true, fontSize: 5.6 };
        desenharCelula(doc, valor, x, y, larguras[coluna], altura, estilo);
        x += larguras[coluna];
      });
      y += altura;
    });
    finalizarDocumento(doc, resolve, reject, rodape);
  });
}

export function gerarPdfDLH(registros, periodo, limiteTemperatura = 0.5, limiteUmidade = 5) {
  return criarDocumento({ size: "A3", layout: "landscape", margins: { top: 16, right: 16, bottom: 60, left: 16 }, title: "Relatorio DLH" }, (doc, rodape, resolve, reject) => {
    const bases = [52, 36, 50, 42, 58, 36, 31, 33, 31, 33, 31, 33, 31, 33, 50, 30, 30, 33, 30, 30, 33, 30, 30, 33, 50, 50];
    let larguras;
    let y;

    const novaPagina = () => {
      doc.addPage();
      larguras = largurasAjustadas(doc, bases);
      y = desenharCabecalho(doc, {
        codigo: "REL 06GQ10",
        periodo,
        titulo: "AVALIACAO DOS CERTIFICADOS DE CALIBRACAO - TESTO 174H",
        meta: ["Instrumento: TESTO", "Modelo: 174H", `DMA temperatura: ${numero(limiteTemperatura)} C`, `DMA umidade: ${numero(limiteUmidade)} %UR`, `Total: ${registros.length}`]
      });
      const h = 15;
      let x = doc.page.margins.left;
      ["N Serie", "TAG", "Calibrado em", "Validade", "Certificado", "Incerteza T"].forEach((titulo, i) => {
        desenharCelula(doc, titulo, x, y, larguras[i], h * 2, { fill: CORES.navy, color: CORES.white, bold: true, fontSize: 5.7 });
        x += larguras[i];
      });
      ["-20 C", "0 C", "15 C", "60 C"].forEach((titulo, grupo) => {
        const indice = 6 + grupo * 2;
        const larguraGrupo = larguras[indice] + larguras[indice + 1];
        desenharCelula(doc, titulo, x, y, larguraGrupo, h, { fill: CORES.teal, color: CORES.white, bold: true });
        desenharCelula(doc, "Erro", x, y + h, larguras[indice], h, { fill: CORES.navy, color: CORES.white, bold: true });
        desenharCelula(doc, "Soma", x + larguras[indice], y + h, larguras[indice + 1], h, { fill: CORES.navy, color: CORES.white, bold: true });
        x += larguraGrupo;
      });
      desenharCelula(doc, "Resultado T", x, y, larguras[14], h * 2, { fill: CORES.teal, color: CORES.white, bold: true, fontSize: 5.5 });
      x += larguras[14];
      ["10% UR", "50% UR", "90% UR"].forEach((titulo, grupo) => {
        const indice = 15 + grupo * 3;
        const larguraGrupo = larguras[indice] + larguras[indice + 1] + larguras[indice + 2];
        desenharCelula(doc, titulo, x, y, larguraGrupo, h, { fill: CORES.green, color: CORES.white, bold: true });
        ["Inc.", "Erro", "Soma"].forEach((subtitulo, i) => {
          desenharCelula(doc, subtitulo, x, y + h, larguras[indice + i], h, { fill: CORES.navy, color: CORES.white, bold: true });
          x += larguras[indice + i];
        });
      });
      desenharCelula(doc, "Resultado UR", x, y, larguras[24], h * 2, { fill: CORES.green, color: CORES.white, bold: true, fontSize: 5.5 });
      x += larguras[24];
      desenharCelula(doc, "RESULTADO", x, y, larguras[25], h * 2, { fill: CORES.navy, color: CORES.white, bold: true, fontSize: 5.5 });
      y += h * 2;
    };

    novaPagina();
    registros.forEach((registro, index) => {
      const altura = 13;
      if (y + altura > doc.page.height - doc.page.margins.bottom - 7) novaPagina();
      const temperaturaOriginal = Array.isArray(registro.pontos_temperatura) ? registro.pontos_temperatura : [];
      const umidadeOriginal = Array.isArray(registro.pontos_umidade) ? registro.pontos_umidade : [];
      const temperatura = [-20, 0, 15, 60].map((alvo, i) => buscarPonto(temperaturaOriginal, alvo, i, ["padrao", "referencia"]));
      const umidade = [10, 50, 90].map((alvo, i) => buscarPonto(umidadeOriginal, alvo, i, ["padrao", "referencia"]));
      const somasT = temperatura.map(resultadoPonto);
      const somasU = umidade.map(resultadoPonto);
      const resultadoT = resultadoGrupo(temperaturaOriginal, limiteTemperatura, 4);
      const resultadoU = resultadoGrupo(umidadeOriginal, limiteUmidade, 3);
      const resultadoGeral = resultadoT === "REPROVADO" || resultadoU === "REPROVADO"
        ? "REPROVADO"
        : resultadoT === "APROVADO" && resultadoU === "APROVADO"
          ? "APROVADO"
          : texto(registro.status || "INDETERMINADO").toUpperCase();
      const incertezaT = temperaturaOriginal.map(ponto => Number(ponto?.incerteza)).find(Number.isFinite);
      const valores = [
        registro.serie, normalizarTag(registro.dlh), formatarData(registro.data), registro.mes_ano_validade, registro.certificado,
        numero(incertezaT), ...temperatura.flatMap((ponto, i) => [numero(ponto?.erro), numero(somasT[i])]), resultadoT,
        ...umidade.flatMap((ponto, i) => [numero(ponto?.incerteza), numero(ponto?.erro), numero(somasU[i])]), resultadoU, resultadoGeral
      ];
      let x = doc.page.margins.left;
      valores.forEach((valor, coluna) => {
        let estilo = { fill: index % 2 ? CORES.alternate : CORES.white, fontSize: 5.4 };
        if ([7, 9, 11, 13].includes(coluna)) {
          const soma = somasT[(coluna - 7) / 2];
          estilo = Number.isFinite(soma) && soma <= Number(limiteTemperatura)
            ? { fill: CORES.okBg, color: CORES.okText, bold: true, fontSize: 5.4 }
            : { fill: CORES.badBg, color: CORES.badText, bold: true, fontSize: 5.4 };
        }
        if ([17, 20, 23].includes(coluna)) {
          const soma = somasU[(coluna - 17) / 3];
          estilo = Number.isFinite(soma) && soma <= Number(limiteUmidade)
            ? { fill: CORES.okBg, color: CORES.okText, bold: true, fontSize: 5.4 }
            : { fill: CORES.badBg, color: CORES.badText, bold: true, fontSize: 5.4 };
        }
        if (coluna === 14) estilo = { ...estiloResultado(resultadoT), bold: true, fontSize: 5 };
        if (coluna === 24) estilo = { ...estiloResultado(resultadoU), bold: true, fontSize: 5 };
        if (coluna === 25) estilo = { ...estiloResultado(resultadoGeral), bold: true, fontSize: 5 };
        desenharCelula(doc, valor, x, y, larguras[coluna], altura, estilo);
        x += larguras[coluna];
      });
      y += altura;
    });
    finalizarDocumento(doc, resolve, reject, rodape);
  });
}
