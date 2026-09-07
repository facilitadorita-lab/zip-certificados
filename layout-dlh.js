import crypto from "crypto";

const CAMPOS_LAYOUT = new Set(["indicado", "padrao", "erro", "incerteza"]);

function numeroEstrutural(valor) {
  return /^-?\d+(?:[,.]\d+)?$/.test(String(valor || "").trim());
}

function textoEstrutural(valor) {
  return String(valor || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/-?\d+(?:[,.]\d+)?/g, "#")
    .replace(/\s+/g, " ")
    .trim();
}

function arredondarX(valor) {
  const numero = Number(valor);
  return Number.isFinite(numero) ? Math.round(numero / 5) * 5 : null;
}

function numeroLayout(valor) {
  if (typeof valor === "number") return Number.isFinite(valor) ? valor : NaN;
  const texto = String(valor || "").trim().replace(",", ".");
  return Number(texto);
}

function padroesLayout(valor, quantidade, padraoFallback) {
  if (!Array.isArray(valor)) return [...padraoFallback];
  const valores = valor.slice(0, quantidade).map(numeroLayout);
  return valores.length === quantidade && valores.every(Number.isFinite)
    ? valores
    : [...padraoFallback];
}

/**
 * Gera uma assinatura estável da estrutura da tabela, ignorando os valores
 * medidos. Assim, certificados diferentes do mesmo modelo reutilizam o layout.
 */
export function criarAssinaturaLayoutDLH(linhas = []) {
  let modo = "";
  const estrutura = [];

  for (const linha of linhas) {
    const texto = String(linha?.texto || "");
    const upper = textoEstrutural(texto);
    if (upper.includes("UMIDADE") || upper.includes("U.R") || upper.includes("%UR")) modo = "UMIDADE";
    if (upper.includes("TEMPERATURA") || upper.includes("TESTE (C") || upper.includes("TESTE (OC")) modo = "TEMPERATURA";
    if (upper.includes("OBSERVACOES") || upper.includes("DATA DA CALIBRACAO")) modo = "";

    const numeros = (linha?.items || [])
      .filter(item => numeroEstrutural(item?.text))
      .sort((a, b) => Number(a?.x || 0) - Number(b?.x || 0));

    if (numeros.length < 2) continue;

    estrutura.push({
      pagina: Number(linha?.pagina || 0),
      modo: modo || "DESCONHECIDO",
      texto: upper,
      quantidade: numeros.length,
      colunas: numeros.map(item => arredondarX(item?.x)).filter(Number.isFinite)
    });
  }

  const material = JSON.stringify(estrutura.slice(0, 120));
  return crypto.createHash("sha256").update(material).digest("hex");
}

/** Mantém o contexto posicional pequeno o bastante para a chamada da IA. */
export function resumirLinhasParaIADLH(linhas = []) {
  return (linhas || [])
    .filter(linha => (linha?.items || []).filter(item => numeroEstrutural(item?.text)).length >= 2)
    .slice(0, 160)
    .map(linha => ({
      pagina: Number(linha?.pagina || 0),
      y: Number(Number(linha?.y || 0).toFixed(2)),
      texto: textoEstrutural(linha?.texto),
      itens: (linha?.items || [])
        .slice(0, 12)
        .map(item => ({ texto: String(item?.text || ""), x: arredondarX(item?.x) }))
    }));
}

export function normalizarLayoutDLHIA(layout, padroesFallback = {}) {
  const bruto = layout?.layout && typeof layout.layout === "object" ? layout.layout : layout;
  if (!bruto || typeof bruto !== "object") return null;

  const padroesUmidade = padroesLayout(
    bruto.padroes_umidade,
    3,
    padroesFallback.umidade || [10, 50, 90]
  );
  const padroesTemperatura = padroesLayout(
    bruto.padroes_temperatura,
    4,
    padroesFallback.temperatura || [-20, 0, 15, 60]
  );
  const ordem = Array.isArray(bruto.ordem_colunas)
    ? bruto.ordem_colunas.map(item => String(item || "").toLowerCase()).filter(item => CAMPOS_LAYOUT.has(item))
    : [];

  return {
    nome: String(bruto.nome || "Layout DLH aprendido").trim().slice(0, 160),
    estrategia: String(bruto.estrategia || "colunas_posicionadas").trim().slice(0, 80),
    padroes_umidade: padroesUmidade,
    padroes_temperatura: padroesTemperatura,
    ordem_colunas: [...new Set(ordem)],
    cabecalhos: Array.isArray(bruto.cabecalhos)
      ? bruto.cabecalhos.map(item => String(item || "").trim().slice(0, 80)).filter(Boolean).slice(0, 20)
      : [],
    observacao: String(bruto.observacao || "").trim().slice(0, 500)
  };
}
