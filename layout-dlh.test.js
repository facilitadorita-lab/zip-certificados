import test from "node:test";
import assert from "node:assert/strict";
import {
  criarAssinaturaLayoutDLH,
  normalizarLayoutDLHIA,
  resumirLinhasParaIADLH
} from "./layout-dlh.js";

function linhasModelo(indicadoUmidade = [14, 51, 86]) {
  return [
    { pagina: 1, y: 400, texto: "Teste (%u.r.)", items: [{ text: "Teste", x: 20 }, { text: "(%u.r.)", x: 70 }] },
    ...indicadoUmidade.map((valor, indice) => ({
      pagina: 1,
      y: 380 - indice * 20,
      texto: `${valor},0 ${[10, 50, 90][indice]},0 0,2 0,4`,
      items: [
        { text: `${valor},0`, x: 100 },
        { text: `${[10, 50, 90][indice]},0`, x: 160 },
        { text: "0,2", x: 220 },
        { text: "0,4", x: 280 }
      ]
    })),
    { pagina: 1, y: 300, texto: "Teste (ºC)", items: [{ text: "Teste", x: 20 }, { text: "(ºC)", x: 70 }] },
    ...[-20, 0, 15, 60].map((padrao, indice) => ({
      pagina: 1,
      y: 280 - indice * 20,
      texto: `${padrao},0 0,2 0,4`,
      items: [
        { text: `${padrao},0`, x: 100 },
        { text: "0,2", x: 220 },
        { text: "0,4", x: 280 }
      ]
    }))
  ];
}

test("assinatura ignora valores e preserva a estrutura posicional", () => {
  assert.equal(
    criarAssinaturaLayoutDLH(linhasModelo()),
    criarAssinaturaLayoutDLH(linhasModelo([18, 48, 88]))
  );
});

test("layout da IA normaliza os padrões para o parser aprendido", () => {
  const layout = normalizarLayoutDLHIA({
    nome: "Modelo laboratório",
    estrategia: "colunas_posicionadas",
    padroes_umidade: [30, 50, 70],
    padroes_temperatura: [-20, 0, 15, 60],
    ordem_colunas: ["indicado", "padrao", "erro", "incerteza"],
    cabecalhos: ["Ind.", "Padrão", "Erro", "Incerteza"],
    observacao: "Layout alternativo"
  });

  assert.deepEqual(layout.padroes_umidade, [30, 50, 70]);
  assert.deepEqual(layout.padroes_temperatura, [-20, 0, 15, 60]);
  assert.deepEqual(layout.ordem_colunas, ["indicado", "padrao", "erro", "incerteza"]);
});

test("resumo posicional limita o contexto enviado à IA", () => {
  const resumo = resumirLinhasParaIADLH(linhasModelo());
  assert.ok(resumo.length > 0);
  assert.equal(typeof resumo[0].itens[0].x, "number");
});

