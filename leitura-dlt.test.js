import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

// Exercise the production parser without starting the server or database jobs.
const source = readFileSync(new URL("./server.js", import.meta.url), "utf8");
const functions = ["parseBR", "fmt2", "somenteNumeroBR", "acharCabecalho",
  "numeroMaisProximoNaColuna", "extrairTabelaPorColunas"]
  .map(name => {
    const match = source.match(new RegExp(`^(?:async )?function ${name}\\([^]*?^}`, "m"));
    assert.ok(match, `Production function ${name} must exist`);
    return match[0];
  }).join("\n");

function fixture(errors = [39.82, 0.23, 0.12, 0.02], uncertainty = 0.2) {
  const br = n => n.toFixed(2).replace(".", ",");
  return {
    items: [
      { text: "Aquecimento", x: 250.56, y: 496 },
      { text: "Erro", x: 357.60, y: 496 },
      { text: "Medi\u00e7\u00e3o Expandida", x: 399.84, y: 496 },
    ],
    linhas: [-20.02, -0.03, 14.98, 59.98].map((temperature, index) => ({
      y: 484 - index * 12,
      texto: `120 ${br(temperature)} ${br(errors[index])} ${br(uncertainty)} 2,00`,
      items: [
        { text: "120", x: 83.04 },
        { text: br(temperature + errors[index]), x: 162 },
        { text: br(temperature), x: 259.92 },
        { text: br(errors[index]), x: 354 },
        { text: br(uncertainty), x: 423.36 },
        { text: "2,00", x: 492.72 },
      ],
    })),
  };
}

async function parse(data) {
  const context = vm.createContext({ extrairTextoELinhasDoPDF: async () => data });
  vm.runInContext(functions, context);
  return context.extrairTabelaPorColunas(new Uint8Array());
}

test("DLT-0311 retains the 39.82 degree error and fails the 0.5 criterion", async () => {
  const result = await parse(fixture());
  assert.equal(result.ok, true);
  assert.equal(result.pontos.length, 4);
  assert.equal(result.pontos[0].erro, 39.82);
  assert.equal(result.pontos[0].soma, 40.02);
  assert.equal(result.pontos.every(p => p.soma <= 0.5), false);
});

test("large negative errors are read and evaluated by absolute value", async () => {
  const result = await parse(fixture([-39.82, 0.23, 0.12, 0.02]));
  assert.equal(result.ok, true);
  assert.equal(result.pontos[0].erro, -39.82);
  assert.equal(result.pontos[0].soma, 40.02);
});

test("uncertainty above two remains a calibration result", async () => {
  const result = await parse(fixture([0.22, 0.23, 0.12, 0.02], 2.5));
  assert.equal(result.ok, true);
  assert.equal(result.pontos[0].soma, 2.72);
  assert.equal(result.pontos.every(p => p.soma <= 0.5), false);
});

test("ordinary passing certificates retain their four points", async () => {
  const result = await parse(fixture([-0.22, 0.23, 0.12, 0.02]));
  assert.equal(result.ok, true);
  assert.deepEqual(Array.from(result.pontos, p => p.soma), [0.42, 0.43, 0.32, 0.22]);
  assert.equal(result.pontos.every(p => p.soma <= 0.5), true);
});

test("incomplete and nonnumeric tables still fail parsing", async () => {
  const incomplete = fixture();
  incomplete.linhas.pop();
  assert.equal((await parse(incomplete)).ok, false);
  const nonnumeric = fixture();
  nonnumeric.linhas[0].items[3].text = "ilegivel";
  assert.equal((await parse(nonnumeric)).ok, false);
});

test("missing headers and duplicate reference points still fail parsing", async () => {
  const noHeaders = fixture();
  noHeaders.items = [];
  assert.equal((await parse(noHeaders)).ok, false);
  const duplicates = fixture();
  duplicates.linhas[0].items[2].text = duplicates.linhas[1].items[2].text;
  assert.equal((await parse(duplicates)).ok, false);
});
