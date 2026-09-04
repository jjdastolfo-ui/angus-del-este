// ─────────────────────────────────────────────────────────────────────────────
// EVALUAR — corre el banco de preguntas contra el bot de verdad y puntúa.
//
//   ANTHROPIC_API_KEY=sk-... npm run evaluar
//   MODELO=claude-sonnet-5 ESFUERZO=medium npm run evaluar     (para comparar)
//   npm run evaluar -- vacias corral                           (sólo algunas)
//
// Arma la base de semilla en una carpeta temporal, hace cada pregunta en una
// conversación limpia (salvo las que dependen de otra), verifica la respuesta
// con la función de cada pregunta, e imprime una tabla con el resultado, los
// tokens y una estimación de costo. Guarda el informe en datos/evaluaciones/.
// ─────────────────────────────────────────────────────────────────────────────
const path = require("path");
const fs = require("fs");
const os = require("os");
const { execFileSync } = require("child_process");

if (!process.env.ANTHROPIC_API_KEY && !process.env.ANTHROPIC_AUTH_TOKEN) {
  console.log("Falta ANTHROPIC_API_KEY: la evaluación llama al bot de verdad.");
  process.exit(1);
}

const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rodeo-eval-"));
process.env.DB_DIR = dir;
execFileSync(process.execPath, [path.join(__dirname, "semilla.js")], { env: { ...process.env, DB_DIR: dir }, stdio: "ignore" });

const S = require("../server.js");
const db = S.getDB("principal");
const mods = { plantelMod: require("../plantel.js"), animalesMod: require("../animales.js"), destinosMod: require("../destinos.js") };
const preguntas = require("./preguntas.js");
const solo = process.argv.slice(2).filter(a => !a.startsWith("-"));
const lista = solo.length ? preguntas.filter(p => solo.includes(p.id)) : preguntas;

// Precio por millón de tokens (entrada, salida, lectura de caché) de los modelos habituales.
const PRECIOS = { "claude-opus-5": [5, 25, 0.5], "claude-sonnet-5": [2, 10, 0.2], "claude-sonnet-4-6": [3, 15, 0.3], "claude-fable-5-1": [10, 50, 1], "claude-opus-4-8": [5, 25, 0.5] };
const costo = (uso, modelo) => { const p = PRECIOS[modelo] || PRECIOS["claude-opus-5"]; return (uso.input * p[0] + uso.output * p[1] + uso.cache_read * p[2] + uso.cache_creation * p[0] * 1.25) / 1e6; };

(async () => {
  console.log(`Modelo ${S.bot.modelo} · esfuerzo ${S.bot.esfuerzo} · ${lista.length} preguntas\n`);
  const resultados = [];
  const total = { input: 0, output: 0, cache_read: 0, cache_creation: 0 };
  for (const p of lista) {
    const t0 = Date.now();
    const usuario = p.depende ? `eval-${p.depende}` : `eval-${p.id}`;
    let r, v;
    try {
      r = await S.bot.responder(db, "Angus del Este", p.pregunta, { campoKey: "principal", canal: "eval", usuario });
      v = p.verificar(r, db, mods);
    } catch (e) { r = { respuesta: `ERROR: ${e.message}`, pasos: [], uso: total }; v = { ok: false, esperado: "sin error", motivo: e.message }; }
    const seg = Math.round((Date.now() - t0) / 100) / 10;
    for (const k of Object.keys(total)) total[k] += (r.uso || {})[k] || 0;
    resultados.push({ id: p.id, pregunta: p.pregunta, ok: v.ok, esperado: v.esperado, motivo: v.motivo, respuesta: r.respuesta, pasos: r.pasos, uso: r.uso, segundos: seg });
    console.log(`${v.ok ? "✓" : "✗"} ${p.id.padEnd(18)} ${seg}s  ${(r.pasos || []).filter(x => x.tipo === "consulta").length} consultas  ${v.ok ? "" : `— esperaba: ${v.esperado}${v.motivo ? " (" + v.motivo + ")" : ""}`}`);
    if (!v.ok) console.log(`    respondió: ${String(r.respuesta).replace(/\s+/g, " ").slice(0, 300)}`);
  }
  const bien = resultados.filter(r => r.ok).length;
  const usd = costo(total, S.bot.modelo);
  console.log(`\n${bien}/${resultados.length} correctas · tokens: ${total.input} entrada, ${total.output} salida, ${total.cache_read} de caché · ≈ US$ ${usd.toFixed(2)} (${(usd / resultados.length * 100).toFixed(1)} centavos por pregunta)`);
  if (total.cache_read === 0 && resultados.length > 1) console.log("Ojo: no hubo lecturas de caché. Algo cambia el prompt entre llamadas.");

  const carpeta = path.join(__dirname, "evaluaciones");
  if (!fs.existsSync(carpeta)) fs.mkdirSync(carpeta);
  const archivo = path.join(carpeta, `${new Date().toISOString().slice(0, 16).replace(/[:T]/g, "-")}_${S.bot.modelo}_${S.bot.esfuerzo}.json`);
  fs.writeFileSync(archivo, JSON.stringify({ modelo: S.bot.modelo, esfuerzo: S.bot.esfuerzo, fecha: new Date().toISOString(), bien, total: resultados.length, tokens: total, usd, resultados }, null, 1));
  console.log(`Informe en ${path.relative(process.cwd(), archivo)}`);
  db.close();
  fs.rmSync(dir, { recursive: true, force: true });
  process.exit(bien === resultados.length ? 0 : 1);
})();
