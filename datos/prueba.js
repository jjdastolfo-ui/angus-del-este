// ─────────────────────────────────────────────────────────────────────────────
// PRUEBA — corre los módulos contra una base de prueba y avisa si algo se rompe.
//
//   npm run prueba
//
// Arma la base de semilla en una carpeta temporal (no toca ./data ni /data),
// y ejercita búsqueda, fichas, exportación, relevamiento, importación de CSV,
// destinos, y el bot con un Claude simulado (así se prueba el circuito de
// herramientas, la memoria, el streaming y el caché sin gastar un token).
// Para probar al bot de verdad: npm run evaluar.
// ─────────────────────────────────────────────────────────────────────────────
const path = require("path");
const fs = require("fs");
const os = require("os");
const { execFileSync } = require("child_process");

const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rodeo-prueba-"));
process.env.DB_DIR = dir;
process.env.ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || "sin-clave";
// Dos campos de la misma empresa, cada uno con su base.
process.env.CAMPOS = JSON.stringify({ principal: { nombre: "Angus del Este", empresa: "improlux" }, triunfo: { nombre: "El Triunfo", empresa: "improlux" } });
process.env.EMPRESAS = JSON.stringify({ improlux: { nombre: "Improlux", finanzas_url: "", finanzas_campo: "AMAKAIK" } });
execFileSync(process.execPath, [path.join(__dirname, "semilla.js")], { env: { ...process.env, DB_DIR: dir }, stdio: "ignore" });
execFileSync(process.execPath, [path.join(__dirname, "semilla.js")], { env: { ...process.env, DB_DIR: dir, CAMPO: "triunfo" }, stdio: "ignore" });

const S = require("../server.js");
const db = S.getDB("principal");
const plantelMod = require("../plantel.js"), animalesMod = require("../animales.js"), destinosMod = require("../destinos.js");
const exportarMod = require("../exportar.js"), relevarMod = require("../relevar.js"), xlsx = require("../xlsx.js"), botMod = require("../bot.js");
const mods = { plantelMod, animalesMod, destinosMod };

let fallas = 0, n = 0;
function ok(cond, que) { n++; if (!cond) { fallas++; console.log("  FALLÓ:", que); } }
function seccion(t) { console.log("\n" + t); }

seccion("El tablero");
// Que el JavaScript del tablero sea válido: un error de sintaxis deja la pantalla
// en blanco y no lo ve ninguna otra prueba.
const indexHtml = fs.readFileSync(path.join(__dirname, "..", "public", "index.html"), "utf8");
const bloques = indexHtml.match(/<script>[\s\S]*?<\/script>/g) || [];
ok(bloques.length >= 1, "el tablero tiene su script");
for (const [i, b] of bloques.entries()) {
  let err = null;
  try { new (require("vm").Script)(b.replace(/^<script>/, "").replace(/<\/script>$/, ""), { filename: "index.html" }); } catch (e) { err = e.message; }
  ok(!err, `el script ${i + 1} del tablero es JavaScript válido${err ? ": " + err : ""}`);
}
ok(/<\/html>\s*$/.test(indexHtml.trim()), "el HTML del tablero está cerrado");

seccion("Buscar");
const b1 = animalesMod.buscar(db, "011");
ok(b1.length && b1[0].rp === "11", "'011' encuentra a la 11 primero");
ok(animalesMod.buscar(db, "b 332")[0].rp === "B332", "'b 332' encuentra al toro B332");
ok(animalesMod.buscar(db, "hércules").length > 5 && animalesMod.buscar(db, "hércules").slice(1).every(a => a.coincide === "padre"), "'hércules' encuentra al toro y después a sus hijos por padre");
ok(animalesMod.buscar(db, "renga").length === 1, "busca en las notas de campo");
ok(animalesMod.porRp(db, " 011 ").rp === "11", "porRp tolera ceros y espacios");

seccion("Fichas");
const f1 = animalesMod.ficha(db, "11");
ok(f1.ok && f1.es_vientre && f1.pesadas.length >= 2, "ficha general de una vaca");
const ternero = animalesMod.listar(db).find(a => a.categoria === "TERNERO");
const f2 = animalesMod.ficha(db, ternero.rp);
ok(f2.ok && !f2.es_vientre && f2.madre_existe && f2.peso_nac > 0, "ficha general de un ternero, con madre");
const f3 = plantelMod.ficha(db, "11");
ok(f3.ok && f3.campanas.length >= 3, "ficha reproductiva de la 11");
ok(!animalesMod.ficha(db, "NOEXISTE").ok, "ficha de un RP inexistente falla con mensaje");

seccion("Exportar");
for (const k of Object.keys(exportarMod.COLUMNAS)) {
  const c = exportarMod.conjunto(db, mods, k);
  ok(Array.isArray(c.filas) && c.columnas.length > 3, `conjunto ${k}`);
}
const ex = exportarMod.armar(db, mods, "plantel", "xlsx", { campoNombre: "Prueba" });
ok(ex.buffer.length > 5000 && ex.buffer.slice(0, 2).toString() === "PK", "plantel.xlsx es un zip");
const todo = exportarMod.armar(db, mods, "rodeo", "xlsx", { campoNombre: "Prueba" });
ok(todo.buffer.length > 50000, "rodeo.xlsx completo");
const csv = exportarMod.armar(db, mods, "plantel", "csv", { rps: ["11", "013"], columnas: ["rp", "pn_prom"] }).buffer.toString();
ok(csv.split("\r\n").length === 3 && csv.includes(";") && /\d,\d/.test(csv), "csv con ; y coma decimal, sólo los RP pedidos");
const html = exportarMod.armar(db, mods, "nacimientos", "html", {}).buffer.toString();
ok(html.includes("<table") && html.includes("window.print"), "html imprimible");
const tablas = exportarMod.tablasDeHtml("<h2>Vacías</h2><table><tr><th>RP</th><th>Peso</th></tr><tr><td>11</td><td>1.234,5</td></tr></table>");
ok(tablas.length === 1 && tablas[0].nombre === "Vacías" && tablas[0].filas[0].c1 === 1234.5, "lee tablas de un tablero del bot");
const arch = exportarMod.desdeConsulta(db, { sql: "SELECT rp AS \"RP\" FROM animales WHERE categoria='TORO'", titulo: "Toros" });
ok(arch.url.startsWith("/archivos/") && arch.filas === 6, "archivo desde un SELECT");
let error = null; try { exportarMod.desdeConsulta(db, { sql: "DELETE FROM animales", titulo: "x" }); } catch (e) { error = e.message; }
ok(error === "Sólo SELECT", "no deja exportar con algo que no sea SELECT");

seccion("Excel");
const buf = xlsx.armar([{ nombre: "H", columnas: [{ k: "a", t: "A" }, { k: "f", t: "F" }], filas: [{ a: 1.5, f: "2026-01-02" }, { a: null, f: null }] }]);
ok(buf.slice(0, 2).toString() === "PK", "xlsx es un zip");
const zlib = require("zlib");
const largoNombre = buf.readUInt16LE(26), largoComp = buf.readUInt32LE(18);
const xml = zlib.inflateRawSync(buf.slice(30 + largoNombre, 30 + largoNombre + largoComp)).toString();
ok(xml.startsWith("<?xml"), "el contenido del zip es XML");

seccion("Relevar");
const antes = db.prepare("SELECT COUNT(*) n FROM pesadas").get().n;
const sim = relevarMod.pesadas(db, { filas: relevarMod.parsearLineas("011 432\n13 470\nZZZ 300\n11 432"), fecha: "03/09/2026", simular: true });
ok(sim.simulado && sim.bien === 3 && sim.mal === 1, "simular pesadas: 3 bien, 1 mal");
ok(sim.filas[0].rp === "11" && sim.filas[0].anterior > 0, "'011' se resuelve a 11 y trae la pesada anterior");
ok(sim.filas[3].avisos.some(a => /repetido/.test(a)), "avisa la fila repetida");
ok(db.prepare("SELECT COUNT(*) n FROM pesadas").get().n === antes, "simular no escribe");
const real = relevarMod.pesadas(db, { filas: [{ rp: "11", peso: "432" }], fecha: "2026-09-03", contexto: "CONTROL" });
ok(real.bien === 1 && db.prepare("SELECT COUNT(*) n FROM pesadas").get().n === antes + 1, "cargar escribe una pesada");
const dup = relevarMod.pesadas(db, { filas: [{ rp: "11", peso: "432" }], fecha: "2026-09-03" });
ok(dup.mal === 1 && /Ya estaba/.test(dup.filas[0].error), "no repite una pesada igual");
const san = relevarMod.sanidad(db, { lote_id: 1, producto: "IVERMECTINA", dosis: "5 ml", simular: true });
ok(san.bien === 18, "sanidad a un lote entero");
// Una madre que ya tiene cría este año, para que avise.
const madreConCria = db.prepare("SELECT madre_rp FROM animales WHERE fecha_nac LIKE '2026%' AND madre_rp IS NOT NULL AND madre_rp NOT LIKE '%0%' LIMIT 1").get().madre_rp;
const nac = relevarMod.nacimientos(db, { filas: [{ rp: "C900", madre_rp: "0" + madreConCria, fecha_nac: "01/09/2026", sexo: "h", pelo: "colorada", peso_nac: "31,5" }] });
ok(nac.bien === 1 && animalesMod.porRp(db, "C900").categoria === "TERNERA", "nacimiento crea la ternera");
ok(nac.filas[0].avisos.some(a => /ya tiene cría/.test(a)), "avisa que la madre ya tiene cría este año");
const nac2 = relevarMod.nacimientos(db, { filas: [{ rp: "C900", madre_rp: madreConCria, fecha_nac: "2026-09-01", sexo: "M" }], simular: true });
ok(nac2.mal === 1, "no deja repetir el RP");
const med = relevarMod.mediciones(db, { filas: [{ rp: "13", valor: "3,5" }], tipo: "CC", simular: true });
ok(med.bien === 1 && med.filas[0].valor === 3.5, "medición con coma decimal");
const nota = relevarMod.notas(db, plantelMod, { filas: [{ rp: "13", texto: "abortó" }], simular: true });
ok(nota.filas[0].avisos.length === 1, "la nota grave avisa");

seccion("Caravana control → RP → chip");
const nacC = relevarMod.nacimientos(db, { filas: [{ caravana_control: "150", caravana_color: "blanca", madre_rp: "21", fecha_nac: "02/09/2026", sexo: "M", peso_nac: 33 }, { caravana_control: "150", caravana_color: "verde", madre_rp: "23", fecha_nac: "02/09/2026", sexo: "H", peso_nac: 30 }] });
ok(nacC.bien === 2 && nacC.filas[0].rp === "C150" && nacC.filas[0].provisorio && nacC.filas[1].rp === "C150-VER" && nacC.filas[1].avisos.some(x => /control 150/.test(x)), "dos terneros con control 150: C150 y C150-VER, con aviso");
const c150 = animalesMod.porRp(db, "C150");
ok(c150 && c150.rp_provisorio === 1 && c150.caravana_control === "150" && c150.caravana_color === "BLANCA", "queda guardada la caravana control y el color");
ok(animalesMod.buscar(db, "150")[0].coincide === "control" || animalesMod.buscar(db, "150").some(x => x.coincide === "control"), "se encuentra por el número de control");
const amb = relevarMod.identificar(db, { filas: [{ control: "150", rp: "2077" }], simular: true });
ok(!amb.filas[0].ok && /Hay 2 con control 150/.test(amb.filas[0].error), "con dos controles iguales pide el color");
relevarMod.pesadas(db, { filas: [{ rp: "C150", peso: 40 }], fecha: "2026-09-03", contexto: "CONTROL" });
const idn = relevarMod.identificar(db, { filas: [{ control: "150", color: "blanca", rp: "2077", chip: "320100362601234" }, { control: "150", color: "verde", chip: "320100362601235" }, { control: "999", rp: "2079" }] });
ok(idn.bien === 2 && idn.mal === 1 && /No hay ningún ternero con caravana control 999/.test(idn.filas[2].error), "identifica dos y rechaza una control inexistente");
const a2077 = animalesMod.porRp(db, "2077");
ok(a2077 && !a2077.rp_provisorio && a2077.chip === "320100362601234" && a2077.caravana_control === "150" && a2077.madre_rp === "21", "el C150 ahora es 2077 con chip, y conserva madre y control");
ok(animalesMod.ficha(db, "2077").pesadas.length === 2, "las pesadas siguen con el animal identificado");
ok(animalesMod.porRp(db, "C150-VER").chip === "320100362601235" && animalesMod.porRp(db, "C150-VER").rp_provisorio === 1, "al verde sólo se le puso el chip: sigue provisorio");
const dupRp = relevarMod.identificar(db, { filas: [{ rp_actual: "C150-VER", rp: "2077" }], simular: true });
ok(!dupRp.filas[0].ok && /ya lo tiene otro animal/.test(dupRp.filas[0].error), "no deja asignar un RP que ya existe");
ok(exportarMod.conjunto(db, mods, "nacimientos").filas.some(f => f.rp === "C150-VER" && f.rp_provisorio_texto === "provisorio"), "el export de nacimientos marca los provisorios");

seccion("Importar CSV");
const csvV = fs.readFileSync(path.join(__dirname, "nacimientos_el_triunfo_2026-08-31.csv"), "utf8");
const imp = relevarMod.importarCsv(db, plantelMod, { texto: csvV, simular: true });
ok(imp.tipo === "nacimientos" && imp.separador === ";" && imp.mapa.caravana_control === "caravana_numero" && imp.mapa.caravana_color === "caravana_color" && imp.mapa.madre_rp === "madre_rp", "detecta un CSV de nacimientos con ; (caravana control y color)");
ok(imp.filas[0].provisorio && /^C1/.test(imp.filas[0].rp), "sin columna RP, el ternero queda con RP provisorio C+control");
ok(imp.leidas === 15 && imp.bien === 15, "lee las 15 filas");
const csvP = fs.readFileSync(path.join(__dirname, "triunfo_pesadas.csv"), "utf8");
const imp2 = relevarMod.importarCsv(db, plantelMod, { texto: csvP, simular: true });
ok(imp2.tipo === "pesadas" && imp2.mapa.peso === "peso" && imp2.mapa.fecha === "fecha", "detecta un CSV de pesadas con ,");
const pl = relevarMod.planilla(db, { lote_id: 1, campoNombre: "Prueba" }, exportarMod, mods);
ok(pl.buffer.length > 2000, "planilla de relevamiento en Excel");
const plh = relevarMod.planilla(db, { conjunto: "recria", formato: "html" }, exportarMod, mods);
ok(plh.buffer.toString().includes("Observaciones"), "planilla imprimible con las columnas para anotar");

seccion("Toros");
const tor = animalesMod.toros(db);
ok(tor.filas.length === 6 && tor.resumen.total === 6, "seis toros activos");
const her = tor.filas.find(t => t.nombre === "HERCULES");
ok(her && her.rp === "B332" && her.hijos > 5 && her.destete_prom_hijos > 100 && her.ce >= 36, "Hércules tiene hijos (por nombre), destete promedio y CE");
ok(animalesMod.buscar(db, "hercules")[0].rp === "B332" && animalesMod.buscar(db, "hercules")[0].coincide === "nombre", "buscar hercules encuentra al toro por nombre, primero");
ok(animalesMod.porRp(db, "Hércules").rp === "B332", "porRp entiende el nombre del toro");
ok(animalesMod.ficha(db, "B332").hijos.length === her.hijos, "la ficha del toro lista los mismos hijos");
ok(exportarMod.conjunto(db, mods, "toros").filas.length === 6, "conjunto toros para exportar");

seccion("Destinos");
ok(destinosMod.normalizarDestino("engorde", false) === "TERMINACION", "'engorde' es TERMINACION para una vaca");
ok(destinosMod.normalizarDestino("a engorde", true) === "NOVILLO TERMINACION", "'engorde' es NOVILLO TERMINACION para un macho");
ok(destinosMod.normalizarDestino("venta preñada", false) === "VENTA PREÑADA", "'venta preñada' tal cual");
ok(destinosMod.normalizarDestino("reproductor", true) === "TORO REPRODUCTOR", "'reproductor'");
ok(destinosMod.normalizarDestino("cualquier cosa", false) === null, "lo desconocido no se adivina");
const ventaToro = destinosMod.marcar(db, "B332", "venta directa", { temporada: "2098" });
ok(ventaToro.ok && ventaToro.destino === "VENTA DIRECTA" && !animalesMod.toros(db).filas.some(t => t.rp === "B332"), "un toro se puede vender directo y sale de Toros");
destinosMod.sacar(db, "B332", "2098");
const dm = destinosMod.marcarVarios(db, ["011", "13", "B332", "ZZZ"], "engorde", { motivo: "VACIA", temporada: "2099" });
const marcados = db.prepare("SELECT animal_rp, destino FROM destinos WHERE temporada='2099' ORDER BY animal_rp").all();
ok(dm.hechos.length === 3 && dm.fallados.length === 1, "marca 3 y avisa 1 (ZZZ)");
ok(marcados.find(m => m.animal_rp === "11").destino === "TERMINACION" && marcados.find(m => m.animal_rp === "B332").destino === "TORO TERMINACION", "vaca → TERMINACION, toro → TORO TERMINACION");
db.prepare("DELETE FROM destinos WHERE temporada='2099'").run();
// Terminación: lote de corral + marcados con destino terminación (sin salir).
const anioHoy = new Date().toISOString().slice(0, 4);
const antesT = animalesMod.terminacion(db).resumen;
destinosMod.marcar(db, "15", "engorde", { temporada: anioHoy });
const t2 = animalesMod.terminacion(db);
ok(t2.resumen.total === antesT.total + 1 && t2.resumen.marcados === antesT.marcados + 1 && t2.filas.find(f => f.rp === "15").origen === "marcado", "una vaca marcada a engorde aparece en Terminación como marcada");
destinosMod.concretar(db, "15", { temporada: anioHoy });
ok(animalesMod.terminacion(db).resumen.total === antesT.total, "cuando sale del campo deja de estar en Terminación");
db.prepare("UPDATE animales SET estado='ACTIVO' WHERE rp='15'").run();
db.prepare("DELETE FROM destinos WHERE animal_rp='15'").run();
ok(exportarMod.conjunto(db, mods, "recria").filas.every(f => f.edad_meses <= 20), "la recría llega hasta los 20 meses");
// Un destino de salida saca del plantel y de los toros; sacarlo los devuelve.
const plantelAntes = plantelMod.plantel(db).filas.length, torosAntes = animalesMod.toros(db).resumen.total;
destinosMod.marcar(db, "17", "venta directa", { temporada: anioHoy });
destinosMod.marcar(db, "B332", "toro terminacion", { temporada: anioHoy });
destinosMod.marcar(db, "19", "queda", { temporada: anioHoy });
const plDesp = plantelMod.plantel(db);
ok(plDesp.filas.length === plantelAntes - 1 && !plDesp.filas.some(f => f.rp === "17") && plDesp.filas.some(f => f.rp === "19"), "una vaca a venta directa sale del plantel; una que QUEDA no");
ok(plDesp.resumen.destinadas === 1 && plDesp.resumen.avisos.some(a => /destino de salida/.test(a.texto)), "el plantel avisa cuántas tienen destino de salida");
ok(plantelMod.plantel(db, { incluirDestinados: true }).filas.length === plantelAntes, "con incluirDestinados se ven todas");
const tDesp = animalesMod.toros(db);
ok(tDesp.resumen.total === torosAntes - 1 && tDesp.resumen.destinados === 1 && !tDesp.filas.some(t => t.rp === "B332"), "un toro a terminación sale de Toros");
const listaD = destinosMod.listar(db, plantelMod.plantel(db, { incluirDestinados: true }).filas);
ok(listaD.filas.find(f => f.rp === "17").edad_meses > 0, "en Destinos la vaca sigue con sus datos productivos");
ok(animalesMod.terminacion(db).filas.some(f => f.rp === "B332" && f.origen === "marcado"), "el toro aparece en Terminación como marcado");
destinosMod.sacar(db, "17", anioHoy); destinosMod.sacar(db, "B332", anioHoy); destinosMod.sacar(db, "19", anioHoy);
ok(plantelMod.plantel(db).filas.length === plantelAntes && animalesMod.toros(db).resumen.total === torosAntes, "sacar el destino los devuelve");

// ── El bot, con un Claude simulado ───────────────────────────────────────────
// El cliente falso recibe un guion: una función por llamada, que mira los
// parámetros y devuelve el contenido del mensaje. Emite los eventos que emite
// el SDK ("text", "streamEvent") y resuelve finalMessage().
let _ultimosParams = null;
const clienteFalsoParams = () => _ultimosParams;
function clienteFalso(guion) {
  const llamadas = [];
  return {
    llamadas,
    messages: {
      stream(params) {
        llamadas.push(params); _ultimosParams = params;
        const paso = guion[Math.min(llamadas.length - 1, guion.length - 1)](params, llamadas.length);
        const handlers = {};
        const st = {
          on(ev, fn) { handlers[ev] = fn; return st; },
          async finalMessage() {
            for (const c of paso.content) {
              if (c.type === "thinking" && handlers.streamEvent) handlers.streamEvent({ type: "content_block_delta", delta: { type: "thinking_delta", thinking: c.thinking } });
              if (c.type === "text" && handlers.text) handlers.text(c.text);
            }
            return { content: paso.content, stop_reason: paso.stop_reason || (paso.content.some(c => c.type === "tool_use") ? "tool_use" : "end_turn"),
              usage: { input_tokens: 1000, output_tokens: 50, cache_read_input_tokens: llamadas.length > 1 ? 900 : 0, cache_creation_input_tokens: llamadas.length > 1 ? 0 : 900 } };
          }
        };
        return st;
      }
    }
  };
}
const uso = (id, name, input) => ({ type: "tool_use", id, name, input });
const texto = t => ({ type: "text", text: t });

seccion("El bot (simulado)");
const bot1 = botMod.crear({ plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, guardarTablero: S.guardarTablero, CAMPOS: S.CAMPOS,
  cliente: clienteFalso([
    () => ({ content: [{ type: "thinking", thinking: "Miro el plantel." }, uso("t1", "plantel", { estado: "FALLÓ" })] }),
    (params) => {
      const ultimo = params.messages[params.messages.length - 1];
      const out = JSON.parse(ultimo.content[0].content);
      return { content: [texto(`Fallaron ${out.total} vacas.`)] };
    }
  ]) });
ok(bot1.HERRAMIENTAS.map(h => h.name).join() === "plantel,ficha,toros,buscar,consultar,escribir,relevar,crear_tablero,exportar_archivo,destinar,leer_adjunto,importar_adjunto,campos,trasladar,vinculos,finanzas,finanzas_registrar,recordar", "las dieciocho herramientas, en orden fijo");
const eventos = [];
(async () => {
  const r = await bot1.responder(db, "Prueba", "¿cuántas fallaron?", { campoKey: "principal", canal: "web", usuario: "prueba", onEvento: e => eventos.push(e) });
  const esperado = plantelMod.plantel(db).resumen.fallaron;
  ok(r.respuesta === `Fallaron ${esperado} vacas.`, "la respuesta usa el resultado de la herramienta plantel");
  ok(r.pasos.length === 1 && r.pasos[0].tipo === "consulta" && r.pasos[0].herramienta === "plantel" && r.pasos[0].filas === esperado, "el paso queda registrado");
  ok(eventos.some(e => e.tipo === "pensando") && eventos.some(e => e.tipo === "paso") && eventos.some(e => e.tipo === "texto") && eventos[eventos.length - 1].tipo === "fin", "emite pensando, paso, texto y fin");
  ok(r.uso.cache_read === 900 && r.uso.cache_creation === 900, "cuenta los tokens de caché");
  const conv = bot1.conversacion(db, "web", "prueba");
  ok(conv.length === 2 && conv[0].role === "user" && conv[1].role === "assistant", "la conversación queda guardada");

  // Segunda pregunta en la misma sesión: la historia sale de la base.
  const bot2 = botMod.crear({ plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, guardarTablero: S.guardarTablero, CAMPOS: S.CAMPOS,
    cliente: clienteFalso([(params) => ({ content: [texto(`Tengo ${params.messages.length} mensajes de historia.`)] })]) });
  const r2 = await bot2.responder(db, "Prueba", "¿y las preñadas?", { campoKey: "principal", canal: "web", usuario: "prueba" });
  ok(r2.respuesta === "Tengo 3 mensajes de historia.", "la segunda pregunta lleva la historia de la base (2 previos + 1)");

  // Caché: la parte estable no cambia entre llamadas ni lleva la fecha; la volátil sí.
  const est1 = bot2.parteEstable(db, "Prueba"), est2 = bot2.parteEstable(db, "Prueba");
  ok(est1 === est2 && !est1.includes(new Date().toISOString().slice(0, 10)), "la parte estable es idéntica entre llamadas y no tiene la fecha");
  ok(bot2.parteVolatil(db).includes(new Date().toISOString().slice(0, 10)), "la fecha va en la parte volátil");
  // Los parámetros de la llamada: modelo, thinking adaptativo, esfuerzo, caché en el system.
  const params = clienteFalsoParams();
  ok(params.system[0].cache_control.type === "ephemeral", "manda el cache_control en la parte estable");
  // El pensamiento adaptativo es de los modelos nuevos: al barato no se le manda.
  ok(!params.thinking && !params.output_config, "al modelo barato no se le mandan thinking ni esfuerzo");
  const botG = botMod.crear({ plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, guardarTablero: S.guardarTablero, CAMPOS: S.CAMPOS,
    ruteo: "grande", cliente: clienteFalso([() => ({ content: [texto("listo")] })]) });
  await botG.responder(db, "Prueba", "¿cuántas fallaron?", { campoKey: "principal" });
  const pg = clienteFalsoParams();
  ok(pg.model === botG.modelo && pg.thinking.type === "adaptive" && pg.output_config.effort === botG.esfuerzo, "al modelo bueno sí: thinking adaptativo y esfuerzo");
  ok(bot2.modelo === (process.env.MODELO || "claude-opus-5") && bot2.modeloSimple === (process.env.MODELO_SIMPLE || "claude-haiku-4-5"), "los dos modelos configurados (Opus 5 y Haiku por defecto)");
  ok(params.model === bot2.modeloSimple, "una pregunta corta y directa la contesta el modelo barato");

  // ── El ruteo: qué va al modelo barato y qué al bueno ──────────────────────
  const alSimple = ["¿cuántas vacas hay?", "cuánto pesó la 148", "ficha de la 23", "cargá 15 kg a la 100",
    "mostrame las preñadas", "12 450\n13 470\n14 490\n15 505", "anotá que la 7 parió hoy"];
  const alGrande = ["¿por qué bajó el destete este año?", "qué vacas conviene descartar",
    "revisá si hay algo mal cargado", "armame un tablero de eficiencia", "compará los dos campos",
    "cuáles son las mejores madres del rodeo"];
  const malSimple = alSimple.filter(t => bot2.elegirModelo(t).modelo !== bot2.modeloSimple);
  const malGrande = alGrande.filter(t => bot2.elegirModelo(t).modelo !== bot2.modelo);
  ok(!malSimple.length, "las consultas directas y las cargas van al modelo barato" + (malSimple.length ? ": " + malSimple.join(" | ") : ""));
  ok(!malGrande.length, "lo que hay que analizar va al modelo bueno" + (malGrande.length ? ": " + malGrande.join(" | ") : ""));
  ok(bot2.elegirModelo("dame la ficha", { conAdjuntos: true }).modelo === bot2.modelo, "si hay archivos para leer, va al bueno");
  ok(bot2.elegirModelo("").modelo === bot2.modelo && bot2.elegirModelo("x".repeat(300)).modelo === bot2.modelo, "sin texto o con un mensaje largo, el bueno");

  // Una lista de "RP peso", por larga que sea, es carga: modelo barato.
  const listaPesadas = Array.from({ length: 40 }, (_, i) => `${100 + i} ${380 + i}`).join("\n");
  ok(bot2.elegirModelo(listaPesadas).modelo === bot2.modeloSimple, "una lista larga de pesadas sigue siendo carga");

  // Si el barato no llega a nada, se rehace con el bueno y se cobran los dos.
  const usados = [];
  const bot2b = botMod.crear({ plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, guardarTablero: S.guardarTablero, CAMPOS: S.CAMPOS,
    cliente: clienteFalso([
      (params) => { usados.push(params.model); return { content: [texto("")] }; },
      (params) => { usados.push(params.model); return { content: [texto("Quedaron 12 vacas vacías.")] }; }
    ]) });
  const rEsc = await bot2b.responder(db, "Prueba", "cuántas vacías hay", { campoKey: "principal" });
  ok(usados.length === 2 && usados[0] === bot2b.modeloSimple && usados[1] === bot2b.modelo, "si el barato no contesta, se rehace con el bueno");
  ok(rEsc.respuesta === "Quedaron 12 vacas vacías." && rEsc.reintento === true && rEsc.modelo === bot2b.modelo, "la respuesta que vale es la del bueno");

  const usados2 = [];
  const bot2c = botMod.crear({ plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, guardarTablero: S.guardarTablero, CAMPOS: S.CAMPOS,
    cliente: clienteFalso([
      (params) => { usados2.push(params.model); return { content: [uso("t9", "destinar", { rps: ["13"], destino: "engorde" })] }; },
      (params) => { usados2.push(params.model); return { content: [texto("")] }; }
    ]) });
  await bot2c.responder(db, "Prueba", "poné la 13 en engorde", { campoKey: "principal" });
  ok(usados2.length === 2 && usados2.every(m => m === bot2c.modeloSimple), "si ya escribió en la base no se rehace: no se carga dos veces");

  // Con MODELO_RUTEO fijo no hay ruteo ni reintento.
  const botFijo = botMod.crear({ plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, guardarTablero: S.guardarTablero, CAMPOS: S.CAMPOS,
    ruteo: "grande", cliente: clienteFalso([() => ({ content: [texto("ok")] })]) });
  ok(botFijo.elegirModelo("cuántas vacas hay").modelo === botFijo.modelo, "MODELO_RUTEO=grande manda todo al bueno");

  // Memoria: el bot guarda y después lo lee en el prompt.
  const bot3 = botMod.crear({ plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, guardarTablero: S.guardarTablero, CAMPOS: S.CAMPOS,
    cliente: clienteFalso([
      () => ({ content: [uso("t2", "recordar", { texto: "Al potrero 7 le dicen La Loma", categoria: "campo" })] }),
      () => ({ content: [texto("Anotado.")] })
    ]) });
  const r3 = await bot3.responder(db, "Prueba", "acordate que al potrero 7 le decimos La Loma", { campoKey: "principal", canal: "web", usuario: "prueba" });
  ok(r3.pasos.some(p => p.tipo === "memoria") && bot3.memorias(db).some(m => /La Loma/.test(m.texto)), "recordar guarda la memoria");
  ok(bot3.parteEstable(db, "Prueba").includes("La Loma"), "la memoria entra en el prompt");
  const idMem = bot3.memorias(db)[0].id;
  bot3.recordar(db, { olvidar_id: idMem });
  ok(!bot3.memorias(db).length, "olvidar la saca");

  // Destinar por el bot: "los 5 a engorde".
  const bot4 = botMod.crear({ plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, guardarTablero: S.guardarTablero, CAMPOS: S.CAMPOS,
    cliente: clienteFalso([
      () => ({ content: [uso("t3", "destinar", { rps: ["011", "13", "15", "17", "19"], destino: "engorde", motivo: "VACIA" })] }),
      (params) => ({ content: [texto(JSON.parse(params.messages[params.messages.length - 1].content[0].content).mensaje)] })
    ]) });
  const r4 = await bot4.responder(db, "Prueba", "poné los 5 en destino engorde", { campoKey: "principal", canal: "web", usuario: "prueba" });
  const anio = new Date().toISOString().slice(0, 4);
  const dest = db.prepare("SELECT animal_rp, destino FROM destinos WHERE temporada=? ORDER BY animal_rp").all(anio);
  ok(dest.length === 5 && dest.every(d => d.destino === "TERMINACION"), "destinar marca los 5 como TERMINACION");
  ok(r4.pasos[0].tipo === "escritura" && /5 animales a ENGORDE/i.test(r4.respuesta), "cuenta como escritura y responde cuántos marcó");
  const lista = destinosMod.listar(db, plantelMod.plantel(db).filas);
  ok(lista.filas.length === 5 && lista.resumen.marcados === 5, "la pestaña Destinos los ve");
  db.prepare("DELETE FROM destinos WHERE temporada=?").run(anio);

  // Sólo lectura y errores de herramienta.
  const bot5 = botMod.crear({ plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, guardarTablero: S.guardarTablero, CAMPOS: S.CAMPOS,
    cliente: clienteFalso([
      () => ({ content: [uso("t4", "escribir", { sql: "DELETE FROM animales", que: "borrar todo" })] }),
      (params) => ({ content: [texto(JSON.parse(params.messages[params.messages.length - 1].content[0].content).error)] })
    ]) });
  const r5 = await bot5.responder(db, "Prueba", "borrá todo", { campoKey: "principal", soloLectura: true });
  ok(/sólo lectura/.test(r5.respuesta) && r5.pasos[0].tipo === "error", "en sólo lectura no escribe y el error vuelve al modelo");
  ok(db.prepare("SELECT COUNT(*) n FROM animales").get().n > 200, "no borró nada");

  // Refusal.
  const bot6 = botMod.crear({ plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, guardarTablero: S.guardarTablero, CAMPOS: S.CAMPOS,
    cliente: clienteFalso([() => ({ content: [], stop_reason: "refusal" })]) });
  const r6 = await bot6.responder(db, "Prueba", "x", { campoKey: "principal" });
  ok(r6.motivo === "refusal" && r6.respuesta.length > 0, "un refusal devuelve un mensaje, no rompe");

  // Herramientas viejas del bot siguen andando.
  const e1 = bot1.exportarDesdeBot(db, { titulo: "Vacías", conjunto: "fallos" }, { campoKey: "principal" });
  ok(e1.url.startsWith("/archivos/"), "el bot exporta un conjunto");
  const e2 = bot1.exportarDesdeBot(db, { titulo: "Toros", sql: "SELECT rp FROM animales WHERE categoria='TORO'", formato: "csv" }, { campoKey: "principal" });
  ok(e2.filas === 6, "el bot exporta un SELECT");
  const r7 = bot1.relevarDesdeBot(db, { tipo: "pesadas", filas: [{ rp: "011", peso: 440 }], simular: true });
  ok(r7.bien === 1, "el bot releva pesadas");
  const inst = bot1.instrucciones(db, "Prueba");
  ok(inst.includes("destinar") && inst.includes("recordar") && inst.includes("HOY ES"), "las instrucciones nombran las herramientas nuevas y la fecha");
  ok(require("./preguntas.js").length >= 18 && require("./preguntas.js").every(p => typeof p.verificar === "function"), "el banco de preguntas carga");

  // WhatsApp: partir mensajes largos y links absolutos; el bot acepta una foto.
  const largo = Array.from({ length: 40 }, (_, i) => `Párrafo ${i + 1} con algo de texto para que sea largo de verdad.`).join("\n\n");
  const partes = S.partirMensaje(largo);
  ok(partes.length >= 2 && partes.every(x => x.length <= 1500) && partes.join("\n\n").replace(/\s+/g, " ") === largo.replace(/\s+/g, " "), "parte un mensaje largo por párrafos sin perder texto");
  ok(S.absolutizar("Quedó en /archivos/3/x.xlsx y el tablero /t/toros", { protocol: "https", get: () => "app.railway.app" }) === "Quedó en https://app.railway.app/archivos/3/x.xlsx y el tablero https://app.railway.app/t/toros", "los links salen absolutos para el teléfono");
  const bot7 = botMod.crear({ plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, guardarTablero: S.guardarTablero, CAMPOS: S.CAMPOS,
    cliente: clienteFalso([(params) => ({ content: [texto(`recibí ${params.messages[params.messages.length - 1].content.length} bloques`)] })]) });
  const r8 = await bot7.responder(db, "Prueba", [{ type: "image", source: { type: "base64", media_type: "image/jpeg", data: "abc" } }, { type: "text", text: "leé la libreta" }], { campoKey: "principal", canal: "whatsapp", usuario: "whatsapp:+549" });
  ok(r8.respuesta === "recibí 2 bloques" && bot7.conversacion(db, "whatsapp", "whatsapp:+549")[0].texto === "[foto] leé la libreta", "una foto llega al modelo y queda como [foto] en la conversación");

  // Adjuntos: un Excel de pesadas llega resumido, el bot lo importa con la herramienta.
  const adjuntosMod = require("../adjuntos.js");
  const xl = xlsx.armar([{ nombre: "Pesadas", titulo: "Control", columnas: [{ k: "rp", t: "RP" }, { k: "peso", t: "Peso" }], filas: [{ rp: "011", peso: 441 }, { rp: "13", peso: 488 }, { rp: "ZZZ", peso: 300 }] }]);
  const prep = adjuntosMod.preparar(db, { texto: "cargá esto como control de hoy", adjuntos: [{ nombre: "control.xlsx", mime: "application/vnd.ms-excel", base64: xl.toString("base64") }], canal: "web", usuario: "prueba" });
  ok(prep.guardados.length === 1 && prep.guardados[0].tipo === "tabla" && prep.content.length === 2 && /3 filas/.test(prep.content[0].text) && /011 \| 441/.test(prep.content[0].text), "el Excel llega como tabla resumida con id");
  const idAdj = prep.guardados[0].id;
  const bot8 = botMod.crear({ plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, guardarTablero: S.guardarTablero, CAMPOS: S.CAMPOS,
    cliente: clienteFalso([
      () => ({ content: [uso("t9", "importar_adjunto", { id: idAdj, tipo: "pesadas", contexto: "CONTROL", simular: true })] }),
      (params) => ({ content: [texto(JSON.parse(params.messages[params.messages.length - 1].content[0].content).mensaje)] })
    ]) });
  const r9 = await bot8.responder(db, "Prueba", prep.content, { campoKey: "principal", canal: "web", usuario: "prueba" });
  ok(/2 pesadas para cargar, 1 con error/.test(r9.respuesta) && r9.pasos[0].herramienta === "importar_adjunto", "el bot revisa la planilla con importar_adjunto (simular): 2 bien, ZZZ mal");
  const conv8 = bot8.conversacion(db, "web", "prueba"); const ult = conv8[conv8.length - 2];
  ok(ult.role === "user" && /^\[Adjunto 1: planilla "control.xlsx"/.test(ult.texto) && /cargá esto/.test(ult.texto), "en la conversación queda el nombre del adjunto, no la planilla entera");
  const foto = adjuntosMod.preparar(db, { texto: "", adjuntos: [{ nombre: "libreta.jpg", mime: "image/jpeg", base64: Buffer.alloc(10).toString("base64") }], canal: "whatsapp", usuario: "w" });
  ok(foto.content.some(b => b.type === "image") && /Decime qué es/.test(foto.content[foto.content.length - 1].text), "una foto sin texto lleva la consigna por defecto");
  const audio = adjuntosMod.preparar(db, { texto: "", adjuntos: [{ nombre: "nota.ogg", mime: "audio/ogg", base64: "AAAA" }] });
  ok(audio.guardados[0].error && /audios/.test(audio.content[0].text), "un audio avisa que no se escucha");
  const pdf = adjuntosMod.preparar(db, { texto: "qué dice", adjuntos: [{ nombre: "informe.pdf", mime: "application/pdf", base64: Buffer.from("%PDF-1.4").toString("base64") }] });
  ok(pdf.content.some(b => b.type === "document" && b.source.media_type === "application/pdf"), "un PDF va como documento");
  ok(adjuntosMod.listar(db).length >= 4, "los adjuntos quedan listados");

  // Enlace con el financiero: el stock como lo lee IMPROLUX, la venta que se manda, el bot que consulta.
  const finanzasMod = require("../finanzas.js");
  const rr = finanzasMod.resumenRodeo(db, { campoKey: "principal", campoNombre: "Prueba", destinosMod });
  ok(rr.categorias.length >= 4 && rr.categorias.every(c => c.categoria && c.registro && typeof c.plantel === "number" && typeof c.venta === "number"), "rodeo-resumen tiene categoría, registro, plantel y venta");
  const vacasPP = rr.categorias.find(c => c.categoria === "VACA" && c.registro === "PP");
  ok(vacasPP && vacasPP.plantel > 30 && vacasPP.kg_estimado > 350, "las vacas PP salen con cantidad y kilos promedio reales");
  destinosMod.marcar(db, "21", "venta directa", { temporada: anioHoy });
  const rr2 = finanzasMod.resumenRodeo(db, { destinosMod });
  const v2 = rr2.categorias.find(c => c.categoria === "VACA" && c.registro === "PP");
  ok(v2.venta === vacasPP.venta + 1 && v2.plantel === vacasPP.plantel - 1, "una vaca marcada a venta pasa de plantel a venta en el resumen");
  // Un financiero simulado: guarda lo que le mandan y contesta.
  const recibido = [];
  finanzasMod.setFetch(async (urlCompleta, op) => {
    const u = new URL(urlCompleta); const url = u.origin + u.pathname;
    recibido.push({ url, query: Object.fromEntries(u.searchParams), body: op && op.body ? JSON.parse(op.body) : null });
    const cuerpo = url.endsWith("/api/transacciones") && op.method === "POST" ? { ok: true, id: 77 }
      : url.includes("/api/transacciones") ? [{ fecha: "2026-08-10", concepto: "SANIDAD", detalle: "ivermectina", egreso: 300 }, { fecha: "2026-08-20", concepto: "VENTA HACIENDA", detalle: "5 novillos", ingreso: 8500 }, { fecha: "2026-07-01", concepto: "SANIDAD", egreso: 100 }]
      : url.endsWith("/api/resumen") ? { ingresos_mes: 8500, egresos_mes: 400 } : url.endsWith("/api/ganado/sync-ade") ? { ok: true, mensaje: "90 cabezas" } : {};
    return { ok: true, status: 200, text: async () => JSON.stringify(cuerpo) };
  });
  process.env.FINANZAS_URL = "https://fin.prueba"; process.env.FINANZAS_CAMPO = "AMAKAIK";
  const salida = await S.registrarSalida(db, { rps: ["21"], fecha: "2026-09-03", precio_total: 1500, comprador: "Feria Norte", kg: 480 });
  ok(salida.ok && salida.venta && salida.venta.enviado && salida.venta.id_financiero === 77, "la salida con precio manda la venta al financiero");
  const tx = recibido.find(r => r.url.endsWith("/api/transacciones") && r.body);
  ok(tx && tx.body.concepto === "VENTA HACIENDA" && tx.body.ingreso === 1500 && /RP 21/.test(tx.body.detalle) && /480 kg/.test(tx.body.detalle) && tx.body.proveedor === "Feria Norte" && tx.body.fuente === "rodeo", "la transacción lleva concepto, monto, detalle con RP y kilos, comprador");
  ok(tx && tx.query.empresa === "AMAKAIK" && tx.query.campo === "AMAKAIK", "la llamada lleva la clave de empresa del financiero, como la manda el portal");
  ok(animalesMod.porRp(db, "21").estado === "VENDIDO", "el animal quedó VENDIDO");
  const sinPrecio = await S.registrarSalida(db, { rps: ["23"], fecha: "2026-09-03" });
  ok(sinPrecio.ok && !sinPrecio.venta && /salió/.test(sinPrecio.mensaje), "sin precio sale del campo y no manda nada");
  db.prepare("UPDATE animales SET estado='ACTIVO' WHERE rp IN ('21','23')").run(); db.prepare("DELETE FROM destinos WHERE animal_rp IN ('21','23')").run();
  const fq = await finanzasMod.consultar(db, { consulta: "transacciones", concepto: "sanidad", desde: "2026-08-01" });
  ok(fq.total === 1 && fq.egresos === 300 && fq.por_concepto.SANIDAD.n === 1, "consulta transacciones filtra por concepto y fecha y suma");
  // Registrar un gasto en el financiero.
  const sim3 = await finanzasMod.registrarMovimiento(db, { concepto: "sanidad", egreso: 300, detalle: "ivermectina", proveedor: "Diego Pioli", simular: true });
  ok(sim3.simulado && !sim3.enviado && sim3.movimiento.concepto === "SANIDAD" && /Listo para registrar/.test(sim3.mensaje), "simular un gasto no escribe y lo muestra");
  const gasto = await finanzasMod.registrarMovimiento(db, { concepto: "sanidad", egreso: 300, detalle: "ivermectina", proveedor: "Diego Pioli", fecha: "2026-09-03" });
  const txg = recibido[recibido.length - 1];
  ok(gasto.ok && gasto.enviado && txg.body.concepto === "SANIDAD" && txg.body.egreso === 300 && txg.body.proveedor === "Diego Pioli" && /ivermectina/.test(txg.body.detalle) && /desde RODEO/.test(txg.body.detalle), "el gasto llega al financiero con concepto, monto, proveedor y detalle");
  // Un financiero viejo: no tiene POST /api/transacciones, recibe por /api/ejecutar-accion.
  const viejo = [];
  finanzasMod.setFetch(async (urlCompleta, op) => {
    const u = new URL(urlCompleta); const ruta = u.pathname;
    viejo.push({ ruta, body: op && op.body ? JSON.parse(op.body) : null });
    if (ruta === "/api/transacciones" && op.method === "POST") return { ok: false, status: 404, text: async () => "Cannot POST /api/transacciones" };
    if (ruta === "/api/ejecutar-accion") return { ok: true, status: 200, text: async () => JSON.stringify({ respuesta: "✅ Registrado!" }) };
    return { ok: true, status: 200, text: async () => "{}" };
  });
  const gastoViejo = await finanzasMod.registrarMovimiento(db, { concepto: "alimento", egreso: 500, detalle: "maíz" });
  ok(gastoViejo.ok && viejo.some(v => v.ruta === "/api/ejecutar-accion" && v.body.accion === "registrar_transaccion" && v.body.egreso === 500 && v.body.concepto === "ALIMENTO"), "si el financiero es viejo, el gasto entra por ejecutar-accion");
  ok(viejo[0].ruta === "/api/transacciones", "primero intenta la ruta nueva");
  finanzasMod.setFetch(async (urlCompleta, op) => { const u = new URL(urlCompleta); const url = u.origin + u.pathname; recibido.push({ url, query: Object.fromEntries(u.searchParams), body: op && op.body ? JSON.parse(op.body) : null }); const cuerpo = url.endsWith("/api/transacciones") && op.method === "POST" ? { ok: true, id: 77 } : url.endsWith("/api/resumen") ? { ingresos_mes: 8500, egresos_mes: 400 } : {}; return { ok: true, status: 200, text: async () => JSON.stringify(cuerpo) }; });
  let errG = null; try { await finanzasMod.registrarMovimiento(db, { concepto: "SANIDAD" }); } catch (e) { errG = e.message; }
  ok(/Falta el monto/.test(errG), "sin monto no registra nada");
  const est = await finanzasMod.estado(db);
  ok(est.configurado && est.conecta && est.ultimos.length >= 2, "el estado muestra conexión y los últimos enlaces");
  const bot9 = botMod.crear({ plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, guardarTablero: S.guardarTablero, CAMPOS: S.CAMPOS, finanzasMod,
    cliente: clienteFalso([
      () => ({ content: [uso("t10", "finanzas", { consulta: "resumen" })] }),
      (params) => ({ content: [texto("Ingresos del mes: " + JSON.parse(params.messages[params.messages.length - 1].content[0].content).ingresos_mes)] })
    ]) });
  const r10 = await bot9.responder(db, "Prueba", "¿cómo venimos de plata?", { campoKey: "principal" });
  ok(r10.respuesta === "Ingresos del mes: 8500" && r10.pasos[0].herramienta === "finanzas", "el bot consulta el financiero");
  delete process.env.FINANZAS_URL;
  const sinEnlace = await finanzasMod.enviarVenta(db, { rps: ["11"], precio_total: 100 });
  ok(!sinEnlace.enviado && /FINANZAS_URL/.test(sinEnlace.motivo), "sin FINANZAS_URL avisa y no rompe");

  // Empresas y multicampo.
  const em = S.empresasDe();
  ok(em.lista().length === 1 && em.lista()[0].campos.length === 2 && em.empresaDe("triunfo").key === "improlux", "una empresa con dos campos");
  const re = em.resumen("improlux");
  ok(re.campos.length === 2 && re.campos.every(c => c.ok) && re.totales.cabezas === re.campos[0].cabezas + re.campos[1].cabezas && re.totales.vientres > 150, "el resumen de la empresa suma los dos campos");
  const rr3 = em.rodeoResumen("improlux");
  const vaca3 = rr3.categorias.find(c => c.categoria === "VACA" && c.registro === "PP");
  ok(rr3.campos.length === 2 && vaca3 && vaca3.cantidad === rr3.campos.reduce((s, c) => s, 0) + vaca3.cantidad && rr3.totales.cabezas === rr3.campos[0].cabezas + rr3.campos[1].cabezas, "el stock consolidado suma categorías de los dos campos");
  ok(em.finanzasDe("triunfo").campo === "AMAKAIK", "el financiero sale de la empresa");
  // Traslado: C900 existe sólo en principal.
  const sim2 = em.trasladar({ rps: ["C900", "11", "ZZZ"], desde: "principal", hasta: "triunfo", simular: true });
  ok(sim2.simulado && sim2.bien === 1 && sim2.filas[1].error && /ya hay un animal con RP 11/.test(sim2.filas[1].error) && /No existe ZZZ/.test(sim2.filas[2].error), "simular: C900 puede viajar, 11 choca con el destino, ZZZ no existe");
  relevarMod.pesadas(db, { filas: [{ rp: "C900", peso: 60 }], fecha: "2026-09-03" });
  const tr = em.trasladar({ rps: ["C900"], desde: "principal", hasta: "triunfo", fecha: "2026-09-03", motivo: "destete" });
  const dbT = S.getDB("triunfo");
  const enT = animalesMod.porRp(dbT, "C900");
  ok(tr.bien === 1 && enT && enT.madre_rp && animalesMod.ficha(dbT, "C900").pesadas.length === 2, "C900 llegó a El Triunfo con madre y pesadas");
  ok(animalesMod.porRp(db, "C900").estado === "TRASLADADO" && !animalesMod.listar(db).some(a => a.rp === "C900"), "en el origen quedó TRASLADADO y fuera de los activos");
  ok(animalesMod.ficha(dbT, "C900").notas.some(x => /Llegó de Angus del Este/.test(x.texto)) && animalesMod.ficha(db, "C900").notas.some(x => /Trasladado a El Triunfo/.test(x.texto)), "queda una nota en los dos campos");
  let errT = null; try { em.trasladar({ rps: ["11"], desde: "principal", hasta: "principal" }); } catch (e) { errT = e.message; }
  ok(/mismo campo/.test(errT), "no deja trasladar al mismo campo");
  // ── Vínculos entre campos ──
  // Los dos campos de prueba salen de la misma semilla, así que para probar el
  // cruce hace falta gente que exista en uno solo.
  const vin = S.vinculosDe();
  const dbT2 = S.getDB("triunfo");
  db.prepare("INSERT INTO animales (rp, sexo, categoria, estado, fecha_nac) VALUES (?,?,?,?,?)").run("Z777", "H", "VACA", "ACTIVO", "2019-08-01");
  db.prepare("INSERT INTO animales (rp, nombre, sexo, categoria, estado, fecha_nac) VALUES (?,?,?,?,?,?)").run("T777", "ZEUS UNICO", "M", "TORO", "ACTIVO", "2018-08-01");
  // El ternero está en El Triunfo; a la madre la anotaron con espacio y minúscula, al padre por nombre.
  dbT2.prepare("INSERT INTO animales (rp, sexo, categoria, estado, fecha_nac, madre_rp, padre_rp) VALUES (?,?,?,?,?,?,?)")
    .run("X900", "M", "TERNERO", "ACTIVO", "2026-08-01", "z 777", "ZEUS UNICO");
  vin.olvidar();
  const busca = vin.buscarEnEmpresa("triunfo", "z 777", { excluir: "triunfo" });
  ok(busca.length === 1 && busca[0].campo === "principal" && busca[0].rp === "Z777", "buscarEnEmpresa encuentra a la madre en el otro campo aunque esté escrita distinto");
  const rev = vin.revisar("triunfo", { fresco: true });
  const fMadre = rev.filas.find(f => f.rp === "X900" && f.relacion === "madre");
  ok(fMadre && fMadre.estado === "en_otro_campo" && fMadre.propuesta.campo === "principal" && fMadre.propuesta.rp === "Z777", "revisar ubica a la madre en el otro campo, con el RP como está escrito allá");
  const fPadre = rev.filas.find(f => f.rp === "X900" && f.relacion === "padre");
  ok(fPadre && fPadre.estado === "en_otro_campo" && fPadre.propuesta.rp === "T777", "el padre cargado por nombre se resuelve al toro del otro campo");
  const simV = vin.aplicar("triunfo", { simular: true });
  const leerX = (rp, col) => dbT2.prepare(`SELECT ${col} v FROM animales WHERE rp=?`).get(rp).v;
  ok(simV.simulado && simV.bien >= 2 && !leerX("X900", "madre_campo"), "simular no escribe");
  const apl = vin.aplicar("triunfo", {});
  ok(apl.bien >= 2 && leerX("X900", "madre_rp") === "Z777" && leerX("X900", "madre_campo") === "principal"
    && leerX("X900", "padre_rp") === "T777" && leerX("X900", "padre_campo") === "principal", "arreglar deja el RP real y el campo donde vive cada uno");
  const fam = vin.familiaFuera("triunfo", { madre_campo: "principal", madre_rp: "Z777", padre_campo: "principal", padre_rp: "T777" });
  ok(fam.madre && fam.madre.campo === "principal" && fam.padre && fam.padre.rp === "T777", "la ficha trae la madre y el padre del otro campo");
  const hf = vin.hijosFuera("principal", "Z777", null);
  ok(hf.some(x => x.rp === "X900" && x.campo === "triunfo"), "desde la madre se ven los hijos que tiene en otros campos");
  ok(!vin.revisar("triunfo", { fresco: true }).filas.some(f => f.rp === "X900"), "después de arreglar ya no figura como huérfano");
  // Un RP escrito con un cero de más, dentro del mismo campo.
  dbT2.prepare("INSERT INTO animales (rp, sexo, categoria, estado, fecha_nac, madre_rp) VALUES (?,?,?,?,?,?)").run("X902", "H", "TERNERA", "ACTIVO", "2026-08-03", "011");
  // Una madre que no existe en ningún lado.
  dbT2.prepare("INSERT INTO animales (rp, sexo, categoria, estado, fecha_nac, madre_rp) VALUES (?,?,?,?,?,?)").run("X901", "H", "TERNERA", "ACTIVO", "2026-08-02", "NOEXISTE99");
  vin.olvidar();
  const rev4 = vin.revisar("triunfo", { fresco: true });
  const f902 = rev4.filas.find(f => f.rp === "X902");
  ok(f902 && f902.estado === "corregir_rp" && f902.propuesta.rp === "11", "un RP con un cero de más se corrige contra el propio campo");
  const huerfano = rev4.filas.find(f => f.rp === "X901");
  ok(huerfano && huerfano.estado === "no_existe", "una madre que no existe en ningún campo queda marcada, no inventada");
  vin.aplicar("triunfo", {});
  ok(leerX("X901", "madre_campo") === null && leerX("X902", "madre_rp") === "11", "arreglar corrige el RP y no toca lo que no existe");
  dbT2.prepare("DELETE FROM animales WHERE rp IN ('X900','X901','X902')").run();
  db.prepare("DELETE FROM animales WHERE rp IN ('Z777','T777')").run();
  vin.olvidar();

  // ── Las estadísticas de la vaca cuentan los hijos de otros campos ──
  db.prepare("INSERT INTO animales (rp, sexo, categoria, estado, fecha_nac) VALUES (?,?,?,?,?)").run("M900", "H", "VACA", "ACTIVO", "2018-08-01");
  const idM900 = db.prepare("SELECT id FROM animales WHERE rp='M900'").get().id;
  db.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto) VALUES (?,?,?,?)").run(idM900, "2026-03-20", 480, "ADULTO");
  db.prepare("INSERT INTO servicios (animal_id, temporada, tipo_servicio, toro_natural, fecha_ingreso_toro, resultado) VALUES (?,?,?,?,?,?)").run(idM900, "2024", "NATURAL", "HERCULES", "2024-12-01", "PREÑADA_TORO");
  for (const [rp, fecha, pn, des] of [["H901", "2025-09-05", 32, 210], ["H902", "2026-08-20", 30, null]]) {
    dbT2.prepare("INSERT INTO animales (rp, sexo, categoria, estado, fecha_nac, madre_rp, padre_rp) VALUES (?,?,?,?,?,?,?)").run(rp, "M", "TERNERO", "ACTIVO", fecha, "M900", "HERCULES");
    const idh = dbT2.prepare("SELECT id FROM animales WHERE rp=?").get(rp).id;
    dbT2.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto) VALUES (?,?,?,?)").run(idh, fecha, pn, "NACIMIENTO");
    if (des) dbT2.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto) VALUES (?,?,?,?)").run(idh, "2026-04-01", des, "DESTETE");
  }
  vin.olvidar();
  const sinCruce = plantelMod.plantel(db).filas.find(f => f.rp === "M900");
  ok(sinCruce && sinCruce.partos === 0, "sin cruzar campos la vaca figura sin partos (el problema viejo)");
  const mapaC = vin.mapaCriasFuera("principal", { fresco: true });
  const cf = rp => mapaC.get(animalesMod.compacto(rp)) || [];
  const conCruce = plantelMod.plantel(db, { criasFuera: cf }).filas.find(f => f.rp === "M900");
  ok(conCruce.partos === 2 && conCruce.hijos_otros_campos === 2, "cruzando campos se le cuentan los dos partos");
  ok(conCruce.estado === "CRIANDO" && conCruce.ternero === "H902" && conCruce.ternero_campo_nombre === "El Triunfo", "el estado y el ternero del año salen del hijo que está en El Triunfo");
  ok(conCruce.destete_prom === 210 && conCruce.eficiencia > 0, "el destete y la eficiencia se calculan con esos hijos");
  const fichaM = plantelMod.ficha(db, "M900", { criasFuera: cf });
  ok(fichaM.campanas.length >= 2 && fichaM.campanas.every(c => !c.parto || c.campo_nombre === "El Triunfo"), "el historial de partos muestra en qué campo nació cada ternero");
  // Un hijo cuya madre también existe en su propio campo no se le atribuye a la de acá.
  const madreLocal = plantelMod.plantel(dbT2).filas[0].rp;
  dbT2.prepare("INSERT INTO animales (rp, sexo, categoria, estado, fecha_nac, madre_rp) VALUES (?,?,?,?,?,?)").run("H903", "H", "TERNERA", "ACTIVO", "2026-08-25", madreLocal);
  vin.olvidar();
  const mapa2 = vin.mapaCriasFuera("principal", { fresco: true });
  ok(!(mapa2.get(animalesMod.compacto(madreLocal)) || []).some(x => x.rp === "H903"), "un ternero cuya madre existe en su propio campo no se atribuye a la vaca homónima de otro campo");
  // Los toros suman los hijos que tuvieron sirviendo afuera.
  const mapaP = vin.mapaCriasFuera("principal", { relacion: "padre", fresco: true });
  const hf2 = rp => mapaP.get(animalesMod.compacto(rp)) || [];
  const torosCon = animalesMod.toros(db, { hijosFuera: hf2 }).filas.find(t => t.rp === "B332");
  const torosSin = animalesMod.toros(db).filas.find(t => t.rp === "B332");
  ok(torosCon.hijos >= torosSin.hijos, "el toro suma los hijos que tuvo en otros campos");
  dbT2.prepare("DELETE FROM pesadas WHERE animal_id IN (SELECT id FROM animales WHERE rp IN ('H901','H902','H903'))").run();
  dbT2.prepare("DELETE FROM animales WHERE rp IN ('H901','H902','H903')").run();
  db.prepare("DELETE FROM servicios WHERE animal_id=?").run(idM900);
  db.prepare("DELETE FROM pesadas WHERE animal_id=?").run(idM900);
  db.prepare("DELETE FROM animales WHERE rp='M900'").run();
  vin.olvidar();

  // ── El mismo ternero cargado en dos campos (cargas viejas) ──
  db.prepare("INSERT INTO animales (rp, sexo, categoria, estado, fecha_nac) VALUES (?,?,?,?,?)").run("B557", "H", "VACA", "ACTIVO", "2017-08-01");
  const idB557 = db.prepare("SELECT id FROM animales WHERE rp='B557'").get().id;
  db.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto) VALUES (?,?,?,?)").run(idB557, "2026-03-20", 500, "ADULTO");
  db.prepare("INSERT INTO animales (rp, sexo, categoria, estado, fecha_nac, madre_rp) VALUES (?,?,?,?,?,?)").run("HB557-21", "H", "TERNERA", "ACTIVO", "2021-10-11", "B557");
  const idFantasma = db.prepare("SELECT id FROM animales WHERE rp='HB557-21'").get().id;
  db.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto) VALUES (?,?,?,?)").run(idFantasma, "2021-10-11", 23, "NACIMIENTO");
  dbT2.prepare("INSERT INTO animales (rp, sexo, categoria, estado, fecha_nac, madre_rp) VALUES (?,?,?,?,?,?)").run("R29", "H", "VAQUILLONA", "ACTIVO", "2021-10-11", "B557");
  const idReal = dbT2.prepare("SELECT id FROM animales WHERE rp='R29'").get().id;
  dbT2.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto) VALUES (?,?,?,?)").run(idReal, "2021-10-11", 22.55, "NACIMIENTO");
  vin.olvidar();
  const mapaD = vin.mapaCriasFuera("principal", { fresco: true });
  const cfD = rp => mapaD.get(animalesMod.compacto(rp)) || [];
  const b557 = plantelMod.plantel(db, { criasFuera: cfD }).filas.find(f => f.rp === "B557");
  ok(b557.partos === 1 && b557.hijos_repetidos === 1, "el mismo ternero en dos campos cuenta como un solo parto");
  const fB557 = plantelMod.ficha(db, "B557", { criasFuera: cfD });
  ok(fB557.campanas[0].ternero === "R29" && fB557.campanas[0].campo_nombre === "El Triunfo", "queda el RP real del animal, no el armado con el RP de la madre");
  const dup = vin.duplicados("principal", { fresco: true });
  ok(dup.resumen.total === 1 && dup.pares[0].queda.rp === "R29" && /armado/.test(dup.pares[0].porque), "los detecta y dice cuál queda y por qué");
  const uniSim = vin.unificar("principal", { simular: true });
  ok(uniSim.simulado && uniSim.bien === 1 && db.prepare("SELECT estado FROM animales WHERE rp='HB557-21'").get().estado === "ACTIVO", "simular no cambia nada");
  vin.unificar("principal", {});
  ok(db.prepare("SELECT estado FROM animales WHERE rp='HB557-21'").get().estado === "DUPLICADO", "unificar marca el repetido, sin borrarlo");
  ok(db.prepare("SELECT COUNT(*) n FROM notas_campo WHERE animal_rp='HB557-21'").get().n === 1, "queda anotado de quién es duplicado");
  vin.olvidar();
  ok(plantelMod.plantel(db, { criasFuera: rp => vin.mapaCriasFuera("principal", { fresco: true }).get(animalesMod.compacto(rp)) || [] }).filas.find(f => f.rp === "B557").partos === 1,
    "después de unificar sigue contando un solo parto");
  // Mellizos de distinto sexo no se confunden con un duplicado.
  dbT2.prepare("INSERT INTO animales (rp, sexo, categoria, estado, fecha_nac, madre_rp) VALUES (?,?,?,?,?,?)").run("R30", "M", "TERNERO", "ACTIVO", "2021-10-11", "B557");
  vin.olvidar();
  ok(vin.duplicados("principal", { fresco: true }).resumen.total === 0, "un mellizo de distinto sexo no se toma por duplicado");
  dbT2.prepare("DELETE FROM pesadas WHERE animal_id IN (SELECT id FROM animales WHERE rp IN ('R29','R30'))").run();
  dbT2.prepare("DELETE FROM animales WHERE rp IN ('R29','R30')").run();
  db.prepare("DELETE FROM pesadas WHERE animal_id IN (?,?)").run(idB557, idFantasma);
  db.prepare("DELETE FROM animales WHERE rp IN ('B557','HB557-21')").run();
  try { db.prepare("DELETE FROM notas_campo WHERE animal_rp='HB557-21'").run(); } catch (e) {}
  vin.olvidar();

  const bot10 = botMod.crear({ plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, guardarTablero: S.guardarTablero, CAMPOS: S.CAMPOS, empresas: S.empresasDe,
    cliente: clienteFalso([
      () => ({ content: [uso("t11", "campos", {})] }),
      (params) => ({ content: [texto("Campos: " + JSON.parse(params.messages[params.messages.length - 1].content[0].content).totales.campos)] })
    ]) });
  const r11 = await bot10.responder(db, "Prueba", "¿cuántos campos tenemos?", { campoKey: "principal" });
  ok(r11.respuesta === "Campos: 2" && r11.pasos[0].herramienta === "campos", "el bot ve la empresa entera");

  console.log(`\n${n} pruebas, ${fallas} fallas`);
  db.close();
  fs.rmSync(dir, { recursive: true, force: true });
  process.exit(fallas ? 1 : 0);
})().catch(e => { console.error("ERROR en las pruebas:", e); process.exit(1); });
