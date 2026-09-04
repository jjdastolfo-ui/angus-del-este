// ─────────────────────────────────────────────────────────────────────────────
// RODEO — servidor
//
// Tres cosas:
//
//   1. Un bot que es Claude con acceso real a la base. No responde con textos
//      armados: consulta, razona y contesta. Si algo no cuadra, lo dice.
//   2. Los datos para el tablero, con todo lo de cada animal.
//   3. Entrar y sacar datos sin fricción: buscar cualquier animal como se lo
//      nombra en la manga, relevar pesadas/sanidad/nacimientos pegando lo que
//      se anotó, y bajar cualquier tabla como Excel, CSV o página imprimible.
//
// Lo que NO hace: calcular estados con reglas fijas y pasárselos masticados al
// bot. Eso fue lo que falló antes — cada regla nueva tapaba un caso y destapaba
// otro. Acá el bot ve los datos y saca sus conclusiones.
// ─────────────────────────────────────────────────────────────────────────────

const express = require("express");
const Database = require("better-sqlite3");
const path = require("path");
const fs = require("fs");

const app = express();
app.use(express.json({ limit: "25mb" }));
app.use(express.urlencoded({ extended: true, limit: "25mb" }));
app.set("trust proxy", true);   // Railway está detrás de un proxy: así se sabe el https y el host reales
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  res.header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

const VERSION = "rodeo-1.2";
const PORT = process.env.PORT || 3001;
const DB_DIR = process.env.DB_DIR || (process.env.DB_PATH && !/\.db$/.test(process.env.DB_PATH) ? process.env.DB_PATH : null) || "/data";
const plantelMod = require("./plantel.js");
let destinosMod; try { destinosMod = require("./destinos.js"); } catch (e) { console.log("destinos.js no disponible:", e.message); }
const animalesMod = require("./animales.js");   // buscar y ficha de cualquier animal
const exportarMod = require("./exportar.js");   // Excel, CSV, imprimible, archivos del bot
const relevarMod = require("./relevar.js");     // carga de campo e importación de planillas
const botMod = require("./bot.js");             // el bot: Claude con la base en la mano
const adjuntosMod = require("./adjuntos.js");   // fotos, PDF, Excel, CSV, Word que le mandan al bot
const finanzasMod = require("./finanzas.js");   // el enlace con IMPROLUX / VIDELA
const empresasMod = require("./empresas.js");   // una empresa: sus campos y su financiero
const vinculosMod = require("./vinculos.js");   // cruzar madres, padres e hijos entre campos
const mods = () => ({ plantelMod, animalesMod, destinosMod });

// ── CAMPOS ───────────────────────────────────────────────────────────────────

// Si CAMPOS no es un JSON válido (comillas curvas al pegar, un salto de línea de
// más), el servidor igual arranca con el campo por defecto y lo dice en /api/salud,
// en vez de caerse sin explicar nada.
const ERRORES_CONFIG = [];
function leerJson(nombre, porDefecto) {
  const crudo = process.env[nombre];
  if (!crudo || !crudo.trim()) return porDefecto;
  // Comillas curvas o comillas simples: se corrigen solas y se avisa.
  const limpio = crudo.trim().replace(/[\u201C\u201D\u201E\u2033]/g, "\"").replace(/[\u2018\u2019]/g, "\"").replace(/^\x27|\x27$/g, "").replace(/^"(\{.*\})"$/s, "$1");
  try {
    const v = JSON.parse(limpio);
    if (limpio !== crudo.trim()) ERRORES_CONFIG.push(`${nombre}: tenía comillas raras, se corrigieron solas (mejor pegarlo limpio)`);
    return v;
  } catch (e) {
    ERRORES_CONFIG.push(`${nombre} no es un JSON válido (${e.message}). Se usa el valor por defecto. Revisá comillas y llaves en Railway → Variables → Raw Editor.`);
    console.error("CONFIG:", ERRORES_CONFIG[ERRORES_CONFIG.length - 1]);
    return porDefecto;
  }
}
const CAMPOS = leerJson("CAMPOS", { principal: { nombre: "Angus del Este", empresa: "improlux" } });
if (process.env.EMPRESAS) leerJson("EMPRESAS", null);   // sólo para validar y avisar; empresas.js lo vuelve a leer
// El portal RODEO manda org=<slug> de la organización. ORGANIZACIONES dice a qué
// empresa corresponde cada slug: {"cabana-amakaik":"gullo","angus-del-este":"improlux"}.
// Si no está, se adivina por parecido con el nombre o la clave de la empresa.
const ORGANIZACIONES = leerJson("ORGANIZACIONES", {});
function empresaDeOrg(slug) {
  if (!slug) return null;
  const em = empresasDe();
  if (ORGANIZACIONES[slug] && em.empresas[ORGANIZACIONES[slug]]) return ORGANIZACIONES[slug];
  // Sólo coincidencia exacta con la clave o el nombre de la empresa: adivinar por
  // parecido se equivoca (una organización "cabana-amakaik" puede ser de otra empresa).
  const n = String(slug).toLowerCase().replace(/[^a-z0-9]/g, "");
  return Object.values(em.empresas).find(e => [e.key, e.nombre, e.razon_social].filter(Boolean).some(x => String(x).toLowerCase().replace(/[^a-z0-9]/g, "") === n))?.key || null;
}
const CAMPO_DEFAULT = Object.keys(CAMPOS)[0];
const bases = {};

function getDB(key) {
  const k = claveCampo(key) || CAMPO_DEFAULT;
  if (bases[k]) return bases[k];
  if (!fs.existsSync(DB_DIR)) fs.mkdirSync(DB_DIR, { recursive: true });
  const db = new Database(path.join(DB_DIR, `${k}.db`));
  db.pragma("journal_mode = WAL");
  crearTablas(db);
  plantelMod.init(db);
  if (destinosMod) { try { destinosMod.init(db); } catch (e) {} }
  animalesMod.init(db);
  exportarMod.init(db);
  botMod.init(db);
  adjuntosMod.init(db);
  finanzasMod.init(db);
  vinculosMod.init(db);
  bases[k] = db;
  return db;
}
const dbDe = req => getDB(campoDe(req));
const campoDe = req => { const k = req.query.campo || (req.body && req.body.campo); return claveCampo(k) || CAMPO_DEFAULT; };
// Las empresas se arman con los campos; se crean después de getDB porque lo usan.
let empresas;
const empresasDe = () => empresas || (empresas = empresasMod.crear({ CAMPOS, getDB, plantelMod, animalesMod, destinosMod, finanzasMod }));
// Los totales de la empresa cuentan los hijos que están en los otros campos.
const conCruce = () => { const em = empresasDe(); em.criasFuera = criasFueraDe; em.hijosFuera = hijosFueraDe; return em; };
let vinculos;
const vinculosDe = () => vinculos || (vinculos = vinculosMod.crear({ CAMPOS, getDB, empresasDe }));
// Los hijos de cada vaca que están en los otros campos de la empresa, listos
// para que plantel.js los cuente como partos suyos.
const { compacto } = animalesMod;
function criasFueraDe(campoKey) {
  try {
    const mapa = vinculosDe().mapaCriasFuera(campoKey);
    if (!mapa.size) return undefined;
    return rp => mapa.get(compacto(rp)) || [];
  } catch (e) { return undefined; }
}
const opcionesPlantel = (campoKey, extra) => ({ criasFuera: criasFueraDe(campoKey), ...(extra || {}) });
// Lo mismo para los toros: los hijos que tuvieron sirviendo en otro campo.
function hijosFueraDe(campoKey) {
  try {
    const mapa = vinculosDe().mapaCriasFuera(campoKey, { relacion: "padre" });
    if (!mapa.size) return undefined;
    return rp => mapa.get(compacto(rp)) || [];
  } catch (e) { return undefined; }
}

function crearTablas(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS animales (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      rp TEXT NOT NULL, chip TEXT, sexo TEXT, categoria TEXT,
      estado TEXT DEFAULT 'ACTIVO', fecha_nac TEXT, pelo TEXT, raza TEXT,
      madre_rp TEXT, padre_rp TEXT, hbu TEXT, registro TEXT, lote TEXT,
      notas TEXT, created_at TEXT DEFAULT (datetime('now')),
      UNIQUE(rp));
    CREATE TABLE IF NOT EXISTS pesadas (
      id INTEGER PRIMARY KEY AUTOINCREMENT, animal_id INTEGER NOT NULL,
      fecha TEXT NOT NULL, peso REAL NOT NULL, contexto TEXT, gdp REAL,
      created_at TEXT DEFAULT (datetime('now')));
    CREATE TABLE IF NOT EXISTS servicios (
      id INTEGER PRIMARY KEY AUTOINCREMENT, animal_id INTEGER NOT NULL,
      temporada TEXT, tipo_servicio TEXT, semen_iatf TEXT, fecha_iatf TEXT,
      toro_natural TEXT, fecha_ingreso_toro TEXT, fecha_salida_toro TEXT,
      resultado TEXT, fecha_tacto TEXT, notas TEXT,
      created_at TEXT DEFAULT (datetime('now')));
    CREATE TABLE IF NOT EXISTS mediciones (
      id INTEGER PRIMARY KEY AUTOINCREMENT, animal_id INTEGER NOT NULL,
      fecha TEXT NOT NULL, tipo TEXT NOT NULL, valor REAL,
      created_at TEXT DEFAULT (datetime('now')));
    CREATE TABLE IF NOT EXISTS sanidad (
      id INTEGER PRIMARY KEY AUTOINCREMENT, animal_id INTEGER NOT NULL,
      fecha TEXT NOT NULL, producto TEXT, dosis TEXT, motivo TEXT,
      created_at TEXT DEFAULT (datetime('now')));
    CREATE INDEX IF NOT EXISTS idx_pes_an ON pesadas(animal_id);
    CREATE INDEX IF NOT EXISTS idx_ser_an ON servicios(animal_id);
    CREATE INDEX IF NOT EXISTS idx_med_an ON mediciones(animal_id);
    CREATE INDEX IF NOT EXISTS idx_ani_madre ON animales(madre_rp);

    -- Los tableros que arma el bot. Cada uno vive aparte: si uno sale roto,
    -- no afecta al tablero principal ni a los demás.
    CREATE TABLE IF NOT EXISTS tableros (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      slug TEXT NOT NULL UNIQUE,
      titulo TEXT NOT NULL,
      pedido TEXT,
      html TEXT NOT NULL,
      creado_por TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now')));
  `);
}

// ── EL BOT ───────────────────────────────────────────────────────────────────
// Vive en bot.js. Acá sólo se crea con lo que necesita del servidor.
// guardarTablero está definido más abajo; como es una declaración de función,
// ya existe cuando se llega acá.
const bot = botMod.crear({ plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, adjuntosMod, finanzasMod, guardarTablero, registrarSalida, CAMPOS, empresas: empresasDe, vinculos: vinculosDe, criasFuera: criasFueraDe, hijosFuera: hijosFueraDe });
const MODELO = bot.modelo;

// ── TABLEROS QUE ARMA EL BOT ─────────────────────────────────────────────────

// La estética del sistema, para que todos los tableros salgan parejos sin que
// el bot tenga que escribirla cada vez (y sin gastar tokens en eso).
function plantilla(titulo, subtitulo, contenido) {
  return `<!DOCTYPE html><html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${titulo}</title>
<link href="https://fonts.googleapis.com/css2?family=Oswald:wght@300;400;500;600&family=JetBrains+Mono:wght@400&display=swap" rel="stylesheet">
<style>
:root{--azul:#0B3D7C;--azul2:#072957;--oro:#C9A24B;--tinta:#10243f;--papel:#F7F3EC;--linea:#E2D9CB;--gris:#8A827A;--verde:#1a7a4a;--rojo:#B83232}
*{box-sizing:border-box}
body{margin:0;background:var(--papel);color:var(--tinta);font-family:Oswald,system-ui,sans-serif;font-weight:300;font-size:14px}
header{background:var(--azul2);color:#fff;padding:18px 24px;border-bottom:4px solid var(--oro)}
header h1{margin:0;font-size:22px;font-weight:600;letter-spacing:2.5px;text-transform:uppercase}
header p{margin:4px 0 0;font-size:11px;color:var(--oro);letter-spacing:2px;text-transform:uppercase}
main{padding:20px 24px 50px;max-width:1400px}
h2{font-size:13px;letter-spacing:2px;text-transform:uppercase;color:var(--azul);font-weight:500;
   margin:26px 0 10px;border-bottom:2px solid var(--linea);padding-bottom:6px}
h2:first-child{margin-top:0}
.kpis{display:flex;gap:1px;background:var(--linea);border:1px solid var(--linea);flex-wrap:wrap;margin-bottom:20px}
.kpi{background:#fff;padding:12px 18px;flex:1;min-width:112px}
.kpi b{display:block;font-size:25px;font-weight:500;color:var(--azul);line-height:1.1}
.kpi span{font-size:9.5px;letter-spacing:1.1px;text-transform:uppercase;color:var(--gris)}
.kpi.al b{color:var(--rojo)}.kpi.bien b{color:var(--verde)}.kpi.oro b{color:#B8860B}
table{width:100%;border-collapse:collapse;background:#fff;font-size:13px;margin-bottom:18px}
th{background:var(--azul2);color:#fff;text-align:left;font-size:9.5px;letter-spacing:1.2px;
   text-transform:uppercase;font-weight:400;padding:10px 8px;white-space:nowrap}
td{padding:8px;border-bottom:1px solid var(--linea)}
tr:hover td{background:#FAF7F0}
th.n,td.n,.n{text-align:right;font-variant-numeric:tabular-nums}
.mut{color:var(--gris)}.al{color:var(--rojo);font-weight:500}.bi{color:var(--verde);font-weight:500}
.tag{font-size:9px;letter-spacing:1.1px;text-transform:uppercase;padding:2px 8px;border-radius:2px;
  background:rgba(11,61,124,.1);color:var(--azul)}
p{line-height:1.6;color:var(--gris);max-width:80ch}
a{color:var(--azul)}
footer{padding:16px 24px;font-size:11px;color:var(--gris);border-top:1px solid var(--linea)}
</style></head><body>
<header><h1>${titulo}</h1>${subtitulo ? `<p>${subtitulo}</p>` : ""}</header>
<main>${contenido}</main>
<footer>Generado por RODEO · <a href="/">volver al tablero</a></footer>
</body></html>`;
}

function guardarTablero(db, t, campoKey) {
  const slug = String(t.slug || "").toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9-]/g, "-").replace(/-+/g, "-").replace(/^-|-$/g, "");
  if (!slug) throw new Error("El nombre para la url no sirve");

  const cuerpo = String(t.contenido || t.html || "").trim();
  if (!cuerpo) throw new Error("Falta el contenido del tablero");

  // Si mandó una página entera igual se acepta; si no, se envuelve.
  let html = /<html/i.test(cuerpo) ? cuerpo
    : plantilla(t.titulo || slug, t.subtitulo, cuerpo);
  if (campoKey) html = html.replace(/CAMPO_AQUI|__CAMPO__/g, campoKey);

  db.prepare(`INSERT INTO tableros (slug,titulo,pedido,html,creado_por) VALUES (?,?,?,?,?)
    ON CONFLICT(slug) DO UPDATE SET titulo=excluded.titulo, html=excluded.html,
      pedido=excluded.pedido, updated_at=datetime('now')`)
    .run(slug, t.titulo || slug, t.pedido || null, html, t.creado_por || null);

  return { ok: true, slug, url: `/t/${slug}`,
    mensaje: `Quedó en /t/${slug}` };
}

// Las tablas de un tablero del bot, como Excel o CSV.
app.get("/t/:slug.:formato(xlsx|csv)", (req, res) => {
  const db = getDB(req.query.campo || CAMPO_DEFAULT);
  try {
    const t = db.prepare("SELECT titulo, html FROM tableros WHERE slug=?").get(req.params.slug);
    if (!t) return res.status(404).json({ error: "No existe ese tablero" });
    const tablas = exportarMod.tablasDeHtml(t.html);
    if (!tablas.length) return res.status(404).json({ error: "Ese tablero no tiene tablas para exportar" });
    if (req.params.formato === "csv") {
      const tb = tablas[Number(req.query.tabla) || 0];
      return enviarArchivo(res, { buffer: Buffer.from(exportarMod.csv(tb.columnas, tb.filas), "utf8"), mime: "text/csv; charset=utf-8",
        nombre: exportarMod.nombreArchivo(t.titulo, "csv") });
    }
    const xlsx = require("./xlsx.js");
    enviarArchivo(res, { buffer: xlsx.armar(tablas.map(tb => ({ ...tb, titulo: t.titulo, subtitulo: tb.nombre }))), mime: xlsx.MIME,
      nombre: exportarMod.nombreArchivo(t.titulo, "xlsx") });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Un tablero armado por el bot.
app.get("/t/:slug", (req, res) => {
  const db = getDB(req.query.campo || CAMPO_DEFAULT);
  try {
    const t = db.prepare("SELECT html FROM tableros WHERE slug=?").get(req.params.slug);
    if (!t) return res.status(404).type("html").send(
      `<body style="font-family:system-ui;padding:60px;text-align:center;color:#666">
       <h2>No existe ese tablero</h2><p><a href="/">Volver</a></p></body>`);
    res.type("html").send(t.html);
  } catch (e) { res.status(500).send(e.message); }
});

app.get("/api/tableros", (req, res) => {
  const db = getDB(req.query.campo || CAMPO_DEFAULT);
  try {
    res.json(db.prepare(`SELECT slug,titulo,pedido,created_at,updated_at
      FROM tableros ORDER BY updated_at DESC`).all()
      .map(t => ({ ...t, url: `/t/${t.slug}` })));
  } catch (e) { res.json([]); }
});

app.delete("/api/tableros/:slug", (req, res) => {
  const db = getDB(req.query.campo || CAMPO_DEFAULT);
  try {
    const r = db.prepare("DELETE FROM tableros WHERE slug=?").run(req.params.slug);
    res.json({ ok: !!r.changes });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── API ──────────────────────────────────────────────────────────────────────

// ?empresa=clave o ?de_campo=clave: sólo los campos de esa empresa (así, embebido en el
// portal, cada organización ve los suyos y no los de las demás).
// Una clave de campo o de empresa, aunque venga con otra mayúscula/minúscula.
function claveCampo(k) { if (!k) return null; if (CAMPOS[k]) return k; const n = String(k).toLowerCase(); return Object.keys(CAMPOS).find(x => x.toLowerCase() === n) || null; }
app.get("/api/campos", (req, res) => {
  const em = empresasDe();
  const pedido = req.query.de_campo ? claveCampo(req.query.de_campo) : null;
  let empresaKey = null, aviso = null;
  if (req.query.empresa) {
    const ek = Object.keys(em.empresas).find(x => x.toLowerCase() === String(req.query.empresa).toLowerCase());
    if (ek) empresaKey = ek; else { empresaKey = em.empresaDe(CAMPO_DEFAULT).key; aviso = `No existe la empresa "${req.query.empresa}"`; }
  } else if (req.query.de_campo || req.query.org) {
    // Pidieron un campo: se muestra su empresa. Si la clave no existe, se intenta con
    // la organización del portal; si tampoco, la del campo por defecto, y se avisa.
    const porOrg = pedido ? null : empresaDeOrg(req.query.org);
    empresaKey = pedido ? em.empresaDe(pedido).key : porOrg || em.empresaDe(CAMPO_DEFAULT).key;
    if (!pedido && req.query.de_campo && !porOrg) aviso = `No existe el campo "${req.query.de_campo}". Las claves son: ${Object.keys(CAMPOS).join(", ")}`;
    if (!pedido && !req.query.de_campo && !porOrg && req.query.org) aviso = `No sé a qué empresa corresponde la organización "${req.query.org}"`;
  }
  let entradas = Object.entries(CAMPOS).filter(([key]) => !empresaKey || em.empresaDe(key).key === empresaKey);
  if (!entradas.length) { entradas = Object.entries(CAMPOS).filter(([key]) => em.empresaDe(key).key === em.empresaDe(CAMPO_DEFAULT).key); aviso = aviso || `La empresa "${empresaKey}" no tiene campos`; }
  if (aviso) res.setHeader("X-Aviso", encodeURIComponent(aviso));
  res.json(entradas.map(([key, c]) => {
    let n = 0;
    try { n = getDB(key).prepare("SELECT COUNT(*) n FROM animales WHERE upper(COALESCE(estado,'ACTIVO'))='ACTIVO'").get().n; }
    catch (e) {}
    const e = empresasDe().empresaDe(key);
    return { key, nombre: c.nombre, empresa: c.empresa, empresa_nombre: e && e.nombre, campos_empresa: e ? e.campos.length : 1, animales: n };
  }));
});

// Todo el plantel con los datos de cada vaca.
app.get("/api/plantel", (req, res) => {
  try { res.json(plantelMod.plantel(dbDe(req), opcionesPlantel(campoDe(req), { anio: req.query.anio }))); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// La ficha: lo reproductivo si es vientre, y lo general (pesadas, sanidad,
// lotes, hijos, notas) para cualquier animal. Tolera "011" por "11".
app.get("/api/ficha/:rp", (req, res) => {
  const db = dbDe(req);
  try {
    const general = animalesMod.ficha(db, req.params.rp);
    if (!general.ok) return res.status(404).json(general);
    const vientre = general.es_vientre ? plantelMod.ficha(db, general.rp, opcionesPlantel(campoDe(req))) : { ok: false };
    const f = vientre.ok ? { ...general, ...vientre, general: true, vientre: true } : { ...general, vientre: false };
    // Lo que vive en otros campos de la misma empresa: la madre, el padre, los hijos.
    const campoKey = campoDe(req);
    try {
      const v = vinculosDe();
      const fam = v.familiaFuera(campoKey, { madre_campo: general.madre_campo, padre_campo: general.padre_campo, madre_rp: general.madre, padre_rp: general.padre });
      const hf = v.hijosFuera(campoKey, general.rp, general.nombre);
      if (fam.madre) f.madre_fuera = fam.madre;
      if (fam.padre) f.padre_fuera = fam.padre;
      if (hf.length) f.hijos_fuera = hf;
      // Si la madre no está acá y no tiene campo anotado, decir dónde podría estar.
      if (general.madre && !general.madre_existe && !general.madre_campo) {
        const cand = v.buscarEnEmpresa(campoKey, general.madre, { excluir: campoKey });
        if (cand.length) f.madre_candidatos = cand;
      }
      if (general.padre && !general.padre_existe && !general.padre_campo) {
        const cand = v.buscarEnEmpresa(campoKey, general.padre, { excluir: campoKey });
        if (cand.length) f.padre_candidatos = cand;
      }
    } catch (e) {}
    res.json(f);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Buscar como se nombra en la manga: RP, caravana, HBA, madre, padre o texto de notas.
app.get("/api/buscar", (req, res) => {
  try { res.json(animalesMod.buscar(dbDe(req), req.query.q, { limite: Number(req.query.limite) || 30 })); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// Todos los animales, para las otras vistas del tablero. ?estado=TODOS trae también muertos y vendidos.
app.get("/api/animales", (req, res) => {
  try { res.json(animalesMod.listar(dbDe(req), { estado: req.query.estado || "ACTIVO" })); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// Los lotes con sus animales. La terminación se define por lote, no por
// categoría: un toro en el corral está terminando, el mismo en el potrero no.
app.get("/api/lotes", (req, res) => {
  const db = dbDe(req);
  try {
    const lotes = db.prepare(`
      SELECT l.id, l.nombre, l.potrero, l.descripcion,
             COUNT(la.animal_id) animales
      FROM lotes l LEFT JOIN lote_animales la ON la.lote_id = l.id
      GROUP BY l.id ORDER BY animales DESC`).all();
    res.json(lotes);
  } catch (e) { res.json([]); }
});

app.get("/api/lote/:id/animales", (req, res) => {
  const db = dbDe(req);
  try {
    res.json(db.prepare(`
      SELECT a.*, la.fecha_ingreso,
        (SELECT peso FROM pesadas p WHERE p.animal_id=a.id ORDER BY p.fecha DESC LIMIT 1) peso_actual,
        (SELECT fecha FROM pesadas p WHERE p.animal_id=a.id ORDER BY p.fecha DESC LIMIT 1) ultima_pesada,
        (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND upper(COALESCE(p.contexto,''))='DESTETE'
         ORDER BY p.fecha DESC LIMIT 1) destete
      FROM lote_animales la JOIN animales a ON a.id = la.animal_id
      WHERE la.lote_id = ? ORDER BY a.rp`).all(req.params.id));
  } catch (e) { res.json([]); }
});

// Los toros del campo, con lo que dicen de ellos sus hijos.
app.get("/api/toros", (req, res) => {
  const k = campoDe(req);
  try { res.json(animalesMod.toros(getDB(k), { estado: req.query.estado, anio: req.query.anio, hijosFuera: hijosFueraDe(k) })); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// Todo lo que está en corral, con cuánto viene ganando cada uno.
app.get("/api/terminacion", (req, res) => {
  try { res.json(animalesMod.terminacion(dbDe(req))); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// ── EXPORTAR ─────────────────────────────────────────────────────────────────
// Cualquier conjunto, en cualquier formato. GET para lo simple; POST cuando el
// tablero manda qué RP y qué columnas están a la vista.

function enviarArchivo(res, a) {
  res.setHeader("Content-Type", a.mime);
  if (!a.inline) res.setHeader("Content-Disposition", `attachment; filename*=UTF-8''${encodeURIComponent(a.nombre)}`);
  res.send(a.buffer);
}
function opcionesExport(req) {
  const b = req.body || {}, q = req.query || {};
  const campoKey = q.campo || b.campo || CAMPO_DEFAULT;
  const rps = b.rps || (q.rps ? String(q.rps).split(",") : null);
  const columnas = b.columnas || (q.columnas ? String(q.columnas).split(",") : null);
  return { campoNombre: (CAMPOS[campoKey] || {}).nombre || "", rps, columnas, filtro: b.filtro || q.filtro,
    criasFuera: criasFueraDe(campoKey), hijosFuera: hijosFueraDe(campoKey),
    orden: b.orden || (q.orden ? { col: q.orden, desc: q.desc === "1" } : null), anio: q.anio || b.anio,
    estado: q.estado || b.estado, temporada: q.temporada || b.temporada, sep: q.sep || b.sep,
    volver: "/" + (campoKey !== CAMPO_DEFAULT ? `?campo=${campoKey}` : "") };
}
app.get("/api/exportar/:conjunto.:formato", (req, res) => {
  try { enviarArchivo(res, exportarMod.armar(dbDe(req), mods(), req.params.conjunto, req.params.formato, opcionesExport(req))); }
  catch (e) { res.status(400).json({ error: e.message }); }
});
app.post("/api/exportar", (req, res) => {
  try { enviarArchivo(res, exportarMod.armar(dbDe(req), mods(), req.body.conjunto, req.body.formato || "xlsx", opcionesExport(req))); }
  catch (e) { res.status(400).json({ error: e.message }); }
});
// Qué conjuntos y columnas hay, para armar menús sin hardcodearlos en el tablero.
app.get("/api/exportar", (req, res) => {
  res.json({ formatos: Object.keys(exportarMod.FORMATOS),
    conjuntos: Object.entries(exportarMod.NOMBRES).map(([clave, nombre]) => ({ clave, nombre, columnas: exportarMod.COLUMNAS[clave].map(c => ({ k: c.k, t: c.t })) })) });
});

// Archivos guardados (los que arma el bot o el relevamiento).
app.get("/api/archivos", (req, res) => {
  try { res.json(exportarMod.listarArchivos(dbDe(req))); } catch (e) { res.json([]); }
});
app.get("/archivos/:id/:nombre?", (req, res) => {
  const db = getDB(req.query.campo || CAMPO_DEFAULT);
  const a = exportarMod.leerArchivo(db, Number(req.params.id));
  if (!a) return res.status(404).send("No existe ese archivo");
  enviarArchivo(res, { buffer: a.bytes, mime: a.mime, nombre: a.nombre, inline: /html/.test(a.mime) && req.query.ver === "1" });
});
app.delete("/api/archivos/:id", (req, res) => {
  try { res.json(exportarMod.borrarArchivo(dbDe(req), Number(req.params.id))); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// ── RELEVAR ──────────────────────────────────────────────────────────────────
// Carga de campo. Cada ruta acepta simular:true para ver qué haría.

app.post("/api/relevar/pesadas", (req, res) => {
  try {
    const filas = req.body.texto ? relevarMod.parsearLineas(req.body.texto).map(l => ({ rp: l.rp, peso: l.valor, fecha: l.extra[0] })) : req.body.filas;
    res.json(relevarMod.pesadas(dbDe(req), { ...req.body, filas }));
  } catch (e) { res.status(400).json({ error: e.message }); }
});
app.post("/api/relevar/sanidad", (req, res) => {
  try {
    const rps = req.body.texto ? relevarMod.parsearLineas(req.body.texto).map(l => l.rp) : req.body.rps;
    res.json(relevarMod.sanidad(dbDe(req), { ...req.body, rps }));
  } catch (e) { res.status(400).json({ error: e.message }); }
});
app.post("/api/relevar/nacimientos", (req, res) => {
  try {
    const b = { ...req.body };
    // Un color de caravana para todas las filas que no lo traigan.
    if (b.color && Array.isArray(b.filas)) b.filas = b.filas.map(f => ({ ...f, caravana_color: f.caravana_color || b.color }));
    res.json(relevarMod.nacimientos(dbDe(req), b));
  }
  catch (e) { res.status(400).json({ error: e.message }); }
});
// Identificar: "control nuevoRP [chip]" o "control chip" o "rpActual nuevoRP [chip]", una línea por animal.
app.post("/api/relevar/identificar", (req, res) => {
  try {
    let filas = req.body.filas;
    if (req.body.texto) filas = relevarMod.parsearLineas(req.body.texto).map(l => {
      const t = [l.rp, l.valor, ...l.extra].filter(Boolean);
      const chip = t.find((x, i) => i > 0 && /^\d{12,16}$/.test(x));
      const resto = t.filter(x => x !== chip);
      const f = { chip, color: req.body.color || undefined };
      if (req.body.por === "rp") f.rp_actual = resto[0]; else f.control = resto[0];
      if (resto[1]) f.rp = resto[1];
      return f;
    });
    res.json(relevarMod.identificar(dbDe(req), { filas, simular: req.body.simular, usuario: req.body.usuario }));
  } catch (e) { res.status(400).json({ error: e.message }); }
});
app.post("/api/relevar/mediciones", (req, res) => {
  try {
    const filas = req.body.texto ? relevarMod.parsearLineas(req.body.texto).map(l => ({ rp: l.rp, valor: l.valor })) : req.body.filas;
    res.json(relevarMod.mediciones(dbDe(req), { ...req.body, filas }));
  } catch (e) { res.status(400).json({ error: e.message }); }
});
app.post("/api/relevar/notas", (req, res) => {
  try {
    const filas = req.body.texto ? relevarMod.parsearLineas(req.body.texto).map(l => ({ rp: l.rp, texto: [l.valor, ...l.extra].filter(Boolean).join(" ") })) : req.body.filas;
    res.json(relevarMod.notas(dbDe(req), plantelMod, { ...req.body, filas }));
  } catch (e) { res.status(400).json({ error: e.message }); }
});
// Un CSV pegado o subido: { texto, tipo?, mapa?, simular? }
app.post("/api/importar/csv", (req, res) => {
  try { res.json(relevarMod.importarCsv(dbDe(req), plantelMod, req.body)); }
  catch (e) { res.status(400).json({ error: e.message }); }
});
// La planilla para llevar al campo. ?lote_id= | ?rps=a,b | ?conjunto=recria · &formato=html para imprimir
app.get("/api/planilla", (req, res) => {
  try {
    const campoKey = req.query.campo || CAMPO_DEFAULT;
    enviarArchivo(res, relevarMod.planilla(dbDe(req), {
      lote_id: req.query.lote_id ? Number(req.query.lote_id) : null,
      rps: req.query.rps ? String(req.query.rps).split(",") : null,
      conjunto: req.query.conjunto, columnas: req.query.columnas ? String(req.query.columnas).split(",") : null,
      titulo: req.query.titulo, formato: req.query.formato, campoNombre: (CAMPOS[campoKey] || {}).nombre
    }, exportarMod, mods()));
  } catch (e) { res.status(400).json({ error: e.message }); }
});
app.post("/api/planilla", (req, res) => {
  try {
    const campoKey = req.query.campo || req.body.campo || CAMPO_DEFAULT;
    enviarArchivo(res, relevarMod.planilla(dbDe(req), { ...req.body, campoNombre: (CAMPOS[campoKey] || {}).nombre }, exportarMod, mods()));
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ── DESTINOS ─────────────────────────────────────────────────────────────────
// Para mostrar cada destino con los datos del animal: las vacas con lo que
// calcula el plantel, los toros con lo suyo (peso, hijos, edad).
function filasParaDestinos(db, campoKey) {
  const vacas = plantelMod.plantel(db, opcionesPlantel(campoKey, { incluirDestinados: true })).filas;
  const toros = animalesMod.toros(db, { incluirDestinados: true }).filas.map(t => ({
    rp: t.rp, categoria: t.categoria, pelo: t.pelo, edad_meses: t.edad_meses, peso_adulto: t.peso_actual,
    partos: t.hijos, destete_prom: t.destete_prom_hijos, eficiencia: null, ipp: null, estado: t.estado, bloque: null }));
  return [...vacas, ...toros];
}

// A dónde va cada animal cuando sale del plantel. No todas las salidas son
// fracasos: el mejor toro también se va, como reproductor.
app.get("/api/destinos", (req, res) => {
  if (!destinosMod) return res.status(503).json({ error: "Módulo no disponible" });
  const db = dbDe(req);
  try {
    res.json(destinosMod.listar(db, filasParaDestinos(db, campoDe(req)), { temporada: req.query.temporada }));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/api/destinos", (req, res) => {
  if (!destinosMod) return res.status(503).json({ error: "Módulo no disponible" });
  const { rp, rps, destino } = req.body;
  if (!destino) return res.status(400).json({ error: "Falta el destino" });
  const db = dbDe(req);
  try {
    const r = Array.isArray(rps) && rps.length
      ? destinosMod.marcarVarios(db, rps, destino, req.body)
      : destinosMod.marcar(db, rp, destino, req.body);
    res.status(r.ok ? 200 : 400).json(r);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete("/api/destinos/:rp", (req, res) => {
  if (!destinosMod) return res.status(503).json({ error: "Módulo no disponible" });
  try { res.json(destinosMod.sacar(dbDe(req), req.params.rp, req.query.temporada)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// Cuando el animal efectivamente sale del campo. Con precio, la venta va al financiero.
app.post("/api/destinos/:rp/salida", async (req, res) => {
  if (!destinosMod) return res.status(503).json({ error: "Módulo no disponible" });
  try { res.json(await registrarSalida(dbDe(req), { ...req.body, rps: [req.params.rp], campo: campoDe(req) })); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
// Varios juntos: "salieron los 5 novillos al frigorífico, 2.300 kg, 8.500 dólares".
app.post("/api/destinos/salida", async (req, res) => {
  if (!destinosMod) return res.status(503).json({ error: "Módulo no disponible" });
  try { res.json(await registrarSalida(dbDe(req), { ...req.body, campo: campoDe(req) })); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
async function registrarSalida(db, b) {
  const rps = Array.isArray(b.rps) ? b.rps : [b.rp].filter(Boolean);
  if (!rps.length) throw new Error("Falta el RP");
  const n = rps.length;
  const porCabeza = b.precio_por_cabeza != null ? Number(b.precio_por_cabeza) : b.precio_total != null ? Number(b.precio_total) / n : b.precio != null ? Number(b.precio) : null;
  // Si nunca se le marcó destino, se le pone uno (venta directa, o el que venga) y sale igual.
  const resultados = rps.map(rp => {
    let r = destinosMod.concretar(db, rp, { fecha: b.fecha, precio: porCabeza, temporada: b.temporada });
    if (!r.ok && /no tenía destino/.test(r.error || "")) {
      const m = destinosMod.marcar(db, rp, b.destino || "venta directa", { temporada: b.temporada, motivo: b.motivo, usuario: "salida" });
      if (m.ok) r = destinosMod.concretar(db, m.rp, { fecha: b.fecha, precio: porCabeza, temporada: b.temporada });
      else r = m;
    }
    return r;
  });
  const salieron = rps.filter((rp, i) => resultados[i].ok);
  let venta = null;
  if (salieron.length && porCabeza > 0) {
    venta = await finanzasMod.enviarVenta(db, { rps: salieron, fecha: b.fecha, comprador: b.comprador, kg: b.kg, precio_total: porCabeza * salieron.length, detalle: b.detalle, concepto: b.concepto }, b.campo ? empresasDe().finanzasDe(b.campo) : undefined);
  }
  return { ok: salieron.length > 0, salieron, resultados, venta,
    mensaje: `${salieron.length} animal${salieron.length === 1 ? "" : "es"} salió${salieron.length === 1 ? "" : "eron"} del campo` +
      (venta ? (venta.enviado ? ` · venta de ${venta.monto} enviada al financiero` : ` · la venta NO se mandó al financiero: ${venta.motivo}`) : "") };
}

app.post("/api/notas", (req, res) => {
  const { rp, texto } = req.body;
  if (!rp || !texto) return res.status(400).json({ error: "Falta el RP o el texto" });
  const db = dbDe(req);
  const a = db.prepare("SELECT rp FROM animales WHERE upper(rp)=upper(?)").get(String(rp).trim());
  if (!a) return res.status(404).json({ error: `No encuentro ${rp}` });
  try { res.json(plantelMod.guardarNota(db, a.rp, texto, req.body)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/notas", (req, res) => {
  const db = dbDe(req);
  try {
    res.json(req.query.rp
      ? db.prepare("SELECT * FROM notas_campo WHERE upper(animal_rp)=upper(?) ORDER BY fecha DESC").all(req.query.rp)
      : db.prepare("SELECT * FROM notas_campo ORDER BY fecha DESC LIMIT 200").all());
  } catch (e) { res.json([]); }
});

// El chat. Es Claude con la base en la mano. La historia sale de la base por
// sesión (el navegador manda `sesion`); si el cliente manda `historia`, se usa ésa.
function ctxChat(req) {
  const campoKey = req.query.campo || req.body.campo || CAMPO_DEFAULT;
  const db = getDB(campoKey);
  const usuario = String(req.body.sesion || "anonimo");
  // El mensaje puede traer archivos: se guardan y se convierten en bloques que el modelo lee.
  const adjuntos = Array.isArray(req.body.adjuntos) ? req.body.adjuntos : [];
  const prep = adjuntos.length ? adjuntosMod.preparar(db, { texto: req.body.mensaje, adjuntos, canal: "web", usuario }) : { content: req.body.mensaje, guardados: [] };
  return { campoKey, db, nombre: (CAMPOS[campoKey] || {}).nombre || "el campo", mensaje: prep.content, adjuntos: prep.guardados,
    opciones: { campoKey, soloLectura: req.body.solo_lectura, canal: "web", usuario,
      historia: Array.isArray(req.body.historia) ? req.body.historia : undefined } };
}
app.post("/api/chat", async (req, res) => {
  if (!req.body.mensaje && !(Array.isArray(req.body.adjuntos) && req.body.adjuntos.length)) return res.status(400).json({ error: "Falta el mensaje" });
  const c = ctxChat(req);
  try { res.json({ ...(await bot.responder(c.db, c.nombre, c.mensaje, c.opciones)), adjuntos: c.adjuntos }); }
  catch (e) { res.status(500).json({ error: e.message, respuesta: `No pude procesarlo: ${e.message}` }); }
});

// Lo mismo, pero contando en vivo qué está haciendo (Server-Sent Events).
// Eventos: vuelta, pensando {delta}, texto {delta}, paso {texto}, fin {respuesta, pasos, uso}, error.
app.post("/api/chat/stream", async (req, res) => {
  if (!req.body.mensaje && !(Array.isArray(req.body.adjuntos) && req.body.adjuntos.length)) return res.status(400).json({ error: "Falta el mensaje" });
  let c;
  try { c = ctxChat(req); } catch (e) { return res.status(400).json({ error: e.message }); }
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("X-Accel-Buffering", "no");
  res.flushHeaders();
  const enviar = e => { try { res.write(`data: ${JSON.stringify(e)}\n\n`); } catch (x) {} };
  const latido = setInterval(() => { try { res.write(":\n\n"); } catch (x) {} }, 15000);
  try {
    if (c.adjuntos.length) enviar({ tipo: "adjuntos", adjuntos: c.adjuntos });
    const r = await bot.responder(c.db, c.nombre, c.mensaje, { ...c.opciones, onEvento: e => { if (e.tipo !== "fin") enviar(e); } });
    enviar({ tipo: "fin", ...r, adjuntos: c.adjuntos });
  } catch (e) {
    enviar({ tipo: "error", error: e.message, respuesta: `No pude procesarlo: ${e.message}` });
  }
  clearInterval(latido);
  res.end();
});

// La conversación guardada de una sesión del navegador.
app.get("/api/conversacion", (req, res) => {
  try { res.json(bot.conversacion(dbDe(req), "web", String(req.query.sesion || "anonimo"), Number(req.query.limite) || 40)); }
  catch (e) { res.json([]); }
});
app.delete("/api/conversacion", (req, res) => {
  try {
    const r = dbDe(req).prepare("DELETE FROM conversaciones WHERE canal='web' AND usuario=?").run(String(req.query.sesion || "anonimo"));
    res.json({ ok: true, borrados: r.changes });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Los archivos que le mandaron al bot.
app.get("/api/adjuntos", (req, res) => { try { res.json(adjuntosMod.listar(dbDe(req))); } catch (e) { res.json([]); } });
app.get("/adjuntos/:id/:nombre?", (req, res) => {
  const a = adjuntosMod.leerBytes(getDB(req.query.campo || CAMPO_DEFAULT), req.params.id);
  if (!a) return res.status(404).send("No existe ese adjunto");
  res.setHeader("Content-Type", a.mime || "application/octet-stream");
  if (!req.query.ver) res.setHeader("Content-Disposition", `attachment; filename*=UTF-8''${encodeURIComponent(a.nombre)}`);
  res.send(a.bytes);
});

// Diagnóstico de WhatsApp: a qué campo y con qué cuenta contesta cada número.
//   /api/whatsapp?to=+5491133334444  → dice qué pasaría con un mensaje a ese número
app.get("/api/whatsapp", (req, res) => {
  const lista = numerosWhatsApp().map(n => ({ nombre: n.nombre, termina_en: n.numero.slice(-4), campo: n.campo,
    campo_nombre: (CAMPOS[n.campo] || {}).nombre, empresa: empresasDe().empresaDe(n.campo).nombre,
    cuenta_twilio: n.sid ? n.sid.slice(0, 6) + "…" + n.sid.slice(-4) : "FALTA" }));
  const out = { numeros: lista, permitidos: WA_PERMITIDOS.length ? WA_PERMITIDOS.length + " números autorizados" : "cualquiera puede escribir",
    por_remitente: WA_CAMPOS };
  if (req.query.to) {
    const n = numeroDe(req.query.to);
    out.prueba = { escriben_a: req.query.to, reconocido: !n.desconocido, numero: n.nombre, campo: n.campo,
      campo_nombre: (CAMPOS[n.campo] || {}).nombre, empresa: empresasDe().empresaDe(n.campo).nombre,
      cuenta_twilio: n.sid ? n.sid.slice(0, 6) + "…" + n.sid.slice(-4) : "FALTA" };
  }
  res.json(out);
});

// Cuánto sale tener el bot andando (tokens y dólares estimados).
app.get("/api/uso", (req, res) => {
  try { res.json(bot.uso(dbDe(req), { desde: req.query.desde, hasta: req.query.hasta })); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// Lo que el bot recuerda del campo.
app.get("/api/memoria", (req, res) => { try { res.json(bot.memorias(dbDe(req))); } catch (e) { res.json([]); } });
app.post("/api/memoria", (req, res) => {
  try { res.json(bot.recordar(dbDe(req), { texto: req.body.texto, categoria: req.body.categoria }, req.body.usuario || "tablero")); }
  catch (e) { res.status(400).json({ error: e.message }); }
});
app.delete("/api/memoria/:id", (req, res) => {
  try { res.json(bot.recordar(dbDe(req), { olvidar_id: Number(req.params.id) })); }
  catch (e) { res.status(400).json({ error: e.message }); }
});

// ── WHATSAPP ─────────────────────────────────────────────────────────────────
// Twilio manda cada mensaje acá. Se responde vacío enseguida (Twilio corta a
// los 15 segundos) y la respuesta va después por la API de Twilio.
//
// Variables: TWILIO_SID, TWILIO_TOKEN · WHATSAPP_PERMITIDOS (números separados
// por coma; si no está, cualquiera puede hablarle) · URL_PUBLICA (para que los
// links de archivos y tableros sirvan desde el teléfono; si no está, se deduce
// del pedido) · WHATSAPP_CAMPOS (JSON número → clave de campo, si hay varios).

const WA_PERMITIDOS = String(process.env.WHATSAPP_PERMITIDOS || "").split(",").map(s => s.replace(/\D/g, "")).filter(Boolean);
let WA_CAMPOS = {}; try { WA_CAMPOS = JSON.parse(process.env.WHATSAPP_CAMPOS || "{}"); } catch (e) {}
const soloDigitos = s => String(s || "").replace(/\D/g, "");

// ── Los números del bot ──────────────────────────────────────────────────────
// Puede haber varios: cada uno con su cuenta de Twilio y su campo. Se arman con
// las variables que hay:
//   · el principal: TWILIO_NUMBER + TWILIO_SID/TWILIO_ACCOUNT_SID + TWILIO_TOKEN/TWILIO_AUTH_TOKEN → CAMPO_DEFAULT
//   · cada WHATSAPP_<SUFIJO> (ej. WHATSAPP_POSTA = "whatsapp:+549…") usa TWILIO_SID_<SUFIJO> y
//     TWILIO_TOKEN_<SUFIJO> si existen (si no, los principales) y el campo WHATSAPP_CAMPO_<SUFIJO>;
//     si no está, se busca un campo cuya clave contenga el sufijo (POSTA → angus_la_posta).
function numerosWhatsApp() {
  const sidBase = process.env.TWILIO_SID || process.env.TWILIO_ACCOUNT_SID || null;
  const tokenBase = process.env.TWILIO_TOKEN || process.env.TWILIO_AUTH_TOKEN || null;
  const lista = [];
  if (process.env.TWILIO_NUMBER) lista.push({ nombre: "principal", numero: soloDigitos(process.env.TWILIO_NUMBER), sid: sidBase, token: tokenBase, campo: CAMPO_DEFAULT });
  for (const [k, v] of Object.entries(process.env)) {
    const m = k.match(/^WHATSAPP_([A-Z0-9]+)$/);
    if (!m || ["PERMITIDOS", "CAMPOS"].includes(m[1]) || !soloDigitos(v)) continue;
    const suf = m[1];
    const campoExplicito = process.env["WHATSAPP_CAMPO_" + suf];
    const campoAdivinado = Object.keys(CAMPOS).find(c => c.toUpperCase().includes(suf)) || null;
    lista.push({ nombre: suf.toLowerCase(), numero: soloDigitos(v), sid: process.env["TWILIO_SID_" + suf] || sidBase,
      token: process.env["TWILIO_TOKEN_" + suf] || tokenBase, campo: (campoExplicito && CAMPOS[campoExplicito]) ? campoExplicito : campoAdivinado || CAMPO_DEFAULT });
  }
  return lista;
}
// A qué número le escribieron: dice la cuenta con la que responder y el campo.
function numeroDe(to) {
  const d = soloDigitos(to);
  const lista = numerosWhatsApp();
  const hallado = lista.find(n => n.numero && d && (n.numero === d || d.endsWith(n.numero) || n.numero.endsWith(d)));
  if (hallado) return hallado;
  // Con un solo número configurado, es ése. Con varios, no se adivina: contestar
  // por el campo equivocado es peor que decir que el número no está configurado.
  if (lista.length === 1) return lista[0];
  return { nombre: "desconocido", numero: d, desconocido: true, sid: (lista[0] || {}).sid || process.env.TWILIO_SID || process.env.TWILIO_ACCOUNT_SID || null,
    token: (lista[0] || {}).token || process.env.TWILIO_TOKEN || process.env.TWILIO_AUTH_TOKEN || null, campo: CAMPO_DEFAULT };
}
function clienteTwilio(cuenta) {
  const c = cuenta || numeroDe("");
  if (!c.sid || !c.token) return null;
  return require("twilio")(c.sid, c.token);
}

// WhatsApp corta en 1600 caracteres: se parte por párrafos, sin cortar palabras.
function partirMensaje(texto, max = 1500) {
  const partes = [];
  let resto = String(texto || "").trim();
  while (resto.length > max) {
    let corte = resto.lastIndexOf("\n\n", max);
    if (corte < max * 0.4) corte = resto.lastIndexOf("\n", max);
    if (corte < max * 0.4) corte = resto.lastIndexOf(". ", max) + 1;
    if (corte < max * 0.4) corte = resto.lastIndexOf(" ", max);
    if (corte <= 0) corte = max;
    partes.push(resto.slice(0, corte).trim());
    resto = resto.slice(corte).trim();
  }
  if (resto) partes.push(resto);
  return partes;
}

// Los links del sistema son relativos (/archivos/3/x.xlsx): desde el teléfono
// necesitan el dominio.
function absolutizar(texto, req) {
  const base = (process.env.URL_PUBLICA || `${req.protocol}://${req.get("host")}`).replace(/\/$/, "");
  return String(texto || "").replace(/(^|[\s(])(\/(archivos|t)\/[^\s)]+)/g, `$1${base}$2`);
}

async function enviarWhatsApp(twilio, from, to, texto) {
  for (const parte of partirMensaje(texto)) await twilio.messages.create({ from, to, body: parte });
}

// Lo que llega por WhatsApp (foto, PDF, planilla, audio…) se baja de Twilio.
async function bajarMedia(url, mimeDeclarado, cuenta) {
  const c = cuenta || numeroDe("");
  const auth = Buffer.from(`${c.sid}:${c.token}`).toString("base64");
  const r = await fetch(url, { headers: { Authorization: `Basic ${auth}` }, redirect: "follow" });
  if (!r.ok) throw new Error(`No pude bajar el adjunto (${r.status})`);
  const mime = (r.headers.get("content-type") || mimeDeclarado || "application/octet-stream").split(";")[0];
  const ext = { "image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "application/pdf": "pdf", "text/csv": "csv", "audio/ogg": "ogg",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx", "application/vnd.ms-excel": "xls",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx", "text/plain": "txt" }[mime] || "bin";
  return { nombre: `whatsapp_${Date.now()}.${ext}`, mime, buffer: Buffer.from(await r.arrayBuffer()) };
}

app.post("/webhook", async (req, res) => {
  res.type("text/xml").send("<Response></Response>");
  const de = req.body.From || "", a = req.body.To || "";
  const texto = String(req.body.Body || "").trim();
  const nMedia = Number(req.body.NumMedia || 0);
  if (!texto && !nMedia) return;

  const numero = soloDigitos(de);
  if (WA_PERMITIDOS.length && !WA_PERMITIDOS.includes(numero)) {
    console.log(`whatsapp: ${de} no está en WHATSAPP_PERMITIDOS, se ignora`);
    return;
  }
  // El número al que escribieron decide la cuenta y el campo; WHATSAPP_CAMPOS (por remitente) manda si está.
  const cuenta = numeroDe(a);
  const campoKey = CAMPOS[WA_CAMPOS[numero]] ? WA_CAMPOS[numero] : cuenta.campo;
  const db = getDB(campoKey);
  const twilio = clienteTwilio(cuenta);
  const emp = empresasDe().empresaDe(campoKey);
  console.log(`whatsapp: ${de} → ${a} (${cuenta.nombre}) · campo ${campoKey} · empresa ${emp.nombre}`);
  if (cuenta.desconocido) {
    const conocidos = numerosWhatsApp().map(n => `${n.nombre} (…${n.numero.slice(-4)} → ${(CAMPOS[n.campo] || {}).nombre || n.campo})`).join(", ");
    console.error(`whatsapp: el número ${a} no está configurado. Configurados: ${conocidos}`);
    if (twilio) { try { await twilio.messages.create({ from: a, to: de,
      body: `Este número (…${soloDigitos(a).slice(-4)}) todavía no está asignado a un campo, así que no sé de cuál contestarte. Falta agregarlo en el sistema. Configurados hoy: ${conocidos}.` }); } catch (x) {} }
    return;
  }

  // Si tarda, un aviso para que no parezca que no llegó.
  let respondido = false;
  const aviso = setTimeout(() => { if (!respondido && twilio) twilio.messages.create({ from: a, to: de, body: "Estoy mirando la base, un momento…" }).catch(() => {}); }, 9000);

  try {
    let mensaje = texto;
    if (nMedia) {
      const adjuntos = [];
      for (let i = 0; i < Math.min(nMedia, 4); i++) {
        try { adjuntos.push(await bajarMedia(req.body[`MediaUrl${i}`], req.body[`MediaContentType${i}`], cuenta)); }
        catch (e) { adjuntos.push({ nombre: `adjunto${i + 1}.bin`, mime: "application/octet-stream", buffer: Buffer.alloc(0) }); console.error("whatsapp media:", e.message); }
      }
      mensaje = adjuntosMod.preparar(db, { texto, adjuntos, canal: "whatsapp", usuario: de }).content;
    }
    const r = await bot.responder(db, CAMPOS[campoKey].nombre, mensaje, { campoKey, canal: "whatsapp", usuario: de || "desconocido" });
    respondido = true; clearTimeout(aviso);
    if (twilio) await enviarWhatsApp(twilio, a, de, absolutizar(r.respuesta, req));
    else console.log(`whatsapp (sin Twilio configurado) → ${de}: ${r.respuesta.slice(0, 200)}`);
  } catch (e) {
    respondido = true; clearTimeout(aviso);
    console.error("webhook:", e.message);
    if (twilio) { try { await twilio.messages.create({ from: a, to: de, body: `No pude: ${e.message}` }); } catch (x) {} }
  }
});


// ── TRAER LA BASE DESDE OTRO SERVIDOR ────────────────────────────────────────
// Railway no comparte volúmenes entre servicios. En vez de bajar el archivo a
// mano y volver a subirlo, este servidor se lo pide al viejo y lo guarda.
// Es para la puesta en marcha: una vez copiada la base, no se usa más.
app.post("/api/importar-base", async (req, res) => {
  const { url, campo, clave } = req.body;
  if (!url) return res.status(400).json({ error: "Falta la url del servidor viejo" });
  const destino = path.join(DB_DIR, `${campo || CAMPO_DEFAULT}.db`);

  try {
    if (fs.existsSync(destino) && !req.body.pisar) {
      const kb = Math.round(fs.statSync(destino).size / 1024);
      return res.status(409).json({
        error: `Ya hay una base en ${destino} de ${kb} KB. Mandá "pisar": true si querés reemplazarla.` });
    }
    const r = await fetch(url + (url.includes("?") ? "&" : "?") + `clave=${encodeURIComponent(clave || "")}`);
    if (!r.ok) return res.status(502).json({ error: `El servidor viejo respondió ${r.status}` });

    const buf = Buffer.from(await r.arrayBuffer());
    // Un SQLite empieza siempre con esta firma: si no está, bajó otra cosa.
    if (buf.slice(0, 15).toString() !== "SQLite format 3")
      return res.status(400).json({ error: "Lo que llegó no es una base SQLite. Revisá la url y la clave." });

    if (!fs.existsSync(DB_DIR)) fs.mkdirSync(DB_DIR, { recursive: true });
    fs.writeFileSync(destino, buf);
    // Se cierra la conexión vieja para que la próxima abra el archivo nuevo.
    const k = campo || CAMPO_DEFAULT;
    if (bases[k]) { try { bases[k].close(); } catch (e) {} delete bases[k]; }

    const db = getDB(k);
    const animales = db.prepare("SELECT COUNT(*) n FROM animales").get().n;
    const vientres = plantelMod.plantel(db).filas.length;
    res.json({ ok: true, archivo: destino, kb: Math.round(buf.length / 1024), animales, vientres,
      mensaje: `Listo: ${animales} animales, ${vientres} vientres.` });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── ENLACE CON EL FINANCIERO (IMPROLUX / VIDELA) ─────────────────────────────
// El financiero viene a buscar el stock acá (su "sync-ade" apunta a esta ruta).
app.get("/api/rodeo-resumen", (req, res) => {
  // ?empresa=… suma todos los campos de la empresa; ?campo=… uno solo.
  if (req.query.empresa) {
    try { return res.json(empresasDe().rodeoResumen(req.query.empresa)); } catch (e) { return res.status(400).json({ error: e.message }); }
  }
  const campoKey = CAMPOS[req.query.campo] ? req.query.campo : CAMPO_DEFAULT;
  try {
    const r = finanzasMod.resumenRodeo(getDB(campoKey), { campoKey, campoNombre: CAMPOS[campoKey].nombre, destinosMod });
    try { getDB(campoKey).prepare("INSERT INTO enlaces (direccion, que, detalle, ok, respuesta) VALUES ('recibido','stock',?,1,?)").run(`el financiero leyó el stock de ${campoKey}`, `${r.totales.cabezas} cabezas`); } catch (e) {}
    res.json(r);
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get("/api/finanzas/estado", async (req, res) => {
  const k = campoDe(req);
  try { res.json({ empresa: empresasDe().empresaDe(k).nombre, ...(await finanzasMod.estado(getDB(k), empresasDe().finanzasDe(k))) }); } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post("/api/finanzas/consultar", async (req, res) => {
  const k = campoDe(req);
  try { res.json(await finanzasMod.consultar(getDB(k), req.body || {}, empresasDe().finanzasDe(k))); } catch (e) { res.status(502).json({ error: e.message }); }
});
// Le pide al financiero que actualice su stock con todos los campos de la empresa.
app.post("/api/finanzas/sincronizar", async (req, res) => {
  const k = campoDe(req);
  const e = empresasDe().empresaDe(k);
  try { res.json(await finanzasMod.pedirSincronizacion(getDB(k), k, e.finanzas, e.campos.map(c => c.key))); } catch (e) { res.status(502).json({ error: e.message }); }
});

// ── VÍNCULOS ENTRE CAMPOS ────────────────────────────────────────────────────
// Qué animales apuntan a una madre o un padre que no está en su campo, dónde
// está realmente cada uno, y el arreglo.
app.get("/api/vinculos", (req, res) => {
  try {
    const k = campoDe(req);
    res.json(req.query.empresa === "1" || req.query.todos === "1"
      ? vinculosDe().revisarEmpresa(k, { estado: req.query.estado, limite: Number(req.query.limite) || undefined })
      : vinculosDe().revisar(k, { estado: req.query.estado, limite: Number(req.query.limite) || undefined, fresco: true }));
  } catch (e) { res.status(500).json({ error: e.message }); }
});
// Arreglar: sin filas, todo lo inequívoco. { filas: [{rp, relacion, campo, rp_madre}], simular }
app.post("/api/vinculos", (req, res) => {
  try { res.json(vinculosDe().aplicar(campoDe(req), req.body || {})); }
  catch (e) { res.status(400).json({ error: e.message }); }
});
// Padres que no son animales del campo (semen, toro prestado): se anotan y dejan de figurar.
app.get("/api/vinculos/externos", (req, res) => {
  try { res.json(vinculosDe().externos(campoDe(req))); } catch (e) { res.status(500).json({ error: e.message }); }
});
app.post("/api/vinculos/externos", (req, res) => {
  try { res.json(vinculosDe().marcarExternos(campoDe(req), req.body || {})); } catch (e) { res.status(400).json({ error: e.message }); }
});
app.delete("/api/vinculos/externos/:valor", (req, res) => {
  try { res.json(vinculosDe().olvidarExterno(campoDe(req), decodeURIComponent(req.params.valor))); } catch (e) { res.status(400).json({ error: e.message }); }
});

// El mismo ternero cargado en dos campos (cargas viejas): detectar y unificar.
app.get("/api/vinculos/duplicados", (req, res) => {
  try { res.json(vinculosDe().duplicados(campoDe(req), { fresco: true })); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
app.post("/api/vinculos/duplicados", (req, res) => {
  try { res.json(vinculosDe().unificar(campoDe(req), req.body || {})); }
  catch (e) { res.status(400).json({ error: e.message }); }
});

// Buscar un animal en todos los campos de la empresa.
app.get("/api/buscar-empresa", (req, res) => {
  try { res.json(vinculosDe().buscarEnEmpresa(campoDe(req), req.query.q, { excluir: req.query.excluir_propio === "1" ? campoDe(req) : null })); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// ── EMPRESAS ─────────────────────────────────────────────────────────────────
app.get("/api/empresas", (req, res) => res.json(empresasDe().lista()));
app.get("/api/empresa/resumen", (req, res) => {
  const key = req.query.empresa || empresasDe().empresaDe(campoDe(req)).key;
  try { res.json(conCruce().resumen(key)); } catch (e) { res.status(400).json({ error: e.message }); }
});
// Traslados entre campos de la misma empresa: { rps, desde, hasta, fecha?, motivo?, simular? }
app.post("/api/traslados", (req, res) => {
  const b = req.body || {};
  try { res.json(empresasDe().trasladar({ rps: b.rps || (b.texto ? relevarMod.parsearLineas(b.texto).map(l => l.rp) : []), desde: b.desde || campoDe(req), hasta: b.hasta, fecha: b.fecha, motivo: b.motivo, simular: b.simular, usuario: b.usuario })); }
  catch (e) { res.status(400).json({ error: e.message }); }
});

// Bajar una copia de la base (respaldo consistente, aunque esté en uso).
// Protegido con la variable RESPALDO_CLAVE: sin ella, esta ruta no existe.
//   curl -o principal.db "https://TU-APP.up.railway.app/api/respaldo?clave=LA_CLAVE&campo=principal"
app.get("/api/respaldo", async (req, res) => {
  const clave = process.env.RESPALDO_CLAVE || process.env.CLAVE_BACKUP;
  if (!clave) return res.status(404).send("No hay RESPALDO_CLAVE configurada");
  if (String(req.query.clave || "") !== clave) return res.status(403).send("Clave incorrecta");
  const k = CAMPOS[req.query.campo] ? req.query.campo : CAMPO_DEFAULT;
  const tmp = path.join(require("os").tmpdir(), `rodeo-respaldo-${k}-${Date.now()}.db`);
  try {
    await getDB(k).backup(tmp);
    res.setHeader("Content-Type", "application/x-sqlite3");
    res.setHeader("Content-Disposition", `attachment; filename="${k}_${new Date().toISOString().slice(0, 10)}.db"`);
    res.sendFile(tmp, () => { try { fs.unlinkSync(tmp); } catch (e) {} });
  } catch (e) { res.status(500).send(e.message); }
});

// Qué hay en el volumen.
app.get("/api/volumen", (req, res) => {
  try {
    if (!fs.existsSync(DB_DIR)) return res.json({ dir: DB_DIR, existe: false, archivos: [] });
    res.json({ dir: DB_DIR, existe: true, archivos: fs.readdirSync(DB_DIR).map(f => {
      const st = fs.statSync(path.join(DB_DIR, f));
      return { archivo: f, kb: Math.round(st.size / 1024), modificado: st.mtime };
    })});
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/salud", (req, res) => {
  const out = { version: VERSION, modelo: MODELO, esfuerzo: bot.esfuerzo, ...(ERRORES_CONFIG.length ? { errores_config: ERRORES_CONFIG } : {}),
    whatsapp: numerosWhatsApp().map(n => ({ numero: n.nombre, termina_en: n.numero.slice(-4), campo: n.campo, cuenta: n.sid ? "configurada" : "FALTA" })),
    empresas: empresasDe().lista(), campos: {} };
  for (const k of Object.keys(CAMPOS)) {
    try {
      const db = getDB(k);
      out.campos[k] = {
        nombre: CAMPOS[k].nombre,
        animales: db.prepare("SELECT COUNT(*) n FROM animales").get().n,
        vientres: plantelMod.plantel(db).filas.length,
        pesadas: db.prepare("SELECT COUNT(*) n FROM pesadas").get().n,
        archivos: db.prepare("SELECT COUNT(*) n FROM archivos").get().n
      };
    } catch (e) { out.campos[k] = { error: e.message }; }
  }
  res.json(out);
});

app.use(express.static(path.join(__dirname, "public")));
app.get("/", (req, res) => {
  const html = path.join(__dirname, "public", "index.html");
  if (fs.existsSync(html)) return res.sendFile(html);
  // Sin el tablero el sistema igual funciona: se avisa qué falta.
  res.type("html").send(`<!DOCTYPE html><html lang="es"><meta charset="utf-8">
    <title>RODEO</title>
    <body style="font-family:system-ui;max-width:640px;margin:60px auto;padding:0 20px;line-height:1.6;color:#10243f">
    <h1 style="letter-spacing:2px">RODEO ${VERSION}</h1>
    <p>El servidor está andando, pero falta el tablero.</p>
    <p>Subí el archivo <b>index.html</b> dentro de una carpeta <b>public</b> en el repo.
       En GitHub, al subirlo escribí el nombre como <code>public/index.html</code> — la barra
       crea la carpeta sola.</p>
    <p>Mientras tanto, el sistema responde por acá:</p>
    <ul>
      <li><a href="/api/salud">/api/salud</a> — qué ve el sistema</li>
      <li><a href="/api/plantel">/api/plantel</a> — los vientres</li>
      <li><a href="/api/animales">/api/animales</a> — todos los animales</li>
    </ul></body></html>`);
});

// Para poder probar las funciones sin levantar el servidor.
module.exports = { app, getDB, bot, guardarTablero, registrarSalida, empresasDe, vinculosDe, numerosWhatsApp, numeroDe, CAMPOS, CAMPO_DEFAULT, partirMensaje, absolutizar };

if (require.main === module) app.listen(PORT, () => {
  console.log(`${VERSION} en el puerto ${PORT} · modelo ${MODELO} · esfuerzo ${bot.esfuerzo}`);
  for (const k of Object.keys(CAMPOS)) {
    try {
      const db = getDB(k);
      const n = db.prepare("SELECT COUNT(*) n FROM animales").get().n;
      console.log(`  ${CAMPOS[k].nombre} (${k}): ${n} animales`);
    } catch (e) { console.log(`  ${k}: ${e.message}`); }
  }
});
