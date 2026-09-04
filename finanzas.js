// ─────────────────────────────────────────────────────────────────────────────
// FINANZAS — el enlace con IMPROLUX (o VIDELA), el sistema financiero.
//
// Tres flujos:
//   1. El financiero le pide a RODEO el stock por categoría para valuar el
//      patrimonio: GET /api/rodeo-resumen. Sale con la cantidad en plantel, la
//      cantidad marcada para venta y los kilos promedio reales de cada categoría.
//   2. Cuando en RODEO se registra una venta (salida con precio), se le manda
//      la transacción al financiero: POST {FINANZAS_URL}/api/transacciones.
//   3. El bot de RODEO puede leer el financiero (resumen, transacciones, stock
//      valuado) para contestar preguntas de plata.
//
// Variables: FINANZAS_URL (la dirección del financiero) · FINANZAS_CAMPO (cómo
// se llama este campo allá, ej. "AMAKAIK") · FINANZAS_CLAVE (opcional, se
// manda como header X-Clave si el financiero la pide).
// Todo lo que se manda o se recibe queda anotado en la tabla `enlaces`.
// ─────────────────────────────────────────────────────────────────────────────

let _fetch = (...a) => globalThis.fetch(...a);
const setFetch = f => { _fetch = f; };   // para las pruebas

function init(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS enlaces (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      direccion TEXT NOT NULL,      -- 'enviado' | 'recibido' | 'consulta'
      que TEXT NOT NULL,            -- 'venta' | 'stock' | 'resumen' | ...
      detalle TEXT,
      ok INTEGER DEFAULT 1,
      respuesta TEXT,
      created_at TEXT DEFAULT (datetime('now')));
  `);
}

// La configuración del financiero: la de la empresa si viene, si no la global.
const config = fin => fin && (fin.url || fin.campo || fin.clave) ? { url: String(fin.url || "").replace(/\/$/, ""), campo: fin.campo || null, clave: fin.clave || null } : ({
  url: String(process.env.FINANZAS_URL || "").replace(/\/$/, ""),
  campo: process.env.FINANZAS_CAMPO || null,
  clave: process.env.FINANZAS_CLAVE || null
});
const configurado = fin => !!config(fin).url;

function anotar(db, direccion, que, detalle, ok, respuesta) {
  try { db.prepare("INSERT INTO enlaces (direccion, que, detalle, ok, respuesta) VALUES (?,?,?,?,?)").run(direccion, que, detalle || null, ok ? 1 : 0, typeof respuesta === "string" ? respuesta.slice(0, 2000) : JSON.stringify(respuesta || null).slice(0, 2000)); } catch (e) {}
}

async function llamar(ruta, opciones = {}, fin) {
  const c = config(fin);
  if (!c.url) throw new Error("No está configurado FINANZAS_URL: el sistema financiero no está enlazado.");
  const headers = { "Content-Type": "application/json", ...(c.clave ? { "X-Clave": c.clave } : {}), ...(opciones.headers || {}) };
  // El financiero multiempresa lee ?empresa= y ?campo= (es lo que le inyecta el portal RODEO).
  let url = c.url + ruta;
  if (c.campo) { const u = new URL(url); if (!u.searchParams.has("empresa")) u.searchParams.set("empresa", c.campo); if (!u.searchParams.has("campo")) u.searchParams.set("campo", c.campo); url = u.toString(); }
  const r = await _fetch(url, { ...opciones, headers, signal: AbortSignal.timeout(opciones.timeout || 15000) });
  const texto = await r.text();
  let datos; try { datos = JSON.parse(texto); } catch (e) { datos = texto; }
  if (!r.ok) throw new Error(`El financiero respondió ${r.status}${datos && datos.error ? ": " + datos.error : ""}`);
  return datos;
}

// ── 1. EL STOCK, COMO LO LEE EL FINANCIERO ───────────────────────────────────
// Cada fila: categoría × registro (PP o GENERAL), cuántos quedan en el plantel,
// cuántos están marcados para venta, y los kilos promedio de las últimas pesadas.

function resumenRodeo(db, { campoKey, campoNombre, destinosMod } = {}) {
  const hoy = new Date().toISOString().slice(0, 10);
  const filas = db.prepare(`
    SELECT a.rp, upper(COALESCE(a.categoria,'SIN CATEGORIA')) categoria, upper(COALESCE(a.registro,'')) registro, a.sexo, a.fecha_nac,
      (SELECT peso FROM pesadas p WHERE p.animal_id=a.id ORDER BY p.fecha DESC, p.id DESC LIMIT 1) peso,
      (SELECT fecha FROM pesadas p WHERE p.animal_id=a.id ORDER BY p.fecha DESC, p.id DESC LIMIT 1) fecha_peso
    FROM animales a WHERE upper(COALESCE(a.estado,'ACTIVO'))='ACTIVO'`).all();
  let salen = new Set();
  try { salen = (destinosMod || require("./destinos.js")).destinadosASalir(db); } catch (e) {}

  const grupos = new Map();
  for (const f of filas) {
    const registro = /^PP$|PURO|PEDIGREE/.test(f.registro) ? "PP" : f.registro === "PC" ? "PC" : "GENERAL";
    const k = `${f.categoria}|${registro}`;
    if (!grupos.has(k)) grupos.set(k, { categoria: f.categoria, registro, plantel: 0, venta: 0, pesos: [], sin_peso: 0 });
    const g = grupos.get(k);
    if (salen.has(String(f.rp).toUpperCase())) g.venta++; else g.plantel++;
    // Una pesada de hace más de un año no dice el peso de hoy.
    if (f.peso && f.fecha_peso && (new Date(hoy) - new Date(f.fecha_peso)) / 86400000 <= 365) g.pesos.push(f.peso); else g.sin_peso++;
  }
  const orden = ["VACA", "VAQUILLONA", "TORO", "TORITO", "NOVILLO", "TERNERO", "TERNERA"];
  const categorias = [...grupos.values()].map(g => {
    const kg = g.pesos.length ? Math.round(g.pesos.reduce((a, b) => a + b, 0) / g.pesos.length) : null;
    return { categoria: g.categoria, registro: g.registro, plantel: g.plantel, venta: g.venta, cantidad: g.plantel + g.venta,
      kg_estimado: kg, kg_prom: kg, con_peso: g.pesos.length, sin_peso: g.sin_peso, kg_total: kg ? kg * (g.plantel + g.venta) : null };
  }).sort((a, b) => (orden.indexOf(a.categoria) + 1 || 99) - (orden.indexOf(b.categoria) + 1 || 99) || a.registro.localeCompare(b.registro));
  const total = categorias.reduce((s, c) => s + c.cantidad, 0);
  return { campo: campoKey || null, nombre: campoNombre || null, generado: new Date().toISOString(), fuente: "rodeo",
    categorias, rodeo: categorias,   // 'rodeo' por compatibilidad con la lectura vieja
    totales: { cabezas: total, plantel: categorias.reduce((s, c) => s + c.plantel, 0), venta: categorias.reduce((s, c) => s + c.venta, 0),
      kg_total: categorias.reduce((s, c) => s + (c.kg_total || 0), 0) } };
}

// ── 2. LAS VENTAS VAN AL FINANCIERO ──────────────────────────────────────────
/**
 * Una venta: uno o varios animales que salieron el mismo día al mismo comprador.
 * `precio` es el total en la moneda del financiero; `precio_por_cabeza` si viene
 * así; `kg` los kilos vendidos si se pesaron.
 */
async function enviarVenta(db, venta, fin) {
  const { rps = [], fecha, comprador, detalle } = venta;
  const total = venta.precio_total != null ? Number(venta.precio_total)
    : venta.precio_por_cabeza != null ? Number(venta.precio_por_cabeza) * rps.length
    : venta.precio != null ? Number(venta.precio) : 0;
  const cats = db.prepare(`SELECT categoria, COUNT(*) n FROM animales WHERE upper(rp) IN (${rps.map(() => "?").join(",") || "''"}) GROUP BY categoria`).all(...rps.map(r => String(r).toUpperCase()));
  const queSeVende = cats.map(c => `${c.n} ${String(c.categoria || "animal").toLowerCase()}${c.n === 1 ? "" : c.categoria && /A$/.test(c.categoria) ? "s" : "s"}`).join(", ");
  const texto = detalle || `Venta ${queSeVende} (RP ${rps.join(", ")})${venta.kg ? ` · ${venta.kg} kg` : ""}${comprador ? ` · ${comprador}` : ""} · desde RODEO`;
  if (!configurado(fin)) { anotar(db, "enviado", "venta", texto, false, "sin FINANZAS_URL"); return { ok: false, enviado: false, motivo: "El financiero no está enlazado (FINANZAS_URL o EMPRESAS)", detalle: texto }; }
  if (!(total > 0)) { anotar(db, "enviado", "venta", texto, false, "sin precio"); return { ok: false, enviado: false, motivo: "Sin precio: no se manda al financiero", detalle: texto }; }
  const cuerpo = { fecha: fecha || new Date().toISOString().slice(0, 10), concepto: venta.concepto || "VENTA HACIENDA", detalle: texto,
    ingreso: total, proveedor: comprador || "", fuente: "rodeo", campo: config(fin).campo || undefined };
  try {
    const r = await llamar("/api/transacciones", { method: "POST", body: JSON.stringify(cuerpo) }, fin);
    anotar(db, "enviado", "venta", texto, true, r);
    return { ok: true, enviado: true, id_financiero: r && r.id, monto: total, detalle: texto };
  } catch (e) {
    anotar(db, "enviado", "venta", texto, false, e.message);
    return { ok: false, enviado: false, motivo: e.message, detalle: texto };
  }
}

// ── 3. LEER EL FINANCIERO ────────────────────────────────────────────────────
/**
 * consulta: "resumen" (el mes en curso), "transacciones" (con desde/hasta/concepto/texto),
 * "ganado" (el stock valuado que tiene el financiero), "cuentas", "cheques".
 */
async function consultar(db, { consulta = "resumen", desde, hasta, concepto, texto, limite = 200 } = {}, fin) {
  const c = String(consulta).toLowerCase();
  let datos;
  if (c === "transacciones") {
    const todas = await llamar(`/api/transacciones?limite=3000`, {}, fin);
    let lista = Array.isArray(todas) ? todas : (todas.transacciones || []);
    if (desde) lista = lista.filter(t => String(t.fecha) >= desde);
    if (hasta) lista = lista.filter(t => String(t.fecha) <= hasta);
    if (concepto) lista = lista.filter(t => String(t.concepto || "").toUpperCase().includes(String(concepto).toUpperCase()));
    if (texto) lista = lista.filter(t => `${t.detalle || ""} ${t.proveedor || ""}`.toUpperCase().includes(String(texto).toUpperCase()));
    const porConcepto = {};
    for (const t of lista) { const k = t.concepto || "?"; porConcepto[k] = porConcepto[k] || { ingresos: 0, egresos: 0, n: 0 }; porConcepto[k].ingresos += Number(t.ingreso) || 0; porConcepto[k].egresos += Number(t.egreso) || 0; porConcepto[k].n++; }
    datos = { total: lista.length, ingresos: lista.reduce((s, t) => s + (Number(t.ingreso) || 0), 0), egresos: lista.reduce((s, t) => s + (Number(t.egreso) || 0), 0),
      por_concepto: porConcepto, transacciones: lista.slice(0, limite).map(t => ({ fecha: t.fecha, concepto: t.concepto, detalle: t.detalle, ingreso: t.ingreso, egreso: t.egreso, proveedor: t.proveedor })) };
  } else if (c === "ganado") datos = await llamar("/api/ganado", {}, fin);
  else if (c === "cuentas") datos = await llamar("/api/cuentas", {}, fin);
  else if (c === "cheques") datos = await llamar("/api/cheques", {}, fin);
  else datos = await llamar("/api/resumen", {}, fin);
  anotar(db, "consulta", c, JSON.stringify({ desde, hasta, concepto, texto }), true, `${JSON.stringify(datos).length} bytes`);
  return datos;
}

// ── ESTADO Y PRUEBA ──────────────────────────────────────────────────────────

async function estado(db, fin) {
  const c = config(fin);
  const ultimos = (() => { try { return db.prepare("SELECT direccion, que, detalle, ok, respuesta, created_at FROM enlaces ORDER BY id DESC LIMIT 20").all(); } catch (e) { return []; } })();
  const out = { configurado: !!c.url, url: c.url || null, campo: c.campo, con_clave: !!c.clave, ultimos };
  if (c.url) {
    try { const r = await llamar("/api/resumen", { timeout: 8000 }, fin); out.conecta = true; out.resumen = r; }
    catch (e) { out.conecta = false; out.error = e.message; }
  }
  return out;
}

// Le pide al financiero que venga a buscar el stock (él llama a /api/rodeo-resumen).
async function pedirSincronizacion(db, campoKey, fin, todosLosCampos) {
  const campos = Array.isArray(todosLosCampos) && todosLosCampos.length ? todosLosCampos : [campoKey];
  const r = await llamar("/api/ganado/sync-ade", { method: "POST", body: JSON.stringify({ campo_ade: campoKey, campos_ade: campos, campo: config(fin).campo || undefined }) }, fin);
  anotar(db, "recibido", "stock", `sync pedido para ${campoKey}`, true, r);
  return r;
}

module.exports = { init, config, configurado, resumenRodeo, enviarVenta, consultar, estado, pedirSincronizacion, setFetch, llamar };
