// ─────────────────────────────────────────────────────────────────────────────
// TRASLADOS Y CONSOLIDADO POR EMPRESA
//
// Cada campo tiene su propia base. Eso está bien para el día a día — cada uno
// declara lo suyo ante SENASA o DICOSE — pero rompe dos cosas cuando la empresa
// tiene varios campos:
//
//   1. Mover un animal significaba darlo de baja y volver a crearlo, perdiendo
//      pesadas, servicios, crías y costos. Acá se lleva todo.
//   2. Un toro usado en dos campos mostraba su progenie partida en dos. Acá se
//      suma: los hijos y los promedios son los de la empresa entera.
// ─────────────────────────────────────────────────────────────────────────────

// Las tablas que cuelgan de un animal, con la columna que lo referencia.
// Se mueven en este orden para que las dependencias existan al insertarse.
const TABLAS_HIJAS = [
  { tabla: "pesadas",    fk: "animal_id" },
  { tabla: "mediciones", fk: "animal_id" },
  { tabla: "sanidad",    fk: "animal_id" },
  { tabla: "servicios",  fk: "animal_id" },
  { tabla: "costos",     fk: "animal_id" },
  { tabla: "eventos",    fk: "animal_id" }
];

function init(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS traslados (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fecha TEXT NOT NULL,
      campo_origen TEXT NOT NULL,
      campo_destino TEXT NOT NULL,
      rps TEXT NOT NULL,              -- los animales movidos, separados por coma
      cantidad INTEGER NOT NULL,
      guia TEXT,                      -- DTe / guía de traslado
      motivo TEXT,
      usuario TEXT,
      detalle TEXT,                   -- qué se movió de cada animal
      created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_traslados_fecha ON traslados(fecha);
  `);
}

function columnasDe(db, tabla) {
  try { return db.prepare(`PRAGMA table_info(${tabla})`).all().map(c => c.name); }
  catch (e) { return []; }
}

function existeTabla(db, tabla) {
  try { return !!db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(tabla); }
  catch (e) { return false; }
}

// Copia una fila respetando las columnas que existen en AMBAS bases: si un
// campo tiene una columna que el otro no, se ignora en vez de fallar.
function copiarFila(destino, tabla, fila, colsDestino, reemplazos = {}) {
  const datos = {};
  for (const c of colsDestino) {
    if (c === "id") continue;                       // el destino asigna el suyo
    if (c in reemplazos) { datos[c] = reemplazos[c]; continue; }
    if (c in fila) datos[c] = fila[c];
  }
  const cols = Object.keys(datos);
  if (!cols.length) return null;
  const sql = `INSERT INTO ${tabla} (${cols.join(",")}) VALUES (${cols.map(() => "?").join(",")})`;
  return destino.prepare(sql).run(...cols.map(c => datos[c])).lastInsertRowid;
}

// ── TRASLADO ─────────────────────────────────────────────────────────────────

/**
 * Mueve animales de un campo a otro con toda su historia.
 * @param bases  { origen, destino } instancias de la base de cada campo
 * @param rps    lista de RP a mover
 * @param datos  { fecha, guia, motivo, usuario, nombreOrigen, nombreDestino }
 */
function trasladar(bases, rps, datos) {
  const { origen, destino } = bases;
  const fecha = datos.fecha || new Date().toISOString().slice(0, 10);

  const movidos = [], errores = [], resumen = [];

  // Antes de tocar nada: verificar que se puedan mover todos.
  for (const rp of rps) {
    const a = origen.prepare("SELECT * FROM animales WHERE upper(rp)=upper(?)").get(String(rp).trim());
    if (!a) { errores.push(`${rp}: no está en el campo de origen`); continue; }
    if (a.estado && a.estado !== "ACTIVO") { errores.push(`${rp}: está ${a.estado}`); continue; }
    const ya = destino.prepare("SELECT rp FROM animales WHERE upper(rp)=upper(?)").get(a.rp);
    if (ya) { errores.push(`${rp}: ya existe en el campo de destino`); continue; }
    movidos.push(a);
  }
  if (!movidos.length) return { ok: false, error: "No se pudo mover ningún animal.", errores };

  const colsAnimalesDestino = columnasDe(destino, "animales");
  let idTraslado = null;

  const correr = origen.transaction(() => {
    for (const a of movidos) {
      // 1. El animal, con su nota de procedencia.
      const notas = [a.notas, `Trasladado desde ${datos.nombreOrigen || "otro campo"} el ${fecha}`]
        .filter(Boolean).join(" · ");
      const nuevoId = copiarFila(destino, "animales", a, colsAnimalesDestino, { notas });
      if (!nuevoId) { errores.push(`${a.rp}: no se pudo crear en el destino`); continue; }

      // 2. Toda su historia, tabla por tabla.
      const detalle = [];
      for (const { tabla, fk } of TABLAS_HIJAS) {
        if (!existeTabla(origen, tabla) || !existeTabla(destino, tabla)) continue;
        const filas = origen.prepare(`SELECT * FROM ${tabla} WHERE ${fk} = ?`).all(a.id);
        if (!filas.length) continue;
        const cols = columnasDe(destino, tabla);
        let n = 0;
        for (const f of filas) {
          if (copiarFila(destino, tabla, f, cols, { [fk]: nuevoId })) n++;
        }
        if (n) detalle.push(`${n} ${tabla}`);
      }

      // 3. Recién ahora se borra del origen: si algo falló antes, no se pierde.
      for (const { tabla, fk } of TABLAS_HIJAS) {
        if (!existeTabla(origen, tabla)) continue;
        try { origen.prepare(`DELETE FROM ${tabla} WHERE ${fk} = ?`).run(a.id); } catch (e) {}
      }
      origen.prepare("DELETE FROM animales WHERE id = ?").run(a.id);

      resumen.push({ rp: a.rp, historia: detalle.join(", ") || "sin historial" });
    }

    // 4. El traslado queda registrado en las dos bases.
    const reg = {
      fecha, campo_origen: datos.campoOrigen, campo_destino: datos.campoDestino,
      rps: resumen.map(r => r.rp).join(","), cantidad: resumen.length,
      guia: datos.guia || null, motivo: datos.motivo || null,
      usuario: datos.usuario || null,
      detalle: resumen.map(r => `${r.rp}: ${r.historia}`).join(" | ")
    };
    const cols = Object.keys(reg);
    const sql = `INSERT INTO traslados (${cols.join(",")}) VALUES (${cols.map(() => "?").join(",")})`;
    idTraslado = origen.prepare(sql).run(...cols.map(c => reg[c])).lastInsertRowid;
    try { destino.prepare(sql).run(...cols.map(c => reg[c])); } catch (e) {}
  });
  correr();

  return {
    ok: true, id: idTraslado, movidos: resumen.length, errores, resumen,
    mensaje: `🚚 ${resumen.length} animal${resumen.length > 1 ? "es" : ""} de ` +
      `${datos.nombreOrigen} a ${datos.nombreDestino}.\n` +
      resumen.slice(0, 8).map(r => `   ${r.rp} · ${r.historia}`).join("\n") +
      (resumen.length > 8 ? `\n   …y ${resumen.length - 8} más` : "") +
      (errores.length ? `\n\n⚠️ ${errores.length} no se movieron:\n   ${errores.slice(0, 5).join("\n   ")}` : "")
  };
}

// ── CONSOLIDADO POR EMPRESA ──────────────────────────────────────────────────
// Un toro usado en varios campos tiene su progenie repartida. Para evaluarlo hay
// que sumarla: dos hijos acá y tres allá son cinco hijos del mismo padre.

/**
 * Progenie de cada padre sumando todos los campos de la empresa.
 * @param campos [{ key, nombre, db }]
 */
function progenieConsolidada(campos, filtroPadre) {
  const acum = new Map();

  for (const { key, nombre, db } of campos) {
    let filas = [];
    try {
      filas = db.prepare(`
        SELECT a.padre_rp AS padre, a.rp, a.sexo, a.fecha_nac,
               (SELECT peso FROM pesadas p WHERE p.animal_id = a.id AND p.contexto='NACIMIENTO'
                ORDER BY p.fecha LIMIT 1) AS peso_nac,
               (SELECT peso FROM pesadas p WHERE p.animal_id = a.id AND p.contexto='DESTETE'
                ORDER BY p.fecha DESC LIMIT 1) AS peso_destete,
               (SELECT gdp FROM pesadas p WHERE p.animal_id = a.id AND p.gdp IS NOT NULL
                ORDER BY p.fecha DESC LIMIT 1) AS gdp
        FROM animales a
        WHERE a.padre_rp IS NOT NULL AND trim(a.padre_rp) <> ''
      `).all();
    } catch (e) { continue; }

    for (const f of filas) {
      const padre = String(f.padre).trim().toUpperCase();
      if (filtroPadre && padre !== String(filtroPadre).trim().toUpperCase()) continue;
      if (!acum.has(padre)) {
        acum.set(padre, { padre: String(f.padre).trim(), hijos: 0, campos: {},
                          pesos_nac: [], pesos_destete: [], gdps: [], machos: 0, hembras: 0 });
      }
      const r = acum.get(padre);
      r.hijos++;
      r.campos[nombre] = (r.campos[nombre] || 0) + 1;
      if (String(f.sexo || "").toUpperCase().startsWith("M")) r.machos++; else r.hembras++;
      if (f.peso_nac > 0) r.pesos_nac.push(f.peso_nac);
      if (f.peso_destete > 0) r.pesos_destete.push(f.peso_destete);
      if (f.gdp > 0) r.gdps.push(f.gdp);
    }
  }

  const prom = a => a.length ? Math.round((a.reduce((x, y) => x + y, 0) / a.length) * 100) / 100 : null;
  return [...acum.values()].map(r => ({
    padre: r.padre,
    hijos: r.hijos, machos: r.machos, hembras: r.hembras,
    campos: r.campos,
    // Cuántos campos lo usaron: un toro probado en varios tiene mejor evaluación.
    en_campos: Object.keys(r.campos).length,
    peso_nac_prom: prom(r.pesos_nac),
    peso_destete_prom: prom(r.pesos_destete),
    gdp_prom: prom(r.gdps),
    con_datos: { nac: r.pesos_nac.length, destete: r.pesos_destete.length, gdp: r.gdps.length }
  })).sort((a, b) => b.hijos - a.hijos);
}

// Resumen de la empresa entera, para ver el rodeo completo de un vistazo.
function resumenEmpresa(campos) {
  const out = { campos: [], total: 0, por_categoria: {}, machos: 0, hembras: 0 };
  for (const { nombre, db } of campos) {
    let n = 0, cats = [];
    try {
      n = db.prepare("SELECT COUNT(*) n FROM animales WHERE estado='ACTIVO'").get().n;
      cats = db.prepare(`SELECT categoria, COUNT(*) n,
                         SUM(CASE WHEN upper(COALESCE(sexo,'')) LIKE 'M%' THEN 1 ELSE 0 END) m
                         FROM animales WHERE estado='ACTIVO' GROUP BY categoria`).all();
    } catch (e) {}
    out.campos.push({ nombre, animales: n });
    out.total += n;
    for (const c of cats) {
      const k = c.categoria || "SIN CATEGORÍA";
      out.por_categoria[k] = (out.por_categoria[k] || 0) + c.n;
      out.machos += c.m || 0;
      out.hembras += c.n - (c.m || 0);
    }
  }
  return out;
}

function historialTraslados(db, limite = 100) {
  try {
    return db.prepare("SELECT * FROM traslados ORDER BY fecha DESC, id DESC LIMIT ?").all(limite);
  } catch (e) { return []; }
}

module.exports = { init, trasladar, progenieConsolidada, resumenEmpresa, historialTraslados, TABLAS_HIJAS };
