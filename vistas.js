// ─────────────────────────────────────────────────────────────────────────────
// VISTAS DEFINIDAS POR EL USUARIO
//
// El admin le pide algo al bot — "compará el bloque de este servicio con el
// anterior y marcá las que se atrasaron" — y eso queda guardado como una
// DEFINICIÓN, no como código. El tablero la lee y arma la columna.
//
// Por qué así y no reescribiendo el HTML:
//   · las definiciones se acumulan en vez de pisarse
//   · si una falla, falla esa sola y el tablero sigue andando
//   · sirven para todos los campos de la empresa
//   · el bot puede explicar qué entendió antes de guardar
//
// Nada de esto ejecuta código arbitrario: cada definición es una combinación de
// operaciones conocidas sobre campos conocidos.
// ─────────────────────────────────────────────────────────────────────────────

// Lo que se puede leer de cada vaca. Si no está acá, no se puede usar.
const CAMPOS = {
  rp:            { etiqueta: "RP", tipo: "texto" },
  bloque:        { tipo: "bloque", etiqueta: "bloque de parición" },
  parto:         { tipo: "fecha",  etiqueta: "fecha de parto" },
  servicio:      { tipo: "fecha",  etiqueta: "fecha de servicio" },
  padre:         { tipo: "texto",  etiqueta: "padre" },
  tacto:         { tipo: "texto",  etiqueta: "resultado del tacto" },
  destete:       { tipo: "numero", etiqueta: "kg al destete" },
  peso_nac:      { tipo: "numero", etiqueta: "peso al nacer" },
  peso_adulto:   { tipo: "numero", etiqueta: "peso de la vaca" },
  productividad: { tipo: "numero", etiqueta: "kg destetados por 100 kg de vaca" },
  ipp:           { tipo: "numero", etiqueta: "días entre partos" },
  crias:         { tipo: "numero", etiqueta: "cantidad de crías" },
  edad:          { tipo: "numero", etiqueta: "edad en años" },
  destete_ok:    { tipo: "bool",   etiqueta: "si destetó" }
};

// El orden importa: es lo que permite decir si una vaca mejoró o empeoró.
const ORDEN_BLOQUE = ["CABEZA", "CUERPO", "COLA", "TARDIA", "VACIA"];

function init(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS vistas_def (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      empresa TEXT NOT NULL,       -- vale para todos los campos de la empresa
      pantalla TEXT NOT NULL,      -- cria | recria | toros
      tipo TEXT NOT NULL,          -- columna | marca | kpi
      nombre TEXT NOT NULL,        -- lo que se ve en el encabezado
      definicion TEXT NOT NULL,    -- JSON con la operación
      pedido TEXT,                 -- lo que dijo el usuario, textual
      activa INTEGER DEFAULT 1,
      orden INTEGER DEFAULT 100,
      creada_por TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_vistas_emp ON vistas_def(empresa, pantalla, activa);
  `);
}

// ── LO QUE EL BOT PUEDE ARMAR ────────────────────────────────────────────────
//
// Cada definición es un objeto con "op" y sus parámetros. Estas son todas las
// operaciones posibles: si el bot pide otra cosa, se rechaza.
//
//  comparar_temporadas  { campo, temporadas:[a,b] }  → cómo cambió entre dos años
//  formula              { expr }                      → cuenta entre campos numéricos
//  clasificar           { campo, rangos:[...] }       → etiqueta según el valor
//  contar               { filtro }                     → un número arriba
//  regla                { filtro, color, texto }       → marca filas

const OPS = ["comparar_temporadas", "formula", "clasificar", "contar", "regla"];

function validar(def) {
  if (!def || !OPS.includes(def.op)) return `Operación desconocida: ${def && def.op}`;
  if (def.op === "comparar_temporadas") {
    if (!CAMPOS[def.campo]) return `No conozco el dato "${def.campo}"`;
  }
  if (def.op === "formula") {
    // Sólo campos conocidos, números y las cuatro operaciones. Nada más.
    const limpio = String(def.expr || "").replace(/\b[a-z_]+\b/g, m => CAMPOS[m] ? "N" : "?");
    if (limpio.includes("?")) return "La fórmula usa un dato que no conozco";
    if (!/^[N0-9+\-*/().\s]+$/.test(limpio)) return "La fórmula tiene algo que no puedo calcular";
  }
  return null;
}

function guardar(db, empresa, d) {
  const err = validar(d.definicion);
  if (err) return { ok: false, error: err };
  const info = db.prepare(`INSERT INTO vistas_def (empresa,pantalla,tipo,nombre,definicion,pedido,orden,creada_por)
                           VALUES (?,?,?,?,?,?,?,?)`)
    .run(empresa, d.pantalla || "cria", d.tipo || "columna", d.nombre,
         JSON.stringify(d.definicion), d.pedido || null, d.orden || 100, d.creada_por || null);
  return { ok: true, id: info.lastInsertRowid };
}

function listar(db, empresa, pantalla) {
  try {
    const filas = pantalla
      ? db.prepare("SELECT * FROM vistas_def WHERE empresa=? AND pantalla=? AND activa=1 ORDER BY orden, id").all(empresa, pantalla)
      : db.prepare("SELECT * FROM vistas_def WHERE empresa=? AND activa=1 ORDER BY pantalla, orden, id").all(empresa);
    return filas.map(f => ({ ...f, definicion: JSON.parse(f.definicion) }));
  } catch (e) { return []; }
}

function borrar(db, empresa, id) {
  const r = db.prepare("UPDATE vistas_def SET activa=0 WHERE id=? AND empresa=?").run(id, empresa);
  return { ok: !!r.changes, error: r.changes ? null : "No encontré esa columna" };
}

// ── APLICAR LAS DEFINICIONES A LOS DATOS ─────────────────────────────────────

function valorDe(fila, campo) {
  const v = fila[campo];
  return v === undefined ? null : v;
}

// Compara un dato entre dos temporadas. Para bloques usa el orden, para números
// la diferencia. Es lo que responde "¿qué vaca pasó de cola a cabeza?".
function compararTemporadas(fila, def, historia) {
  const [a, b] = def.temporadas || [];
  const h = (historia && historia[fila.rp]) || {};
  const va = h[a], vb = h[b];
  if (va == null || vb == null) return { valor: null, texto: "—", nivel: null };

  const tipo = (CAMPOS[def.campo] || {}).tipo;
  if (tipo === "bloque") {
    const ia = ORDEN_BLOQUE.indexOf(String(va).toUpperCase());
    const ib = ORDEN_BLOQUE.indexOf(String(vb).toUpperCase());
    if (ia < 0 || ib < 0) return { valor: null, texto: "—", nivel: null };
    const dif = ia - ib;   // el más nuevo es "b": si b está antes, mejoró
    return {
      valor: dif,
      texto: dif > 0 ? `${va} → ${vb}` : dif < 0 ? `${va} → ${vb}` : `sigue en ${vb}`,
      nivel: dif > 0 ? "mejor" : dif < 0 ? "peor" : "igual",
      // Lo que uno quiere leer de un vistazo
      resumen: dif > 0 ? `adelantó ${dif} bloque${dif > 1 ? "s" : ""}`
             : dif < 0 ? `se atrasó ${-dif} bloque${dif < -1 ? "s" : ""}`
             : "sin cambio"
    };
  }
  const na = parseFloat(va), nb = parseFloat(vb);
  if (isNaN(na) || isNaN(nb)) return { valor: null, texto: `${va} → ${vb}`, nivel: null };
  const dif = Math.round((nb - na) * 100) / 100;
  return { valor: dif, texto: `${dif > 0 ? "+" : ""}${dif}`,
           nivel: dif > 0 ? "mejor" : dif < 0 ? "peor" : "igual",
           resumen: `${dif > 0 ? "subió" : dif < 0 ? "bajó" : "sin cambio"} ${Math.abs(dif)}` };
}

function calcularFormula(fila, expr) {
  try {
    const nombres = Object.keys(CAMPOS);
    const vals = nombres.map(n => { const v = parseFloat(fila[n]); return isNaN(v) ? 0 : v; });
    // Sólo se pasan los campos conocidos: el resto del alcance no está disponible.
    const f = new Function(...nombres, `"use strict"; return (${expr});`);
    const r = f(...vals);
    return Number.isFinite(r) ? Math.round(r * 100) / 100 : null;
  } catch (e) { return null; }
}

function clasificar(valor, rangos) {
  if (valor == null) return null;
  for (const r of rangos || []) {
    const okMin = r.desde == null || valor >= r.desde;
    const okMax = r.hasta == null || valor < r.hasta;
    if (okMin && okMax) return r;
  }
  return null;
}

// Un filtro simple: { campo, op, valor }. Con "y" se encadenan.
function cumple(fila, filtro) {
  if (!filtro) return true;
  if (Array.isArray(filtro.y)) return filtro.y.every(f => cumple(fila, f));
  const v = valorDe(fila, filtro.campo);
  const c = filtro.valor;
  switch (filtro.op) {
    case "=":  return String(v).toUpperCase() === String(c).toUpperCase();
    case "!=": return String(v).toUpperCase() !== String(c).toUpperCase();
    case ">":  return parseFloat(v) > parseFloat(c);
    case "<":  return parseFloat(v) < parseFloat(c);
    case ">=": return parseFloat(v) >= parseFloat(c);
    case "<=": return parseFloat(v) <= parseFloat(c);
    case "es_null": return v == null;
    case "no_null": return v != null;
    default: return false;
  }
}

/**
 * Aplica todas las definiciones a las filas ya calculadas.
 * @param historia  { rp: { "2025": valor, "2024": valor } } para las comparaciones
 */
function aplicar(filas, defs, historia) {
  const columnas = [], kpis = [], marcas = [];

  for (const d of defs) {
    const def = d.definicion;
    try {
      if (d.tipo === "columna") {
        columnas.push({ id: d.id, nombre: d.nombre, tipo: def.op, pedido: d.pedido });
        for (const f of filas) {
          f.extra = f.extra || {};
          if (def.op === "comparar_temporadas") f.extra[d.id] = compararTemporadas(f, def, historia);
          else if (def.op === "formula") {
            const v = calcularFormula(f, def.expr);
            f.extra[d.id] = { valor: v, texto: v == null ? "—" : String(v), nivel: null };
          } else if (def.op === "clasificar") {
            const r = clasificar(valorDe(f, def.campo), def.rangos);
            f.extra[d.id] = { valor: r ? r.etiqueta : null, texto: r ? r.etiqueta : "—", nivel: r ? r.nivel : null };
          }
        }
      } else if (d.tipo === "marca") {
        marcas.push({ id: d.id, nombre: d.nombre, color: def.color, pedido: d.pedido });
        for (const f of filas) {
          if (cumple(f, def.filtro)) {
            f.marcas = f.marcas || [];
            f.marcas.push({ id: d.id, nombre: d.nombre, color: def.color || "amarillo", texto: def.texto || d.nombre });
          }
        }
      } else if (d.tipo === "kpi") {
        const n = def.op === "contar" ? filas.filter(f => cumple(f, def.filtro)).length
                : filas.reduce((a, f) => a + (parseFloat(valorDe(f, def.campo)) || 0), 0);
        kpis.push({ id: d.id, nombre: d.nombre, valor: def.op === "contar" ? n : Math.round(n * 100) / 100 });
      }
    } catch (e) {
      // Una definición rota no puede tirar abajo el tablero.
      columnas.push({ id: d.id, nombre: d.nombre, error: String(e.message).slice(0, 80) });
    }
  }
  return { columnas, kpis, marcas };
}

// ── HISTORIA POR TEMPORADA ───────────────────────────────────────────────────
// Para comparar años hace falta el valor de cada vaca en cada temporada.

function historiaBloques(db) {
  const out = {};
  try {
    const crias = db.prepare(`
      SELECT madre_rp rp, fecha_nac FROM animales
      WHERE madre_rp IS NOT NULL AND fecha_nac IS NOT NULL`).all();
    for (const c of crias) {
      const anio = String(c.fecha_nac).slice(0, 4);
      const md = String(c.fecha_nac).slice(5, 10);
      const bloque = md <= "08-31" ? "CABEZA" : md <= "09-30" ? "CUERPO" : md <= "10-31" ? "COLA" : "TARDIA";
      const rp = String(c.rp).toUpperCase();
      if (!out[rp]) out[rp] = {};
      // Si parió dos veces en el año, vale el primero.
      if (!out[rp][anio] || c.fecha_nac < out[rp][anio + "_f"]) {
        out[rp][anio] = bloque; out[rp][anio + "_f"] = c.fecha_nac;
      }
    }
  } catch (e) {}
  return out;
}

function historiaCampo(db, campo) {
  if (campo === "bloque") return historiaBloques(db);
  const out = {};
  try {
    if (campo === "destete") {
      db.prepare(`SELECT a.madre_rp rp, a.fecha_nac,
                  (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='DESTETE' ORDER BY p.fecha DESC LIMIT 1) v
                  FROM animales a WHERE a.madre_rp IS NOT NULL AND a.fecha_nac IS NOT NULL`).all()
        .forEach(c => { if (c.v == null) return;
          const rp = String(c.rp).toUpperCase(); const anio = String(c.fecha_nac).slice(0, 4);
          if (!out[rp]) out[rp] = {}; out[rp][anio] = c.v; });
    }
  } catch (e) {}
  return out;
}

// ── LO QUE EL BOT LE EXPLICA AL USUARIO ──────────────────────────────────────

function explicar(d) {
  const def = d.definicion;
  if (def.op === "comparar_temporadas") {
    const c = (CAMPOS[def.campo] || {}).etiqueta || def.campo;
    return `Una columna "${d.nombre}" que compara ${c} de ${def.temporadas[1]} contra ${def.temporadas[0]}, ` +
           `y muestra si mejoró o empeoró.`;
  }
  if (def.op === "formula") return `Una columna "${d.nombre}" que calcula: ${def.expr}`;
  if (def.op === "clasificar") {
    const c = (CAMPOS[def.campo] || {}).etiqueta || def.campo;
    return `Una columna "${d.nombre}" que clasifica según ${c}: ` +
      (def.rangos || []).map(r => `${r.etiqueta} ${r.desde != null ? `desde ${r.desde}` : ""}${r.hasta != null ? ` hasta ${r.hasta}` : ""}`).join(", ");
  }
  if (d.tipo === "marca") return `Marcar en ${def.color || "amarillo"} las que cumplan: ${describirFiltro(def.filtro)}`;
  if (d.tipo === "kpi") return `Un número arriba, "${d.nombre}": ${def.op === "contar" ? "cuántas cumplen " + describirFiltro(def.filtro) : "suma de " + def.campo}`;
  return d.nombre;
}

function describirFiltro(f) {
  if (!f) return "todas";
  if (Array.isArray(f.y)) return f.y.map(describirFiltro).join(" y ");
  const c = (CAMPOS[f.campo] || {}).etiqueta || f.campo;
  const ops = { "=": "es", "!=": "no es", ">": "mayor a", "<": "menor a", ">=": "al menos", "<=": "como mucho",
                es_null: "está vacío", no_null: "tiene dato" };
  return `${c} ${ops[f.op] || f.op} ${f.valor != null ? f.valor : ""}`.trim();
}

module.exports = {
  init, guardar, listar, borrar, aplicar, validar, explicar, describirFiltro,
  historiaCampo, historiaBloques, compararTemporadas, cumple,
  CAMPOS, OPS, ORDEN_BLOQUE
};
