// ─────────────────────────────────────────────────────────────────────────────
// EXPORTAR — emisión de archivos.
//
// Cada cosa que se ve en el tablero se puede bajar como Excel, CSV, página
// imprimible o JSON. Y no sólo lo que se ve: hay conjuntos que existen sólo
// para exportar (todas las pesadas, todos los servicios, la sanidad) porque
// a veces lo que se necesita es la planilla entera para trabajarla afuera.
//
// El mismo módulo guarda los archivos que arma el bot, para que "dame un
// excel de las vacías" termine en un link que se puede bajar.
// ─────────────────────────────────────────────────────────────────────────────
const xlsx = require("./xlsx.js");

const hoyIso = () => new Date().toISOString().slice(0, 10);
const fechaAr = f => f ? `${String(f).slice(8, 10)}/${String(f).slice(5, 7)}/${String(f).slice(0, 4)}` : "";
// "011" y "11" son el mismo RP: la comparación se hace como en la búsqueda.
const { compacto } = require("./animales.js");

function init(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS archivos (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      nombre TEXT NOT NULL,
      mime TEXT NOT NULL,
      bytes BLOB NOT NULL,
      tamano INTEGER,
      descripcion TEXT,
      creado_por TEXT,
      created_at TEXT DEFAULT (datetime('now')));
  `);
}

// ── COLUMNAS DE CADA CONJUNTO ────────────────────────────────────────────────
// Las mismas para el Excel, el CSV y la impresión. El tipo manda cómo sale la
// celda: entero, decimal, fecha, porcentaje o texto.

const F = "fecha", E = "entero", D = "decimal", P = "porcentaje";
const sexoTexto = s => !s ? null : String(s).toUpperCase().startsWith("M") ? "Macho" : "Hembra";

const COLUMNAS = {
  plantel: [
    { k: "rp", t: "RP", ancho: 9 }, { k: "hba", t: "HBA" }, { k: "chip", t: "Caravana elec." },
    { k: "pelo", t: "Pelaje" }, { k: "categoria", t: "Categoría" }, { k: "fecha_nac", t: "Nacimiento", tipo: F },
    { k: "edad_meses", t: "Edad (m)", tipo: E }, { k: "padre", t: "Padre" }, { k: "madre", t: "Madre" },
    { k: "peso_adulto", t: "Peso adulto", tipo: D }, { k: "partos", t: "Partos", tipo: E },
    { k: "pn_prom", t: "PN prom", tipo: D }, { k: "destete_prom", t: "Destete prom", tipo: D },
    { k: "eficiencia", t: "Eficiencia", tipo: P }, { k: "ipp", t: "Interv. partos (d)", tipo: E },
    { k: "primera_prenez", t: "1ª preñez (m)", tipo: E },
    { k: "estado", t: "Estado" }, { k: "causa_texto", t: "Causa" },
    { k: "servicio", t: "Servicio", tipo: F }, { k: "padre_servicio", t: "Padre servicio" }, { k: "tacto", t: "Tacto" },
    { k: "fpp", t: "Fecha prob. parto", tipo: F }, { k: "parto", t: "Parto", tipo: F }, { k: "ternero", t: "Ternero" },
    { k: "peso_nac", t: "Peso nac ternero", tipo: D }, { k: "destete", t: "Destete ternero", tipo: D },
    { k: "bloque", t: "Bloque" }, { k: "se_atrasa", t: "Se atrasa" }, { k: "notas_texto", t: "Notas", ancho: 40 }
  ],
  animales: [
    { k: "rp", t: "RP", ancho: 9 }, { k: "nombre", t: "Nombre" }, { k: "chip", t: "Caravana elec." }, { k: "caravana_control", t: "Caravana control" }, { k: "caravana_color", t: "Color control" }, { k: "hbu", t: "HBA" }, { k: "registro", t: "Registro" },
    { k: "sexo_texto", t: "Sexo" }, { k: "categoria", t: "Categoría" }, { k: "estado", t: "Estado" },
    { k: "fecha_nac", t: "Nacimiento", tipo: F }, { k: "edad_meses", t: "Edad (m)", tipo: E },
    { k: "pelo", t: "Pelaje" }, { k: "raza", t: "Raza" }, { k: "madre_rp", t: "Madre" }, { k: "padre_rp", t: "Padre" },
    { k: "peso_nac", t: "Peso nac", tipo: D }, { k: "destete", t: "Destete", tipo: D }, { k: "fecha_destete", t: "Fecha destete", tipo: F },
    { k: "peso_actual", t: "Último peso", tipo: D }, { k: "ultima_pesada", t: "Última pesada", tipo: F },
    { k: "gdp_destete", t: "GDP desde destete", tipo: D }, { k: "n_pesadas", t: "Pesadas", tipo: E },
    { k: "crias", t: "Crías", tipo: E }, { k: "lote_actual", t: "Lote" }, { k: "potrero", t: "Potrero" },
    { k: "notas", t: "Notas", ancho: 40 }
  ],
  nacimientos: [
    { k: "rp", t: "RP", ancho: 9 }, { k: "rp_provisorio_texto", t: "RP" }, { k: "caravana_control", t: "Caravana control" }, { k: "caravana_color", t: "Color" },
    { k: "fecha_nac", t: "Nacimiento", tipo: F }, { k: "bloque", t: "Bloque" },
    { k: "sexo_texto", t: "Sexo" }, { k: "pelo", t: "Pelaje" }, { k: "madre_rp", t: "Madre" }, { k: "padre_rp", t: "Padre" },
    { k: "peso_nac", t: "Peso nac", tipo: D }, { k: "destete", t: "Destete", tipo: D },
    { k: "peso_actual", t: "Último peso", tipo: D }, { k: "ultima_pesada", t: "Última pesada", tipo: F },
    { k: "chip", t: "Caravana elec." }, { k: "estado", t: "Estado" }, { k: "notas", t: "Notas", ancho: 40 }
  ],
  recria: [
    { k: "rp", t: "RP", ancho: 9 }, { k: "fecha_nac", t: "Nacimiento", tipo: F }, { k: "edad_meses", t: "Edad (m)", tipo: E },
    { k: "sexo_texto", t: "Sexo" }, { k: "categoria", t: "Categoría" }, { k: "pelo", t: "Pelaje" },
    { k: "madre_rp", t: "Madre" }, { k: "padre_rp", t: "Padre" },
    { k: "destete", t: "Destete", tipo: D }, { k: "fecha_destete", t: "Fecha destete", tipo: F },
    { k: "peso_actual", t: "Peso", tipo: D }, { k: "ultima_pesada", t: "Última pesada", tipo: F },
    { k: "gdp_destete", t: "GDP desde destete", tipo: D }, { k: "dias_sin_pesar", t: "Días sin pesar", tipo: E },
    { k: "lote_actual", t: "Lote" }, { k: "potrero", t: "Potrero" }
  ],
  terminacion: [
    { k: "rp", t: "RP", ancho: 9 }, { k: "origen", t: "Origen" }, { k: "lote", t: "Lote" }, { k: "potrero", t: "Potrero" }, { k: "destino", t: "Destino" }, { k: "categoria", t: "Categoría" },
    { k: "sexo_texto", t: "Sexo" }, { k: "meses", t: "Edad (m)", tipo: E }, { k: "padre_rp", t: "Padre" },
    { k: "fecha_ingreso", t: "Ingresó", tipo: F }, { k: "dias_corral", t: "Días en corral", tipo: E },
    { k: "destete", t: "Destete", tipo: D }, { k: "peso_entrada", t: "Peso entrada", tipo: D },
    { k: "peso_actual", t: "Peso actual", tipo: D }, { k: "ganancia", t: "Ganó (kg)", tipo: D }, { k: "gdp", t: "GDP", tipo: D },
    { k: "ultima_pesada", t: "Última pesada", tipo: F }, { k: "dias_sin_pesar", t: "Días sin pesar", tipo: E }
  ],
  destinos: [
    { k: "rp", t: "RP", ancho: 9 }, { k: "destino", t: "Destino" }, { k: "motivo_texto", t: "Motivo" },
    { k: "categoria", t: "Categoría" }, { k: "pelo", t: "Pelaje" }, { k: "edad_meses", t: "Edad (m)", tipo: E },
    { k: "peso_adulto", t: "Peso", tipo: D }, { k: "partos", t: "Partos", tipo: E }, { k: "destete_prom", t: "Destete prom", tipo: D },
    { k: "eficiencia", t: "Eficiencia", tipo: P }, { k: "estado", t: "Estado" },
    { k: "fecha", t: "Decidido", tipo: F }, { k: "concretado_texto", t: "Salió" }, { k: "fecha_salida", t: "Fecha salida", tipo: F },
    { k: "precio", t: "Precio", tipo: D }, { k: "nota", t: "Nota", ancho: 40 }
  ],
  fallos: [
    { k: "rp", t: "RP", ancho: 9 }, { k: "causa_texto", t: "Motivo" }, { k: "tacto", t: "Tacto" },
    { k: "servicio", t: "Servicio", tipo: F }, { k: "fpp", t: "Fecha prob. parto", tipo: F },
    { k: "categoria", t: "Categoría" }, { k: "edad_meses", t: "Edad (m)", tipo: E }, { k: "peso_adulto", t: "Peso adulto", tipo: D },
    { k: "partos", t: "Partos", tipo: E }, { k: "destete_prom", t: "Destete prom", tipo: D }, { k: "eficiencia", t: "Eficiencia", tipo: P },
    { k: "notas_texto", t: "Notas", ancho: 40 }
  ],
  pesadas: [
    { k: "rp", t: "RP", ancho: 9 }, { k: "fecha", t: "Fecha", tipo: F }, { k: "peso", t: "Peso", tipo: D },
    { k: "contexto", t: "Contexto" }, { k: "gdp", t: "GDP desde anterior", tipo: D }, { k: "dias", t: "Días desde anterior", tipo: E },
    { k: "sexo_texto", t: "Sexo" }, { k: "categoria", t: "Categoría" }, { k: "fecha_nac", t: "Nacimiento", tipo: F }
  ],
  servicios: [
    { k: "rp", t: "RP", ancho: 9 }, { k: "temporada", t: "Temporada" }, { k: "tipo_servicio", t: "Tipo" },
    { k: "semen_iatf", t: "Semen IATF" }, { k: "fecha_iatf", t: "Fecha IATF", tipo: F }, { k: "toro_natural", t: "Toro" },
    { k: "fecha_ingreso_toro", t: "Ingreso toro", tipo: F }, { k: "fecha_salida_toro", t: "Salida toro", tipo: F },
    { k: "resultado", t: "Tacto" }, { k: "fecha_tacto", t: "Fecha tacto", tipo: F }, { k: "notas", t: "Notas", ancho: 30 }
  ],
  sanidad: [
    { k: "rp", t: "RP", ancho: 9 }, { k: "fecha", t: "Fecha", tipo: F }, { k: "producto", t: "Producto" },
    { k: "dosis", t: "Dosis" }, { k: "motivo", t: "Motivo" }, { k: "categoria", t: "Categoría" }
  ],
  mediciones: [
    { k: "rp", t: "RP", ancho: 9 }, { k: "fecha", t: "Fecha", tipo: F }, { k: "tipo", t: "Medición" },
    { k: "valor", t: "Valor", tipo: D }, { k: "categoria", t: "Categoría" }
  ],
  notas: [
    { k: "animal_rp", t: "RP", ancho: 9 }, { k: "fecha", t: "Fecha", tipo: F }, { k: "texto", t: "Nota", ancho: 50 },
    { k: "causa", t: "Entendido como" }, { k: "grave_texto", t: "Grave" }, { k: "usuario", t: "Quién" }
  ],
  toros: [
    { k: "rp", t: "RP", ancho: 9 }, { k: "nombre", t: "Nombre" }, { k: "hba", t: "HBA" }, { k: "chip", t: "Caravana elec." },
    { k: "pelo", t: "Pelaje" }, { k: "categoria", t: "Categoría" }, { k: "estado", t: "Estado" },
    { k: "fecha_nac", t: "Nacimiento", tipo: F }, { k: "edad_meses", t: "Edad (m)", tipo: E }, { k: "padre", t: "Padre" }, { k: "madre", t: "Madre" },
    { k: "peso_actual", t: "Peso", tipo: D }, { k: "ultima_pesada", t: "Última pesada", tipo: F }, { k: "ce", t: "CE", tipo: D },
    { k: "hijos", t: "Hijos", tipo: E }, { k: "hijos_anio", t: "Hijos del año", tipo: E }, { k: "machos", t: "Machos", tipo: E }, { k: "hembras", t: "Hembras", tipo: E },
    { k: "pn_prom_hijos", t: "PN prom hijos", tipo: D }, { k: "destete_prom_hijos", t: "Destete prom hijos", tipo: D },
    { k: "servicios", t: "Servicios", tipo: E }, { k: "temporadas", t: "Temporadas" }, { k: "prenez", t: "Preñez", tipo: P },
    { k: "lote", t: "Lote" }, { k: "potrero", t: "Potrero" }, { k: "destino", t: "Destino" }, { k: "notas", t: "Notas", ancho: 40 }
  ],
  lotes: [
    { k: "lote", t: "Lote" }, { k: "potrero", t: "Potrero" }, { k: "rp", t: "RP", ancho: 9 },
    { k: "categoria", t: "Categoría" }, { k: "sexo_texto", t: "Sexo" }, { k: "fecha_ingreso", t: "Ingresó", tipo: F },
    { k: "peso_actual", t: "Último peso", tipo: D }, { k: "ultima_pesada", t: "Última pesada", tipo: F }
  ]
};

const NOMBRES = {
  plantel: "Plantel", toros: "Toros", animales: "Animales", nacimientos: "Nacimientos", recria: "Recría",
  terminacion: "Terminación", destinos: "Destinos", fallos: "No destetaron", pesadas: "Pesadas",
  servicios: "Servicios", sanidad: "Sanidad", mediciones: "Mediciones", notas: "Notas de campo", lotes: "Lotes"
};

// ── LOS DATOS DE CADA CONJUNTO ───────────────────────────────────────────────

function datos(db, mods, clave, opciones = {}) {
  const { plantelMod, animalesMod, destinosMod } = mods;
  const hoy = hoyIso();
  const conSexo = filas => filas.map(f => ({ ...f, sexo_texto: sexoTexto(f.sexo) }));
  const rpDe = db.prepare("SELECT rp, sexo, categoria, fecha_nac FROM animales WHERE id=?");
  const conRp = filas => filas.map(f => { const a = rpDe.get(f.animal_id) || {}; return { ...f, rp: a.rp, sexo_texto: sexoTexto(a.sexo), categoria: a.categoria, fecha_nac: a.fecha_nac }; });

  switch (clave) {
    case "plantel": case "fallos": {
      const p = plantelMod.plantel(db, { anio: opciones.anio });
      let filas = p.filas.map(f => ({ ...f, notas_texto: (f.notas || []).map(n => `${fechaAr(n.fecha)} ${n.texto}`).join(" · ") || null,
        se_atrasa: f.se_atrasa ? "sí" : null }));
      if (clave === "fallos") filas = filas.filter(f => f.estado === "FALLÓ");
      return { filas, subtitulo: `Parición ${p.anio_paricion} · ${filas.length} vientres` };
    }
    case "toros": {
      const t = animalesMod.toros(db, { estado: opciones.estado || "ACTIVO", anio: opciones.anio });
      return { filas: t.filas, subtitulo: `${t.resumen.total} toros · ${t.resumen.hijos_totales} hijos registrados` };
    }
    case "animales": {
      const filas = conSexo(animalesMod.listar(db, { estado: opciones.estado || "ACTIVO" }));
      return { filas, subtitulo: `${filas.length} animales ${String(opciones.estado || "activos").toLowerCase()}` };
    }
    case "nacimientos": {
      const p = plantelMod.plantel(db, { anio: opciones.anio });
      const anio = opciones.anio || p.anio_paricion;
      const filas = conSexo(animalesMod.listar(db, { estado: "TODOS" })
        .filter(a => String(a.fecha_nac || "").startsWith(anio) && a.madre_rp)
        .map(a => ({ ...a, bloque: plantelMod.bloqueDe(a.fecha_nac, p.calendario.cortes), rp_provisorio_texto: a.rp_provisorio ? "provisorio" : "definitivo" })));
      return { filas, subtitulo: `Nacidos en ${anio} · ${filas.length} terneros` };
    }
    case "recria": {
      const filas = conSexo(animalesMod.listar(db).filter(a => a.edad_meses != null && a.edad_meses >= 6 && a.edad_meses <= 20));
      return { filas, subtitulo: `De 6 a 20 meses · ${filas.length} animales` };
    }
    case "terminacion": {
      const t = animalesMod.terminacion(db);
      return { filas: conSexo(t.filas), subtitulo: `${t.resumen.total} terminando (${t.resumen.en_corral} en corral, ${t.resumen.marcados} marcados) · ${t.resumen.kg_totales || 0} kg` };
    }
    case "destinos": {
      if (!destinosMod) return { filas: [], subtitulo: "" };
      const toros = animalesMod.toros(db, { incluirDestinados: true }).filas.map(t => ({ rp: t.rp, categoria: t.categoria, pelo: t.pelo, edad_meses: t.edad_meses, peso_adulto: t.peso_actual, partos: t.hijos, destete_prom: t.destete_prom_hijos, estado: t.estado }));
      const p = plantelMod.plantel(db, { incluirDestinados: true });
      const d = destinosMod.listar(db, [...p.filas, ...toros], { temporada: opciones.temporada });
      return { filas: d.filas.map(f => ({ ...f, concretado_texto: f.concretado ? "sí" : "pendiente" })),
        subtitulo: `Temporada ${d.resumen.temporada} · ${d.resumen.marcados} marcados` };
    }
    case "pesadas": {
      const filas = conRp(db.prepare("SELECT * FROM pesadas ORDER BY animal_id, fecha, id").all());
      let prev = null;
      for (const f of filas) {
        if (prev && prev.animal_id === f.animal_id) {
          const d = Math.round((new Date(f.fecha) - new Date(prev.fecha)) / 86400000);
          f.dias = d; f.gdp = d > 0 ? Math.round((f.peso - prev.peso) / d * 1000) / 1000 : null;
        }
        prev = f;
      }
      filas.sort((a, b) => String(b.fecha).localeCompare(String(a.fecha)) || String(a.rp).localeCompare(String(b.rp), "es", { numeric: true }));
      return { filas, subtitulo: `${filas.length} pesadas` };
    }
    case "servicios": return { filas: conRp(db.prepare("SELECT * FROM servicios ORDER BY temporada DESC, animal_id").all()), subtitulo: "Todos los servicios" };
    case "sanidad": return { filas: conRp(db.prepare("SELECT * FROM sanidad ORDER BY fecha DESC, animal_id").all()), subtitulo: "Todo lo aplicado" };
    case "mediciones": return { filas: conRp(db.prepare("SELECT * FROM mediciones ORDER BY fecha DESC, animal_id").all()), subtitulo: "Todas las mediciones" };
    case "notas": {
      const filas = db.prepare("SELECT * FROM notas_campo ORDER BY fecha DESC, id DESC").all()
        .map(n => ({ ...n, grave_texto: n.grave ? "sí" : null }));
      return { filas, subtitulo: `${filas.length} notas` };
    }
    case "lotes": {
      const filas = conSexo(db.prepare(`
        SELECT l.nombre lote, l.potrero, a.rp, a.sexo, a.categoria, la.fecha_ingreso,
          (SELECT peso FROM pesadas p WHERE p.animal_id=a.id ORDER BY p.fecha DESC LIMIT 1) peso_actual,
          (SELECT fecha FROM pesadas p WHERE p.animal_id=a.id ORDER BY p.fecha DESC LIMIT 1) ultima_pesada
        FROM lote_animales la JOIN lotes l ON l.id=la.lote_id JOIN animales a ON a.id=la.animal_id
        ORDER BY l.nombre, a.rp`).all());
      return { filas, subtitulo: `${new Set(filas.map(f => f.lote)).size} lotes` };
    }
    default: throw new Error(`No existe el conjunto "${clave}". Los que hay: ${Object.keys(COLUMNAS).join(", ")}`);
  }
}

/**
 * Un conjunto listo para exportar: columnas, filas y títulos.
 *   opciones.rps       sólo estos RP (lo que el tablero tiene filtrado)
 *   opciones.columnas  sólo estas claves, en este orden
 *   opciones.orden     { col, desc }
 */
function conjunto(db, mods, clave, opciones = {}) {
  if (!COLUMNAS[clave]) throw new Error(`No existe el conjunto "${clave}". Los que hay: ${Object.keys(COLUMNAS).join(", ")}`);
  let { filas, subtitulo } = datos(db, mods, clave, opciones);
  let columnas = COLUMNAS[clave];
  const rpKey = clave === "notas" ? "animal_rp" : "rp";

  if (Array.isArray(opciones.rps) && opciones.rps.length) {
    const set = new Set(opciones.rps.map(compacto));
    filas = filas.filter(f => set.has(compacto(f[rpKey])));
    subtitulo = `${filas.length} seleccionados`;
  }
  if (Array.isArray(opciones.columnas) && opciones.columnas.length) {
    const quiere = opciones.columnas.map(String);
    const porClave = Object.fromEntries(columnas.map(c => [c.k, c]));
    columnas = quiere.map(k => porClave[k]).filter(Boolean);
    if (!columnas.length) columnas = COLUMNAS[clave];
  }
  if (opciones.orden && opciones.orden.col) {
    const c = opciones.orden.col, desc = !!opciones.orden.desc;
    filas = [...filas].sort((a, b) => {
      const x = a[c], y = b[c];
      if (x == null && y == null) return 0;
      if (x == null) return 1; if (y == null) return -1;
      const r = (typeof x === "number" && typeof y === "number") ? x - y : String(x).localeCompare(String(y), "es", { numeric: true });
      return desc ? -r : r;
    });
  }
  return { clave, nombre: NOMBRES[clave], titulo: NOMBRES[clave], subtitulo, columnas, filas };
}

// ── FORMATOS ─────────────────────────────────────────────────────────────────

/** CSV para Excel en español: separador ; y coma decimal. `sep: ","` para el otro. */
function csv(columnas, filas, opciones = {}) {
  const sep = opciones.sep || ";";
  const decimal = sep === ";" ? "," : ".";
  const celda = (v, c) => {
    if (v == null) return "";
    if (typeof v === "number") return String(v).replace(".", decimal);
    if (typeof v === "boolean") return v ? "sí" : "no";
    if (c && c.tipo === "fecha" && /^\d{4}-\d{2}-\d{2}/.test(String(v))) v = fechaAr(v);
    if (c && c.tipo === "porcentaje" && v !== "") return String(v).replace(".", decimal) + "%";
    v = String(v);
    return /[";\n\r,]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v;
  };
  const lineas = [columnas.map(c => celda(c.t)).join(sep)];
  for (const f of filas) lineas.push(columnas.map(c => celda(f[c.k], c)).join(sep));
  return "﻿" + lineas.join("\r\n");
}

/** Página lista para imprimir o guardar como PDF desde el navegador. */
function htmlImprimible({ titulo, subtitulo, columnas, filas, campoNombre, volver }) {
  const esc = s => String(s == null ? "" : s).replace(/[<>&"]/g, c => ({ "<": "&lt;", ">": "&gt;", "&": "&amp;", '"': "&quot;" }[c]));
  const fmt = (v, c) => {
    if (v == null || v === "") return '<span class="mut">—</span>';
    if (c.tipo === "fecha") return esc(fechaAr(v));
    if (c.tipo === "porcentaje") return esc(v) + "%";
    if (typeof v === "boolean") return v ? "sí" : "no";
    if (typeof v === "number") return esc(Number.isInteger(v) ? v : Math.round(v * 10) / 10);
    return esc(v);
  };
  const num = c => ["entero", "decimal", "porcentaje"].includes(c.tipo);
  return `<!DOCTYPE html><html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${esc(titulo)} · ${esc(campoNombre || "RODEO")}</title>
<link href="https://fonts.googleapis.com/css2?family=Oswald:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
:root{--azul:#0B3D7C;--azul2:#072957;--oro:#C9A24B;--tinta:#10243f;--papel:#F7F3EC;--linea:#E2D9CB;--gris:#8A827A}
*{box-sizing:border-box}
body{margin:0;background:#fff;color:var(--tinta);font-family:Oswald,system-ui,sans-serif;font-weight:300;font-size:12px}
header{display:flex;align-items:flex-end;gap:16px;padding:18px 22px 12px;border-bottom:3px solid var(--oro)}
header h1{margin:0;font-size:20px;font-weight:600;letter-spacing:2.5px;text-transform:uppercase;color:var(--azul2)}
header p{margin:2px 0 0;font-size:11px;color:var(--gris);letter-spacing:1.5px;text-transform:uppercase}
header .campo{margin-left:auto;text-align:right;font-size:11px;color:var(--gris);letter-spacing:1px;text-transform:uppercase}
.acciones{padding:10px 22px;display:flex;gap:8px;background:var(--papel);border-bottom:1px solid var(--linea)}
.acciones button,.acciones a{font-family:inherit;font-size:11px;letter-spacing:1.2px;text-transform:uppercase;padding:7px 14px;
  border:1px solid var(--linea);background:#fff;color:var(--tinta);border-radius:3px;cursor:pointer;text-decoration:none}
.acciones button.p{background:var(--azul);color:#fff;border-color:var(--azul)}
main{padding:14px 22px 40px}
table{width:100%;border-collapse:collapse}
th{background:var(--azul2);color:#fff;text-align:left;font-size:9px;letter-spacing:1.1px;text-transform:uppercase;
  font-weight:400;padding:7px 6px;white-space:nowrap}
td{padding:5px 6px;border-bottom:1px solid var(--linea);white-space:nowrap}
tbody tr:nth-child(even) td{background:#FAF7F0}
th.n,td.n{text-align:right;font-variant-numeric:tabular-nums}
.mut{color:#bbb}
tfoot td{padding-top:10px;color:var(--gris);font-size:10px;border:0}
@page{size:landscape;margin:12mm}
@media print{.acciones{display:none} body{font-size:10px} th{-webkit-print-color-adjust:exact;print-color-adjust:exact}
  tbody tr:nth-child(even) td{-webkit-print-color-adjust:exact;print-color-adjust:exact} thead{display:table-header-group}}
</style></head><body>
<header><div><h1>${esc(titulo)}</h1>${subtitulo ? `<p>${esc(subtitulo)}</p>` : ""}</div>
<div class="campo">${esc(campoNombre || "")}<br>${fechaAr(hoyIso())}</div></header>
<div class="acciones"><button class="p" onclick="window.print()">Imprimir / guardar PDF</button>
${volver ? `<a href="${esc(volver)}">Volver al tablero</a>` : ""}</div>
<main><table><thead><tr>${columnas.map(c => `<th class="${num(c) ? "n" : ""}">${esc(c.t)}</th>`).join("")}</tr></thead>
<tbody>${filas.map(f => `<tr>${columnas.map(c => `<td class="${num(c) ? "n" : ""}">${fmt(f[c.k], c)}</td>`).join("")}</tr>`).join("\n")}</tbody>
<tfoot><tr><td colspan="${columnas.length}">${filas.length} filas · Generado por RODEO el ${fechaAr(hoyIso())}</td></tr></tfoot>
</table></main></body></html>`;
}

const FORMATOS = {
  xlsx: { mime: xlsx.MIME, ext: "xlsx" },
  csv: { mime: "text/csv; charset=utf-8", ext: "csv" },
  html: { mime: "text/html; charset=utf-8", ext: "html" },
  json: { mime: "application/json; charset=utf-8", ext: "json" }
};

const nombreArchivo = (base, ext) => `${String(base).normalize("NFD").replace(/[̀-ͯ]/g, "").replace(/[^A-Za-z0-9_-]+/g, "_").replace(/^_|_$/g, "")}_${hoyIso()}.${ext}`;

/**
 * Arma el archivo de un conjunto (o de varios, si `clave` es "rodeo": todo en
 * un Excel con una hoja por conjunto).
 */
function armar(db, mods, clave, formato, opciones = {}) {
  const f = FORMATOS[formato];
  if (!f) throw new Error(`Formato "${formato}" no. Puede ser: ${Object.keys(FORMATOS).join(", ")}`);
  const campoNombre = opciones.campoNombre || "";

  if (clave === "rodeo") {
    if (formato !== "xlsx") throw new Error("El rodeo completo sólo sale en Excel: es una hoja por conjunto");
    const claves = ["plantel", "toros", "nacimientos", "recria", "terminacion", "destinos", "fallos", "animales", "pesadas", "servicios", "sanidad", "notas", "lotes"];
    const hojas = claves.map(k => { const c = conjunto(db, mods, k, { anio: opciones.anio });
      return { nombre: c.nombre, titulo: `${c.titulo} · ${campoNombre}`, subtitulo: c.subtitulo, columnas: c.columnas, filas: c.filas }; })
      .filter(h => h.filas.length || ["plantel", "animales"].includes(h.nombre.toLowerCase()));
    return { buffer: xlsx.armar(hojas), mime: f.mime, nombre: nombreArchivo(`Rodeo_${campoNombre || "completo"}`, "xlsx") };
  }

  const c = conjunto(db, mods, clave, opciones);
  const nombre = nombreArchivo(`${c.nombre}${campoNombre ? "_" + campoNombre : ""}`, f.ext);
  const sub = [c.subtitulo, opciones.filtro].filter(Boolean).join(" · ");
  if (formato === "xlsx")
    return { buffer: xlsx.armar([{ nombre: c.nombre, titulo: `${c.titulo}${campoNombre ? " · " + campoNombre : ""}`, subtitulo: sub, columnas: c.columnas, filas: c.filas }]), mime: f.mime, nombre };
  if (formato === "csv") return { buffer: Buffer.from(csv(c.columnas, c.filas, { sep: opciones.sep }), "utf8"), mime: f.mime, nombre };
  if (formato === "html") return { buffer: Buffer.from(htmlImprimible({ ...c, subtitulo: sub, campoNombre, volver: opciones.volver }), "utf8"), mime: f.mime, nombre, inline: true };
  return { buffer: Buffer.from(JSON.stringify({ conjunto: c.clave, titulo: c.titulo, subtitulo: sub, columnas: c.columnas, filas: c.filas }, null, 1), "utf8"), mime: f.mime, nombre };
}

// ── TABLAS DENTRO DE UN TABLERO DEL BOT ──────────────────────────────────────
// El bot arma páginas HTML. Para bajarlas como Excel se leen las <table> que
// tienen adentro y se convierten a filas.

function tablasDeHtml(html) {
  const limpiar = s => String(s).replace(/<[^>]+>/g, " ").replace(/&nbsp;/g, " ").replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/\s+/g, " ").trim();
  const tablas = [];
  const re = /<h2[^>]*>([\s\S]*?)<\/h2>|<table[\s\S]*?<\/table>/gi;
  let m, ultimoTitulo = null;
  while ((m = re.exec(html))) {
    if (m[1] !== undefined) { ultimoTitulo = limpiar(m[1]); continue; }
    const filas = [...m[0].matchAll(/<tr[\s\S]*?<\/tr>/gi)].map(tr => [...tr[0].matchAll(/<t[hd][^>]*>([\s\S]*?)<\/t[hd]>/gi)].map(c => limpiar(c[1])));
    if (filas.length < 2) continue;
    const enc = filas[0];
    const cols = enc.map((t, i) => ({ k: `c${i}`, t: t || `Col ${i + 1}` }));
    const datos = filas.slice(1).map(r => Object.fromEntries(cols.map((c, i) => [c.k, convertir(r[i])])));
    tablas.push({ nombre: ultimoTitulo || `Tabla ${tablas.length + 1}`, columnas: cols, filas: datos });
    ultimoTitulo = null;
  }
  return tablas;
}
// "1.234,5" → 1234.5 · "42%" → 42 · "—" → null
function convertir(s) {
  if (s == null) return null;
  const t = String(s).trim();
  if (!t || t === "—" || t === "-") return null;
  if (/^-?\d{1,3}(\.\d{3})*(,\d+)?$/.test(t)) return Number(t.replace(/\./g, "").replace(",", "."));
  if (/^-?\d+([.,]\d+)?$/.test(t)) return Number(t.replace(",", "."));
  if (/^-?\d+([.,]\d+)?\s?%$/.test(t)) return Number(t.replace("%", "").replace(",", ".").trim());
  return t;
}

// ── ARCHIVOS GUARDADOS ───────────────────────────────────────────────────────
// Lo que arma el bot (o cualquiera) queda en la base, con un link para bajarlo.

function guardarArchivo(db, { nombre, mime, buffer, descripcion, creado_por }) {
  const r = db.prepare(`INSERT INTO archivos (nombre, mime, bytes, tamano, descripcion, creado_por) VALUES (?,?,?,?,?,?)`)
    .run(nombre, mime, buffer, buffer.length, descripcion || null, creado_por || null);
  return { id: Number(r.lastInsertRowid), nombre, url: `/archivos/${r.lastInsertRowid}/${encodeURIComponent(nombre)}`, kb: Math.round(buffer.length / 1024) };
}
function listarArchivos(db) {
  return db.prepare("SELECT id, nombre, mime, tamano, descripcion, creado_por, created_at FROM archivos ORDER BY id DESC").all()
    .map(a => ({ ...a, url: `/archivos/${a.id}/${encodeURIComponent(a.nombre)}`, kb: Math.round((a.tamano || 0) / 1024) }));
}
const leerArchivo = (db, id) => db.prepare("SELECT * FROM archivos WHERE id=?").get(id);
const borrarArchivo = (db, id) => ({ ok: !!db.prepare("DELETE FROM archivos WHERE id=?").run(id).changes });

/**
 * Para el bot: una consulta SELECT → archivo guardado con link.
 * Los encabezados salen de los nombres de columna del SELECT (se pueden poner
 * con AS: `SELECT rp AS "RP", peso AS "Peso kg"`).
 */
function desdeConsulta(db, { sql, titulo, nombre, formato = "xlsx", descripcion, creado_por, campoNombre }) {
  const limpio = String(sql || "").trim().replace(/;+\s*$/, "");
  if (!/^select\b/i.test(limpio)) throw new Error("Sólo SELECT");
  if (/;/.test(limpio)) throw new Error("Una sola consulta");
  const filas = db.prepare(limpio).all();
  const claves = filas.length ? Object.keys(filas[0]) : db.prepare(limpio).columns().map(c => c.name);
  const columnas = claves.map(k => ({ k, t: k.replace(/_/g, " ").replace(/^\w/, c => c.toUpperCase()) }));
  const base = nombre || titulo || "consulta";
  let buffer, mime;
  if (formato === "csv") { buffer = Buffer.from(csv(columnas, filas), "utf8"); mime = FORMATOS.csv.mime; }
  else if (formato === "html") { buffer = Buffer.from(htmlImprimible({ titulo: titulo || base, subtitulo: descripcion, columnas, filas, campoNombre }), "utf8"); mime = FORMATOS.html.mime; formato = "html"; }
  else { buffer = xlsx.armar([{ nombre: (titulo || base).slice(0, 31), titulo: titulo || base, subtitulo: descripcion, columnas, filas }]); mime = FORMATOS.xlsx.mime; formato = "xlsx"; }
  const guardado = guardarArchivo(db, { nombre: nombreArchivo(base, formato), mime, buffer, descripcion: descripcion || titulo, creado_por });
  return { ...guardado, filas: filas.length, columnas: claves };
}

module.exports = {
  init, COLUMNAS, NOMBRES, FORMATOS, conjunto, armar, csv, htmlImprimible, tablasDeHtml,
  guardarArchivo, listarArchivos, leerArchivo, borrarArchivo, desdeConsulta, nombreArchivo
};
