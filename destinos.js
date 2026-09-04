// ─────────────────────────────────────────────────────────────────────────────
// DESTINOS
//
// Todo animal, en algún momento, sale del plantel hacia algún lado. Y no todas
// las salidas son fracasos: el mejor toro de la camada también se va, sólo que
// como reproductor.
//
// Por eso esto no es una lista de descartes: es a dónde va cada uno.
//
//   VENTA PREÑADA          se va como vientre, el valor está en la preñez
//   TERMINACION            al corral, se vende gorda
//   VENTA DIRECTA          se vende como está (vientre o toro)
//   TORO REPRODUCTOR       lo mejor de la camada
//   TORO TERMINACION       no calificó para reproductor
//   NOVILLO TERMINACION    a carne
//
// El motivo y el destino son cosas distintas: una vaca puede descartarse por
// vieja y venderse preñada igual, si está bien.
// ─────────────────────────────────────────────────────────────────────────────

const DESTINOS = {
  "VENTA PREÑADA":       { grupo: "vientre", positivo: null,  texto: "se vende preñada" },
  "TERMINACION":         { grupo: "vientre", positivo: false, texto: "al corral, se vende gorda" },
  "VENTA DIRECTA":       { grupo: "ambos",   positivo: null,  texto: "se vende como está" },
  "TORO REPRODUCTOR":    { grupo: "macho",   positivo: true,  texto: "queda de padre" },
  "TORO TERMINACION":    { grupo: "macho",   positivo: false, texto: "no calificó, al corral" },
  "NOVILLO TERMINACION": { grupo: "macho",   positivo: false, texto: "a carne" },
  "QUEDA":               { grupo: "ambos",   positivo: true,  texto: "sigue en el plantel" }
};

// Por qué se decidió. Sirve para mirar atrás y entender el rodeo.
const MOTIVOS = {
  NO_DESTETO:     "no destetó",
  VACIA:          "quedó vacía",
  EDAD:           "por edad",
  PRODUCTIVIDAD:  "desteta poco",
  CARACTER:       "mal carácter",
  APLOMOS:        "aplomos",
  UBRE:           "ubre",
  SANIDAD:        "sanidad",
  SELECCION:      "por selección",
  COMERCIAL:      "decisión comercial"
};

function init(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS destinos (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      animal_rp TEXT NOT NULL,
      destino TEXT NOT NULL,
      motivo TEXT,
      nota TEXT,
      temporada TEXT,
      fecha TEXT DEFAULT (date('now')),
      usuario TEXT,
      -- Cuando efectivamente se va: hasta entonces sigue en el campo.
      concretado INTEGER DEFAULT 0,
      fecha_salida TEXT,
      precio REAL,
      created_at TEXT DEFAULT (datetime('now')),
      UNIQUE(animal_rp, temporada));
    CREATE INDEX IF NOT EXISTS idx_dest_rp ON destinos(animal_rp);
  `);
}

const temporadaDe = f => String(f || new Date().toISOString().slice(0, 10)).slice(0, 4);

// Un destino de salida es todo lo que no sea quedarse: el animal deja de
// contar en Plantel y Toros desde que se decide, aunque todavía esté en el
// campo. QUEDA y TORO REPRODUCTOR no sacan a nadie.
const SALE = d => !!DESTINOS[d] && !["QUEDA", "TORO REPRODUCTOR"].includes(d);

/** RP (en mayúsculas) de los que tienen un destino de salida sin concretar, de cualquier temporada. */
function destinadosASalir(db) {
  try {
    return new Set(db.prepare("SELECT animal_rp, destino FROM destinos WHERE COALESCE(concretado,0)=0").all()
      .filter(d => SALE(String(d.destino || "").toUpperCase())).map(d => String(d.animal_rp).toUpperCase()));
  } catch (e) { return new Set(); }
}

/**
 * Marca a dónde va un animal. Si ya tenía destino esta temporada, lo reemplaza:
 * la decisión puede cambiar — se marca para terminación y al final se vende
 * preñada porque quedó servida.
 */
// Cómo lo dice la gente → cómo lo guarda el sistema. "Engorde", "gordas", "al
// corral" y "feedlot" son terminación; para un macho, novillo o toro a terminación.
function normalizarDestino(texto, esMacho) {
  const t = String(texto || "").toUpperCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[_\-]+/g, " ").replace(/\s+/g, " ").trim();
  const conTilde = String(texto || "").toUpperCase().trim();
  if (DESTINOS[conTilde]) return conTilde;
  if (/PRENAD|SERVID/.test(t)) return "VENTA PREÑADA";
  if (/REPRODUCTOR|PADRE|SEMENTAL/.test(t)) return "TORO REPRODUCTOR";
  if (/QUEDA|PLANTEL|SIGUE/.test(t)) return "QUEDA";
  if (/DIRECTA|COMO ESTA|FERIA|REMATE/.test(t)) return "VENTA DIRECTA";
  if (/NOVILLO/.test(t)) return "NOVILLO TERMINACION";
  if (/TORO/.test(t) && /TERMIN|ENGORD|CORRAL|DESCART/.test(t)) return "TORO TERMINACION";
  if (/TERMIN|ENGORD|GORD|CORRAL|FEEDLOT|INVERN|CARNE|FAEN|FRIGOR/.test(t)) return esMacho ? "NOVILLO TERMINACION" : "TERMINACION";
  if (/VENT|VEND/.test(t)) return "VENTA DIRECTA";
  return null;
}

function marcar(db, rp, destino, opciones = {}) {
  const a = db.prepare("SELECT rp, sexo, categoria FROM animales WHERE upper(rp)=upper(?)").get(String(rp).trim())
    || (() => { try { return require("./animales.js").porRp(db, rp); } catch (e) { return null; } })();
  if (!a) return { ok: false, error: `No encuentro el animal ${rp}` };
  const esMachoPara = String(a.sexo || "").toUpperCase().startsWith("M");
  // Un toro que "va a terminación" es TORO TERMINACION, no NOVILLO.
  let d = normalizarDestino(destino, esMachoPara);
  if (d === "NOVILLO TERMINACION" && /TORO/i.test(a.categoria || "")) d = "TORO TERMINACION";
  if (!d) return { ok: false, error: `No conozco el destino "${destino}". Los que hay: ${Object.keys(DESTINOS).join(", ")}` };

  // Un destino de macho sobre una hembra casi siempre es un error de tipeo.
  const esMacho = String(a.sexo || "").toUpperCase().startsWith("M");
  const g = DESTINOS[d].grupo;
  if (g === "macho" && !esMacho) return { ok: false, error: `${a.rp} es hembra: "${d}" es un destino de machos` };
  if (g === "vientre" && esMacho)  return { ok: false, error: `${a.rp} es macho: "${d}" es un destino de vientres` };

  const temporada = opciones.temporada || temporadaDe();
  const previo = db.prepare("SELECT destino FROM destinos WHERE upper(animal_rp)=upper(?) AND temporada=?")
    .get(a.rp, temporada);

  db.prepare(`INSERT INTO destinos (animal_rp,destino,motivo,nota,temporada,usuario,fecha)
    VALUES (?,?,?,?,?,?,?)
    ON CONFLICT(animal_rp,temporada) DO UPDATE SET
      destino=excluded.destino, motivo=excluded.motivo, nota=excluded.nota,
      usuario=excluded.usuario, fecha=excluded.fecha`)
    .run(a.rp, d, opciones.motivo || null, opciones.nota || null, temporada,
         opciones.usuario || null, opciones.fecha || new Date().toISOString().slice(0, 10));

  return { ok: true, rp: a.rp, destino: d, previo: previo ? previo.destino : null,
    mensaje: previo && previo.destino !== d
      ? `${a.rp}: de ${previo.destino} a ${d}`
      : `${a.rp} → ${d}${opciones.motivo ? ` (${MOTIVOS[opciones.motivo] || opciones.motivo})` : ""}` };
}

function sacar(db, rp, temporada) {
  const t = temporada || temporadaDe();
  const r = db.prepare("DELETE FROM destinos WHERE upper(animal_rp)=upper(?) AND temporada=?")
    .run(String(rp).trim(), t);
  return r.changes
    ? { ok: true, mensaje: `${rp} vuelve al plantel: le saqué el destino.` }
    : { ok: false, error: `${rp} no tenía destino marcado esta temporada` };
}

// Marcar varios de una: "todas las vacías a terminación".
function marcarVarios(db, rps, destino, opciones = {}) {
  const hechos = [], fallados = [];
  for (const rp of rps) {
    const r = marcar(db, rp, destino, opciones);
    if (r.ok) hechos.push(r.rp); else fallados.push(`${rp}: ${r.error}`);
  }
  return { ok: !!hechos.length, hechos, fallados,
    mensaje: `${hechos.length} animal${hechos.length === 1 ? "" : "es"} a ${String(destino).toUpperCase()}` +
      (hechos.length ? `: ${hechos.slice(0, 12).join(", ")}${hechos.length > 12 ? ` y ${hechos.length - 12} más` : ""}` : "") +
      (fallados.length ? `\n\nNo pude con ${fallados.length}:\n${fallados.slice(0, 5).join("\n")}` : "") };
}

function concretar(db, rp, opciones = {}) {
  const t = opciones.temporada || temporadaDe();
  const r = db.prepare(`UPDATE destinos SET concretado=1, fecha_salida=?, precio=?
    WHERE upper(animal_rp)=upper(?) AND temporada=?`)
    .run(opciones.fecha || new Date().toISOString().slice(0, 10), opciones.precio || null,
         String(rp).trim(), t);
  if (!r.changes) return { ok: false, error: `${rp} no tenía destino marcado` };
  // Al salir de verdad, deja de estar activo en el campo.
  db.prepare("UPDATE animales SET estado='VENDIDO' WHERE upper(rp)=upper(?)").run(String(rp).trim());
  return { ok: true, mensaje: `${rp} salió del campo.` };
}

/**
 * Todo lo marcado, más los candidatos que el sistema detecta solo.
 * @param plantel  las filas de plantel.js, para cruzar con los datos productivos
 */
function listar(db, plantel, opciones = {}) {
  const temporada = opciones.temporada || temporadaDe();
  let marcados = [];
  try {
    marcados = db.prepare("SELECT * FROM destinos WHERE temporada=? ORDER BY destino, animal_rp").all(temporada);
  } catch (e) {}

  const porRp = {};
  (plantel || []).forEach(f => { porRp[String(f.rp).toUpperCase()] = f; });

  const filas = marcados.map(m => {
    const v = porRp[String(m.animal_rp).toUpperCase()] || {};
    const info = DESTINOS[m.destino] || {};
    return {
      rp: m.animal_rp, destino: m.destino, grupo: info.grupo,
      positivo: info.positivo, destino_texto: info.texto,
      motivo: m.motivo, motivo_texto: m.motivo ? (MOTIVOS[m.motivo] || m.motivo) : null,
      nota: m.nota, fecha: m.fecha, concretado: !!m.concretado,
      fecha_salida: m.fecha_salida, precio: m.precio,
      // Lo productivo, para poder revisar la decisión
      edad_meses: v.edad_meses, peso_adulto: v.peso_adulto, partos: v.partos,
      destete_prom: v.destete_prom, eficiencia: v.eficiencia, ipp: v.ipp,
      estado: v.estado, bloque: v.bloque, categoria: v.categoria, pelo: v.pelo
    };
  });

  // Los que el sistema propone y todavía no tienen destino.
  const yaEstan = new Set(marcados.map(m => String(m.animal_rp).toUpperCase()));
  const candidatos = (plantel || [])
    .filter(f => f.estado === "FALLÓ" && !yaEstan.has(String(f.rp).toUpperCase()))
    .map(f => ({
      rp: f.rp, motivo: f.causa, motivo_texto: f.causa_texto,
      sugerido: f.causa === "VACIA" ? "TERMINACION" : "TERMINACION",
      edad_meses: f.edad_meses, peso_adulto: f.peso_adulto, partos: f.partos,
      destete_prom: f.destete_prom, eficiencia: f.eficiencia, categoria: f.categoria
    }));

  const porDestino = {};
  filas.forEach(f => { porDestino[f.destino] = (porDestino[f.destino] || 0) + 1; });

  return {
    filas, candidatos,
    resumen: {
      temporada,
      marcados: filas.length,
      concretados: filas.filter(f => f.concretado).length,
      pendientes: filas.filter(f => !f.concretado).length,
      candidatos: candidatos.length,
      por_destino: porDestino,
      vientres: filas.filter(f => f.grupo === "vientre").length,
      machos: filas.filter(f => f.grupo === "macho").length
    },
    destinos: Object.entries(DESTINOS).map(([k, v]) => ({ destino: k, ...v })),
    motivos: Object.entries(MOTIVOS).map(([k, v]) => ({ motivo: k, texto: v }))
  };
}

// Lo que se le dice al bot para que pueda marcar hablando.
const INSTRUCCIONES = `DESTINOS: todo animal que sale del plantel va a algún lado, y no todas las salidas son fracasos — el mejor toro de la camada también se va, como reproductor.

Los destinos posibles son:
· Para vientres: VENTA PREÑADA (se vende servida), TERMINACION (al corral, se vende gorda).
· Para machos: TORO REPRODUCTOR (queda de padre), TORO TERMINACION (no calificó), NOVILLO TERMINACION (a carne).
· Para cualquiera: VENTA DIRECTA (se vende como está; un toro que se vende a otra cabaña, una vaca que sale a feria).
· QUEDA: sigue en el plantel.

El motivo es aparte del destino: una vaca puede descartarse por edad y venderse preñada igual. Motivos: NO_DESTETO, VACIA, EDAD, PRODUCTIVIDAD, CARACTER, APLOMOS, UBRE, SANIDAD, SELECCION, COMERCIAL.

Si te piden marcar animales — "las vacías van a terminación", "los 5 a engorde", "el S402 queda de reproductor", "la 2077 se vende preñada" — usá la herramienta destinar (NO escribir): entiende sinónimos como engorde, gordas, corral. Si la condición es una lista ("las vacías"), consultá primero quiénes cumplen y mandá todos los RP juntos. Contá cuántos marcaste y cuáles, y si alguno no se pudo, por qué.

Una vaca vacía no necesariamente va a terminación: si está gorda puede venderse directa. Preguntá si no está claro.

Desde que un animal tiene un destino de salida (venta o terminación) deja de contar en el plantel y en los toros, aunque siga en el campo: aparece en Destinos y en Terminación. Cuando efectivamente se va, registrá la salida (destinar con accion salida) y pasa a VENDIDO. Si lo marcaron por error, sacar el destino lo devuelve al plantel.`;

module.exports = { init, marcar, marcarVarios, sacar, concretar, listar, normalizarDestino, destinadosASalir, SALE, DESTINOS, MOTIVOS, INSTRUCCIONES };
