// ─────────────────────────────────────────────────────────────────────────────
// ANIMALES — buscar cualquier animal y ver su ficha, sea vaca, toro o ternero.
//
// El plantel (plantel.js) sabe todo de los vientres. Pero en el campo se
// pregunta por cualquier bicho: "el 148", "la caravana 3201…848", "los hijos
// de Hércules". Acá está la búsqueda que entiende cómo se escribe un RP en
// la manga — con o sin ceros adelante, con o sin espacios, mayúsculas o no —
// y la ficha general con todo lo que se registró del animal.
// ─────────────────────────────────────────────────────────────────────────────

const dias = (a, b) => (a && b) ? Math.round((new Date(b) - new Date(a)) / 86400000) : null;
const r1 = n => n == null || !isFinite(n) ? null : Math.round(n * 10) / 10;
const r3 = n => n == null || !isFinite(n) ? null : Math.round(n * 1000) / 1000;

// "b 332" → "B332" · "  011 " → "011" · "Hércules" → "HERCULES"
function norm(s) {
  return String(s == null ? "" : s).toUpperCase().normalize("NFD").replace(/[̀-ͯ]/g, "")
    .replace(/[\s\-_.\/]/g, "");
}
// "011" → "11" · "B0332" → "B332": los ceros adelante de un número no cuentan.
const compacto = s => norm(s).replace(/(^|\D)0+(?=\d)/g, "$1");

// ── ESQUEMA ──────────────────────────────────────────────────────────────────
// Tablas que el sistema usa pero que una base vieja puede no tener.
function init(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS lotes (
      id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL, potrero TEXT, descripcion TEXT,
      created_at TEXT DEFAULT (datetime('now')));
    CREATE TABLE IF NOT EXISTS lote_animales (
      lote_id INTEGER NOT NULL, animal_id INTEGER NOT NULL, fecha_ingreso TEXT,
      UNIQUE(lote_id, animal_id));
    CREATE INDEX IF NOT EXISTS idx_la_animal ON lote_animales(animal_id);
    CREATE INDEX IF NOT EXISTS idx_pes_fecha ON pesadas(fecha);
  `);
  // Los toros tienen nombre ("Hércules") y los hijos suelen traer al padre por
  // nombre o por RP. La columna se agrega si la base vieja no la tiene.
  const cols = db.prepare("PRAGMA table_info(animales)").all().map(c => c.name);
  if (!cols.includes("nombre")) db.exec("ALTER TABLE animales ADD COLUMN nombre TEXT");
  // Al nacer, el ternero lleva una caravana control (número al azar y color)
  // hasta que se le asigna el RP definitivo y el chip. Mientras tanto su RP
  // es provisorio ("C" + número) y rp_provisorio = 1.
  if (!cols.includes("caravana_control")) db.exec("ALTER TABLE animales ADD COLUMN caravana_control TEXT");
  if (!cols.includes("caravana_color")) db.exec("ALTER TABLE animales ADD COLUMN caravana_color TEXT");
  if (!cols.includes("rp_provisorio")) db.exec("ALTER TABLE animales ADD COLUMN rp_provisorio INTEGER DEFAULT 0");
}

// ── TOROS ────────────────────────────────────────────────────────────────────
// Los reproductores del campo, con lo que dicen de ellos sus hijos: cuántos,
// cuánto pesaron al nacer y al destete, en qué temporadas trabajaron.
// Un hijo es del toro si padre_rp coincide con su RP o con su nombre.

function toros(db, opciones = {}) {
  const hoy = opciones.hoy || new Date().toISOString().slice(0, 10);
  const estado = String(opciones.estado || "ACTIVO").toUpperCase();
  const anio = opciones.anio || hoy.slice(0, 4);
  const filas = db.prepare(SQL_LISTA + ` WHERE upper(COALESCE(a.sexo,'')) LIKE 'M%' AND upper(COALESCE(a.categoria,'')) LIKE 'TORO%'
    ${estado === "TODOS" ? "" : "AND upper(COALESCE(a.estado,'ACTIVO')) = ?"} ORDER BY a.rp`).all(...(estado === "TODOS" ? [] : [estado]));
  const hijosDe = db.prepare(`SELECT h.rp, h.sexo, h.fecha_nac, h.estado, h.madre_rp, h.padre_rp,
      (SELECT peso FROM pesadas p WHERE p.animal_id=h.id AND upper(COALESCE(p.contexto,''))='NACIMIENTO' ORDER BY p.fecha LIMIT 1) pn,
      (SELECT peso FROM pesadas p WHERE p.animal_id=h.id AND upper(COALESCE(p.contexto,''))='DESTETE' ORDER BY p.fecha DESC LIMIT 1) destete
    FROM animales h WHERE COALESCE(h.padre_rp,'') <> ''`).all();
  const servicios = (() => { try { return db.prepare("SELECT toro_natural, semen_iatf, temporada, resultado FROM servicios").all(); } catch (e) { return []; } })();
  const ce = (() => { try { return db.prepare("SELECT animal_id, valor, fecha FROM mediciones WHERE upper(tipo)='CE' ORDER BY fecha DESC").all(); } catch (e) { return []; } })();
  const destinos = (() => { try { return db.prepare("SELECT animal_rp, destino, concretado FROM destinos WHERE temporada=?").all(anio); } catch (e) { return []; } })();
  const prom = a => a.length ? Math.round(a.reduce((x, y) => x + y, 0) / a.length * 10) / 10 : null;
  // Un toro con destino de salida (venta, terminación) ya no es un toro del plantel.
  let salen = new Set();
  try { salen = require("./destinos.js").destinadosASalir(db); } catch (e) {}
  const destinados = filas.filter(t => salen.has(String(t.rp).toUpperCase())).length;
  const quedan = opciones.incluirDestinados ? filas : filas.filter(t => !salen.has(String(t.rp).toUpperCase()));

  const out = quedan.map(t => {
    const claves = new Set([compacto(t.rp), compacto(t.nombre)].filter(Boolean));
    // Un toro puede haber servido en otro campo de la empresa: esos hijos son suyos.
    const fuera = (opciones.hijosFuera ? opciones.hijosFuera(t.rp) : [])
      .concat(t.nombre && opciones.hijosFuera ? opciones.hijosFuera(t.nombre) : [])
      .filter((h, i, l) => l.findIndex(x => x.rp === h.rp && x.campo === h.campo) === i);
    const hijos = hijosDe.filter(h => claves.has(compacto(h.padre_rp)))
      .concat(fuera.filter(h => !hijosDe.some(x => String(x.rp).toUpperCase() === String(h.rp).toUpperCase())));
    const delAnio = hijos.filter(h => String(h.fecha_nac || "").startsWith(anio));
    const serv = servicios.filter(s => claves.has(compacto(s.toro_natural)) || claves.has(compacto(s.semen_iatf)));
    const temporadas = [...new Set(serv.map(s => s.temporada).filter(Boolean))].sort();
    const medCE = ce.find(m => m.animal_id === t.id);
    const d = destinos.find(x => compacto(x.animal_rp) === compacto(t.rp));
    return {
      rp: t.rp, nombre: t.nombre, hba: t.hbu || t.registro || null, chip: t.chip, pelo: t.pelo, categoria: t.categoria, estado: t.estado || "ACTIVO",
      fecha_nac: t.fecha_nac, edad_meses: t.edad_meses != null ? t.edad_meses : (t.fecha_nac ? Math.round(dias(t.fecha_nac, hoy) / 30.44) : null),
      padre: t.padre_rp, madre: t.madre_rp,
      peso_actual: t.peso_actual, ultima_pesada: t.ultima_pesada, dias_sin_pesar: t.ultima_pesada ? dias(t.ultima_pesada, hoy) : null,
      ce: medCE ? medCE.valor : null, fecha_ce: medCE ? medCE.fecha : null,
      hijos: hijos.length, hijos_anio: delAnio.length, hijos_otros_campos: fuera.length || undefined,
      machos: hijos.filter(h => String(h.sexo || "").toUpperCase().startsWith("M")).length,
      hembras: hijos.filter(h => String(h.sexo || "").toUpperCase().startsWith("H")).length,
      pn_prom_hijos: prom(hijos.map(h => h.pn).filter(x => x > 0)),
      destete_prom_hijos: prom(hijos.map(h => h.destete).filter(x => x > 0)),
      hijos_muertos: hijos.filter(h => String(h.estado || "").toUpperCase() === "MUERTO").length,
      servicios: serv.length, temporadas: temporadas.join(", ") || null, ultima_temporada: temporadas[temporadas.length - 1] || null,
      prenez: serv.length ? Math.round(serv.filter(s => /PRE/i.test(String(s.resultado || ""))).length / serv.length * 100) : null,
      lote: t.lote_actual, potrero: t.potrero,
      destino: d ? d.destino : null, salio: d ? !!d.concretado : false,
      notas: t.notas
    };
  });
  const conHijos = out.filter(t => t.hijos);
  return {
    filas: out, anio,
    resumen: {
      total: out.length,
      destinados,
      con_hijos_anio: out.filter(t => t.hijos_anio).length,
      hijos_totales: out.reduce((a, t) => a + t.hijos, 0),
      hijos_anio: out.reduce((a, t) => a + t.hijos_anio, 0),
      destete_prom_hijos: prom(conHijos.map(t => t.destete_prom_hijos).filter(Boolean)),
      pn_prom_hijos: prom(conHijos.map(t => t.pn_prom_hijos).filter(Boolean)),
      sin_pesar: out.filter(t => t.dias_sin_pesar == null || t.dias_sin_pesar > 180).length,
      en_corral: out.filter(t => /TERMIN|CORRAL/i.test((t.lote || "") + " " + (t.potrero || ""))).length
    }
  };
}

// ── TODOS LOS ANIMALES, CON LO QUE MÁS SE MIRA ───────────────────────────────

const SQL_LISTA = `
  SELECT a.*,
    (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND upper(COALESCE(p.contexto,''))='NACIMIENTO'
     ORDER BY p.fecha LIMIT 1) peso_nac,
    (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND upper(COALESCE(p.contexto,''))='DESTETE'
     ORDER BY p.fecha DESC LIMIT 1) destete,
    (SELECT fecha FROM pesadas p WHERE p.animal_id=a.id AND upper(COALESCE(p.contexto,''))='DESTETE'
     ORDER BY p.fecha DESC LIMIT 1) fecha_destete,
    (SELECT peso FROM pesadas p WHERE p.animal_id=a.id ORDER BY p.fecha DESC, p.id DESC LIMIT 1) peso_actual,
    (SELECT fecha FROM pesadas p WHERE p.animal_id=a.id ORDER BY p.fecha DESC, p.id DESC LIMIT 1) ultima_pesada,
    (SELECT COUNT(*) FROM pesadas p WHERE p.animal_id=a.id) n_pesadas,
    (SELECT COUNT(*) FROM animales h WHERE upper(COALESCE(h.madre_rp,''))=upper(a.rp)) crias,
    (SELECT l.nombre FROM lote_animales la JOIN lotes l ON l.id=la.lote_id
     WHERE la.animal_id=a.id ORDER BY la.fecha_ingreso DESC LIMIT 1) lote_actual,
    (SELECT l.potrero FROM lote_animales la JOIN lotes l ON l.id=la.lote_id
     WHERE la.animal_id=a.id ORDER BY la.fecha_ingreso DESC LIMIT 1) potrero
  FROM animales a`;

/**
 * Lista de animales. `estado` "ACTIVO" (default), "TODOS", o cualquier otro.
 */
function listar(db, opciones = {}) {
  const hoy = opciones.hoy || new Date().toISOString().slice(0, 10);
  const estado = String(opciones.estado || "ACTIVO").toUpperCase();
  const where = estado === "TODOS" ? "" : ` WHERE upper(COALESCE(a.estado,'ACTIVO')) = ?`;
  const filas = db.prepare(SQL_LISTA + where + " ORDER BY a.rp").all(...(estado === "TODOS" ? [] : [estado]));
  for (const f of filas) {
    f.edad_meses = f.fecha_nac ? Math.round(dias(f.fecha_nac, hoy) / 30.44) : null;
    if (f.edad_meses != null && (f.edad_meses < 0 || f.edad_meses > 300)) f.edad_meses = null;
    // Ganancia diaria desde el destete, si hay pesada posterior.
    f.gdp_destete = (f.destete && f.peso_actual && f.fecha_destete && f.ultima_pesada > f.fecha_destete)
      ? r3((f.peso_actual - f.destete) / dias(f.fecha_destete, f.ultima_pesada)) : null;
    f.dias_sin_pesar = f.ultima_pesada ? dias(f.ultima_pesada, hoy) : null;
  }
  return filas;
}

// ── BÚSQUEDA ─────────────────────────────────────────────────────────────────
// Devuelve pocos resultados, ordenados por qué tan bien coinciden: primero el
// RP exacto, después lo que empieza igual, después lo que contiene el texto.

function buscar(db, q, opciones = {}) {
  const texto = String(q || "").trim();
  if (!texto) return [];
  const limite = opciones.limite || 30;
  const nq = norm(texto), cq = compacto(texto);
  const palabras = texto.toUpperCase().normalize("NFD").replace(/[̀-ͯ]/g, "").split(/\s+/).filter(Boolean);

  const todos = db.prepare(`
    SELECT a.id, a.rp, a.nombre, a.chip, a.hbu, a.registro, a.sexo, a.categoria, a.estado, a.fecha_nac, a.pelo,
           a.madre_rp, a.padre_rp, a.lote, a.notas, a.caravana_control, a.caravana_color, a.rp_provisorio,
      (SELECT peso FROM pesadas p WHERE p.animal_id=a.id ORDER BY p.fecha DESC, p.id DESC LIMIT 1) peso_actual,
      (SELECT COUNT(*) FROM animales h WHERE upper(COALESCE(h.madre_rp,''))=upper(a.rp)) crias,
      (SELECT GROUP_CONCAT(texto, ' | ') FROM notas_campo n WHERE upper(n.animal_rp)=upper(a.rp)) notas_campo
    FROM animales a`).all();

  const puntuados = [];
  for (const a of todos) {
    let puntos = 0, por = null;
    const campos = [["RP", a.rp], ["nombre", a.nombre], ["caravana", a.chip], ["control", a.caravana_control], ["HBA", a.hbu], ["registro", a.registro]];
    for (const [nombre, v] of campos) {
      if (v == null || v === "") continue;
      const nv = norm(v), cv = compacto(v);
      let p = 0;
      if (nv === nq || cv === cq) p = 100;
      else if (nv.startsWith(nq) || cv.startsWith(cq)) p = 60;
      else if (nv.includes(nq) || cv.includes(cq)) p = 35;
      if (nombre !== "RP" && p) p -= 5;
      if (p > puntos) { puntos = p; por = nombre; }
    }
    // Madre y padre: sirve para "hijos de Hércules" o "crías de la 23".
    for (const [nombre, v] of [["madre", a.madre_rp], ["padre", a.padre_rp]]) {
      if (!v) continue;
      const nv = norm(v), cv = compacto(v);
      const p = (nv === nq || cv === cq) ? 30 : (nv.includes(nq) && nq.length >= 3) ? 15 : 0;
      if (p > puntos) { puntos = p; por = nombre; }
    }
    // Texto libre en notas y lote: todas las palabras tienen que estar.
    if (!puntos && palabras.length) {
      const libre = norm([a.lote, a.notas, a.notas_campo, a.categoria, a.pelo].filter(Boolean).join(" "));
      if (palabras.every(w => libre.includes(norm(w)))) { puntos = 10; por = "notas"; }
    }
    if (puntos) puntuados.push({ ...a, _p: puntos + (String(a.estado || "ACTIVO").toUpperCase() === "ACTIVO" ? 2 : 0), coincide: por });
  }
  puntuados.sort((x, y) => y._p - x._p || String(x.rp).localeCompare(String(y.rp), "es", { numeric: true }));
  return puntuados.slice(0, limite).map(({ _p, notas_campo, id, ...a }) => a);
}

// Encuentra un animal por RP tolerando ceros y espacios. Prefiere el exacto.
function porRp(db, rp) {
  const t = String(rp || "").trim();
  if (!t) return null;
  const exacto = db.prepare("SELECT * FROM animales WHERE upper(rp)=upper(?)").get(t);
  if (exacto) return exacto;
  const c = compacto(t);
  const candidatos = db.prepare("SELECT * FROM animales").all().filter(a => compacto(a.rp) === c || norm(a.chip) === norm(t) || (a.nombre && norm(a.nombre) === norm(t)));
  if (candidatos.length === 1) return candidatos[0];
  // Si hay varios (uno activo y uno muerto con el mismo número), el activo.
  return candidatos.find(a => String(a.estado || "ACTIVO").toUpperCase() === "ACTIVO") || candidatos[0] || null;
}

// ── FICHA GENERAL ────────────────────────────────────────────────────────────
// Todo lo que se registró del animal, sea lo que sea. La ficha de vientre
// (plantel.js) agrega lo reproductivo; ésta es la base común.

function ficha(db, rp, opciones = {}) {
  const hoy = opciones.hoy || new Date().toISOString().slice(0, 10);
  const a = porRp(db, rp);
  if (!a) return { ok: false, error: `No encuentro ningún animal con RP o caravana "${rp}"` };

  const pesadas = db.prepare("SELECT id, fecha, peso, contexto FROM pesadas WHERE animal_id=? ORDER BY fecha, id").all(a.id);
  for (let i = 1; i < pesadas.length; i++) {
    const d = dias(pesadas[i - 1].fecha, pesadas[i].fecha);
    pesadas[i].dias = d;
    pesadas[i].gdp = d > 0 ? r3((pesadas[i].peso - pesadas[i - 1].peso) / d) : null;
  }
  const q = (sql, ...p) => { try { return db.prepare(sql).all(...p); } catch (e) { return []; } };
  const servicios = q("SELECT * FROM servicios WHERE animal_id=? ORDER BY temporada DESC, id DESC", a.id);
  const sanidad = q("SELECT id, fecha, producto, dosis, motivo FROM sanidad WHERE animal_id=? ORDER BY fecha DESC", a.id);
  const mediciones = q("SELECT id, fecha, tipo, valor FROM mediciones WHERE animal_id=? ORDER BY fecha DESC", a.id);
  const lotes = q(`SELECT l.id, l.nombre, l.potrero, la.fecha_ingreso FROM lote_animales la
                   JOIN lotes l ON l.id=la.lote_id WHERE la.animal_id=? ORDER BY la.fecha_ingreso DESC`, a.id);
  const notas = q("SELECT id, fecha, texto, causa, grave, usuario FROM notas_campo WHERE upper(animal_rp)=upper(?) ORDER BY fecha DESC", a.rp);
  const destinos = q("SELECT * FROM destinos WHERE upper(animal_rp)=upper(?) ORDER BY temporada DESC", a.rp);
  const hijos = q(`SELECT h.rp, h.fecha_nac, h.sexo, h.pelo, h.estado, h.padre_rp, h.madre_rp,
      (SELECT peso FROM pesadas p WHERE p.animal_id=h.id AND upper(COALESCE(p.contexto,''))='NACIMIENTO' ORDER BY p.fecha LIMIT 1) peso_nac,
      (SELECT peso FROM pesadas p WHERE p.animal_id=h.id AND upper(COALESCE(p.contexto,''))='DESTETE' ORDER BY p.fecha DESC LIMIT 1) destete
    FROM animales h WHERE upper(COALESCE(h.madre_rp,''))=upper(?) OR upper(COALESCE(h.padre_rp,''))=upper(?)
       OR (? <> '' AND upper(COALESCE(h.padre_rp,''))=upper(?))
    ORDER BY h.fecha_nac`, a.rp, a.rp, a.nombre || "", a.nombre || "");
  const madre = a.madre_rp ? porRp(db, a.madre_rp) : null;
  const padre = a.padre_rp ? porRp(db, a.padre_rp) : null;

  const ult = pesadas[pesadas.length - 1] || null;
  const primera = pesadas[0] || null;
  const nac = pesadas.find(p => /NACIMIENTO/i.test(p.contexto || "")) || null;
  const dest = [...pesadas].reverse().find(p => /DESTETE/i.test(p.contexto || "")) || null;
  const edadM = a.fecha_nac ? Math.round(dias(a.fecha_nac, hoy) / 30.44) : null;

  return {
    ok: true,
    rp: a.rp, nombre: a.nombre || null, chip: a.chip, hba: a.hbu || a.registro || null, sexo: a.sexo, categoria: a.categoria,
    caravana_control: a.caravana_control || null, caravana_color: a.caravana_color || null, rp_provisorio: !!a.rp_provisorio,
    estado: a.estado || "ACTIVO", fecha_nac: a.fecha_nac, pelo: a.pelo, raza: a.raza,
    edad_meses: (edadM != null && edadM >= 0 && edadM < 300) ? edadM : null,
    madre: a.madre_rp, madre_existe: !!madre, madre_campo: a.madre_campo || null,
    padre: a.padre_rp, padre_existe: !!padre, padre_campo: a.padre_campo || null,
    lote: lotes[0] ? lotes[0].nombre : a.lote || null, potrero: lotes[0] ? lotes[0].potrero : null,
    peso_nac: nac ? nac.peso : null, destete: dest ? dest.peso : null, fecha_destete: dest ? dest.fecha : null,
    peso_actual: ult ? ult.peso : null, ultima_pesada: ult ? ult.fecha : null,
    dias_sin_pesar: ult ? dias(ult.fecha, hoy) : null,
    gdp_total: (primera && ult && primera.fecha < ult.fecha) ? r3((ult.peso - primera.peso) / dias(primera.fecha, ult.fecha)) : null,
    gdp_ultima: ult && ult.gdp != null ? ult.gdp : null,
    pesadas, servicios, sanidad, mediciones, lotes, notas, destinos, hijos,
    es_vientre: String(a.sexo || "").toUpperCase().startsWith("H") && (hijos.some(h => norm(h.padre_rp) !== norm(a.rp)) || servicios.length > 0 || /VACA|VIENTRE/i.test(a.categoria || "")),
    notas_animal: a.notas || null
  };
}

// ── TERMINACIÓN ──────────────────────────────────────────────────────────────
// Lo que está terminando, con cuánto viene ganando cada uno. Entran dos cosas:
//   · los que están en un lote de corral (nombre TERMINACION/CORRAL o potrero CORRAL)
//   · los marcados con un destino de terminación que todavía no salieron
// La columna `origen` dice cuál es cada caso: "corral" o "marcado".

function terminacion(db, opciones = {}) {
  const hoy = opciones.hoy || new Date().toISOString().slice(0, 10);
  const enCorral = db.prepare(`
    SELECT a.id, a.rp, a.sexo, a.categoria, a.fecha_nac, a.pelo, a.padre_rp,
           l.nombre lote, l.potrero, la.fecha_ingreso, 'corral' origen
    FROM lote_animales la
    JOIN lotes l ON l.id = la.lote_id
    JOIN animales a ON a.id = la.animal_id
    WHERE (upper(l.nombre) LIKE '%TERMINACION%' OR upper(l.nombre) LIKE '%CORRAL%'
       OR upper(COALESCE(l.potrero,'')) LIKE '%CORRAL%')
      AND upper(COALESCE(a.estado,'ACTIVO'))='ACTIVO'
    ORDER BY a.rp`).all();
  let marcados = [];
  try {
    marcados = db.prepare(`
      SELECT a.id, a.rp, a.sexo, a.categoria, a.fecha_nac, a.pelo, a.padre_rp,
             NULL lote, NULL potrero, d.fecha fecha_ingreso, 'marcado' origen, d.destino, d.motivo
      FROM destinos d JOIN animales a ON upper(a.rp)=upper(d.animal_rp)
      WHERE upper(d.destino) LIKE '%TERMINACION%' AND COALESCE(d.concretado,0)=0
        AND d.temporada=? AND upper(COALESCE(a.estado,'ACTIVO'))='ACTIVO'
      ORDER BY a.rp`).all(hoy.slice(0, 4));
  } catch (e) {}
  const ya = new Set(enCorral.map(f => f.id));
  const filas = [...enCorral.map(f => ({ ...f, destino: null })), ...marcados.filter(f => !ya.has(f.id))];
  // Si uno está en corral y además marcado, se muestra el destino igual.
  for (const f of filas) if (f.origen === "corral") { const m = marcados.find(x => x.id === f.id); if (m) { f.destino = m.destino; f.origen = "corral"; } }

  const out = filas.map(f => {
    const pes = db.prepare(`SELECT fecha,peso,contexto FROM pesadas WHERE animal_id=? ORDER BY fecha, id`).all(f.id);
    const ult = pes[pes.length - 1] || null;
    const entrada = f.fecha_ingreso ? pes.filter(p => p.fecha <= f.fecha_ingreso).pop() || pes[0] : pes[0];
    const d = (entrada && ult && entrada.fecha !== ult.fecha) ? dias(entrada.fecha, ult.fecha) : null;
    const destete = [...pes].reverse().find(p => /DESTETE/i.test(p.contexto || ""));
    return {
      rp: f.rp, sexo: f.sexo, categoria: f.categoria, pelo: f.pelo, padre_rp: f.padre_rp,
      origen: f.origen, destino: f.destino || null,
      lote: f.lote, potrero: f.potrero, fecha_ingreso: f.fecha_ingreso,
      meses: f.fecha_nac ? Math.round(dias(f.fecha_nac, hoy) / 30.44) : null,
      peso_entrada: entrada ? entrada.peso : null,
      peso_actual: ult ? ult.peso : null,
      ultima_pesada: ult ? ult.fecha : null,
      dias_corral: f.fecha_ingreso ? dias(f.fecha_ingreso, hoy) : null,
      ganancia: (entrada && ult) ? r1(ult.peso - entrada.peso) : null,
      gdp: (d && d > 0) ? r3((ult.peso - entrada.peso) / d) : null,
      destete: destete ? destete.peso : null,
      dias_sin_pesar: ult ? dias(ult.fecha, hoy) : null
    };
  });

  const num = a => a.filter(x => x != null && isFinite(x));
  const prom = a => a.length ? r1(a.reduce((x, y) => x + y, 0) / a.length) : null;
  const gdps = num(out.map(f => f.gdp));
  return {
    filas: out,
    resumen: {
      total: out.length,
      en_corral: out.filter(f => f.origen === "corral").length,
      marcados: out.filter(f => f.origen === "marcado").length,
      lotes: [...new Set(out.map(f => f.lote).filter(Boolean))],
      peso_prom: prom(num(out.map(f => f.peso_actual))),
      gdp_prom: gdps.length ? r3(gdps.reduce((a, b) => a + b, 0) / gdps.length) : null,
      ganancia_prom: prom(num(out.map(f => f.ganancia))),
      dias_prom: prom(num(out.map(f => f.dias_corral))),
      kg_totales: Math.round(num(out.map(f => f.peso_actual)).reduce((a, b) => a + b, 0)),
      sin_pesar: out.filter(f => f.dias_sin_pesar > 30).length
    }
  };
}

module.exports = { init, listar, buscar, porRp, ficha, terminacion, toros, norm, compacto, SQL_LISTA };
