// ─────────────────────────────────────────────────────────────────────────────
// TABLERO
//
// Calcula lo que hace falta para decidir, no para consultar: en qué bloque de
// parición cae cada vaca, cuántos días pasan entre sus partos, qué proporción
// de servicios terminó en ternero destetado, y qué está faltando cargar.
//
// Todo sale de datos que ya existen — animales, servicios, pesadas. Acá sólo
// se combinan. No escribe nada.
// ─────────────────────────────────────────────────────────────────────────────

// Los bloques dividen la parición en tramos: cuanto más vacas en cabeza, más
// concentrada y más kilos al destete. Son configurables por campo.
const BLOQUES_DEFAULT = [
  { nombre: "CABEZA", hasta: "08-30", detalle: "hasta el 30/08" },
  { nombre: "CUERPO", hasta: "09-30", detalle: "septiembre" },
  { nombre: "COLA",   hasta: "10-31", detalle: "octubre" },
  { nombre: "TARDIA", hasta: "12-31", detalle: "noviembre o más" }
];

const UMBRALES = {
  ipp_alto: 420,        // días entre partos que ya preocupan
  eficiencia_baja: 70,  // % de servicios que terminaron en destete
  gdp_bajo: 0.40,       // kg/día que delatan un problema
  dias_sin_pesar: 90,
  edad_servicio: 14,    // meses
  peso_servicio: 280,   // kg
  parto_vencido: 25,    // días pasada la fecha probable
  gestacion: 283
};

const dias = (a, b) => Math.round((new Date(b) - new Date(a)) / 86400000);
const sumarDias = (f, n) => { const d = new Date(f); d.setDate(d.getDate() + n); return d.toISOString().slice(0, 10); };
const prom = a => a.length ? Math.round((a.reduce((x, y) => x + y, 0) / a.length) * 100) / 100 : null;

function bloqueDe(fechaParto, bloques = BLOQUES_DEFAULT) {
  if (!fechaParto) return "VACIA";
  const md = String(fechaParto).slice(5, 10);
  for (const b of bloques) if (md <= b.hasta) return b.nombre;
  return "TARDIA";
}

// ── VACAS ────────────────────────────────────────────────────────────────────

function vacas(db, opciones = {}) {
  const temporada = opciones.temporada || String(new Date().getFullYear());
  const hoy = opciones.hoy || new Date().toISOString().slice(0, 10);
  const bloques = opciones.bloques || BLOQUES_DEFAULT;

  const madres = db.prepare(`
    SELECT id, rp, categoria, fecha_nac
    FROM animales
    WHERE upper(COALESCE(sexo,'')) LIKE 'H%' AND upper(COALESCE(estado,'ACTIVO')) = 'ACTIVO'
      AND (upper(COALESCE(categoria,'')) LIKE '%VACA%'
        OR upper(COALESCE(categoria,'')) LIKE '%VIENTRE%'
        OR EXISTS (SELECT 1 FROM animales h WHERE upper(COALESCE(h.madre_rp,'')) = upper(animales.rp)))
    ORDER BY rp
  `).all();

  const filas = [], pend = { parto_vencido: [], sin_servicio: [], descarte: [] };

  for (const m of madres) {
    // Todos sus servicios, del más nuevo al más viejo.
    let servicios = [];
    try {
      servicios = db.prepare(`
        SELECT * FROM servicios WHERE animal_id = ?
        ORDER BY COALESCE(temporada,'') DESC, id DESC`).all(m.id);
    } catch (e) {}

    const actual = servicios.find(s => String(s.temporada || "") === temporada) || null;

    // Sus crías, para el historial y la eficiencia.
    const crias = db.prepare(`
      SELECT a.rp, a.fecha_nac, a.sexo,
             (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='NACIMIENTO' ORDER BY p.fecha LIMIT 1) pn,
             (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='DESTETE' ORDER BY p.fecha DESC LIMIT 1) des
      FROM animales a WHERE upper(COALESCE(a.madre_rp,'')) = upper(?)
      ORDER BY a.fecha_nac DESC`).all(m.rp);

    // Intervalo entre los dos últimos partos: mide si se atrasa cada año.
    const fechas = crias.map(c => c.fecha_nac).filter(Boolean).sort().reverse();
    const ipp = fechas.length >= 2 ? dias(fechas[1], fechas[0]) : null;

    // Eficiencia: de cuántos servicios salió un ternero.
    const conResultado = servicios.filter(s => s.resultado);
    // Cuántos de sus servicios terminaron en cría. Se topea en 100: más de una
    // cría por servicio sólo pasa con mellizos o con datos mal cargados.
    const eficiencia = conResultado.length
      ? Math.min(100, Math.round((crias.length / conResultado.length) * 100)) : null;

    // La parición ocurre el año siguiente al servicio: temporada 2025 → partos 2026.
    const anioParicion = opciones.anio_paricion || String(+temporada + 1);
    const criaActual = crias.find(c => String(c.fecha_nac || "").startsWith(anioParicion)) || null;
    const parto = criaActual ? criaActual.fecha_nac : (actual && actual.fecha_parto) || null;

    // Fecha probable, para saber si el parto está vencido.
    const base = actual && (actual.fecha_iatf || actual.fecha_ingreso_toro);
    const fpp = base ? sumarDias(base, UMBRALES.gestacion) : null;
    const prenada = actual && /PRE/i.test(String(actual.resultado || ""));
    const vencido = prenada && !parto && fpp && dias(fpp, hoy) > UMBRALES.parto_vencido;

    const fila = {
      rp: m.rp, categoria: m.categoria,
      bloque: bloqueDe(parto, bloques),
      servicio: actual ? (actual.fecha_iatf || actual.fecha_ingreso_toro || null) : null,
      padre: actual ? (actual.semen_iatf || actual.toro_natural || null) : null,
      resultado: actual ? actual.resultado : null,
      parto,
      ternero: criaActual ? criaActual.rp : (actual && actual.ternero_rp) || null,
      peso_nac: criaActual ? criaActual.pn : null,
      destete: criaActual ? criaActual.des : null,
      ipp, crias: crias.length, eficiencia,
      fpp, parto_vencido: !!vencido,
      // Cada alerta dice por qué, no sólo que la hay.
      alertas: []
    };

    if (vencido) {
      fila.alertas.push(`parto vencido hace ${dias(fpp, hoy)} días`);
      pend.parto_vencido.push(m.rp);
    }
    if (!actual) {
      fila.alertas.push("sin servicio esta temporada");
      pend.sin_servicio.push(m.rp);
    }
    if (ipp && ipp > UMBRALES.ipp_alto) fila.alertas.push(`${ipp} días entre partos`);
    // Descarte: se atrasa Y produce poco. Una sola cosa no alcanza.
    if ((eficiencia !== null && eficiencia < UMBRALES.eficiencia_baja) &&
        (ipp === null || ipp > UMBRALES.ipp_alto) && crias.length >= 2) {
      fila.alertas.push("candidata a descarte");
      pend.descarte.push(m.rp);
    }
    filas.push(fila);
  }

  const paridas = filas.filter(f => f.parto);
  const porBloque = {};
  BLOQUES_DEFAULT.forEach(b => { porBloque[b.nombre] = paridas.filter(f => f.bloque === b.nombre).length; });

  const conResultado = filas.filter(f => f.resultado);
  const prenadas = conResultado.filter(f => /PRE/i.test(f.resultado)).length;

  return {
    filas, bloques,
    resumen: {
      total: filas.length,
      prenadas, vacias: conResultado.length - prenadas,
      prenez: conResultado.length ? Math.round((prenadas / conResultado.length) * 100) : null,
      paridas: paridas.length,
      por_bloque: porBloque,
      en_cabeza: paridas.length ? Math.round((porBloque.CABEZA / paridas.length) * 100) : null,
      ipp_prom: prom(filas.map(f => f.ipp).filter(Boolean)),
      destete_prom: prom(filas.map(f => f.destete).filter(Boolean)),
      peso_nac_prom: prom(filas.map(f => f.peso_nac).filter(Boolean))
    },
    pendientes: [
      { n: pend.parto_vencido.length, texto: "preñadas con parto vencido, sin registrar", nivel: "alta", rps: pend.parto_vencido },
      { n: pend.sin_servicio.length, texto: "sin servicio esta temporada", nivel: "media", rps: pend.sin_servicio },
      { n: pend.descarte.length, texto: "candidatas a descarte", nivel: "media", rps: pend.descarte }
    ].filter(p => p.n > 0)
  };
}

// ── TOROS ────────────────────────────────────────────────────────────────────
// La progenie suma todos los campos de la empresa: un toro usado en dos lugares
// se evalúa entero. `campos` viene del server, que sabe cuáles son hermanos.

function toros(campos, stockPorNombre = {}) {
  const acum = new Map();

  for (const { nombre: campo, db } of campos) {
    let filas = [];
    try {
      filas = db.prepare(`
        SELECT a.padre_rp padre, a.rp, a.sexo,
               (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='NACIMIENTO' ORDER BY p.fecha LIMIT 1) pn,
               (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='DESTETE' ORDER BY p.fecha DESC LIMIT 1) des,
               (SELECT gdp FROM pesadas p WHERE p.animal_id=a.id AND p.gdp IS NOT NULL ORDER BY p.fecha DESC LIMIT 1) gdp
        FROM animales a WHERE a.padre_rp IS NOT NULL AND trim(a.padre_rp) <> ''`).all();
    } catch (e) { continue; }

    for (const f of filas) {
      const k = String(f.padre).trim().toUpperCase();
      if (!acum.has(k)) acum.set(k, { nombre: String(f.padre).trim(), hijos: 0, campos: {}, pn: [], des: [], gdp: [] });
      const r = acum.get(k);
      r.hijos++;
      r.campos[campo] = (r.campos[campo] || 0) + 1;
      if (f.pn > 0) r.pn.push(f.pn);
      if (f.des > 0) r.des.push(f.des);
      if (f.gdp > 0) r.gdp.push(f.gdp);
    }
  }

  // Los datos del toro (tipo, registro) salen del módulo de reproducción.
  const datos = {};
  for (const { db } of campos) {
    try {
      db.prepare("SELECT nombre, rp, tipo, hbu, hba, producto_stock FROM repro_toros").all()
        .forEach(t => { datos[String(t.nombre).toUpperCase()] = t; });
    } catch (e) {}
  }

  const filas = [...acum.entries()].map(([k, r]) => {
    const d = datos[k] || {};
    const dosis = d.producto_stock != null ? stockPorNombre[d.producto_stock] : undefined;
    const alertas = [];
    if (d.tipo && !d.hbu && !d.hba) alertas.push("sin registro cargado");
    if (dosis != null && dosis < 5) alertas.push(`quedan ${dosis} dosis`);
    return {
      nombre: r.nombre, tipo: d.tipo || null,
      registro: d.hbu || d.hba || null,
      hijos: r.hijos, campos: r.campos, en_campos: Object.keys(r.campos).length,
      peso_nac: prom(r.pn), destete: prom(r.des), gdp: prom(r.gdp),
      dosis: dosis != null ? dosis : null,
      alertas
    };
  }).sort((a, b) => b.hijos - a.hijos);

  const mejor = filas.filter(f => f.destete).sort((a, b) => b.destete - a.destete)[0];
  return {
    filas,
    resumen: {
      toros: filas.length,
      iatf: filas.filter(f => f.tipo === "IATF").length,
      repaso: filas.filter(f => f.tipo === "REPASO").length,
      hijos: filas.reduce((a, f) => a + f.hijos, 0),
      mejor_destete: mejor ? { nombre: mejor.nombre, kg: mejor.destete } : null,
      dosis_total: Object.values(stockPorNombre).reduce((a, b) => a + (b || 0), 0)
    },
    pendientes: [
      { n: filas.filter(f => f.alertas.some(a => a.includes("registro"))).length,
        texto: "toros sin registro HBU/HBA cargado", nivel: "media" },
      { n: filas.filter(f => f.dosis != null && f.dosis < 5).length,
        texto: "pajuelas con menos de 5 dosis", nivel: "media" }
    ].filter(p => p.n > 0)
  };
}

// ── NACIMIENTOS ──────────────────────────────────────────────────────────────

function nacimientos(db, opciones = {}) {
  const anio = opciones.anio || String(new Date().getFullYear());
  const filas = db.prepare(`
    SELECT a.id, a.rp, a.chip, a.fecha_nac, a.sexo, a.pelo, a.madre_rp, a.padre_rp,
           (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='NACIMIENTO' ORDER BY p.fecha LIMIT 1) peso_nac
    FROM animales a
    WHERE a.fecha_nac LIKE ? AND upper(COALESCE(a.estado,'ACTIVO'))='ACTIVO'
    ORDER BY a.fecha_nac`).all(`${anio}%`);

  // El origen se deduce del servicio de la madre: IATF o repaso.
  const origen = {};
  try {
    db.prepare(`SELECT a.rp madre, s.semen_iatf, s.toro_natural, s.resultado
                FROM servicios s JOIN animales a ON a.id = s.animal_id
                WHERE s.temporada = ?`).all(String(+anio - 1)).forEach(s => {
      origen[String(s.madre).toUpperCase()] = /IATF/i.test(String(s.resultado || "")) ? "IATF" : "REPASO";
    });
  } catch (e) {}

  const out = filas.map(f => {
    const falta = [];
    if (f.peso_nac == null) falta.push("peso");
    if (!f.madre_rp) falta.push("madre");
    if (!f.chip) falta.push("caravana");
    if (!f.pelo) falta.push("pelo");
    return { ...f, origen: origen[String(f.madre_rp || "").toUpperCase()] || null, falta };
  });

  const pesos = out.map(f => f.peso_nac).filter(Boolean);
  const fechas = out.map(f => f.fecha_nac).filter(Boolean).sort();
  return {
    filas: out,
    resumen: {
      total: out.length,
      machos: out.filter(f => String(f.sexo || "").toUpperCase().startsWith("M")).length,
      hembras: out.filter(f => !String(f.sexo || "").toUpperCase().startsWith("M")).length,
      peso_prom: prom(pesos),
      peso_min: pesos.length ? Math.min(...pesos) : null,
      peso_max: pesos.length ? Math.max(...pesos) : null,
      de_iatf: out.filter(f => f.origen === "IATF").length,
      de_repaso: out.filter(f => f.origen === "REPASO").length,
      primer_parto: fechas[0] || null, ultimo_parto: fechas[fechas.length - 1] || null
    },
    pendientes: [
      { n: out.filter(f => f.falta.includes("peso")).length, texto: "sin peso al nacer", nivel: "alta" },
      { n: out.filter(f => f.falta.includes("madre")).length, texto: "sin madre asignada", nivel: "alta" },
      { n: out.filter(f => f.falta.includes("caravana")).length, texto: "sin caravana electrónica", nivel: "media" }
    ].filter(p => p.n > 0)
  };
}

// ── RECRÍAS ──────────────────────────────────────────────────────────────────

function recrias(db, opciones = {}) {
  const hoy = opciones.hoy || new Date().toISOString().slice(0, 10);
  const animales = db.prepare(`
    SELECT id, rp, fecha_nac, sexo, categoria, padre_rp
    FROM animales
    WHERE upper(COALESCE(estado,'ACTIVO'))='ACTIVO'
      AND (upper(COALESCE(categoria,'')) LIKE '%RECRIA%'
        OR upper(COALESCE(categoria,'')) LIKE '%VAQUILLONA%'
        OR upper(COALESCE(categoria,'')) LIKE '%TERNER%')
      AND fecha_nac IS NOT NULL
      AND fecha_nac <= date('now','-6 months')
    ORDER BY fecha_nac DESC`).all();

  const filas = animales.map(a => {
    const pesadas = db.prepare("SELECT fecha, peso, contexto, gdp FROM pesadas WHERE animal_id=? ORDER BY fecha").all(a.id);
    const destete = (pesadas.find(p => p.contexto === "DESTETE") || {}).peso || null;
    const ultima = pesadas[pesadas.length - 1] || null;
    const previa = pesadas.length > 1 ? pesadas[pesadas.length - 2] : null;

    // GDP del último tramo: es el que dice cómo viene ahora, no el histórico.
    let gdp = ultima && ultima.gdp != null ? Math.round(ultima.gdp * 1000) / 1000 : null;
    if (!gdp && ultima && previa) {
      const d = dias(previa.fecha, ultima.fecha);
      if (d > 0) gdp = Math.round(((ultima.peso - previa.peso) / d) * 1000) / 1000;
    }

    const meses = a.fecha_nac ? Math.floor(dias(a.fecha_nac, hoy) / 30.44) : null;
    const sinPesar = ultima ? dias(ultima.fecha, hoy) : null;
    const hembra = !String(a.sexo || "").toUpperCase().startsWith("M");
    const peso = ultima ? ultima.peso : null;

    const alertas = [];
    let estado = "OK";
    if (gdp != null && gdp < UMBRALES.gdp_bajo) { alertas.push(`GDP ${gdp}`); estado = "ATRASADA"; }
    // Con una sola pesada no se puede calcular la ganancia diaria.
    if (gdp == null && pesadas.length < 2) { alertas.push("falta una segunda pesada"); estado = "SIN DATOS"; }
    if (sinPesar != null && sinPesar > UMBRALES.dias_sin_pesar) alertas.push(`${sinPesar} días sin pesar`);
    if (hembra && meses >= UMBRALES.edad_servicio && peso >= UMBRALES.peso_servicio && estado !== "ATRASADA") {
      estado = "SERVICIO";
    }

    return {
      rp: a.rp, fecha_nac: a.fecha_nac, meses, sexo: a.sexo, padre: a.padre_rp,
      destete, ultima_fecha: ultima ? ultima.fecha : null, peso, gdp,
      dias_sin_pesar: sinPesar, estado, alertas
    };
  });

  const gdps = filas.map(f => f.gdp).filter(Boolean);
  const pesos = filas.map(f => f.peso).filter(Boolean);
  return {
    filas,
    resumen: {
      total: filas.length,
      hembras: filas.filter(f => !String(f.sexo || "").toUpperCase().startsWith("M")).length,
      machos: filas.filter(f => String(f.sexo || "").toUpperCase().startsWith("M")).length,
      gdp_prom: prom(gdps),
      peso_prom: pesos.length ? Math.round(prom(pesos)) : null,
      para_servicio: filas.filter(f => f.estado === "SERVICIO").length
    },
    pendientes: [
      { n: filas.filter(f => f.estado === "ATRASADA").length,
        texto: `con GDP por debajo de ${UMBRALES.gdp_bajo}`, nivel: "alta" },
      { n: filas.filter(f => f.dias_sin_pesar > UMBRALES.dias_sin_pesar).length,
        texto: `sin pesar hace más de ${UMBRALES.dias_sin_pesar} días`, nivel: "media" },
      { n: filas.filter(f => f.estado === "SERVICIO").length,
        texto: "en edad de primer servicio", nivel: "media" },
      { n: filas.filter(f => f.estado === "SIN DATOS").length,
        texto: "sin GDP: les falta una segunda pesada", nivel: "media" }
    ].filter(p => p.n > 0)
  };
}

module.exports = { vacas, toros, nacimientos, recrias, bloqueDe, BLOQUES_DEFAULT, UMBRALES };
