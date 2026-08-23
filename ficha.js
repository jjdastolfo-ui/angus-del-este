// ─────────────────────────────────────────────────────────────────────────────
// TACTO CORREGIDO Y FICHA DE LA VACA
//
// El veterinario estima en la manga de dónde viene la preñez, pero el
// nacimiento manda: la fecha real del parto dice cuándo se preñó esa vaca.
//
// Contando desde la fecha probable de parto de la IATF:
//
//     ±10 días        →  IATF
//     +10 a +30       →  TORO CABEZA
//     +30 a +50       →  TORO CUERPO
//     +50 a +70       →  TORO COLA
//
// Las vacas donde el tacto de la manga no coincide con lo que dijo el
// nacimiento son las "corregidas". Importa porque no es lo mismo preñar en la
// cabeza del repaso que en la cola: son 40 días de diferencia en el destete.
// ─────────────────────────────────────────────────────────────────────────────

const GESTACION = 283;
const VENTANA_IATF = 10;   // días a cada lado de la FPP
const TRAMO = 20;          // duración de cada tramo del repaso

const dias = (a, b) => Math.round((new Date(b) - new Date(a)) / 86400000);
const sumar = (f, n) => { const d = new Date(f); d.setDate(d.getDate() + n); return d.toISOString().slice(0, 10); };
const prom = a => a.length ? Math.round((a.reduce((x, y) => x + y, 0) / a.length) * 100) / 100 : null;
const meses = (a, b) => a && b ? Math.round(dias(a, b) / 30.44) : null;

/**
 * De dónde vino la preñez, según cuándo nació el ternero.
 * @param fechaIatf   fecha de la inseminación
 * @param fechaParto  fecha real de nacimiento del ternero
 */
function origenPorNacimiento(fechaIatf, fechaParto) {
  if (!fechaIatf || !fechaParto) return null;
  const fpp = sumar(fechaIatf, GESTACION);
  const d = dias(fpp, fechaParto);   // negativo = antes de la FPP

  if (Math.abs(d) <= VENTANA_IATF) return { origen: "IATF", dias: d, fpp };
  if (d < -VENTANA_IATF) {
    // Nació antes de la ventana: prematuro o la fecha del servicio está mal.
    return { origen: "REVISAR", dias: d, fpp, aviso: `nació ${Math.abs(d)} días antes de la fecha probable` };
  }
  const desde = d - VENTANA_IATF;          // cuántos días después de cerrar la ventana
  if (desde <= TRAMO)     return { origen: "TORO CABEZA", dias: d, fpp };
  if (desde <= TRAMO * 2) return { origen: "TORO CUERPO", dias: d, fpp };
  if (desde <= TRAMO * 3) return { origen: "TORO COLA",   dias: d, fpp };
  return { origen: "TORO TARDIO", dias: d, fpp, aviso: `nació ${d} días después de la FPP de IATF` };
}

// Compara lo que dijo el veterinario con lo que dijo el nacimiento.
function corregirTacto(tactoManga, origenReal) {
  if (!origenReal) return { corregido: false };
  const norm = t => String(t || "").toUpperCase()
    .replace(/PREÑADA_?/g, "").replace(/PRENADA_?/g, "").replace(/_/g, " ").trim();
  const dijo = norm(tactoManga);
  const es = origenReal.origen;

  // "IATF" contra "IATF", "TORO" contra cualquier tramo de repaso.
  const mismoGrupo = (dijo.includes("IATF") && es === "IATF") ||
                     (dijo.includes("TORO") && es.startsWith("TORO"));
  // Si dijo el tramo exacto, se compara completo.
  const exacto = dijo === es || (dijo.includes("IATF") && es === "IATF");

  return {
    manga: tactoManga || null,
    real: es,
    corregido: !exacto && !!tactoManga,
    // Cambió de grupo: dijo IATF y era del toro, o al revés. Es lo importante.
    cambio_grupo: !!tactoManga && !mismoGrupo,
    dias: origenReal.dias, fpp: origenReal.fpp, aviso: origenReal.aviso
  };
}

// ── FICHA DE LA VACA ─────────────────────────────────────────────────────────

/**
 * Todo lo de una vaca en un solo lugar: sus números, de dónde viene, y una
 * fila por campaña con el servicio, el tacto, el parto y el ternero.
 */
function ficha(db, rp, opciones = {}) {
  const hoy = opciones.hoy || new Date().toISOString().slice(0, 10);
  const v = db.prepare("SELECT * FROM animales WHERE upper(rp)=upper(?)").get(String(rp).trim());
  if (!v) return { ok: false, error: `No encuentro la vaca ${rp}` };

  // Peso adulto: el de la vaca al destete, preñada de 3 a 6 meses.
  const pesoAdulto = (db.prepare(`SELECT peso FROM pesadas WHERE animal_id=?
      AND upper(COALESCE(contexto,'')) LIKE '%ADULT%' ORDER BY fecha DESC LIMIT 1`).get(v.id)
    || db.prepare("SELECT peso FROM pesadas WHERE animal_id=? ORDER BY fecha DESC LIMIT 1").get(v.id)
    || {}).peso || null;

  let servicios = [];
  try {
    servicios = db.prepare(`SELECT * FROM servicios WHERE animal_id=?
                            ORDER BY COALESCE(temporada,'') DESC, id DESC`).all(v.id);
  } catch (e) {}

  const crias = db.prepare(`
    SELECT a.id, a.rp, a.fecha_nac, a.sexo, a.pelo, a.estado, a.padre_rp,
           (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='NACIMIENTO' ORDER BY p.fecha LIMIT 1) pn,
           (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='DESTETE' ORDER BY p.fecha DESC LIMIT 1) destete
    FROM animales a WHERE upper(COALESCE(a.madre_rp,''))=upper(?)
    ORDER BY a.fecha_nac`).all(v.rp);

  // Una fila por campaña: se cruza el servicio con la cría que salió de él.
  const campanas = [];
  for (const s of servicios) {
    const temporada = s.temporada || "";
    // La parición cae el año siguiente al servicio.
    const anioPar = temporada.includes("/")
      ? "20" + temporada.split("/")[1]
      : String(parseInt(temporada) + 1);
    const cria = crias.find(c => String(c.fecha_nac || "").startsWith(anioPar)) || null;

    const fIatf = s.fecha_iatf || null;
    const real = cria ? origenPorNacimiento(fIatf, cria.fecha_nac) : null;
    const tacto = corregirTacto(s.resultado, real);

    campanas.push({
      campana: temporada,
      // El servicio como se cargó: los toros que entraron.
      // El servicio como se cargó, con todos los toros que entraron.
      servicio: [s.semen_iatf, s.toro_natural].filter(Boolean).join(" · ") || null,
      semen: s.semen_iatf || null, repaso: s.toro_natural || null,
      fecha_iatf: fIatf, fecha_toro: s.fecha_ingreso_toro,
      tacto_manga: s.resultado || null,
      tacto_real: tacto.real || null,
      tacto_corregido: tacto.corregido,
      cambio_grupo: tacto.cambio_grupo,
      fpp: tacto.fpp || (fIatf ? sumar(fIatf, GESTACION) : null),
      dias_vs_fpp: tacto.dias ?? null,
      aviso: tacto.aviso || null,
      parto: cria ? cria.fecha_nac : null,
      bloque: cria ? bloqueDe(cria.fecha_nac) : null,
      ternero: cria ? cria.rp : null,
      peso_nac: cria ? cria.pn : null,
      destete: cria ? cria.destete : null,
      sexo: cria ? cria.sexo : null,
      pelo: cria ? cria.pelo : null,
      padre_cria: cria ? cria.padre_rp : null
    });
  }
  // Campañas sin servicio cargado pero con cría: no se pierden.
  for (const c of crias) {
    const anio = String(c.fecha_nac || "").slice(0, 4);
    if (campanas.some(x => String(x.parto || "").startsWith(anio))) continue;
    campanas.push({ campana: `${+anio - 1}/${String(anio).slice(2)}`, servicio: null,
      tacto_manga: null, tacto_real: null, parto: c.fecha_nac, bloque: bloqueDe(c.fecha_nac),
      ternero: c.rp, peso_nac: c.pn, destete: c.destete, sexo: c.sexo, pelo: c.pelo,
      padre_cria: c.padre_rp, sin_servicio: true });
  }
  campanas.sort((a, b) => String(a.campana).localeCompare(String(b.campana)));

  // Los números de arriba.
  const pns = crias.map(c => c.pn).filter(Boolean);
  const des = crias.map(c => c.destete).filter(Boolean);
  const destetePromedio = prom(des);
  const fechas = crias.map(c => c.fecha_nac).filter(Boolean).sort();
  const intervalos = [];
  for (let i = 1; i < fechas.length; i++) intervalos.push(dias(fechas[i - 1], fechas[i]));

  // Edad a la primera preñez: mide precocidad. Se calcula hacia atrás desde el
  // primer parto, restando la gestación.
  let primeraPrenez = null;
  if (fechas.length && v.fecha_nac) {
    const m = meses(v.fecha_nac, sumar(fechas[0], -GESTACION));
    // Menos de 12 meses es imposible: significa que falta el primer parto o que
    // la fecha de nacimiento está mal. Mejor no mostrar nada que un dato falso.
    primeraPrenez = (m >= 12 && m <= 60) ? m : null;
  }

  return {
    ok: true,
    rp: v.rp,
    hba: v.hbu || v.registro || null,
    chip: v.chip || null,
    pelo: v.pelo || null,
    categoria: v.categoria || null,
    estado: v.estado || null,
    // Los ocho números
    resumen: {
      edad_meses: meses(v.fecha_nac, hoy),
      peso_adulto: pesoAdulto,
      partos: crias.length,
      pn_promedio: prom(pns),
      destete_promedio: destetePromedio,
      // La eficiencia: destete promedio sobre el peso de la vaca.
      eficiencia: (destetePromedio && pesoAdulto)
        ? Math.round((destetePromedio / pesoAdulto) * 100) : null,
      intervalo_partos: intervalos.length ? Math.round(prom(intervalos)) : null,
      primera_prenez_meses: primeraPrenez
    },
    // De dónde viene
    origen: {
      nacimiento: v.fecha_nac || null,
      peso_nacer: (db.prepare(`SELECT peso FROM pesadas WHERE animal_id=? AND contexto='NACIMIENTO'
                               ORDER BY fecha LIMIT 1`).get(v.id) || {}).peso || null,
      padre: v.padre_rp || null,
      madre: v.madre_rp || null,
      hba: v.hbu || v.registro || null,
      caravana: v.chip || null
    },
    campanas,
    // Cuántas veces el nacimiento desmintió al tacto de la manga.
    corregidos: campanas.filter(c => c.tacto_corregido).length
  };
}

function bloqueDe(f) {
  if (!f) return null;
  const md = String(f).slice(5, 10);
  if (md <= "08-31") return "CABEZA";
  if (md <= "09-30") return "CUERPO";
  if (md <= "10-31") return "COLA";
  return "TARDIA";
}

// Cuántas vacas del campo tienen el tacto corregido por el nacimiento.
function resumenCorregidos(db, opciones = {}) {
  const temporada = opciones.temporada || null;
  let servicios = [];
  try {
    servicios = db.prepare(`
      SELECT s.*, a.rp FROM servicios s JOIN animales a ON a.id = s.animal_id
      ${temporada ? "WHERE s.temporada = ?" : ""}`).all(...(temporada ? [temporada] : []));
  } catch (e) { return { total: 0, corregidos: [], por_origen: [] }; }

  const corregidos = [], porOrigen = {};
  for (const s of servicios) {
    if (!s.fecha_iatf) continue;
    const anioPar = String(parseInt(s.temporada) + 1);
    const cria = db.prepare(`SELECT rp, fecha_nac FROM animales
      WHERE upper(COALESCE(madre_rp,''))=upper(?) AND fecha_nac LIKE ?
      ORDER BY fecha_nac LIMIT 1`).get(s.rp, `${anioPar}%`);
    if (!cria) continue;

    const real = origenPorNacimiento(s.fecha_iatf, cria.fecha_nac);
    const t = corregirTacto(s.resultado, real);
    porOrigen[t.real] = (porOrigen[t.real] || 0) + 1;
    if (t.corregido) {
      corregidos.push({ rp: s.rp, temporada: s.temporada, manga: t.manga, real: t.real,
        parto: cria.fecha_nac, dias: t.dias, cambio_grupo: t.cambio_grupo });
    }
  }
  return {
    total: corregidos.length,
    cambiaron_grupo: corregidos.filter(c => c.cambio_grupo).length,
    corregidos,
    por_origen: Object.entries(porOrigen).map(([origen, n]) => ({ origen, n })).sort((a, b) => b.n - a.n)
  };
}

module.exports = {
  ficha, origenPorNacimiento, corregirTacto, resumenCorregidos, bloqueDe,
  GESTACION, VENTANA_IATF, TRAMO
};
