// ─────────────────────────────────────────────────────────────────────────────
// PLANTEL
//
// Reemplaza a decision.js, ficha.js y tablero.js juntos.
//
// El error que se arrastraba: la lista se partía en tres (eliminatorias,
// ordenadas, en curso) y cualquier consumidor que olvidara una de las tres
// perdía animales sin darse cuenta. Acá hay UNA lista con todos los vientres;
// cada uno lleva su estado, y quien quiera filtrar filtra.
//
// Todo lo que se calcula sale de los datos. Lo único que se asume es
// conocimiento de ganadería: la gestación son 283 días, una vaca desteta un
// ternero por año. Los cortes de bloque y las temporadas se deducen del campo.
// ─────────────────────────────────────────────────────────────────────────────

const GESTACION = 283;
const VENTANA_IATF = 10;   // días a cada lado de la FPP para atribuir a IATF
const TRAMO_REPASO = 20;   // cada tramo del toro de repaso

const dias = (a, b) => (a && b) ? Math.round((new Date(b) - new Date(a)) / 86400000) : null;
const sumar = (f, n) => { const d = new Date(f); d.setDate(d.getDate() + n); return d.toISOString().slice(0, 10); };
const meses = (a, b) => (a && b) ? Math.round(dias(a, b) / 30.44) : null;
const r1 = n => n == null || !isFinite(n) ? null : Math.round(n * 10) / 10;
const prom = a => a.length ? r1(a.reduce((x, y) => x + y, 0) / a.length) : null;

// ── ESQUEMA ──────────────────────────────────────────────────────────────────

function init(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS notas_campo (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      animal_rp TEXT NOT NULL, fecha TEXT NOT NULL, texto TEXT NOT NULL,
      causa TEXT, grave INTEGER DEFAULT 0, usuario TEXT,
      created_at TEXT DEFAULT (datetime('now')));
    CREATE INDEX IF NOT EXISTS idx_notas_rp ON notas_campo(animal_rp);
  `);
}

// ── EL CALENDARIO SE DEDUCE, NO SE CARGA ─────────────────────────────────────
// Cuándo se sirve y cuándo se pare sale de los propios registros del campo. Si
// un año la parición se corre, el cálculo se corre solo.

function calendario(db) {
  const q = (sql, ...p) => { try { return db.prepare(sql).all(...p); } catch (e) { return []; } };

  const servicios = q(`
    SELECT temporada,
           MIN(COALESCE(fecha_iatf, fecha_ingreso_toro)) desde,
           MAX(COALESCE(fecha_ingreso_toro, fecha_iatf)) hasta,
           COUNT(*) n
    FROM servicios WHERE temporada IS NOT NULL AND temporada <> ''
    GROUP BY temporada ORDER BY temporada DESC LIMIT 6`);

  const pariciones = q(`
    SELECT substr(fecha_nac,1,4) anio, MIN(fecha_nac) primero, MAX(fecha_nac) ultimo, COUNT(*) n
    FROM animales WHERE fecha_nac IS NOT NULL AND COALESCE(madre_rp,'') <> ''
    GROUP BY anio ORDER BY anio DESC LIMIT 6`);

  // Los cortes de bloque salen de cuándo DEBERÍA parir el rodeo, que es la fecha
  // probable de parto del servicio. Tomar el primer nacimiento del año no sirve:
  // un parto suelto en marzo correría todos los bloques ocho meses.
  let cortes = null;
  const serv = servicios[0];
  if (serv && serv.desde) {
    const fpp = sumar(serv.desde, GESTACION);
    cortes = { referencia: fpp, origen: `FPP del servicio ${serv.temporada}`,
      CABEZA: sumar(fpp, 20), CUERPO: sumar(fpp, 40), COLA: sumar(fpp, 60) };
  } else {
    // Sin servicios cargados, se busca dónde se concentra la parición: el mes
    // con más nacimientos, no el primero que aparece.
    const meses = q(`
      SELECT substr(fecha_nac,1,7) mes, COUNT(*) n FROM animales
      WHERE fecha_nac IS NOT NULL AND COALESCE(madre_rp,'') <> ''
      GROUP BY mes ORDER BY n DESC LIMIT 1`);
    if (meses[0]) {
      const inicio = meses[0].mes + "-01";
      cortes = { referencia: inicio, origen: `mes con más partos (${meses[0].mes})`,
        CABEZA: sumar(inicio, 20), CUERPO: sumar(inicio, 40), COLA: sumar(inicio, 60) };
    }
  }

  return { servicios, pariciones, cortes };
}

function bloqueDe(fecha, cortes) {
  if (!fecha) return null;
  if (cortes && cortes.referencia) {
    if (fecha <= cortes.CABEZA) return "CABEZA";   // incluye los adelantados
    if (fecha <= cortes.CUERPO) return "CUERPO";
    if (fecha <= cortes.COLA)   return "COLA";
    return "TARDIA";
  }
  // Sin datos para deducir, se usan los tramos habituales del hemisferio sur.
  const md = String(fecha).slice(5, 10);
  if (md <= "08-31") return "CABEZA";
  if (md <= "09-30") return "CUERPO";
  if (md <= "10-31") return "COLA";
  return "TARDIA";
}

// ── DE DÓNDE VINO LA PREÑEZ ──────────────────────────────────────────────────
// El veterinario estima en la manga; el nacimiento confirma. Contando desde la
// fecha probable de parto de la IATF: ±10 días es IATF, después tramos de 20.

function origenPreñez(fechaIatf, fechaParto) {
  if (!fechaIatf || !fechaParto) return null;
  const fpp = sumar(fechaIatf, GESTACION);
  const d = dias(fpp, fechaParto);
  if (Math.abs(d) <= VENTANA_IATF) return { origen: "IATF", dias: d, fpp };
  if (d < 0) return { origen: "REVISAR", dias: d, fpp,
    aviso: `nació ${Math.abs(d)} días antes de lo posible para esa IATF` };
  const t = d - VENTANA_IATF;
  if (t <= TRAMO_REPASO)     return { origen: "TORO CABEZA", dias: d, fpp };
  if (t <= TRAMO_REPASO * 2) return { origen: "TORO CUERPO", dias: d, fpp };
  if (t <= TRAMO_REPASO * 3) return { origen: "TORO COLA",   dias: d, fpp };
  return { origen: "REVISAR", dias: d, fpp,
    aviso: `nació ${d} días después de la FPP: puede haber repetido celo o faltar un servicio` };
}

function tactoCorregido(tactoManga, real) {
  if (!real || !tactoManga) return { manga: tactoManga || null, real: real ? real.origen : null, corregido: false };
  const dijo = String(tactoManga).toUpperCase().replace(/PREÑADA_?|PRENADA_?/g, "").replace(/_/g, " ").trim();
  const es = real.origen;
  const mismoGrupo = (dijo.includes("IATF") && es === "IATF") ||
                     (dijo.includes("TORO") && es.startsWith("TORO"));
  const exacto = dijo === es || (dijo.includes("IATF") && es === "IATF");
  return { manga: tactoManga, real: es, corregido: !exacto, cambio_grupo: !mismoGrupo,
           dias: real.dias, fpp: real.fpp, aviso: real.aviso };
}

// ── EL PLANTEL: UNA SOLA LISTA ───────────────────────────────────────────────

/**
 * Todos los vientres del campo, cada uno con su historia completa y su estado.
 * No se parte en listas: quien necesite un subconjunto, filtra por `estado`.
 */
function plantel(db, opciones = {}) {
  const hoy = opciones.hoy || new Date().toISOString().slice(0, 10);
  const cal = calendario(db);

  // La temporada de parición en curso: la más reciente con nacimientos, o el
  // año de hoy si todavía no hay ninguno cargado.
  const anioParicion = opciones.anio ||
    (cal.pariciones[0] && cal.pariciones[0].anio >= String(+hoy.slice(0, 4) - 1)
      ? cal.pariciones[0].anio : hoy.slice(0, 4));

  // Un vientre es toda hembra activa que ya parió o que está en categoría de
  // vientre. Se incluyen las vaquillonas con servicio: son el plantel futuro.
  const vientres = db.prepare(`
    SELECT a.* FROM animales a
    WHERE upper(COALESCE(a.sexo,'')) LIKE 'H%'
      AND upper(COALESCE(a.estado,'ACTIVO')) = 'ACTIVO'
      AND (
        upper(COALESCE(a.categoria,'')) LIKE '%VACA%'
        OR upper(COALESCE(a.categoria,'')) LIKE '%VIENTRE%'
        -- Ya parió: es vientre sin discusión.
        OR EXISTS (SELECT 1 FROM animales h WHERE upper(COALESCE(h.madre_rp,'')) = upper(a.rp))
        -- Entró a servicio: es vientre aunque todavía no haya parido.
        OR EXISTS (SELECT 1 FROM servicios s WHERE s.animal_id = a.id)
      )
    ORDER BY a.rp`).all();

  // Las que ya tienen decidida la salida (venta, terminación) no son plantel:
  // están en Destinos hasta que se van. Se pueden pedir igual con incluirDestinados.
  let salen = new Set();
  try { salen = require("./destinos.js").destinadosASalir(db); } catch (e) {}
  const destinadas = vientres.filter(v => salen.has(String(v.rp).toUpperCase())).length;
  const quedan = opciones.incluirDestinados ? vientres : vientres.filter(v => !salen.has(String(v.rp).toUpperCase()));

  // Las crías que viven en otros campos de la empresa cuentan igual: son hijos
  // de esta vaca. Quien llama pasa una función rp → [crías]; sin eso, sólo las
  // de este campo (el módulo sigue sirviendo solo).
  const criasFuera = typeof opciones.criasFuera === "function" ? opciones.criasFuera : () => [];
  const filas = quedan.map(v => armarVientre(db, v, { hoy, anioParicion, cortes: cal.cortes, criasFuera }));
  const resumen = resumir(filas, anioParicion);
  resumen.destinadas = destinadas;
  if (destinadas && !opciones.incluirDestinados) resumen.avisos.push({ n: destinadas, texto: "con destino de salida marcado: no se cuentan acá, están en Destinos" });

  // Los internos sólo se devuelven si alguien los pide: engordan la respuesta.
  const salida = opciones.completo ? filas : filas.map(({ _id, _crias, _servicios, ...f }) => f);
  return { filas: salida, resumen, calendario: cal, anio_paricion: anioParicion, hoy };
}

function armarVientre(db, v, ctx) {
  const { hoy, anioParicion, cortes } = ctx;

  // Peso adulto: el de la vaca al destete, preñada de 3 a 6 meses. Si no está
  // marcado como tal, vale la última pesada.
  const pAdulto = (db.prepare(`SELECT peso FROM pesadas WHERE animal_id=?
      AND upper(COALESCE(contexto,'')) LIKE '%ADULT%' ORDER BY fecha DESC LIMIT 1`).get(v.id)
    || db.prepare(`SELECT peso FROM pesadas WHERE animal_id=? ORDER BY fecha DESC LIMIT 1`).get(v.id)
    || {}).peso || null;

  let servicios = [];
  try {
    servicios = db.prepare(`SELECT * FROM servicios WHERE animal_id=?
                            ORDER BY COALESCE(temporada,'') DESC, id DESC`).all(v.id);
  } catch (e) {}

  const criasAca = db.prepare(`
    SELECT a.id, a.rp, a.fecha_nac, a.sexo, a.pelo, a.estado, a.padre_rp,
      (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND upper(COALESCE(p.contexto,''))='NACIMIENTO'
       ORDER BY p.fecha LIMIT 1) pn,
      (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND upper(COALESCE(p.contexto,''))='DESTETE'
       ORDER BY p.fecha DESC LIMIT 1) destete
    FROM animales a WHERE upper(COALESCE(a.madre_rp,''))=upper(?)
    ORDER BY a.fecha_nac`).all(v.rp);
  // Los hijos que están en otro campo de la empresa cuentan como partos suyos:
  // si no, la vaca figura vacía cuando en realidad crió afuera.
  const deFuera = (ctx.criasFuera ? ctx.criasFuera(v.rp) : []).filter(c => !criasAca.some(x => String(x.rp).toUpperCase() === String(c.rp).toUpperCase()));
  const crias = [...criasAca, ...deFuera].sort((a, b) => String(a.fecha_nac || "").localeCompare(String(b.fecha_nac || "")));

  let notas = [];
  try {
    notas = db.prepare(`SELECT fecha,texto,causa,grave FROM notas_campo
                        WHERE upper(animal_rp)=upper(?) ORDER BY fecha DESC`).all(v.rp);
  } catch (e) {}

  // El servicio del que sale la parición en curso: el del año anterior.
  const servicio = servicios.find(s => {
    const t = String(s.temporada || "");
    return t === String(+anioParicion - 1) || t.startsWith(String(+anioParicion - 1));
  }) || servicios[0] || null;

  const criaAnio = crias.find(c => String(c.fecha_nac || "").startsWith(anioParicion)) || null;

  // ── ESTADO ─────────────────────────────────────────────────────────────
  // Depende de dónde está la vaca en el ciclo, no de una regla fija. Una
  // preñada que todavía no llegó a su fecha de parto no falló: está esperando.
  const fServ = servicio ? (servicio.fecha_iatf || servicio.fecha_ingreso_toro) : null;
  const fpp = fServ ? sumar(fServ, GESTACION) : null;
  const prenada = servicio && /PRE/i.test(String(servicio.resultado || ""));
  const vencida = fpp ? dias(fpp, hoy) > 25 : false;
  const notaGrave = notas.find(n => n.grave && String(n.fecha || "").startsWith(anioParicion));

  let estado, causa = null, destete_ok = null;
  if (notaGrave) {
    estado = "FALLÓ"; destete_ok = false; causa = (notaGrave.causa || "").split(",")[0];
  } else if (criaAnio) {
    if (criaAnio.estado && String(criaAnio.estado).toUpperCase() !== "ACTIVO") {
      estado = "FALLÓ"; destete_ok = false; causa = "TERNERO_MUERTO";
    } else if (criaAnio.destete != null) {
      estado = "DESTETÓ"; destete_ok = true;
    } else if (dias(criaAnio.fecha_nac, hoy) > 240) {
      estado = "FALLÓ"; destete_ok = false; causa = "NO_CRIO";
    } else {
      estado = "CRIANDO";   // el ternero está al pie
    }
  } else if (prenada && !vencida) {
    estado = "PREÑADA";
  } else if (prenada && vencida) {
    estado = "FALLÓ"; destete_ok = false; causa = "ABORTO";
  } else if (servicio) {
    estado = "FALLÓ"; destete_ok = false; causa = "VACIA";
  } else {
    estado = "SIN SERVICIO";
  }

  // ── NÚMEROS ────────────────────────────────────────────────────────────
  const pns = crias.map(c => c.pn).filter(x => x > 0);
  const dess = crias.map(c => c.destete).filter(x => x > 0);
  const destetePromedio = prom(dess);
  const fechasParto = crias.map(c => c.fecha_nac).filter(Boolean).sort();
  const intervalos = [];
  for (let i = 1; i < fechasParto.length; i++) {
    const d = dias(fechasParto[i - 1], fechasParto[i]);
    if (d > 250 && d < 900) intervalos.push(d);   // uno por año: fuera de ahí es error
  }

  // Edad, descartando fechas imposibles.
  let edadM = meses(v.fecha_nac, hoy);
  const edadRara = edadM != null && (edadM < 12 || edadM > 300);
  if (edadRara) edadM = null;

  // Primera preñez: hacia atrás desde el primer parto.
  let primeraPrenez = null;
  if (fechasParto.length && v.fecha_nac && !edadRara) {
    const m = meses(v.fecha_nac, sumar(fechasParto[0], -GESTACION));
    if (m >= 12 && m <= 60) primeraPrenez = m;
  }

  const bloque = criaAnio ? bloqueDe(criaAnio.fecha_nac, cortes) : null;
  const bloquesPrevios = fechasParto.slice(-4).map(f => bloqueDe(f, cortes));

  return {
    rp: v.rp,
    hba: v.hbu || v.registro || null,
    chip: v.chip || null,
    pelo: v.pelo || null,
    categoria: v.categoria || null,
    fecha_nac: v.fecha_nac || null,
    padre: v.padre_rp || null,
    madre: v.madre_rp || null,

    estado, causa, causa_texto: causa ? textoCausa(causa) : null, destete_ok,

    edad_meses: edadM, edad_rara: edadRara,
    peso_adulto: pAdulto,
    partos: crias.length,
    hijos_otros_campos: deFuera.length || undefined,
    ternero_campo: criaAnio && criaAnio.campo ? criaAnio.campo : undefined,
    ternero_campo_nombre: criaAnio && criaAnio.campo_nombre ? criaAnio.campo_nombre : undefined,
    pn_prom: prom(pns),
    destete_prom: destetePromedio,
    // La medida que decide: cuánto desteta en relación a su propio peso.
    eficiencia: (destetePromedio && pAdulto) ? Math.round((destetePromedio / pAdulto) * 100) : null,
    ipp: intervalos.length ? Math.round(prom(intervalos)) : null,
    primera_prenez: primeraPrenez,

    // La temporada en curso
    servicio: fServ, padre_servicio: servicio ? (servicio.semen_iatf || servicio.toro_natural) : null,
    tacto: servicio ? servicio.resultado : null,
    fpp, parto: criaAnio ? criaAnio.fecha_nac : null,
    ternero: criaAnio ? criaAnio.rp : null,
    peso_nac: criaAnio ? criaAnio.pn : null,
    destete: criaAnio ? criaAnio.destete : null,
    bloque, bloques_previos: bloquesPrevios,
    se_atrasa: bloquesPrevios.length >= 2 &&
      ["COLA", "TARDIA"].includes(bloquesPrevios[bloquesPrevios.length - 1]) &&
      ["COLA", "TARDIA"].includes(bloquesPrevios[bloquesPrevios.length - 2]),

    notas: notas.map(n => ({ fecha: n.fecha, texto: n.texto, grave: !!n.grave })),
    _id: v.id, _crias: crias, _servicios: servicios
  };
}

const CAUSAS = { VACIA: "quedó vacía", ABORTO: "abortó", TERNERO_MUERTO: "el ternero nació muerto",
                 NO_CRIO: "no crió el ternero", MUERTA: "murió" };
const textoCausa = c => CAUSAS[c] || String(c || "").toLowerCase();

function resumir(filas, anioParicion) {
  const porEstado = {};
  filas.forEach(f => { porEstado[f.estado] = (porEstado[f.estado] || 0) + 1; });

  // El destete efectivo se calcula sólo sobre las que completaron el ciclo:
  // contar las preñadas como fracaso da un número falso a mitad de parición.
  const evaluables = filas.filter(f => f.destete_ok !== null);
  const destetaron = evaluables.filter(f => f.destete_ok).length;

  const paridas = filas.filter(f => f.parto);
  const porBloque = {};
  ["CABEZA", "CUERPO", "COLA", "TARDIA"].forEach(b => {
    porBloque[b] = paridas.filter(f => f.bloque === b).length;
  });

  const causas = {};
  filas.filter(f => f.causa).forEach(f => { causas[f.causa] = (causas[f.causa] || 0) + 1; });

  return {
    anio_paricion: anioParicion,
    total: filas.length,
    por_estado: porEstado,
    prenadas: porEstado["PREÑADA"] || 0,
    criando: porEstado["CRIANDO"] || 0,
    destetaron,
    fallaron: porEstado["FALLÓ"] || 0,
    sin_servicio: porEstado["SIN SERVICIO"] || 0,
    evaluables: evaluables.length,
    destete_efectivo: evaluables.length ? Math.round((destetaron / evaluables.length) * 100) : null,
    paridas: paridas.length,
    por_bloque: porBloque,
    en_cabeza: paridas.length ? Math.round((porBloque.CABEZA / paridas.length) * 100) : null,
    peso_adulto_prom: prom(filas.map(f => f.peso_adulto).filter(Boolean)),
    destete_prom: prom(filas.map(f => f.destete_prom).filter(Boolean)),
    pn_prom: prom(filas.map(f => f.pn_prom).filter(Boolean)),
    eficiencia_prom: prom(filas.map(f => f.eficiencia).filter(Boolean)),
    ipp_prom: prom(filas.map(f => f.ipp).filter(Boolean)),
    causas: Object.entries(causas).map(([causa, n]) => ({ causa, texto: textoCausa(causa), n }))
      .sort((a, b) => b.n - a.n),
    // Lo que conviene revisar antes de confiar en los números.
    avisos: avisosDe(filas)
  };
}

function avisosDe(filas) {
  const a = [];
  const sinPeso = filas.filter(f => !f.peso_adulto).length;
  const edadRara = filas.filter(f => f.edad_rara).length;
  const sinDestete = filas.filter(f => f.partos > 0 && !f.destete_prom).length;
  if (sinPeso) a.push({ n: sinPeso, texto: "sin peso adulto cargado: no se les puede calcular la eficiencia" });
  if (edadRara) a.push({ n: edadRara, texto: "con fecha de nacimiento imposible" });
  if (sinDestete) a.push({ n: sinDestete, texto: "con crías pero sin peso de destete" });
  return a;
}

// ── FICHA ────────────────────────────────────────────────────────────────────
// La misma vaca del plantel, más el historial campaña por campaña.

function ficha(db, rp, opciones = {}) {
  const p = plantel(db, { ...opciones, completo: true });
  const v = p.filas.find(f => String(f.rp).toUpperCase() === String(rp).trim().toUpperCase());
  if (!v) return { ok: false, error: `No encuentro ${rp} entre los vientres del campo` };

  const cortes = p.calendario.cortes;
  const campanas = [];

  for (const s of v._servicios) {
    const temporada = String(s.temporada || "");
    const anioPar = temporada.includes("/") ? "20" + temporada.split("/")[1] : String(+temporada + 1);
    const cria = v._crias.find(c => String(c.fecha_nac || "").startsWith(anioPar)) || null;
    const real = cria ? origenPreñez(s.fecha_iatf, cria.fecha_nac) : null;
    const t = tactoCorregido(s.resultado, real);
    campanas.push({
      campana: temporada.includes("/") ? temporada : `${temporada}/${String(+temporada + 1).slice(2)}`,
      servicio: [s.semen_iatf, s.toro_natural].filter(Boolean).join(" · ") || null,
      tacto_manga: s.resultado || null, tacto_real: t.real, tacto_corregido: t.corregido,
      aviso: t.aviso || null,
      parto: cria ? cria.fecha_nac : null,
      bloque: cria ? bloqueDe(cria.fecha_nac, cortes) : null,
      ternero: cria ? cria.rp : null, peso_nac: cria ? cria.pn : null,
      destete: cria ? cria.destete : null, sexo: cria ? cria.sexo : null, pelo: cria ? cria.pelo : null,
      campo: cria && cria.campo ? cria.campo : null, campo_nombre: cria && cria.campo_nombre ? cria.campo_nombre : null
    });
  }
  // Crías sin servicio cargado: igual aparecen.
  for (const c of v._crias) {
    const anio = String(c.fecha_nac || "").slice(0, 4);
    if (campanas.some(x => String(x.parto || "").startsWith(anio))) continue;
    campanas.push({ campana: `${+anio - 1}/${anio.slice(2)}`, servicio: null,
      tacto_manga: null, tacto_real: null, parto: c.fecha_nac, bloque: bloqueDe(c.fecha_nac, cortes),
      ternero: c.rp, peso_nac: c.pn, destete: c.destete, sexo: c.sexo, pelo: c.pelo, sin_servicio: true,
      campo: c.campo || null, campo_nombre: c.campo_nombre || null });
  }
  campanas.sort((a, b) => String(a.campana).localeCompare(String(b.campana)));

  const pesoNacer = (db.prepare(`SELECT peso FROM pesadas WHERE animal_id=?
    AND upper(COALESCE(contexto,''))='NACIMIENTO' ORDER BY fecha LIMIT 1`).get(v._id) || {}).peso || null;

  const { _id, _crias, _servicios, ...limpio } = v;
  return {
    ok: true, ...limpio,
    origen: { nacimiento: v.fecha_nac, peso_nacer: pesoNacer, padre: v.padre,
              madre: v.madre, hba: v.hba, caravana: v.chip },
    campanas,
    corregidos: campanas.filter(c => c.tacto_corregido).length
  };
}

// ── NOTAS DE CAMPO ───────────────────────────────────────────────────────────

const SENALES = [
  { re: /\b(mal\s?pari|nacio muerto|nació muerto|ternero muerto|parto muerto)\b/i, causa: "TERNERO_MUERTO", grave: 1 },
  { re: /\b(no cri|no lo cri|abandon|dejo la cria|dejó la cría|mala madre)\b/i,     causa: "NO_CRIO",        grave: 1 },
  { re: /\b(abort)\b/i,                                                              causa: "ABORTO",         grave: 1 },
  { re: /\b(muri|muerta|se murio|se murió)\b/i,                                      causa: "MUERTA",         grave: 1 },
  { re: /\b(brava|arisca|mal caracter|mal carácter|peligrosa)\b/i,                    causa: "CARACTER",       grave: 0 },
  { re: /\b(mansa|docil|dócil|buena madre|excelente madre)\b/i,                       causa: "BUENA",          grave: 0 },
  { re: /\b(patas|casco|renga|coja|pezu)\b/i,                                          causa: "APLOMOS",        grave: 0 },
  { re: /\b(ubre|mastitis|pezon|pezón)\b/i,                                             causa: "UBRE",           grave: 0 }
];

function guardarNota(db, rp, texto, opciones = {}) {
  const halladas = SENALES.filter(s => s.re.test(String(texto || "")));
  const grave = halladas.find(s => s.grave);
  db.prepare(`INSERT INTO notas_campo (animal_rp,fecha,texto,causa,grave,usuario) VALUES (?,?,?,?,?,?)`)
    .run(rp, opciones.fecha || new Date().toISOString().slice(0, 10), texto,
         halladas.map(s => s.causa).join(",") || null, grave ? 1 : 0, opciones.usuario || null);
  return { ok: true, entendido: halladas.map(s => s.causa), grave: !!grave,
    aviso: grave ? `Queda anotado: ${textoCausa(grave.causa)}. Esta vaca no desteta esta temporada.` : null };
}

module.exports = {
  init, plantel, ficha, calendario, bloqueDe, origenPreñez, tactoCorregido,
  guardarNota, textoCausa, GESTACION, SENALES
};
