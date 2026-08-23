// ─────────────────────────────────────────────────────────────────────────────
// DECISIÓN
//
// El campo tiene tres sistemas, no categorías de inventario:
//
//   CRÍA        la vaca pare y desteta un ternero por año
//   RECRÍA      del destete a los 18 meses, cuando se define la categoría
//   TERMINACIÓN corral, dieta y costo por kilo
//
// Lo que decide qué vaca se queda no es cuánto pesa su ternero, sino cuánto
// desteta EN RELACIÓN A SU PROPIO TAMAÑO: una vaca de 420 kg que desteta 210 es
// más eficiente que una de 600 que desteta 250, porque come menos todo el año.
//
// El único corte duro es NO DESTETAR. Todo lo demás se pondera.
// ─────────────────────────────────────────────────────────────────────────────

const dias = (a, b) => Math.round((new Date(b) - new Date(a)) / 86400000);
const sumarDias = (f, n) => { const d = new Date(f); d.setDate(d.getDate() + n); return d.toISOString().slice(0, 10); };
const GESTACION = 283;
const prom = a => a.length ? Math.round((a.reduce((x, y) => x + y, 0) / a.length) * 100) / 100 : null;
const r2 = n => n == null ? null : Math.round(n * 100) / 100;

// Bloques de parición. Cuanto más concentrada, más kilos al destete.
const BLOQUES = [
  { nombre: "CABEZA", hasta: "08-31" },
  { nombre: "CUERPO", hasta: "09-30" },
  { nombre: "COLA",   hasta: "10-31" }
];
function bloqueDe(f) {
  if (!f) return "VACIA";
  const md = String(f).slice(5, 10);
  for (const b of BLOQUES) if (md <= b.hasta) return b.nombre;
  return "TARDIA";
}

// Motivos por los que una vaca no destetó. Todos son eliminatorios.
const NO_DESTETO = {
  VACIA:          "quedó vacía",
  ABORTO:         "abortó",
  TERNERO_MUERTO: "el ternero nació muerto",
  NO_CRIO:        "no crió el ternero",
  MUERTA:         "murió"
};

// ── PALABRAS QUE APARECEN EN LAS NOTAS DE CAMPO ──────────────────────────────
// Las notas se escriben o se dictan sueltas: "la 2077 malparió", "no crió al
// ternero". Acá se reconoce qué significan para la decisión.
const SENALES = [
  { re: /\b(mal\s?pari|nacio muerto|nació muerto|ternero muerto|parto muerto)\b/i, causa: "TERNERO_MUERTO", grave: true },
  { re: /\b(no cri|no lo cri|abandon|dejo la cria|dejó la cría|mala madre)\b/i,    causa: "NO_CRIO",        grave: true },
  { re: /\b(abort)\b/i,                                                            causa: "ABORTO",         grave: true },
  { re: /\b(muri|muerta|se murio|se murió)\b/i,                                    causa: "MUERTA",         grave: true },
  { re: /\b(brava|arisca|mal caracter|mal carácter|peligrosa|saltarina)\b/i,        causa: "CARACTER",       grave: false },
  { re: /\b(mansa|docil|dócil|buena madre|excelente madre|muy buena)\b/i,           causa: "BUENA",          grave: false, positiva: true },
  { re: /\b(patas|casco|renga|coja|pezu)\b/i,                                       causa: "APLOMOS",        grave: false },
  { re: /\b(ubre|mastitis|pezon|pezón)\b/i,                                          causa: "UBRE",           grave: false },
  { re: /\b(flaca|baja condicion|baja condición|desmejor)\b/i,                       causa: "ESTADO",         grave: false }
];

function leerNota(texto) {
  const out = [];
  for (const s of SENALES) if (s.re.test(String(texto || ""))) out.push(s);
  return out;
}

function init(db) {
  db.exec(`
    -- Notas de campo: lo que se ve mirando los animales, no en la manga.
    -- Se cargan sueltas (por texto o audio) y el sistema las interpreta.
    CREATE TABLE IF NOT EXISTS notas_campo (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      animal_rp TEXT NOT NULL,
      fecha TEXT NOT NULL,
      texto TEXT NOT NULL,
      causa TEXT,              -- lo que el sistema entendió: NO_CRIO, CARACTER…
      grave INTEGER DEFAULT 0, -- si por sí sola define un descarte
      temporada TEXT,
      usuario TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_notas_rp ON notas_campo(animal_rp);

    -- La decisión de marzo, una vez tomada, queda registrada con su motivo.
    CREATE TABLE IF NOT EXISTS decisiones (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      animal_rp TEXT NOT NULL,
      temporada TEXT NOT NULL,
      decision TEXT NOT NULL,   -- QUEDA | TERMINACION | TORO_PADRE | TORO_VENTA | ENGORDE
      motivo TEXT,
      usuario TEXT,
      fecha TEXT DEFAULT (date('now')),
      created_at TEXT DEFAULT (datetime('now')),
      UNIQUE(animal_rp, temporada)
    );
  `);
}

function guardarNota(db, rp, texto, opciones = {}) {
  const senales = leerNota(texto);
  const grave = senales.find(s => s.grave);
  const info = db.prepare(`INSERT INTO notas_campo (animal_rp,fecha,texto,causa,grave,temporada,usuario)
                           VALUES (?,?,?,?,?,?,?)`)
    .run(rp, opciones.fecha || new Date().toISOString().slice(0, 10), texto,
         senales.map(s => s.causa).join(",") || null, grave ? 1 : 0,
         opciones.temporada || null, opciones.usuario || null);
  return {
    id: info.lastInsertRowid,
    entendido: senales.map(s => s.causa),
    grave: !!grave,
    // Si la nota dice algo eliminatorio, se avisa en el momento.
    aviso: grave ? `Queda registrado: ${NO_DESTETO[grave.causa] || grave.causa}. Esta vaca no desteta esta temporada.` : null
  };
}

// ── CRÍA: la productividad de cada vaca ──────────────────────────────────────

/**
 * Para cada vaca: cuánto destetó en relación a su propio peso, en qué bloque
 * parió, cuántos días entre partos, y qué dicen las notas de campo.
 */
function cria(db, opciones = {}) {
  const temporada = opciones.temporada || String(new Date().getFullYear());
  const anioParicion = opciones.anio_paricion || temporada;
  const hoy = opciones.hoy || new Date().toISOString().slice(0, 10);

  const vientres = db.prepare(`
    SELECT id, rp, fecha_nac, categoria
    FROM animales
    WHERE upper(COALESCE(sexo,'')) LIKE 'H%' AND upper(COALESCE(estado,'ACTIVO'))='ACTIVO'
      AND (upper(COALESCE(categoria,'')) LIKE '%VACA%'
        OR EXISTS (SELECT 1 FROM animales h WHERE upper(COALESCE(h.madre_rp,''))=upper(animales.rp)))
    ORDER BY rp`).all();

  const filas = [];

  for (const v of vientres) {
    // Peso adulto: el de la vaca al destete, preñada de 3 a 6 meses.
    const pesoAdulto = (db.prepare(`
      SELECT peso FROM pesadas WHERE animal_id=? AND upper(COALESCE(contexto,'')) LIKE '%ADULT%'
      ORDER BY fecha DESC LIMIT 1`).get(v.id)
      || db.prepare(`SELECT peso FROM pesadas WHERE animal_id=? ORDER BY fecha DESC LIMIT 1`).get(v.id)
      || {}).peso || null;

    const servicio = db.prepare(`SELECT * FROM servicios WHERE animal_id=? AND temporada=?
                                 ORDER BY id DESC LIMIT 1`).get(v.id, String(+temporada - 1)) ||
                     db.prepare(`SELECT * FROM servicios WHERE animal_id=? ORDER BY id DESC LIMIT 1`).get(v.id);

    const crias = db.prepare(`
      SELECT a.id, a.rp, a.fecha_nac, a.sexo, a.estado,
             (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='NACIMIENTO' ORDER BY p.fecha LIMIT 1) pn,
             (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='DESTETE' ORDER BY p.fecha DESC LIMIT 1) des
      FROM animales a WHERE upper(COALESCE(a.madre_rp,''))=upper(?)
      ORDER BY a.fecha_nac DESC`).all(v.rp);

    const criaAnio = crias.find(c => String(c.fecha_nac || "").startsWith(anioParicion)) || null;

    // Notas de campo de esta temporada.
    let notas = [];
    try {
      notas = db.prepare(`SELECT * FROM notas_campo WHERE upper(animal_rp)=upper(?)
                          ORDER BY fecha DESC`).all(v.rp);
    } catch (e) {}
    const notasAnio = notas.filter(n => String(n.fecha || "").startsWith(anioParicion));
    const notaGrave = notasAnio.find(n => n.grave);

    // El estado depende de dónde está la vaca en el ciclo. Una preñada que
    // todavía no llegó a su fecha de parto NO es un aborto: está esperando.
    const prenada = servicio && /PRE/i.test(String(servicio.resultado || ""));
    const base = servicio && (servicio.fecha_iatf || servicio.fecha_ingreso_toro);
    const fppServicio = base ? sumarDias(base, GESTACION) : null;
    // Margen de tolerancia: recién después se considera que falló.
    const yaDebioParir = fppServicio ? dias(fppServicio, hoy) > 25 : false;

    let destete_ok = null, causa = null, estado = null;

    if (notaGrave) {
      // Una nota de campo manda sobre cualquier cálculo.
      destete_ok = false;
      causa = (notaGrave.causa || "").split(",")[0];
      estado = "FALLÓ";
    } else if (criaAnio) {
      // Parió. Falta saber si llegó a destetar.
      if (criaAnio.estado && criaAnio.estado !== "ACTIVO") {
        destete_ok = false; causa = "TERNERO_MUERTO"; estado = "FALLÓ";
      } else if (criaAnio.des != null) {
        destete_ok = true; estado = "DESTETÓ";
      } else if (dias(criaAnio.fecha_nac, hoy) > 240) {
        // Más de 8 meses sin peso de destete: no lo crió.
        destete_ok = false; causa = "NO_CRIO"; estado = "FALLÓ";
      } else {
        // Parió hace poco: el ternero está al pie, todavía no se destetó.
        destete_ok = null; estado = "CRIANDO";
      }
    } else if (prenada && !yaDebioParir) {
      // Preñada esperando parto. No se juzga todavía.
      destete_ok = null; estado = "PREÑADA";
    } else if (prenada && yaDebioParir) {
      // Pasó la fecha con margen y no hay cría: acá sí falló.
      destete_ok = false; causa = "ABORTO"; estado = "FALLÓ";
    } else if (servicio) {
      destete_ok = false; causa = "VACIA"; estado = "FALLÓ";
    } else {
      // Sin servicio esta temporada: no se puede evaluar.
      destete_ok = null; estado = "SIN SERVICIO";
    }

    const destete = criaAnio ? criaAnio.des : null;
    // La medida que importa: kilos destetados por cada 100 kg de vaca.
    const productividad = (destete && pesoAdulto) ? r2((destete / pesoAdulto) * 100) : null;

    const fechas = crias.map(c => c.fecha_nac).filter(Boolean).sort().reverse();
    const ipp = fechas.length >= 2 ? dias(fechas[1], fechas[0]) : null;
    const bloque = bloqueDe(criaAnio ? criaAnio.fecha_nac : null);

    // Historial de bloques: una vaca que se atrasa año a año no se recupera.
    const bloquesPrevios = fechas.slice(0, 4).map(f => bloqueDe(f));
    const seAtrasa = bloquesPrevios.length >= 2 &&
      ["COLA", "TARDIA"].includes(bloquesPrevios[0]) &&
      ["COLA", "TARDIA"].includes(bloquesPrevios[1]);

    const destetes = crias.map(c => c.des).filter(Boolean);
    // Una edad imposible delata una fecha de nacimiento mal cargada.
    let edad = v.fecha_nac ? Math.floor(dias(v.fecha_nac, hoy) / 365.25) : null;
    const edad_sospechosa = edad != null && (edad > 25 || edad < 0);
    if (edad_sospechosa) edad = null;

    filas.push({
      rp: v.rp, edad, edad_sospechosa, fecha_nac: v.fecha_nac, peso_adulto: pesoAdulto,
      servicio: servicio ? (servicio.fecha_iatf || servicio.fecha_ingreso_toro) : null,
      padre: servicio ? (servicio.semen_iatf || servicio.toro_natural) : null,
      tacto: servicio ? servicio.resultado : null,
      parto: criaAnio ? criaAnio.fecha_nac : null,
      ternero: criaAnio ? criaAnio.rp : null,
      peso_nac: criaAnio ? criaAnio.pn : null,
      destete, productividad, bloque, ipp,
      estado, fpp: fppServicio, prenada: !!prenada,
      crias: crias.length,
      destete_prom: prom(destetes),
      destete_ok, causa,
      causa_texto: causa ? (NO_DESTETO[causa] || causa) : null,
      se_atrasa: seAtrasa,
      bloques_previos: bloquesPrevios,
      notas: notasAnio.map(n => ({ fecha: n.fecha, texto: n.texto, causa: n.causa, grave: !!n.grave })),
      notas_total: notas.length
    });
  }

  // Rankings sobre las que sí destetaron: comparar con las que fallaron no sirve.
  const conDatos = filas.filter(f => f.productividad != null);
  const promProd = prom(conDatos.map(f => f.productividad));
  conDatos.sort((a, b) => b.productividad - a.productividad);
  conDatos.forEach((f, i) => {
    f.ranking = i + 1;
    f.vs_promedio = promProd ? r2(f.productividad - promProd) : null;
  });

  // Sólo se juzga a las que ya completaron el ciclo: las preñadas y las que
  // están criando todavía no fallaron ni acertaron.
  const evaluables = filas.filter(f => f.destete_ok !== null);
  const noDestetaron = filas.filter(f => f.destete_ok === false);
  const enCurso = filas.filter(f => f.destete_ok === null);
  const paridas = filas.filter(f => f.parto);
  const porBloque = {};
  ["CABEZA", "CUERPO", "COLA", "TARDIA"].forEach(b => { porBloque[b] = paridas.filter(f => f.bloque === b).length; });

  return {
    filas,
    resumen: {
      vientres: filas.length,
      destetaron: evaluables.filter(f => f.destete_ok).length,
      no_destetaron: noDestetaron.length,
      // Las que todavía no se pueden juzgar, por estado.
      en_curso: enCurso.length,
      por_estado: ["PREÑADA","CRIANDO","DESTETÓ","FALLÓ","SIN SERVICIO"]
        .map(e => ({ estado: e, n: filas.filter(f => f.estado === e).length }))
        .filter(x => x.n > 0),
      // El destete efectivo se calcula sobre las evaluables, no sobre el total:
      // con la parición a mitad de camino, contar el total da un número falso.
      destete_efectivo: evaluables.length
        ? Math.round((evaluables.filter(f => f.destete_ok).length / evaluables.length) * 100) : null,
      evaluables: evaluables.length,
      productividad_prom: promProd,
      destete_prom: prom(filas.map(f => f.destete).filter(Boolean)),
      peso_adulto_prom: prom(filas.map(f => f.peso_adulto).filter(Boolean)),
      ipp_prom: prom(filas.map(f => f.ipp).filter(Boolean)),
      por_bloque: porBloque,
      en_cabeza: paridas.length ? Math.round((porBloque.CABEZA / paridas.length) * 100) : null,
      // Por qué no destetaron, para saber dónde está el problema.
      causas: Object.entries(noDestetaron.reduce((a, f) => {
        const k = f.causa || "SIN CAUSA"; a[k] = (a[k] || 0) + 1; return a;
      }, {})).map(([causa, n]) => ({ causa, texto: NO_DESTETO[causa] || causa, n }))
        .sort((a, b) => b.n - a.n)
    }
  };
}

// ── LA DECISIÓN DE MARZO ─────────────────────────────────────────────────────

/**
 * Ordena los vientres de peor a mejor. Las que no destetaron van arriba y salen
 * sin discusión; el resto se ordena por productividad y se puede ir apretando
 * el criterio hasta llegar a la cantidad que se necesite sacar.
 */
function descartes(db, opciones = {}) {
  const c = cria(db, opciones);
  const objetivo = opciones.objetivo || null;

  const evaluar = f => {
    const motivos = [];
    let puntos = 0;
    // Las que están en curso no se juzgan: se muestran aparte.
    if (f.destete_ok === null) return { puntos: -1, motivos: [{ texto: f.estado.toLowerCase(), peso: "en curso" }] };
    if (!f.destete_ok) {
      motivos.push({ texto: f.causa_texto, peso: "eliminatorio" });
      puntos += 1000;   // sale sí o sí
    }
    if (f.se_atrasa) { motivos.push({ texto: "se atrasa: dos pariciones seguidas en cola o tardía", peso: "alto" }); puntos += 40; }
    else if (["COLA", "TARDIA"].includes(f.bloque)) { motivos.push({ texto: `parió en ${f.bloque.toLowerCase()}`, peso: "medio" }); puntos += 15; }
    if (f.ipp && f.ipp > 420) { motivos.push({ texto: `${f.ipp} días entre partos`, peso: "alto" }); puntos += 30; }
    if (f.vs_promedio != null && f.vs_promedio < -5) {
      motivos.push({ texto: `desteta ${Math.abs(f.vs_promedio)} puntos menos que el promedio`, peso: "alto" });
      puntos += Math.min(40, Math.abs(f.vs_promedio) * 2);
    }
    f.notas.filter(n => !n.grave && n.causa).forEach(n => {
      const positiva = String(n.causa).includes("BUENA");
      motivos.push({ texto: n.texto, peso: positiva ? "a favor" : "nota" });
      // Lo bueno resta puntos de descarte; lo malo suma.
      puntos += positiva ? -15 : 12;
    });
    if (f.edad && f.edad >= 10 && !f.edad_sospechosa) {
      motivos.push({ texto: `${f.edad} años`, peso: "medio" }); puntos += 15;
    }
    return { puntos, motivos };
  };

  const filas = c.filas.map(f => ({ ...f, ...evaluar(f) }))
    .sort((a, b) => b.puntos - a.puntos);

  const eliminatorias = filas.filter(f => f.destete_ok === false);
  const resto = filas.filter(f => f.destete_ok === true);
  // Preñadas y criando: todavía no se pueden evaluar.
  const en_curso = filas.filter(f => f.destete_ok === null);

  return {
    // Las que salen sin discusión.
    eliminatorias,
    // El resto, de peor a mejor: acá se aprieta el criterio.
    ordenadas: resto,
    // Las que están a mitad del ciclo, para que no desaparezcan del tablero.
    en_curso,
    objetivo,
    faltan: objetivo ? Math.max(0, objetivo - eliminatorias.length) : null,
    resumen: {
      ...c.resumen,
      eliminatorias: eliminatorias.length,
      sugeridas: objetivo ? resto.slice(0, Math.max(0, objetivo - eliminatorias.length)).map(f => f.rp) : []
    }
  };
}

// ── RECRÍA: hasta los 18 meses, cuando se define la categoría ────────────────

function recria(db, opciones = {}) {
  const hoy = opciones.hoy || new Date().toISOString().slice(0, 10);
  const animales = db.prepare(`
    SELECT id, rp, sexo, fecha_nac, categoria, padre_rp, madre_rp
    FROM animales
    WHERE upper(COALESCE(estado,'ACTIVO'))='ACTIVO' AND fecha_nac IS NOT NULL
      AND fecha_nac <= date('now','-6 months') AND fecha_nac >= date('now','-24 months')
    ORDER BY fecha_nac DESC`).all();

  const filas = animales.map(a => {
    const pes = db.prepare("SELECT fecha,peso,contexto,gdp FROM pesadas WHERE animal_id=? ORDER BY fecha").all(a.id);
    const meses = Math.floor(dias(a.fecha_nac, hoy) / 30.44);
    const destete = (pes.find(p => p.contexto === "DESTETE") || {}).peso || null;
    const ult = pes[pes.length - 1] || null;
    const prev = pes.length > 1 ? pes[pes.length - 2] : null;
    let gdp = ult && ult.gdp != null ? r2(ult.gdp) : null;
    if (gdp == null && ult && prev) {
      const d = dias(prev.fecha, ult.fecha);
      if (d > 0) gdp = Math.round(((ult.peso - prev.peso) / d) * 1000) / 1000;
    }

    // Lo que va a los sistemas de evaluación.
    let med = [];
    try { med = db.prepare("SELECT tipo,valor,fecha FROM mediciones WHERE animal_id=?").all(a.id); } catch (e) {}
    const dato = t => (med.find(m => m.tipo === t) || {}).valor ?? null;
    const eco = { aob: dato("AOB"), gd: dato("GD"), gc: dato("GC"), gi: dato("GI") };
    const conEco = Object.values(eco).some(v => v != null);

    const macho = String(a.sexo || "").toUpperCase().startsWith("M");
    const falta = [];
    if (!destete) falta.push("peso al destete");
    if (meses >= 12 && !pes.some(p => p.contexto === "AÑO")) falta.push("peso al año");
    if (meses >= 18 && conEco === false) falta.push("ecografía de carcasa");
    if (dato("CC") == null && meses >= 12) falta.push("mansedumbre");

    // A los 18 meses se define la categoría.
    const define = meses >= 18;

    return {
      rp: a.rp, sexo: a.sexo, macho, fecha_nac: a.fecha_nac, meses,
      padre: a.padre_rp, madre: a.madre_rp,
      destete, peso: ult ? ult.peso : null, ultima: ult ? ult.fecha : null,
      gdp, eco, con_eco: conEco,
      dias_sin_pesar: ult ? dias(ult.fecha, hoy) : null,
      falta, define,
      // Para los machos: el ranking de la camada decide toro o novillo.
      listo_evaluacion: !falta.length
    };
  });

  // Ranking dentro de la camada: es como se eligen los toros.
  const machos = filas.filter(f => f.macho && f.peso).sort((a, b) => b.peso - a.peso);
  machos.forEach((f, i) => { f.ranking_camada = i + 1; f.de = machos.length; });

  const gdps = filas.map(f => f.gdp).filter(Boolean);
  return {
    filas,
    resumen: {
      total: filas.length,
      machos: filas.filter(f => f.macho).length,
      hembras: filas.filter(f => !f.macho).length,
      gdp_prom: prom(gdps),
      peso_prom: prom(filas.map(f => f.peso).filter(Boolean)) ,
      definen: filas.filter(f => f.define).length,
      con_eco: filas.filter(f => f.con_eco).length,
      listos: filas.filter(f => f.listo_evaluacion).length,
      incompletos: filas.filter(f => f.falta.length).length
    }
  };
}

// ── LA VACA MÁS PRODUCTIVA ───────────────────────────────────────────────────
// Para que el chat pueda razonar la respuesta, no sólo listarla.

function masProductiva(db, opciones = {}) {
  const c = cria(db, opciones);
  const cand = c.filas.filter(f => f.productividad != null && f.crias >= 2)
    .sort((a, b) => b.productividad - a.productividad);
  if (!cand.length) return { hay: false, motivo: "Todavía no hay vacas con dos crías y peso de destete cargado." };

  const g = cand[0];
  const razones = [
    `desteta ${g.productividad} kg por cada 100 kg de su propio peso, contra un promedio de ${c.resumen.productividad_prom}`,
    `${g.crias} crías${g.destete_prom ? `, con ${g.destete_prom} kg de destete promedio` : ""}`,
    g.bloque === "CABEZA" ? "pare en cabeza de parición" : `pare en ${g.bloque.toLowerCase()}`,
    g.ipp ? `${g.ipp} días entre partos` : null,
    g.peso_adulto ? `pesa ${g.peso_adulto} kg: come menos que una vaca grande para destetar lo mismo` : null
  ].filter(Boolean);

  return {
    hay: true, vaca: g,
    razones,
    segundas: cand.slice(1, 4).map(f => ({ rp: f.rp, productividad: f.productividad, destete: f.destete })),
    promedio: c.resumen.productividad_prom
  };
}

module.exports = {
  init, cria, descartes, recria, masProductiva,
  guardarNota, leerNota, bloqueDe,
  BLOQUES, NO_DESTETO, SENALES
};
