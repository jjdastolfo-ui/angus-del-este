// ─────────────────────────────────────────────────────────────────────────────
// ANÁLISIS
//
// La diferencia con `decision.js`: allá yo escribo las reglas y el bot las
// ejecuta. Acá le doy los datos crudos y el bot razona.
//
// Por qué importa: cuando en Angus del Este aparecieron 34 vacas preñadas sin
// cría, mi regla las marcó como aborto. Cualquiera que mire los datos ve que el
// servicio fue en diciembre y hoy es agosto — todavía no parieron. Eso no se
// arregla agregando otra regla ni cargando un parámetro de "mes de parición":
// el año que llueva y se atrase todo, vuelve a fallar.
//
// Acá el bot ve el servicio, el tacto, los partos anteriores y la fecha de hoy,
// y saca la conclusión él. Sabe que la gestación son 283 días sin que nadie se
// lo cargue, porque eso es conocimiento de ganadería, no dato del campo.
// ─────────────────────────────────────────────────────────────────────────────

// Todo lo que se sabe de una vaca, sin interpretar. Lo que el bot necesita para
// razonar: los hechos con sus fechas, no un estado ya decidido.
function datosCrudos(db, opciones = {}) {
  const hoy = opciones.hoy || new Date().toISOString().slice(0, 10);
  const limite = opciones.limite || 60;

  const vientres = db.prepare(`
    SELECT id, rp, fecha_nac, categoria, estado
    FROM animales
    WHERE upper(COALESCE(sexo,'')) LIKE 'H%' AND upper(COALESCE(estado,'ACTIVO'))='ACTIVO'
      AND (upper(COALESCE(categoria,'')) LIKE '%VACA%'
        OR EXISTS (SELECT 1 FROM animales h WHERE upper(COALESCE(h.madre_rp,''))=upper(animales.rp)))
    ORDER BY rp LIMIT ?`).all(limite);

  return vientres.map(v => {
    let servicios = [];
    try {
      servicios = db.prepare(`
        SELECT temporada, tipo_servicio, semen_iatf, fecha_iatf, toro_natural,
               fecha_ingreso_toro, fecha_salida_toro, resultado, tacto_servicio, fecha_parto
        FROM servicios WHERE animal_id=? ORDER BY COALESCE(temporada,'') DESC, id DESC LIMIT 4`).all(v.id);
    } catch (e) {}

    const crias = db.prepare(`
      SELECT a.rp, a.fecha_nac, a.sexo, a.estado,
             (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='NACIMIENTO' ORDER BY p.fecha LIMIT 1) peso_nac,
             (SELECT peso FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='DESTETE' ORDER BY p.fecha DESC LIMIT 1) destete,
             (SELECT fecha FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='DESTETE' ORDER BY p.fecha DESC LIMIT 1) fecha_destete
      FROM animales a WHERE upper(COALESCE(a.madre_rp,''))=upper(?)
      ORDER BY a.fecha_nac DESC LIMIT 8`).all(v.rp);

    const pesadas = db.prepare(`
      SELECT fecha, peso, contexto FROM pesadas WHERE animal_id=? ORDER BY fecha DESC LIMIT 6`).all(v.id);

    let notas = [];
    try {
      notas = db.prepare(`SELECT fecha, texto FROM notas_campo WHERE upper(animal_rp)=upper(?)
                          ORDER BY fecha DESC LIMIT 6`).all(v.rp);
    } catch (e) {}

    return { rp: v.rp, fecha_nac: v.fecha_nac, categoria: v.categoria,
             servicios, crias, pesadas, notas };
  });
}

// Lo que el bot ve del campo entero, para poder deducir el calendario solo.
function contexto(db, opciones = {}) {
  const hoy = opciones.hoy || new Date().toISOString().slice(0, 10);
  const q = (sql, ...p) => { try { return db.prepare(sql).all(...p); } catch (e) { return []; } };

  // Cuándo se sirvió cada temporada, sacado de los datos.
  const servicios = q(`
    SELECT temporada,
           MIN(COALESCE(fecha_iatf, fecha_ingreso_toro)) AS desde,
           MAX(COALESCE(fecha_salida_toro, fecha_ingreso_toro, fecha_iatf)) AS hasta,
           COUNT(*) n
    FROM servicios WHERE temporada IS NOT NULL
    GROUP BY temporada ORDER BY temporada DESC LIMIT 4`);

  // Cuándo se pare, sacado de los nacimientos reales.
  const pariciones = q(`
    SELECT substr(fecha_nac,1,4) anio, MIN(fecha_nac) primero, MAX(fecha_nac) ultimo, COUNT(*) n
    FROM animales WHERE fecha_nac IS NOT NULL AND madre_rp IS NOT NULL
    GROUP BY anio ORDER BY anio DESC LIMIT 4`);

  // Cuándo se desteta.
  const destetes = q(`
    SELECT substr(fecha,1,4) anio, MIN(fecha) primero, MAX(fecha) ultimo,
           COUNT(*) n, ROUND(AVG(peso),1) peso_prom
    FROM pesadas WHERE contexto='DESTETE'
    GROUP BY anio ORDER BY anio DESC LIMIT 4`);

  const totales = q(`SELECT
    (SELECT COUNT(*) FROM animales WHERE upper(COALESCE(estado,'ACTIVO'))='ACTIVO') activos,
    (SELECT COUNT(*) FROM animales WHERE upper(COALESCE(sexo,'')) LIKE 'H%' AND upper(COALESCE(estado,'ACTIVO'))='ACTIVO') hembras,
    (SELECT COUNT(*) FROM servicios) servicios,
    (SELECT COUNT(*) FROM pesadas) pesadas`)[0] || {};

  return { hoy, servicios, pariciones, destetes, totales };
}

// ── LO QUE SE LE DICE AL BOT ─────────────────────────────────────────────────
// No se le dan estados calculados ni parámetros de calendario: se le dan los
// hechos y se le pide que razone. El calendario lo deduce de los datos.

function instrucciones(ctx) {
  return `Sos un asesor ganadero mirando los datos de un campo. HOY ES ${ctx.hoy}.

NO recibís estados calculados. Recibís los hechos: servicios con sus fechas, resultados de tacto,
crías con su fecha de nacimiento y su peso, pesadas y notas de campo. La conclusión la sacás vos.

Lo que sabés de ganadería y no hace falta que nadie te cargue:
· La gestación de un bovino son 283 días, unos nueve meses y medio.
· Una vaca desteta un ternero por año. El destete es a los 6-8 meses del parto.
· Un tacto "PREÑADA" significa que estaba preñada EL DÍA DEL TACTO. Va a parir unos nueve
  meses después del servicio, no del tacto.
· "Cabeza, cuerpo, cola" son tramos de la parición: cuanto antes pare, más kilos desteta el ternero.

EL ERROR QUE TENÉS QUE EVITAR: si una vaca figura preñada y no tiene cría registrada, NO
concluyas que abortó sin antes fijarte CUÁNDO fue el servicio. Si el servicio fue hace menos
de nueve meses, esa vaca simplemente todavía no parió. Es la diferencia entre un problema
sanitario grave y una parición que está por empezar.

Deducí el calendario del campo mirando los datos, no lo asumas. Estos son los períodos reales
de este campo, sacados de sus propios registros:

SERVICIOS POR TEMPORADA:
${ctx.servicios.map(s => `  ${s.temporada}: del ${s.desde || "?"} al ${s.hasta || "?"} · ${s.n} vientres`).join("\n") || "  sin datos"}

PARICIONES (nacimientos reales):
${ctx.pariciones.map(p => `  ${p.anio}: del ${p.primero} al ${p.ultimo} · ${p.n} terneros`).join("\n") || "  sin datos"}

DESTETES:
${ctx.destetes.map(d => `  ${d.anio}: del ${d.primero} al ${d.ultimo} · ${d.n} terneros · ${d.peso_prom} kg promedio`).join("\n") || "  sin datos"}

Con eso ya sabés en qué momento del ciclo está el campo hoy.

CÓMO RESPONDER:
· Decí en qué está cada vaca y POR QUÉ lo deducís, con las fechas en la mano.
· Si algo no cuadra, decilo en vez de forzar una conclusión: "hay 34 preñadas sin parto
  registrado, pero el servicio fue en diciembre — todavía no deberían haber parido".
· Si te falta un dato para concluir, pedilo. No inventes.
· Hablá como un asesor, no como un reporte: frases cortas, sin listas de más.`;
}

// El bot razona sobre un grupo de vacas y devuelve su lectura.
async function analizar(db, anthropic, pregunta, opciones = {}) {
  const ctx = contexto(db, opciones);
  const datos = datosCrudos(db, opciones);
  if (!datos.length) return { ok: false, error: "No hay vientres cargados en este campo." };

  const cuerpo = datos.map(v => {
    const s = v.servicios[0];
    const partes = [`${v.rp}${v.fecha_nac ? ` (nac ${v.fecha_nac})` : ""}`];
    if (s) partes.push(`servicio ${s.temporada}: ${s.fecha_iatf || s.fecha_ingreso_toro || "sin fecha"}` +
      `${s.semen_iatf ? ` IATF ${s.semen_iatf}` : ""}${s.toro_natural ? ` repaso ${s.toro_natural}` : ""}` +
      `${s.resultado ? ` → tacto ${s.resultado}` : ""}`);
    else partes.push("sin servicio registrado");
    if (v.crias.length) partes.push("crías: " + v.crias.map(c =>
      `${c.rp} nació ${c.fecha_nac}${c.destete ? `, destetó ${c.destete}kg` : ", sin destete"}`).join(" | "));
    else partes.push("sin crías registradas");
    const ult = v.pesadas[0];
    if (ult) partes.push(`última pesada: ${ult.peso}kg el ${ult.fecha} (${ult.contexto || "s/d"})`);
    if (v.notas.length) partes.push("notas: " + v.notas.map(n => `${n.fecha} "${n.texto}"`).join(" | "));
    return partes.join("\n  ");
  }).join("\n\n");

  try {
    const r = await anthropic.messages.create({
      model: "claude-sonnet-4-6",
      max_tokens: 1600,
      system: instrucciones(ctx),
      messages: [{ role: "user", content:
        `${pregunta}\n\nLOS VIENTRES (${datos.length}):\n\n${cuerpo}` }]
    });
    const texto = (r.content || []).filter(c => c.type === "text").map(c => c.text).join("\n");
    return { ok: true, respuesta: texto, analizados: datos.length, contexto: ctx };
  } catch (e) {
    return { ok: false, error: String(e.message).slice(0, 200) };
  }
}

module.exports = { datosCrudos, contexto, instrucciones, analizar };
