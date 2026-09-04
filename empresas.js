// ─────────────────────────────────────────────────────────────────────────────
// EMPRESAS — una empresa tiene uno o más campos y un sistema financiero.
//
// Cada campo sigue viviendo en su propia base (/data/<campo>.db). Acá está lo
// que cruza campos:
//   · la configuración: qué campos tiene cada empresa y cuál es su financiero
//   · el resumen de la empresa (cabezas, vientres, parición, toros por campo)
//   · el stock consolidado por categoría, para valuar el patrimonio entero
//   · los traslados: un animal que pasa de un campo a otro con todo su historial
//
// Variables:
//   CAMPOS    {"principal":{"nombre":"Angus del Este","empresa":"improlux"}, "triunfo":{...}}
//   EMPRESAS  {"improlux":{"nombre":"Improlux","finanzas_url":"https://...","finanzas_campo":"AMAKAIK","finanzas_clave":""}}
// Si EMPRESAS no está, se arma sola con las empresas que nombran los campos, y
// el financiero sale de FINANZAS_URL / FINANZAS_CAMPO / FINANZAS_CLAVE (globales).
// ─────────────────────────────────────────────────────────────────────────────

function configurar(CAMPOS) {
  let EMPRESAS = {};
  try { EMPRESAS = JSON.parse(String(process.env.EMPRESAS || "{}").trim().replace(/[\u201C\u201D\u201E\u2033]/g, "\"").replace(/[\u2018\u2019]/g, "\"")); } catch (e) { console.log("EMPRESAS no es un JSON válido:", e.message); }
  const claves = new Set(Object.keys(EMPRESAS));
  for (const c of Object.values(CAMPOS)) if (c.empresa) claves.add(c.empresa);
  if (!claves.size) claves.add("empresa");
  const empresas = {};
  for (const k of claves) {
    const e = EMPRESAS[k] || {};
    empresas[k] = {
      key: k,
      nombre: e.nombre || k.charAt(0).toUpperCase() + k.slice(1),
      razon_social: e.razon_social || null,
      finanzas: {
        nombre: e.finanzas_nombre || null,
        url: String(e.finanzas_url || (Object.keys(EMPRESAS).length ? "" : process.env.FINANZAS_URL) || "").replace(/\/$/, ""),
        campo: e.finanzas_campo || (Object.keys(EMPRESAS).length ? null : process.env.FINANZAS_CAMPO) || null,
        clave: e.finanzas_clave || (Object.keys(EMPRESAS).length ? null : process.env.FINANZAS_CLAVE) || null
      },
      campos: Object.entries(CAMPOS).filter(([, c]) => (c.empresa || [...claves][0]) === k).map(([key, c]) => ({ key, nombre: c.nombre }))
    };
  }
  // Un campo sin empresa va a la primera.
  const primera = Object.keys(empresas)[0];
  for (const [key, c] of Object.entries(CAMPOS)) if (!c.empresa) { c.empresa = primera; if (!empresas[primera].campos.some(x => x.key === key)) empresas[primera].campos.push({ key, nombre: c.nombre }); }
  return empresas;
}

function crear({ CAMPOS, getDB, plantelMod, animalesMod, destinosMod, finanzasMod, criasFuera, hijosFuera }) {
  const empresas = configurar(CAMPOS);
  // Cuando el servidor las provee, los totales cuentan también los hijos que
  // cada animal tiene en los otros campos.
  const cruceCrias = k => { try { return (api.criasFuera || criasFuera) ? (api.criasFuera || criasFuera)(k) : undefined; } catch (e) { return undefined; } };
  const cruceHijos = k => { try { return (api.hijosFuera || hijosFuera) ? (api.hijosFuera || hijosFuera)(k) : undefined; } catch (e) { return undefined; } };
  const empresaDe = campoKey => empresas[(CAMPOS[campoKey] || {}).empresa] || empresas[Object.keys(empresas)[0]];
  const camposDe = empresaKey => (empresas[empresaKey] || { campos: [] }).campos.map(c => c.key);
  const finanzasDe = campoKey => empresaDe(campoKey).finanzas;

  /** Lo que se ve de cada campo, y los totales de la empresa. */
  function resumen(empresaKey) {
    const e = empresas[empresaKey];
    if (!e) throw new Error(`No existe la empresa "${empresaKey}". Hay: ${Object.keys(empresas).join(", ")}`);
    const campos = e.campos.map(c => {
      try {
        const db = getDB(c.key);
        const pl = plantelMod.plantel(db, { criasFuera: cruceCrias(c.key) });
        const R = pl.resumen;
        const toros = animalesMod.toros(db, { hijosFuera: cruceHijos(c.key) }).resumen;
        const term = animalesMod.terminacion(db).resumen;
        const activos = db.prepare("SELECT COUNT(*) n FROM animales WHERE upper(COALESCE(estado,'ACTIVO'))='ACTIVO'").get().n;
        let marcados = 0; try { marcados = destinosMod.destinadosASalir(db).size; } catch (x) {}
        return { key: c.key, nombre: c.nombre, ok: true, cabezas: activos, vientres: pl.filas.length, prenadas: R.prenadas, criando: R.criando,
          destetaron: R.destetaron, fallaron: R.fallaron, destete_efectivo: R.destete_efectivo, eficiencia: R.eficiencia_prom,
          toros: toros.total, terminando: term.total, kg_corral: term.kg_totales, marcados_salida: marcados, anio_paricion: pl.anio_paricion };
      } catch (err) { return { key: c.key, nombre: c.nombre, ok: false, error: err.message }; }
    });
    const suma = k => campos.filter(c => c.ok).reduce((s, c) => s + (Number(c[k]) || 0), 0);
    return { empresa: e.key, nombre: e.nombre, razon_social: e.razon_social, campos, finanzas: { nombre: e.finanzas.nombre, configurado: !!e.finanzas.url, url: e.finanzas.url || null, campo: e.finanzas.campo },
      totales: { campos: campos.length, cabezas: suma("cabezas"), vientres: suma("vientres"), prenadas: suma("prenadas"), criando: suma("criando"),
        fallaron: suma("fallaron"), toros: suma("toros"), terminando: suma("terminando"), marcados_salida: suma("marcados_salida") } };
  }

  /** El stock por categoría de todos los campos, sumado. Formato del financiero. */
  function rodeoResumen(empresaKey) {
    const e = empresas[empresaKey];
    if (!e) throw new Error(`No existe la empresa "${empresaKey}"`);
    const acum = new Map(), porCampo = [];
    for (const c of e.campos) {
      let r;
      try { r = finanzasMod.resumenRodeo(getDB(c.key), { campoKey: c.key, campoNombre: c.nombre, destinosMod }); } catch (err) { porCampo.push({ campo: c.key, error: err.message }); continue; }
      porCampo.push({ campo: c.key, nombre: c.nombre, cabezas: r.totales.cabezas, plantel: r.totales.plantel, venta: r.totales.venta, kg_total: r.totales.kg_total });
      for (const cat of r.categorias) {
        const k = `${cat.categoria}|${cat.registro}`;
        if (!acum.has(k)) acum.set(k, { categoria: cat.categoria, registro: cat.registro, plantel: 0, venta: 0, cantidad: 0, kg_suma: 0, con_peso: 0, sin_peso: 0 });
        const a = acum.get(k);
        a.plantel += cat.plantel; a.venta += cat.venta; a.cantidad += cat.cantidad; a.sin_peso += cat.sin_peso;
        if (cat.kg_estimado) { a.kg_suma += cat.kg_estimado * cat.con_peso; a.con_peso += cat.con_peso; }
      }
    }
    const categorias = [...acum.values()].map(a => { const kg = a.con_peso ? Math.round(a.kg_suma / a.con_peso) : null; return { categoria: a.categoria, registro: a.registro, plantel: a.plantel, venta: a.venta, cantidad: a.cantidad, kg_estimado: kg, kg_prom: kg, con_peso: a.con_peso, sin_peso: a.sin_peso, kg_total: kg ? kg * a.cantidad : null }; });
    return { empresa: e.key, nombre: e.nombre, generado: new Date().toISOString(), fuente: "rodeo", campos: porCampo, categorias, rodeo: categorias,
      totales: { cabezas: categorias.reduce((s, c) => s + c.cantidad, 0), plantel: categorias.reduce((s, c) => s + c.plantel, 0), venta: categorias.reduce((s, c) => s + c.venta, 0), kg_total: categorias.reduce((s, c) => s + (c.kg_total || 0), 0) } };
  }

  // ── TRASLADOS ──────────────────────────────────────────────────────────────
  // El animal se copia entero al campo de destino (datos, pesadas, servicios,
  // sanidad, mediciones, notas) y en el origen queda TRASLADADO, con una nota.
  const TABLAS_HIJAS = ["pesadas", "servicios", "sanidad", "mediciones"];

  function trasladar({ rps, desde, hasta, fecha, motivo, simular, usuario }) {
    if (!CAMPOS[desde]) throw new Error(`No existe el campo de origen "${desde}"`);
    if (!CAMPOS[hasta]) throw new Error(`No existe el campo de destino "${hasta}"`);
    if (desde === hasta) throw new Error("El origen y el destino son el mismo campo");
    if ((CAMPOS[desde].empresa) !== (CAMPOS[hasta].empresa)) throw new Error("Los dos campos tienen que ser de la misma empresa");
    const dbA = getDB(desde), dbB = getDB(hasta);
    const fe = fecha || new Date().toISOString().slice(0, 10);
    const out = [];
    const colsDe = (db, t) => db.prepare(`PRAGMA table_info(${t})`).all().map(c => c.name);
    const colsAnimalB = colsDe(dbB, "animales").filter(c => c !== "id" && c !== "created_at");

    const hacer = () => {
      for (const rp of (rps || [])) {
        const r = { rp: String(rp).trim(), ok: false, avisos: [] };
        out.push(r);
        const a = animalesMod.porRp(dbA, rp);
        if (!a) { r.error = `No existe ${rp} en ${CAMPOS[desde].nombre}`; continue; }
        r.rp = a.rp; r.categoria = a.categoria;
        if (String(a.estado || "ACTIVO").toUpperCase() !== "ACTIVO") { r.error = `${a.rp} figura ${a.estado}`; continue; }
        if (dbB.prepare("SELECT 1 FROM animales WHERE upper(rp)=upper(?)").get(a.rp)) { r.error = `En ${CAMPOS[hasta].nombre} ya hay un animal con RP ${a.rp}`; continue; }
        const cria = dbA.prepare("SELECT rp FROM animales WHERE upper(madre_rp)=upper(?) AND fecha_nac >= date('now','-240 days') AND upper(COALESCE(estado,'ACTIVO'))='ACTIVO'").all(a.rp);
        if (cria.length && !(rps || []).some(x => cria.some(c => String(c.rp).toUpperCase() === String(x).toUpperCase()))) r.avisos.push(`tiene ternero al pie (${cria.map(c => c.rp).join(", ")}) que no viaja`);
        r.ok = true;
        if (simular) continue;
        // Copiar el animal con las columnas que existan en las dos bases.
        const datos = { ...a }; delete datos.id; delete datos.created_at;
        const cols = colsAnimalB.filter(c => c in datos);
        const idB = dbB.prepare(`INSERT INTO animales (${cols.join(",")}) VALUES (${cols.map(() => "?").join(",")})`).run(...cols.map(c => datos[c])).lastInsertRowid;
        for (const t of TABLAS_HIJAS) {
          let filas = []; try { filas = dbA.prepare(`SELECT * FROM ${t} WHERE animal_id=?`).all(a.id); } catch (e) { continue; }
          if (!filas.length) continue;
          const cb = colsDe(dbB, t).filter(c => c !== "id" && c !== "animal_id" && c !== "created_at" && c in filas[0]);
          const ins = dbB.prepare(`INSERT INTO ${t} (animal_id,${cb.join(",")}) VALUES (?,${cb.map(() => "?").join(",")})`);
          for (const f of filas) ins.run(idB, ...cb.map(c => f[c]));
        }
        try { for (const n of dbA.prepare("SELECT * FROM notas_campo WHERE upper(animal_rp)=upper(?)").all(a.rp))
          dbB.prepare("INSERT INTO notas_campo (animal_rp, fecha, texto, causa, grave, usuario) VALUES (?,?,?,?,?,?)").run(a.rp, n.fecha, n.texto, n.causa, n.grave, n.usuario); } catch (e) {}
        try { dbB.prepare("INSERT INTO notas_campo (animal_rp, fecha, texto, usuario) VALUES (?,?,?,?)").run(a.rp, fe, `Llegó de ${CAMPOS[desde].nombre}${motivo ? " · " + motivo : ""}`, usuario || "traslado"); } catch (e) {}
        // En el origen queda constancia.
        dbA.prepare("UPDATE animales SET estado='TRASLADADO' WHERE id=?").run(a.id);
        try { dbA.prepare("INSERT INTO notas_campo (animal_rp, fecha, texto, usuario) VALUES (?,?,?,?)").run(a.rp, fe, `Trasladado a ${CAMPOS[hasta].nombre}${motivo ? " · " + motivo : ""}`, usuario || "traslado"); } catch (e) {}
        try { dbA.prepare("DELETE FROM lote_animales WHERE animal_id=?").run(a.id); } catch (e) {}
        r.hecho = true;
      }
    };
    if (simular) hacer(); else dbB.transaction(() => dbA.transaction(hacer)())();
    const bien = out.filter(r => r.ok).length, mal = out.filter(r => !r.ok).length;
    return { ok: bien > 0, simulado: !!simular, total: out.length, bien, mal, desde, hasta, filas: out,
      mensaje: simular ? `${bien} animal${bien === 1 ? "" : "es"} para trasladar de ${CAMPOS[desde].nombre} a ${CAMPOS[hasta].nombre}${mal ? `, ${mal} con error` : ""}. Revisá y confirmá.`
        : `${bien} animal${bien === 1 ? "" : "es"} trasladado${bien === 1 ? "" : "s"} de ${CAMPOS[desde].nombre} a ${CAMPOS[hasta].nombre}${mal ? `. ${mal} no: ${out.filter(r => !r.ok).slice(0, 4).map(r => `${r.rp} (${r.error})`).join(", ")}` : ""}.` };
  }

  const lista = () => Object.values(empresas).map(e => ({ key: e.key, nombre: e.nombre, razon_social: e.razon_social, campos: e.campos, finanzas: !!e.finanzas.url, finanzas_nombre: e.finanzas.nombre }));

  const api = { empresas, empresaDe, camposDe, finanzasDe, resumen, rodeoResumen, trasladar, lista };
  return api;
}

module.exports = { configurar, crear };
