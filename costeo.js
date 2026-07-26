/* ════════════════════════════════════════════════════════════════════════════
   COSTEO kgNE — Módulo para ADE (Angus del Este)
   v1.0.0

   INTEGRACIÓN en server.js — DOS líneas, justo antes de app.listen:

       const costeo = require("./costeo");
       costeo.init(databases, app, { CAMPO_DEFAULT });

   Y en el health check agregar:   costeo: costeo.VERSION

   ─────────────────────────────────────────────────────────────────────────────
   No duplica nada. Usa las tablas que ya existen:
     · animales      → identidad (id, chip, rp, sexo, fecha_nac)
     · costos        → la plata, por animal_id. NO se crea tabla nueva de costos.
     · lote_animales → origen del backfill de permanencia
   Crea solo lo que falta, todo con prefijo costeo_.

   Una DB por campo: las tablas se crean en todas y las rutas resuelven la base
   con req.campoDB / req.campoKey del middleware que ya existe.
   ════════════════════════════════════════════════════════════════════════════ */

const VERSION = "1.0.0";

const ETAPAS = {
  AL_PIE:      { label: "Al pie",            sexo: "AMBOS" },
  RECRIA:      { label: "Recría",            sexo: "AMBOS" },
  INTERMEDIA:  { label: "Intermedia",        sexo: "H" },
  PLANTEL:     { label: "Plantel",           sexo: "AMBOS" },
  ENGORDE:     { label: "Engorde",           sexo: "H" },
  SERV_INV:    { label: "Servicio invierno", sexo: "H" },
  PREPARACION: { label: "Preparación",       sexo: "M" },
  TERMINACION: { label: "Terminación",       sexo: "AMBOS" },
  FIN:         { label: "Cerrada",           sexo: "AMBOS" }
};

const HITOS_DEFAULT = [
  ["PRENADA_18M", 400, "Vaquillona preñada de 15/18 meses"],
  ["PRENADA_30M", 500, "Segunda preñez confirmada — alta a plantel"],
  ["REFUGO_VACA", 350, "Residual de vaca, base de amortización"],
  ["REFUGO_TORO", 680, "Residual de toro plantel"],
  ["VIDA_VACA",    10, "Pariciones de vida útil (total)"],
  ["VIDA_TORO",     5, "Años de servicio del toro"]
];

/* productivo=1 entra al costo directo; 0 es estructura (solo costo pleno) */
const CONCEPTOS_DEFAULT = [
  ["ALQUILER", "DIA_ANIMAL", 1], ["ALQUILER ESTRUCTURA", "DIA_ANIMAL", 1],
  ["ALIMENTACION RECRIA", "DIA_ANIMAL", 1], ["ALIMENTACION CRIA", "DIA_ANIMAL", 1],
  ["TERMINACION", "DIA_ANIMAL", 1], ["RACION", "DIA_ANIMAL", 1],
  ["INSUMOS VETERINARIOS", "DIRECTO", 1], ["SANIDAD", "DIRECTO", 1],
  ["TRABAJOS VETERINARIOS", "CABEZA", 1], ["IATF", "DIRECTO", 1],
  ["SERVICIO", "DIRECTO", 1], ["VERDEOS Y PASTURAS", "DIA_ANIMAL", 1],
  ["SUELDO JORNAL", "DIA_ANIMAL", 1], ["SUELDO ENCARGADO", "DIA_ANIMAL", 1],
  ["COMBUSTIBLE CAMPO", "DIA_ANIMAL", 1], ["MANTENIMIENTO CAMPO", "DIA_ANIMAL", 1],
  ["ESTRUCTURA GANADERA", "DIA_ANIMAL", 1], ["GASTOS VENTAS GANADERAS", "CABEZA", 1],
  ["BREEDPLAN", "CABEZA", 1], ["ARU", "CABEZA", 1],
  ["INTERESES", "DIA_ANIMAL", 0], ["GASTOS ADM", "DIA_ANIMAL", 0],
  ["BPS", "DIA_ANIMAL", 0], ["TELEFONO", "DIA_ANIMAL", 0],
  ["CONTADOR", "DIA_ANIMAL", 0], ["GASTO BANCARIO", "DIA_ANIMAL", 0]
];

const MARCA = "costeo:prorrateo";

/* ── Helpers ──────────────────────────────────────────────────────────────── */
const hoy = () => new Date().toISOString().slice(0, 10);

/* Ciclo ganadero marzo–marzo. 2026-05-10 → "26/27" */
function cicloDe(fecha) {
  const f = String(fecha || hoy()).slice(0, 10);
  const y = parseInt(f.slice(0, 4), 10), m = parseInt(f.slice(5, 7), 10);
  const a = m >= 3 ? y : y - 1;
  return String(a).slice(2) + "/" + String(a + 1).slice(2);
}
const rangoCiclo = c => {
  const a = 2000 + parseInt(String(c).slice(0, 2), 10);
  return [a + "-03-01", (a + 1) + "-02-29"];
};
const dias  = (a, b) => Math.max(0, (new Date(b) - new Date(a)) / 86400000);
const anios = (a, b) => Math.max(0, (new Date(b) - new Date(a)) / (86400000 * 365.25));
const esM   = s => String(s || "").toUpperCase().startsWith("M");

/* ════════════════════════════════════════════════════════════════════════════
   ESQUEMA — solo lo que no existe
   ════════════════════════════════════════════════════════════════════════════ */
function crearTablas(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS costeo_precios (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      semana TEXT, fecha_desde TEXT, fecha_hasta TEXT,
      indice_ternero REAL, ternero_pie REAL, ternera_pie REAL,
      vaca_invernada_pie REAL, novillo_pie REAL,
      novillo_4a REAL, vaca_4a REAL, vaquillona_4a REAL,
      fuente TEXT DEFAULT 'ACG',
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS costeo_hitos (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ciclo TEXT DEFAULT 'TODOS',
      hito TEXT NOT NULL, kgne REAL NOT NULL, nota TEXT,
      UNIQUE(ciclo, hito)
    );

    CREATE TABLE IF NOT EXISTS costeo_conceptos (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      concepto TEXT NOT NULL UNIQUE,
      metodo TEXT DEFAULT 'DIA_ANIMAL',
      productivo INTEGER DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS costeo_kgne (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      animal_id INTEGER NOT NULL, chip TEXT, rp TEXT,
      fecha TEXT NOT NULL, ciclo TEXT, generacion TEXT,
      etapa TEXT, tipo TEXT, origen TEXT, concepto TEXT, detalle TEXT,
      kg_fisicos REAL, coeficiente REAL, novillo_pie REAL,
      kgne_saldo REAL, kgne_produccion REAL,
      transferencia INTEGER DEFAULT 0, estimado INTEGER DEFAULT 0,
      ref_id INTEGER, created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS costeo_etapas (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      animal_id INTEGER NOT NULL, rp TEXT, etapa TEXT NOT NULL,
      fecha_ini TEXT, fecha_fin TEXT,
      kgne_entrada REAL, kgne_salida REAL, kgne_produccion REAL,
      motivo_salida TEXT, rama TEXT, ciclo TEXT, generacion TEXT
    );

    CREATE TABLE IF NOT EXISTS costeo_eventos (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      concepto TEXT NOT NULL,
      fecha_desde TEXT NOT NULL, fecha_hasta TEXT NOT NULL,
      monto_usd REAL NOT NULL, lote_id INTEGER, categoria TEXT,
      metodo TEXT DEFAULT 'DIA_ANIMAL', origen TEXT DEFAULT 'MANUAL',
      ref_improlux TEXT, estado TEXT DEFAULT 'PENDIENTE', ciclo TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS costeo_permanencia (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      animal_id INTEGER NOT NULL, rp TEXT,
      lote_id INTEGER, categoria TEXT,
      fecha_desde TEXT NOT NULL, fecha_hasta TEXT
    );

    CREATE INDEX IF NOT EXISTS ix_ckgne_animal ON costeo_kgne(animal_id, fecha);
    CREATE INDEX IF NOT EXISTS ix_ckgne_ciclo  ON costeo_kgne(ciclo);
    CREATE INDEX IF NOT EXISTS ix_cperm_animal ON costeo_permanencia(animal_id);
    CREATE INDEX IF NOT EXISTS ix_cetap_animal ON costeo_etapas(animal_id);
  `);

  const h = db.prepare("INSERT OR IGNORE INTO costeo_hitos (ciclo,hito,kgne,nota) VALUES ('TODOS',?,?,?)");
  HITOS_DEFAULT.forEach(x => h.run(x[0], x[1], x[2]));
  const c = db.prepare("INSERT OR IGNORE INTO costeo_conceptos (concepto,metodo,productivo) VALUES (?,?,?)");
  CONCEPTOS_DEFAULT.forEach(x => c.run(x[0], x[1], x[2]));
}

/* ════════════════════════════════════════════════════════════════════════════
   MOTOR — una instancia por campo
   ════════════════════════════════════════════════════════════════════════════ */
function motor(db) {

  function precios() {
    const p = db.prepare("SELECT * FROM costeo_precios ORDER BY id DESC LIMIT 1").get();
    if (!p) return null;
    const nov = p.novillo_pie || (p.ternero_pie / p.indice_ternero);
    return { ...p, novillo_pie: nov, coef: {
      TERNERO: p.ternero_pie / nov,
      TERNERA: p.ternera_pie / nov,
      NOVILLO: 1,
      VACA_INVERNADA: p.vaca_invernada_pie / nov
    }};
  }

  function hito(nombre, ciclo) {
    const r = db.prepare(`SELECT kgne FROM costeo_hitos
      WHERE hito = ? AND (ciclo = ? OR ciclo = 'TODOS')
      ORDER BY (ciclo != 'TODOS') DESC LIMIT 1`).get(nombre, ciclo || "TODOS");
    return r ? r.kgne : null;
  }

  /* Acepta animal_id, chip o RP */
  function animal(ref) {
    if (ref === undefined || ref === null || ref === "") return null;
    const s = String(ref).trim();
    let a = db.prepare("SELECT * FROM animales WHERE chip = ?").get(s);
    if (a) return a;
    a = db.prepare("SELECT * FROM animales WHERE rp = ?").get(s);
    if (a) return a;
    if (/^\d+$/.test(s)) {
      a = db.prepare("SELECT * FROM animales WHERE id = ?").get(parseInt(s, 10));
      if (a) return a;
    }
    if (s.length > 9) {
      a = db.prepare("SELECT * FROM animales WHERE chip LIKE ?").get("%" + s.slice(-9));
      if (a) return a;
    }
    return null;
  }

  const resolver = ref => {
    const a = animal(ref);
    if (!a) throw new Error("Animal no encontrado: " + ref);
    return a;
  };

  const saldo = aid => {
    const r = db.prepare("SELECT kgne_saldo FROM costeo_kgne WHERE animal_id = ? ORDER BY fecha DESC, id DESC LIMIT 1").get(aid);
    return r ? r.kgne_saldo : 0;
  };

  const etapaActual = aid => {
    const r = db.prepare("SELECT etapa FROM costeo_kgne WHERE animal_id = ? ORDER BY fecha DESC, id DESC LIMIT 1").get(aid);
    return r ? r.etapa : "AL_PIE";
  };

  /* Núcleo: producción = delta del saldo. Excepción: PROD (destete de un hijo)
     es producción pura y no mueve el saldo de la madre. */
  function asiento(a, o) {
    const p = precios();
    const fecha = o.fecha || hoy();
    const prev = saldo(a.id);
    let nuevo, prod;

    if (o.tipo === "PROD")       { nuevo = prev;                    prod = o.kgne; }
    else if (o.tipo === "TRANS") { nuevo = o.kgne;                  prod = 0; }
    else if (o.tipo === "AMORT") { nuevo = prev - Math.abs(o.kgne); prod = -Math.abs(o.kgne); }
    else if (o.tipo === "BAJA")  { nuevo = 0;                       prod = o.kgne - prev; }
    else                         { nuevo = o.kgne;                  prod = o.kgne - prev; }

    const i = db.prepare(`INSERT INTO costeo_kgne (animal_id,chip,rp,fecha,ciclo,generacion,
      etapa,tipo,origen,concepto,detalle,kg_fisicos,coeficiente,novillo_pie,
      kgne_saldo,kgne_produccion,transferencia,estimado,ref_id)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(
      a.id, a.chip || null, a.rp || null, fecha, o.ciclo || cicloDe(fecha),
      a.fecha_nac ? String(a.fecha_nac).slice(0, 4) : null,
      o.etapa, o.tipo, o.origen || null, o.concepto || null, o.detalle || null,
      o.kg_fisicos || null, o.coeficiente || null, p ? p.novillo_pie : null,
      nuevo, prod, o.transferencia ? 1 : 0, o.estimado ? 1 : 0, o.ref_id || null);

    return { id: Number(i.lastInsertRowid), saldo: nuevo, produccion: prod };
  }

  function cerrarEtapa(a, etapa, fechaFin, motivo, rama) {
    const f = db.prepare("SELECT * FROM costeo_kgne WHERE animal_id = ? AND etapa = ? ORDER BY fecha, id").all(a.id, etapa);
    if (!f.length) return null;
    const prod = f.reduce((s, x) => s + (x.kgne_produccion || 0), 0);
    db.prepare(`INSERT INTO costeo_etapas (animal_id,rp,etapa,fecha_ini,fecha_fin,
      kgne_entrada,kgne_salida,kgne_produccion,motivo_salida,rama,ciclo,generacion)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`).run(
      a.id, a.rp, etapa, f[0].fecha, fechaFin,
      f[0].kgne_saldo - (f[0].kgne_produccion || 0), f[f.length - 1].kgne_saldo,
      prod, motivo || null, rama || null, cicloDe(fechaFin),
      a.fecha_nac ? String(a.fecha_nac).slice(0, 4) : null);
    return prod;
  }

  const abrir = (a, etapa, fecha, kgne) => asiento(a, {
    fecha, etapa, tipo: "TRANS", origen: "APERTURA",
    concepto: "Apertura " + ETAPAS[etapa].label, detalle: "", kgne, transferencia: 1 });

  /* ── Eventos de vida ────────────────────────────────────────────────────── */

  function destetar(ref, fecha, kg, sexo) {
    const a = resolver(ref), p = precios();
    if (!p) throw new Error("No hay precios cargados. Corré POST /api/costeo/precios/acg");
    const coef = esM(sexo || a.sexo) ? p.coef.TERNERO : p.coef.TERNERA;
    const kgne = kg * coef;
    const r = asiento(a, { fecha, etapa: "AL_PIE", tipo: "TRANS", origen: "DESTETE",
      concepto: "Destete · entrada", detalle: kg + " kg × " + coef.toFixed(3),
      kg_fisicos: kg, coeficiente: coef, kgne, transferencia: 1 });
    cerrarEtapa(a, "AL_PIE", fecha, "DESTETE");
    abrir(a, "RECRIA", fecha, kgne);
    return { ...r, animal_id: a.id, rp: a.rp, kgne };
  }

  /* Producción de la MADRE. No mueve su saldo. */
  function desteteHijo(ref, fecha, kg, sexo, refHijo) {
    const a = resolver(ref), p = precios();
    const coef = esM(sexo) ? p.coef.TERNERO : p.coef.TERNERA;
    const hijo = refHijo ? animal(refHijo) : null;
    return asiento(a, { fecha, etapa: etapaActual(a.id), tipo: "PROD", origen: "DESTETE_HIJO",
      concepto: "Desteta " + (esM(sexo) ? "ternero" : "ternera"),
      detalle: kg + " kg × " + coef.toFixed(3) + (hijo ? " · RP " + hijo.rp : ""),
      kg_fisicos: kg, coeficiente: coef, kgne: kg * coef, ref_id: hijo ? hijo.id : null });
  }

  function tacto(ref, fecha, edad, resultado, rama) {
    const a = resolver(ref), ciclo = cicloDe(fecha), prev = etapaActual(a.id);
    const prenada = String(resultado || "").toUpperCase().includes("PRE");

    if (prenada) {
      const destino = Number(edad) === 18 ? "INTERMEDIA" : "PLANTEL";
      const kgne = hito(Number(edad) === 18 ? "PRENADA_18M" : "PRENADA_30M", ciclo);
      if (kgne == null) throw new Error("Falta el hito de valuación");
      const r = asiento(a, { fecha, etapa: prev, tipo: "REVAL", origen: "TACTO_" + edad,
        concepto: "Preñada " + edad + "m · pasa a " + ETAPAS[destino].label.toLowerCase(),
        detalle: "hito " + kgne + " kgNE", kgne });
      cerrarEtapa(a, prev, fecha, "PRENADA");
      abrir(a, destino, fecha, kgne);
      return { ...r, etapa_nueva: destino };
    }

    const destino = rama === "ENGORDE" ? "ENGORDE" : "SERV_INV";
    const s = saldo(a.id);
    const r = asiento(a, { fecha, etapa: prev, tipo: "REVAL", origen: "TACTO_" + edad,
      concepto: "Vacía " + edad + "m · " + ETAPAS[destino].label.toLowerCase(),
      detalle: "sin revalúo", kgne: s });
    cerrarEtapa(a, prev, fecha, "VACIA", destino);
    abrir(a, destino, fecha, s);
    return { ...r, etapa_nueva: destino };
  }

  /* VENTA | PLANTEL | DESCARTE */
  function evaluar18(ref, fecha, kg, destino, refVenta, metodo) {
    const a = resolver(ref), p = precios(), prev = etapaActual(a.id);
    let kgne, detalle, nueva;

    if (destino === "PLANTEL") {
      const usarRef = (metodo || "REF") === "REF" && refVenta > 0;
      kgne = usarRef ? refVenta / p.novillo_pie : kg;
      detalle = usarRef ? "ref. hermanos US$ " + refVenta : kg + " kg físicos";
      nueva = "PLANTEL";
    } else {
      kgne = kg; detalle = kg + " kg × 1,000";
      nueva = destino === "VENTA" ? "PREPARACION" : "TERMINACION";
    }

    const r = asiento(a, { fecha, etapa: prev, tipo: "REVAL", origen: "EVAL_18M",
      concepto: "Evaluación 18m · " + String(destino).toLowerCase(),
      detalle, kg_fisicos: kg, coeficiente: destino === "PLANTEL" ? null : 1, kgne });
    cerrarEtapa(a, prev, fecha, destino);
    abrir(a, nueva, fecha, kgne);
    return { ...r, etapa_nueva: nueva };
  }

  function amortizar(ref, hasta) {
    const a = resolver(ref);
    const alta = db.prepare(`SELECT * FROM costeo_kgne
      WHERE animal_id = ? AND etapa = 'PLANTEL' AND origen = 'APERTURA'
      ORDER BY fecha, id LIMIT 1`).get(a.id);
    if (!alta) return { anual: 0, nuevo: 0, nota: "No está en plantel" };

    const ya = db.prepare("SELECT COALESCE(SUM(ABS(kgne_produccion)),0) t FROM costeo_kgne WHERE animal_id = ? AND tipo = 'AMORT'").get(a.id).t;
    const macho = esM(a.sexo), ciclo = cicloDe(hasta);
    const residual = hito(macho ? "REFUGO_TORO" : "REFUGO_VACA", ciclo);
    const vidaTot  = hito(macho ? "VIDA_TORO"  : "VIDA_VACA",   ciclo);
    /* La vaca ya parió una vez en la intermedia: le quedan vidaTot − 1 en plantel */
    const vida = macho ? vidaTot : Math.max(1, vidaTot - 1);
    const anual = (alta.kgne_saldo - residual) / vida;
    const pend = anual * anios(alta.fecha, hasta) - ya;
    if (pend <= 0.5) return { anual, acumulada: ya, nuevo: 0 };

    asiento(a, { fecha: hasta, etapa: "PLANTEL", tipo: "AMORT", origen: "AMORTIZACION",
      concepto: "Amortización",
      detalle: pend.toFixed(0) + " kgNE · " + anual.toFixed(1) + "/año", kgne: pend });
    return { anual, acumulada: ya + pend, nuevo: pend };
  }

  function baja(ref, fecha, tipo, kg, precioUsd) {
    const a = resolver(ref), p = precios();
    if (etapaActual(a.id) === "PLANTEL") amortizar(a.id, fecha);
    const etapa = etapaActual(a.id);
    const kgne = tipo === "MUERTE" ? 0 : precioUsd / p.novillo_pie;
    const nom = { REMATE: "Remate", REFUGO: "Refugo · venta", VENTA: "Venta",
                  VENTA_GORDA: "Venta gordo", MUERTE: "Muerte" };
    const r = asiento(a, { fecha, etapa, tipo: "BAJA", origen: tipo,
      concepto: nom[tipo] || "Baja",
      detalle: tipo === "MUERTE" ? "pérdida del saldo" : (kg ? kg + " kg · " : "") + "US$ " + precioUsd,
      kg_fisicos: kg || null, kgne });
    cerrarEtapa(a, etapa, fecha, tipo);
    asiento(a, { fecha, etapa: "FIN", tipo: "TRANS", origen: "CIERRE",
      concepto: "Ficha cerrada", detalle: "", kgne: 0, transferencia: 1 });
    return r;
  }

  /* ── Costos: escriben en la tabla `costos` que ya existe ────────────────── */

  function esProductivo(concepto) {
    const c = String(concepto || "").toUpperCase();
    const r = db.prepare("SELECT productivo FROM costeo_conceptos WHERE concepto = ?").get(c);
    if (r) return r.productivo;
    const l = db.prepare("SELECT productivo FROM costeo_conceptos WHERE ? LIKE '%' || concepto || '%' LIMIT 1").get(c);
    return l ? l.productivo : 1;
  }

  function cargarCosto(ref, o) {
    const a = resolver(ref);
    db.prepare("INSERT INTO costos (animal_id,fecha,concepto,detalle,monto,moneda,notas) VALUES (?,?,?,?,?,?,?)")
      .run(a.id, o.fecha || hoy(), o.concepto, o.detalle || "",
           parseFloat(o.monto) || 0, o.moneda || "USD", o.notas || "");
    return { ok: true, animal_id: a.id, rp: a.rp };
  }

  function prorratear(eventoId) {
    const e = db.prepare("SELECT * FROM costeo_eventos WHERE id = ?").get(eventoId);
    if (!e) throw new Error("Evento inexistente");
    if (e.estado === "PRORRATEADO") throw new Error("Ya prorrateado. Borrá el prorrateo y recalculá.");

    let sql = `SELECT p.* FROM costeo_permanencia p JOIN animales a ON a.id = p.animal_id
               WHERE p.fecha_desde <= ? AND (p.fecha_hasta IS NULL OR p.fecha_hasta >= ?)`;
    const args = [e.fecha_hasta, e.fecha_desde];
    if (e.lote_id)   { sql += " AND p.lote_id = ?";   args.push(e.lote_id); }
    if (e.categoria) { sql += " AND p.categoria = ?"; args.push(e.categoria); }

    const conDias = db.prepare(sql).all(...args).map(pm => {
      const ini = pm.fecha_desde > e.fecha_desde ? pm.fecha_desde : e.fecha_desde;
      const fin = (!pm.fecha_hasta || pm.fecha_hasta > e.fecha_hasta) ? e.fecha_hasta : pm.fecha_hasta;
      return { ...pm, d: dias(ini, fin) };
    }).filter(x => x.d > 0);

    if (!conDias.length) throw new Error("Ningún animal en permanencia para ese evento");
    const total = conDias.reduce((s, x) => s + x.d, 0);
    if (!total) throw new Error("Días-animal en cero");

    const ins = db.prepare("INSERT INTO costos (animal_id,fecha,concepto,detalle,monto,moneda) VALUES (?,?,?,?,?,'USD')");
    const tx = db.transaction(() => {
      conDias.forEach(x => {
        const monto = e.metodo === "CABEZA" ? e.monto_usd / conDias.length
                                            : e.monto_usd * x.d / total;
        ins.run(x.animal_id, e.fecha_hasta, e.concepto,
          `${MARCA}#${e.id} · ${Math.round(x.d)} de ${Math.round(total)} días-animal`, monto);
      });
      db.prepare("UPDATE costeo_eventos SET estado = 'PRORRATEADO' WHERE id = ?").run(e.id);
    });
    tx();
    return { animales: conDias.length, dias_animal: Math.round(total), monto: e.monto_usd };
  }

  function borrarProrrateo(eventoId) {
    const tx = db.transaction(() => {
      db.prepare("DELETE FROM costos WHERE detalle LIKE ?").run(`${MARCA}#${eventoId} %`);
      db.prepare("UPDATE costeo_eventos SET estado = 'PENDIENTE' WHERE id = ?").run(eventoId);
    });
    tx();
    return { ok: true };
  }

  /* ── Consultas ──────────────────────────────────────────────────────────── */

  function ficha(ref) {
    const a = resolver(ref), p = precios();
    const nov = p ? p.novillo_pie : 0;
    const asientos = db.prepare("SELECT * FROM costeo_kgne WHERE animal_id = ? ORDER BY fecha, id").all(a.id);
    const costos   = db.prepare("SELECT * FROM costos WHERE animal_id = ? ORDER BY fecha, id").all(a.id);
    const etapas   = db.prepare("SELECT * FROM costeo_etapas WHERE animal_id = ? ORDER BY fecha_ini").all(a.id);

    const prod = asientos.reduce((s, x) => s + (x.kgne_produccion || 0), 0);
    const cTot = costos.reduce((s, x) => s + x.monto, 0);
    const cPro = costos.filter(x => esProductivo(x.concepto)).reduce((s, x) => s + x.monto, 0);

    const porEtapa = {};
    const b = k => (porEtapa[k] = porEtapa[k] || { prod: 0, costo: 0 });
    asientos.forEach(x => { b(x.etapa).prod += x.kgne_produccion || 0; });
    costos.forEach(x => {
      const anteriores = asientos.filter(s => s.fecha <= x.fecha);
      const et = anteriores.length ? anteriores[anteriores.length - 1].etapa : "RECRIA";
      b(et).costo += x.monto;
    });

    return {
      animal: a, etapa_actual: etapaActual(a.id), saldo_kgne: saldo(a.id),
      novillo_pie: nov, asientos, costos, etapas, por_etapa: porEtapa,
      resumen: {
        produccion_kgne: prod, usd_producidos: prod * nov,
        costo_total: cTot, costo_productivo: cPro,
        margen: prod * nov - cTot,
        usd_por_kgne: prod > 0 ? cTot / prod : null,
        usd_por_kgne_directo: prod > 0 ? cPro / prod : null,
        margen_pct: prod > 0 && nov > 0 ? (nov - cTot / prod) / nov : null
      }
    };
  }

  function ranking(ciclo, sexo, limite) {
    const p = precios(), nov = p ? p.novillo_pie : 0;
    let sql = `SELECT k.animal_id, k.rp, k.chip, k.generacion, a.categoria, a.sexo, a.registro,
        SUM(k.kgne_produccion) prod,
        (SELECT COALESCE(SUM(c.monto),0) FROM costos c WHERE c.animal_id = k.animal_id) costo
      FROM costeo_kgne k JOIN animales a ON a.id = k.animal_id`;
    const w = [], args = [];
    if (ciclo) { w.push("k.ciclo = ?"); args.push(ciclo); }
    if (sexo)  { w.push("a.sexo = ?");  args.push(sexo); }
    if (w.length) sql += " WHERE " + w.join(" AND ");
    sql += " GROUP BY k.animal_id ORDER BY prod DESC LIMIT " + (parseInt(limite, 10) || 200);

    const filas = db.prepare(sql).all(...args).map(f => ({
      ...f, usd_producidos: (f.prod || 0) * nov,
      margen: (f.prod || 0) * nov - (f.costo || 0),
      usd_por_kgne: f.prod > 0 ? f.costo / f.prod : null
    }));
    return {
      ciclo, sexo, novillo_pie: nov, animales: filas.length,
      produccion_kgne: filas.reduce((s, x) => s + (x.prod || 0), 0),
      costo_total: filas.reduce((s, x) => s + (x.costo || 0), 0),
      margen_total: filas.reduce((s, x) => s + x.margen, 0),
      detalle: filas
    };
  }

  function cuadre(ciclo, totalImprolux) {
    const c = ciclo || cicloDe(hoy());
    const [d1, d2] = rangoCiclo(c);
    const asig = db.prepare("SELECT COALESCE(SUM(monto),0) t, COUNT(*) n FROM costos WHERE fecha BETWEEN ? AND ?").get(d1, d2);
    const pend = db.prepare("SELECT COUNT(*) n, COALESCE(SUM(monto_usd),0) t FROM costeo_eventos WHERE estado = 'PENDIENTE'").get();
    const sinPerm = db.prepare(`SELECT COUNT(*) n FROM animales a WHERE a.estado = 'ACTIVO'
      AND NOT EXISTS (SELECT 1 FROM costeo_permanencia p WHERE p.animal_id = a.id)`).get();
    const sinFicha = db.prepare(`SELECT COUNT(*) n FROM animales a WHERE a.estado = 'ACTIVO'
      AND NOT EXISTS (SELECT 1 FROM costeo_kgne k WHERE k.animal_id = a.id)`).get();
    const tot = (totalImprolux != null && totalImprolux !== "") ? parseFloat(totalImprolux) : null;
    return {
      ciclo: c, desde: d1, hasta: d2,
      asignado_usd: asig.t, asientos_costo: asig.n,
      eventos_pendientes: pend.n, monto_pendiente: pend.t,
      animales_sin_permanencia: sinPerm.n, animales_sin_ficha: sinFicha.n,
      total_improlux: tot,
      diferencia: tot != null ? tot - asig.t : null,
      cuadra: tot != null ? Math.abs(tot - asig.t) < 1 : null
    };
  }

  /* ── Permanencia ────────────────────────────────────────────────────────── */

  function backfillPermanencia() {
    const filas = db.prepare(`SELECT la.lote_id, la.animal_id, la.fecha_ingreso, a.rp, a.categoria
      FROM lote_animales la JOIN animales a ON a.id = la.animal_id`).all();
    const ins = db.prepare(`INSERT INTO costeo_permanencia
      (animal_id,rp,lote_id,categoria,fecha_desde,fecha_hasta) VALUES (?,?,?,?,?,NULL)`);
    let n = 0;
    const tx = db.transaction(() => {
      filas.forEach(f => {
        const ya = db.prepare("SELECT 1 FROM costeo_permanencia WHERE animal_id = ? AND fecha_hasta IS NULL").get(f.animal_id);
        if (!ya) { ins.run(f.animal_id, f.rp, f.lote_id, f.categoria, f.fecha_ingreso || hoy()); n++; }
      });
    });
    tx();
    return { ok: true, insertados: n, leidos: filas.length };
  }

  function moverLote(ref, loteId, categoria, fecha) {
    const a = resolver(ref), f = fecha || hoy();
    db.prepare("UPDATE costeo_permanencia SET fecha_hasta = ? WHERE animal_id = ? AND fecha_hasta IS NULL").run(f, a.id);
    db.prepare("INSERT INTO costeo_permanencia (animal_id,rp,lote_id,categoria,fecha_desde) VALUES (?,?,?,?,?)")
      .run(a.id, a.rp, loteId || null, categoria || a.categoria, f);
    return { ok: true };
  }

  return { precios, hito, animal, saldo, etapaActual, asiento, cerrarEtapa,
           destetar, desteteHijo, tacto, evaluar18, amortizar, baja,
           cargarCosto, esProductivo, prorratear, borrarProrrateo,
           ficha, ranking, cuadre, backfillPermanencia, moverLote };
}

/* ════════════════════════════════════════════════════════════════════════════
   SCRAPER ACG — sobre texto plano, tolerante a cambios de markup
   ════════════════════════════════════════════════════════════════════════════ */
function parsearACG(html) {
  let txt = String(html)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ").replace(/&nbsp;/g, " ").replace(/\s+/g, " ");

  /* "Indice ternero / Novillo" contiene Ternero y Novillo y ensucia las
     búsquedas siguientes: se captura el índice y se borra la frase. */
  const mI = txt.match(/Indice\s+ternero\s*\/\s*Novillo\s*([\d.]*,\d{2,4})/i);
  const indice = mI ? parseFloat(mI[1].replace(/\./g, "").replace(",", ".")) : null;
  txt = txt.replace(/Indice\s+ternero\s*\/\s*Novillo[^A-Za-zÁÉÍÓÚÑ]*/gi, " ");

  const tras = (etq, v) => {
    const i = txt.search(new RegExp(etq, "i"));
    if (i < 0) return null;
    const m = txt.slice(i, i + (v || 180)).match(/(\d{1,3}(?:\.\d{3})*,\d{2,4}|\d+,\d{2,4})/);
    return m ? parseFloat(m[1].replace(/\./g, "").replace(",", ".")) : null;
  };

  const ternero = tras("Ternero", 200);
  const sem = txt.match(/semana\s+N.?\s*(\d+)\s*\(del\s*([\d\/]+)\s*-\s*([\d\/]+)\)/i);

  return {
    semana: sem ? "Sem " + sem[1] : null,
    fecha_desde: sem ? sem[2] : null,
    fecha_hasta: sem ? sem[3] : null,
    indice_ternero: indice,
    ternero_pie: ternero,
    ternera_pie: tras("Ternera", 200),
    vaca_invernada_pie: tras("Vaca\\s+de\\s+Invernada", 200),
    novillo_pie: (ternero && indice) ? ternero / indice : null,
    novillo_4a: tras("Novillo", 220),
    vaca_4a: tras("Vaca(?!\\s+de)", 220),
    vaquillona_4a: tras("Vaquillona", 220)
  };
}

async function bajarACG() {
  const r = await fetch("https://acg.com.uy/", { headers: { "User-Agent": "Mozilla/5.0 (ADE)" } });
  if (!r.ok) throw new Error("ACG respondió " + r.status);
  return parsearACG(await r.text());
}

/* ════════════════════════════════════════════════════════════════════════════
   RUTAS — la DB del campo sale del middleware existente
   ════════════════════════════════════════════════════════════════════════════ */
function rutas(app, motores, CAMPO_DEFAULT) {
  const M = req => motores[req.campoKey] || motores[CAMPO_DEFAULT];
  const D = req => req.campoDB;
  const ok  = (res, d) => res.json({ ok: true, ...d });
  const err = (res, e) => res.status(400).json({ ok: false, error: e.message || String(e) });
  const ref = b => b.rp || b.chip || b.animal_id;

  app.get("/api/costeo/version", (req, res) =>
    ok(res, { version: VERSION, ciclo: cicloDe(hoy()), campo: req.campoKey, etapas: Object.keys(ETAPAS) }));

  app.get("/api/costeo/precios", (req, res) => ok(res, { precios: M(req).precios() }));

  app.post("/api/costeo/precios/acg", async (req, res) => {
    try {
      const p = await bajarACG();
      if (!p.ternero_pie || !p.indice_ternero)
        return err(res, new Error("No pude leer ternero o índice en ACG. Cargalos a mano."));
      D(req).prepare(`INSERT INTO costeo_precios (semana,fecha_desde,fecha_hasta,indice_ternero,
        ternero_pie,ternera_pie,vaca_invernada_pie,novillo_pie,novillo_4a,vaca_4a,vaquillona_4a,fuente)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,'ACG')`).run(p.semana, p.fecha_desde, p.fecha_hasta,
        p.indice_ternero, p.ternero_pie, p.ternera_pie, p.vaca_invernada_pie,
        p.novillo_pie, p.novillo_4a, p.vaca_4a, p.vaquillona_4a);
      ok(res, { precios: M(req).precios(), leido: p });
    } catch (e) { err(res, e); }
  });

  app.post("/api/costeo/precios", (req, res) => {
    try {
      const b = req.body;
      D(req).prepare(`INSERT INTO costeo_precios (semana,fecha_desde,fecha_hasta,indice_ternero,
        ternero_pie,ternera_pie,vaca_invernada_pie,novillo_pie,fuente) VALUES (?,?,?,?,?,?,?,?,'MANUAL')`)
        .run(b.semana || null, b.fecha_desde || null, b.fecha_hasta || null,
             b.indice_ternero, b.ternero_pie, b.ternera_pie, b.vaca_invernada_pie,
             b.novillo_pie || (b.ternero_pie / b.indice_ternero));
      ok(res, { precios: M(req).precios() });
    } catch (e) { err(res, e); }
  });

  app.get("/api/costeo/hitos", (req, res) =>
    ok(res, { hitos: D(req).prepare("SELECT * FROM costeo_hitos ORDER BY hito").all() }));

  app.post("/api/costeo/hitos", (req, res) => {
    try {
      const { hito, kgne, ciclo, nota } = req.body;
      D(req).prepare(`INSERT INTO costeo_hitos (ciclo,hito,kgne,nota) VALUES (?,?,?,?)
        ON CONFLICT(ciclo,hito) DO UPDATE SET kgne = excluded.kgne, nota = excluded.nota`)
        .run(ciclo || "TODOS", hito, kgne, nota || null);
      ok(res, { hitos: D(req).prepare("SELECT * FROM costeo_hitos ORDER BY hito").all() });
    } catch (e) { err(res, e); }
  });

  const post = (ruta, fn) => app.post(ruta, (req, res) => {
    try { ok(res, fn(M(req), req.body)); } catch (e) { err(res, e); }
  });

  post("/api/costeo/destetar",     (m, b) => m.destetar(ref(b), b.fecha, parseFloat(b.kg), b.sexo));
  post("/api/costeo/destete-hijo", (m, b) => m.desteteHijo(ref(b), b.fecha, parseFloat(b.kg), b.sexo, b.rp_hijo));
  post("/api/costeo/tacto",        (m, b) => m.tacto(ref(b), b.fecha, parseInt(b.edad, 10), b.resultado, b.rama));
  post("/api/costeo/evaluar18",    (m, b) => m.evaluar18(ref(b), b.fecha, parseFloat(b.kg), b.destino, parseFloat(b.ref_venta) || 0, b.metodo));
  post("/api/costeo/amortizar",    (m, b) => m.amortizar(ref(b), b.hasta || hoy()));
  post("/api/costeo/baja",         (m, b) => m.baja(ref(b), b.fecha, b.tipo, parseFloat(b.kg) || null, parseFloat(b.precio) || 0));
  post("/api/costeo/costo",        (m, b) => m.cargarCosto(ref(b), b));
  post("/api/costeo/permanencia/mover", (m, b) => m.moverLote(ref(b), b.lote_id, b.categoria, b.fecha));

  app.get("/api/costeo/eventos", (req, res) => {
    const st = D(req).prepare(req.query.estado
      ? "SELECT * FROM costeo_eventos WHERE estado = ? ORDER BY fecha_desde DESC"
      : "SELECT * FROM costeo_eventos ORDER BY fecha_desde DESC");
    ok(res, { eventos: req.query.estado ? st.all(req.query.estado) : st.all() });
  });

  app.post("/api/costeo/evento", (req, res) => {
    try {
      const b = req.body;
      const i = D(req).prepare(`INSERT INTO costeo_eventos (concepto,fecha_desde,fecha_hasta,
        monto_usd,lote_id,categoria,metodo,origen,ref_improlux,ciclo) VALUES (?,?,?,?,?,?,?,?,?,?)`)
        .run(b.concepto, b.fecha_desde, b.fecha_hasta, parseFloat(b.monto_usd),
             b.lote_id || null, b.categoria || null, b.metodo || "DIA_ANIMAL",
             b.origen || "MANUAL", b.ref_improlux || null, b.ciclo || cicloDe(b.fecha_hasta));
      ok(res, { id: Number(i.lastInsertRowid) });
    } catch (e) { err(res, e); }
  });

  app.post("/api/costeo/evento/:id/prorratear", (req, res) => {
    try { ok(res, M(req).prorratear(parseInt(req.params.id, 10))); } catch (e) { err(res, e); }
  });
  app.delete("/api/costeo/evento/:id/prorrateo", (req, res) => {
    try { ok(res, M(req).borrarProrrateo(parseInt(req.params.id, 10))); } catch (e) { err(res, e); }
  });
  app.post("/api/costeo/permanencia/backfill", (req, res) => {
    try { ok(res, M(req).backfillPermanencia()); } catch (e) { err(res, e); }
  });

  app.get("/api/costeo/animal/:ref", (req, res) => {
    try { ok(res, M(req).ficha(req.params.ref)); } catch (e) { err(res, e); }
  });
  app.get("/api/costeo/ranking", (req, res) => {
    try { ok(res, M(req).ranking(req.query.ciclo, req.query.sexo, req.query.limite)); } catch (e) { err(res, e); }
  });
  app.get("/api/costeo/cuadre", (req, res) => {
    try { ok(res, M(req).cuadre(req.query.ciclo, req.query.total_improlux)); } catch (e) { err(res, e); }
  });
}

/* ════════════════════════════════════════════════════════════════════════════ */
function init(databases, app, opts) {
  const o = opts || {};
  const def = o.CAMPO_DEFAULT || Object.keys(databases)[0];
  const motores = {};
  for (const [key, db] of Object.entries(databases)) {
    crearTablas(db);
    motores[key] = motor(db);
  }
  if (app) rutas(app, motores, def);
  console.log(`COSTEO kgNE v${VERSION} activo · ciclo ${cicloDe(hoy())} · campos: ${Object.keys(databases).join(", ")}`);
  return motores;
}

module.exports = { init, crearTablas, motor, parsearACG, bajarACG,
                   cicloDe, rangoCiclo, ETAPAS, VERSION };
