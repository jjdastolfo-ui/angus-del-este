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

const VERSION = "2.2.0";

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
      ref_id INTEGER, origen_id INTEGER, created_at TEXT DEFAULT (datetime('now'))
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

    CREATE TABLE IF NOT EXISTS costeo_dietas (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lote_id INTEGER NOT NULL,
      producto TEXT NOT NULL,               -- nombre del producto en IMPROLUX
      alimento_id INTEGER,                  -- legado, sin uso
      modo TEXT DEFAULT 'TOTAL',            -- TOTAL = kg/día del lote · POR_ANIMAL = kg/día por cabeza
      kg_dia REAL NOT NULL,
      fecha_desde TEXT NOT NULL,
      fecha_hasta TEXT,
      activo INTEGER DEFAULT 1,
      notas TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    -- Un renglón por lote y día: cuántos comieron, cuánto y a qué costo.
    CREATE TABLE IF NOT EXISTS costeo_dieta_dias (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      dieta_id INTEGER NOT NULL, lote_id INTEGER, producto TEXT,
      fecha TEXT NOT NULL, animales INTEGER, kg REAL, costo_kg REAL, costo_total REAL,
      consolidado INTEGER DEFAULT 0,
      enviado INTEGER DEFAULT 0, error_envio TEXT,
      UNIQUE(dieta_id, fecha)
    );

    -- Lista de precios de sanidad: sin esto no se puede costear un tratamiento.
    -- Espejo del stock de IMPROLUX. ADE no es dueño de nada de esto:
    -- solo guarda la última foto para poder costear sin depender de la red.
    CREATE TABLE IF NOT EXISTS costeo_productos (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      producto TEXT NOT NULL UNIQUE,
      tipo TEXT DEFAULT 'INSUMO',
      unidad TEXT DEFAULT 'kg',
      costo_unitario REAL DEFAULT 0,
      stock_improlux REAL DEFAULT 0,
      actualizado TEXT
    );

    CREATE TABLE IF NOT EXISTS costeo_sanidad_precios (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      producto TEXT NOT NULL UNIQUE,
      costo_dosis REAL NOT NULL,
      notas TEXT
    );

    CREATE INDEX IF NOT EXISTS ix_ckgne_animal ON costeo_kgne(animal_id, fecha);
    CREATE INDEX IF NOT EXISTS ix_ckgne_ciclo  ON costeo_kgne(ciclo);
    CREATE INDEX IF NOT EXISTS ix_cperm_animal ON costeo_permanencia(animal_id);
    CREATE INDEX IF NOT EXISTS ix_cetap_animal ON costeo_etapas(animal_id);
  `);

  try { db.exec("ALTER TABLE costeo_kgne ADD COLUMN origen_id INTEGER"); } catch (e) {}

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
      kgne_saldo,kgne_produccion,transferencia,estimado,ref_id,origen_id)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(
      a.id, a.chip || null, a.rp || null, fecha, o.ciclo || cicloDe(fecha),
      a.fecha_nac ? String(a.fecha_nac).slice(0, 4) : null,
      o.etapa, o.tipo, o.origen || null, o.concepto || null, o.detalle || null,
      o.kg_fisicos || null, o.coeficiente || null, p ? p.novillo_pie : null,
      nuevo, prod, o.transferencia ? 1 : 0, o.estimado ? 1 : 0, o.ref_id || null, o.origen_id || null);

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
  function desteteHijo(ref, fecha, kg, sexo, refHijo, origenId) {
    const a = resolver(ref), p = precios();
    const coef = esM(sexo) ? p.coef.TERNERO : p.coef.TERNERA;
    const hijo = refHijo ? animal(refHijo) : null;
    return asiento(a, { fecha, etapa: etapaActual(a.id), tipo: "PROD", origen: "DESTETE_HIJO",
      concepto: "Desteta " + (esM(sexo) ? "ternero" : "ternera"),
      detalle: kg + " kg × " + coef.toFixed(3) + (hijo ? " · RP " + hijo.rp : ""),
      kg_fisicos: kg, coeficiente: coef, kgne: kg * coef,
      ref_id: hijo ? hijo.id : null, origen_id: origenId });
  }

  function tacto(ref, fecha, edad, resultado, rama, origenId) {
    const a = resolver(ref), ciclo = cicloDe(fecha), prev = etapaActual(a.id);
    const prenada = String(resultado || "").toUpperCase().includes("PRE");

    if (prenada) {
      const destino = Number(edad) === 18 ? "INTERMEDIA" : "PLANTEL";
      const kgne = hito(Number(edad) === 18 ? "PRENADA_18M" : "PRENADA_30M", ciclo);
      if (kgne == null) throw new Error("Falta el hito de valuación");
      const r = asiento(a, { fecha, etapa: prev, tipo: "REVAL", origen: "TACTO_" + edad,
        concepto: "Preñada " + edad + "m · pasa a " + ETAPAS[destino].label.toLowerCase(),
        detalle: "hito " + kgne + " kgNE", kgne, origen_id: origenId });
      cerrarEtapa(a, prev, fecha, "PRENADA");
      abrir(a, destino, fecha, kgne);
      return { ...r, etapa_nueva: destino };
    }

    const destino = rama === "ENGORDE" ? "ENGORDE" : "SERV_INV";
    const s = saldo(a.id);
    const r = asiento(a, { fecha, etapa: prev, tipo: "REVAL", origen: "TACTO_" + edad,
      concepto: "Vacía " + edad + "m · " + ETAPAS[destino].label.toLowerCase(),
      detalle: "sin revalúo", kgne: s, origen_id: origenId });
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


  /* ── Generación automática desde los datos que ADE ya tiene ─────────────────
     Lee pesadas(contexto=DESTETE), servicios(resultado/fecha_parto/peso_destete)
     y animales(estado/fecha_salida) y reconstruye el libro mayor completo.
     Idempotente: borra y regenera la ficha del animal. */

  function borrarFicha(aid) {
    db.prepare("DELETE FROM costeo_kgne WHERE animal_id = ?").run(aid);
    db.prepare("DELETE FROM costeo_etapas WHERE animal_id = ?").run(aid);
  }

  function generar(ref, opts) {
    const o = opts || {};
    const a = resolver(ref);
    const p = precios();
    if (!p) throw new Error("No hay precios cargados");

    const log = [];
    const hembra = !esM(a.sexo);

    /* datos fuente */
    const destete = db.prepare(`SELECT * FROM pesadas WHERE animal_id = ?
      AND UPPER(COALESCE(contexto,'')) = 'DESTETE' ORDER BY fecha LIMIT 1`).get(a.id);
    const servicios = db.prepare(`SELECT * FROM servicios WHERE animal_id = ?
      ORDER BY COALESCE(fecha_iatf, fecha_ingreso_toro, created_at)`).all(a.id);

    if (!destete && !servicios.length)
      return { ok: false, rp: a.rp, motivo: "Sin pesada de destete ni servicios" };

    if (o.dryRun) {
      return { ok: true, rp: a.rp, sexo: a.sexo, dry: true,
        destete: destete ? { fecha: destete.fecha, peso: destete.peso } : null,
        servicios: servicios.length,
        partos: servicios.filter(x => x.fecha_parto && x.peso_destete).length };
    }

    borrarFicha(a.id);

    /* 1 · destete propio */
    if (destete) {
      destetar(a.id, destete.fecha, destete.peso, a.sexo);
      log.push("destete " + destete.fecha + " · " + destete.peso + " kg");
    }

    /* 2 · recorrido de servicios */
    let preneces = 0, abierta = !!destete;
    for (const sv of servicios) {
      const fTacto = sv.fecha_parto
        || sv.fecha_iatf || sv.fecha_ingreso_toro || sv.created_at;
      if (!fTacto) continue;
      const f = String(fTacto).slice(0, 10);
      const res = String(sv.resultado || "").toUpperCase();
      const prenada = res.includes("PRE");

      if (hembra) {
        if (prenada) {
          preneces++;
          if (preneces <= 2) {
            /* si nunca hubo destete, la primera preñez abre la ficha directo */
            if (!abierta) {
              const kg = hito(preneces === 1 ? "PRENADA_18M" : "PRENADA_30M", cicloDe(f));
              abrir(a, preneces === 1 ? "INTERMEDIA" : "PLANTEL", f, kg);
              log.push("alta directa " + f + " · " + kg + " kgNE (sin destete previo)");
              abierta = true;
            } else {
              tacto(a.id, f, preneces === 1 ? 18 : 30, "PRENADA");
              log.push("tacto " + (preneces === 1 ? 18 : 30) + "m preñada " + f);
            }
          }
        } else if (res.includes("VAC") && abierta && preneces < 2) {
          tacto(a.id, f, preneces === 0 ? 18 : 30, "VACIA", o.rama || "SERV_INV");
          log.push("tacto vacía " + f + " · rama " + (o.rama || "SERV_INV"));
          break; /* rama terminal */
        }
      }

      /* 3 · destete de la cría */
      if (sv.fecha_parto && sv.peso_destete > 0 && abierta) {
        const fd = sv.fecha_destete_cria
          || new Date(new Date(sv.fecha_parto).getTime() + 180 * 86400000).toISOString().slice(0, 10);
        desteteHijo(a.id, fd, sv.peso_destete, sv.sexo_cria || "MACHO", sv.ternero_rp);
        log.push("desteta cría " + fd + " · " + sv.peso_destete + " kg");
      }
    }

    /* 4 · macho: evaluación de 18 meses según destino */
    if (!hembra && abierta) {
      const p18 = db.prepare(`SELECT * FROM pesadas WHERE animal_id = ?
        AND UPPER(COALESCE(contexto,'')) IN ('18MESES','AÑO','DESARROLLO')
        ORDER BY fecha DESC LIMIT 1`).get(a.id);
      if (p18) {
        const d = String(a.destino || "").toUpperCase();
        const destino = d.includes("PLANTEL") ? "PLANTEL"
                      : d.includes("VENTA") || d.includes("TORO") ? "VENTA" : "DESCARTE";
        evaluar18(a.id, p18.fecha, p18.peso, destino, o.ref_venta || 0,
                  o.ref_venta ? "REF" : "FISICO");
        log.push("evaluación 18m " + p18.fecha + " · " + destino);
      }
    }

    /* 5 · baja */
    if (String(a.estado || "").toUpperCase() !== "ACTIVO" && a.fecha_salida && abierta) {
      const m = String(a.motivo_salida || "").toUpperCase();
      const tipo = m.includes("MUERT") ? "MUERTE"
                 : m.includes("REMATE") ? "REMATE"
                 : m.includes("REFUGO") ? "REFUGO" : "VENTA";
      baja(a.id, String(a.fecha_salida).slice(0, 10), tipo, null, o.precio_baja || 0);
      log.push("baja " + a.fecha_salida + " · " + tipo);
    }

    /* 6 · amortización al día si quedó en plantel */
    if (etapaActual(a.id) === "PLANTEL") amortizar(a.id, hoy());

    return { ok: true, rp: a.rp, sexo: a.sexo, asientos: log.length,
             etapa: etapaActual(a.id), saldo: saldo(a.id), log };
  }

  function generarTodos(opts) {
    const o = opts || {};
    let sql = "SELECT id, rp, sexo FROM animales WHERE 1=1";
    const args = [];
    if (o.sexo)  { sql += " AND sexo = ?"; args.push(o.sexo); }
    if (!o.incluirBajas) sql += " AND estado = 'ACTIVO'";
    sql += " ORDER BY id";
    if (o.limite) sql += " LIMIT " + parseInt(o.limite, 10);

    const animales = db.prepare(sql).all(...args);
    const res = { total: animales.length, generados: 0, omitidos: 0, errores: 0, detalle: [] };

    for (const an of animales) {
      try {
        const r = generar(an.id, o);
        if (r.ok) { res.generados++; if (o.dryRun) res.detalle.push(r); }
        else { res.omitidos++; res.detalle.push({ rp: an.rp, motivo: r.motivo }); }
      } catch (e) {
        res.errores++;
        res.detalle.push({ rp: an.rp, error: e.message });
      }
    }
    return res;
  }


  /* ── APERTURA DE INVENTARIO ────────────────────────────────────────────────
     La vía simple: en vez de reconstruir la historia, se le pone a cada animal
     un saldo de apertura según la categoría que ya tiene en ADE, con fecha de
     corte. De ahí en adelante todo acumula solo. Es lo que hace cualquier
     inventario de arranque: no se reconstruye el pasado, se cuenta lo que hay. */

  const PESO_DEFECTO = { TERNERO:200, RECRIA:300, VAQUILLONA:380, VACA:470, TORO:700, NOVILLO:450 };

  function clasificar(a, fechaCorte) {
    const cat = String(a.categoria || "").toUpperCase();
    const macho = esM(a.sexo);
    const ciclo = cicloDe(fechaCorte);
    let meses = null;
    if (a.fecha_nac) meses = anios(a.fecha_nac, fechaCorte) * 12;

    if (cat === "TERNERO" || cat === "TERNERA")
      return { etapa: "RECRIA", modo: "PESO", coef: macho ? "TERNERO" : "TERNERA" };
    if (cat === "RECRIA" || cat === "VAQUILLONA")
      return { etapa: "RECRIA", modo: "PESO", coef: "NOVILLO" };
    if (cat === "NOVILLO")
      return { etapa: "TERMINACION", modo: "PESO", coef: "NOVILLO" };
    if (cat === "TORO")
      return { etapa: "PLANTEL", modo: "PESO", coef: "NOVILLO" };
    if (cat === "VACA") {
      /* primeriza (menos de 42 meses) entra a intermedia, el resto a plantel */
      const primeriza = meses !== null && meses < 42;
      return { etapa: primeriza ? "INTERMEDIA" : "PLANTEL", modo: "HITO",
               hito: primeriza ? "PRENADA_18M" : "PRENADA_30M" };
    }
    /* sin categoría reconocible: por sexo y edad */
    if (!macho && meses !== null && meses >= 36)
      return { etapa: "PLANTEL", modo: "HITO", hito: "PRENADA_30M" };
    return { etapa: "RECRIA", modo: "PESO", coef: "NOVILLO" };
  }

  function apertura(opts) {
    const o = opts || {};
    const fecha = o.fecha || hoy();
    const p = precios();
    if (!p) throw new Error("No hay precios cargados. Bajá los de ACG primero.");

    let sql = "SELECT * FROM animales WHERE estado = 'ACTIVO'";
    const args = [];
    if (o.categoria) { sql += " AND categoria = ?"; args.push(o.categoria); }
    sql += " ORDER BY id";
    const animales = db.prepare(sql).all(...args);

    const res = { fecha, total: animales.length, abiertos: 0, yaTenian: 0,
                  estimados: 0, kgne_total: 0, por_categoria: {}, detalle: [] };

    for (const a of animales) {
      const ya = db.prepare("SELECT 1 FROM costeo_kgne WHERE animal_id = ? LIMIT 1").get(a.id);
      if (ya && !o.forzar) { res.yaTenian++; continue; }

      const c = clasificar(a, fecha);
      let kgne, detalle, estimado = 0;

      if (c.modo === "HITO") {
        kgne = hito(c.hito, cicloDe(fecha));
        detalle = "hito " + c.hito + " · " + kgne + " kgNE";
      } else {
        const pe = db.prepare("SELECT peso, fecha FROM pesadas WHERE animal_id = ? ORDER BY fecha DESC LIMIT 1").get(a.id);
        const cat = String(a.categoria || "").toUpperCase();
        const peso = pe ? pe.peso : (PESO_DEFECTO[cat] || 300);
        if (!pe) { estimado = 1; res.estimados++; }
        const coef = p.coef[c.coef] || 1;
        kgne = peso * coef;
        detalle = peso + " kg × " + coef.toFixed(3) + (pe ? " · pesada " + pe.fecha : " · PESO ESTIMADO");
      }

      const k = String(a.categoria || "SIN CATEGORIA").toUpperCase();
      res.por_categoria[k] = res.por_categoria[k] || { n: 0, kgne: 0, etapa: c.etapa, estimados: 0 };
      res.por_categoria[k].n++;
      res.por_categoria[k].kgne += kgne;
      if (estimado) res.por_categoria[k].estimados++;
      res.kgne_total += kgne;

      if (!o.dryRun) {
        if (ya) borrarFicha(a.id);
        asiento(a, { fecha, etapa: c.etapa, tipo: "TRANS", origen: "APERTURA_INVENTARIO",
          concepto: "Apertura de inventario · " + ETAPAS[c.etapa].label,
          detalle, kgne, transferencia: 1, estimado });
      }
      res.abiertos++;
      if (o.detallado) res.detalle.push({ rp: a.rp, categoria: a.categoria,
        etapa: c.etapa, kgne: Math.round(kgne), estimado });
    }

    res.usd_total = res.kgne_total * p.novillo_pie;
    return res;
  }


  /* ── SINCRONIZACIÓN INCREMENTAL ────────────────────────────────────────────
     No engancha ningún endpoint existente: lee servicios y pesadas y agrega al
     libro mayor solo lo que todavía no está. Idempotente y repetible. Se puede
     correr a mano o dejarla en el cron diario. */

  function yaProcesado(aid, origen, origenId) {
    return !!db.prepare("SELECT 1 FROM costeo_kgne WHERE animal_id = ? AND origen = ? AND origen_id = ? LIMIT 1")
      .get(aid, origen, origenId);
  }

  function sincronizarAnimal(a, o) {
    const log = [];
    if (etapaActual(a.id) === "FIN") return log;
    const hembra = !esM(a.sexo);
    const primero = db.prepare(
      "SELECT fecha FROM costeo_kgne WHERE animal_id = ? ORDER BY fecha, id LIMIT 1").get(a.id);
    if (!primero) return log;
    /* Solo hacia adelante: un evento anterior a la apertura desordena el saldo.
       Lo que pasó antes ya está incorporado en el saldo de apertura. */
    const corte = o.desde && o.desde > primero.fecha ? o.desde : primero.fecha;

    /* cuántas preñeces ya registró el libro */
    let preneces = db.prepare(
      "SELECT COUNT(*) n FROM costeo_kgne WHERE animal_id = ? AND origen LIKE 'TACTO_%'"
    ).get(a.id).n;
    /* si abrió en plantel por inventario, ya cuenta como dos */
    if (etapaActual(a.id) === "PLANTEL" && !preneces) preneces = 2;
    if (etapaActual(a.id) === "INTERMEDIA" && !preneces) preneces = 1;

    const servicios = db.prepare(`SELECT * FROM servicios WHERE animal_id = ?
      ORDER BY COALESCE(fecha_iatf, fecha_ingreso_toro, created_at)`).all(a.id);

    for (const sv of servicios) {
      const res = String(sv.resultado || "").toUpperCase();

      /* tacto */
      if (hembra && res && preneces < 2 && !yaProcesado(a.id, "TACTO_SYNC", sv.id)) {
        const f = String(sv.tacto_servicio || sv.fecha_iatf || sv.fecha_ingreso_toro || "").slice(0, 10);
        if (f && f >= corte) {
          if (res.includes("PRE")) {
            preneces++;
            tacto(a.id, f, preneces === 1 ? 18 : 30, "PRENADA", null, sv.id);
            db.prepare("UPDATE costeo_kgne SET origen = 'TACTO_SYNC' WHERE animal_id = ? AND origen_id = ? AND origen LIKE 'TACTO_%'").run(a.id, sv.id);
            log.push(`tacto ${preneces === 1 ? 18 : 30}m preñada ${f}`);
          } else if (res.includes("VAC")) {
            tacto(a.id, f, preneces === 0 ? 18 : 30, "VACIA", o.rama || "SERV_INV", sv.id);
            db.prepare("UPDATE costeo_kgne SET origen = 'TACTO_SYNC' WHERE animal_id = ? AND origen_id = ? AND origen LIKE 'TACTO_%'").run(a.id, sv.id);
            log.push(`tacto vacía ${f}`);
            break;
          }
        }
      }

      /* destete de la cría */
      if (sv.fecha_parto && sv.peso_destete > 0 && !yaProcesado(a.id, "DESTETE_HIJO", sv.id)) {
        let fd = new Date(new Date(sv.fecha_parto).getTime() + 180 * 86400000).toISOString().slice(0, 10);
        if (fd < corte) fd = corte;
        desteteHijo(a.id, fd, sv.peso_destete, sv.sexo_cria || "MACHO", sv.ternero_rp, sv.id);
        log.push(`desteta cría ${fd} · ${sv.peso_destete} kg`);
      }
    }

    /* destete propio del animal joven que todavía no lo tiene */
    if (etapaActual(a.id) === "RECRIA") {
      const d = db.prepare(`SELECT * FROM pesadas WHERE animal_id = ?
        AND UPPER(COALESCE(contexto,'')) = 'DESTETE' ORDER BY fecha LIMIT 1`).get(a.id);
      if (d && d.fecha >= corte && !yaProcesado(a.id, "DESTETE_SYNC", d.id)) {
        const tieneDestete = db.prepare(
          "SELECT 1 FROM costeo_kgne WHERE animal_id = ? AND origen = 'DESTETE' LIMIT 1").get(a.id);
        if (!tieneDestete) {
          const p = precios();
          const coef = esM(a.sexo) ? p.coef.TERNERO : p.coef.TERNERA;
          asiento(a, { fecha: d.fecha, etapa: "RECRIA", tipo: "REVAL", origen: "DESTETE_SYNC",
            concepto: "Destete registrado", detalle: d.peso + " kg × " + coef.toFixed(3),
            kg_fisicos: d.peso, coeficiente: coef, kgne: d.peso * coef, origen_id: d.id });
          log.push(`destete propio ${d.fecha}`);
        }
      }
    }

    /* baja */
    if (String(a.estado || "").toUpperCase() !== "ACTIVO" && a.fecha_salida
        && etapaActual(a.id) !== "FIN") {
      const m = String(a.motivo_salida || "").toUpperCase();
      const tipo = m.includes("MUERT") ? "MUERTE" : m.includes("REMATE") ? "REMATE"
                 : m.includes("REFUGO") ? "REFUGO" : "VENTA";
      baja(a.id, String(a.fecha_salida).slice(0, 10), tipo, null, o.precio_baja || 0);
      log.push(`baja ${a.fecha_salida} · ${tipo}`);
    }

    return log;
  }

  /* Alta de la generación nueva: todo animal con pesada de destete y sin ficha
     entra por su destete, no por inventario. Esos son los que van a tener
     historia económica completa de la cuna al remate. */
  function altaNuevos(o) {
    const nuevos = db.prepare(`SELECT a.*, p.fecha AS f_dest, p.peso AS peso_dest, p.id AS pesada_id
      FROM animales a
      JOIN pesadas p ON p.animal_id = a.id AND UPPER(COALESCE(p.contexto,'')) = 'DESTETE'
      WHERE a.estado = 'ACTIVO'
        AND NOT EXISTS (SELECT 1 FROM costeo_kgne k WHERE k.animal_id = a.id)
      GROUP BY a.id`).all();
    const out = [];
    for (const a of nuevos) {
      try {
        destetar(a.id, String(a.f_dest).slice(0, 10), a.peso_dest, a.sexo);
        db.prepare("UPDATE costeo_kgne SET origen_id = ? WHERE animal_id = ? AND origen = 'DESTETE'")
          .run(a.pesada_id, a.id);
        out.push({ rp: a.rp, fecha: a.f_dest, kg: a.peso_dest, generacion: a.fecha_nac ? String(a.fecha_nac).slice(0,4) : null });
      } catch (e) { out.push({ rp: a.rp, error: e.message }); }
    }
    return out;
  }

  function sincronizar(opts) {
    const o = opts || {};
    if (!precios()) throw new Error("No hay precios cargados");
    const altas = o.altaNuevos === false ? [] : altaNuevos(o);
    const animales = db.prepare(`SELECT DISTINCT a.* FROM animales a
      JOIN costeo_kgne k ON k.animal_id = a.id`).all();
    const res = { revisados: animales.length, altas: altas.filter(x => !x.error).length,
                  detalle_altas: o.detallado ? altas : undefined,
                  actualizados: 0, asientos: 0, detalle: [] };
    for (const a of animales) {
      try {
        const log = sincronizarAnimal(a, o);
        if (log.length) {
          res.actualizados++; res.asientos += log.length;
          if (o.detallado) res.detalle.push({ rp: a.rp, cambios: log });
        }
      } catch (e) { res.detalle.push({ rp: a.rp, error: e.message }); }
    }
    /* ración del día y sanidad valorizada */
    if (o.dietas !== false) { try { res.dietas = aplicarDietas(); } catch (e) {} }
    if (o.sanidad !== false) { try { res.sanidad = importarSanidad({ desde: o.desde_sanidad }); } catch (e) {} }

    /* amortización al día de todo el plantel */
    if (o.amortizar !== false) {
      const enPlantel = db.prepare(`SELECT DISTINCT animal_id FROM costeo_kgne k1
        WHERE etapa = 'PLANTEL' AND NOT EXISTS (
          SELECT 1 FROM costeo_kgne k2 WHERE k2.animal_id = k1.animal_id AND k2.etapa = 'FIN')`).all();
      enPlantel.forEach(x => { try { amortizar(x.animal_id, hoy()); } catch (e) {} });
      res.amortizados = enPlantel.length;
    }
    return res;
  }


  /* ── COHORTES ──────────────────────────────────────────────────────────────
     El historial económico por generación. Distingue los animales con historia
     completa (abrieron por destete) de los que entraron por inventario. */
  function cohortes() {
    const p = precios(); const nov = p ? p.novillo_pie : 0;
    const filas = db.prepare(`
      SELECT a.id, a.rp, a.sexo, a.categoria, a.registro,
        COALESCE(k.generacion, substr(a.fecha_nac,1,4)) nacimiento,
        SUM(k.kgne_produccion) prod,
        MIN(k.fecha) desde,
        (SELECT ciclo  FROM costeo_kgne k4 WHERE k4.animal_id = a.id ORDER BY fecha, id LIMIT 1) ciclo_alta,
        (SELECT origen FROM costeo_kgne k2 WHERE k2.animal_id = a.id ORDER BY fecha, id LIMIT 1) origen1,
        (SELECT etapa  FROM costeo_kgne k3 WHERE k3.animal_id = a.id ORDER BY fecha DESC, id DESC LIMIT 1) etapa,
        (SELECT COALESCE(SUM(c.monto),0) FROM costos c WHERE c.animal_id = a.id) costo
      FROM costeo_kgne k JOIN animales a ON a.id = k.animal_id
      GROUP BY a.id`).all();

    /* La cohorte es el ciclo en que el animal entró al libro. El ternero
       destetado en marzo 2026 nació en primavera 2025: agruparlo por año de
       nacimiento parte la camada al medio. */
    const g = {};
    for (const f of filas) {
      const completo = f.origen1 === "DESTETE";
      const k = (completo ? "Camada " : "Inventario ") + (f.ciclo_alta || "s/c");
      g[k] = g[k] || { generacion: k, ciclo: f.ciclo_alta, tipo: completo ? "CAMADA" : "INVENTARIO",
                       n: 0, completos: 0, inventario: 0,
                       prod: 0, costo: 0, machos: 0, hembras: 0, activos: 0 };
      const x = g[k];
      x.n++;
      if (completo) x.completos++; else x.inventario++;
      x.prod += f.prod || 0;
      x.costo += f.costo || 0;
      if (esM(f.sexo)) x.machos++; else x.hembras++;
      if (f.etapa !== "FIN") x.activos++;
    }
    const lista = Object.values(g).sort((a, b) => String(b.generacion).localeCompare(String(a.generacion)))
      .map(x => ({ ...x,
        usd_producidos: x.prod * nov,
        margen: x.prod * nov - x.costo,
        usd_por_kgne: x.prod > 0 ? x.costo / x.prod : null,
        prod_por_cabeza: x.n ? x.prod / x.n : 0,
        pct_completos: x.n ? x.completos / x.n : 0 }));
    return { novillo_pie: nov, generaciones: lista,
             total_animales: filas.length,
             total_completos: filas.filter(f => f.origen1 === "DESTETE").length };
  }


  /* ── RACIÓN POR LOTE ───────────────────────────────────────────────────────
     La dieta se asigna al lote, no al animal. El conteo sale de lote_animales,
     así que no se desfasa cuando movés hacienda. Cada día: descuenta stock y
     deja el renglón del día. La consolidación mensual lo baja a costo por animal
     prorrateado por los días que cada uno estuvo en el lote. */

  function animalesDelLote(loteId) {
    return db.prepare("SELECT animal_id FROM lote_animales WHERE lote_id = ?").all(loteId)
             .map(x => x.animal_id);
  }

  function productoCache(nombre) {
    return db.prepare("SELECT * FROM costeo_productos WHERE UPPER(TRIM(producto)) = UPPER(TRIM(?))").get(nombre || "");
  }

  /* Avisa el consumo a IMPROLUX. Si no llega, queda pendiente y reintenta. */
  async function avisarConsumo(baseUrl, payload) {
    const base = String(baseUrl).replace(/\/$/, "");
    const rutas = ["/api/stock/consumo", "/api/stock/aplicar", "/api/stock/movimiento"];
    const errs = [];
    for (const r of rutas) {
      try {
        const resp = await fetch(base + r, { method: "POST",
          headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
        const d = await resp.json().catch(() => ({}));
        if (resp.ok && (d.ok || d.id || d.restante !== undefined)) return { ok: true, ruta: r, data: d };
        errs.push(`${r} → ${d.error || resp.status}`);
      } catch (e) { errs.push(`${r} → ${String(e.message).slice(0, 40)}`); }
    }
    return { ok: false, error: errs.join(" | ") };
  }

  /* Deja el renglón del día. No toca stock local porque ADE no tiene stock:
     el descuento real lo hace IMPROLUX cuando se le avisa el consumo. */
  function aplicarDietas(fecha) {
    const f = (fecha || hoy()).slice(0, 10);
    const dietas = db.prepare(`SELECT * FROM costeo_dietas
      WHERE activo = 1 AND fecha_desde <= ? AND (fecha_hasta IS NULL OR fecha_hasta >= ?)`).all(f, f);
    const res = { fecha: f, dietas: dietas.length, aplicadas: 0, kg: 0, costo: 0, sin_precio: [], detalle: [] };

    for (const d of dietas) {
      if (db.prepare("SELECT 1 FROM costeo_dieta_dias WHERE dieta_id = ? AND fecha = ?").get(d.id, f)) continue;
      const n = animalesDelLote(d.lote_id).length;
      if (!n) { res.detalle.push({ dieta: d.id, aviso: "lote vacío" }); continue; }
      const pr = productoCache(d.producto);
      if (!pr) { res.sin_precio.push(d.producto); }
      const costoKg = pr ? pr.costo_unitario : 0;
      const kg = d.modo === "POR_ANIMAL" ? d.kg_dia * n : d.kg_dia;
      const costo = kg * costoKg;

      db.prepare(`INSERT INTO costeo_dieta_dias (dieta_id,lote_id,producto,fecha,animales,kg,costo_kg,costo_total)
        VALUES (?,?,?,?,?,?,?,?)`).run(d.id, d.lote_id, d.producto, f, n, kg, costoKg, costo);
      res.aplicadas++; res.kg += kg; res.costo += costo;
      res.detalle.push({ lote_id: d.lote_id, producto: d.producto, animales: n,
                         kg: Math.round(kg * 10) / 10, costo: Math.round(costo * 100) / 100 });
    }
    return res;
  }

  /* Manda a IMPROLUX los consumos que todavía no salieron */
  async function enviarConsumos(baseUrl) {
    const pend = db.prepare("SELECT * FROM costeo_dieta_dias WHERE enviado = 0 ORDER BY fecha").all();
    const res = { pendientes: pend.length, enviados: 0, fallidos: 0, errores: [] };
    for (const x of pend) {
      const r = await avisarConsumo(baseUrl, { producto: x.producto, cantidad: x.kg,
        fecha: x.fecha, lote: x.lote_id, tipo: "CONSUMO",
        detalle: `ADE · ${x.animales} animales` });
      if (r.ok) { db.prepare("UPDATE costeo_dieta_dias SET enviado = 1, error_envio = NULL WHERE id = ?").run(x.id); res.enviados++; }
      else { db.prepare("UPDATE costeo_dieta_dias SET error_envio = ? WHERE id = ?").run(r.error, x.id);
             res.fallidos++; if (res.errores.length < 3) res.errores.push(r.error); }
    }
    return res;
  }

  /* Días de stock: el saldo lo dice IMPROLUX, el consumo lo sabe ADE */
  function diasStock() {
    const consumo = db.prepare(`SELECT d.producto,
        SUM(CASE WHEN d.modo='POR_ANIMAL'
          THEN d.kg_dia * (SELECT COUNT(*) FROM lote_animales la WHERE la.lote_id = d.lote_id)
          ELSE d.kg_dia END) kg_dia
      FROM costeo_dietas d WHERE d.activo = 1
        AND d.fecha_desde <= date('now') AND (d.fecha_hasta IS NULL OR d.fecha_hasta >= date('now'))
      GROUP BY UPPER(TRIM(d.producto))`).all();
    return consumo.map(c => {
      const pr = productoCache(c.producto) || {};
      const stock = pr.stock_improlux || 0;
      return { producto: c.producto, unidad: pr.unidad || "kg",
        stock_improlux: stock, costo_unitario: pr.costo_unitario || 0,
        actualizado: pr.actualizado || null, kg_dia: c.kg_dia,
        usd_dia: c.kg_dia * (pr.costo_unitario || 0),
        dias: c.kg_dia > 0 && stock > 0 ? Math.floor(stock / c.kg_dia) : null,
        sin_precio: !pr.producto };
    });
  }

  /* Consolida los días en un costo por animal, prorrateado por permanencia */
  function consolidarRacion(desde, hasta) {
    const d1 = desde || new Date(Date.now() - 31 * 86400000).toISOString().slice(0, 10);
    const d2 = hasta || hoy();
    const dias = db.prepare(`SELECT * FROM costeo_dieta_dias
      WHERE consolidado = 0 AND fecha BETWEEN ? AND ? ORDER BY fecha`).all(d1, d2);
    if (!dias.length) return { consolidados: 0, animales: 0, costo: 0 };

    /* acumula costo por animal según en qué lote estuvo cada día */
    const acum = {};
    for (const x of dias) {
      const enLote = db.prepare(`SELECT animal_id FROM costeo_permanencia
        WHERE lote_id = ? AND fecha_desde <= ? AND (fecha_hasta IS NULL OR fecha_hasta >= ?)`)
        .all(x.lote_id, x.fecha, x.fecha).map(r => r.animal_id);
      const lista = enLote.length ? enLote : animalesDelLote(x.lote_id);
      if (!lista.length) continue;
      const porCabeza = x.costo_total / lista.length;
      lista.forEach(id => { acum[id] = (acum[id] || 0) + porCabeza; });
    }

    const ins = db.prepare("INSERT INTO costos (animal_id,fecha,concepto,detalle,monto,moneda) VALUES (?,?,'RACION',?,?,'USD')");
    let total = 0;
    const tx = db.transaction(() => {
      for (const [id, monto] of Object.entries(acum)) {
        ins.run(parseInt(id, 10), d2, `costeo:racion ${d1} a ${d2}`, Math.round(monto * 100) / 100);
        total += monto;
      }
      db.prepare("UPDATE costeo_dieta_dias SET consolidado = 1 WHERE consolidado = 0 AND fecha BETWEEN ? AND ?").run(d1, d2);
    });
    tx();
    return { desde: d1, hasta: d2, consolidados: dias.length,
             animales: Object.keys(acum).length, costo: Math.round(total * 100) / 100 };
  }

  /* ── SANIDAD ───────────────────────────────────────────────────────────────
     Cada registro de sanidad se valoriza con la lista de precios por producto. */
  function importarSanidad(opts) {
    const o = opts || {};
    const filas = db.prepare(`SELECT s.*, p.costo_dosis FROM sanidad s
      LEFT JOIN costeo_sanidad_precios p ON UPPER(TRIM(p.producto)) = UPPER(TRIM(s.producto))
      WHERE s.fecha >= ?`).all(o.desde || "2000-01-01");
    const ins = db.prepare("INSERT INTO costos (animal_id,fecha,concepto,detalle,monto,moneda) VALUES (?,?,'SANIDAD',?,?,'USD')");
    const res = { revisados: filas.length, cargados: 0, sin_precio: [], costo: 0 };
    const faltantes = new Set();
    const tx = db.transaction(() => {
      for (const f of filas) {
        if (!f.costo_dosis) { if (f.producto) faltantes.add(f.producto); continue; }
        const ya = db.prepare("SELECT 1 FROM costos WHERE animal_id = ? AND detalle = ?")
          .get(f.animal_id, "costeo:sanidad#" + f.id);
        if (ya) continue;
        ins.run(f.animal_id, f.fecha, "costeo:sanidad#" + f.id, f.costo_dosis);
        res.cargados++; res.costo += f.costo_dosis;
      }
    });
    tx();
    res.sin_precio = [...faltantes];
    return res;
  }


  /* ── PRECIOS DESDE IMPROLUX ────────────────────────────────────────────────
     El stock veterinario y el alimento viven en IMPROLUX con su costo. ADE los
     lee y actualiza sus listas de precios. Como no sé la forma exacta del
     endpoint, prueba varias rutas y varios nombres de campo, y reporta lo que
     encontró. Si IMPROLUX cambia, se ajusta acá y nada más. */

  const RUTAS_STOCK = ["/api/stock", "/api/stock/productos", "/api/stock/insumos",
                       "/api/productos", "/api/stock/lista", "/api/stock/todos"];
  const CAMPOS_NOMBRE = ["nombre", "producto", "descripcion", "detalle", "item"];
  const CAMPOS_COSTO  = ["costo_unitario", "costo_promedio", "costo", "precio_unitario",
                         "precio", "costo_por_kg", "costo_kg", "pcp"];
  const CAMPOS_UNIDAD = ["unidad", "um", "medida"];
  const CAMPOS_CANT   = ["cantidad", "stock_actual", "stock", "existencia", "saldo"];

  const primerCampo = (obj, lista) => {
    for (const k of lista) if (obj[k] !== undefined && obj[k] !== null && obj[k] !== "") return obj[k];
    return null;
  };

  function normalizarStock(payload) {
    let arr = payload;
    if (!Array.isArray(arr)) {
      for (const k of ["productos", "stock", "items", "insumos", "data", "resultado"]) {
        if (Array.isArray(payload && payload[k])) { arr = payload[k]; break; }
      }
    }
    if (!Array.isArray(arr)) return [];
    return arr.map(x => {
      const nombre = primerCampo(x, CAMPOS_NOMBRE);
      const costo  = parseFloat(primerCampo(x, CAMPOS_COSTO));
      if (!nombre || !(costo > 0)) return null;
      return { nombre: String(nombre).trim(), costo,
               unidad: primerCampo(x, CAMPOS_UNIDAD) || "",
               rubro: x.rubro || null, categoria: x.categoria || null,
               cantidad: parseFloat(primerCampo(x, CAMPOS_CANT)) || 0 };
    }).filter(Boolean);
  }

  /* IMPROLUX marca el rubro: ALIMENTO / VETERINARIO / AGRICOLA. Si viene, manda
     el rubro; si no, se deduce por el nombre. Lo agrícola no entra al costeo
     animal: la urea no es sanidad ni comida. */
  const clasificarProducto = x => {
    const r = String(x.rubro || "").toUpperCase();
    if (r === "ALIMENTO" || r === "VETERINARIO" || r === "AGRICOLA") return r;
    const t = (x.nombre + " " + (x.categoria || "")).toUpperCase();
    if (/RACION|RACIÓN|MAIZ|MAÍZ|SORGO|EXPELLER|AFRECHILLO|BALANCEADO|PELLET|SILO|FARDO|ROLLO|GRANO|NUCLEO|NÚCLEO|SAL\s*MINERAL/.test(t)) return "ALIMENTO";
    if (/UREA|FERTILIZ|SEMILLA|GLIFOSATO|HERBICID|FUNGICID|INSECTICID|AGROQUIM|FOSFATO|SUPERFOSFATO/.test(t)) return "AGRICOLA";
    return "VETERINARIO";
  };

  async function traerStockImprolux(baseUrl) {
    const base = String(baseUrl).replace(/\/$/, "");
    const errores = [];
    for (const ruta of RUTAS_STOCK) {
      try {
        const r = await fetch(base + ruta, { headers: { Accept: "application/json" } });
        if (!r.ok) { errores.push(`${ruta} → HTTP ${r.status}`); continue; }
        const j = await r.json();
        const items = normalizarStock(j);
        if (items.length) return { ruta, items, errores };
        errores.push(`${ruta} → sin ítems con costo`);
      } catch (e) { errores.push(`${ruta} → ${String(e.message).slice(0, 50)}`); }
    }
    return { ruta: null, items: [], errores };
  }

  async function sincronizarPreciosImprolux(baseUrl) {
    const r = await traerStockImprolux(baseUrl);
    if (!r.items.length)
      throw new Error("No pude leer el stock de IMPROLUX. Rutas probadas: " + r.errores.join(" | "));

    const res = { ruta: r.ruta, leidos: r.items.length, alimentos: 0, sanidad: 0, detalle: [] };

    const upProd = db.prepare(`INSERT INTO costeo_productos (producto,tipo,unidad,costo_unitario,stock_improlux,actualizado)
      VALUES (?,?,?,?,?,datetime('now'))
      ON CONFLICT(producto) DO UPDATE SET tipo = excluded.tipo, unidad = excluded.unidad,
        costo_unitario = excluded.costo_unitario, stock_improlux = excluded.stock_improlux,
        actualizado = datetime('now')`);
    const upSan = db.prepare(`INSERT INTO costeo_sanidad_precios (producto,costo_dosis,notas)
      VALUES (?,?,'desde IMPROLUX')
      ON CONFLICT(producto) DO UPDATE SET costo_dosis = excluded.costo_dosis, notas = 'desde IMPROLUX'`);

    res.agricolas = 0;
    const tx = db.transaction(() => {
      for (const it of r.items) {
        const tipo = clasificarProducto(it);
        upProd.run(it.nombre, tipo, it.unidad || (tipo === "ALIMENTO" ? "kg" : "dosis"),
                   it.costo, it.cantidad || 0);
        if (tipo === "ALIMENTO") res.alimentos++;
        else if (tipo === "VETERINARIO") { upSan.run(it.nombre, it.costo); res.sanidad++; }
        else res.agricolas++;
        res.detalle.push({ nombre: it.nombre, costo: it.costo, stock: it.cantidad, tipo });
      }
    });
    tx();
    return res;
  }


  return { precios, hito, animal, saldo, etapaActual, asiento, cerrarEtapa,
           destetar, desteteHijo, tacto, evaluar18, amortizar, baja,
           cargarCosto, esProductivo, prorratear, borrarProrrateo,
           ficha, ranking, cuadre, backfillPermanencia, moverLote,
           generar, generarTodos, borrarFicha, apertura, clasificar,
           sincronizar, sincronizarAnimal, altaNuevos, cohortes,
           aplicarDietas, diasStock, consolidarRacion, importarSanidad, animalesDelLote,
           sincronizarPreciosImprolux, traerStockImprolux,
           enviarConsumos, productoCache };
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

  app.post("/api/costeo/sync-precios-improlux", async (req, res) => {
    try {
      const url = (req.body && req.body.url) || process.env.IMPROLUX_URL
               || "https://improlux-bot-production.up.railway.app";
      ok(res, await M(req).sincronizarPreciosImprolux(url));
    } catch (e) { err(res, e); }
  });
  app.post("/api/costeo/alimento/compra", (req, res) => {
    try { ok(res, M(req).comprarAlimento(req.body || {})); } catch (e) { err(res, e); }
  });
  app.get("/api/costeo/alimento/resumen", (req, res) => {
    try { ok(res, M(req).resumenAlimento(req.query.desde, req.query.hasta)); } catch (e) { err(res, e); }
  });
  app.get("/api/costeo/productos", async (req, res) => {
    const leer = () => D(req).prepare("SELECT * FROM costeo_productos ORDER BY tipo, producto").all();
    let productos = leer();
    /* Si está vacío o lo piden expreso, se trae de IMPROLUX en el momento.
       Así el desplegable nunca aparece vacío por no haber sincronizado antes. */
    const vacio = !productos.length;
    if (req.query.refrescar === "1" || vacio) {
      try {
        const url = process.env.IMPROLUX_URL || "https://improlux-bot-production.up.railway.app";
        const r = await M(req).sincronizarPreciosImprolux(url);
        return ok(res, { productos: leer(), sincronizado: true, ruta: r.ruta,
                         alimentos: r.alimentos, sanidad: r.sanidad });
      } catch (e) {
        return ok(res, { productos, sincronizado: false, error_sync: e.message });
      }
    }
    ok(res, { productos, sincronizado: false });
  });
  app.post("/api/costeo/enviar-consumos", async (req, res) => {
    try {
      const url = (req.body && req.body.url) || process.env.IMPROLUX_URL
               || "https://improlux-bot-production.up.railway.app";
      ok(res, await M(req).enviarConsumos(url));
    } catch (e) { err(res, e); }
  });
  app.get("/api/costeo/dietas", (req, res) => {
    ok(res, { dietas: D(req).prepare(`SELECT d.*, l.nombre AS lote,
      p.costo_unitario, p.stock_improlux, p.unidad, p.actualizado,
      (SELECT COUNT(*) FROM lote_animales la WHERE la.lote_id = d.lote_id) animales
      FROM costeo_dietas d LEFT JOIN lotes l ON l.id = d.lote_id
      LEFT JOIN costeo_productos p ON UPPER(TRIM(p.producto)) = UPPER(TRIM(d.producto))
      ORDER BY d.activo DESC, d.id DESC`).all() });
  });
  app.post("/api/costeo/dieta", (req, res) => {
    try {
      const b = req.body;
      if (!b.producto) throw new Error("Elegí el producto de IMPROLUX");
      const i = D(req).prepare(`INSERT INTO costeo_dietas (lote_id,producto,modo,kg_dia,fecha_desde,fecha_hasta,notas)
        VALUES (?,?,?,?,?,?,?)`).run(b.lote_id, String(b.producto).trim(), b.modo || "TOTAL",
        parseFloat(b.kg_dia), b.fecha_desde || hoy(), b.fecha_hasta || null, b.notas || null);
      ok(res, { id: Number(i.lastInsertRowid) });
    } catch (e) { err(res, e); }
  });
  app.delete("/api/costeo/dieta/:id", (req, res) => {
    D(req).prepare("UPDATE costeo_dietas SET activo = 0, fecha_hasta = ? WHERE id = ?").run(hoy(), req.params.id);
    ok(res, { mensaje: "Dieta cerrada" });
  });
  app.get("/api/costeo/stock-dias", (req, res) => {
    try { ok(res, { stock: M(req).diasStock() }); } catch (e) { err(res, e); }
  });
  app.post("/api/costeo/consolidar-racion", (req, res) => {
    try { ok(res, M(req).consolidarRacion(req.body.desde, req.body.hasta)); } catch (e) { err(res, e); }
  });
  app.get("/api/costeo/sanidad-precios", (req, res) => {
    ok(res, { precios: D(req).prepare("SELECT * FROM costeo_sanidad_precios ORDER BY producto").all(),
      productos_usados: D(req).prepare(`SELECT s.producto, COUNT(*) n,
        (SELECT costo_dosis FROM costeo_sanidad_precios p WHERE UPPER(TRIM(p.producto))=UPPER(TRIM(s.producto))) costo
        FROM sanidad s WHERE s.producto IS NOT NULL AND s.producto != ''
        GROUP BY UPPER(TRIM(s.producto)) ORDER BY n DESC`).all() });
  });
  app.post("/api/costeo/sanidad-precio", (req, res) => {
    try {
      D(req).prepare(`INSERT INTO costeo_sanidad_precios (producto,costo_dosis,notas) VALUES (?,?,?)
        ON CONFLICT(producto) DO UPDATE SET costo_dosis = excluded.costo_dosis`)
        .run(String(req.body.producto).trim(), parseFloat(req.body.costo_dosis), req.body.notas || null);
      ok(res, { mensaje: "Precio guardado" });
    } catch (e) { err(res, e); }
  });
  app.post("/api/costeo/importar-sanidad", (req, res) => {
    try { ok(res, M(req).importarSanidad(req.body || {})); } catch (e) { err(res, e); }
  });
  app.post("/api/costeo/aplicar-dietas", (req, res) => {
    try { ok(res, M(req).aplicarDietas((req.body || {}).fecha)); } catch (e) { err(res, e); }
  });
  app.get("/api/costeo/cohortes", (req, res) => {
    try { ok(res, M(req).cohortes()); } catch (e) { err(res, e); }
  });
  app.post("/api/costeo/sincronizar", (req, res) => {
    try { ok(res, M(req).sincronizar(req.body || {})); } catch (e) { err(res, e); }
  });
  app.post("/api/costeo/apertura", (req, res) => {
    try { ok(res, M(req).apertura(req.body || {})); } catch (e) { err(res, e); }
  });
  app.post("/api/costeo/generar", (req, res) => {
    try { ok(res, M(req).generar(ref(req.body), req.body)); } catch (e) { err(res, e); }
  });
  app.post("/api/costeo/generar-todos", (req, res) => {
    try { ok(res, M(req).generarTodos(req.body || {})); } catch (e) { err(res, e); }
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
