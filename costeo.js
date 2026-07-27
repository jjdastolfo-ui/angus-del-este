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

const VERSION = "3.13.0";

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

const POTREROS_DEFAULT = [
  ["Natural Amistad",   "NATURAL",  35, "ANUAL",     0,   0,  0, "Campo natural"],
  ["Pasturas Amistad",  "PASTURA",  85, "ANUAL",     5, 350, 50, "Implantación US$350/ha en 5 años + US$50/ha/año de fertilización y malezas"],
  ["Verdeos Amistad",   "VERDEO",   85, "SEMESTRAL", 1, 150,  0, "US$150/ha cada año — se resiembra"],
  ["Corrales Amistad",  "CORRAL",    0, "ANUAL",     0,   0,  0, "Sin renta — el costo es la ración"],
  ["Isla eucaliptus",   "FORESTAL",  7, "ANUAL",     0,   0,  0, "Campo natural forestal"]
];

/* Vida útil de la inversión en pasto, por tipo. El verdeo se resiembra todos
   los años: su siembra es costo del año, no se estira. La pastura perenne dura
   unos 5 años y ahí sí corresponde amortizar. Lo natural no lleva inversión. */
const VIDA_UTIL_TIPO = { VERDEO: 1, PASTURA: 5, NATURAL: 0, CORRAL: 0, FORESTAL: 0 };
const vidaUtilDe = tipo => VIDA_UTIL_TIPO[String(tipo || "").toUpperCase()] ?? 1;

/* Costos de referencia del pasto, en US$/ha. La implantación se amortiza en la
   vida útil; el mantenimiento (fertilización y malezas) es del año y no se estira. */
const PASTO_REF = {
  VERDEO:  { implantacion: 150, mantenimiento: 0 },
  PASTURA: { implantacion: 350, mantenimiento: 50 },
  NATURAL: { implantacion: 0,   mantenimiento: 0 },
  CORRAL:  { implantacion: 0,   mantenimiento: 0 },
  FORESTAL:{ implantacion: 0,   mantenimiento: 0 }
};

/* UG = vaca de cría con ternero al pie. El resto en proporción. */
const UG_DEFAULT = [
  ["VACA",       1.20, "Con ternero al pie"],
  ["VAQUILLONA", 0.90, "De 2 a 3 años"],
  ["RECRIA",     0.70, "De 1 a 2 años"],
  ["TERNERO",    0.50, "Destetado"],
  ["NOVILLO",    1.00, "De 2 a 3 años"],
  ["TORO",       1.30, "Adulto"]
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
      fecha TEXT NOT NULL, animales INTEGER, ug REAL, kg REAL, costo_kg REAL, costo_total REAL,
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

    -- ══════════════════════════════════════════════════════════════════════
    -- TIERRA: el lote de IMPROLUX es la unidad. No hay "tipos de potrero":
    -- hay lotes concretos, con su superficie, su renta y lo que tengan
    -- sembrado arriba según las órdenes de trabajo.
    -- ══════════════════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS costeo_lotes_campo (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      improlux_id INTEGER UNIQUE,           -- id del lote en IMPROLUX: es el vínculo real
      nombre TEXT NOT NULL UNIQUE,          -- se sincroniza desde IMPROLUX
      nombres_previos TEXT,                 -- para que las órdenes viejas sigan matcheando
      ha_totales REAL DEFAULT 0,            -- superficie del lote
      ha_sembrables REAL DEFAULT 0,         -- referencia, no se usa para el piso
      kg_ha_renta REAL DEFAULT 35,          -- ISLA FORESTAL va en 7 por la forestación
      tipo_actual TEXT DEFAULT 'NATURAL',   -- NATURAL / PASTURA / VERDEO / CORRAL / AGRICULTURA
      sector TEXT DEFAULT 'CRIA',           -- sector dominante, para mostrar
      pct_cria REAL DEFAULT 0,              -- un lote puede repartirse entre dos
      pct_recria REAL DEFAULT 0,
      pct_corral REAL DEFAULT 0,
      activo INTEGER DEFAULT 1,
      notas TEXT
    );

    -- Un lote puede prestarse a otro sector por un período
    CREATE TABLE IF NOT EXISTS costeo_lote_sector (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lote_campo_id INTEGER NOT NULL,
      sector TEXT NOT NULL,
      pct_cria REAL, pct_recria REAL, pct_corral REAL,
      fecha_desde TEXT NOT NULL,
      fecha_hasta TEXT,
      activo INTEGER DEFAULT 1,
      notas TEXT
    );

    -- Lo que se sembró o aplicó, sacado de las órdenes de trabajo
    CREATE TABLE IF NOT EXISTS costeo_implantaciones (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lote_campo_id INTEGER NOT NULL,
      tipo TEXT NOT NULL,                   -- PASTURA / VERDEO / FERTILIZACION / CONTROL
      anio INTEGER NOT NULL,
      costo_total REAL DEFAULT 0,
      ha REAL DEFAULT 0,
      vida_util REAL DEFAULT 1,             -- pastura 5 · el resto 1
      orden_improlux INTEGER,
      notas TEXT
    );

    -- Los lotes de ADE que son corral: ahí el animal no sale hasta la venta
    CREATE TABLE IF NOT EXISTS costeo_sector_ade (
      lote_id INTEGER PRIMARY KEY,
      sector TEXT NOT NULL
    );

    -- Devengo diario por sector
    CREATE TABLE IF NOT EXISTS costeo_sector_dias (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fecha TEXT NOT NULL, sector TEXT NOT NULL,
      ha REAL, costo_dia REAL, animales INTEGER, ug REAL,
      consolidado INTEGER DEFAULT 0,
      UNIQUE(fecha, sector)
    );

    -- Ventas y bajas. Una fila por operación y una por animal, para poder
    -- ver el resultado del remate completo y no solo animal por animal.
    CREATE TABLE IF NOT EXISTS costeo_ventas (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fecha TEXT NOT NULL,
      tipo TEXT NOT NULL,              -- REMATE / VENTA_GORDA / VENTA / REFUGO / MUERTE
      comprador TEXT, detalle TEXT,
      animales INTEGER DEFAULT 0,
      importe_total REAL DEFAULT 0,
      kgne_venta REAL DEFAULT 0,
      kgne_produccion REAL DEFAULT 0,
      costo_acumulado REAL DEFAULT 0,
      novillo_pie REAL,
      ciclo TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS costeo_venta_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      venta_id INTEGER NOT NULL,
      animal_id INTEGER NOT NULL, rp TEXT, chip TEXT,
      categoria TEXT, registro TEXT, sexo TEXT,
      peso REAL, importe REAL,
      saldo_previo REAL, kgne_venta REAL, kgne_produccion REAL,
      costo_acumulado REAL, margen REAL
    );

    CREATE TABLE IF NOT EXISTS costeo_config (
      clave TEXT PRIMARY KEY, valor TEXT
    );

    -- Potreros: la renta se expresa en kg de carne por hectárea, que es como
    -- se habla el arrendamiento acá. Al estar en kg, se convierte a kgNE sola.
    CREATE TABLE IF NOT EXISTS costeo_potreros (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      nombre TEXT NOT NULL UNIQUE,
      tipo TEXT DEFAULT 'NATURAL',          -- NATURAL / PASTURA / VERDEO / CORRAL / FORESTAL
      kg_carne_ha REAL DEFAULT 0,           -- kg de carne por hectárea y período
      periodo TEXT DEFAULT 'ANUAL',         -- ANUAL / SEMESTRAL
      ha_totales REAL DEFAULT 0,
      ha_aprovechables REAL DEFAULT 0,      -- las que realmente pastorean
      inversion_ha REAL DEFAULT 0,          -- US$/ha de implantación, se amortiza
      mantenimiento_ha REAL DEFAULT 0,      -- US$/ha/año de fertilización y malezas, NO se amortiza
      vida_util_anios REAL DEFAULT 0,       -- verdeo 1 · pastura perenne 5 · natural 0
      lote_improlux TEXT,                   -- nombre del lote en IMPROLUX, si mapea
      activo INTEGER DEFAULT 1,
      notas TEXT
    );

    -- Qué lote de animales está en qué potrero y desde cuándo
    CREATE TABLE IF NOT EXISTS costeo_lote_potrero (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      lote_id INTEGER NOT NULL,
      potrero_id INTEGER NOT NULL,
      fecha_desde TEXT NOT NULL,
      fecha_hasta TEXT,
      activo INTEGER DEFAULT 1
    );

    -- Renglón por potrero y día: cuánto costó y entre cuántos se reparte
    CREATE TABLE IF NOT EXISTS costeo_potrero_dias (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      asignacion_id INTEGER NOT NULL, potrero_id INTEGER, lote_id INTEGER,
      fecha TEXT NOT NULL, animales INTEGER, ug REAL, ha REAL,
      kgne_dia REAL, novillo_pie REAL, costo_total REAL,
      ocioso INTEGER DEFAULT 0,
      consolidado INTEGER DEFAULT 0,
      UNIQUE(asignacion_id, fecha)
    );

    -- Unidad ganadera: cuánto come cada categoría respecto de una vaca con cría.
    -- Sin esto, un ternero destetado paga el mismo piso que una vaca. Editable.
    -- Gastos de la empresa, no de la tierra: sueldos, admin, camioneta, impuestos.
    -- No dependen de en qué potrero está el animal, así que se reparten por UG.
    CREATE TABLE IF NOT EXISTS costeo_estructura (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      concepto TEXT NOT NULL,
      categoria TEXT DEFAULT 'ESTRUCTURA',  -- ESTRUCTURA / ADMIN / MANO_OBRA / IMPUESTOS / OTRO
      monto_anual REAL NOT NULL,
      pct_ganaderia REAL DEFAULT 100,       -- si el gasto se comparte con agricultura
      activo INTEGER DEFAULT 1,
      notas TEXT
    );

    CREATE TABLE IF NOT EXISTS costeo_estructura_dias (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fecha TEXT NOT NULL UNIQUE,
      monto_dia REAL, ug_campo REAL, consolidado INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS costeo_ug (
      categoria TEXT PRIMARY KEY,
      coeficiente REAL NOT NULL,
      nota TEXT
    );

    CREATE INDEX IF NOT EXISTS ix_ckgne_animal ON costeo_kgne(animal_id, fecha);
    CREATE INDEX IF NOT EXISTS ix_ckgne_ciclo  ON costeo_kgne(ciclo);
    CREATE INDEX IF NOT EXISTS ix_cperm_animal ON costeo_permanencia(animal_id);
    CREATE INDEX IF NOT EXISTS ix_cetap_animal ON costeo_etapas(animal_id);
  `);

  /* Migraciones para bases que ya venían de versiones anteriores.
     Cada una en su try: si la columna ya está, sigue de largo. */
  ["ALTER TABLE costeo_kgne ADD COLUMN origen_id INTEGER",
   "ALTER TABLE costeo_dietas ADD COLUMN producto TEXT",
   "ALTER TABLE costeo_dieta_dias ADD COLUMN producto TEXT",
   "ALTER TABLE costeo_dieta_dias ADD COLUMN enviado INTEGER DEFAULT 0",
   "ALTER TABLE costeo_dieta_dias ADD COLUMN error_envio TEXT",
   "ALTER TABLE costeo_dieta_dias ADD COLUMN ug REAL",
   "ALTER TABLE costeo_potrero_dias ADD COLUMN ug REAL",
   "ALTER TABLE costeo_potrero_dias ADD COLUMN ocioso INTEGER DEFAULT 0",
   "ALTER TABLE costeo_potreros ADD COLUMN inversion_ha REAL DEFAULT 0",
   "ALTER TABLE costeo_potreros ADD COLUMN vida_util_anios REAL DEFAULT 0",
   "ALTER TABLE costeo_potreros ADD COLUMN mantenimiento_ha REAL DEFAULT 0",
   "ALTER TABLE costeo_lotes_campo ADD COLUMN pct_cria REAL DEFAULT 0",
   "ALTER TABLE costeo_lotes_campo ADD COLUMN pct_recria REAL DEFAULT 0",
   "ALTER TABLE costeo_lotes_campo ADD COLUMN pct_corral REAL DEFAULT 0",
   "ALTER TABLE costeo_lote_sector ADD COLUMN pct_cria REAL",
   "ALTER TABLE costeo_lote_sector ADD COLUMN pct_recria REAL",
   "ALTER TABLE costeo_lote_sector ADD COLUMN pct_corral REAL",
   "ALTER TABLE costeo_lotes_campo ADD COLUMN tipo_actual TEXT DEFAULT 'NATURAL'",
   "ALTER TABLE costeo_lotes_campo ADD COLUMN improlux_id INTEGER",
   "ALTER TABLE costeo_lotes_campo ADD COLUMN nombres_previos TEXT"
  ].forEach(q => { try { db.exec(q); } catch (e) {} });

  const h = db.prepare("INSERT OR IGNORE INTO costeo_hitos (ciclo,hito,kgne,nota) VALUES ('TODOS',?,?,?)");
  HITOS_DEFAULT.forEach(x => h.run(x[0], x[1], x[2]));
  db.prepare("INSERT OR IGNORE INTO costeo_config (clave,valor) VALUES ('reparto_tierra','CABEZA')").run();
  db.prepare("INSERT OR IGNORE INTO costeo_config (clave,valor) VALUES ('meses_recria','30')").run();
  db.prepare("INSERT OR IGNORE INTO costeo_config (clave,valor) VALUES ('kg_ha_default','35')").run();

  /* Los lotes que venían con sector único pasan a 100% en ese sector */
  try {
    db.prepare(`UPDATE costeo_lotes_campo
      SET pct_cria   = CASE WHEN sector='CRIA'   THEN 100 ELSE 0 END,
          pct_recria = CASE WHEN sector='RECRIA' THEN 100 ELSE 0 END,
          pct_corral = CASE WHEN sector='CORRAL' THEN 100 ELSE 0 END
      WHERE COALESCE(pct_cria,0)+COALESCE(pct_recria,0)+COALESCE(pct_corral,0) = 0
        AND sector != 'EXCLUIDO'`).run();
  } catch (e) {}

  const ug = db.prepare("INSERT OR IGNORE INTO costeo_ug (categoria,coeficiente,nota) VALUES (?,?,?)");
  UG_DEFAULT.forEach(x => ug.run(x[0], x[1], x[2]));

  const pot = db.prepare(`INSERT OR IGNORE INTO costeo_potreros
    (nombre,tipo,kg_carne_ha,periodo,vida_util_anios,inversion_ha,mantenimiento_ha,notas)
    VALUES (?,?,?,?,?,?,?,?)`);
  POTREROS_DEFAULT.forEach(x => pot.run(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7]));

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

  /* Acepta animal_id, chip o RP.
     Los RP se reusan: si hay un animal de baja y uno activo con el mismo RP,
     gana el activo. El de baja es solo el registro de que estuvo. */
  const ORDEN = " ORDER BY (estado = 'ACTIVO') DESC, id DESC";
  function animal(ref) {
    if (ref === undefined || ref === null || ref === "") return null;
    const s = String(ref).trim();
    /* El chip es único, así que si viene chip no hay ambigüedad posible */
    let a = db.prepare("SELECT * FROM animales WHERE chip = ?").get(s);
    if (a) return a;
    a = db.prepare("SELECT * FROM animales WHERE rp = ?" + ORDEN).get(s);
    if (a) return a;
    if (/^\d+$/.test(s)) {
      a = db.prepare("SELECT * FROM animales WHERE id = ?").get(parseInt(s, 10));
      if (a) return a;
    }
    if (s.length > 9) {
      a = db.prepare("SELECT * FROM animales WHERE chip LIKE ?" + ORDEN).get("%" + s.slice(-9));
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
    /* El alta puede venir del pase a plantel (APERTURA) o del inventario
       (APERTURA_INVENTARIO). Antes solo miraba el primero y por eso los
       animales abiertos por inventario nunca amortizaban. */
    const alta = db.prepare(`SELECT * FROM costeo_kgne
      WHERE animal_id = ? AND etapa = 'PLANTEL' AND origen IN ('APERTURA','APERTURA_INVENTARIO')
      ORDER BY fecha, id LIMIT 1`).get(a.id);
    if (!alta) return { anual: 0, nuevo: 0, nota: "No está en plantel" };

    const ya = db.prepare("SELECT COALESCE(SUM(ABS(kgne_produccion)),0) t FROM costeo_kgne WHERE animal_id = ? AND tipo = 'AMORT'").get(a.id).t;
    const macho = esM(a.sexo), ciclo = cicloDe(hasta);
    const residual = hito(macho ? "REFUGO_TORO" : "REFUGO_VACA", ciclo);
    const vidaTot  = hito(macho ? "VIDA_TORO"  : "VIDA_VACA",   ciclo);
    /* La vaca ya parió una vez en la intermedia: le quedan vidaTot − 1 en plantel */
    const vidaPlena = macho ? vidaTot : Math.max(1, vidaTot - 1);

    /* Si entró por inventario ya venía usada: el saldo de alta trae descontados
       los años vividos. Amortizar sobre la vida completa daría una cuota
       demasiado chica, así que se reparte en la vida que le QUEDA. */
    let vida = vidaPlena;
    if (a.fecha_nac) {
      const mesesAlta = macho ? 24 : 30;
      const yaVividos = Math.max(0, anios(a.fecha_nac, alta.fecha) - mesesAlta / 12);
      vida = Math.max(0.5, vidaPlena - yaVividos);
    }
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
    if (cat === "TORO") {
      /* "Toro" es la categoría desde que nace macho de cabaña. Lo que define
         dónde está es la edad y el destino: a los 22 meses camino al remate
         está en preparación, no sirviendo vacas. */
      const dest = String(a.destino || "").toUpperCase();
      if (meses !== null && meses < 24) return { etapa: "RECRIA", modo: "PESO", coef: "NOVILLO" };
      if (dest.includes("VENTA")) return { etapa: "PREPARACION", modo: "PESO", coef: "NOVILLO" };
      if (dest.includes("DESCARTE")) return { etapa: "TERMINACION", modo: "PESO", coef: "NOVILLO" };
      return { etapa: "PLANTEL", modo: "PESO", coef: "NOVILLO" };
    }
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

    /* El destetado del ciclo NO entra por inventario: entra por su destete, que
       es su entrada real y ya está cargado con fecha y peso. Así la camada del
       año queda con historia completa aunque el resto del rodeo abra hoy.
       Con incluir_destetados=true se fuerza a que entren igual. */
    const cicloIni = o.desde_destete || rangoCiclo(cicloDe(fecha))[0];
    if (!o.incluir_destetados) {
      sql += ` AND NOT EXISTS (SELECT 1 FROM pesadas p WHERE p.animal_id = animales.id
                 AND UPPER(COALESCE(p.contexto,'')) = 'DESTETE' AND p.fecha >= ?)`;
      args.push(cicloIni);
    }
    sql += " ORDER BY id";
    const animales = db.prepare(sql).all(...args);

    const reservados = o.incluir_destetados ? 0 : db.prepare(`SELECT COUNT(DISTINCT a.id) n
      FROM animales a JOIN pesadas p ON p.animal_id = a.id
      WHERE a.estado = 'ACTIVO' AND UPPER(COALESCE(p.contexto,'')) = 'DESTETE' AND p.fecha >= ?
        AND NOT EXISTS (SELECT 1 FROM costeo_kgne k WHERE k.animal_id = a.id)`).get(cicloIni).n;

    const res = { fecha, total: animales.length, abiertos: 0, yaTenian: 0,
                  estimados: 0, kgne_total: 0, por_categoria: {}, detalle: [],
                  por_destete: reservados, ciclo_desde: cicloIni };

    for (const a of animales) {
      const ya = db.prepare("SELECT 1 FROM costeo_kgne WHERE animal_id = ? LIMIT 1").get(a.id);
      if (ya && !o.forzar) { res.yaTenian++; continue; }

      const c = clasificar(a, fecha);
      let kgne, detalle, estimado = 0;

      if (c.modo === "HITO") {
        kgne = hito(c.hito, cicloDe(fecha));
        detalle = "hito " + c.hito + " · " + kgne + " kgNE";

        /* La vaca de 10 años no vale lo mismo que la de 4: ya consumió parte
           de su vida útil. Se le descuenta la amortización de los años que
           lleva en el plantel, con piso en el valor de refugo. */
        /* Sin fecha de nacimiento no hay edad, y sin edad no se puede descontar
           nada: la vaca entra al valor pleno. Se cuenta aparte para avisarlo. */
        if (c.etapa === "PLANTEL" && !a.fecha_nac) {
          res.sin_edad = (res.sin_edad || 0) + 1;
          (res.sin_edad_rp = res.sin_edad_rp || []).push(a.rp);
        }
        if (c.etapa === "PLANTEL" && a.fecha_nac) {
          const macho = esM(a.sexo);
          const residual = hito(macho ? "REFUGO_TORO" : "REFUGO_VACA", cicloDe(fecha));
          const vidaTot  = hito(macho ? "VIDA_TORO"  : "VIDA_VACA",   cicloDe(fecha));
          const vida = macho ? vidaTot : Math.max(1, vidaTot - 1);
          const mesesAlta = macho ? 24 : 30;          // edad de ingreso al plantel
          const aniosPlantel = Math.max(0, anios(a.fecha_nac, fecha) - mesesAlta / 12);
          if (residual != null && vida > 0 && aniosPlantel > 0) {
            const anual = (kgne - residual) / vida;
            const gastado = Math.min(anual * aniosPlantel, kgne - residual);
            const bruto = kgne;
            kgne = Math.max(residual, kgne - gastado);
            detalle = `hito ${bruto} - ${Math.round(gastado)} de amortización` +
                      ` (${aniosPlantel.toFixed(1)} años en plantel) = ${Math.round(kgne)} kgNE`;
            res.amortizado = (res.amortizado || 0) + gastado;
          }
        }
      } else {
        /* El peso que tenía A LA FECHA DE CORTE, no el de hoy: si la apertura
           se fecha al 1 de marzo, lo que engordó desde entonces es producción
           del ciclo y la levanta el sincronizador con las pesadas siguientes. */
        const pe = db.prepare(`SELECT peso, fecha FROM pesadas
          WHERE animal_id = ? AND fecha <= ? ORDER BY fecha DESC LIMIT 1`).get(a.id, fecha)
          || db.prepare("SELECT peso, fecha FROM pesadas WHERE animal_id = ? ORDER BY fecha ASC LIMIT 1").get(a.id);
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
      if (c.etapa === "PLANTEL" && a.fecha_nac) {
        res.por_categoria[k].edad_prom = (res.por_categoria[k].edad_prom || 0)
          + anios(a.fecha_nac, fecha);
      }
      if (estimado) res.por_categoria[k].estimados++;
      res.kgne_total += kgne;

      if (!o.dryRun) {
        /* Reabrir borra los asientos kgNE y las etapas, NO los costos ya
           cargados. Después hay que volver a sincronizar para que se
           reconstruyan tactos, destetes y pesadas. */
        if (ya) borrarFicha(a.id);
        asiento(a, { fecha, etapa: c.etapa, tipo: "TRANS", origen: "APERTURA_INVENTARIO",
          concepto: "Apertura de inventario · " + ETAPAS[c.etapa].label,
          detalle, kgne, transferencia: 1, estimado });
      }
      res.abiertos++;
      if (o.detallado) res.detalle.push({ rp: a.rp, categoria: a.categoria,
        etapa: c.etapa, kgne: Math.round(kgne), estimado,
        edad: a.fecha_nac ? Math.round(anios(a.fecha_nac, fecha) * 10) / 10 : null });
    }

    Object.values(res.por_categoria).forEach(v => {
      if (v.edad_prom) v.edad_prom = Math.round(v.edad_prom / v.n * 10) / 10;
    });
    res.usd_total = res.kgne_total * p.novillo_pie;
    res.amortizado = Math.round((res.amortizado || 0) * 10) / 10;
    res.sin_edad = res.sin_edad || 0;
    if (res.sin_edad_rp) res.sin_edad_rp = res.sin_edad_rp.slice(0, 40);
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

      /* destete de la cría, si el peso vino cargado en el servicio */
      if (sv.fecha_parto && sv.peso_destete > 0 && !yaProcesado(a.id, "DESTETE_HIJO", sv.id)) {
        const fd = new Date(new Date(sv.fecha_parto).getTime() + 180 * 86400000).toISOString().slice(0, 10);
        desteteHijo(a.id, fd, sv.peso_destete, sv.sexo_cria || "MACHO", sv.ternero_rp, sv.id);
        log.push(`desteta cria ${fd} - ${sv.peso_destete} kg`);
      }
    }

    /* DESTETES POR LA PESADA DEL TERNERO
       El destete casi siempre entra con Gallagher como pesada del ternero, no
       como peso_destete del servicio. Sin esto la vaca acumula piso y racion y
       nunca cobra el ternero: da negativo para siempre.
       El vinculo es animales.madre_rp. */
    if (hembra && a.rp) {
      const desdeD = o.desde_destetes || rangoCiclo(cicloDe(primero.fecha))[0];
      const crias = db.prepare(`SELECT a2.id, a2.rp, a2.sexo, p.id pesada_id, p.fecha, p.peso
        FROM animales a2
        JOIN pesadas p ON p.animal_id = a2.id AND UPPER(COALESCE(p.contexto,'')) = 'DESTETE'
        WHERE a2.madre_rp = ? AND p.fecha >= ? AND p.peso > 0
        ORDER BY p.fecha`).all(a.rp, desdeD);

      for (const c of crias) {
        /* No duplicar: puede haber venido por el servicio. Se descarta si ya
           hay un destete de esa misma cria, o uno a menos de 90 dias. */
        const porCria = db.prepare(`SELECT 1 FROM costeo_kgne
          WHERE animal_id = ? AND origen LIKE 'DESTETE%' AND ref_id = ? LIMIT 1`).get(a.id, c.id);
        if (porCria) continue;
        if (yaProcesado(a.id, "DESTETE_CRIA", c.pesada_id)) continue;
        const cerca = db.prepare(`SELECT 1 FROM costeo_kgne
          WHERE animal_id = ? AND origen IN ('DESTETE_HIJO','DESTETE_CRIA')
            AND ABS(julianday(fecha) - julianday(?)) < 90 LIMIT 1`).get(a.id, c.fecha);
        if (cerca) continue;

        const pr = precios();
        const coef = esM(c.sexo) ? pr.coef.TERNERO : pr.coef.TERNERA;
        asiento(a, { fecha: c.fecha, etapa: etapaActual(a.id), tipo: "PROD",
          origen: "DESTETE_CRIA",
          concepto: "Desteta " + (esM(c.sexo) ? "ternero" : "ternera") + " RP " + c.rp,
          detalle: c.peso + " kg x " + coef.toFixed(3),
          kg_fisicos: c.peso, coeficiente: coef, kgne: c.peso * coef,
          ref_id: c.id, origen_id: c.pesada_id });
        log.push(`desteta ${c.rp} ${c.fecha} - ${c.peso} kg`);
      }
    }

    /* ── PESADAS ───────────────────────────────────────────────────────────
       En las etapas que se miden por balanza, cada pesada actualiza el saldo:
       el macho de recría y la hembra que todavía no se preñó crecen todo el
       año y eso es producción. En intermedia y plantel NO: ahí manda el hito
       reproductivo y el peso no revalúa.
       Solo se toman las pesadas posteriores al último movimiento de saldo,
       para no desordenar la cuenta. */
    const ETAPAS_PESO = new Set(["RECRIA", "ENGORDE", "TERMINACION", "PREPARACION"]);
    const etHoy = etapaActual(a.id);
    if (ETAPAS_PESO.has(etHoy)) {
      const ult = db.prepare(`SELECT fecha FROM costeo_kgne WHERE animal_id = ?
        AND tipo IN ('TRANS','REVAL','AMORT','BAJA') ORDER BY fecha DESC, id DESC LIMIT 1`).get(a.id);
      const desdeP = ult ? ult.fecha : corte;
      const pesadas = db.prepare(`SELECT * FROM pesadas WHERE animal_id = ?
        AND fecha >= ? AND peso > 0
        AND UPPER(COALESCE(contexto,'')) != 'DESTETE'
        ORDER BY fecha, id`).all(a.id, desdeP);

      for (const pe of pesadas) {
        if (yaProcesado(a.id, "PESADA", pe.id)) continue;
        const antes = saldo(a.id);
        /* En el campo el animal vale sus kg: el premio de categoría se
           reconoce solo al destete y en la venta. */
        asiento(a, { fecha: pe.fecha, etapa: etapaActual(a.id), tipo: "REVAL",
          origen: "PESADA",
          concepto: "Pesada" + (pe.contexto ? " " + pe.contexto : ""),
          detalle: pe.peso + " kg" + (pe.gdp ? " - GDP " + pe.gdp : ""),
          kg_fisicos: pe.peso, coeficiente: 1, kgne: pe.peso, origen_id: pe.id });
        const dif = pe.peso - antes;
        log.push(`pesada ${pe.fecha} - ${pe.peso} kg (${dif >= 0 ? "+" : ""}${Math.round(dif)} kgNE)`);
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
                  actualizados: 0, asientos: 0, destetes: 0, pesadas: 0, detalle: [] };
    for (const a of animales) {
      try {
        const log = sincronizarAnimal(a, o);
        if (log.length) {
          res.actualizados++; res.asientos += log.length;
          res.destetes += log.filter(x => x.indexOf("desteta") === 0).length;
          res.pesadas = (res.pesadas || 0) + log.filter(x => x.indexOf("pesada") === 0).length;
          if (o.detallado) res.detalle.push({ rp: a.rp, cambios: log });
        }
      } catch (e) { res.detalle.push({ rp: a.rp, error: e.message }); }
    }
    /* ración del día y sanidad valorizada */
    if (o.dietas !== false) { try { res.dietas = aplicarDietas(); } catch (e) {} }
    if (o.tierra !== false) { try { res.tierra = aplicarSectores(); } catch (e) {} }
    if (o.estructura !== false) { try { res.estructura = aplicarEstructura(); } catch (e) {} }
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

  /* ── UNIDADES GANADERAS ────────────────────────────────────────────────────
     El piso y la ración se reparten por UG, no por cabeza: una vaca con cría
     come más del doble que un ternero destetado y tiene que pagar en esa
     proporción. Repartir en partes iguales infla el costo de la recría y
     abarata la cría, que es justo el error que arruina la comparación. */
  let _ugCache = null;
  function tablaUG() {
    if (!_ugCache) {
      _ugCache = {};
      db.prepare("SELECT * FROM costeo_ug").all().forEach(x => { _ugCache[x.categoria.toUpperCase()] = x.coeficiente; });
    }
    return _ugCache;
  }
  const invalidarUG = () => { _ugCache = null; };
  const ugDe = cat => tablaUG()[String(cat || "").toUpperCase()] || 1;

  /* UG de un lote, por los animales que tiene adentro hoy */
  function ugDelLote(loteId) {
    const r = db.prepare(`SELECT a.categoria, COUNT(*) n FROM lote_animales la
      JOIN animales a ON a.id = la.animal_id
      WHERE la.lote_id = ? AND a.estado = 'ACTIVO' GROUP BY a.categoria`).all(loteId);
    return r.reduce((s, x) => s + x.n * ugDe(x.categoria), 0);
  }

  /* Reparte un monto entre animales, ponderado por UG */
  function repartirPorUG(ids, monto) {
    if (!ids.length || !monto) return {};
    const cats = {};
    db.prepare(`SELECT id, categoria FROM animales WHERE id IN (${ids.map(() => "?").join(",")})`)
      .all(...ids).forEach(a => { cats[a.id] = ugDe(a.categoria); });
    const total = ids.reduce((s, id) => s + (cats[id] || 1), 0);
    if (!total) return {};
    const out = {};
    ids.forEach(id => { out[id] = monto * (cats[id] || 1) / total; });
    return out;
  }

  /* UG de todo el campo, para repartir lo que no cae en un potrero puntual */
  function ugDelCampo() {
    const r = db.prepare(`SELECT categoria, COUNT(*) n FROM animales
      WHERE estado = 'ACTIVO' GROUP BY categoria`).all();
    return r.reduce((s, x) => s + x.n * ugDe(x.categoria), 0);
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

      db.prepare(`INSERT INTO costeo_dieta_dias (dieta_id,lote_id,producto,fecha,animales,ug,kg,costo_kg,costo_total)
        VALUES (?,?,?,?,?,?,?,?,?)`).run(d.id, d.lote_id, d.producto, f, n, ugDelLote(d.lote_id), kg, costoKg, costo);
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
      const rep = repartirPorUG(lista, x.costo_total);
      for (const [id, m] of Object.entries(rep)) acum[id] = (acum[id] || 0) + m;
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



  /* ── POTREROS ──────────────────────────────────────────────────────────────
     La renta viene en kg de carne por hectárea y año. Se prorratea por día y
     se reparte entre los animales que están pastoreando ese potrero. */

  /* Renta base del potrero, en kg de carne por hectárea y año */
  const kgRentaHa = p => (p.periodo === "SEMESTRAL" ? p.kg_carne_ha * 2 : p.kg_carne_ha);

  /* La siembra de una pradera dura varios años: se amortiza y se suma al kg/ha
     de ESE potrero. Cargarla entera al año de la siembra le pega todo el costo
     al lote que pastoreó primero y le deja el resto gratis a los que siguen. */
  /* Costo anual del pasto por hectárea, en US$:
       implantación ÷ vida útil   (se amortiza: la pastura dura 5 años)
     + mantenimiento del año      (fertilización y malezas: no se amortiza) */
  function usdPastoHa(p) {
    const inv = p.inversion_ha || 0;
    const vida = p.vida_util_anios > 0 ? p.vida_util_anios : vidaUtilDe(p.tipo);
    const amort = (inv > 0 && vida > 0) ? inv / vida : 0;
    return amort + (p.mantenimiento_ha || 0);
  }

  /* Lo mismo expresado en kg de carne, para sumarlo a la renta */
  function kgAmortHa(p, nov) {
    if (!(nov > 0)) return 0;
    const usd = usdPastoHa(p);
    return usd > 0 ? usd / nov : 0;
  }

  /* kg/ha efectivo = renta + amortización de la inversión */
  const kgAnualHa = (p, nov) => kgRentaHa(p) + kgAmortHa(p, nov || (precios() || {}).novillo_pie || 0);

  function asignarPotrero(loteId, potreroId, fecha) {
    const f = fecha || hoy();
    db.prepare("UPDATE costeo_lote_potrero SET activo = 0, fecha_hasta = ? WHERE lote_id = ? AND activo = 1").run(f, loteId);
    const i = db.prepare(`INSERT INTO costeo_lote_potrero (lote_id,potrero_id,fecha_desde) VALUES (?,?,?)`)
      .run(loteId, potreroId, f);
    return { id: Number(i.lastInsertRowid) };
  }

  function sacarDePotrero(loteId, fecha) {
    const r = db.prepare("UPDATE costeo_lote_potrero SET activo = 0, fecha_hasta = ? WHERE lote_id = ? AND activo = 1")
      .run(fecha || hoy(), loteId);
    return { cerradas: r.changes };
  }

  /* Devenga el día para cada lote que está pastoreando */
  function aplicarPotreros(fecha) {
    const f = (fecha || hoy()).slice(0, 10);
    const p = precios();
    const nov = p ? p.novillo_pie : 0;
    const asig = db.prepare(`SELECT a.*, po.nombre, po.tipo, po.kg_carne_ha, po.periodo,
        po.ha_aprovechables, po.ha_totales
      FROM costeo_lote_potrero a JOIN costeo_potreros po ON po.id = a.potrero_id
      WHERE a.activo = 1 AND a.fecha_desde <= ? AND (a.fecha_hasta IS NULL OR a.fecha_hasta >= ?)
        AND po.activo = 1`).all(f, f);

    const res = { fecha: f, asignaciones: asig.length, aplicadas: 0, kgne: 0, costo: 0,
                  ociosos: 0, kgne_ocioso: 0, costo_ocioso: 0, ha_ociosa: 0, detalle: [] };
    const ins = db.prepare(`INSERT INTO costeo_potrero_dias
      (asignacion_id,potrero_id,lote_id,fecha,animales,ug,ha,kgne_dia,novillo_pie,costo_total,ocioso)
      VALUES (?,?,?,?,?,?,?,?,?,?,?)`);

    const ocupados = new Set();
    for (const a of asig) {
      if (db.prepare("SELECT 1 FROM costeo_potrero_dias WHERE asignacion_id = ? AND fecha = ?").get(a.id, f)) {
        ocupados.add(a.potrero_id); continue;
      }
      const n = animalesDelLote(a.lote_id).length;
      if (!n) continue;
      ocupados.add(a.potrero_id);
      const ha = a.ha_aprovechables || a.ha_totales || 0;
      const kgneDia = ha * kgAnualHa(a, nov) / 365;
      const costo = kgneDia * nov;
      ins.run(a.id, a.potrero_id, a.lote_id, f, n, ugDelLote(a.lote_id), ha, kgneDia, nov, costo, 0);
      res.aplicadas++; res.kgne += kgneDia; res.costo += costo;
      res.detalle.push({ potrero: a.nombre, lote_id: a.lote_id, animales: n, ha,
        kgne_dia: Math.round(kgneDia * 100) / 100, costo: Math.round(costo * 100) / 100 });
    }

    /* Potreros en descanso: la renta se paga igual. El potrero descansa PARA el
       rodeo, así que su costo es del rodeo entero, repartido por UG. */
    const libres = db.prepare(`SELECT * FROM costeo_potreros
      WHERE activo = 1 AND kg_carne_ha > 0
        AND COALESCE(ha_aprovechables, ha_totales, 0) > 0`).all()
      .filter(po => !ocupados.has(po.id));

    for (const po of libres) {
      if (db.prepare("SELECT 1 FROM costeo_potrero_dias WHERE potrero_id = ? AND fecha = ? AND ocioso = 1").get(po.id, f)) continue;
      const ha = po.ha_aprovechables || po.ha_totales || 0;
      const kgneDia = ha * kgAnualHa(po, nov) / 365;
      const costo = kgneDia * nov;
      ins.run(-po.id, po.id, null, f, 0, 0, ha, kgneDia, nov, costo, 1);
      res.ociosos++; res.kgne_ocioso += kgneDia; res.costo_ocioso += costo; res.ha_ociosa += ha;
      res.detalle.push({ potrero: po.nombre, descanso: true, ha,
        costo: Math.round(costo * 100) / 100 });
    }
    return res;
  }

  function consolidarPotrero(desde, hasta) {
    const d1 = desde || new Date(Date.now() - 31 * 86400000).toISOString().slice(0, 10);
    const d2 = hasta || hoy();
    const dias = db.prepare(`SELECT * FROM costeo_potrero_dias
      WHERE consolidado = 0 AND fecha BETWEEN ? AND ? ORDER BY fecha`).all(d1, d2);
    if (!dias.length) return { consolidados: 0, animales: 0, costo: 0 };

    /* Todos los activos del campo, para repartir lo que está en descanso */
    const todos = db.prepare("SELECT id FROM animales WHERE estado = 'ACTIVO'").all().map(x => x.id);

    const acum = {}, acumOcio = {};
    const sumar = (dest, rep) => { for (const [id, m] of Object.entries(rep)) dest[id] = (dest[id] || 0) + m; };

    for (const x of dias) {
      if (x.ocioso) {
        /* Potrero en descanso: lo paga todo el rodeo, por UG */
        sumar(acumOcio, repartirPorUG(todos, x.costo_total));
        continue;
      }
      const enLote = db.prepare(`SELECT animal_id FROM costeo_permanencia
        WHERE lote_id = ? AND fecha_desde <= ? AND (fecha_hasta IS NULL OR fecha_hasta >= ?)`)
        .all(x.lote_id, x.fecha, x.fecha).map(r => r.animal_id);
      const lista = enLote.length ? enLote : animalesDelLote(x.lote_id);
      if (!lista.length) continue;
      sumar(acum, repartirPorUG(lista, x.costo_total));
    }

    const ins = db.prepare("INSERT INTO costos (animal_id,fecha,concepto,detalle,monto,moneda) VALUES (?,?,'PISO',?,?,'USD')");
    let total = 0, totalOcio = 0;
    const tx = db.transaction(() => {
      for (const [id, monto] of Object.entries(acum)) {
        if (monto < 0.005) continue;
        ins.run(parseInt(id, 10), d2, `costeo:potrero ${d1} a ${d2}`, Math.round(monto * 100) / 100);
        total += monto;
      }
      for (const [id, monto] of Object.entries(acumOcio)) {
        if (monto < 0.005) continue;
        ins.run(parseInt(id, 10), d2, `costeo:potrero-descanso ${d1} a ${d2}`, Math.round(monto * 100) / 100);
        totalOcio += monto;
      }
      db.prepare("UPDATE costeo_potrero_dias SET consolidado = 1 WHERE consolidado = 0 AND fecha BETWEEN ? AND ?").run(d1, d2);
    });
    tx();
    const ids = new Set([...Object.keys(acum), ...Object.keys(acumOcio)]);
    return { desde: d1, hasta: d2, consolidados: dias.length, animales: ids.size,
             costo_pastoreo: Math.round(total * 100) / 100,
             costo_descanso: Math.round(totalOcio * 100) / 100,
             costo: Math.round((total + totalOcio) * 100) / 100 };
  }

  function resumenPotreros() {
    const p = precios(); const nov = p ? p.novillo_pie : 0;
    return db.prepare(`SELECT po.*,
        (SELECT COUNT(*) FROM costeo_lote_potrero a WHERE a.potrero_id = po.id AND a.activo = 1) lotes,
        (SELECT COALESCE(SUM((SELECT COUNT(*) FROM lote_animales la WHERE la.lote_id = a.lote_id)),0)
           FROM costeo_lote_potrero a WHERE a.potrero_id = po.id AND a.activo = 1) animales,
        (SELECT GROUP_CONCAT(a.lote_id) FROM costeo_lote_potrero a WHERE a.potrero_id = po.id AND a.activo = 1) lotes_ids
      FROM costeo_potreros po ORDER BY po.activo DESC, po.nombre`).all().map(x => {
      const ha = x.ha_aprovechables || x.ha_totales || 0;
      const kgAnual = ha * kgAnualHa(x, nov);
      const ug = (x.lotes_ids || "").split(",").filter(Boolean)
        .reduce((s, id) => s + ugDelLote(parseInt(id, 10)), 0);
      const vida = x.vida_util_anios > 0 ? x.vida_util_anios : vidaUtilDe(x.tipo);
      const inv = x.inversion_ha || 0;
      return { ...x, kg_ha_anual: kgAnualHa(x, nov), kg_ha_renta: kgRentaHa(x),
        kg_ha_amort: kgAmortHa(x, nov), vida_efectiva: vida,
        usd_pasto_ha: usdPastoHa(x),
        usd_implantacion_ha: (inv > 0 && vida > 0) ? inv / vida : 0,
        usd_mantenimiento_ha: x.mantenimiento_ha || 0,
        ha_usada: ha, ug,
        kgne_anual: kgAnual, usd_anual: kgAnual * nov,
        usd_dia: kgAnual * nov / 365,
        usd_ug_dia: ug ? (kgAnual * nov / 365) / ug : null,
        usd_animal_dia: x.animales ? (kgAnual * nov / 365) / x.animales : null,
        carga: ug && ha ? ug / ha : null,
        en_descanso: !x.lotes };
    });
  }

  /* ── IATF ──────────────────────────────────────────────────────────────────
     Carga masiva: pajuela por animal + honorarios repartidos + hormonas del
     stock de IMPROLUX. Un costo por animal, discriminado. */
  function costearIATF(o) {
    const animales = o.animales || [];
    if (!animales.length) throw new Error("Sin animales");
    const f = o.fecha || hoy();
    const n = animales.length;
    const pajuela = parseFloat(o.costo_pajuela) || 0;
    const vetTotal = o.veterinario_modo === "POR_ANIMAL"
      ? (parseFloat(o.costo_veterinario) || 0) * n
      : (parseFloat(o.costo_veterinario) || 0);
    const vetCabeza = vetTotal / n;

    const ins = db.prepare("INSERT INTO costos (animal_id,fecha,concepto,detalle,monto,moneda) VALUES (?,?,?,?,?,'USD')");
    const lineas = [];
    const tx = db.transaction(() => {
      animales.forEach(a => {
        if (pajuela > 0) ins.run(a.id, f, "IATF",
          `costeo:pajuela${o.semen ? " " + o.semen : ""}`, pajuela);
        if (vetCabeza > 0) ins.run(a.id, f, "IATF", "costeo:honorarios veterinarios", vetCabeza);
      });
    });
    tx();
    if (pajuela > 0) lineas.push(`  • Pajuela${o.semen ? " " + o.semen : ""}: US$ ${pajuela.toFixed(2)} × ${n} = US$ ${(pajuela * n).toFixed(2)}`);
    if (vetTotal > 0) lineas.push(`  • Honorarios: US$ ${vetTotal.toFixed(2)} → US$ ${vetCabeza.toFixed(2)} c/u`);

    return { animales: n, fecha: f, pajuela_total: pajuela * n, veterinario_total: vetTotal,
             lineas, costo_base: pajuela * n + vetTotal };
  }


  /* ── ESTRUCTURA Y ADMINISTRACIÓN ───────────────────────────────────────────
     Sueldos, contador, camioneta, impuestos. No dependen del potrero, así que
     se reparten parejo por UG-día de todo el campo. Este es el único rubro que
     corresponde aplanar: aplanar la tierra borraría la diferencia entre el
     campo de cría y el de recría, que es justo lo que hay que medir. */

  function estructuraAnual() {
    const r = db.prepare(`SELECT COALESCE(SUM(monto_anual * COALESCE(pct_ganaderia,100) / 100),0) t,
      COUNT(*) n FROM costeo_estructura WHERE activo = 1`).get();
    return { total: r.t, conceptos: r.n };
  }

  function aplicarEstructura(fecha) {
    const f = (fecha || hoy()).slice(0, 10);
    if (db.prepare("SELECT 1 FROM costeo_estructura_dias WHERE fecha = ?").get(f)) return { fecha: f, ya: true };
    const anual = estructuraAnual().total;
    if (!(anual > 0)) return { fecha: f, monto: 0 };
    const ug = ugDelCampo();
    if (!(ug > 0)) return { fecha: f, monto: 0, aviso: "sin animales activos" };
    const monto = anual / 365;
    db.prepare("INSERT INTO costeo_estructura_dias (fecha,monto_dia,ug_campo) VALUES (?,?,?)").run(f, monto, ug);
    return { fecha: f, monto, ug, usd_ug_dia: monto / ug };
  }

  function consolidarEstructura(desde, hasta) {
    const d1 = desde || new Date(Date.now() - 31 * 86400000).toISOString().slice(0, 10);
    const d2 = hasta || hoy();
    const dias = db.prepare(`SELECT * FROM costeo_estructura_dias
      WHERE consolidado = 0 AND fecha BETWEEN ? AND ? ORDER BY fecha`).all(d1, d2);
    if (!dias.length) return { consolidados: 0, animales: 0, costo: 0 };

    const acum = {};
    for (const x of dias) {
      const vivos = db.prepare(`SELECT id FROM animales WHERE estado = 'ACTIVO'`).all().map(a => a.id);
      if (!vivos.length) continue;
      const rep = repartirPorUG(vivos, x.monto_dia);
      for (const [id, m] of Object.entries(rep)) acum[id] = (acum[id] || 0) + m;
    }

    const ins = db.prepare("INSERT INTO costos (animal_id,fecha,concepto,detalle,monto,moneda) VALUES (?,?,'ESTRUCTURA',?,?,'USD')");
    let total = 0;
    const tx = db.transaction(() => {
      for (const [id, monto] of Object.entries(acum)) {
        if (monto < 0.005) continue;
        ins.run(parseInt(id, 10), d2, `costeo:estructura ${d1} a ${d2}`, Math.round(monto * 100) / 100);
        total += monto;
      }
      db.prepare("UPDATE costeo_estructura_dias SET consolidado = 1 WHERE consolidado = 0 AND fecha BETWEEN ? AND ?").run(d1, d2);
    });
    tx();
    return { desde: d1, hasta: d2, consolidados: dias.length,
             animales: Object.keys(acum).length, costo: Math.round(total * 100) / 100 };
  }

  /* ── COSTO POR UNIDAD GANADERA ─────────────────────────────────────────────
     El número que se usa para hablar: cuánto cuesta mantener una UG por año.
     Armado, no promediado: se ve cuánto es tierra y cuánto estructura. */
  function costoPorUG() {
    const p = precios(); const nov = p ? p.novillo_pie : 0;
    const ug = ugDelCampo();
    const pots = db.prepare("SELECT * FROM costeo_potreros WHERE activo = 1").all();

    let tierraRenta = 0, implantacion = 0, mantenimiento = 0, haTotal = 0;
    pots.forEach(x => {
      const ha = x.ha_aprovechables || x.ha_totales || 0;
      haTotal += ha;
      tierraRenta += ha * kgRentaHa(x) * nov;
      const inv = x.inversion_ha || 0;
      const vida = x.vida_util_anios > 0 ? x.vida_util_anios : vidaUtilDe(x.tipo);
      if (inv > 0 && vida > 0) implantacion += ha * inv / vida;
      mantenimiento += ha * (x.mantenimiento_ha || 0);
    });
    const tierraAmort = implantacion + mantenimiento;
    const est = estructuraAnual().total;
    const total = tierraRenta + tierraAmort + est;

    const cats = db.prepare(`SELECT categoria, COUNT(*) n FROM animales
      WHERE estado = 'ACTIVO' GROUP BY categoria`).all();

    return {
      novillo_pie: nov, ug_campo: ug, ha_total: haTotal,
      carga_general: haTotal ? ug / haTotal : null,
      arrendamiento: tierraRenta,
      inversion_pasto: tierraAmort,
      implantacion, mantenimiento_pasto: mantenimiento,
      estructura: est,
      total_anual: total,
      usd_ug_anio: ug ? total / ug : null,
      usd_ug_dia: ug ? total / ug / 365 : null,
      usd_ha_anio: haTotal ? total / haTotal : null,
      desglose_ug: ug ? {
        arrendamiento: tierraRenta / ug,
        inversion_pasto: tierraAmort / ug,
        implantacion: implantacion / ug,
        mantenimiento_pasto: mantenimiento / ug,
        estructura: est / ug
      } : null,
      por_categoria: cats.map(c => {
        const coef = ugDe(c.categoria);
        const anual = ug ? (total / ug) * coef : 0;
        return { categoria: c.categoria, cabezas: c.n, ug_coef: coef,
                 ug_total: c.n * coef, usd_animal_anio: anual,
                 usd_animal_dia: anual / 365, usd_categoria_anio: anual * c.n };
      }).sort((a, b) => b.usd_categoria_anio - a.usd_categoria_anio)
    };
  }


  /* ══════════════════════════════════════════════════════════════════════════
     TIERRA POR SECTOR
     El costo de la tierra no se sigue animal por animal ni parcela por parcela:
     se agrupa por sector. Todo lo que es de cría lo pagan los animales de cría,
     esté ocupado o descansando. Es lo que refleja la realidad: el potrero que
     descansa, descansa para ese rodeo.
     ══════════════════════════════════════════════════════════════════════════ */

  const cfg = (k, def) => {
    const r = db.prepare("SELECT valor FROM costeo_config WHERE clave = ?").get(k);
    return r ? r.valor : def;
  };
  const setCfg = (k, v) => db.prepare(
    "INSERT INTO costeo_config (clave,valor) VALUES (?,?) ON CONFLICT(clave) DO UPDATE SET valor = excluded.valor"
  ).run(k, String(v));

  const VIDA_IMPLANT = { PASTURA: 5, VERDEO: 1, FERTILIZACION: 1, CONTROL: 1, OTRO: 1 };

  /* Costo anual del lote en el año dado: renta + lo que le quede por devengar
     de cada implantación. La pastura del 2024 devenga hasta el 2028. */
  function costoAnualLote(lote, anio, nov) {
    const a = anio || new Date().getUTCFullYear();
    const ha = lote.ha_totales || 0;
    const renta = ha * (lote.kg_ha_renta || 0) * (nov || 0);

    const imps = db.prepare("SELECT * FROM costeo_implantaciones WHERE lote_campo_id = ?").all(lote.id);
    let implantacion = 0, mantenimiento = 0;
    const vigentes = [];
    for (const im of imps) {
      const vida = im.vida_util > 0 ? im.vida_util : (VIDA_IMPLANT[im.tipo] || 1);
      const transcurridos = a - im.anio;
      if (transcurridos < 0 || transcurridos >= vida) continue;   // no empezó o ya se devengó
      const cuota = (im.costo_total || 0) / vida;
      if (im.tipo === "PASTURA" || im.tipo === "VERDEO") implantacion += cuota;
      else mantenimiento += cuota;
      vigentes.push({ tipo: im.tipo, anio: im.anio, vida, cuota,
                      restan: vida - transcurridos });
    }
    const total = renta + implantacion + mantenimiento;
    return { ha, renta, implantacion, mantenimiento, total,
             usd_ha: ha ? total / ha : 0, vigentes };
  }

  /* Cómo se reparte un lote entre sectores en una fecha dada.
     Un lote puede ser mitad cría y mitad recría: devuelve {CRIA:0.5, RECRIA:0.5}.
     El préstamo temporal pisa el reparto de base mientras dure. */
  function normalizarPct(o) {
    const t = (o.pct_cria || 0) + (o.pct_recria || 0) + (o.pct_corral || 0);
    if (!(t > 0)) return null;
    const d = {};
    if (o.pct_cria > 0)   d.CRIA   = o.pct_cria / t;
    if (o.pct_recria > 0) d.RECRIA = o.pct_recria / t;
    if (o.pct_corral > 0) d.CORRAL = o.pct_corral / t;
    return d;
  }

  /* Sectores que NO cargan a los animales: el lote salió de ganadería. */
  const FUERA_GANADERIA = new Set(["AGRICULTURA", "EXCLUIDO"]);

  function distribucionLote(loteId, fecha) {
    const f = fecha || hoy();
    const tmp = db.prepare(`SELECT * FROM costeo_lote_sector
      WHERE lote_campo_id = ? AND activo = 1 AND fecha_desde <= ?
        AND (fecha_hasta IS NULL OR fecha_hasta >= ?) ORDER BY fecha_desde DESC LIMIT 1`)
      .get(loteId, f, f);
    if (tmp) {
      if (FUERA_GANADERIA.has(tmp.sector)) return {};
      return normalizarPct(tmp) || (tmp.sector ? { [tmp.sector]: 1 } : { CRIA: 1 });
    }
    const l = db.prepare("SELECT * FROM costeo_lotes_campo WHERE id = ?").get(loteId);
    if (!l) return { CRIA: 1 };
    if (FUERA_GANADERIA.has(l.sector)) return {};
    return normalizarPct(l) || { [l.sector || "CRIA"]: 1 };
  }

  /* Etiqueta para mostrar: el sector dominante, o "mixto" si está repartido */
  function sectorDeLote(loteId, fecha) {
    const d = distribucionLote(loteId, fecha);
    const ks = Object.keys(d);
    if (!ks.length) {
      const f = fecha || hoy();
      const tmp = db.prepare(`SELECT sector FROM costeo_lote_sector
        WHERE lote_campo_id = ? AND activo = 1 AND fecha_desde <= ?
          AND (fecha_hasta IS NULL OR fecha_hasta >= ?) ORDER BY fecha_desde DESC LIMIT 1`).get(loteId, f, f);
      if (tmp) return tmp.sector;
      const l = db.prepare("SELECT sector FROM costeo_lotes_campo WHERE id = ?").get(loteId);
      return l ? l.sector : "EXCLUIDO";
    }
    if (ks.length === 1) return ks[0];
    return ks.sort((a, b) => d[b] - d[a])[0] + "+";
  }

  /* Animales de cada sector.
       CORRAL  → los que están en un lote de ADE marcado como corral
       RECRIA  → destetados de menos de 30 meses, fuera del corral
       CRIA    → el resto: vacas, vaquillonas de más de 30 meses y machos adultos */
  function animalesDelSector(sector, fecha) {
    const f = fecha || hoy();
    const meses = parseFloat(cfg("meses_recria", "30")) || 30;

    const enCorral = db.prepare(`SELECT la.animal_id FROM lote_animales la
      JOIN costeo_sector_ade sa ON sa.lote_id = la.lote_id
      JOIN animales a ON a.id = la.animal_id
      WHERE sa.sector = 'CORRAL' AND a.estado = 'ACTIVO'`).all().map(x => x.animal_id);
    const setCorral = new Set(enCorral);

    if (sector === "CORRAL") {
      if (!enCorral.length) return [];
      return db.prepare(`SELECT id, categoria FROM animales
        WHERE id IN (${enCorral.map(() => "?").join(",")})`).all(...enCorral);
    }

    /* Sin fecha de nacimiento no se puede saber la edad: van a cría, que es
       donde está el rodeo adulto. */
    const todos = db.prepare(`SELECT id, categoria, fecha_nac,
        CASE WHEN fecha_nac IS NULL OR fecha_nac = '' THEN 999
             ELSE (julianday(?) - julianday(fecha_nac)) / 30.44 END edad_meses
      FROM animales WHERE estado = 'ACTIVO'`).all(f);

    return todos.filter(a => {
      if (setCorral.has(a.id)) return false;
      const esRecria = a.edad_meses < meses;
      return sector === "RECRIA" ? esRecria : !esRecria;
    });
  }

  /* Devenga el día: cada sector suma sus lotes y lo reparte entre sus animales */
  function aplicarSectores(fecha) {
    const f = (fecha || hoy()).slice(0, 10);
    const p = precios();
    const nov = p ? p.novillo_pie : 0;
    if (!nov) return { fecha: f, error: "sin precios" };
    const anio = parseInt(f.slice(0, 4), 10);
    const porUG = cfg("reparto_tierra", "CABEZA") === "UG";

    const lotes = db.prepare("SELECT * FROM costeo_lotes_campo WHERE activo = 1").all();
    const acumSector = {};
    for (const l of lotes) {
      const dist = distribucionLote(l.id, f);
      const c = costoAnualLote(l, anio, nov);
      for (const [sec, pct] of Object.entries(dist)) {
        acumSector[sec] = acumSector[sec] || { ha: 0, anual: 0, lotes: 0 };
        acumSector[sec].ha += c.ha * pct;
        acumSector[sec].anual += c.total * pct;
        acumSector[sec].lotes += pct;
      }
    }

    const res = { fecha: f, sectores: [], costo: 0 };
    for (const [sec, d] of Object.entries(acumSector)) {
      if (db.prepare("SELECT 1 FROM costeo_sector_dias WHERE fecha = ? AND sector = ?").get(f, sec)) continue;
      const ans = animalesDelSector(sec, f);
      const ug = ans.reduce((t, a) => t + ugDe(a.categoria), 0);
      const costoDia = d.anual / 365;
      db.prepare(`INSERT INTO costeo_sector_dias (fecha,sector,ha,costo_dia,animales,ug)
        VALUES (?,?,?,?,?,?)`).run(f, sec, d.ha, costoDia, ans.length, ug);
      res.costo += costoDia;
      res.sectores.push({ sector: sec, lotes: d.lotes, ha: Math.round(d.ha * 10) / 10,
        animales: ans.length, ug: Math.round(ug * 10) / 10,
        costo_dia: Math.round(costoDia * 100) / 100,
        por_animal: ans.length ? Math.round(costoDia / ans.length * 1000) / 1000 : null,
        base: porUG ? "UG" : "cabeza" });
    }
    return res;
  }

  function consolidarSectores(desde, hasta) {
    const d1 = desde || new Date(Date.now() - 31 * 86400000).toISOString().slice(0, 10);
    const d2 = hasta || hoy();
    const dias = db.prepare(`SELECT * FROM costeo_sector_dias
      WHERE consolidado = 0 AND fecha BETWEEN ? AND ? ORDER BY fecha`).all(d1, d2);
    if (!dias.length) return { consolidados: 0, animales: 0, costo: 0 };
    const porUG = cfg("reparto_tierra", "CABEZA") === "UG";

    const acum = {}, porSector = {};
    for (const x of dias) {
      const ans = animalesDelSector(x.sector, x.fecha);
      if (!ans.length) continue;
      const ids = ans.map(a => a.id);
      const rep = porUG ? repartirPorUG(ids, x.costo_dia)
                        : Object.fromEntries(ids.map(id => [id, x.costo_dia / ids.length]));
      for (const [id, m] of Object.entries(rep)) acum[id] = (acum[id] || 0) + m;
      porSector[x.sector] = (porSector[x.sector] || 0) + x.costo_dia;
    }

    const ins = db.prepare("INSERT INTO costos (animal_id,fecha,concepto,detalle,monto,moneda) VALUES (?,?,'PISO',?,?,'USD')");
    let total = 0;
    const tx = db.transaction(() => {
      for (const [id, monto] of Object.entries(acum)) {
        if (monto < 0.005) continue;
        ins.run(parseInt(id, 10), d2, `costeo:tierra ${d1} a ${d2}`, Math.round(monto * 100) / 100);
        total += monto;
      }
      db.prepare("UPDATE costeo_sector_dias SET consolidado = 1 WHERE consolidado = 0 AND fecha BETWEEN ? AND ?").run(d1, d2);
    });
    tx();
    return { desde: d1, hasta: d2, consolidados: dias.length, animales: Object.keys(acum).length,
             costo: Math.round(total * 100) / 100,
             por_sector: Object.fromEntries(Object.entries(porSector).map(([k, v]) => [k, Math.round(v * 100) / 100])) };
  }

  /* Foto del costo de la tierra: por lote y por sector */
  function resumenTierra(anio) {
    const p = precios(); const nov = p ? p.novillo_pie : 0;
    const a = anio || new Date().getUTCFullYear();
    const lotes = db.prepare("SELECT * FROM costeo_lotes_campo ORDER BY sector, nombre").all();

    const detalle = lotes.map(l => {
      const c = costoAnualLote(l, a, nov);
      return { ...l, ...c, sector_hoy: sectorDeLote(l.id), distribucion: distribucionLote(l.id) };
    });

    const sectores = {};
    detalle.forEach(l => {
      if (!l.activo) return;
      for (const [sec, pct] of Object.entries(l.distribucion || {})) {
        const s = sectores[sec] = sectores[sec] || { ha: 0, anual: 0, lotes: 0 };
        s.ha += l.ha * pct; s.anual += l.total * pct; s.lotes += pct;
      }
    });

    const out = Object.entries(sectores).map(([sec, d]) => {
      const ans = animalesDelSector(sec);
      const ug = ans.reduce((t, x) => t + ugDe(x.categoria), 0);
      return { sector: sec, lotes: d.lotes, ha: d.ha, anual: d.anual,
        usd_ha: d.ha ? d.anual / d.ha : 0,
        animales: ans.length, ug,
        usd_animal_anio: ans.length ? d.anual / ans.length : null,
        usd_animal_dia: ans.length ? d.anual / ans.length / 365 : null,
        usd_ug_anio: ug ? d.anual / ug : null,
        carga: d.ha ? ans.length / d.ha : null };
    });

    return { anio: a, novillo_pie: nov, reparto: cfg("reparto_tierra", "CABEZA"),
             meses_recria: parseFloat(cfg("meses_recria", "30")),
             lotes: detalle, sectores: out,
             ha_total: detalle.filter(l => l.activo).reduce((s, l) => s + l.ha, 0),
             costo_total: out.reduce((s, x) => s + x.anual, 0) };
  }


  /* ══════════════════════════════════════════════════════════════════════════
     CARGA RETROACTIVA
     El devengo diario solo corre hacia adelante. Para el arranque hace falta
     recorrer los días que ya pasaron: ración, piso y sanidad de la temporada.
     Es idempotente — el día que ya está no se vuelve a escribir.
     ══════════════════════════════════════════════════════════════════════════ */

  function devengarRango(desde, hasta, opts) {
    const o = opts || {};
    if (!precios()) throw new Error("No hay precios cargados");
    const d1 = new Date((desde || hoy()) + "T00:00:00Z");
    const d2 = new Date((hasta || hoy()) + "T00:00:00Z");
    if (isNaN(d1) || isNaN(d2)) throw new Error("Fechas inválidas");
    const dias = Math.floor((d2 - d1) / 86400000) + 1;
    if (dias < 1) throw new Error("El rango está al revés");
    if (dias > 800) throw new Error("Rango demasiado largo (máximo ~2 años)");

    const res = { desde: desde, hasta: hasta, dias,
                  tierra: { dias: 0, costo: 0 }, racion: { dias: 0, kg: 0, costo: 0 },
                  estructura: { dias: 0, costo: 0 }, errores: [] };

    for (let i = 0; i < dias; i++) {
      const f = new Date(d1.getTime() + i * 86400000).toISOString().slice(0, 10);
      if (o.tierra !== false) {
        try {
          const r = aplicarSectores(f);
          if (r.sectores && r.sectores.length) { res.tierra.dias++; res.tierra.costo += r.costo || 0; }
        } catch (e) { if (res.errores.length < 5) res.errores.push(`tierra ${f}: ${e.message}`); }
      }
      if (o.racion !== false) {
        try {
          const r = aplicarDietas(f);
          if (r.aplicadas) { res.racion.dias++; res.racion.kg += r.kg; res.racion.costo += r.costo; }
        } catch (e) { if (res.errores.length < 5) res.errores.push(`ración ${f}: ${e.message}`); }
      }
      /* Sueldos, contador, camioneta, impuestos: son del año y se devengan
         todos los días igual que el piso. Antes solo corrían hacia adelante. */
      if (o.estructura !== false) {
        try {
          const r = aplicarEstructura(f);
          if (r.monto) { res.estructura.dias++; res.estructura.costo += r.monto; }
        } catch (e) { if (res.errores.length < 5) res.errores.push(`estructura ${f}: ${e.message}`); }
      }
    }
    res.tierra.costo = Math.round(res.tierra.costo * 100) / 100;
    res.racion.costo = Math.round(res.racion.costo * 100) / 100;
    res.racion.kg = Math.round(res.racion.kg);
    res.estructura.costo = Math.round(res.estructura.costo * 100) / 100;
    return res;
  }

  /* Qué animales entran en un filtro. Sirve para cargar un costo a un grupo
     sin tener que seleccionarlos uno por uno. */
  function seleccionar(filtro) {
    const f = filtro || {};
    if (Array.isArray(f.rps) && f.rps.length) {
      const out = [];
      f.rps.forEach(rp => { const a = animal(String(rp).trim()); if (a) out.push(a); });
      return out;
    }
    let sql = `SELECT a.* FROM animales a`;
    const w = ["a.estado = 'ACTIVO'"], args = [];
    if (f.lote_id) {
      sql += " JOIN lote_animales la ON la.animal_id = a.id";
      w.push("la.lote_id = ?"); args.push(f.lote_id);
    }
    if (f.categoria)  { w.push("UPPER(a.categoria) = UPPER(?)"); args.push(f.categoria); }
    if (f.sexo)       { w.push("UPPER(a.sexo) = UPPER(?)");      args.push(f.sexo); }
    if (f.registro)   { w.push("UPPER(a.registro) = UPPER(?)");  args.push(f.registro); }
    if (f.generacion) { w.push("substr(a.fecha_nac,1,4) = ?");   args.push(String(f.generacion)); }
    if (f.nacidos_desde) { w.push("a.fecha_nac >= ?"); args.push(f.nacidos_desde); }
    if (f.nacidos_hasta) { w.push("a.fecha_nac <= ?"); args.push(f.nacidos_hasta); }
    if (f.con_ficha)  { w.push("EXISTS (SELECT 1 FROM costeo_kgne k WHERE k.animal_id = a.id)"); }
    if (f.etapa) {
      w.push(`(SELECT etapa FROM costeo_kgne k WHERE k.animal_id = a.id ORDER BY fecha DESC, id DESC LIMIT 1) = ?`);
      args.push(f.etapa);
    }
    sql += " WHERE " + w.join(" AND ") + " ORDER BY a.rp";
    return db.prepare(sql).all(...args);
  }

  /* Carga un costo a un grupo entero. TOTAL lo reparte; POR_ANIMAL lo repite. */
  function costoMasivo(o) {
    const animales = seleccionar(o.filtro || o);
    if (!animales.length) throw new Error("El filtro no seleccionó ningún animal");
    if (!o.concepto) throw new Error("Falta el concepto");
    const monto = parseFloat(o.monto) || 0;
    if (!(monto > 0)) throw new Error("Falta el monto");
    const f = o.fecha || hoy();
    const porUG = o.reparto === "UG";

    let montos = {};
    if (o.modo === "POR_ANIMAL") {
      animales.forEach(a => { montos[a.id] = monto; });
    } else if (porUG) {
      montos = repartirPorUG(animales.map(a => a.id), monto);
    } else {
      const c = monto / animales.length;
      animales.forEach(a => { montos[a.id] = c; });
    }

    const ins = db.prepare("INSERT INTO costos (animal_id,fecha,concepto,detalle,monto,moneda) VALUES (?,?,?,?,?,'USD')");
    const marca = `costeo:masivo ${o.detalle || ""}`.trim();
    let total = 0;
    const tx = db.transaction(() => {
      for (const a of animales) {
        const m = montos[a.id] || 0;
        if (m < 0.005) continue;
        ins.run(a.id, f, o.concepto, marca, Math.round(m * 100) / 100);
        total += m;
      }
    });
    tx();
    return { animales: animales.length, fecha: f, concepto: o.concepto,
             total: Math.round(total * 100) / 100,
             por_animal: Math.round(total / animales.length * 100) / 100,
             modo: o.modo || "TOTAL", reparto: porUG ? "UG" : "cabeza",
             muestra: animales.slice(0, 10).map(a => a.rp) };
  }

  /* Deshacer una carga masiva por su marca y fecha */
  function borrarCostoMasivo(fecha, concepto, detalle) {
    const r = db.prepare(`DELETE FROM costos WHERE fecha = ? AND concepto = ?
      AND detalle LIKE 'costeo:masivo%' ${detalle ? "AND detalle = ?" : ""}`)
      .run(...(detalle ? [fecha, concepto, `costeo:masivo ${detalle}`.trim()] : [fecha, concepto]));
    return { borrados: r.changes };
  }


  /* ══════════════════════════════════════════════════════════════════════════
     RESET
     Dos niveles. COSTEO borra lo calculado y deja la configuración; TODO deja
     el módulo como recién instalado. Ninguno de los dos toca los datos reales:
     animales, pesadas, servicios, sanidad y lotes quedan intactos.
     Los costos cargados a mano tampoco se borran: solo los que generó el
     sistema, que llevan la marca "costeo:".
     ══════════════════════════════════════════════════════════════════════════ */

  const TABLAS_CALCULO = [
    ["costeo_kgne",           "Asientos kgNE"],
    ["costeo_etapas",         "Etapas cerradas"],
    ["costeo_sector_dias",    "Devengo de tierra"],
    ["costeo_potrero_dias",   "Devengo de potreros (viejo)"],
    ["costeo_dieta_dias",     "Devengo de ración"],
    ["costeo_estructura_dias","Devengo de estructura"],
    ["costeo_permanencia",    "Permanencia en lotes"]
  ];
  const TABLAS_CONFIG = [
    ["costeo_lotes_campo",   "Lotes del campo"],
    ["costeo_lote_sector",   "Préstamos de lote"],
    ["costeo_implantaciones","Implantaciones"],
    ["costeo_sector_ade",    "Corrales"],
    ["costeo_dietas",        "Dietas"],
    ["costeo_estructura",    "Gastos de estructura"],
    ["costeo_eventos",       "Eventos a prorratear"],
    ["costeo_productos",     "Espejo del stock"],
    ["costeo_sanidad_precios","Precios de sanidad"],
    ["costeo_precios",       "Precios ACG"],
    ["costeo_potreros",      "Potreros (viejo)"]
  ];

  function resetCosteo(o) {
    const op = o || {};
    const nivel = String(op.nivel || "COSTEO").toUpperCase();
    const tablas = nivel === "TODO" ? TABLAS_CALCULO.concat(TABLAS_CONFIG) : TABLAS_CALCULO;

    const cuenta = t => { try { return db.prepare(`SELECT COUNT(*) n FROM ${t}`).get().n; } catch (e) { return 0; } };
    const costosGen = (() => { try {
      return db.prepare("SELECT COUNT(*) n, COALESCE(SUM(monto),0) t FROM costos WHERE detalle LIKE 'costeo:%'").get();
    } catch (e) { return { n: 0, t: 0 }; } })();
    const costosMan = (() => { try {
      return db.prepare("SELECT COUNT(*) n FROM costos WHERE detalle IS NULL OR detalle NOT LIKE 'costeo:%'").get().n;
    } catch (e) { return 0; } })();

    const detalle = tablas.map(([t, label]) => ({ tabla: t, label, filas: cuenta(t) }))
                          .filter(x => x.filas > 0);
    const resumen = {
      nivel, tablas: detalle,
      total_filas: detalle.reduce((s, x) => s + x.filas, 0),
      costos_generados: costosGen.n, costos_generados_usd: Math.round(costosGen.t * 100) / 100,
      costos_manuales: costosMan,
      intactos: ["animales", "pesadas", "servicios", "sanidad", "lotes", "lote_animales",
                 "mediciones", "ecografias", "toros"]
    };
    if (op.dryRun) return { ...resumen, dry: true };

    const tx = db.transaction(() => {
      /* Solo los costos que generó el sistema. Lo cargado a mano se respeta. */
      try { db.prepare("DELETE FROM costos WHERE detalle LIKE 'costeo:%'").run(); } catch (e) {}
      tablas.forEach(([t]) => { try { db.prepare(`DELETE FROM ${t}`).run(); } catch (e) {} });
      if (nivel === "TODO") {
        try { db.prepare("DELETE FROM costeo_config").run(); } catch (e) {}
        try { db.prepare("DELETE FROM costeo_hitos").run(); } catch (e) {}
        try { db.prepare("DELETE FROM costeo_ug").run(); } catch (e) {}
      }
    });
    tx();
    if (nivel === "TODO") { crearTablas(db); invalidarUG(); }
    return { ...resumen, borrado: true };
  }


  /* ── REABRIR POR DESTETE ───────────────────────────────────────────────────
     La camada de años anteriores quedó abierta por inventario, con la fecha de
     hoy. Así su destete y todas sus pesadas caen antes del corte y se ignoran.
     Esto les rehace la ficha desde su propio destete: después el sincronizador
     reproduce las pesadas y sale la ganancia de peso de toda su vida. */
  function reabrirPorDestete(o) {
    const op = o || {};
    const desde = op.desde || "2000-01-01";
    if (!precios()) throw new Error("No hay precios cargados");

    const candidatos = db.prepare(`SELECT a.*, p.fecha f_dest, p.peso peso_dest, p.id pesada_id
      FROM animales a
      JOIN pesadas p ON p.animal_id = a.id AND UPPER(COALESCE(p.contexto,'')) = 'DESTETE'
      WHERE a.estado = 'ACTIVO' AND p.fecha >= ? AND p.peso > 0
      GROUP BY a.id
      HAVING p.fecha = MIN(p.fecha)
      ORDER BY p.fecha`).all(desde);

    const res = { desde, candidatos: candidatos.length, reabiertos: 0,
                  ya_estaban: 0, en_plantel: 0, detalle: [] };

    for (const a of candidatos) {
      const primero = db.prepare(
        "SELECT fecha, origen FROM costeo_kgne WHERE animal_id = ? ORDER BY fecha, id LIMIT 1").get(a.id);
      /* Si ya abrió por su destete, no hay nada que hacer */
      if (primero && primero.origen === "DESTETE") { res.ya_estaban++; continue; }
      const et = etapaActual(a.id);
      /* Cerrado: no se toca */
      if (et === "FIN") { res.en_plantel++; continue; }
      /* Con un hito reproductivo real (tacto o evaluación de 18m) tampoco: ahí
         su valor lo manda el hito y reabrir le borraría la historia buena.
         Pero si llegó a plantel solo porque la apertura lo clasificó por
         categoría —el toro joven, la vaca sin fecha— sí se puede reabrir. */
      const conHito = db.prepare(`SELECT 1 FROM costeo_kgne WHERE animal_id = ?
        AND origen IN ('TACTO_18','TACTO_30','TACTO_SYNC','EVAL_18M') LIMIT 1`).get(a.id);
      if (conHito) { res.en_plantel++; continue; }
      /* La hembra en intermedia o plantel es vientre: su destete fue hace años
         y no aporta nada reabrirlo. */
      if (!esM(a.sexo) && ["INTERMEDIA", "PLANTEL"].includes(et)) { res.en_plantel++; continue; }

      if (op.dryRun) {
        res.reabiertos++;
        res.detalle.push({ rp: a.rp, destete: a.f_dest, peso: a.peso_dest, etapa: et });
        continue;
      }
      borrarFicha(a.id);
      destetar(a.id, String(a.f_dest).slice(0, 10), a.peso_dest, a.sexo);
      db.prepare("UPDATE costeo_kgne SET origen_id = ? WHERE animal_id = ? AND origen = 'DESTETE'")
        .run(a.pesada_id, a.id);
      res.reabiertos++;
      res.detalle.push({ rp: a.rp, destete: a.f_dest, peso: a.peso_dest });
    }
    return res;
  }


  /* ══════════════════════════════════════════════════════════════════════════
     VENTAS Y BAJAS
     Una sola operación cierra todo: el asiento kgNE, el estado del animal, la
     salida del lote y la permanencia. Antes la baja del costeo dejaba al animal
     activo en el rodeo, que era medio inútil.
     En la muerte se pierde el saldo entero: es lo que pasa de verdad.
     ══════════════════════════════════════════════════════════════════════════ */

  const MOTIVO_ESTADO = {
    REMATE:      { estado: "VENDIDO", motivo: "Remate" },
    VENTA:       { estado: "VENDIDO", motivo: "Venta" },
    VENTA_GORDA: { estado: "VENDIDO", motivo: "Venta gordo" },
    REFUGO:      { estado: "VENDIDO", motivo: "Refugo" },
    MUERTE:      { estado: "MUERTO",  motivo: "Muerte" }
  };

  function registrarVenta(o) {
    const tipo = String(o.tipo || "VENTA").toUpperCase();
    const cfgE = MOTIVO_ESTADO[tipo];
    if (!cfgE) throw new Error("Tipo de baja no reconocido: " + tipo);
    const fecha = (o.fecha || hoy()).slice(0, 10);
    const items = (o.animales || []).filter(x => x && (x.rp || x.animal_id || x.chip));
    if (!items.length) throw new Error("Sin animales");
    const p = precios();
    if (!p && tipo !== "MUERTE") throw new Error("No hay precios cargados");
    const nov = p ? p.novillo_pie : 0;

    /* Vista previa: no escribe nada */
    if (o.dryRun) {
      const prev = items.map(it => {
        const a = animal(it.rp || it.animal_id || it.chip);
        if (!a) return { rp: it.rp, error: "no encontrado" };
        const saldo0 = saldo(a.id);
        const imp = tipo === "MUERTE" ? 0 : parseFloat(it.importe) || 0;
        const kgneV = tipo === "MUERTE" ? 0 : (nov ? imp / nov : 0);
        const costo = db.prepare("SELECT COALESCE(SUM(monto),0) t FROM costos WHERE animal_id = ?").get(a.id).t;
        return { rp: a.rp, categoria: a.categoria, registro: a.registro,
                 peso: parseFloat(it.peso) || null, importe: imp,
                 saldo_previo: Math.round(saldo0), kgne_venta: Math.round(kgneV),
                 kgne_produccion: Math.round(kgneV - saldo0),
                 costo_acumulado: Math.round(costo * 100) / 100,
                 margen: Math.round((imp - costo) * 100) / 100,
                 usd_kg: it.peso ? Math.round(imp / it.peso * 100) / 100 : null };
      });
      const ok2 = prev.filter(x => !x.error);
      return { dry: true, tipo, fecha, animales: ok2.length,
        no_encontrados: prev.filter(x => x.error).map(x => x.rp),
        importe_total: ok2.reduce((s, x) => s + x.importe, 0),
        kgne_produccion: ok2.reduce((s, x) => s + x.kgne_produccion, 0),
        costo_acumulado: Math.round(ok2.reduce((s, x) => s + x.costo_acumulado, 0) * 100) / 100,
        margen: Math.round(ok2.reduce((s, x) => s + x.margen, 0) * 100) / 100,
        detalle: prev };
    }

    const insV = db.prepare(`INSERT INTO costeo_ventas
      (fecha,tipo,comprador,detalle,novillo_pie,ciclo) VALUES (?,?,?,?,?,?)`);
    const insI = db.prepare(`INSERT INTO costeo_venta_items
      (venta_id,animal_id,rp,chip,categoria,registro,sexo,peso,importe,
       saldo_previo,kgne_venta,kgne_produccion,costo_acumulado,margen)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);

    const ventaId = Number(insV.run(fecha, tipo, o.comprador || null, o.detalle || null,
      nov, cicloDe(fecha)).lastInsertRowid);

    const res = { venta_id: ventaId, tipo, fecha, animales: 0, no_encontrados: [],
                  importe_total: 0, kgne_venta: 0, kgne_produccion: 0,
                  costo_acumulado: 0, margen: 0, detalle: [] };

    for (const it of items) {
      const a = animal(it.rp || it.animal_id || it.chip);
      if (!a) { res.no_encontrados.push(it.rp || it.animal_id); continue; }
      const saldo0 = saldo(a.id);
      const peso = parseFloat(it.peso) || null;
      const imp = tipo === "MUERTE" ? 0 : parseFloat(it.importe) || 0;
      const costo = db.prepare("SELECT COALESCE(SUM(monto),0) t FROM costos WHERE animal_id = ?").get(a.id).t;

      /* 1 · asiento kgNE. En muerte va contra cero: se pierde todo el saldo. */
      baja(a.id, fecha, tipo, peso, imp);
      const kgneV = tipo === "MUERTE" ? 0 : (nov ? imp / nov : 0);

      /* 2 · sale del rodeo */
      db.prepare("UPDATE animales SET estado = ?, fecha_salida = ?, motivo_salida = ? WHERE id = ?")
        .run(cfgE.estado, fecha, o.detalle ? `${cfgE.motivo} · ${o.detalle}` : cfgE.motivo, a.id);

      /* 3 · sale del lote y se cierra la permanencia, para que deje de
             recibir piso y ración desde ese día */
      db.prepare("DELETE FROM lote_animales WHERE animal_id = ?").run(a.id);
      db.prepare("UPDATE costeo_permanencia SET fecha_hasta = ? WHERE animal_id = ? AND fecha_hasta IS NULL")
        .run(fecha, a.id);

      insI.run(ventaId, a.id, a.rp, a.chip, a.categoria, a.registro, a.sexo,
        peso, imp, saldo0, kgneV, kgneV - saldo0, costo, imp - costo);

      res.animales++; res.importe_total += imp; res.kgne_venta += kgneV;
      res.kgne_produccion += kgneV - saldo0; res.costo_acumulado += costo;
      res.margen += imp - costo;
      res.detalle.push({ rp: a.rp, peso, importe: imp,
        kgne_produccion: Math.round(kgneV - saldo0), margen: Math.round((imp - costo) * 100) / 100 });
    }

    db.prepare(`UPDATE costeo_ventas SET animales=?, importe_total=?, kgne_venta=?,
      kgne_produccion=?, costo_acumulado=? WHERE id=?`)
      .run(res.animales, res.importe_total, res.kgne_venta, res.kgne_produccion,
           res.costo_acumulado, ventaId);

    ["importe_total","kgne_venta","kgne_produccion","costo_acumulado","margen"]
      .forEach(k => { res[k] = Math.round(res[k] * 100) / 100; });
    res.promedio = res.animales ? Math.round(res.importe_total / res.animales * 100) / 100 : 0;
    return res;
  }

  function listarVentas(o) {
    const op = o || {};
    let sql = "SELECT * FROM costeo_ventas WHERE 1=1";
    const args = [];
    if (op.ciclo) { sql += " AND ciclo = ?"; args.push(op.ciclo); }
    if (op.tipo)  { sql += " AND tipo = ?";  args.push(op.tipo); }
    sql += " ORDER BY fecha DESC, id DESC LIMIT " + (parseInt(op.limite, 10) || 100);
    const ventas = db.prepare(sql).all(...args).map(v => ({
      ...v, margen: Math.round((v.importe_total - v.costo_acumulado) * 100) / 100,
      promedio: v.animales ? Math.round(v.importe_total / v.animales * 100) / 100 : 0
    }));
    return {
      ventas,
      totales: {
        operaciones: ventas.length,
        animales: ventas.reduce((s, v) => s + v.animales, 0),
        importe: Math.round(ventas.reduce((s, v) => s + v.importe_total, 0) * 100) / 100,
        kgne: Math.round(ventas.reduce((s, v) => s + v.kgne_produccion, 0)),
        costo: Math.round(ventas.reduce((s, v) => s + v.costo_acumulado, 0) * 100) / 100,
        margen: Math.round(ventas.reduce((s, v) => s + v.margen, 0) * 100) / 100
      }
    };
  }

  function detalleVenta(id) {
    const v = db.prepare("SELECT * FROM costeo_ventas WHERE id = ?").get(id);
    if (!v) throw new Error("Venta no encontrada");
    const items = db.prepare("SELECT * FROM costeo_venta_items WHERE venta_id = ? ORDER BY rp").all(id);
    return { ...v, margen: v.importe_total - v.costo_acumulado, items };
  }

  /* Deshacer: revive al animal y borra los asientos de la baja */
  function anularVenta(id) {
    const v = db.prepare("SELECT * FROM costeo_ventas WHERE id = ?").get(id);
    if (!v) throw new Error("Venta no encontrada");
    const items = db.prepare("SELECT * FROM costeo_venta_items WHERE venta_id = ?").all(id);
    const tx = db.transaction(() => {
      for (const it of items) {
        db.prepare("UPDATE animales SET estado='ACTIVO', fecha_salida=NULL, motivo_salida=NULL WHERE id = ?").run(it.animal_id);
        db.prepare(`DELETE FROM costeo_kgne WHERE animal_id = ? AND fecha = ?
          AND (tipo = 'BAJA' OR (tipo = 'TRANS' AND origen = 'CIERRE'))`).run(it.animal_id, v.fecha);
        db.prepare("UPDATE costeo_permanencia SET fecha_hasta = NULL WHERE animal_id = ? AND fecha_hasta = ?")
          .run(it.animal_id, v.fecha);
      }
      db.prepare("DELETE FROM costeo_venta_items WHERE venta_id = ?").run(id);
      db.prepare("DELETE FROM costeo_ventas WHERE id = ?").run(id);
    });
    tx();
    return { anulada: id, animales: items.length,
             aviso: "Los animales volvieron a ACTIVO. Revisá si hay que reasignarlos a un lote." };
  }

  return { precios, hito, animal, saldo, etapaActual, asiento, cerrarEtapa,
           destetar, desteteHijo, tacto, evaluar18, amortizar, baja,
           cargarCosto, esProductivo, prorratear, borrarProrrateo,
           ficha, ranking, cuadre, backfillPermanencia, moverLote,
           generar, generarTodos, borrarFicha, apertura, clasificar,
           sincronizar, sincronizarAnimal, altaNuevos, cohortes,
           aplicarDietas, diasStock, consolidarRacion, importarSanidad, animalesDelLote,
           sincronizarPreciosImprolux, traerStockImprolux,
           enviarConsumos, productoCache,
           asignarPotrero, sacarDePotrero, aplicarPotreros, consolidarPotrero,
           resumenPotreros, costearIATF,
           tablaUG, ugDe, ugDelLote, ugDelCampo, repartirPorUG, invalidarUG,
           estructuraAnual, aplicarEstructura, consolidarEstructura, costoPorUG,
           kgRentaHa, kgAmortHa, usdPastoHa,
           cfg, setCfg, costoAnualLote, sectorDeLote, animalesDelSector,
           aplicarSectores, consolidarSectores, resumenTierra, distribucionLote,
           devengarRango, seleccionar, costoMasivo, borrarCostoMasivo, resetCosteo,
           reabrirPorDestete, registrarVenta, listarVentas, detalleVenta, anularVenta };
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
  app.get("/api/costeo/ug", (req, res) => {
    ok(res, { ug: D(req).prepare("SELECT * FROM costeo_ug ORDER BY coeficiente DESC").all(),
      ug_campo: M(req).ugDelCampo(),
      por_categoria: D(req).prepare(`SELECT a.categoria, COUNT(*) n FROM animales a
        WHERE a.estado='ACTIVO' GROUP BY a.categoria ORDER BY n DESC`).all() });
  });
  app.post("/api/costeo/ug", (req, res) => {
    try {
      D(req).prepare(`INSERT INTO costeo_ug (categoria,coeficiente,nota) VALUES (?,?,?)
        ON CONFLICT(categoria) DO UPDATE SET coeficiente = excluded.coeficiente`)
        .run(String(req.body.categoria).toUpperCase(), parseFloat(req.body.coeficiente), req.body.nota || null);
      M(req).invalidarUG();
      ok(res, { mensaje: "Coeficiente guardado" });
    } catch (e) { err(res, e); }
  });
  app.post("/api/costeo/pesadas-a-produccion", (req, res) => {
    try {
      const r = M(req).sincronizar({ desde: req.body.desde, dietas: false, potreros: false,
        tierra: false, sanidad: false, altaNuevos: false, amortizar: false,
        estructura: false, detallado: true });
      ok(res, { revisados: r.revisados, animales: r.actualizados,
                pesadas: r.pesadas, destetes: r.destetes,
                detalle: (r.detalle || []).filter(d => d.cambios).slice(0, 30) });
    } catch (e) { err(res, e); }
  });

  app.post("/api/costeo/destetes-a-madres", (req, res) => {
    try {
      const r = M(req).sincronizar({ desde_destetes: req.body.desde, dietas: false,
        potreros: false, tierra: false, sanidad: false, altaNuevos: false,
        amortizar: false, estructura: false, detallado: true });
      ok(res, { revisados: r.revisados, madres: r.actualizados, destetes: r.destetes,
                detalle: (r.detalle || []).filter(d => d.cambios).slice(0, 30) });
    } catch (e) { err(res, e); }
  });

  app.post("/api/costeo/reset", (req, res) => {
    try {
      const b = req.body || {};
      if (!b.dryRun && b.confirmar !== "BORRAR")
        return err(res, new Error('Falta la confirmación. Mandá confirmar:"BORRAR".'));
      ok(res, M(req).resetCosteo(b));
    } catch (e) { err(res, e); }
  });

  /* ── VENTAS Y BAJAS ── */
  app.get("/api/costeo/ventas", (req, res) => {
    try { ok(res, M(req).listarVentas(req.query)); } catch (e) { err(res, e); }
  });
  app.get("/api/costeo/venta/:id", (req, res) => {
    try { ok(res, M(req).detalleVenta(parseInt(req.params.id, 10))); } catch (e) { err(res, e); }
  });
  app.post("/api/costeo/venta", (req, res) => {
    try { ok(res, M(req).registrarVenta(req.body || {})); } catch (e) { err(res, e); }
  });
  app.delete("/api/costeo/venta/:id", (req, res) => {
    try { ok(res, M(req).anularVenta(parseInt(req.params.id, 10))); } catch (e) { err(res, e); }
  });

  /* ── CARGA RETROACTIVA ── */
  app.post("/api/costeo/reabrir-por-destete", (req, res) => {
    try { ok(res, M(req).reabrirPorDestete(req.body || {})); } catch (e) { err(res, e); }
  });

  app.post("/api/costeo/devengar-rango", (req, res) => {
    try { ok(res, M(req).devengarRango(req.body.desde, req.body.hasta, req.body)); }
    catch (e) { err(res, e); }
  });
  app.post("/api/costeo/seleccionar", (req, res) => {
    try {
      const a = M(req).seleccionar(req.body || {});
      ok(res, { animales: a.length,
                detalle: a.slice(0, 200).map(x => ({ id: x.id, rp: x.rp, categoria: x.categoria,
                  sexo: x.sexo, fecha_nac: x.fecha_nac, registro: x.registro })) });
    } catch (e) { err(res, e); }
  });
  app.post("/api/costeo/costo-masivo", (req, res) => {
    try { ok(res, M(req).costoMasivo(req.body || {})); } catch (e) { err(res, e); }
  });
  app.delete("/api/costeo/costo-masivo", (req, res) => {
    try { ok(res, M(req).borrarCostoMasivo(req.query.fecha, req.query.concepto, req.query.detalle)); }
    catch (e) { err(res, e); }
  });

  /* ── TIERRA POR SECTOR ── */
  app.get("/api/costeo/tierra", (req, res) => {
    try { ok(res, M(req).resumenTierra(req.query.anio ? parseInt(req.query.anio, 10) : null)); }
    catch (e) { err(res, e); }
  });

  app.post("/api/costeo/lote-campo", (req, res) => {
    try {
      const b = req.body, db2 = D(req);
      if (b.id) {
        const pc = parseFloat(b.pct_cria) || 0, pr = parseFloat(b.pct_recria) || 0, pk = parseFloat(b.pct_corral) || 0;
        const dom = pc + pr + pk > 0
          ? (pc >= pr && pc >= pk ? "CRIA" : pr >= pk ? "RECRIA" : "CORRAL")
          : (b.sector || "CRIA");
        db2.prepare(`UPDATE costeo_lotes_campo SET nombre=?, ha_totales=?, ha_sembrables=?,
          kg_ha_renta=?, tipo_actual=?, sector=?, pct_cria=?, pct_recria=?, pct_corral=?, activo=?, notas=? WHERE id=?`)
          .run(b.nombre, parseFloat(b.ha_totales) || 0, parseFloat(b.ha_sembrables) || 0,
               parseFloat(b.kg_ha_renta) || 0, (b.tipo_actual || "NATURAL").toUpperCase(), dom, pc, pr, pk,
               b.activo === 0 ? 0 : 1, b.notas || null, b.id);
        /* Si lo mandan a agricultura a mano, sale de ganadería desde hoy */
        if ((b.tipo_actual || "").toUpperCase() === "AGRICULTURA") {
          db2.prepare("UPDATE costeo_lote_sector SET activo = 0, fecha_hasta = ? WHERE lote_campo_id = ? AND activo = 1").run(hoy(), b.id);
          db2.prepare("INSERT INTO costeo_lote_sector (lote_campo_id,sector,fecha_desde,notas) VALUES (?,'AGRICULTURA',?,'manual')").run(b.id, hoy());
        } else {
          db2.prepare("UPDATE costeo_lote_sector SET activo = 0, fecha_hasta = ? WHERE lote_campo_id = ? AND activo = 1 AND sector = 'AGRICULTURA'").run(hoy(), b.id);
        }
        return ok(res, { id: b.id });
      }
      const sec = b.sector || "CRIA";
      const i = db2.prepare(`INSERT INTO costeo_lotes_campo
        (nombre,ha_totales,ha_sembrables,kg_ha_renta,sector,pct_cria,pct_recria,pct_corral,notas)
        VALUES (?,?,?,?,?,?,?,?,?)`).run(b.nombre, parseFloat(b.ha_totales) || 0,
        parseFloat(b.ha_sembrables) || 0, parseFloat(b.kg_ha_renta) || 35, sec,
        parseFloat(b.pct_cria) || (sec === "CRIA" ? 100 : 0),
        parseFloat(b.pct_recria) || (sec === "RECRIA" ? 100 : 0),
        parseFloat(b.pct_corral) || (sec === "CORRAL" ? 100 : 0), b.notas || null);
      ok(res, { id: Number(i.lastInsertRowid) });
    } catch (e) { err(res, e); }
  });

  /* Sincroniza con IMPROLUX. El vínculo es el ID, no el nombre: así un lote
     renombrado allá se actualiza acá en vez de duplicarse. El nombre viejo se
     guarda para que las órdenes de trabajo anteriores sigan matcheando. */
  app.post("/api/costeo/tierra/sync-improlux", async (req, res) => {
    try {
      const base = ((req.body && req.body.url) || process.env.IMPROLUX_URL
                 || "https://improlux-bot-production.up.railway.app").replace(/\/$/, "");
      const r = await fetch(base + "/api/lotes", { headers: { Accept: "application/json" } });
      if (!r.ok) throw new Error("IMPROLUX respondió " + r.status);
      const lotes = await r.json();
      if (!Array.isArray(lotes)) throw new Error("Respuesta inesperada de IMPROLUX");
      const db2 = D(req);
      const kgDef = parseFloat(M(req).cfg("kg_ha_default", "35")) || 35;
      const res2 = { nuevos: 0, actualizados: 0, renombrados: [], sin_ha: [],
                     total: lotes.length, huerfanos: [] };
      const vistos = new Set();

      const tx = db2.transaction(() => {
        for (const l of lotes) {
          const nom = String(l.nombre || "").trim();
          if (!nom) continue;
          const ha = parseFloat(l.hectareas) || 0, hs = parseFloat(l.ha_sembrables) || 0;

          /* 1º por id, 2º por nombre para enganchar los que ya estaban */
          let ex = l.id ? db2.prepare("SELECT * FROM costeo_lotes_campo WHERE improlux_id = ?").get(l.id) : null;
          if (!ex) ex = db2.prepare("SELECT * FROM costeo_lotes_campo WHERE UPPER(TRIM(nombre)) = UPPER(TRIM(?))").get(nom);

          if (ex) {
            vistos.add(ex.id);
            if (ex.nombre.trim().toUpperCase() !== nom.toUpperCase()) {
              const prev = ex.nombres_previos ? JSON.parse(ex.nombres_previos) : [];
              if (!prev.includes(ex.nombre)) prev.push(ex.nombre);
              db2.prepare("UPDATE costeo_lotes_campo SET nombre = ?, nombres_previos = ? WHERE id = ?")
                .run(nom, JSON.stringify(prev), ex.id);
              res2.renombrados.push(`${ex.nombre} → ${nom}`);
            }
            db2.prepare("UPDATE costeo_lotes_campo SET improlux_id = ?, ha_totales = ?, ha_sembrables = ? WHERE id = ?")
              .run(l.id || ex.improlux_id, ha, hs, ex.id);
            res2.actualizados++;
          } else {
            const forestal = /ISLA|FOREST|EUCALIPT/i.test(nom);
            const i = db2.prepare(`INSERT INTO costeo_lotes_campo
              (improlux_id,nombre,ha_totales,ha_sembrables,kg_ha_renta,tipo_actual,sector,pct_cria,notas)
              VALUES (?,?,?,?,?,'NATURAL','CRIA',100,?)`).run(l.id || null, nom, ha, hs,
              forestal ? 7 : kgDef, forestal ? "Renta menor por la forestación" : null);
            vistos.add(Number(i.lastInsertRowid));
            res2.nuevos++;
          }
          if (!(ha > 0)) res2.sin_ha.push(nom);
        }

        /* Los que ya no están en IMPROLUX se desactivan, no se borran:
           tienen historial de costo colgando. */
        const todos = db2.prepare("SELECT id, nombre, activo FROM costeo_lotes_campo").all();
        for (const t of todos) {
          if (vistos.has(t.id)) {
            if (!t.activo) db2.prepare("UPDATE costeo_lotes_campo SET activo = 1 WHERE id = ?").run(t.id);
          } else if (t.activo) {
            db2.prepare("UPDATE costeo_lotes_campo SET activo = 0 WHERE id = ?").run(t.id);
            res2.huerfanos.push(t.nombre);
          }
        }
      });
      tx();
      ok(res, res2);
    } catch (e) { err(res, e); }
  });

  /* Implantaciones: traídas de las órdenes de trabajo o cargadas a mano */
  app.get("/api/costeo/implantaciones", (req, res) => {
    ok(res, { implantaciones: D(req).prepare(`SELECT i.*, l.nombre lote
      FROM costeo_implantaciones i LEFT JOIN costeo_lotes_campo l ON l.id = i.lote_campo_id
      ORDER BY i.anio DESC, l.nombre`).all() });
  });
  app.post("/api/costeo/implantacion", (req, res) => {
    try {
      const b = req.body;
      const vida = b.vida_util ? parseFloat(b.vida_util)
                 : (String(b.tipo).toUpperCase() === "PASTURA" ? 5 : 1);
      if (b.id) {
        D(req).prepare(`UPDATE costeo_implantaciones SET lote_campo_id=?, tipo=?, anio=?,
          costo_total=?, ha=?, vida_util=?, notas=? WHERE id=?`)
          .run(b.lote_campo_id, String(b.tipo).toUpperCase(), parseInt(b.anio, 10),
               parseFloat(b.costo_total) || 0, parseFloat(b.ha) || 0, vida, b.notas || null, b.id);
        return ok(res, { id: b.id });
      }
      const i = D(req).prepare(`INSERT INTO costeo_implantaciones
        (lote_campo_id,tipo,anio,costo_total,ha,vida_util,orden_improlux,notas)
        VALUES (?,?,?,?,?,?,?,?)`).run(b.lote_campo_id, String(b.tipo).toUpperCase(),
        parseInt(b.anio, 10), parseFloat(b.costo_total) || 0, parseFloat(b.ha) || 0,
        vida, b.orden_improlux || null, b.notas || null);
      ok(res, { id: Number(i.lastInsertRowid) });
    } catch (e) { err(res, e); }
  });
  app.delete("/api/costeo/implantacion/:id", (req, res) => {
    D(req).prepare("DELETE FROM costeo_implantaciones WHERE id = ?").run(req.params.id);
    ok(res, { mensaje: "Implantación eliminada" });
  });

  /* Órdenes de trabajo de IMPROLUX → implantaciones.
     Solo entran las que declaran qué implantan. Si la orden no dice nada,
     es un trabajo que no deja pasto y no suma costo de tierra. */
  const VIDA_TIPO_IMP = { PASTURA: 5, VERDEO: 1, FERTILIZACION: 1, CONTROL: 1 };
  /* Agricultura y reserva sacan el lote de ganadería: su costo deja de caer
     sobre los animales y pasa a ser costo agrícola. La reserva vuelve después
     como ración, cuando se consume el silo o los fardos. */
  const SACA_DE_GANADERIA = new Set(["AGRICULTURA", "RESERVA"]);
  const TIPO_A_ESTADO = { PASTURA: "PASTURA", VERDEO: "VERDEO",
                          AGRICULTURA: "AGRICULTURA", RESERVA: "AGRICULTURA" };

  /* Fecha del primer trabajo ejecutado de la orden */
  function primerTrabajo(o) {
    const fechas = (o.items || [])
      .filter(i => i.ejecutado && i.fecha_ejecucion)
      .map(i => String(i.fecha_ejecucion).slice(0, 10))
      .sort();
    if (fechas.length) return fechas[0];
    if (o.fecha_ejecucion) return String(o.fecha_ejecucion).slice(0, 10);
    return (parseInt(o.anio, 10) || new Date().getUTCFullYear()) + "-01-01";
  }

  async function traerOrdenes() {
    const base = (process.env.IMPROLUX_URL || "https://improlux-bot-production.up.railway.app").replace(/\/$/, "");
    for (const ruta of ["/api/ordenes", "/api/ordenes-trabajo", "/api/ot"]) {
      try {
        const r = await fetch(base + ruta, { headers: { Accept: "application/json" } });
        if (!r.ok) continue;
        const j = await r.json();
        const arr = Array.isArray(j) ? j : (j.ordenes || j.data || []);
        if (Array.isArray(arr)) return { ruta, ordenes: arr };
      } catch (e) {}
    }
    throw new Error("No pude leer las órdenes de trabajo de IMPROLUX");
  }

  app.get("/api/costeo/ordenes-improlux", async (req, res) => {
    try {
      const { ruta, ordenes } = await traerOrdenes();
      const db2 = D(req);
      const yaImp = new Set(db2.prepare("SELECT orden_improlux FROM costeo_implantaciones WHERE orden_improlux IS NOT NULL")
        .all().map(x => x.orden_improlux));
      ok(res, { ruta, ordenes: ordenes.map(o => ({ ...o, ya_importada: yaImp.has(o.id) })) });
    } catch (e) { err(res, e); }
  });

  app.post("/api/costeo/importar-ordenes", async (req, res) => {
    try {
      const { ordenes } = await traerOrdenes();
      const db2 = D(req);
      const res2 = { revisadas: ordenes.length, importadas: 0, sin_tipo: 0,
                     sin_ejecutar: 0, sin_lote: [], ya_estaban: 0,
                     a_agricultura: 0, de_vuelta: 0, detalle: [] };
      const ins = db2.prepare(`INSERT INTO costeo_implantaciones
        (lote_campo_id,tipo,anio,costo_total,ha,vida_util,orden_improlux,notas)
        VALUES (?,?,?,?,?,?,?,?)`);

      const tx = db2.transaction(() => {
        for (const o of ordenes) {
          const tipo = String(o.tipo_implantacion || "").toUpperCase();
          if (!tipo) { res2.sin_tipo++; continue; }
          if (!VIDA_TIPO_IMP[tipo] && !SACA_DE_GANADERIA.has(tipo)) { res2.sin_tipo++; continue; }
          /* Si la orden ya estaba pero se ejecutó más desde entonces, se rehace:
             el costo de una siembra se completa a medida que se ejecutan los items. */
          const previas = db2.prepare("SELECT COALESCE(SUM(costo_total),0) t, COUNT(*) n FROM costeo_implantaciones WHERE orden_improlux = ?").get(o.id);
          const ejecutado = parseFloat(o.total_ejecutado) || 0;
          if (previas.n > 0) {
            if (Math.abs(previas.t - ejecutado) < 0.5) { res2.ya_estaban++; continue; }
            db2.prepare("DELETE FROM costeo_implantaciones WHERE orden_improlux = ?").run(o.id);
            res2.actualizadas = (res2.actualizadas || 0) + 1;
          }
          /* Solo lo que realmente se hizo: lo planificado todavía no es un costo. */
          const costo = parseFloat(o.total_ejecutado) || 0;
          if (!(costo > 0)) { res2.sin_ejecutar++; continue; }

          /* La orden puede abarcar varios lotes: se reparte por hectáreas */
          const nombres = String(o.lote || "").split(/[,;+]/).map(x => x.trim()).filter(Boolean);
          if (!nombres.length) { res2.sin_lote.push(o.titulo || o.id); continue; }
          /* Busca por nombre actual y, si no, por los nombres que tuvo antes:
             una orden vieja puede traer el nombre anterior del lote. */
          const buscarLote = n => db2.prepare("SELECT * FROM costeo_lotes_campo WHERE UPPER(TRIM(nombre)) = UPPER(TRIM(?))").get(n)
            || db2.prepare(`SELECT * FROM costeo_lotes_campo
                 WHERE nombres_previos IS NOT NULL AND UPPER(nombres_previos) LIKE UPPER(?)`).get('%"' + n + '"%');
          const lotes = nombres.map(buscarLote).filter(Boolean);
          if (!lotes.length) { res2.sin_lote.push(nombres.join(", ")); continue; }

          const desde = primerTrabajo(o);
          const nota = `OT ${o.numero || o.id}/${o.anio || ""} · ${o.titulo || ""}`.trim();

          if (SACA_DE_GANADERIA.has(tipo)) {
            /* El lote entra en proceso agrícola con el primer trabajo. Desde esa
               fecha su costo deja de repartirse entre los animales. */
            for (const l of lotes) {
              db2.prepare("UPDATE costeo_lote_sector SET activo = 0, fecha_hasta = ? WHERE lote_campo_id = ? AND activo = 1").run(desde, l.id);
              db2.prepare(`INSERT INTO costeo_lote_sector (lote_campo_id,sector,fecha_desde,notas)
                VALUES (?,'AGRICULTURA',?,?)`).run(l.id, desde, nota);
              db2.prepare("UPDATE costeo_lotes_campo SET tipo_actual = 'AGRICULTURA' WHERE id = ?").run(l.id);
            }
            res2.a_agricultura++;
            res2.detalle.push({ orden: `${o.numero || o.id}/${o.anio || ""}`, tipo,
              lotes: lotes.map(l => l.nombre), desde, costo, nota: "sale de ganadería" });
            continue;
          }

          /* Una siembra de pasto devuelve el lote a la ganadería */
          const haTot = lotes.reduce((t, l) => t + (l.ha_totales || 0), 0) || lotes.length;
          for (const l of lotes) {
            const parte = (l.ha_totales || 1) / haTot;
            ins.run(l.id, tipo, parseInt(o.anio, 10) || new Date().getUTCFullYear(),
              costo * parte, l.ha_totales || 0, VIDA_TIPO_IMP[tipo], o.id, nota);
            if (TIPO_A_ESTADO[tipo]) db2.prepare("UPDATE costeo_lotes_campo SET tipo_actual = ? WHERE id = ?").run(TIPO_A_ESTADO[tipo], l.id);
            if (tipo === "PASTURA" || tipo === "VERDEO") {
              const vig = db2.prepare(`SELECT id FROM costeo_lote_sector
                WHERE lote_campo_id = ? AND activo = 1 AND sector = 'AGRICULTURA'`).get(l.id);
              if (vig) {
                db2.prepare("UPDATE costeo_lote_sector SET activo = 0, fecha_hasta = ? WHERE id = ?").run(desde, vig.id);
                res2.de_vuelta++;
              }
            }
          }
          res2.importadas++;
          res2.detalle.push({ orden: `${o.numero || o.id}/${o.anio || ""}`, tipo,
            lotes: lotes.map(l => l.nombre), costo });
        }
      });
      tx();
      ok(res, res2);
    } catch (e) { err(res, e); }
  });

  /* Reasignación temporal de un lote a otro sector */
  app.post("/api/costeo/lote-campo/sector-temporal", (req, res) => {
    try {
      const b = req.body;
      D(req).prepare("UPDATE costeo_lote_sector SET activo = 0, fecha_hasta = ? WHERE lote_campo_id = ? AND activo = 1")
        .run(b.fecha_desde || hoy(), b.lote_campo_id);
      if (b.sector) {
        D(req).prepare(`INSERT INTO costeo_lote_sector
          (lote_campo_id,sector,pct_cria,pct_recria,pct_corral,fecha_desde,fecha_hasta,notas)
          VALUES (?,?,?,?,?,?,?,?)`).run(b.lote_campo_id, b.sector,
          parseFloat(b.pct_cria) || null, parseFloat(b.pct_recria) || null, parseFloat(b.pct_corral) || null,
          b.fecha_desde || hoy(), b.fecha_hasta || null, b.notas || null);
      }
      ok(res, { mensaje: "Sector actualizado" });
    } catch (e) { err(res, e); }
  });

  /* Qué lotes de ADE son corral */
  app.get("/api/costeo/sector-ade", (req, res) => {
    ok(res, { corrales: D(req).prepare(`SELECT sa.*, l.nombre,
      (SELECT COUNT(*) FROM lote_animales la WHERE la.lote_id = sa.lote_id) animales
      FROM costeo_sector_ade sa LEFT JOIN lotes l ON l.id = sa.lote_id`).all() });
  });
  app.post("/api/costeo/sector-ade", (req, res) => {
    try {
      const { lote_id, sector } = req.body;
      if (!sector || sector === "AUTO") D(req).prepare("DELETE FROM costeo_sector_ade WHERE lote_id = ?").run(lote_id);
      else D(req).prepare(`INSERT INTO costeo_sector_ade (lote_id,sector) VALUES (?,?)
        ON CONFLICT(lote_id) DO UPDATE SET sector = excluded.sector`).run(lote_id, sector);
      ok(res, { mensaje: "Listo" });
    } catch (e) { err(res, e); }
  });

  app.post("/api/costeo/tierra/consolidar", (req, res) => {
    try { ok(res, M(req).consolidarSectores(req.body.desde, req.body.hasta)); } catch (e) { err(res, e); }
  });

  app.get("/api/costeo/config", (req, res) => {
    ok(res, { config: D(req).prepare("SELECT * FROM costeo_config").all() });
  });
  app.post("/api/costeo/config", (req, res) => {
    try { M(req).setCfg(req.body.clave, req.body.valor); ok(res, { mensaje: "Guardado" }); }
    catch (e) { err(res, e); }
  });

  /* ── ESTRUCTURA Y COSTO POR UG ── */
  app.get("/api/costeo/estructura", (req, res) => {
    try {
      ok(res, { conceptos: D(req).prepare("SELECT * FROM costeo_estructura ORDER BY activo DESC, monto_anual DESC").all(),
                resumen: M(req).estructuraAnual(),
                pendientes: D(req).prepare("SELECT COUNT(*) n, COALESCE(SUM(monto_dia),0) t FROM costeo_estructura_dias WHERE consolidado = 0").get() });
    } catch (e) { err(res, e); }
  });
  app.post("/api/costeo/estructura", (req, res) => {
    try {
      const b = req.body;
      if (b.id) {
        D(req).prepare(`UPDATE costeo_estructura SET concepto=?, categoria=?, monto_anual=?,
          pct_ganaderia=?, activo=?, notas=? WHERE id=?`)
          .run(b.concepto, b.categoria || "ESTRUCTURA", parseFloat(b.monto_anual) || 0,
               parseFloat(b.pct_ganaderia) || 100, b.activo === 0 ? 0 : 1, b.notas || null, b.id);
        return ok(res, { id: b.id });
      }
      const i = D(req).prepare(`INSERT INTO costeo_estructura (concepto,categoria,monto_anual,pct_ganaderia,notas)
        VALUES (?,?,?,?,?)`).run(b.concepto, b.categoria || "ESTRUCTURA",
        parseFloat(b.monto_anual) || 0, parseFloat(b.pct_ganaderia) || 100, b.notas || null);
      ok(res, { id: Number(i.lastInsertRowid) });
    } catch (e) { err(res, e); }
  });
  app.delete("/api/costeo/estructura/:id", (req, res) => {
    D(req).prepare("DELETE FROM costeo_estructura WHERE id = ?").run(req.params.id);
    ok(res, { mensaje: "Concepto eliminado" });
  });
  app.post("/api/costeo/estructura/consolidar", (req, res) => {
    try { ok(res, M(req).consolidarEstructura(req.body.desde, req.body.hasta)); } catch (e) { err(res, e); }
  });
  app.get("/api/costeo/costo-ug", (req, res) => {
    try { ok(res, M(req).costoPorUG()); } catch (e) { err(res, e); }
  });

  /* ── POTREROS ── */
  app.get("/api/costeo/potreros", (req, res) => {
    try { ok(res, { potreros: M(req).resumenPotreros(),
      asignaciones: D(req).prepare(`SELECT a.*, po.nombre potrero, po.tipo, l.nombre lote,
          (SELECT COUNT(*) FROM lote_animales la WHERE la.lote_id = a.lote_id) animales
        FROM costeo_lote_potrero a
        JOIN costeo_potreros po ON po.id = a.potrero_id
        LEFT JOIN lotes l ON l.id = a.lote_id
        WHERE a.activo = 1 ORDER BY po.nombre`).all() });
    } catch (e) { err(res, e); }
  });

  app.post("/api/costeo/potrero", (req, res) => {
    try {
      const b = req.body;
      if (b.id) {
        D(req).prepare(`UPDATE costeo_potreros SET nombre=?, tipo=?, kg_carne_ha=?, periodo=?,
          ha_totales=?, ha_aprovechables=?, inversion_ha=?, vida_util_anios=?, mantenimiento_ha=?,
          lote_improlux=?, activo=?, notas=? WHERE id=?`)
          .run(b.nombre, b.tipo || "NATURAL", parseFloat(b.kg_carne_ha) || 0, b.periodo || "ANUAL",
               parseFloat(b.ha_totales) || 0, parseFloat(b.ha_aprovechables) || 0,
               parseFloat(b.inversion_ha) || 0,
               b.vida_util_anios !== undefined && b.vida_util_anios !== "" ? parseFloat(b.vida_util_anios) : vidaUtilDe(b.tipo),
               parseFloat(b.mantenimiento_ha) || 0,
               b.lote_improlux || null, b.activo === 0 ? 0 : 1, b.notas || null, b.id);
        return ok(res, { id: b.id });
      }
      const i = D(req).prepare(`INSERT INTO costeo_potreros
        (nombre,tipo,kg_carne_ha,periodo,ha_totales,ha_aprovechables,inversion_ha,vida_util_anios,mantenimiento_ha,lote_improlux,notas)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)`).run(b.nombre, b.tipo || "NATURAL",
        parseFloat(b.kg_carne_ha) || 0, b.periodo || "ANUAL",
        parseFloat(b.ha_totales) || 0, parseFloat(b.ha_aprovechables) || 0,
        parseFloat(b.inversion_ha) || 0,
        b.vida_util_anios !== undefined && b.vida_util_anios !== "" ? parseFloat(b.vida_util_anios) : vidaUtilDe(b.tipo),
        parseFloat(b.mantenimiento_ha) || 0,
        b.lote_improlux || null, b.notas || null);
      ok(res, { id: Number(i.lastInsertRowid) });
    } catch (e) { err(res, e); }
  });

  app.post("/api/costeo/potrero/asignar", (req, res) => {
    try { ok(res, M(req).asignarPotrero(req.body.lote_id, req.body.potrero_id, req.body.fecha)); }
    catch (e) { err(res, e); }
  });
  app.post("/api/costeo/potrero/sacar", (req, res) => {
    try { ok(res, M(req).sacarDePotrero(req.body.lote_id, req.body.fecha)); } catch (e) { err(res, e); }
  });
  app.post("/api/costeo/potrero/consolidar", (req, res) => {
    try { ok(res, M(req).consolidarPotrero(req.body.desde, req.body.hasta)); } catch (e) { err(res, e); }
  });

  /* Trae las hectáreas de los lotes de IMPROLUX para no cargarlas a mano */
  app.post("/api/costeo/potreros/sync-improlux", async (req, res) => {
    try {
      const base = ((req.body && req.body.url) || process.env.IMPROLUX_URL
                 || "https://improlux-bot-production.up.railway.app").replace(/\/$/, "");
      const r = await fetch(base + "/api/lotes", { headers: { Accept: "application/json" } });
      if (!r.ok) throw new Error("IMPROLUX respondió " + r.status);
      const lotes = await r.json();
      if (!Array.isArray(lotes)) throw new Error("Respuesta inesperada de IMPROLUX");
      const db2 = D(req);
      const up = db2.prepare(`UPDATE costeo_potreros SET ha_totales = ?, ha_aprovechables = ?
        WHERE UPPER(TRIM(lote_improlux)) = UPPER(TRIM(?))`);
      let n = 0;
      const tx = db2.transaction(() => {
        lotes.forEach(l => {
          const c = up.run(l.hectareas || 0, l.ha_sembrables || l.hectareas || 0, l.nombre);
          n += c.changes;
        });
      });
      tx();
      ok(res, { actualizados: n, lotes_improlux: lotes.map(l => ({ nombre: l.nombre, ha: l.hectareas, ha_sembrables: l.ha_sembrables })) });
    } catch (e) { err(res, e); }
  });

  /* ── IATF masiva ── */
  app.post("/api/costeo/iatf", async (req, res) => {
    try {
      const b = req.body;
      const m = M(req), db2 = D(req);
      let animales = [];
      if (b.lote_id) {
        animales = db2.prepare(`SELECT a.id, a.rp FROM animales a
          JOIN lote_animales la ON la.animal_id = a.id
          WHERE la.lote_id = ? AND a.estado = 'ACTIVO'`).all(b.lote_id);
      } else if (Array.isArray(b.rps)) {
        b.rps.forEach(rp => { const a = m.animal(String(rp).trim()); if (a) animales.push({ id: a.id, rp: a.rp }); });
      }
      if (!animales.length) return err(res, new Error("Seleccioná animales o un lote"));

      const base = m.costearIATF({ ...b, animales });
      let extra = "", costoInsumos = 0;

      /* Hormonas y descartables salen del stock de IMPROLUX */
      const prods = (b.productos || []).filter(p => p.producto && parseFloat(p.dosis) > 0);
      const url = (process.env.IMPROLUX_URL || "https://improlux-bot-production.up.railway.app").replace(/\/$/, "");
      const insC = db2.prepare("INSERT INTO costos (animal_id,fecha,concepto,detalle,monto,moneda) VALUES (?,?,'IATF',?,?,'USD')");
      for (const p of prods) {
        const total = parseFloat(p.dosis) * animales.length;
        try {
          const resp = await fetch(url + "/api/stock/aplicar", {
            method: "POST", headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ producto: p.producto, cantidad: total,
              fecha: base.fecha, detalle: `IATF · ${animales.length} vientres` })
          });
          const d = await resp.json().catch(() => ({}));
          if (d.ok) {
            const ct = (d.costo_total != null) ? parseFloat(d.costo_total)
              : (d.costo_unitario != null) ? parseFloat(d.costo_unitario) * total : 0;
            if (ct > 0) {
              const pc = ct / animales.length;
              const tx = db2.transaction(() => {
                animales.forEach(a => insC.run(a.id, base.fecha, `costeo:improlux ${d.producto} ${p.dosis}${d.unidad || ''}`, pc));
              });
              tx();
              costoInsumos += ct;
              base.lineas.push(`  • ${d.producto}: ${total} ${d.unidad || ''} · US$ ${ct.toFixed(2)} → US$ ${pc.toFixed(2)} c/u`);
            } else {
              base.lineas.push(`  • ${d.producto}: sin precio en IMPROLUX`);
            }
            if (d.negativo) extra += `\n⚠️ ${d.producto} quedó en negativo.`;
          } else {
            base.lineas.push(`  • ${p.producto}: ⚠️ no está en el stock (${d.error || "no encontrado"})`);
          }
        } catch (e) {
          base.lineas.push(`  • ${p.producto}: ⚠️ sin conexión con IMPROLUX`);
        }
      }

      const total = base.costo_base + costoInsumos;
      ok(res, { animales: base.animales, fecha: base.fecha,
        costo_total: total, costo_por_animal: total / base.animales,
        mensaje: `🧬 *IATF* — ${base.animales} vientres · ${base.fecha}\n${base.lineas.join("\n")}\n\n` +
                 `💰 Total: US$ ${total.toFixed(2)} · US$ ${(total / base.animales).toFixed(2)} por vientre${extra}` });
    } catch (e) { err(res, e); }
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
  app.get("/api/costeo/consumos-pendientes", (req, res) => {
    const r = D(req).prepare(`SELECT COUNT(*) pendientes, COALESCE(SUM(kg),0) kg,
      COALESCE(SUM(costo_total),0) costo, MIN(fecha) desde, MAX(error_envio) ultimo_error
      FROM costeo_dieta_dias WHERE enviado = 0`).get();
    ok(res, r);
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

module.exports = { init, crearTablas, motor, parsearACG, bajarACG, vidaUtilDe, VIDA_UTIL_TIPO, PASTO_REF,
                   cicloDe, rangoCiclo, ETAPAS, VERSION };
