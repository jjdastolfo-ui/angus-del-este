const express = require("express");
const twilio = require("twilio");
const Anthropic = require("@anthropic-ai/sdk");
const Database = require("better-sqlite3");
const PDFDocument = require("pdfkit");
const path = require("path");
const fs = require("fs");

const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

// ── CORS ──────────────────────────────────────────────────────────────────────
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

// ── CONFIGURACIÓN ─────────────────────────────────────────────────────────────
const NUMERO_ADMIN = process.env.NUMERO_ADMIN || "";

const CATEGORIAS = [
  "ALQUILER","ALQUILER ESTRUCTURA","ALIMENTACION RECRIA","ALIMENTACION CRIA",
  "TERMINACION","INSUMOS VETERINARIOS","TRABAJOS VETERINARIOS",
  "COMBUSTIBLE CAMPO","COMBUSTIBLE VIATICOS","SUELDO JORNAL","SUELDO ENCARGADO","SUELDO ADM",
  "VERDEOS Y PASTURAS","ESTRUCTURA GANADERA","MANTENIMIENTO CAMPO",
  "MANTENIMIENTO MAQUINARIA","GASTOS VENTAS GANADERAS","INVERSION MAQUINARIA",
  "COMPRA GANADO","COMPRA HERRAMIENTAS","BPS","GASTOS ADM","PROVISTA",
  "VEHICULOS","TELEFONO","INTERESES","GASTO BANCARIO","AMORTIZACION MAQUINARIA","OTROS"
];

const DB_PATH = process.env.DB_PATH || "./improlux.db";
const DB_DIR = path.dirname(DB_PATH);
if (!fs.existsSync(DB_DIR)) fs.mkdirSync(DB_DIR, { recursive: true });
const db = new Database(DB_PATH);

// ── BASE DE DATOS ─────────────────────────────────────────────────────────────
db.exec(`
  CREATE TABLE IF NOT EXISTS transacciones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha TEXT NOT NULL,
    concepto TEXT NOT NULL,
    detalle TEXT,
    ingreso REAL DEFAULT 0,
    egreso REAL DEFAULT 0,
    proveedor TEXT,
    es_cc INTEGER DEFAULT 0,
    tc REAL,
    fuente TEXT DEFAULT 'whatsapp',
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS cuentas_corrientes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proveedor TEXT NOT NULL UNIQUE,
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS cheques (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha_emision TEXT NOT NULL,
    fecha_cobro TEXT,
    tipo TEXT NOT NULL,
    proveedor TEXT,
    monto REAL NOT NULL,
    estado TEXT DEFAULT 'PENDIENTE',
    banco TEXT DEFAULT 'BROU',
    concepto TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS inversores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inversor TEXT NOT NULL,
    fecha_ingreso TEXT NOT NULL,
    capital REAL NOT NULL,
    tasa REAL NOT NULL,
    fecha_vencimiento TEXT,
    deuda_actual REAL,
    estado TEXT DEFAULT 'ACTIVO',
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS sesiones (
    usuario TEXT PRIMARY KEY,
    historial TEXT DEFAULT '[]',
    updated_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS cc_movimientos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha TEXT NOT NULL,
    proveedor TEXT NOT NULL,
    monto REAL NOT NULL,
    medio TEXT DEFAULT 'EFECTIVO',
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS presupuestos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ciclo TEXT NOT NULL,
    concepto TEXT NOT NULL,
    monto_anual REAL NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(ciclo, concepto)
  );

  CREATE TABLE IF NOT EXISTS bienes_muebles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL,
    categoria TEXT,
    valor_compra REAL NOT NULL,
    fecha_compra TEXT NOT NULL,
    vida_util_anios REAL NOT NULL DEFAULT 10,
    valor_residual REAL DEFAULT 0,
    notas TEXT,
    activo INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS amortizaciones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bien_id INTEGER NOT NULL,
    ciclo TEXT NOT NULL,
    monto REAL NOT NULL,
    transaccion_id INTEGER,
    fecha TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(bien_id, ciclo)
  );

  CREATE TABLE IF NOT EXISTS stock_ganadero (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campo TEXT DEFAULT 'LA AMISTAD',
    categoria TEXT NOT NULL,
    cantidad REAL DEFAULT 0,
    valor_cabeza REAL DEFAULT 0,
    orden INTEGER DEFAULT 0,
    origen TEXT DEFAULT 'manual',
    notas TEXT,
    updated_at TEXT DEFAULT (datetime('now')),
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS dividendos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha TEXT NOT NULL,
    socio TEXT NOT NULL,
    monto REAL NOT NULL,
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS lotes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL UNIQUE,
    hectareas REAL,
    ha_sembrables REAL,
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS laboreos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lote TEXT NOT NULL,
    tipo TEXT NOT NULL,
    descripcion TEXT,
    ciclo TEXT NOT NULL,
    estado TEXT DEFAULT 'PLANIFICADO',
    fecha_ejecucion TEXT,
    total_presupuestado REAL DEFAULT 0,
    total_ejecutado REAL DEFAULT 0,
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS laboreo_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    laboreo_id INTEGER NOT NULL,
    categoria TEXT NOT NULL,
    descripcion TEXT NOT NULL,
    cantidad REAL DEFAULT 0,
    unidad TEXT DEFAULT 'ha',
    precio_unitario REAL DEFAULT 0,
    total REAL DEFAULT 0,
    ejecutado INTEGER DEFAULT 0,
    fecha_ejecucion TEXT,
    notas_ejecucion TEXT,
    FOREIGN KEY(laboreo_id) REFERENCES laboreos(id)
  );

  -- ===== ÓRDENES DE TRABAJO + STOCK (v4.3) =====
  CREATE TABLE IF NOT EXISTS stock_productos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL UNIQUE,
    categoria TEXT DEFAULT 'OTRO',      -- SEMILLA / FERTILIZANTE / AGROQUIMICO / OTRO
    unidad TEXT DEFAULT 'kg',
    cantidad REAL DEFAULT 0,            -- stock actual
    precio_unitario REAL DEFAULT 0,     -- costo promedio ponderado (USD)
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS stock_movimientos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    producto_id INTEGER NOT NULL,
    fecha TEXT NOT NULL,
    tipo TEXT NOT NULL,                 -- ENTRADA / SALIDA / AJUSTE
    cantidad REAL NOT NULL,
    precio_unitario REAL DEFAULT 0,
    orden_id INTEGER,                   -- si la salida viene de ejecutar una orden
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(producto_id) REFERENCES stock_productos(id)
  );

  CREATE TABLE IF NOT EXISTS ordenes_trabajo (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    numero INTEGER NOT NULL,            -- correlativo dentro del año
    anio INTEGER NOT NULL,
    lote TEXT,
    titulo TEXT,
    ciclo TEXT,
    hectareas REAL DEFAULT 0,           -- ha a trabajar (default: ha sembrables del lote)
    estado TEXT DEFAULT 'PLANIFICADA',  -- PLANIFICADA / EN_EJECUCION / EJECUTADA
    notas TEXT,
    total_planificado REAL DEFAULT 0,
    total_ejecutado REAL DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS orden_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    orden_id INTEGER NOT NULL,
    tipo TEXT DEFAULT 'INSUMO',         -- INSUMO (linkea stock) / SERVICIO (mano de obra)
    etapa TEXT DEFAULT 'GENERAL',       -- etapa libre: PRE-EMERGENCIA / SIEMBRA / POST-EMERGENCIA / COSECHA / APLICACION / LABOREO / etc
    producto_id INTEGER,               -- solo INSUMO: link a stock_productos
    descripcion TEXT NOT NULL,
    dosis REAL DEFAULT 0,              -- dosis por hectárea (opcional); cantidad = dosis × hectareas
    cantidad REAL DEFAULT 0,            -- cantidad planificada
    unidad TEXT DEFAULT 'kg',
    precio_unitario REAL DEFAULT 0,
    total REAL DEFAULT 0,              -- planificado = cantidad × precio_unitario
    ejecutado INTEGER DEFAULT 0,
    cantidad_ejecutada REAL DEFAULT 0,
    total_ejecutado REAL DEFAULT 0,
    fecha_ejecucion TEXT,
    notas TEXT,
    FOREIGN KEY(orden_id) REFERENCES ordenes_trabajo(id),
    FOREIGN KEY(producto_id) REFERENCES stock_productos(id)
  );

  CREATE TABLE IF NOT EXISTS orden_cambios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    orden_id INTEGER NOT NULL,
    fecha TEXT NOT NULL,
    texto TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(orden_id) REFERENCES ordenes_trabajo(id)
  );
`);

// Inicializar proveedores conocidos si no existen
const proveedoresIniciales = [
  { proveedor: 'AMAKAIK', notas: 'Compra de ganado' },
  { proveedor: 'MERCADO RURAL', notas: 'Insumos varios - cuenta corriente' },
  { proveedor: 'ZAMBRANO INSUMOS', notas: 'Insumos veterinarios y campo' },
  { proveedor: 'ZAMBRANO Y CIA', notas: 'Insumos veterinarios y campo' },
  { proveedor: 'DIEGO PIOLI', notas: 'Cuenta corriente - pagos frecuentes' },
  { proveedor: 'SELECTA SRL', notas: 'Servicios' },
  { proveedor: 'INVITRO', notas: 'Servicios veterinarios / genética' },
];
const stmtProv = db.prepare('INSERT OR IGNORE INTO cuentas_corrientes (proveedor, notas) VALUES (?, ?)');
proveedoresIniciales.forEach(p => stmtProv.run(p.proveedor, p.notas));
// Inicializar lotes de La Amistad si no existen
const lotesIniciales = [
  { nombre: 'EUCALIPTUS', hectareas: 11.90, ha_sembrables: 11.90 },
  { nombre: 'SAUZAL CHICO', hectareas: 5.51, ha_sembrables: 4.50 },
  { nombre: 'SAUZAL GRANDE', hectareas: 4.99, ha_sembrables: 4.99 },
  { nombre: 'BAÑADO', hectareas: 3.74, ha_sembrables: 0 },
  { nombre: 'ZARZO', hectareas: 10.20, ha_sembrables: 0 },
  { nombre: 'CORONILLAS', hectareas: 2.68, ha_sembrables: 2.00 },
  { nombre: 'VALLE 3', hectareas: 4.00, ha_sembrables: 4.00 },
  { nombre: 'VALLE 2', hectareas: 7.11, ha_sembrables: 5.10 },
  { nombre: 'VALLE 1', hectareas: 9.00, ha_sembrables: 5.56 },
  { nombre: 'TREPADA', hectareas: 8.34, ha_sembrables: 2.29 },
  { nombre: 'CORRAL 1', hectareas: 0.70, ha_sembrables: 0.50 },
  { nombre: 'CORRAL 2', hectareas: 1.00, ha_sembrables: 0.50 },
  { nombre: 'CERRO', hectareas: 12.30, ha_sembrables: 2.00 },
];
const stmtLote = db.prepare('INSERT OR IGNORE INTO lotes (nombre, hectareas, ha_sembrables) VALUES (?,?,?)');
lotesIniciales.forEach(l => stmtLote.run(l.nombre, l.hectareas, l.ha_sembrables));

// ── MIGRACIONES ─────────────────────────────────────────────────────────────
try { db.exec(`ALTER TABLE laboreo_items ADD COLUMN ejecutado INTEGER DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE laboreo_items ADD COLUMN fecha_ejecucion TEXT`); } catch(e) {}
try { db.exec(`ALTER TABLE laboreo_items ADD COLUMN notas_ejecucion TEXT`); } catch(e) {}
try { db.exec(`ALTER TABLE laboreos ADD COLUMN total_ejecutado REAL DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE orden_items ADD COLUMN etapa TEXT DEFAULT 'GENERAL'`); } catch(e) {}
try { db.exec(`ALTER TABLE ordenes_trabajo ADD COLUMN hectareas REAL DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE ordenes_trabajo ADD COLUMN lotes TEXT`); } catch(e) {}
try { db.exec(`ALTER TABLE orden_items ADD COLUMN dosis REAL DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE bienes_muebles ADD COLUMN fecha_baja TEXT`); } catch(e) {}
try { db.exec(`ALTER TABLE bienes_muebles ADD COLUMN valor_venta REAL`); } catch(e) {}
try { db.exec(`ALTER TABLE stock_ganadero ADD COLUMN registro TEXT DEFAULT 'GENERAL'`); } catch(e) {}
try { db.exec(`ALTER TABLE stock_ganadero ADD COLUMN kg_estimado REAL DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE stock_ganadero ADD COLUMN cantidad_venta REAL DEFAULT 0`); } catch(e) {}
try { db.exec(`CREATE TABLE IF NOT EXISTS diario_campo (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  campo TEXT DEFAULT 'LA AMISTAD',
  fecha TEXT NOT NULL,
  tipo TEXT DEFAULT 'ACONTECIMIENTO',
  mm REAL,
  titulo TEXT,
  detalle TEXT,
  created_at TEXT DEFAULT (datetime('now'))
)`); } catch(e) {}
try { db.exec(`CREATE TABLE IF NOT EXISTS tareas_campo (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  campo TEXT,
  texto TEXT NOT NULL,
  estado TEXT DEFAULT 'PENDIENTE',
  origen TEXT DEFAULT 'web',
  created_at TEXT DEFAULT (datetime('now')),
  done_at TEXT
)`); } catch(e) {}
try { db.exec(`CREATE TABLE IF NOT EXISTS proyeccion_ajustes (
  mes TEXT PRIMARY KEY,
  egreso_estimado REAL,
  ingreso_estimado REAL,
  notas TEXT,
  updated_at TEXT DEFAULT (datetime('now'))
)`); } catch(e) {}
try { db.exec(`ALTER TABLE lotes ADD COLUMN poligono TEXT`); } catch(e) {}
try { db.exec(`ALTER TABLE inversores ADD COLUMN deuda_actual REAL`); } catch(e) {}
try { db.exec(`ALTER TABLE inversores ADD COLUMN fecha_vencimiento TEXT`); } catch(e) {}
try { db.exec(`ALTER TABLE inversores ADD COLUMN notas TEXT`); } catch(e) {}
try { db.exec(`ALTER TABLE patrimonio_snapshots ADD COLUMN fondo REAL DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE patrimonio_snapshots ADD COLUMN deuda_cheques REAL DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE patrimonio_snapshots ADD COLUMN deuda_cc REAL DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE patrimonio_snapshots ADD COLUMN deuda_inversores REAL DEFAULT 0`); } catch(e) {}
try { db.exec(`ALTER TABLE stock_productos ADD COLUMN rubro TEXT DEFAULT 'AGRICOLA'`); } catch(e) {}
// Qué implanta la orden. Lo lee ADE para saber si el costo se amortiza (pastura,
// 5 años) o va todo al año (verdeo, fertilización, control). Vacío = no implanta.
try { db.exec(`ALTER TABLE ordenes_trabajo ADD COLUMN tipo_implantacion TEXT`); } catch(e) {}
try { db.exec(`CREATE TABLE IF NOT EXISTS config (clave TEXT PRIMARY KEY, valor TEXT, updated_at TEXT DEFAULT (datetime('now')))`); } catch(e) {}
try { db.exec(`CREATE TABLE IF NOT EXISTS patrimonio_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ciclo TEXT,
  fecha TEXT,
  caja REAL DEFAULT 0,
  ganado REAL DEFAULT 0,
  bienes REAL DEFAULT 0,
  stock REAL DEFAULT 0,
  total REAL DEFAULT 0,
  notas TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(ciclo)
)`); } catch(e) {}
try { db.exec(`UPDATE laboreo_items SET ejecutado=0 WHERE ejecutado IS NULL`); } catch(e) {}
// ── BASE DE CAMPO (multi-campo): lo físico se etiqueta por campo ───────────────
try { db.exec(`CREATE TABLE IF NOT EXISTS campos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nombre TEXT NOT NULL UNIQUE,
  activo INTEGER DEFAULT 1,
  orden INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now'))
)`); } catch(e) {}
try { db.exec(`INSERT OR IGNORE INTO campos (nombre, orden) VALUES ('LA AMISTAD', 0)`); } catch(e) {}
try { db.exec(`ALTER TABLE lotes ADD COLUMN campo TEXT DEFAULT 'LA AMISTAD'`); } catch(e) {}
try { db.exec(`ALTER TABLE ordenes_trabajo ADD COLUMN campo TEXT DEFAULT 'LA AMISTAD'`); } catch(e) {}
try { db.exec(`ALTER TABLE stock_productos ADD COLUMN campo TEXT DEFAULT 'LA AMISTAD'`); } catch(e) {}
try { db.exec(`ALTER TABLE bienes_muebles ADD COLUMN campo TEXT DEFAULT 'LA AMISTAD'`); } catch(e) {}
// Rellenar los registros viejos (por si la columna quedó en NULL)
try { db.exec(`UPDATE lotes SET campo='LA AMISTAD' WHERE campo IS NULL OR campo=''`); } catch(e) {}
try { db.exec(`UPDATE ordenes_trabajo SET campo='LA AMISTAD' WHERE campo IS NULL OR campo=''`); } catch(e) {}
try { db.exec(`UPDATE stock_productos SET campo='LA AMISTAD' WHERE campo IS NULL OR campo=''`); } catch(e) {}
try { db.exec(`UPDATE bienes_muebles SET campo='LA AMISTAD' WHERE campo IS NULL OR campo=''`); } catch(e) {}
try { db.exec(`UPDATE stock_ganadero SET campo='LA AMISTAD' WHERE campo IS NULL OR campo=''`); } catch(e) {}
console.log('Migraciones aplicadas');

const twilioClient = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
const anthropic = new Anthropic();
const TWILIO_NUMBER = process.env.TWILIO_NUMBER || "";
const PUBLIC_URL = (process.env.PUBLIC_URL || "").replace(/\/$/, "");
const ADE_URL = (process.env.ADE_URL || "https://angus-del-este-production.up.railway.app").replace(/\/$/, "");

// ── TIPO DE CAMBIO ────────────────────────────────────────────────────────────
let tcCache = { valor: null, fecha: null };

async function getTipoCambio() {
  const ahora = new Date();
  if (tcCache.valor && tcCache.fecha && (ahora - tcCache.fecha) < 60 * 60 * 1000) {
    return tcCache.valor;
  }
  try {
    const resp = await fetch("https://open.er-api.com/v6/latest/USD");
    const data = await resp.json();
    if (data?.rates?.UYU) {
      tcCache = { valor: data.rates.UYU, fecha: ahora };
      console.log(`TC obtenido: $${data.rates.UYU.toFixed(2)} UYU/USD`);
      return data.rates.UYU;
    }
  } catch (e) {
    console.error("Error TC:", e.message);
  }
  return null;
}

// ── HELPERS DB ────────────────────────────────────────────────────────────────
function getHistorial(usuario) {
  const row = db.prepare("SELECT historial FROM sesiones WHERE usuario = ?").get(usuario);
  return row ? JSON.parse(row.historial) : [];
}

function saveHistorial(usuario, historial) {
  const reciente = historial.slice(-20);
  db.prepare(`
    INSERT INTO sesiones (usuario, historial, updated_at) VALUES (?, ?, datetime('now'))
    ON CONFLICT(usuario) DO UPDATE SET historial = excluded.historial, updated_at = excluded.updated_at
  `).run(usuario, JSON.stringify(reciente));
}

function fmt(n) {
  return parseFloat(n).toLocaleString("es-UY", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function getSaldoProveedor(proveedor) {
  // Compras = egresos registrados con ese proveedor (excluye pagos CC)
  const compras = db.prepare(`
    SELECT COALESCE(SUM(egreso), 0) as total
    FROM transacciones
    WHERE LOWER(proveedor) = LOWER(?)
    AND concepto != 'PAGO CUENTA CORRIENTE'
  `).get(proveedor);
  // Pagos = lo registrado en cc_movimientos
  const pagos = db.prepare(`
    SELECT COALESCE(SUM(monto), 0) as total
    FROM cc_movimientos
    WHERE LOWER(proveedor) = LOWER(?)
  `).get(proveedor);
  return (compras.total || 0) - (pagos.total || 0);
}

function getDetalleCuentaCorriente(proveedor) {
  const compras = db.prepare(`
    SELECT fecha, concepto, detalle, egreso as monto, 'COMPRA' as tipo, created_at
    FROM transacciones
    WHERE LOWER(proveedor) = LOWER(?)
    AND concepto != 'PAGO CUENTA CORRIENTE'
    AND egreso > 0
    ORDER BY fecha ASC, created_at ASC
  `).all(proveedor);
  const pagos = db.prepare(`
    SELECT fecha, 'PAGO' as concepto, COALESCE(notas, medio) as detalle, monto, 'PAGO' as tipo, created_at
    FROM cc_movimientos
    WHERE LOWER(proveedor) = LOWER(?)
    ORDER BY fecha ASC, created_at ASC
  `).all(proveedor);
  // Combinar y ordenar por fecha
  const todos = [...compras.map(c => ({...c, signo: -1})), ...pagos.map(p => ({...p, signo: 1}))]
    .sort((a,b) => a.fecha.localeCompare(b.fecha) || a.created_at.localeCompare(b.created_at));
  // Calcular saldo acumulado
  let saldoAcum = 0;
  return todos.map(t => {
    saldoAcum += t.tipo === 'COMPRA' ? t.monto : -t.monto;
    return { ...t, saldo_acumulado: saldoAcum };
  });
}

function getResumenCuentasCorrientes() {
  const proveedores = db.prepare("SELECT * FROM cuentas_corrientes ORDER BY proveedor").all();
  return proveedores.map(p => ({
    ...p,
    saldo: getSaldoProveedor(p.proveedor)
  })).filter(p => p.saldo !== 0);
}

function getUltimasTransacciones(limite = 10) {
  return db.prepare("SELECT * FROM transacciones ORDER BY created_at DESC LIMIT ?").all(limite);
}

function getChequesPendientes() {
  return db.prepare("SELECT * FROM cheques WHERE estado = 'PENDIENTE' ORDER BY fecha_cobro ASC").all();
}

function getInversoresActivos() {
  return db.prepare("SELECT * FROM inversores WHERE estado = 'ACTIVO' ORDER BY inversor").all();
}

function calcularDeudaInversor(inversor) {
  const diasTranscurridos = Math.floor(
    (new Date() - new Date(inversor.fecha_ingreso)) / (1000 * 60 * 60 * 24)
  );
  const interesesAcumulados = inversor.capital * inversor.tasa * (diasTranscurridos / 365);
  return inversor.capital + interesesAcumulados;
}

// ── CICLO GANADERO (marzo a marzo) ────────────────────────────────────────────
function parseCiclo(cicloStr, tipo = 'productivo') {
  // Acepta "25/26", "2025/2026", "25-26", etc.
  const match = String(cicloStr).match(/(\d{2,4})[\/\-](\d{2,4})/);
  if (!match) return null;
  let anioInicio = parseInt(match[1]);
  let anioFin = parseInt(match[2]);
  if (anioInicio < 100) anioInicio += 2000;
  if (anioFin < 100) anioFin += 2000;
  const contable = tipo === 'contable';
  return {
    ciclo: `${anioInicio % 100}/${anioFin % 100}`,
    tipo: contable ? 'contable' : 'productivo',
    mesInicio: contable ? 7 : 3,                 // contable: jul→jun · productivo: mar→feb
    fecha_desde: contable ? `${anioInicio}-07-01` : `${anioInicio}-03-01`,
    fecha_hasta: contable ? `${anioFin}-06-30` : `${anioFin}-02-29`,
    label: `${anioInicio}/${anioFin}`
  };
}

function getCicloActual(tipo = 'productivo') {
  const hoy = new Date();
  const mes = hoy.getMonth() + 1; // 1-12
  const anio = hoy.getFullYear();
  const mesInicio = tipo === 'contable' ? 7 : 3;
  // Si estamos en el mes de inicio o después → ciclo es anio/anio+1; si no, anio-1/anio
  if (mes >= mesInicio) {
    return parseCiclo(`${anio}/${anio + 1}`, tipo);
  } else {
    return parseCiclo(`${anio - 1}/${anio}`, tipo);
  }
}

function getInformeCiclo(cicloStr, tipo = 'productivo') {
  const ciclo = parseCiclo(cicloStr, tipo);
  if (!ciclo) return null;

  const hoy = new Date().toISOString().slice(0, 10);
  const fechaHasta = ciclo.fecha_hasta < hoy ? ciclo.fecha_hasta : hoy;

  const rows = db.prepare(`
    SELECT concepto, 
           SUM(egreso) as total_egreso, 
           SUM(ingreso) as total_ingreso,
           COUNT(*) as cant_movimientos
    FROM transacciones 
    WHERE fecha >= ? AND fecha <= ?
    GROUP BY concepto ORDER BY total_egreso DESC
  `).all(ciclo.fecha_desde, fechaHasta);

  const totalEgresos = rows.reduce((s, r) => s + (r.total_egreso || 0), 0);
  const totalIngresos = rows.reduce((s, r) => s + (r.total_ingreso || 0), 0);
  const totalMovimientos = rows.reduce((s, r) => s + r.cant_movimientos, 0);

  // Presupuestos del ciclo
  const presupuestos = db.prepare(
    "SELECT * FROM presupuestos WHERE ciclo = ?"
  ).all(ciclo.ciclo);
  const presupuestoMap = {};
  presupuestos.forEach(p => { presupuestoMap[p.concepto] = p.monto_anual; });

  return { ciclo, rows, totalEgresos, totalIngresos, totalMovimientos, presupuestoMap, fechaHasta };
}

function getInformeMensual(anio, mes) {
  const periodo = `${anio}-${String(mes).padStart(2, '0')}`;
  const rows = db.prepare(`
    SELECT concepto, 
           SUM(egreso) as total_egreso, 
           SUM(ingreso) as total_ingreso,
           COUNT(*) as cant
    FROM transacciones WHERE fecha LIKE ?
    GROUP BY concepto ORDER BY total_egreso DESC
  `).all(`${periodo}-%`);

  const totalEgresos = rows.reduce((s, r) => s + (r.total_egreso || 0), 0);
  const totalIngresos = rows.reduce((s, r) => s + (r.total_ingreso || 0), 0);

  // Presupuestos del ciclo que contiene ese mes
  const ciclo = mes >= 3
    ? parseCiclo(`${anio}/${anio + 1}`)
    : parseCiclo(`${anio - 1}/${anio}`);
  
  const presupuestos = ciclo ? db.prepare(
    "SELECT * FROM presupuestos WHERE ciclo = ?"
  ).all(ciclo.ciclo) : [];
  const presupuestoMap = {};
  presupuestos.forEach(p => { presupuestoMap[p.concepto] = p.monto_anual / 12; }); // mensualizado

  return { periodo, rows, totalEgresos, totalIngresos, presupuestoMap, ciclo };
}

// ── CRON INFORME MENSUAL WhatsApp ─────────────────────────────────────────────
function scheduleInformeMensual() {
  function checkAndSend() {
    const ahora = new Date();
    if (ahora.getDate() === 1 && ahora.getHours() === 8) {
      // Primer día del mes a las 8am → enviar informe del mes anterior
      const mesAnterior = ahora.getMonth(); // 0-11, el mes actual -1 = mes anterior (0=enero → diciembre año anterior)
      const anio = mesAnterior === 0 ? ahora.getFullYear() - 1 : ahora.getFullYear();
      const mes = mesAnterior === 0 ? 12 : mesAnterior;
      enviarInformeMensualWhatsApp(anio, mes);
    }
  }
  // Chequear cada hora
  setInterval(checkAndSend, 60 * 60 * 1000);
  console.log("📅 Cron de informe mensual programado (1ro de cada mes, 8am)");
}

async function enviarInformeMensualWhatsApp(anio, mes) {
  if (!NUMERO_ADMIN || !TWILIO_NUMBER) {
    console.log("⚠️ No se puede enviar informe: falta NUMERO_ADMIN o TWILIO_NUMBER");
    return;
  }

  const meses = ["","Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"];
  const informe = getInformeMensual(anio, mes);
  
  let msg = `📊 *IMPROLUX — Informe ${meses[mes]} ${anio}*\n\n`;
  
  if (!informe.rows.length) {
    msg += "Sin movimientos en este período.\n";
  } else {
    const lineas = informe.rows.filter(r => r.total_egreso > 0).map(r => {
      const presup = informe.presupuestoMap[r.concepto];
      const pct = presup ? ` (${((r.total_egreso / presup) * 100).toFixed(0)}% presup.)` : "";
      const warn = presup && r.total_egreso > presup ? " ⚠️" : "";
      return `  • ${r.concepto}: $${fmt(r.total_egreso)}${pct}${warn}`;
    });
    msg += lineas.join("\n");
    msg += `\n\n📤 Egresos: $${fmt(informe.totalEgresos)} USD`;
    msg += `\n📥 Ingresos: $${fmt(informe.totalIngresos)} USD`;
    msg += `\n💰 Neto: $${fmt(informe.totalIngresos - informe.totalEgresos)} USD`;
    if (PUBLIC_URL) {
      msg += `\n\n📄 PDF detallado: ${PUBLIC_URL}/api/informe-mensual-pdf?anio=${anio}&mes=${mes}`;
    }
  }

  try {
    await twilioClient.messages.create({
      body: msg,
      from: TWILIO_NUMBER,
      to: NUMERO_ADMIN
    });
    console.log(`✅ Informe mensual enviado a ${NUMERO_ADMIN}`);
  } catch (e) {
    console.error("❌ Error enviando informe:", e.message);
  }
}

// ── CONTEXTO IA ───────────────────────────────────────────────────────────────
// ── HERRAMIENTA DE CONSULTA (el bot lee TODA la base con SQL de solo lectura) ──
const DB_SCHEMA = `Base SQLite de IMPROLUX. Montos en USD. El ciclo ganadero va de MARZO a FEBRERO del año siguiente. Tablas y columnas:
- transacciones(id, fecha 'YYYY-MM-DD', concepto [=categoría de gasto/ingreso], detalle, ingreso, egreso, proveedor, es_cc, tc, fuente) — TODOS los movimientos.
- cuentas_corrientes(id, proveedor, notas). Saldo de un proveedor = SUM(egreso) - SUM(ingreso) de transacciones con ese proveedor.
- cheques(id, fecha_emision, fecha_cobro, tipo 'EMITIDO'/'RECIBIDO', proveedor, monto, estado 'PENDIENTE'/'COBRADO', banco, concepto)
- inversores(id, inversor, fecha_ingreso, capital, tasa, estado, notas)
- dividendos(id, socio, monto, fecha, notas)
- lotes(id, nombre, hectareas, ha_sembrables [=ha aprovechables que se siembran], notas)
- ordenes_trabajo(id, numero, anio [AÑO CALENDARIO: 2026, 2027, etc. — NO es el ciclo ganadero], lote, titulo, ciclo, hectareas, estado 'PLANIFICADA'/'EN_EJECUCION'/'EJECUTADA', total_planificado, total_ejecutado). Un lote multi puede venir como "A + B + C". IMPORTANTE: las órdenes se filtran por anio (año calendario): "las órdenes/insumos de 2026" = WHERE anio = 2026. NO uses el ciclo ganadero (marzo-febrero) para las órdenes; el ciclo es solo para transacciones/finanzas.
- orden_items(id, orden_id, tipo 'INSUMO'/'SERVICIO', etapa, producto_id, descripcion, dosis, cantidad, unidad, precio_unitario, total, ejecutado 0/1, cantidad_ejecutada, total_ejecutado)
- stock_productos(id, nombre, categoria, unidad, cantidad [=stock actual], precio_unitario [=costo promedio])
- stock_movimientos(id, producto_id, fecha, tipo 'ENTRADA'/'SALIDA'/'AJUSTE', cantidad, precio_unitario)
- presupuestos(id, ciclo, concepto, monto_anual)
Insumos faltantes de un año = por cada orden_items con tipo='INSUMO', ejecutado=0 y producto_id, sumar cantidad por producto y restar stock_productos.cantidad.`;

const DASHBOARD_TOOL = {
  name: "consultar_datos",
  description: "Ejecuta una consulta SQL de SOLO LECTURA (SELECT) sobre la base de IMPROLUX para responder cualquier pregunta del usuario sobre sus datos. Usala SIEMPRE que necesites un dato que no tengas a mano (gastos, ingresos, saldos, cheques, stock, órdenes, insumos faltantes, lo que sea). Podés hacer varias consultas si hace falta.\n\n" + DB_SCHEMA,
  input_schema: {
    type: "object",
    properties: { sql: { type: "string", description: "Una única consulta SELECT válida de SQLite (solo lectura)." } },
    required: ["sql"]
  }
};

function consultarDB(input) {
  try {
    const q = String(input?.sql || "").trim().replace(/;+\s*$/, "");
    if (!/^select\b/i.test(q)) return JSON.stringify({ error: "Solo se permiten consultas SELECT." });
    if (/\b(insert|update|delete|drop|alter|attach|detach|pragma|create|replace|vacuum|reindex)\b/i.test(q))
      return JSON.stringify({ error: "Consulta no permitida (solo lectura)." });
    if (q.includes(";")) return JSON.stringify({ error: "Una sola consulta por vez." });
    const rows = db.prepare(q).all();
    const cap = rows.slice(0, 300);
    return JSON.stringify({ filas: cap, total_filas: rows.length, truncado: rows.length > 300 });
  } catch (e) {
    return JSON.stringify({ error: String(e.message).slice(0, 200) });
  }
}


async function buildContexto() {
  const tc = await getTipoCambio();
  const ultimas = getUltimasTransacciones(15);
  const cuentas = getResumenCuentasCorrientes();
  const chequesPend = getChequesPendientes();
  const inversores = getInversoresActivos();
  const totalDeuda = inversores.reduce((s, i) => s + calcularDeudaInversor(i), 0);

  // Resumen de egresos del mes actual
  const mesActual = new Date().toISOString().slice(0, 7);
  const egresosMes = db.prepare(`
    SELECT concepto, SUM(egreso) as total 
    FROM transacciones 
    WHERE fecha LIKE ? AND egreso > 0
    GROUP BY concepto ORDER BY total DESC LIMIT 10
  `).all(`${mesActual}-%`);

  // ── DATOS DEL DASHBOARD (para responder consultas) ──
  const cicloAct = getCicloActual();
  const inf = cicloAct ? getInformeCiclo(cicloAct.ciclo) : null;
  const evol = db.prepare(`
    SELECT substr(fecha,1,7) as mes, SUM(egreso) as egresos, SUM(ingreso) as ingresos, COUNT(*) as movs
    FROM transacciones GROUP BY mes ORDER BY mes DESC LIMIT 24
  `).all();
  // ── STOCK, LOTES, ÓRDENES (resumen liviano; el detalle lo consulta el bot con la herramienta) ──
  const stock = db.prepare("SELECT nombre, categoria, unidad, cantidad, precio_unitario FROM stock_productos ORDER BY categoria, nombre").all();
  const lotes = db.prepare("SELECT nombre, hectareas, ha_sembrables FROM lotes ORDER BY nombre").all();
  const ordenesCtx = db.prepare("SELECT numero, anio, lote, titulo, estado, hectareas, total_planificado, total_ejecutado FROM ordenes_trabajo ORDER BY anio DESC, numero DESC").all()
    .map(o => ({ num: `${o.numero}/${o.anio}`, lote: o.lote, titulo: o.titulo, estado: o.estado, ha: o.hectareas, plan: Math.round(o.total_planificado || 0), ejec: Math.round(o.total_ejecutado || 0) }));


  return `Sos el asistente financiero de IMPROLUX, empresa ganadera uruguaya. Respondés en español rioplatense, claro y al grano (apto para WhatsApp, sin relleno).

FECHA DE HOY: ${new Date().toISOString().slice(0,10)} — SIEMPRE usar esta fecha en los registros, nunca inventar fechas.
MONEDA DEL SISTEMA: TODO EN DÓLARES AMERICANOS (USD).
TC BROU HOY: ${tc ? `$${tc.toFixed(2)} UYU/USD` : "No disponible"}
Si el usuario menciona pesos/UYU, convertir automáticamente y aclararlo.

CATEGORÍAS DE GASTO: ${CATEGORIAS.join(", ")}

HERRAMIENTAS — cuando sea una acción respondé SOLO con JSON exacto sin texto extra, sin markdown, sin bloques de código. NUNCA muestres el JSON al usuario — es solo para uso interno del sistema:
{"accion":"registrar_transaccion","fecha":"YYYY-MM-DD","concepto":"CATEGORIA","detalle":"descripción","ingreso":0,"egreso":0,"proveedor":"nombre o vacío","tc":${tc || 0}}
{"accion":"compra_insumo","fecha":"YYYY-MM-DD","producto":"nombre del insumo","rubro":"VETERINARIO/AGRICOLA/ALIMENTO","categoria":"tipo","envases":0,"contenido_envase":0,"unidad":"ml/kg/unidad","precio_envase":0,"proveedor":"nombre o vacío"}
// USAR compra_insumo cuando compran un INSUMO que se guarda en stock (veterinario, alimento, agrícola). Ej: "compré 10 frascos de ivermectina de 250ml a 20 usd en Diego Pioli" → {"accion":"compra_insumo","producto":"Ivermectina","rubro":"VETERINARIO","categoria":"ANTIPARASITARIO","envases":10,"contenido_envase":250,"unidad":"ml","precio_envase":20,"proveedor":"Diego Pioli"}. Esto genera el egreso Y la entrada de stock a la vez — NO usar también registrar_transaccion para lo mismo.
// REPREGUNTAR OBLIGATORIO en compras stockeables: si el usuario dice que compró un insumo pero FALTAN datos para el stock (cantidad de envases/frascos, o el contenido por envase como ml/cc/kg por frasco, o el precio), NO registres todavía. Respondé con accion "texto" pidiendo lo que falta, ej: "Para cargarlo bien al stock necesito: ¿cuántos frascos y cuántos ml por frasco?". Recién cuando tengas producto + envases + contenido_envase + precio, emitís compra_insumo. Ej: "compramos ivermectina 200 dolares insumos vet" → falta frascos y ml/frasco → responder texto pidiéndolos. La planilla de stock debe quedar sana, no inventes cantidades.
{"accion":"nuevo_proveedor","proveedor":"nombre","notas":""}
{"accion":"pago_proveedor","proveedor":"nombre","monto":0,"fecha":"YYYY-MM-DD"}
{"accion":"nuevo_cheque","fecha_emision":"YYYY-MM-DD","fecha_cobro":"YYYY-MM-DD","tipo":"EMITIDO o RECIBIDO","proveedor":"nombre","monto":0,"banco":"BROU","concepto":""}
{"accion":"marcar_cheque_cobrado","id":0}
{"accion":"nuevo_inversor","inversor":"nombre","capital":0,"tasa":0.08,"notas":""}
{"accion":"pago_inversor","inversor":"nombre","fecha":"YYYY-MM-DD"}
// nuevo_inversor: el capital ENTRA como ingreso de cash automáticamente. pago_inversor: SALE el cash (capital + intereses acumulados) y cierra al inversor. Ej: "pagué al inversor Pablo" → {"accion":"pago_inversor","inversor":"Pablo"}
{"accion":"borrar_transaccion","id":0}
{"accion":"editar_transaccion","id":0,"concepto":"","detalle":"","egreso":0,"ingreso":0,"proveedor":"","fecha":"YYYY-MM-DD"}
{"accion":"ver_ultimos"}
{"accion":"ver_cuentas"}
{"accion":"ver_cc_detalle","proveedor":"nombre"}
{"accion":"nuevo_laboreo","lote":"NOMBRE LOTE","tipo":"PRADERA o VERDEO","descripcion":"","ciclo":"25/26","notas":""}
{"accion":"agregar_item_laboreo","laboreo_id":0,"categoria":"INSUMO o SERVICIO","descripcion":"nombre","cantidad":0,"unidad":"ha o kg o lt","precio_unitario":0}
{"accion":"ver_laboreos","lote":"","ciclo":""}
{"accion":"ver_laboreo","id":0}
{"accion":"ejecutar_laboreo","id":0}
{"accion":"ver_cheques"}
{"accion":"ver_inversores"}
{"accion":"registrar_dividendo","socio":"PABLO MASNATTA o JONATAN D ASTOLFO","monto":0,"fecha":"YYYY-MM-DD","notas":""}
{"accion":"ver_dividendos"}
{"accion":"resumen_mes"}
{"accion":"resumen_periodo","fecha_desde":"YYYY-MM-DD","fecha_hasta":"YYYY-MM-DD"}
{"accion":"ver_por_fecha","fecha":"YYYY-MM-DD"}
{"accion":"informe_ciclo","ciclo":"25/26","tipo":"productivo"}
{"accion":"set_presupuesto","ciclo":"25/26","concepto":"CATEGORIA","monto_anual":0}
{"accion":"ver_presupuestos","ciclo":"25/26"}
{"accion":"informe_mensual","anio":2026,"mes":3}
{"accion":"informe_pdf","ciclo":"25/26","tipo":"productivo"}
{"accion":"informe_mensual_pdf","anio":2026,"mes":3}
{"accion":"backup","tipo":"transacciones"}
// TIPOS DE INFORME: "productivo" = ciclo ganadero MARZO→FEBRERO (default). "contable" = ciclo CONTABLE 1/JULIO→30/JUNIO. Si el usuario pide "informe" o "informe productivo" → tipo:"productivo". Si pide "informe contable" → tipo:"contable". El ciclo "25/26" significa jul2025→jun2026 para contable, o mar2025→feb2026 para productivo.
{"accion":"backup","tipo":"completo"}
{"accion":"texto","mensaje":"respuesta en texto"}

CICLOS GANADEROS:
- El ciclo va de MARZO a FEBRERO del año siguiente
- "ciclo 25/26" = marzo 2025 → febrero 2026
- "ciclo 26/27" = marzo 2026 → febrero 2027
- Si piden "informe anual" sin especificar → usar ciclo actual
- "presupuesto nafta 500" → set_presupuesto con ciclo actual y la categoría correcta
- "informe pdf", "pdf anual", "generar informe anual" → usar informe_pdf con el ciclo correspondiente
- "informe pdf marzo", "pdf de marzo", "informe mensual pdf" → usar informe_mensual_pdf con mes y año
- "backup", "respaldo", "descargar datos" → usar backup tipo transacciones
- "backup completo", "respaldo total" → usar backup tipo completo

VOCABULARIO DEL USUARIO — mapeo de palabras que usa → categoría correcta:
NAFTA/NARFA → COMBUSTIBLE CAMPO
GASOIL CAMPO/NAFTA CAMPO → COMBUSTIBLE CAMPO
GASOIL/GASOIL CAMIONETA/CAMIONETA GASOIL/GSAOIL CAMIONETA → COMBUSTIBLE VIATICOS
COMBUSTIBLE CAMIONETA/COMBUSTIBE CAMPO → según contexto: campo=COMBUSTIBLE CAMPO, camioneta/viaticos=COMBUSTIBLE VIATICOS
VIATICOS/GASOIL VIATICOS/PEAJES/COMIDA VIAJE → COMBUSTIBLE VIATICOS
PROVISTA/COMIDA/COMIDA EDUARDO/VERDULERIA/SUPERMERCADO/EL DORADO/GARRAFA → PROVISTA
GIRO EDUARDO/PAGO EDUARDO/SUELDO EDUARDO/TRANSFERENCIA EDUARDO/TRASNFERENIA EDUARDO/RECARGAS CELULAR/RECARGAS EDUARDO/ENCARGUE COSAS EDUARDO/NAFTA CAMPO (cuando va a Eduardo)/ALIMENTO PERROS/ROPA EDUARDO/BOMBACHAS EDUARDO/LIMA PARA EDUARDO/PAGO EDUARDO PREMIO/TRANSFERENCIAS TUERTO/PROVISTA EDUARDO → SUELDO JORNAL
PAGO EDUARDO (cuando dice encargado) → SUELDO ENCARGADO
PORTERA/PIQUES/TORNILLOS/CLAVOS/AISLADORES/BATERIAS/BULONES MANGA/CANDADOS/LIMA Y ACEITE MOTOSIERRA/MANTENIMIENTO CAMPO/PAGO LIMPIEZA → MANTENIMIENTO CAMPO
SERVICIO TRACTOR/ACEITE GRUPO/ACEITE TRACTOR/ARREGLO ZORRA Y MAQUINARIA/SERVICIO CUATRI/MANTENIMIENTO MOTO/REPUESTOS MOTO/CAMARA MOTO/ARREGLO CUBIERTAS → MANTENIMIENTO MAQUINARIA
INSUMOS (sin especificar)/GASOIL CHILQUERA/COMBUSTIBLE CHILQUERA/FERTILIZANTE → VERDEOS Y PASTURAS
INSUMOS VETERINARIOS/INSUMOS VETERINATIOS/CARAVANAS → INSUMOS VETERINARIOS
FLETE ALIMENTO/ENVIO FLETE ALIMENTO → ALIMENTACION RECRIA
PAGO LAURA TACTO/ECOGRAFIAS/PAGO ECOGRAFIAS → TRABAJOS VETERINARIOS
BREEDPLAN/PAGO ARU INSCRIPCION → GASTOS DATOS Y PEDEGREE
PAGO FLETES/GUIAS → GASTOS VENTAS GANADERAS
ENVIO PANTALLAS/PANTALLAS CAMPO/ENVIO CABLES/INVERSOR LUZ → ALQUILER ESTRUCTURA
PAGO ETIENNE/PAGO BINLADEN/GASTOS CONTRATO NUEVO MARTIN/ALQUILER → ALQUILER
CONTADOR/PAGO CONTADOR → CREACION INICIO EMPRESA Y CONTADOR
TELEFONO/TELEFONO CAMPO/TELEFONO JONI → TELEFONO
BPS/PAGO BPS → BPS
ADMINISTRATIVOS/COMIDA AGUSTIN/PAGO DAC/ENCOMIENDAS → GASTOS ADM
CUBIERTAS CAMIONETA → VEHICULOS
TIJERA → COMPRA HERRAMIENTAS
COCINA A LEÑA → ESTRUCTURA GANADERA
RETIRO PABLO/DIVIDENDO PABLO/PAGO PABLO → registrar_dividendo socio:PABLO MASNATTA
RETIRO JONI/DIVIDENDO JONI/RETIRO JONATAN/PAGO JONI → registrar_dividendo socio:JONATAN D ASTOLFO
PAGO DIEGO/PAGO CUENTA DIEGO/PAGO FDIEGO/PAGO DIEGO DEBITO/PAGO CUENTA CORRIENTE DIEGO/PAGO CHEQUE → PAGOS CUENTA CORRIENTE (proveedor: Diego Pioli)

PROVEEDORES CONOCIDOS (cuentas corrientes):
- AMAKAIK — compra de ganado
- MERCADO RURAL — insumos varios, siempre cuenta corriente
- ZAMBRANO INSUMOS / ZAMBRANO Y CIA — insumos veterinarios y campo
- DIEGO PIOLI — cuenta corriente, pagos frecuentes
- SELECTA SRL — servicios
- INVITRO — servicios veterinarios/genética
Cuando el detalle menciona estos nombres → registrar con ese proveedor

REGLAS CRÍTICAS:
- Vocabulario propio del usuario arriba → respetar siempre ese mapeo
- Si el nombre coincide con un proveedor conocido → usar accion pago_proveedor, NO registrar como sueldo
- "pago a [nombre]" con nombre en proveedores → SIEMPRE es pago_proveedor
- Gasto en pesos → convertir a USD con TC del día, aclarar conversión
- "borrar", "eliminar", "anular" + ID → usar borrar_transaccion (borra permanentemente)
- "corregir", "editar", "cambiar" + ID → usar editar_transaccion
- Para consultas de períodos específicos → usar resumen_periodo o ver_por_fecha
- EDUARDO = empleado de campo (SUELDO JORNAL generalmente)
- JONI = Jonatan, dueño (gastos administrativos/personales de la empresa)
- Si no entendés bien → usar accion texto y preguntar

DATOS DEL DASHBOARD — RESUMEN (vista rápida; para el detalle o cualquier dato que no esté acá, usá la herramienta consultar_datos):
${inf ? `CICLO ACTUAL ${inf.ciclo.label} (mar→feb): egresos $${fmt(inf.totalEgresos)}, ingresos $${fmt(inf.totalIngresos)}, neto $${fmt(inf.totalIngresos - inf.totalEgresos)}, ${inf.totalMovimientos} movimientos.
GASTOS DEL CICLO POR CATEGORÍA: ${JSON.stringify(inf.rows.filter(r => r.total_egreso > 0).map(r => ({ cat: r.concepto, egreso: Math.round(r.total_egreso) })))}` : ''}
EVOLUCIÓN MENSUAL (últimos 24 meses, más reciente primero): ${JSON.stringify(evol.map(m => ({ mes: m.mes, egresos: Math.round(m.egresos || 0), ingresos: Math.round(m.ingresos || 0), neto: Math.round((m.ingresos || 0) - (m.egresos || 0)), movs: m.movs })))}
CUENTAS CORRIENTES (saldo): ${JSON.stringify(cuentas.map(c => ({ proveedor: c.proveedor, saldo: c.saldo.toFixed(2) })))}
CHEQUES PENDIENTES: ${JSON.stringify(chequesPend.map(c => ({ id: c.id, tipo: c.tipo, proveedor: c.proveedor, monto: c.monto, vence: c.fecha_cobro })))}
INVERSORES: ${JSON.stringify(inversores.map(i => ({ inversor: i.inversor, capital: i.capital, tasa: i.tasa, deuda: calcularDeudaInversor(i).toFixed(2) })))}
LOTES: ${JSON.stringify(lotes.map(l => ({ lote: l.nombre, ha: l.hectareas, ha_aprovechable: l.ha_sembrables })))}
STOCK DE INSUMOS ACTUAL: ${JSON.stringify(stock.map(s => ({ producto: s.nombre, categoria: s.categoria, cantidad: s.cantidad, unidad: s.unidad, costo_unit: s.precio_unitario })))}
ÓRDENES DE TRABAJO (resumen): ${JSON.stringify(ordenesCtx)}
Últimas 15 transacciones: ${JSON.stringify(ultimas.map(t => ({ id: t.id, fecha: t.fecha, concepto: t.concepto, detalle: t.detalle, ingreso: t.ingreso, egreso: t.egreso, proveedor: t.proveedor })))}

CÓMO RESPONDER CONSULTAS (muy importante):
- Tenés acceso COMPLETO a todos los datos del sistema mediante la herramienta *consultar_datos* (SQL de solo lectura). NUNCA digas que no tenés acceso ni le pidas al usuario que te pase planillas, stock, laboreos o nada: consultá vos.
- Si la respuesta ya está en el RESUMEN de arriba, contestá directo. Si te falta cualquier detalle (una transacción puntual, un gasto por categoría y mes, los items de una orden, insumos faltantes, movimientos de stock, comparaciones, etc.), llamá a consultar_datos con una consulta SELECT y después respondé.
- Para insumos faltantes de un año: tomá SOLO las ordenes_trabajo con anio = <ese año calendario> (ej: 2026 → WHERE anio=2026, NO el ciclo 26/27). Sumá cantidad de sus orden_items (tipo='INSUMO', ejecutado=0, con producto_id) y restá stock_productos.cantidad. Si el usuario pide "2026", es el año calendario 2026, no marzo2026-febrero2027.
- Respondé natural, preciso y al grano (apto WhatsApp), en español rioplatense. Redondeá y aclará la moneda (USD).
- Usá JSON SOLO para ACCIONES que crean o modifican datos (registrar, pagar, borrar, editar, informe/pdf, backup). Para preguntas, nunca uses JSON.
- Sé como un buen contador que conoce el campo.`;
}

// ── HELPERS LABOREOS ─────────────────────────────────────────────────────────
function calcularTotalLaboreo(laboreoId) {
  const items = db.prepare("SELECT SUM(total) as total FROM laboreo_items WHERE laboreo_id = ?").get(laboreoId);
  return items?.total || 0;
}

function getLaboreoDetalle(laboreoId) {
  const lab = db.prepare("SELECT * FROM laboreos WHERE id = ?").get(laboreoId);
  if (!lab) return null;
  const items = db.prepare("SELECT * FROM laboreo_items WHERE laboreo_id = ? ORDER BY categoria, ejecutado, id").all(laboreoId);
  const lote = db.prepare("SELECT * FROM lotes WHERE nombre = ?").get(lab.lote);
  const totalEjec = items.filter(i=>i.ejecutado).reduce((s,i)=>s+i.total,0);
  const pctAvance = items.length > 0 ? Math.round(items.filter(i=>i.ejecutado).length / items.length * 100) : 0;
  return { ...lab, items, lote, totalEjec, pctAvance };
}

function recalcularTotalLaboreo(laboreoId) {
  const total = calcularTotalLaboreo(laboreoId);
  const ejecutado = db.prepare("SELECT COALESCE(SUM(total),0) as t FROM laboreo_items WHERE laboreo_id=? AND ejecutado=1").get(laboreoId).t || 0;
  const countTotal = db.prepare("SELECT COUNT(*) as c FROM laboreo_items WHERE laboreo_id=?").get(laboreoId).c;
  const countEjec = db.prepare("SELECT COUNT(*) as c FROM laboreo_items WHERE laboreo_id=? AND ejecutado=1").get(laboreoId).c;
  // Auto-update laboreo estado
  let estado = 'PLANIFICADO';
  if (countEjec > 0 && countEjec < countTotal) estado = 'EN EJECUCION';
  if (countTotal > 0 && countEjec === countTotal) estado = 'EJECUTADO';
  db.prepare("UPDATE laboreos SET total_presupuestado=?, total_ejecutado=?, estado=? WHERE id=?").run(total, ejecutado, estado, laboreoId);
  return { total, ejecutado, estado };
}

// ── HELPERS ÓRDENES DE TRABAJO + STOCK (v4.3) ─────────────────────────────────
function getNextNumeroOrden(anio) {
  const row = db.prepare("SELECT COALESCE(MAX(numero),0) as m FROM ordenes_trabajo WHERE anio = ?").get(anio);
  return (row.m || 0) + 1;
}

function logCambioOrden(ordenId, texto) {
  const hoy = new Date().toISOString().slice(0, 10);
  db.prepare("INSERT INTO orden_cambios (orden_id, fecha, texto) VALUES (?,?,?)").run(ordenId, hoy, texto);
}

// Recalcula totales y estado de la orden (planificado, ejecutado, avance)
function recalcularOrden(ordenId) {
  const planificado = db.prepare("SELECT COALESCE(SUM(total),0) as t FROM orden_items WHERE orden_id=?").get(ordenId).t || 0;
  const ejecutado = db.prepare("SELECT COALESCE(SUM(total_ejecutado),0) as t FROM orden_items WHERE orden_id=? AND ejecutado=1").get(ordenId).t || 0;
  const countTotal = db.prepare("SELECT COUNT(*) as c FROM orden_items WHERE orden_id=?").get(ordenId).c;
  const countEjec = db.prepare("SELECT COUNT(*) as c FROM orden_items WHERE orden_id=? AND ejecutado=1").get(ordenId).c;
  let estado = 'PLANIFICADA';
  if (countEjec > 0 && countEjec < countTotal) estado = 'EN_EJECUCION';
  if (countTotal > 0 && countEjec === countTotal) estado = 'EJECUTADA';
  db.prepare("UPDATE ordenes_trabajo SET total_planificado=?, total_ejecutado=?, estado=? WHERE id=?")
    .run(planificado, ejecutado, estado, ordenId);
  const pctAvance = countTotal > 0 ? Math.round(countEjec / countTotal * 100) : 0;
  return { total_planificado: planificado, total_ejecutado: ejecutado, estado, pctAvance };
}

function getOrdenDetalle(ordenId) {
  const ord = db.prepare("SELECT * FROM ordenes_trabajo WHERE id = ?").get(ordenId);
  if (!ord) return null;
  const items = db.prepare(`
    SELECT oi.*, sp.nombre as producto_nombre, sp.cantidad as stock_actual
    FROM orden_items oi
    LEFT JOIN stock_productos sp ON sp.id = oi.producto_id
    WHERE oi.orden_id = ? ORDER BY oi.tipo, oi.ejecutado, oi.id
  `).all(ordenId);
  const cambios = db.prepare("SELECT * FROM orden_cambios WHERE orden_id = ? ORDER BY id DESC").all(ordenId);
  const pctAvance = items.length ? Math.round(items.filter(i=>i.ejecutado).length / items.length * 100) : 0;
  // ha sembrables del lote (referencia para sugerir ha de la orden)
  const loteInfo = ord.lote ? db.prepare("SELECT hectareas, ha_sembrables FROM lotes WHERE nombre = ?").get(ord.lote) : null;
  return { ...ord, numero_display: `${ord.numero}/${ord.anio}`, items, cambios, pctAvance,
    lote_hectareas: loteInfo?.hectareas ?? null, lote_ha_sembrables: loteInfo?.ha_sembrables ?? null };
}

// Recalcula cantidad/total de los items con dosis>0 (no ejecutados) según las ha de la orden
function recomputarDosisOrden(ordenId) {
  const ord = db.prepare("SELECT hectareas FROM ordenes_trabajo WHERE id = ?").get(ordenId);
  const ha = parseFloat(ord?.hectareas) || 0;
  if (ha <= 0) return;
  const items = db.prepare("SELECT * FROM orden_items WHERE orden_id = ? AND ejecutado = 0 AND dosis > 0").all(ordenId);
  const upd = db.prepare("UPDATE orden_items SET cantidad = ?, total = ? WHERE id = ?");
  items.forEach(i => { const cant = i.dosis * ha; upd.run(cant, cant * i.precio_unitario, i.id); });
}

// Suma de ha aprovechables (sembrables) de una lista de nombres de lote
function haDeLotes(loteArr) {
  return (loteArr || []).reduce((s, n) => {
    const lt = db.prepare("SELECT ha_sembrables FROM lotes WHERE nombre = ?").get(n);
    return s + (parseFloat(lt?.ha_sembrables) || 0);
  }, 0);
}
// Normaliza el input de lotes (array o string) → array de nombres en mayúscula
function normalizarLotes(lotes, lote) {
  let arr = Array.isArray(lotes) ? lotes : (lote ? [lote] : []);
  return arr.filter(Boolean).map(x => String(x).toUpperCase().trim());
}

// ENTRADA de stock: suma cantidad y recalcula costo promedio ponderado
function entradaStock(productoId, cantidad, precioUnitario, fecha, notas, ordenId = null) {
  const p = db.prepare("SELECT * FROM stock_productos WHERE id = ?").get(productoId);
  if (!p) return null;
  const cant = parseFloat(cantidad) || 0;
  const precio = parseFloat(precioUnitario) || 0;
  const nuevaCantidad = (p.cantidad || 0) + cant;
  // Promedio ponderado: solo si hay precio de entrada; si no, mantiene el costo previo
  let nuevoPrecio = p.precio_unitario || 0;
  if (precio > 0 && nuevaCantidad > 0) {
    nuevoPrecio = ((p.cantidad || 0) * (p.precio_unitario || 0) + cant * precio) / nuevaCantidad;
  }
  db.prepare("UPDATE stock_productos SET cantidad=?, precio_unitario=? WHERE id=?").run(nuevaCantidad, nuevoPrecio, productoId);
  db.prepare("INSERT INTO stock_movimientos (producto_id,fecha,tipo,cantidad,precio_unitario,orden_id,notas) VALUES (?,?,'ENTRADA',?,?,?,?)")
    .run(productoId, fecha, cant, precio, ordenId, notas || '');
  return { cantidad: nuevaCantidad, precio_unitario: nuevoPrecio };
}

// Concepto contable por defecto según el rubro del insumo (para no inventar conceptos)
function conceptoPorRubro(rubro, categoria) {
  const r = (rubro || '').toUpperCase();
  if (r === 'VETERINARIO') return 'INSUMOS VETERINARIOS';
  if (r === 'ALIMENTO') return 'ALIMENTACION RECRIA';
  if (r === 'GENETICA') return 'INSUMOS VETERINARIOS';
  if (r === 'AGRICOLA') return 'VERDEOS Y PASTURAS';
  return 'INSUMOS VETERINARIOS';
}

// COMPRA que carga stock: un solo movimiento → egreso en el flujo + entrada al stock.
// Presentación opcional: N envases × contenido (ej. 10 frascos × 250 ml) a precio por envase.
function comprarInsumo(o) {
  const campo = (o.campo || 'LA AMISTAD').toUpperCase();
  const fecha = o.fecha || new Date().toISOString().slice(0, 10);
  const nombre = (o.producto || o.nombre || '').trim();
  if (!nombre) return { error: 'Falta el nombre del insumo' };

  const envases = parseFloat(o.envases) || 0;
  const contenido = parseFloat(o.contenido_envase) || 0;           // ml/kg por envase (opcional)
  const precioEnvase = parseFloat(o.precio_envase);
  const unidad = o.unidad || (contenido > 0 ? 'ml' : 'unidad');

  // Cantidad total (en unidad base) y costo total
  let cantTotal, costoTotal;
  if (contenido > 0 && envases > 0) {
    cantTotal = envases * contenido;                               // 10 × 250 = 2500 ml
    costoTotal = envases * (isNaN(precioEnvase) ? (parseFloat(o.precio_unitario) || 0) * contenido : precioEnvase);
  } else {
    cantTotal = envases > 0 ? envases : (parseFloat(o.cantidad) || 0);
    const pu = !isNaN(precioEnvase) ? precioEnvase : (parseFloat(o.precio_unitario) || 0);
    costoTotal = cantTotal * pu;
  }
  const precioUnit = cantTotal > 0 ? costoTotal / cantTotal : 0;   // costo por ml/kg/unidad
  if (cantTotal <= 0) return { error: 'La cantidad de la compra es 0' };

  // Buscar producto por nombre (nombre es único a nivel base); si no existe, crearlo
  let prod = db.prepare("SELECT * FROM stock_productos WHERE LOWER(nombre) = LOWER(?)").get(nombre);
  if (!prod) {
    const r = db.prepare("INSERT INTO stock_productos (nombre,rubro,categoria,unidad,cantidad,precio_unitario,campo) VALUES (?,?,?,?,0,0,?)")
      .run(nombre.toUpperCase(), (o.rubro || 'VETERINARIO').toUpperCase(), (o.categoria || 'OTRO').toUpperCase(), unidad, campo);
    prod = db.prepare("SELECT * FROM stock_productos WHERE id = ?").get(r.lastInsertRowid);
  }

  // 1) Entrada al stock (recalcula costo promedio ponderado)
  const presentacion = contenido > 0 && envases > 0 ? `${envases} × ${contenido}${prod.unidad || unidad}` : `${cantTotal} ${prod.unidad || unidad}`;
  const detalleStock = `Compra ${presentacion}${o.proveedor ? ' — ' + o.proveedor : ''}`;
  entradaStock(prod.id, cantTotal, precioUnit, fecha, detalleStock);

  // 2) Egreso en el flujo (una sola vez, marcado fuente='compra_insumo')
  const concepto = o.concepto || conceptoPorRubro(prod.rubro || o.rubro, prod.categoria || o.categoria);
  const detalleFlujo = `Compra ${presentacion} de ${prod.nombre}`;
  const tr = db.prepare("INSERT INTO transacciones (fecha,concepto,detalle,ingreso,egreso,proveedor,tc,fuente) VALUES (?,?,?,0,?,?,0,'compra_insumo')")
    .run(fecha, concepto, detalleFlujo, Math.round(costoTotal * 100) / 100, o.proveedor || '');

  return {
    ok: true, producto_id: prod.id, producto: prod.nombre, campo,
    cantidad_total: cantTotal, unidad: prod.unidad || unidad,
    costo_total: Math.round(costoTotal * 100) / 100, precio_unitario: Math.round(precioUnit * 10000) / 10000,
    concepto, proveedor: o.proveedor || '', transaccion_id: tr.lastInsertRowid
  };
}

// SALIDA de stock: descuenta cantidad (permite negativo, sin bloquear ni alertar)
function salidaStock(productoId, cantidad, fecha, notas, ordenId = null) {
  const p = db.prepare("SELECT * FROM stock_productos WHERE id = ?").get(productoId);
  if (!p) return null;
  const cant = parseFloat(cantidad) || 0;
  const nuevaCantidad = (p.cantidad || 0) - cant;
  db.prepare("UPDATE stock_productos SET cantidad=? WHERE id=?").run(nuevaCantidad, productoId);
  db.prepare("INSERT INTO stock_movimientos (producto_id,fecha,tipo,cantidad,precio_unitario,orden_id,notas) VALUES (?,?,'SALIDA',?,?,?,?)")
    .run(productoId, fecha, cant, p.precio_unitario || 0, ordenId, notas || '');
  return { cantidad: nuevaCantidad, precio_unitario: p.precio_unitario || 0 };
}

function getStockValorizado(campo) {
  const productos = campo
    ? db.prepare("SELECT * FROM stock_productos WHERE campo = ? ORDER BY rubro, categoria, nombre").all(campo)
    : db.prepare("SELECT * FROM stock_productos ORDER BY rubro, categoria, nombre").all();
  return productos.map(p => ({ ...p, rubro: p.rubro || 'AGRICOLA', valor: (p.cantidad || 0) * (p.precio_unitario || 0) }));
}

// ── EJECUTAR ACCIÓN ───────────────────────────────────────────────────────────
async function ejecutarAccion(accion) {
  const hoy = new Date().toISOString().split("T")[0];
  const tc = await getTipoCambio();

  // REGISTRAR TRANSACCIÓN
  if (accion.accion === "registrar_transaccion") {
    const { concepto, detalle, proveedor } = accion;
    let { ingreso, egreso } = accion;
    if (!concepto) return "❌ Faltan datos para registrar.";

    // Validar fecha — si es muy antigua o futura, usar hoy
    let fecha = accion.fecha || hoy;
    const fechaDate = new Date(fecha);
    const diff = Math.abs(new Date() - fechaDate) / (1000 * 60 * 60 * 24);
    if (isNaN(fechaDate) || diff > 365) fecha = hoy;

    db.prepare(`
      INSERT INTO transacciones (fecha, concepto, detalle, ingreso, egreso, proveedor, tc, fuente)
      VALUES (?, ?, ?, ?, ?, ?, ?, 'whatsapp')
    `).run(fecha || hoy, concepto, detalle || "", parseFloat(ingreso) || 0, parseFloat(egreso) || 0, proveedor || "", tc || 0);

    const tipo = ingreso > 0 ? `📥 Ingreso: $${fmt(ingreso)} USD` : `📤 Egreso: $${fmt(egreso)} USD`;
    return `✅ Registrado!\n📝 ${detalle || concepto}\n${tipo}\n📁 ${concepto}${proveedor ? `\n🏪 ${proveedor}` : ""}`;
  }

  // COMPRA DE INSUMO (egreso en flujo + entrada al stock, un solo paso)
  if (accion.accion === "compra_insumo") {
    const r = comprarInsumo(accion);
    if (r.error) return `❌ ${r.error}`;
    return `✅ Compra registrada!\n📦 ${r.producto}: +${fmt(r.cantidad_total)} ${r.unidad}\n📤 Egreso: $${fmt(r.costo_total)} USD (${r.concepto})${r.proveedor ? `\n🏪 ${r.proveedor}` : ""}\n💧 Costo: $${r.precio_unitario}/${r.unidad}\n\n_Impactó en el flujo y en el stock. No lo cargues de nuevo como gasto._`;
  }

  // NUEVO PROVEEDOR
  if (accion.accion === "nuevo_proveedor") {
    const { proveedor, notas } = accion;
    if (!proveedor) return "❌ Falta el nombre del proveedor.";
    try {
      db.prepare("INSERT INTO cuentas_corrientes (proveedor, notas) VALUES (?, ?)").run(proveedor, notas || "");
      return `✅ Proveedor creado!\n🏪 ${proveedor}\nSaldo inicial: $0.00 USD`;
    } catch (e) {
      if (e.message.includes("UNIQUE")) return `⚠️ El proveedor "${proveedor}" ya existe en cuentas corrientes.`;
      return "❌ Error al crear proveedor.";
    }
  }

  // PAGO A PROVEEDOR
  if (accion.accion === "pago_proveedor") {
    const { proveedor, monto, fecha, medio, notas } = accion;
    if (!proveedor || !monto) return "❌ Faltan datos para registrar el pago.";

    // Registrar en cc_movimientos — NO afecta flujo de caja
    db.prepare(`
      INSERT INTO cc_movimientos (fecha, proveedor, monto, medio, notas)
      VALUES (?, ?, ?, ?, ?)
    `).run(fecha || hoy, proveedor, parseFloat(monto), medio || 'EFECTIVO', notas || `Pago a ${proveedor}`);

    const saldoNuevo = getSaldoProveedor(proveedor);
    return `✅ Pago CC registrado!\n🏪 ${proveedor}\n💰 $${fmt(monto)} USD\n📊 Saldo pendiente: $${fmt(saldoNuevo)} USD\n💡 No afecta el flujo de caja`;
  }

  // NUEVO CHEQUE
  if (accion.accion === "nuevo_cheque") {
    const { fecha_emision, fecha_cobro, tipo, proveedor, monto, banco, concepto } = accion;
    if (!monto || !tipo) return "❌ Faltan datos para el cheque.";

    const result = db.prepare(`
      INSERT INTO cheques (fecha_emision, fecha_cobro, tipo, proveedor, monto, estado, banco, concepto)
      VALUES (?, ?, ?, ?, ?, 'PENDIENTE', ?, ?)
    `).run(fecha_emision || hoy, fecha_cobro || "", tipo, proveedor || "", parseFloat(monto), banco || "BROU", concepto || "");

    const emoji = tipo === "RECIBIDO" ? "📥" : "📤";
    return `✅ Cheque registrado! (ID: ${result.lastInsertRowid})\n${emoji} ${tipo}\n🏪 ${proveedor || "Sin proveedor"}\n💰 $${fmt(monto)} USD\n📅 Vence: ${fecha_cobro || "Sin fecha"}`;
  }

  // MARCAR CHEQUE COBRADO
  if (accion.accion === "marcar_cheque_cobrado") {
    const cheque = db.prepare("SELECT * FROM cheques WHERE id = ?").get(accion.id);
    if (!cheque) return "❌ No encontré ese cheque.";
    db.prepare("UPDATE cheques SET estado = 'COBRADO' WHERE id = ?").run(accion.id);
    return `✅ Cheque #${accion.id} marcado como cobrado.\n🏪 ${cheque.proveedor}\n💰 $${fmt(cheque.monto)} USD`;
  }

  // NUEVO INVERSOR
  if (accion.accion === "nuevo_inversor") {
    const { inversor, capital, tasa, notas } = accion;
    if (!inversor || !capital) return "❌ Faltan datos del inversor.";
    const cap = parseFloat(capital);
    const fecha = accion.fecha || hoy;

    db.prepare(`
      INSERT INTO inversores (inversor, fecha_ingreso, capital, tasa, deuda_actual, estado, notas)
      VALUES (?, ?, ?, ?, ?, 'ACTIVO', ?)
    `).run(inversor, fecha, cap, parseFloat(tasa) || 0.08, cap, notas || "");

    // El capital entra como INGRESO de cash en el flujo
    db.prepare(`INSERT INTO transacciones (fecha, concepto, detalle, ingreso, egreso, proveedor, tc, fuente)
      VALUES (?, 'INGRESO INVERSOR', ?, ?, 0, ?, ?, 'inversor')`)
      .run(fecha, `Ingreso capital inversor ${inversor}`, cap, inversor, tc || 0);

    return `✅ Inversor registrado!\n👤 ${inversor}\n💰 Capital: $${fmt(cap)} USD\n📥 Ingresó al flujo como cash\n📈 Tasa: ${(parseFloat(tasa || 0.08) * 100).toFixed(1)}% anual\n📅 ${fecha}`;
  }

  // PAGO / DEVOLUCIÓN A INVERSOR (sale cash: capital + intereses)
  if (accion.accion === "pago_inversor" || accion.accion === "devolucion_inversor") {
    const inv = db.prepare("SELECT * FROM inversores WHERE LOWER(inversor) = LOWER(?) AND estado = 'ACTIVO'").get(accion.inversor);
    if (!inv) return `❌ No encontré un inversor activo llamado "${accion.inversor}".`;
    const fecha = accion.fecha || hoy;
    const deudaTotal = calcularDeudaInversor(inv);
    const interes = Math.round((deudaTotal - inv.capital) * 100) / 100;

    // Egreso 1: devolución del capital
    db.prepare(`INSERT INTO transacciones (fecha, concepto, detalle, ingreso, egreso, proveedor, tc, fuente)
      VALUES (?, 'DEVOLUCION INVERSOR', ?, 0, ?, ?, ?, 'inversor')`)
      .run(fecha, `Devolución capital ${inv.inversor}`, inv.capital, inv.inversor, tc || 0);
    // Egreso 2: intereses (si hay)
    if (interes > 0) {
      db.prepare(`INSERT INTO transacciones (fecha, concepto, detalle, ingreso, egreso, proveedor, tc, fuente)
        VALUES (?, 'INTERESES', ?, 0, ?, ?, ?, 'inversor')`)
        .run(fecha, `Intereses inversor ${inv.inversor}`, interes, inv.inversor, tc || 0);
    }
    db.prepare("UPDATE inversores SET estado = 'PAGADO', deuda_actual = 0 WHERE id = ?").run(inv.id);

    return `✅ Inversor pagado!\n👤 ${inv.inversor}\n💵 Capital devuelto: $${fmt(inv.capital)}\n📈 Intereses: $${fmt(interes)}\n📤 Salida total del flujo: $${fmt(deudaTotal)} USD`;
  }

  // BORRAR TRANSACCIÓN (borrado real)
  if (accion.accion === "anular_transaccion" || accion.accion === "borrar_transaccion") {
    const t = db.prepare("SELECT * FROM transacciones WHERE id = ?").get(accion.id);
    if (!t) return "❌ No encontré esa transacción con ese ID.";
    db.prepare("DELETE FROM transacciones WHERE id = ?").run(accion.id);
    return `🗑️ Eliminado!\n📝 ${t.detalle || t.concepto}\n💰 ${t.egreso > 0 ? `-$${fmt(t.egreso)}` : `+$${fmt(t.ingreso)}`} USD\n📅 ${t.fecha}`;
  }

  // EDITAR TRANSACCIÓN
  if (accion.accion === "editar_transaccion") {
    const t = db.prepare("SELECT * FROM transacciones WHERE id = ?").get(accion.id);
    if (!t) return "❌ No encontré esa transacción.";
    const campos = {};
    if (accion.concepto) campos.concepto = accion.concepto;
    if (accion.detalle) campos.detalle = accion.detalle;
    if (accion.egreso !== undefined) campos.egreso = parseFloat(accion.egreso);
    if (accion.ingreso !== undefined) campos.ingreso = parseFloat(accion.ingreso);
    if (accion.proveedor !== undefined) campos.proveedor = accion.proveedor;
    if (accion.fecha) campos.fecha = accion.fecha;
    if (!Object.keys(campos).length) return "❌ No hay campos para editar.";
    const sets = Object.keys(campos).map(k => `${k} = ?`).join(', ');
    db.prepare(`UPDATE transacciones SET ${sets} WHERE id = ?`).run(...Object.values(campos), accion.id);
    return `✅ Transacción #${accion.id} actualizada!\n📝 ${campos.detalle || t.detalle}`;
  }

  // VER ÚLTIMOS
  if (accion.accion === "ver_ultimos") {
    const ultimos = getUltimasTransacciones(8);
    if (!ultimos.length) return "📋 No hay transacciones registradas.";
    const lineas = ultimos.map((t, i) =>
      `${i + 1}. [#${t.id}] ${t.concepto} · ${t.egreso > 0 ? `-$${fmt(t.egreso)}` : `+$${fmt(t.ingreso)}`} · ${t.fecha}${t.proveedor ? ` · ${t.proveedor}` : ""}`
    ).join("\n");
    return `📋 *Últimas transacciones:*\n\n${lineas}\n\nPara anular alguna decí "anular #ID"`;
  }

  // VER CUENTAS CORRIENTES
  if (accion.accion === "ver_cc_detalle") {
    const { proveedor } = accion;
    if (!proveedor) return "❌ Necesito el nombre del proveedor.";
    const detalle = getDetalleCuentaCorriente(proveedor);
    if (!detalle.length) return `📋 No hay movimientos para ${proveedor}.`;
    const saldo = getSaldoProveedor(proveedor);
    const lineas = detalle.slice(-15).map(t => {
      const emoji = t.tipo === 'COMPRA' ? '📤' : '💳';
      return `${emoji} ${t.fecha} · ${t.detalle||t.concepto} · $${fmt(t.monto)} · Saldo: $${fmt(t.saldo_acumulado)}`;
    }).join("\n");
    return `📊 *CC ${proveedor}*\n\n${lineas}\n\n💰 Saldo actual: $${fmt(saldo)} USD`;
  }

  if (accion.accion === "ver_cuentas") {
    const cuentas = getResumenCuentasCorrientes();
    if (!cuentas.length) return "📋 No hay cuentas corrientes con saldo pendiente.";
    const lineas = cuentas.map(c =>
      `${c.saldo > 0 ? "🔴" : "🟢"} ${c.proveedor}: $${fmt(Math.abs(c.saldo))} USD ${c.saldo > 0 ? "(debemos)" : "(a favor)"}`
    ).join("\n");
    const total = cuentas.reduce((s, c) => s + c.saldo, 0);
    return `🔄 *Cuentas Corrientes:*\n\n${lineas}\n\n💳 Total adeudado: $${fmt(total)} USD`;
  }

  // VER CHEQUES
  if (accion.accion === "ver_cheques") {
    const cheques = getChequesPendientes();
    if (!cheques.length) return "✅ No hay cheques pendientes.";
    const lineas = cheques.map(c =>
      `${c.tipo === "EMITIDO" ? "📤" : "📥"} [#${c.id}] ${c.proveedor || "Sin prov."} · $${fmt(c.monto)} USD · vence ${c.fecha_cobro || "sin fecha"}`
    ).join("\n");
    const total = cheques.reduce((s, c) => s + c.monto, 0);
    return `🏦 *Cheques pendientes:*\n\n${lineas}\n\n💳 Total: $${fmt(total)} USD`;
  }

  // VER INVERSORES
  if (accion.accion === "ver_inversores") {
    const inversores = getInversoresActivos();
    if (!inversores.length) return "📋 No hay inversores activos.";
    const lineas = inversores.map(i => {
      const deuda = calcularDeudaInversor(i);
      return `👤 ${i.inversor}\n   Capital: $${fmt(i.capital)} · Tasa: ${(i.tasa * 100).toFixed(1)}%\n   Deuda actual: $${fmt(deuda)} USD`;
    }).join("\n\n");
    const totalDeuda = inversores.reduce((s, i) => s + calcularDeudaInversor(i), 0);
    return `👥 *Inversores activos:*\n\n${lineas}\n\n💳 Deuda total: $${fmt(totalDeuda)} USD`;
  }

  // RESUMEN MES — acepta mes/año específico
  if (accion.accion === "resumen_mes") {
    // Detectar si piden un mes específico (ej: "marzo 2026" → fecha_desde en el JSON)
    const periodo = accion.periodo || new Date().toISOString().slice(0, 7);
    const rows = db.prepare(`
      SELECT concepto, SUM(egreso) as total_egreso, SUM(ingreso) as total_ingreso
      FROM transacciones WHERE fecha LIKE ?
      GROUP BY concepto ORDER BY total_egreso DESC
    `).all(`${periodo}-%`);

    if (!rows.length) return `📊 No hay movimientos en ${periodo}.`;

    const totalEgresos = rows.reduce((s, r) => s + (r.total_egreso || 0), 0);
    const totalIngresos = rows.reduce((s, r) => s + (r.total_ingreso || 0), 0);
    const lineas = rows.filter(r => r.total_egreso > 0)
      .map(r => `  • ${r.concepto}: $${fmt(r.total_egreso)}`).join("\n");

    return `📊 *Resumen ${periodo}*\n\n${lineas || "Sin egresos"}\n\n📤 Total egresos: $${fmt(totalEgresos)} USD\n📥 Total ingresos: $${fmt(totalIngresos)} USD\n💰 Neto: $${fmt(totalIngresos - totalEgresos)} USD`;
  }

  // RESUMEN POR PERÍODO
  if (accion.accion === "resumen_periodo") {
    const { fecha_desde, fecha_hasta } = accion;
    if (!fecha_desde || !fecha_hasta) return "❌ Necesito fecha_desde y fecha_hasta.";

    const rows = db.prepare(`
      SELECT concepto, SUM(egreso) as total_egreso, SUM(ingreso) as total_ingreso
      FROM transacciones WHERE fecha BETWEEN ? AND ?
      GROUP BY concepto ORDER BY total_egreso DESC
    `).all(fecha_desde, fecha_hasta);

    if (!rows.length) return `📊 No hay movimientos entre ${fecha_desde} y ${fecha_hasta}.`;

    const totalEgresos = rows.reduce((s, r) => s + (r.total_egreso || 0), 0);
    const totalIngresos = rows.reduce((s, r) => s + (r.total_ingreso || 0), 0);
    const lineas = rows.filter(r => r.total_egreso > 0)
      .map(r => `  • ${r.concepto}: $${fmt(r.total_egreso)}`).join("\n");

    return `📊 *Período ${fecha_desde} → ${fecha_hasta}*\n\n${lineas || "Sin egresos"}\n\n📤 Egresos: $${fmt(totalEgresos)} USD\n📥 Ingresos: $${fmt(totalIngresos)} USD`;
  }

  // VER MOVIMIENTOS DE UN DÍA ESPECÍFICO
  if (accion.accion === "ver_por_fecha") {
    const { fecha } = accion;
    if (!fecha) return "❌ Necesito una fecha.";

    const rows = db.prepare(`
      SELECT * FROM transacciones WHERE fecha = ? ORDER BY created_at ASC
    `).all(fecha);

    if (!rows.length) return `📋 No hay movimientos el ${fecha}.`;

    const lineas = rows.map((t, i) =>
      `${i+1}. [#${t.id}] ${t.concepto} · ${t.detalle} · ${t.egreso > 0 ? `-$${fmt(t.egreso)}` : `+$${fmt(t.ingreso)}`} USD`
    ).join("\n");
    const total = rows.reduce((s, t) => s + t.egreso - t.ingreso, 0);

    return `📋 *Movimientos del ${fecha}:*\n\n${lineas}\n\n💰 Total del día: $${fmt(Math.abs(total))} USD`;
  }

  // REGISTRAR DIVIDENDO
  if (accion.accion === "registrar_dividendo") {
    const { socio, monto, fecha, notas } = accion;
    if (!socio || !monto) return "❌ Falta socio o monto.";
    const socios = ['PABLO MASNATTA', 'JONATAN D ASTOLFO'];
    const socioNorm = socios.find(s => s.toLowerCase().includes((socio||'').toLowerCase().split(' ')[0])) || socio.toUpperCase();
    db.prepare(`INSERT INTO dividendos (fecha, socio, monto, notas) VALUES (?, ?, ?, ?)`)
      .run(fecha || hoy, socioNorm, parseFloat(monto), notas || '');
    // También registrar como egreso en transacciones para el flujo de caja
    db.prepare(`INSERT INTO transacciones (fecha, concepto, detalle, ingreso, egreso, proveedor, es_cc, tc, fuente) VALUES (?, 'DIVIDENDOS', ?, 0, ?, ?, 0, ?, 'whatsapp')`)
      .run(fecha || hoy, `Retiro ${socioNorm}`, parseFloat(monto), socioNorm, tc || 0);
    const totalSocio = db.prepare(`SELECT COALESCE(SUM(monto),0) as total FROM dividendos WHERE LOWER(socio) LIKE LOWER(?)`).get('%' + (socio.split(' ')[0]) + '%').total;
    return `✅ Dividendo registrado!\n👤 ${socioNorm}\n💰 $${fmt(monto)} USD\n📊 Total retirado por ${socioNorm.split(' ')[0]}: $${fmt(totalSocio)} USD`;
  }

  // VER DIVIDENDOS
  if (accion.accion === "ver_dividendos") {
    const pablo = db.prepare(`SELECT COALESCE(SUM(monto),0) as total FROM dividendos WHERE LOWER(socio) LIKE '%pablo%'`).get().total;
    const joni  = db.prepare(`SELECT COALESCE(SUM(monto),0) as total FROM dividendos WHERE LOWER(socio) LIKE '%jonatan%' OR LOWER(socio) LIKE '%astolfo%'`).get().total;
    const ultimos = db.prepare(`SELECT * FROM dividendos ORDER BY fecha DESC LIMIT 10`).all();
    const lineas = ultimos.map(d => `  ${d.fecha} · ${d.socio.split(' ')[0]} · $${fmt(d.monto)}`).join('\n');
    return `💰 *Dividendos / Retiros*\n\n👤 Pablo Masnatta: $${fmt(pablo)} USD\n👤 Jonatan D Astolfo: $${fmt(joni)} USD\n📊 Total: $${fmt(pablo + joni)} USD\n\n*Últimos retiros:*\n${lineas || 'Sin retiros registrados'}`;
  }

  // ── NUEVO LABOREO ──
  if (accion.accion === "nuevo_laboreo") {
    const { lote, tipo, descripcion, ciclo, notas } = accion;
    if (!lote || !tipo) return "❌ Necesito lote y tipo de laboreo.";
    const cicloActual = (() => { const n=new Date(); const y=n.getMonth()>=2?n.getFullYear():n.getFullYear()-1; return `${String(y).slice(2)}/${String(y+1).slice(2)}`; })();
    const result = db.prepare(`
      INSERT INTO laboreos (lote, tipo, descripcion, ciclo, estado, notas)
      VALUES (?, ?, ?, ?, 'PLANIFICADO', ?)
    `).run(lote.toUpperCase(), tipo.toUpperCase(), descripcion||'', ciclo||cicloActual, notas||'');
    return `✅ Laboreo creado! (ID: ${result.lastInsertRowid})\n📍 Lote: ${lote.toUpperCase()}\n🌱 Tipo: ${tipo.toUpperCase()}\n📋 Estado: PLANIFICADO\n\nAhora podés agregar items con: "agregar item #${result.lastInsertRowid}"`;
  }

  // ── AGREGAR ITEM A LABOREO ──
  if (accion.accion === "agregar_item_laboreo") {
    const { laboreo_id, categoria, descripcion, cantidad, unidad, precio_unitario } = accion;
    if (!laboreo_id || !descripcion) return "❌ Faltan datos del item.";
    const total = (parseFloat(cantidad)||0) * (parseFloat(precio_unitario)||0);
    db.prepare(`
      INSERT INTO laboreo_items (laboreo_id, categoria, descripcion, cantidad, unidad, precio_unitario, total)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(laboreo_id, categoria||'INSUMO', descripcion, parseFloat(cantidad)||0, unidad||'ha', parseFloat(precio_unitario)||0, total);
    const totalLab = recalcularTotalLaboreo(laboreo_id);
    return `✅ Item agregado!\n📝 ${descripcion}\n💰 ${fmt(cantidad||0)} ${unidad||'ha'} × $${fmt(precio_unitario||0)} = $${fmt(total)} USD\n📊 Total laboreo: $${fmt(totalLab)} USD`;
  }

  // ── VER LABOREOS ──
  if (accion.accion === "ver_laboreos") {
    const { lote, ciclo } = accion;
    let query = "SELECT l.*, lt.hectareas, lt.ha_sembrables FROM laboreos l LEFT JOIN lotes lt ON lt.nombre = l.lote WHERE 1=1";
    const params = [];
    if (lote) { query += " AND LOWER(l.lote) LIKE ?"; params.push('%'+lote.toLowerCase()+'%'); }
    if (ciclo) { query += " AND l.ciclo = ?"; params.push(ciclo); }
    query += " ORDER BY l.created_at DESC LIMIT 10";
    const labs = db.prepare(query).all(...params);
    if (!labs.length) return "📋 No hay laboreos registrados.";
    const lineas = labs.map(l => `🌱 [#${l.id}] ${l.lote} · ${l.tipo} · ${l.ciclo}\n   ${l.estado} · $${fmt(l.total_presupuestado)} USD`).join("\n");
    return `🌱 *Laboreos:*\n\n${lineas}`;
  }

  // ── VER DETALLE LABOREO ──
  if (accion.accion === "ver_laboreo") {
    const det = getLaboreoDetalle(accion.id);
    if (!det) return "❌ No encontré ese laboreo.";
    const hasSembrables = det.lote?.ha_sembrables;
    const costoPorHa = hasSembrables ? (det.total_presupuestado / det.lote.ha_sembrables) : null;
    const itemsLineas = det.items.map(i =>
      `  • ${i.descripcion}: ${i.cantidad} ${i.unidad} × $${fmt(i.precio_unitario)} = $${fmt(i.total)}`
    ).join("\n");
    return `🌱 *Laboreo #${det.id} — ${det.lote} · ${det.tipo}*\n` +
      `📅 Ciclo: ${det.ciclo} · Estado: ${det.estado}\n\n` +
      `*Items:*\n${itemsLineas||'Sin items'}\n\n` +
      `💰 Total: $${fmt(det.total_presupuestado)} USD` +
      (costoPorHa ? `\n📐 Costo/ha: $${fmt(costoPorHa)} USD` : '');
  }

  // ── CAMBIAR ESTADO LABOREO ──
  if (accion.accion === "ejecutar_laboreo") {
    const lab = db.prepare("SELECT * FROM laboreos WHERE id = ?").get(accion.id);
    if (!lab) return "❌ No encontré ese laboreo.";
    db.prepare("UPDATE laboreos SET estado = 'EJECUTADO', fecha_ejecucion = ? WHERE id = ?").run(hoy, accion.id);
    return `✅ Laboreo #${accion.id} marcado como EJECUTADO!\n📍 ${lab.lote} · ${lab.tipo}\n💰 Presupuestado: $${fmt(lab.total_presupuestado)} USD`;
  }

  if (accion.accion === "texto") return accion.mensaje;

  // INFORME CICLO GANADERO
  if (accion.accion === "informe_ciclo") {
    const tipo = (accion.tipo === 'contable') ? 'contable' : 'productivo';
    const cicloStr = accion.ciclo || `${getCicloActual(tipo).ciclo}`;
    const informe = getInformeCiclo(cicloStr, tipo);
    if (!informe) return "❌ No pude interpretar el ciclo. Usá formato 25/26.";

    if (!informe.rows.length) return `📊 No hay movimientos en el ciclo ${informe.ciclo.tipo === 'contable' ? 'contable' : 'productivo'} ${informe.ciclo.label}.`;

    const lineas = informe.rows.filter(r => r.total_egreso > 0).map(r => {
      const presup = informe.presupuestoMap[r.concepto];
      let extra = "";
      if (presup) {
        const pct = ((r.total_egreso / presup) * 100).toFixed(0);
        extra = ` (${pct}% de $${fmt(presup)})`;
        if (r.total_egreso > presup) extra += " ⚠️";
      }
      return `  • ${r.concepto}: $${fmt(r.total_egreso)}${extra}`;
    });

    let msg = `📊 *IMPROLUX — Ciclo ${informe.ciclo.label}*\n`;
    msg += `📅 ${informe.ciclo.fecha_desde} → ${informe.fechaHasta}\n`;
    msg += `📋 ${informe.totalMovimientos} movimientos\n\n`;
    msg += lineas.join("\n");
    msg += `\n\n📤 Total egresos: $${fmt(informe.totalEgresos)} USD`;
    msg += `\n📥 Total ingresos: $${fmt(informe.totalIngresos)} USD`;
    msg += `\n💰 Neto: $${fmt(informe.totalIngresos - informe.totalEgresos)} USD`;

    // Resumen de presupuesto total si hay
    const totalPresup = Object.values(informe.presupuestoMap).reduce((s, v) => s + v, 0);
    if (totalPresup > 0) {
      const pctTotal = ((informe.totalEgresos / totalPresup) * 100).toFixed(0);
      msg += `\n\n📐 Presupuesto total ciclo: $${fmt(totalPresup)} USD`;
      msg += `\n📊 Ejecutado: ${pctTotal}%`;
    }

    return msg;
  }

  // SET PRESUPUESTO
  if (accion.accion === "set_presupuesto") {
    const cicloStr = accion.ciclo || getCicloActual().ciclo;
    const ciclo = parseCiclo(cicloStr);
    if (!ciclo) return "❌ Ciclo inválido. Usá formato 25/26.";
    if (!accion.concepto || !accion.monto_anual) return "❌ Necesito categoría y monto anual.";

    db.prepare(`
      INSERT INTO presupuestos (ciclo, concepto, monto_anual)
      VALUES (?, ?, ?)
      ON CONFLICT(ciclo, concepto) DO UPDATE SET monto_anual = excluded.monto_anual
    `).run(ciclo.ciclo, accion.concepto.toUpperCase(), parseFloat(accion.monto_anual));

    return `✅ Presupuesto definido!\n📁 ${accion.concepto.toUpperCase()}\n💰 $${fmt(accion.monto_anual)} USD/año\n📅 Ciclo ${ciclo.label}`;
  }

  // VER PRESUPUESTOS
  if (accion.accion === "ver_presupuestos") {
    const cicloStr = accion.ciclo || getCicloActual().ciclo;
    const ciclo = parseCiclo(cicloStr);
    if (!ciclo) return "❌ Ciclo inválido.";

    const presupuestos = db.prepare(
      "SELECT * FROM presupuestos WHERE ciclo = ? ORDER BY concepto"
    ).all(ciclo.ciclo);

    if (!presupuestos.length) return `📋 No hay presupuestos definidos para ciclo ${ciclo.label}.\nUsá "presupuesto [categoría] [monto]" para crear uno.`;

    // Obtener gastos reales del ciclo
    const hoy = new Date().toISOString().slice(0, 10);
    const fechaHasta = ciclo.fecha_hasta < hoy ? ciclo.fecha_hasta : hoy;

    const lineas = presupuestos.map(p => {
      const real = db.prepare(`
        SELECT COALESCE(SUM(egreso), 0) as total
        FROM transacciones 
        WHERE concepto = ? AND fecha >= ? AND fecha <= ?
      `).get(p.concepto, ciclo.fecha_desde, fechaHasta);

      const gastado = real.total;
      const pct = ((gastado / p.monto_anual) * 100).toFixed(0);
      const warn = gastado > p.monto_anual ? " ⚠️ EXCEDIDO" : "";
      const bar = gastado > 0 ? ` [${"█".repeat(Math.min(Math.round(pct / 10), 10))}${"░".repeat(Math.max(10 - Math.round(pct / 10), 0))}]` : "";
      return `📁 ${p.concepto}\n   $${fmt(gastado)} / $${fmt(p.monto_anual)} (${pct}%)${bar}${warn}`;
    });

    const totalPresup = presupuestos.reduce((s, p) => s + p.monto_anual, 0);
    return `📐 *Presupuestos — Ciclo ${ciclo.label}*\n\n${lineas.join("\n\n")}\n\n💰 Total presupuestado: $${fmt(totalPresup)} USD`;
  }

  // INFORME MENSUAL (bajo demanda)
  if (accion.accion === "informe_mensual") {
    const meses = ["","Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"];
    const anio = accion.anio || new Date().getFullYear();
    const mes = accion.mes || new Date().getMonth() + 1;
    const informe = getInformeMensual(anio, mes);

    if (!informe.rows.length) return `📊 No hay movimientos en ${meses[mes]} ${anio}.`;

    const lineas = informe.rows.filter(r => r.total_egreso > 0).map(r => {
      const presup = informe.presupuestoMap[r.concepto];
      const pct = presup ? ` (${((r.total_egreso / presup) * 100).toFixed(0)}% presup.)` : "";
      const warn = presup && r.total_egreso > presup ? " ⚠️" : "";
      return `  • ${r.concepto}: $${fmt(r.total_egreso)}${pct}${warn}`;
    });

    let msg = `📊 *IMPROLUX — ${meses[mes]} ${anio}*\n\n`;
    msg += lineas.join("\n");
    msg += `\n\n📤 Egresos: $${fmt(informe.totalEgresos)} USD`;
    msg += `\n📥 Ingresos: $${fmt(informe.totalIngresos)} USD`;
    msg += `\n💰 Neto: $${fmt(informe.totalIngresos - informe.totalEgresos)} USD`;
    return msg;
  }

  // INFORME PDF (devuelve link)
  if (accion.accion === "informe_pdf") {
    const tipo = (accion.tipo === 'contable') ? 'contable' : 'productivo';
    const cicloStr = accion.ciclo || getCicloActual(tipo).ciclo;
    const ciclo = parseCiclo(cicloStr, tipo);
    if (!ciclo) return "❌ Ciclo inválido. Usá formato 25/26.";
    const url = `${PUBLIC_URL}/api/informe-pdf?ciclo=${encodeURIComponent(cicloStr)}&tipo=${tipo}`;
    const etiqueta = tipo === 'contable' ? 'Contable (jul→jun)' : 'Productivo (mar→feb)';
    return `📄 *Informe PDF — Ciclo ${etiqueta} ${ciclo.label}*\n\n📥 Descargá tu informe acá:\n${url}\n\nIncluye: desglose por categoría, evolución mensual y gastos mes a mes.`;
  }

  // INFORME MENSUAL PDF (devuelve link)
  if (accion.accion === "informe_mensual_pdf") {
    const meses = ["","Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"];
    const anio = accion.anio || new Date().getFullYear();
    const mes = accion.mes || new Date().getMonth() + 1;
    const url = `${PUBLIC_URL}/api/informe-mensual-pdf?anio=${anio}&mes=${mes}`;
    return `📄 *Informe PDF — ${meses[mes]} ${anio}*\n\n📥 Descargá tu informe acá:\n${url}\n\nIncluye: gastos por categoría con presupuesto y totales.`;
  }

  // BACKUP CSV
  if (accion.accion === "backup") {
    const tipo = accion.tipo || "transacciones";
    if (tipo === "completo") {
      const url = `${PUBLIC_URL}/api/backup-completo`;
      return `💾 *Backup completo generado!*\n\n📥 Descargá acá:\n${url}\n\nIncluye: transacciones, cuentas corrientes, cheques, inversores y presupuestos.`;
    }
    const url = `${PUBLIC_URL}/api/backup`;
    return `💾 *Backup de transacciones generado!*\n\n📥 Descargá acá:\n${url}\n\nPara backup completo (todas las tablas) decí "backup completo".`;
  }

  return "No entendí eso. Intentá de nuevo.";
}


// ── PROCESAR IMAGEN (Claude Vision) ──────────────────────────────────────────
async function procesarImagen(mediaUrl, mediaType, bodyText) {
  const tc = await getTipoCambio();
  const hoy = new Date().toISOString().slice(0,10);

  const authHeader = 'Basic ' + Buffer.from(
    `${process.env.TWILIO_ACCOUNT_SID}:${process.env.TWILIO_AUTH_TOKEN}`
  ).toString('base64');

  const imgResp = await fetch(mediaUrl, { headers: { Authorization: authHeader } });
  if (!imgResp.ok) throw new Error(`No pude descargar la imagen: ${imgResp.status}`);

  const buffer = await imgResp.arrayBuffer();
  const base64 = Buffer.from(buffer).toString('base64');
  const mime = (mediaType || 'image/jpeg').split(';')[0];

  const prompt = `Sos el asistente financiero de IMPROLUX, empresa ganadera uruguaya.
Analiza esta imagen (ticket, factura o comprobante) y extrae los datos.
FECHA HOY: ${hoy}. TC BROU: ${tc ? `$${tc.toFixed(2)} UYU/USD` : 'no disponible'}.
MONEDA SISTEMA: USD. Si el monto esta en pesos UYU, dividir por el TC para convertir a USD.
CATEGORIAS: ALQUILER, ALQUILER ESTRUCTURA, ALIMENTACION RECRIA, ALIMENTACION CRIA, TERMINACION, INSUMOS VETERINARIOS, TRABAJOS VETERINARIOS, COMBUSTIBLE CAMPO, COMBUSTIBLE VIATICOS, SUELDO JORNAL, SUELDO ENCARGADO, SUELDO ADM, VERDEOS Y PASTURAS, ESTRUCTURA GANADERA, MANTENIMIENTO CAMPO, MANTENIMIENTO MAQUINARIA, GASTOS VENTAS GANADERAS, INVERSION MAQUINARIA, COMPRA GANADO, COMPRA HERRAMIENTAS, BPS, GASTOS ADM, PROVISTA, VEHICULOS, TELEFONO, INTERESES, OTROS
Responde SOLO con JSON valido sin texto extra ni markdown:
{"encontrado":true,"fecha":"YYYY-MM-DD","concepto":"CATEGORIA","detalle":"descripcion breve","monto_uyu":0,"egreso_usd":0,"nota":"conversion u otros detalles"}
Si no es comprobante o no podes leer los datos: {"encontrado":false,"nota":"motivo"}
${bodyText ? `El usuario tambien escribio: "${bodyText}"` : ''}`;

  const result = await anthropic.messages.create({
    model: "claude-haiku-4-5-20251001",
    max_tokens: 500,
    messages: [{
      role: "user",
      content: [
        { type: "image", source: { type: "base64", media_type: mime, data: base64 } },
        { type: "text", text: prompt }
      ]
    }]
  });

  const raw = result.content[0].text.trim().replace(/```json\s*/gi,"").replace(/```\s*/g,"").trim();
  return JSON.parse(raw);
}

// ── PROCESAR AUDIO (Whisper via Twilio URL → transcripción con Claude) ────────
async function procesarAudio(mediaUrl, mediaType) {
  const authHeader = 'Basic ' + Buffer.from(
    `${process.env.TWILIO_ACCOUNT_SID}:${process.env.TWILIO_AUTH_TOKEN}`
  ).toString('base64');

  const audioResp = await fetch(mediaUrl, { headers: { Authorization: authHeader } });
  if (!audioResp.ok) throw new Error(`No pude descargar el audio: ${audioResp.status}`);

  const buffer = await audioResp.arrayBuffer();
  const base64 = Buffer.from(buffer).toString('base64');

  // Claude puede transcribir audio OGG/MP3 directamente
  const result = await anthropic.messages.create({
    model: "claude-haiku-4-5-20251001",
    max_tokens: 300,
    messages: [{
      role: "user",
      content: [
        {
          type: "document",
          source: { type: "base64", media_type: mediaType || "audio/ogg", data: base64 }
        },
        { type: "text", text: "Transcribí exactamente lo que dice este audio de WhatsApp. Devolvé solo el texto transcripto, sin comentarios ni explicaciones." }
      ]
    }]
  });

  return result.content[0].text.trim();
}

// ── PROCESADOR CENTRAL ────────────────────────────────────────────────────────
function campoDefaultTareas(){
  try { const r=db.prepare("SELECT nombre FROM campos ORDER BY orden LIMIT 1").get(); return r?r.nombre:null; } catch(e){ return null; }
}
function detectarConsultaPendientes(body){
  const b=(body||'').trim().toLowerCase(); if(!b) return false;
  if(/^(ver|mostr[a\u00e1]?r?|list[a\u00e1]r?|dame|pas[a\u00e1]me|cu[a\u00e1]les son|mis)\b.*(pendiente|tarea|para hacer|anot|nota|hacer)/.test(b)) return true;
  if(/^(pendiente|tarea)s?\b\s*\??$/.test(b)) return true;
  if(/(pendiente|tarea)s\b\s*\??$/.test(b)) return true;
  if(/qu[e\u00e9]\s+(hay|tengo|ten[e\u00e9]s|falta|me falta).*(hacer|pendiente|anotad)/.test(b)) return true;
  if(/qu[e\u00e9]\s+(anot[e\u00e9]|apunt[e\u00e9]|hay anotado|ten[i\u00ed]a anotado)/.test(b)) return true;
  if(/^(para hacer|to-?do)\s*\??$/.test(b)) return true;
  return false;
}
function detectarTareaHecha(body){
  const b=(body||'').trim(); const low=b.toLowerCase();
  const m=low.match(/^(list[oa]|hecho|terminad[oa]|termin[e\u00e9]|complet[e\u00e9]|completad[oa]|ya est[a\u00e1]|marc[a\u00e1]r?\s+(?:como\s+)?hecho)(?=\s|$|\d)/);
  if(!m) return null;
  const num=b.match(/\b(\d{1,3})\b/);
  let resto=b.slice(m[0].length).replace(/^\s*(el|la|los|las|lo de|de|n[\u00b0\u00ba]?|numero|nro\.?|#|tarea|pendiente)\s*/i,'').trim();
  return { n: num?parseInt(num[1]):null, texto: (!num && resto.length>2)?resto:null };
}
function detectarNotaCampo(body){
  const b=(body||'').trim(); if(!b) return null;
  const low=b.toLowerCase();
  const kw=low.match(/^(nota|not\u00e1|recordar|record\u00e1|recordatorio|acordate|acord\u00e1te|acordarme|anot\u00e1|anotar|anota|apunt\u00e1|apuntar|apunta|pendiente|tarea|hay que|para hacer|todo)\b[:\-\s]*/i);
  if(kw){ let t=b.slice(kw[0].length).trim().replace(/^(de|que)\s+/i,""); return t||null; }
  const tieneMonto=/\d[\d.,]*\s*(usd|uyu|u\$s|pesos|d[o\u00f3]lares)/i.test(low) || /\$\s*\d/.test(b) || /\d\s*\$/.test(b);
  if(!tieneMonto){
    const verbo=low.match(/^(arreglar|arregl\u00e1|revisar|revis\u00e1|reparar|repar\u00e1|cambiar|cambi\u00e1|cortar|limpiar|controlar|comprar|compr\u00e1|llamar|llam\u00e1|buscar|busc\u00e1|traer|tra\u00e9|llevar|llev\u00e1|marcar|apartar|sacar|poner|pon\u00e9|mandar|mand\u00e1|pedir|ped\u00ed|pintar|soldar|desmalezar|alambrar|vacunar|curar|mover|sembrar|fumigar|rociar|colocar|instalar|verificar|chequear|terminar)\b/i);
    if(verbo) return b;
  }
  return null;
}
async function procesarMensaje(body, mediaUrl, mediaType) {
  const usuario = "improlux";
  const historial = getHistorial(usuario);
  const hoy = new Date().toISOString().slice(0,10);
  const tc = await getTipoCambio();

  // ── IMAGEN ──
  if (mediaUrl && mediaType && mediaType.startsWith('image/')) {
    try {
      const datos = await procesarImagen(mediaUrl, mediaType, body);

      if (!datos.encontrado) {
        return `📷 No pude extraer datos del comprobante.
${datos.nota || "Intentá con una foto más clara."}`;
      }

      const msg = `📷 *Comprobante detectado*

` +
        `📅 Fecha: ${datos.fecha}
` +
        `📁 Concepto: ${datos.concepto}
` +
        `📝 Detalle: ${datos.detalle}
` +
        `💰 Monto: $${parseFloat(datos.egreso_usd).toFixed(2)} USD` +
        (datos.monto_uyu ? ` ($${datos.monto_uyu} UYU)` : '') +
        (datos.nota ? `
💬 ${datos.nota}` : '') +
        `

Respondé *SI* para confirmar o corregí lo que necesites.`;

      db.prepare(`INSERT INTO sesiones (usuario,historial,updated_at) VALUES (?,?,datetime('now'))
        ON CONFLICT(usuario) DO UPDATE SET historial=excluded.historial,updated_at=excluded.updated_at`)
        .run('improlux_img_pending', JSON.stringify(datos));

      historial.push({ role: "user", content: "[Foto de comprobante]" });
      historial.push({ role: "assistant", content: msg });
      saveHistorial(usuario, historial);
      return msg;
    } catch(e) {
      console.error("Error imagen:", e.message);
      return `❌ Error procesando imagen: ${e.message}`;
    }
  }

  // ── AUDIO ──
  if (mediaUrl && mediaType && (mediaType.startsWith('audio/') || mediaType.includes('ogg'))) {
    try {
      const transcripcion = await procesarAudio(mediaUrl, mediaType);
      if (!transcripcion) return "🎤 No pude transcribir el audio. Intentá de nuevo.";
      // Procesar la transcripción como si fuera texto normal
      return await procesarMensaje(transcripcion, null, null);
    } catch(e) {
      console.error("Error audio:", e.message);
      return `❌ Error procesando audio: ${e.message}`;
    }
  }

  // ── CONFIRMAR IMAGEN PENDIENTE ──
  if (body && body.trim().toUpperCase() === 'SI') {
    const pending = db.prepare("SELECT historial FROM sesiones WHERE usuario='improlux_img_pending'").get();
    if (pending) {
      try {
        const datos = JSON.parse(pending.historial);
        db.prepare(`INSERT INTO transacciones (fecha,concepto,detalle,ingreso,egreso,proveedor,es_cc,tc,fuente)
          VALUES (?,?,?,0,?,?,0,?,'whatsapp_foto')`)
          .run(datos.fecha||hoy, datos.concepto, datos.detalle, parseFloat(datos.egreso_usd)||0, "", tc||0);
        db.prepare("DELETE FROM sesiones WHERE usuario='improlux_img_pending'").run();
        const resp = `✅ Registrado!
📝 ${datos.detalle}
📤 $${parseFloat(datos.egreso_usd).toFixed(2)} USD
📁 ${datos.concepto}`;
        historial.push({ role: "user", content: "SI" });
        historial.push({ role: "assistant", content: resp });
        saveHistorial(usuario, historial);
        return resp;
      } catch(e) { console.error("Error confirmando imagen:", e); }
    }
  }

  // ── INTERCEPT INVERSORES (antes del LLM, para que registre sí o sí) ──
  {
    const b = (body || "").trim();
    const low = b.toLowerCase();
    const mencionaInv = /\binversor(es)?\b/.test(low);
    const esConsulta = /(cu[aá]nto|deuda|lista|listar|ver\b|mostr|estado|activos|total|qui[eé]n)/.test(low);
    const esPago = /(pag[aoóué]|devol|cerr|salda|liquid)/.test(low);

    if (mencionaInv && esPago) {
      const mn = b.match(/(?:pag[a-záéíóúñ]*|devol[a-záéíóúñ]*|cerr[a-záéíóúñ]*|salda[a-záéíóúñ]*|liquid[a-záéíóúñ]*)\s+(?:a\s+|al\s+)?(?:inversor\s+)?([a-záéíóúñ][a-záéíóúñ\s.]+)/i)
             || b.match(/inversor\s+([a-záéíóúñ][a-záéíóúñ\s.]+)/i);
      const nombre = mn ? mn[1].trim().replace(/\s+(hoy|ahora|ya)$/i, '') : null;
      if (nombre) {
        const resp = await ejecutarAccion({ accion: 'pago_inversor', inversor: nombre });
        historial.push({ role: "user", content: body }); historial.push({ role: "assistant", content: resp }); saveHistorial(usuario, historial);
        return resp;
      }
    }

    if (mencionaInv && !esConsulta && !esPago && /\d/.test(b)) {
      // Tasa: "7%" o "al 7"
      let tasa = null;
      const mt = b.match(/(\d+(?:[.,]\d+)?)\s*%/) || b.match(/\bal\s+(\d+(?:[.,]\d+)?)\b/i);
      if (mt) tasa = parseFloat(mt[1].replace(',', '.')) / 100;
      const bSinTasa = mt ? b.replace(mt[0], ' ') : b;
      // Capital: primer número (con miles/decimales)
      const mc = bSinTasa.match(/(\d[\d.]*(?:,\d+)?)/);
      const capital = mc ? parseFloat(mc[1].replace(/\./g, '').replace(',', '.')) : null;
      // Nombre: entre "inversor" y el primer dígito
      const mn = b.match(/inversor(?:es)?\s+([a-záéíóúñ][a-záéíóúñ\s.]+?)\s*\d/i);
      const nombre = mn ? mn[1].trim() : null;
      if (nombre && capital) {
        const resp = await ejecutarAccion({ accion: 'nuevo_inversor', inversor: nombre.toUpperCase(), capital, tasa: tasa || 0.08 });
        historial.push({ role: "user", content: body }); historial.push({ role: "assistant", content: resp }); saveHistorial(usuario, historial);
        return resp;
      }
    }
  }

  // \u2500\u2500 CONSULTA / GESTI\u00d3N DE PENDIENTES (leer y marcar hecho) \u2500\u2500
  {
    const campoP = campoDefaultTareas();
    const cond = campoP ? " AND campo=?" : "";
    const arg = campoP ? [campoP] : [];
    const hecha = detectarTareaHecha(body);
    let manejado = false;
    if (hecha) {
      const pend = db.prepare("SELECT id, texto FROM tareas_campo WHERE estado='PENDIENTE'"+cond+" ORDER BY id").all(...arg);
      let obj = null;
      if (hecha.n && hecha.n>=1 && hecha.n<=pend.length) obj = pend[hecha.n-1];
      else if (hecha.texto) obj = pend.find(t => (t.texto||'').toLowerCase().includes(hecha.texto.toLowerCase()));
      if (obj) {
        db.prepare("UPDATE tareas_campo SET estado='HECHO', done_at=datetime('now') WHERE id=?").run(obj.id);
        const rest = db.prepare("SELECT COUNT(*) n FROM tareas_campo WHERE estado='PENDIENTE'"+cond).get(...arg).n;
        const resp = `\u2705 Marcado como hecho: "${obj.texto}"\n\nQuedan ${rest} pendiente${rest===1?'':'s'}.`;
        historial.push({ role: "user", content: body }); historial.push({ role: "assistant", content: resp }); saveHistorial(usuario, historial);
        return resp;
      }
      manejado = true; // pidi\u00f3 marcar algo pero no lo encontr\u00e9 \u2192 muestro la lista
    }
    if (detectarConsultaPendientes(body) || manejado) {
      const pend = db.prepare("SELECT texto FROM tareas_campo WHERE estado='PENDIENTE'"+cond+" ORDER BY id").all(...arg);
      let resp;
      if (!pend.length) resp = `\ud83d\udccb No hay pendientes${campoP?` en ${campoP}`:''}. \ud83c\udf89`;
      else resp = `\ud83d\udccb *Pendientes${campoP?` \u00b7 ${campoP}`:''}* (${pend.length})\n\n` + pend.map((t,i)=>`${i+1}. ${t.texto}`).join('\n') + `\n\n_Para marcar una: escrib\u00ed "hecho 2" o "listo <lo que sea>"._`;
      historial.push({ role: "user", content: body }); historial.push({ role: "assistant", content: resp }); saveHistorial(usuario, historial);
      return resp;
    }
  }

  // \u2500\u2500 INTERCEPT NOTA / PENDIENTE DE CAMPO \u2500\u2500
  {
    const texto = detectarNotaCampo(body);
    if (texto) {
      const campo = campoDefaultTareas();
      db.prepare("INSERT INTO tareas_campo (campo, texto, estado, origen) VALUES (?,?, 'PENDIENTE', 'bot')").run(campo, texto);
      const resp = `\ud83d\udccc Anotado en pendientes${campo?` de ${campo}`:''}:\n"${texto}"\n\nLo ves y lo gest\u00edonas desde la app \u2192 Campo \u2192 Pendientes.`;
      historial.push({ role: "user", content: body }); historial.push({ role: "assistant", content: resp }); saveHistorial(usuario, historial);
      return resp;
    }
  }

  // \u2500\u2500 TEXTO NORMAL \u2500\u2500
  historial.push({ role: "user", content: body || "" });
  const contexto = await buildContexto();

  // Loop de tool-use: el modelo puede consultar la base (SELECT) las veces que necesite antes de responder.
  const mensajes = historial.map(m => ({ role: m.role, content: m.content }));
  let raw = "";
  let guard = 0;
  while (guard++ < 6) {
    const result = await anthropic.messages.create({
      model: "claude-sonnet-4-6",
      max_tokens: 1500,
      system: contexto,
      tools: [DASHBOARD_TOOL],
      messages: mensajes,
    });

    if (result.stop_reason === "tool_use") {
      mensajes.push({ role: "assistant", content: result.content });
      const toolResults = [];
      for (const bloque of result.content) {
        if (bloque.type === "tool_use") {
          const salida = bloque.name === "consultar_datos" ? consultarDB(bloque.input) : JSON.stringify({ error: "herramienta desconocida" });
          toolResults.push({ type: "tool_result", tool_use_id: bloque.id, content: salida });
        }
      }
      mensajes.push({ role: "user", content: toolResults });
      continue; // volver a llamar al modelo con los resultados
    }

    // Respuesta final (texto)
    raw = (result.content.filter(c => c.type === "text").map(c => c.text).join("\n")).trim();
    break;
  }

  historial.push({ role: "assistant", content: raw });
  saveHistorial(usuario, historial);

  const limpio = raw.replace(/```json\s*/gi,"").replace(/```\s*/g,"").trim();
  try {
    // Multi-accion: array [{...},{...}]
    const matchArray = limpio.match(/\[[\s\S]*"accion"[\s\S]*\]/);
    if (matchArray) {
      const acciones = JSON.parse(matchArray[0]);
      if (Array.isArray(acciones) && acciones.length > 0) {
        const resultados = [];
        for (const accion of acciones) {
          if (accion?.accion) resultados.push(await ejecutarAccion(accion));
        }
        if (resultados.length > 0) return resultados.join("\n\n");
      }
    }
    // Accion unica
    const matchSingle = limpio.match(/\{[\s\S]*"accion"[\s\S]*\}/);
    if (matchSingle) {
      const accion = JSON.parse(matchSingle[0]);
      if (accion?.accion) return await ejecutarAccion(accion);
    }
    return limpio;
  } catch(_) { return limpio; }
}

// Ejecutar una acción ya parseada (usado por el chat de la app web)
app.post("/api/ejecutar-accion", async (req, res) => {
  try {
    const accion = req.body.accion || req.body;
    if (!accion || !accion.accion) return res.status(400).json({ error: "Falta la acción" });
    const respuesta = await ejecutarAccion(accion);
    res.json({ respuesta });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── WEBHOOK INTERNO (desde panel web) ────────────────────────────────────────
app.post("/webhook-interno", async (req, res) => {
  try {
    const body = (req.body.Body || "").trim();
    const respuesta = await procesarMensaje(body, null, null);
    res.json({ respuesta });
  } catch(err) {
    console.error("Error webhook-interno:", err);
    res.json({ respuesta: "❌ Error interno. Intentá de nuevo." });
  }
});

// Envía uno o varios mensajes de WhatsApp (parte los largos, límite ~1500 chars)
async function enviarWhatsApp(from, to, texto) {
  let t = String(texto || "").trim();
  if (!t || !to) return;
  const MAX = 1500;
  const partes = [];
  while (t.length > MAX) {
    let corte = t.lastIndexOf("\n", MAX);
    if (corte < MAX * 0.6) corte = MAX;
    partes.push(t.slice(0, corte).trim());
    t = t.slice(corte).replace(/^\n+/, "");
  }
  if (t) partes.push(t);
  for (const p of partes) {
    await twilioClient.messages.create({ from, to, body: p });
  }
}

// ── WEBHOOK WHATSAPP ──────────────────────────────────────────────────────────
app.post("/webhook", async (req, res) => {
  // Ack inmediato: Twilio corta a los ~15s, así que respondemos vacío y contestamos por API.
  res.type("text/xml").send("<Response></Response>");

  const to   = req.body.From;              // usuario que escribió
  const from = req.body.To || TWILIO_NUMBER; // número del bot (formato whatsapp:+...)
  try {
    const body      = (req.body.Body || "").trim();
    const numMedia  = parseInt(req.body.NumMedia || "0");
    const mediaUrl  = numMedia > 0 ? req.body.MediaUrl0 : null;
    const mediaType = numMedia > 0 ? (req.body.MediaContentType0 || "") : null;
    const respuesta = await procesarMensaje(body, mediaUrl, mediaType);
    await enviarWhatsApp(from, to, respuesta);
  } catch(err) {
    console.error("Error webhook:", err);
    try { await enviarWhatsApp(from, to, "❌ Ocurrió un error. Intentá de nuevo."); } catch(_) {}
  }
});


// ── CARGA MASIVA (importar histórico) ─────────────────────────────────────────
app.post("/api/importar", (req, res) => {
  const { transacciones } = req.body;
  if (!Array.isArray(transacciones)) return res.status(400).json({ error: 'Formato inválido' });
  
  let ok = 0, errores = 0;
  const stmt = db.prepare(`
    INSERT INTO transacciones (fecha, concepto, detalle, ingreso, egreso, proveedor, tc, fuente)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);
  
  for (const t of transacciones) {
    try {
      stmt.run(
        t.fecha || new Date().toISOString().slice(0,10),
        t.concepto || '',
        t.detalle || '',
        parseFloat(t.ingreso) || 0,
        parseFloat(t.egreso) || 0,
        t.proveedor || '',
        t.tc || null,
        t.fuente || 'historico'
      );
      ok++;
    } catch(e) { errores++; }
  }
  res.json({ ok, errores, total: transacciones.length });
});

// ── API REST PARA BOT HTML ────────────────────────────────────────────────────
app.get("/api/transacciones", (req, res) => {
  const limite = parseInt(req.query.limite) || 100;
  const rows = db.prepare("SELECT * FROM transacciones ORDER BY fecha DESC, created_at DESC LIMIT ?").all(limite);
  res.json(rows);
});

app.get("/api/cc_movimientos", (req, res) => {
  const proveedor = req.query.proveedor;
  if (proveedor) {
    const detalle = getDetalleCuentaCorriente(proveedor);
    const saldo = getSaldoProveedor(proveedor);
    res.json({ proveedor, saldo, movimientos: detalle });
  } else {
    const movs = db.prepare("SELECT * FROM cc_movimientos ORDER BY fecha DESC, created_at DESC LIMIT 100").all();
    res.json(movs);
  }
});

app.get("/api/cuentas", (req, res) => {
  const cuentas = db.prepare("SELECT * FROM cuentas_corrientes ORDER BY proveedor").all();
  const conSaldo = cuentas.map(c => ({ ...c, saldo: getSaldoProveedor(c.proveedor) }));
  res.json(conSaldo);
});

app.get("/api/cheques", (req, res) => {
  const rows = db.prepare("SELECT * FROM cheques ORDER BY fecha_cobro ASC").all();
  res.json(rows);
});

app.get("/api/inversores", (req, res) => {
  const rows = db.prepare("SELECT * FROM inversores ORDER BY inversor").all();
  const conDeuda = rows.map(i => ({ ...i, deuda_calculada: calcularDeudaInversor(i) }));
  res.json(conDeuda);
});

app.get("/api/tc", async (req, res) => {
  const tc = await getTipoCambio();
  res.json({ tc, fecha: new Date().toISOString().slice(0, 10) });
});

app.get("/api/resumen", (req, res) => {
  const mesActual = new Date().toISOString().slice(0, 7);
  const egresosMes = db.prepare(`SELECT SUM(egreso) as total FROM transacciones WHERE fecha LIKE ?`).get(`${mesActual}-%`);
  const ingresosMes = db.prepare(`SELECT SUM(ingreso) as total FROM transacciones WHERE fecha LIKE ?`).get(`${mesActual}-%`);
  const chequesPend = db.prepare("SELECT COUNT(*) as total, SUM(monto) as monto FROM cheques WHERE estado = 'PENDIENTE'").get();
  const inversores = getInversoresActivos();
  const totalDeuda = inversores.reduce((s, i) => s + calcularDeudaInversor(i), 0);
  const totalMovimientos = db.prepare("SELECT COUNT(*) as total FROM transacciones").get();

  res.json({
    egresos_mes: egresosMes?.total || 0,
    ingresos_mes: ingresosMes?.total || 0,
    cheques_pendientes: chequesPend?.total || 0,
    monto_cheques: chequesPend?.monto || 0,
    deuda_inversores: totalDeuda,
    total_movimientos: totalMovimientos?.total || 0
  });
});

// ── API PRESUPUESTOS ──────────────────────────────────────────────────────────
app.get("/api/presupuestos", (req, res) => {
  const cicloStr = req.query.ciclo || getCicloActual().ciclo;
  const ciclo = parseCiclo(cicloStr);
  if (!ciclo) return res.status(400).json({ error: "Ciclo inválido" });

  const presupuestos = db.prepare(
    "SELECT * FROM presupuestos WHERE ciclo = ? ORDER BY concepto"
  ).all(ciclo.ciclo);

  const hoy = new Date().toISOString().slice(0, 10);
  const fechaHasta = ciclo.fecha_hasta < hoy ? ciclo.fecha_hasta : hoy;

  const resultado = presupuestos.map(p => {
    const real = db.prepare(`
      SELECT COALESCE(SUM(egreso), 0) as total
      FROM transacciones WHERE concepto = ? AND fecha >= ? AND fecha <= ?
    `).get(p.concepto, ciclo.fecha_desde, fechaHasta);
    return { ...p, gastado: real.total, porcentaje: p.monto_anual > 0 ? ((real.total / p.monto_anual) * 100) : 0 };
  });

  res.json({ ciclo: ciclo.label, presupuestos: resultado });
});

app.post("/api/presupuestos", (req, res) => {
  const { ciclo, concepto, monto_anual } = req.body;
  const cicloObj = parseCiclo(ciclo || getCicloActual().ciclo);
  if (!cicloObj || !concepto || !monto_anual) return res.status(400).json({ error: "Faltan datos" });

  db.prepare(`
    INSERT INTO presupuestos (ciclo, concepto, monto_anual)
    VALUES (?, ?, ?)
    ON CONFLICT(ciclo, concepto) DO UPDATE SET monto_anual = excluded.monto_anual
  `).run(cicloObj.ciclo, concepto.toUpperCase(), parseFloat(monto_anual));

  res.json({ ok: true, ciclo: cicloObj.label, concepto: concepto.toUpperCase(), monto_anual: parseFloat(monto_anual) });
});

app.post("/api/presupuestos/bulk", (req, res) => {
  const { ciclo, presupuestos } = req.body;
  const cicloObj = parseCiclo(ciclo || getCicloActual().ciclo);
  if (!cicloObj || !Array.isArray(presupuestos)) return res.status(400).json({ error: "Datos inválidos" });

  const stmt = db.prepare(`
    INSERT INTO presupuestos (ciclo, concepto, monto_anual)
    VALUES (?, ?, ?)
    ON CONFLICT(ciclo, concepto) DO UPDATE SET monto_anual = excluded.monto_anual
  `);

  let ok = 0;
  for (const p of presupuestos) {
    if (p.concepto && p.monto_anual) {
      stmt.run(cicloObj.ciclo, p.concepto.toUpperCase(), parseFloat(p.monto_anual));
      ok++;
    }
  }
  res.json({ ok, ciclo: cicloObj.label });
});

app.delete("/api/presupuestos/:id", (req, res) => {
  db.prepare("DELETE FROM presupuestos WHERE id = ?").run(req.params.id);
  res.json({ ok: true });
});

// Promedio histórico de egreso por categoría (sobre ciclos productivos COMPLETOS) → sugerencia de presupuesto
app.get("/api/presupuesto-sugerido", (req, res) => {
  const hoy = new Date().toISOString().slice(0, 10);
  const rows = db.prepare("SELECT concepto, fecha, egreso FROM transacciones WHERE egreso > 0").all();
  const porCatCiclo = {};       // porCatCiclo[concepto][anioInicio] = suma
  const ciclosCompletos = new Set();
  for (const r of rows) {
    if (!r.fecha || r.fecha.length < 7) continue;
    const Y = parseInt(r.fecha.slice(0, 4));
    const M = parseInt(r.fecha.slice(5, 7));
    const anioInicio = M >= 3 ? Y : Y - 1;             // ciclo productivo mar→feb
    const finCiclo = `${anioInicio + 1}-02-29`;
    if (!(finCiclo < hoy)) continue;                   // solo ciclos ya cerrados
    ciclosCompletos.add(anioInicio);
    (porCatCiclo[r.concepto] = porCatCiclo[r.concepto] || {});
    porCatCiclo[r.concepto][anioInicio] = (porCatCiclo[r.concepto][anioInicio] || 0) + r.egreso;
  }
  const nCiclos = ciclosCompletos.size;
  const promedios = {};
  Object.keys(porCatCiclo).forEach(cat => {
    const total = Object.values(porCatCiclo[cat]).reduce((s, v) => s + v, 0);
    promedios[cat] = nCiclos > 0 ? Math.round((total / nCiclos) * 100) / 100 : 0;
  });
  res.json({
    ciclos_completos: nCiclos,
    ciclos: [...ciclosCompletos].sort().map(y => `${String(y % 100).padStart(2, '0')}/${String((y + 1) % 100).padStart(2, '0')}`),
    promedio_por_categoria: promedios
  });
});

// ── BIENES MUEBLES (activos valuados con amortización lineal) ──────────────────
function calcBien(b) {
  const valor = parseFloat(b.valor_compra) || 0;
  const residual = parseFloat(b.valor_residual) || 0;
  const vida = parseFloat(b.vida_util_anios) || 1;
  const amortAnual = Math.max(0, (valor - residual) / vida);
  const anios = b.fecha_compra ? Math.max(0, (new Date() - new Date(b.fecha_compra)) / (365.25 * 24 * 3600 * 1000)) : 0;
  const base = Math.max(0, valor - residual);
  const amortAcum = Math.min(base, amortAnual * anios);
  const valorActual = Math.max(residual, valor - amortAcum);
  return {
    ...b,
    amort_anual: Math.round(amortAnual * 100) / 100,
    anios_transcurridos: Math.round(anios * 10) / 10,
    amort_acumulada: Math.round(amortAcum * 100) / 100,
    valor_actual: Math.round(valorActual * 100) / 100,
    pct_amortizado: base > 0 ? Math.round(Math.min(100, (amortAcum / base) * 100)) : 100,
    totalmente_amortizado: valorActual <= residual + 0.01
  };
}

app.get("/api/bienes", (req, res) => {
  const campo = req.query.campo;
  const rows = campo
    ? db.prepare("SELECT * FROM bienes_muebles WHERE activo = 1 AND campo = ? ORDER BY valor_compra DESC").all(campo)
    : db.prepare("SELECT * FROM bienes_muebles WHERE activo = 1 ORDER BY valor_compra DESC").all();
  res.json(rows.map(calcBien));
});

app.post("/api/bienes", (req, res) => {
  const { nombre, categoria, valor_compra, fecha_compra, vida_util_anios, valor_residual, notas, campo } = req.body;
  if (!nombre || !valor_compra || !fecha_compra) return res.status(400).json({ error: "Faltan datos (nombre, valor y fecha de compra)" });
  const r = db.prepare(`INSERT INTO bienes_muebles (nombre, categoria, valor_compra, fecha_compra, vida_util_anios, valor_residual, notas, campo)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)`).run(nombre, categoria || '', parseFloat(valor_compra), fecha_compra, parseFloat(vida_util_anios) || 10, parseFloat(valor_residual) || 0, notas || '', (campo||'LA AMISTAD').toUpperCase());
  res.json({ id: r.lastInsertRowid });
});

app.put("/api/bienes/:id", (req, res) => {
  const b = db.prepare("SELECT * FROM bienes_muebles WHERE id = ?").get(req.params.id);
  if (!b) return res.status(404).json({ error: "No encontrado" });
  const { nombre, categoria, valor_compra, fecha_compra, vida_util_anios, valor_residual, notas } = req.body;
  db.prepare(`UPDATE bienes_muebles SET nombre=?, categoria=?, valor_compra=?, fecha_compra=?, vida_util_anios=?, valor_residual=?, notas=? WHERE id=?`)
    .run(nombre ?? b.nombre, categoria ?? b.categoria, valor_compra !== undefined ? parseFloat(valor_compra) : b.valor_compra,
      fecha_compra ?? b.fecha_compra, vida_util_anios !== undefined ? parseFloat(vida_util_anios) : b.vida_util_anios,
      valor_residual !== undefined ? parseFloat(valor_residual) : b.valor_residual, notas ?? b.notas, req.params.id);
  res.json({ ok: true });
});

app.delete("/api/bienes/:id", (req, res) => {
  db.prepare("DELETE FROM bienes_muebles WHERE id = ?").run(req.params.id);
  res.json({ ok: true });
});

// Registrar la depreciación del ciclo como gasto (AMORTIZACION MAQUINARIA)
app.post("/api/bienes/amortizar", (req, res) => {
  const cicloObj = parseCiclo(req.body.ciclo || getCicloActual('productivo').ciclo, 'productivo');
  if (!cicloObj) return res.status(400).json({ error: "Ciclo inválido" });
  const hoy = new Date().toISOString().slice(0, 10);
  let fecha = cicloObj.fecha_hasta < hoy ? cicloObj.fecha_hasta : hoy;
  if (fecha.endsWith('-02-29')) fecha = fecha.replace('-02-29', '-02-28');

  const bienes = db.prepare("SELECT * FROM bienes_muebles WHERE activo = 1").all().map(calcBien);
  const yaHechos = new Set(db.prepare("SELECT bien_id FROM amortizaciones WHERE ciclo = ?").all(cicloObj.ciclo).map(r => r.bien_id));

  let saltados = 0;
  const elegibles = [];
  for (const b of bienes) {
    if (yaHechos.has(b.id)) { saltados++; continue; }
    if (b.amort_anual <= 0) continue;
    if (b.fecha_compra && b.fecha_compra > cicloObj.fecha_hasta) continue;   // el bien no existía en ese ciclo
    if (b.totalmente_amortizado) continue;                                    // ya llegó a valor residual
    elegibles.push(b);
  }
  const total = Math.round(elegibles.reduce((s, b) => s + b.amort_anual, 0) * 100) / 100;

  let creados = 0;
  if (elegibles.length > 0) {
    // UN solo item consolidado en Movimientos por todo el ciclo
    const detalle = `Depreciación maquinaria · ciclo ${cicloObj.ciclo} (${elegibles.length} bien${elegibles.length > 1 ? 'es' : ''})`;
    const t = db.prepare(`INSERT INTO transacciones (fecha, concepto, detalle, ingreso, egreso, proveedor, tc, fuente)
      VALUES (?, 'AMORTIZACION MAQUINARIA', ?, 0, ?, '', 0, 'amortizacion')`).run(fecha, detalle, total);
    const insA = db.prepare("INSERT OR IGNORE INTO amortizaciones (bien_id, ciclo, monto, transaccion_id, fecha) VALUES (?, ?, ?, ?, ?)");
    for (const b of elegibles) insA.run(b.id, cicloObj.ciclo, b.amort_anual, t.lastInsertRowid, fecha);
    creados = elegibles.length;
  }
  res.json({ ok: true, ciclo: cicloObj.ciclo, creados, total, ya_existentes: saltados });
});

app.get("/api/bienes/amortizaciones", (req, res) => {
  const ciclo = req.query.ciclo;
  const rows = ciclo
    ? db.prepare("SELECT a.*, b.nombre FROM amortizaciones a LEFT JOIN bienes_muebles b ON b.id=a.bien_id WHERE a.ciclo=? ORDER BY a.created_at DESC").all(ciclo)
    : db.prepare("SELECT a.*, b.nombre FROM amortizaciones a LEFT JOIN bienes_muebles b ON b.id=a.bien_id ORDER BY a.created_at DESC").all();
  res.json(rows);
});

// Reemplazo: da de baja el viejo (con la pérdida/ganancia real de venta) y da de alta el nuevo
app.post("/api/bienes/reemplazar", (req, res) => {
  const { viejo_id, valor_venta, fecha_reemplazo, nuevo } = req.body;
  const viejo = db.prepare("SELECT * FROM bienes_muebles WHERE id = ?").get(viejo_id);
  if (!viejo) return res.status(404).json({ error: "No encontré el bien a reemplazar" });
  if (!nuevo || !nuevo.nombre || !nuevo.valor_compra || !nuevo.fecha_compra) return res.status(400).json({ error: "Faltan datos del bien nuevo (nombre, valor y fecha)" });

  const fecha = fecha_reemplazo || new Date().toISOString().slice(0, 10);
  const valorLibro = calcBien(viejo).valor_actual;      // valor de libro del viejo hoy
  const venta = parseFloat(valor_venta) || 0;
  const perdida = Math.round((valorLibro - venta) * 100) / 100;  // >0 pérdida · <0 ganancia

  // 1) Alta del bien nuevo
  const rNuevo = db.prepare(`INSERT INTO bienes_muebles (nombre, categoria, valor_compra, fecha_compra, vida_util_anios, valor_residual, notas)
    VALUES (?, ?, ?, ?, ?, ?, ?)`).run(nuevo.nombre, nuevo.categoria || '', parseFloat(nuevo.valor_compra), nuevo.fecha_compra,
      parseFloat(nuevo.vida_util_anios) || 10, parseFloat(nuevo.valor_residual) || 0, `Reemplaza a: ${viejo.nombre}`);

  // 2) Registrar la pérdida/ganancia real de la venta del viejo
  let transaccion = null;
  if (perdida > 0.01) {
    transaccion = db.prepare(`INSERT INTO transacciones (fecha, concepto, detalle, ingreso, egreso, proveedor, tc, fuente)
      VALUES (?, 'AMORTIZACION MAQUINARIA', ?, 0, ?, '', 0, 'baja_bien')`)
      .run(fecha, `Pérdida por venta de ${viejo.nombre} (valor libro $${fmt(valorLibro)} − venta $${fmt(venta)})`, perdida).lastInsertRowid;
  } else if (perdida < -0.01) {
    transaccion = db.prepare(`INSERT INTO transacciones (fecha, concepto, detalle, ingreso, egreso, proveedor, tc, fuente)
      VALUES (?, 'AMORTIZACION MAQUINARIA', ?, ?, 0, '', 0, 'baja_bien')`)
      .run(fecha, `Ganancia por venta de ${viejo.nombre} (venta $${fmt(venta)} − valor libro $${fmt(valorLibro)})`, Math.abs(perdida)).lastInsertRowid;
  }

  // 3) Baja del viejo (archivado, no borrado)
  db.prepare("UPDATE bienes_muebles SET activo = 0, fecha_baja = ?, valor_venta = ? WHERE id = ?").run(fecha, venta, viejo_id);

  res.json({
    ok: true, nuevo_id: rNuevo.lastInsertRowid,
    valor_libro: valorLibro, valor_venta: venta, perdida,
    resultado: perdida > 0.01 ? 'perdida' : (perdida < -0.01 ? 'ganancia' : 'neutro'),
    transaccion
  });
});

// ── PROYECCIÓN DE CAJA (12 meses hacia adelante) ──────────────────────────────
app.get("/api/proyeccion", (req, res) => {
  const meses = parseInt(req.query.meses) || 12;
  // Caja inicial: saldo real desde el ciclo 25/26
  const desde = parseCiclo('25/26', 'productivo').fecha_desde;
  const c = db.prepare("SELECT COALESCE(SUM(ingreso),0) ing, COALESCE(SUM(egreso),0) egr FROM transacciones WHERE fecha >= ?").get(desde);
  let saldo = Math.round((c.ing - c.egr) * 100) / 100;

  // Perfil de gasto POR MES del ciclo 25/26 (no un promedio plano): cada mes calendario
  // usa lo que realmente se gastó ese mes (alquiler, siembra, etc. quedan reflejados).
  const cicloDesde = parseCiclo('25/26', 'productivo').fecha_desde;   // 2025-03-01
  const cicloHasta = parseCiclo('25/26', 'productivo').fecha_hasta;   // 2026-02-28
  const perfilRows = db.prepare(`SELECT substr(fecha,1,7) mes, COALESCE(SUM(egreso),0) t FROM transacciones
    WHERE fecha >= ? AND fecha <= ? AND egreso > 0 AND fuente NOT IN ('amortizacion','inversor') GROUP BY mes`).all(cicloDesde, cicloHasta);
  const perfilMesCal = {};   // { '03': monto, '04': monto, ... }
  perfilRows.forEach(r => { const mm = r.mes.slice(5, 7); perfilMesCal[mm] = (perfilMesCal[mm] || 0) + r.t; });
  const valoresPerfil = Object.values(perfilMesCal);
  const promedioEgreso = valoresPerfil.length ? Math.round((valoresPerfil.reduce((a, b) => a + b, 0) / valoresPerfil.length) * 100) / 100 : 0;

  const ajustes = {};
  db.prepare("SELECT * FROM proyeccion_ajustes").all().forEach(a => ajustes[a.mes] = a);

  const hoy = new Date();
  const proyeccion = [];
  for (let i = 0; i < meses; i++) {
    const dfut = new Date(hoy.getFullYear(), hoy.getMonth() + i + 1, 1);
    const mes = dfut.toISOString().slice(0, 7);
    const mmCal = mes.slice(5, 7);
    const like = `${mes}-%`;
    // Cheques emitidos (a pagar) y recibidos (a cobrar) que vencen ese mes
    const chE = db.prepare("SELECT COALESCE(SUM(monto),0) m FROM cheques WHERE estado='PENDIENTE' AND tipo='EMITIDO' AND fecha_cobro LIKE ?").get(like).m;
    const chR = db.prepare("SELECT COALESCE(SUM(monto),0) m FROM cheques WHERE estado='PENDIENTE' AND tipo='RECIBIDO' AND fecha_cobro LIKE ?").get(like).m;
    const invs = db.prepare("SELECT * FROM inversores WHERE estado='ACTIVO' AND fecha_vencimiento LIKE ?").all(like);
    const invVenc = Math.round(invs.reduce((s, inv) => s + calcularDeudaInversor(inv), 0) * 100) / 100;

    const aj = ajustes[mes] || {};
    // Gasto estimado = ajuste manual, si no el gasto de ESE mes calendario en 25/26, si no el promedio
    const baseMes = (perfilMesCal[mmCal] != null) ? Math.round(perfilMesCal[mmCal] * 100) / 100 : promedioEgreso;
    const egresoEst = (aj.egreso_estimado != null) ? aj.egreso_estimado : baseMes;
    const ingresoEst = (aj.ingreso_estimado != null) ? aj.ingreso_estimado : 0;

    const saldoInicial = saldo;
    const totalIn = ingresoEst + chR;
    const totalOut = egresoEst + chE + invVenc;
    saldo = Math.round((saldoInicial + totalIn - totalOut) * 100) / 100;

    proyeccion.push({
      mes, saldo_inicial: saldoInicial,
      ingreso_estimado: ingresoEst, cheques_recibir: chR,
      egreso_estimado: egresoEst, cheques_pagar: chE, inversores: invVenc,
      saldo_final: saldo, editado: !!ajustes[mes], base_mes: baseMes
    });
  }
  res.json({ caja_inicial: Math.round((c.ing - c.egr) * 100) / 100, promedio_egreso: promedioEgreso, perfil_mensual: perfilMesCal, proyeccion });
});

app.post("/api/proyeccion/ajuste", (req, res) => {
  const { mes, egreso_estimado, ingreso_estimado, notas } = req.body;
  if (!mes) return res.status(400).json({ error: "Falta el mes" });
  db.prepare(`INSERT INTO proyeccion_ajustes (mes, egreso_estimado, ingreso_estimado, notas, updated_at)
    VALUES (?, ?, ?, ?, datetime('now'))
    ON CONFLICT(mes) DO UPDATE SET egreso_estimado=excluded.egreso_estimado, ingreso_estimado=excluded.ingreso_estimado, notas=excluded.notas, updated_at=datetime('now')`)
    .run(mes, egreso_estimado != null ? parseFloat(egreso_estimado) : null, ingreso_estimado != null ? parseFloat(ingreso_estimado) : null, notas || '');
  res.json({ ok: true });
});

app.delete("/api/proyeccion/ajuste/:mes", (req, res) => {
  db.prepare("DELETE FROM proyeccion_ajustes WHERE mes = ?").run(req.params.mes);
  res.json({ ok: true });
});

// ── NDVI SATELITAL (Sentinel-2 vía Copernicus Data Space) ─────────────────────
const COPERNICUS_ID = process.env.COPERNICUS_CLIENT_ID || "";
const COPERNICUS_SECRET = process.env.COPERNICUS_CLIENT_SECRET || "";
let _copToken = { valor: null, exp: 0 };

async function getCopernicusToken() {
  if (_copToken.valor && Date.now() < _copToken.exp) return _copToken.valor;
  if (!COPERNICUS_ID || !COPERNICUS_SECRET) throw new Error("Faltan credenciales de Copernicus (COPERNICUS_CLIENT_ID / COPERNICUS_CLIENT_SECRET)");
  const resp = await fetch("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: `grant_type=client_credentials&client_id=${encodeURIComponent(COPERNICUS_ID)}&client_secret=${encodeURIComponent(COPERNICUS_SECRET)}`
  });
  const d = await resp.json();
  if (!d.access_token) throw new Error("No pude autenticar con Copernicus");
  _copToken = { valor: d.access_token, exp: Date.now() + (d.expires_in - 60) * 1000 };
  return d.access_token;
}

// NDVI promedio del polígono de un lote (últimos N días, por pasada del satélite)
app.get("/api/lotes/:id/ndvi", async (req, res) => {
  try {
    const lote = db.prepare("SELECT * FROM lotes WHERE id = ?").get(req.params.id);
    if (!lote) return res.status(404).json({ error: "Lote no encontrado" });
    if (!lote.poligono) return res.status(400).json({ error: "El lote no tiene contorno dibujado" });

    const coords = JSON.parse(lote.poligono);              // [[lat,lng],...]
    // GeoJSON usa [lng,lat] y el anillo debe cerrar
    const ring = coords.map(c => [c[1], c[0]]);
    if (ring.length && (ring[0][0] !== ring[ring.length - 1][0] || ring[0][1] !== ring[ring.length - 1][1])) ring.push(ring[0]);

    const dias = parseInt(req.query.dias) || 60;
    const hasta = new Date();
    const desde = new Date(hasta.getTime() - dias * 86400000);
    const iso = d => d.toISOString().slice(0, 10);

    const token = await getCopernicusToken();
    const evalscript = `//VERSION=3
function setup(){return{input:[{bands:["B04","B08","dataMask"]}],output:[{id:"ndvi",bands:1,sampleType:"FLOAT32"},{id:"dataMask",bands:1}]}}
function evaluatePixel(s){let ndvi=(s.B08-s.B04)/(s.B08+s.B04);return{ndvi:[ndvi],dataMask:[s.dataMask]}}`;

    const body = {
      input: {
        bounds: { geometry: { type: "Polygon", coordinates: [ring] }, properties: { crs: "http://www.opengis.net/def/crs/EPSG/0/4326" } },
        data: [{ type: "sentinel-2-l2a", dataFilter: { timeRange: { from: iso(desde) + "T00:00:00Z", to: iso(hasta) + "T23:59:59Z" }, maxCloudCoverage: 40 } }]
      },
      aggregation: {
        timeRange: { from: iso(desde) + "T00:00:00Z", to: iso(hasta) + "T23:59:59Z" },
        aggregationInterval: { of: "P5D" },
        evalscript,
        resx: 10, resy: 10
      },
      calculations: { ndvi: { statistics: { default: { percentiles: { k: [50] } } } } }
    };

    const r = await fetch("https://sh.dataspace.copernicus.eu/api/v1/statistics", {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${token}` },
      body: JSON.stringify(body)
    });
    const d = await r.json();
    if (!r.ok) return res.status(502).json({ error: "Error de Copernicus", detalle: d });

    // Parsear la serie: para cada intervalo con datos, el NDVI medio
    const serie = (d.data || []).map(item => {
      const st = item.outputs?.ndvi?.bands?.B0?.stats;
      return st && st.sampleCount > 0 && st.mean != null
        ? { fecha: (item.interval?.from || "").slice(0, 10), ndvi: Math.round(st.mean * 1000) / 1000 }
        : null;
    }).filter(Boolean);

    const ultimo = serie.length ? serie[serie.length - 1] : null;
    res.json({ lote: lote.nombre, ndvi_actual: ultimo ? ultimo.ndvi : null, fecha: ultimo ? ultimo.fecha : null, serie });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// NDVI de TODOS los lotes con contorno (para el ranking / comparación)
app.get("/api/ndvi-ranking", async (req, res) => {
  try {
    const campo = req.query.campo;
    const dias = parseInt(req.query.dias) || 30;
    const lotes = (campo
      ? db.prepare("SELECT * FROM lotes WHERE campo = ? AND poligono IS NOT NULL AND poligono != ''").all(campo)
      : db.prepare("SELECT * FROM lotes WHERE poligono IS NOT NULL AND poligono != ''").all());
    if (!lotes.length) return res.json({ ranking: [], sin_contorno: 0 });

    let token;
    try { token = await getCopernicusToken(); }
    catch (e) { return res.status(400).json({ error: e.message }); }

    const hasta = new Date();
    const desde = new Date(hasta.getTime() - dias * 86400000);
    const iso = d => d.toISOString().slice(0, 10);
    const evalscript = `//VERSION=3
function setup(){return{input:[{bands:["B04","B08","dataMask"]}],output:[{id:"ndvi",bands:1,sampleType:"FLOAT32"},{id:"dataMask",bands:1}]}}
function evaluatePixel(s){let ndvi=(s.B08-s.B04)/(s.B08+s.B04);return{ndvi:[ndvi],dataMask:[s.dataMask]}}`;

    async function ndviDe(lote) {
      try {
        const coords = JSON.parse(lote.poligono);
        const ring = coords.map(c => [c[1], c[0]]);
        if (ring.length && (ring[0][0] !== ring[ring.length-1][0] || ring[0][1] !== ring[ring.length-1][1])) ring.push(ring[0]);
        const body = {
          input: { bounds: { geometry: { type: "Polygon", coordinates: [ring] }, properties: { crs: "http://www.opengis.net/def/crs/EPSG/0/4326" } },
            data: [{ type: "sentinel-2-l2a", dataFilter: { timeRange: { from: iso(desde)+"T00:00:00Z", to: iso(hasta)+"T23:59:59Z" }, maxCloudCoverage: 40 } }] },
          aggregation: { timeRange: { from: iso(desde)+"T00:00:00Z", to: iso(hasta)+"T23:59:59Z" }, aggregationInterval: { of: "P30D" }, evalscript, resx: 10, resy: 10 },
          calculations: { ndvi: { statistics: { default: { percentiles: { k: [50] } } } } }
        };
        const r = await fetch("https://sh.dataspace.copernicus.eu/api/v1/statistics", { method: "POST", headers: { "Content-Type": "application/json", "Authorization": `Bearer ${token}` }, body: JSON.stringify(body) });
        const d = await r.json();
        const items = (d.data || []).filter(it => it.outputs?.ndvi?.bands?.B0?.stats?.sampleCount > 0);
        const last = items.length ? items[items.length-1] : null;
        const ndvi = last ? Math.round(last.outputs.ndvi.bands.B0.stats.mean * 1000) / 1000 : null;
        return { id: lote.id, nombre: lote.nombre, hectareas: lote.hectareas || 0, ndvi, fecha: last ? (last.interval?.from||"").slice(0,10) : null };
      } catch (e) { return { id: lote.id, nombre: lote.nombre, hectareas: lote.hectareas || 0, ndvi: null, error: true }; }
    }

    // Procesar de a tandas de 4 para no saturar
    const resultados = [];
    for (let i = 0; i < lotes.length; i += 4) {
      const tanda = await Promise.all(lotes.slice(i, i+4).map(ndviDe));
      resultados.push(...tanda);
    }
    resultados.sort((a, b) => (b.ndvi ?? -1) - (a.ndvi ?? -1));
    res.json({ ranking: resultados, dias });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── CAMPOS (multi-campo) ──────────────────────────────────────────────────────
app.get("/api/campos", (req, res) => {
  const rows = db.prepare("SELECT * FROM campos WHERE activo = 1 ORDER BY orden, nombre").all();
  res.json(rows);
});

app.post("/api/campos", (req, res) => {
  const nombre = (req.body.nombre || '').trim().toUpperCase();
  if (!nombre) return res.status(400).json({ error: "Falta el nombre del campo" });
  try {
    const r = db.prepare("INSERT INTO campos (nombre, orden) VALUES (?, (SELECT COALESCE(MAX(orden),0)+1 FROM campos))").run(nombre);
    res.json({ id: r.lastInsertRowid, nombre });
  } catch (e) {
    if (String(e.message).includes('UNIQUE')) return res.status(400).json({ error: 'Ya existe un campo con ese nombre' });
    res.status(400).json({ error: e.message });
  }
});

// ── DIARIO DE CAMPO (lluvias + acontecimientos) ───────────────────────────────
app.get("/api/diario", (req, res) => {
  const { campo, anio } = req.query;
  let q = "SELECT * FROM diario_campo WHERE 1=1";
  const p = [];
  if (campo) { q += " AND campo = ?"; p.push(campo); }
  if (anio) { q += " AND fecha LIKE ?"; p.push(`${anio}-%`); }
  q += " ORDER BY fecha DESC, id DESC";
  const registros = db.prepare(q).all(...p);

  // Resumen de lluvias por mes (del año/campo pedido, o de todo)
  let ql = "SELECT substr(fecha,1,7) mes, COALESCE(SUM(mm),0) mm, COUNT(*) dias FROM diario_campo WHERE tipo='LLUVIA'";
  const pl = [];
  if (campo) { ql += " AND campo = ?"; pl.push(campo); }
  if (anio) { ql += " AND fecha LIKE ?"; pl.push(`${anio}-%`); }
  ql += " GROUP BY mes ORDER BY mes";
  const lluviasPorMes = db.prepare(ql).all(...pl);
  const totalMm = lluviasPorMes.reduce((s, m) => s + (m.mm || 0), 0);

  res.json({ registros, lluvias_por_mes: lluviasPorMes, total_mm: Math.round(totalMm * 10) / 10 });
});

app.post("/api/diario", (req, res) => {
  const { campo, fecha, tipo, mm, titulo, detalle } = req.body;
  if (!fecha) return res.status(400).json({ error: "Falta la fecha" });
  const t = (tipo || (mm != null ? 'LLUVIA' : 'ACONTECIMIENTO')).toUpperCase();
  const r = db.prepare("INSERT INTO diario_campo (campo, fecha, tipo, mm, titulo, detalle) VALUES (?, ?, ?, ?, ?, ?)")
    .run((campo || 'LA AMISTAD').toUpperCase(), fecha, t, (mm != null && mm !== '') ? parseFloat(mm) : null, titulo || '', detalle || '');
  res.json({ id: r.lastInsertRowid, ok: true });
});

app.delete("/api/diario/:id", (req, res) => {
  db.prepare("DELETE FROM diario_campo WHERE id = ?").run(req.params.id);
  res.json({ ok: true });
});

// \u2500\u2500 TAREAS / PENDIENTES DE CAMPO \u2500\u2500
app.get("/api/tareas", (req, res) => {
  let q="SELECT * FROM tareas_campo WHERE 1=1"; const p=[];
  if(req.query.campo){ q+=" AND campo=?"; p.push(req.query.campo); }
  if(req.query.estado){ q+=" AND estado=?"; p.push(req.query.estado); }
  q+=" ORDER BY (estado='HECHO'), COALESCE(done_at, created_at) DESC";
  try { res.json(db.prepare(q).all(...p)); } catch(e){ res.json([]); }
});
app.post("/api/tareas", (req, res) => {
  const { texto, campo, origen } = req.body || {};
  if(!texto || !String(texto).trim()) return res.status(400).json({ error: "Falta el texto" });
  const c = campo || campoDefaultTareas();
  const r = db.prepare("INSERT INTO tareas_campo (campo, texto, estado, origen) VALUES (?,?, 'PENDIENTE', ?)").run(c, String(texto).trim(), origen||'web');
  res.json({ ok:true, id:r.lastInsertRowid });
});
app.put("/api/tareas/:id", (req, res) => {
  const { estado, texto, campo } = req.body || {};
  db.prepare(`UPDATE tareas_campo SET estado=COALESCE(?,estado), texto=COALESCE(?,texto), campo=COALESCE(?,campo), done_at=CASE WHEN ?='HECHO' THEN datetime('now') WHEN ?='PENDIENTE' THEN NULL ELSE done_at END WHERE id=?`)
    .run(estado||null, texto||null, campo||null, estado||'', estado||'', req.params.id);
  res.json({ ok:true });
});
app.delete("/api/tareas/:id", (req, res) => {
  db.prepare("DELETE FROM tareas_campo WHERE id=?").run(req.params.id);
  res.json({ ok:true });
});

// ── STOCK GANADERO (hacienda valuada en kg de carne × precio IGU) ─────────────
function getIGU() {
  const row = db.prepare("SELECT valor, updated_at FROM config WHERE clave = 'precio_igu'").get();
  return { precio: row ? parseFloat(row.valor) || 0 : 0, actualizado: row ? row.updated_at : null };
}

// ── PATRIMONIO: caja + fondo reposición + ganado + bienes + stock ─────────────
function getPatrimonioActual() {
  // Caja: saldo real acumulado, SOLO desde el ciclo 25/26 (lo anterior es histórico
  // incompleto cargado por archivo, sin ingresos → distorsiona la caja).
  const desde = parseCiclo('25/26', 'productivo').fecha_desde;   // 2025-03-01
  const c = db.prepare("SELECT COALESCE(SUM(ingreso),0) ing, COALESCE(SUM(egreso),0) egr FROM transacciones WHERE fecha >= ?").get(desde);
  const caja = Math.round((c.ing - c.egr) * 100) / 100;
  // Fondo de reposición: amortizaciones que salieron del flujo pero están resguardadas
  const f = db.prepare("SELECT COALESCE(SUM(monto),0) f FROM amortizaciones").get();
  const fondo = Math.round((f.f || 0) * 100) / 100;
  // Ganado: plantel + venta (ambos son hacienda que vale). kg × IGU, o $/cabeza si no hay kg.
  const igu = getIGU().precio;
  const ganado = Math.round(db.prepare("SELECT * FROM stock_ganadero").all().reduce((s, r) => {
    const kg = r.kg_estimado || 0;
    const cabezas = (r.cantidad || 0) + (r.cantidad_venta || 0);
    const v = (kg > 0 && igu > 0) ? cabezas * kg * igu : cabezas * (r.valor_cabeza || 0);
    return s + v;
  }, 0) * 100) / 100;
  // Bienes: valor actual (depreciado)
  const bienes = Math.round(db.prepare("SELECT * FROM bienes_muebles WHERE activo = 1").all().map(calcBien).reduce((s, b) => s + (b.valor_actual || 0), 0) * 100) / 100;
  // Stock de insumos: valorizado
  const stock = Math.round(getStockValorizado().reduce((s, p) => s + (p.valor || 0), 0) * 100) / 100;

  // ── PASIVOS (lo que debemos) ──
  const chP = db.prepare("SELECT COALESCE(SUM(monto),0) m FROM cheques WHERE estado='PENDIENTE' AND tipo='EMITIDO'").get();
  const deuda_cheques = Math.round((chP.m || 0) * 100) / 100;
  const deuda_cc = Math.round(getResumenCuentasCorrientes().reduce((s, c) => s + Math.max(parseFloat(c.saldo) || 0, 0), 0) * 100) / 100;
  const deuda_inversores = Math.round(getInversoresActivos().reduce((s, i) => s + calcularDeudaInversor(i), 0) * 100) / 100;

  const activos = Math.round((caja + fondo + ganado + bienes + stock) * 100) / 100;
  const pasivos = Math.round((deuda_cheques + deuda_cc + deuda_inversores) * 100) / 100;
  const total = Math.round((activos - pasivos) * 100) / 100;
  return { caja, fondo, ganado, bienes, stock, deuda_cheques, deuda_cc, deuda_inversores, activos, pasivos, total };
}

app.get("/api/patrimonio", (req, res) => {
  const actual = getPatrimonioActual();
  const historial = db.prepare("SELECT * FROM patrimonio_snapshots ORDER BY fecha, ciclo").all();
  res.json({ actual, historial });
});

// Capturar una foto del patrimonio actual para un ciclo (reemplaza si ya existe ese ciclo)
app.post("/api/patrimonio/snapshot", (req, res) => {
  const p = getPatrimonioActual();
  const ciclo = req.body.ciclo || getCicloActual('productivo').ciclo;
  const cicloObj = parseCiclo(ciclo, 'productivo');
  const hoy = new Date().toISOString().slice(0, 10);
  let fecha = (cicloObj && cicloObj.fecha_hasta < hoy) ? cicloObj.fecha_hasta : hoy;
  if (fecha.endsWith('-02-29')) fecha = fecha.replace('-02-29', '-02-28');
  db.prepare(`INSERT INTO patrimonio_snapshots (ciclo, fecha, caja, ganado, bienes, stock, total, fondo, deuda_cheques, deuda_cc, deuda_inversores, notas)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(ciclo) DO UPDATE SET fecha=excluded.fecha, caja=excluded.caja, ganado=excluded.ganado,
      bienes=excluded.bienes, stock=excluded.stock, total=excluded.total, fondo=excluded.fondo,
      deuda_cheques=excluded.deuda_cheques, deuda_cc=excluded.deuda_cc, deuda_inversores=excluded.deuda_inversores, created_at=datetime('now')`)
    .run(ciclo, fecha, p.caja, p.ganado, p.bienes, p.stock, p.total, p.fondo, p.deuda_cheques, p.deuda_cc, p.deuda_inversores, req.body.notas || '');
  res.json({ ok: true, ciclo, fecha, ...p });
});

app.delete("/api/patrimonio/snapshot/:id", (req, res) => {
  db.prepare("DELETE FROM patrimonio_snapshots WHERE id = ?").run(req.params.id);
  res.json({ ok: true });
});

app.get("/api/ganado", (req, res) => {
  const campo = req.query.campo;
  const igu = getIGU();
  const rows = campo
    ? db.prepare("SELECT * FROM stock_ganadero WHERE campo = ? ORDER BY registro, categoria, id").all(campo)
    : db.prepare("SELECT * FROM stock_ganadero ORDER BY registro, categoria, id").all();
  const conValor = rows.map(r => {
    const kg = r.kg_estimado || 0;
    const usaIgu = kg > 0 && igu.precio > 0;
    const valPl = usaIgu ? Math.round((r.cantidad || 0) * kg * igu.precio * 100) / 100 : Math.round((r.cantidad || 0) * (r.valor_cabeza || 0) * 100) / 100;
    const valVt = usaIgu ? Math.round((r.cantidad_venta || 0) * kg * igu.precio * 100) / 100 : Math.round((r.cantidad_venta || 0) * (r.valor_cabeza || 0) * 100) / 100;
    return {
      ...r,
      cantidad_venta: r.cantidad_venta || 0,
      valor_plantel: valPl,
      valor_venta: valVt,
      valor: valPl + valVt,
      valuado_por: usaIgu ? 'igu' : 'cabeza'
    };
  });
  res.json({ igu: igu.precio, igu_actualizado: igu.actualizado, items: conValor });
});

app.post("/api/ganado/igu", (req, res) => {
  const precio = parseFloat(req.body.precio);
  if (!precio || precio <= 0) return res.status(400).json({ error: "Precio IGU inválido" });
  db.prepare(`INSERT INTO config (clave, valor, updated_at) VALUES ('precio_igu', ?, datetime('now'))
    ON CONFLICT(clave) DO UPDATE SET valor = excluded.valor, updated_at = datetime('now')`).run(String(precio));
  res.json({ ok: true, precio });
});

app.post("/api/ganado", (req, res) => {
  const { campo, categoria, registro, cantidad, cantidad_venta, kg_estimado, valor_cabeza, orden, notas } = req.body;
  if (!categoria) return res.status(400).json({ error: "Falta la categoría" });
  const r = db.prepare(`INSERT INTO stock_ganadero (campo, categoria, registro, cantidad, cantidad_venta, kg_estimado, valor_cabeza, orden, notas)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(campo || 'LA AMISTAD', categoria, (registro || 'GENERAL').toUpperCase(),
      parseFloat(cantidad) || 0, parseFloat(cantidad_venta) || 0, parseFloat(kg_estimado) || 0, parseFloat(valor_cabeza) || 0, parseInt(orden) || 0, notas || '');
  res.json({ id: r.lastInsertRowid });
});

app.put("/api/ganado/:id", (req, res) => {
  const g = db.prepare("SELECT * FROM stock_ganadero WHERE id = ?").get(req.params.id);
  if (!g) return res.status(404).json({ error: "No encontrado" });
  const { categoria, registro, cantidad, cantidad_venta, kg_estimado, valor_cabeza, orden, notas } = req.body;
  db.prepare(`UPDATE stock_ganadero SET categoria=?, registro=?, cantidad=?, cantidad_venta=?, kg_estimado=?, valor_cabeza=?, orden=?, notas=?, updated_at=datetime('now') WHERE id=?`)
    .run(categoria ?? g.categoria, (registro ?? g.registro ?? 'GENERAL').toUpperCase(),
      cantidad !== undefined ? parseFloat(cantidad) : g.cantidad,
      cantidad_venta !== undefined ? parseFloat(cantidad_venta) : g.cantidad_venta,
      kg_estimado !== undefined ? parseFloat(kg_estimado) : g.kg_estimado,
      valor_cabeza !== undefined ? parseFloat(valor_cabeza) : g.valor_cabeza,
      orden !== undefined ? parseInt(orden) : g.orden, notas ?? g.notas, req.params.id);
  res.json({ ok: true });
});

app.delete("/api/ganado/:id", (req, res) => {
  db.prepare("DELETE FROM stock_ganadero WHERE id = ?").run(req.params.id);
  res.json({ ok: true });
});

// Sincronizar cantidades del rodeo desde Angus del Este (server → server)
app.post("/api/ganado/sync-ade", async (req, res) => {
  const campoAde = req.body.campo_ade || 'angus_del_este';
  try {
    const resp = await fetch(`${ADE_URL}/api/rodeo-resumen?campo=${encodeURIComponent(campoAde)}`);
    if (!resp.ok) return res.status(502).json({ error: `ADE respondió ${resp.status}. Verificá que el endpoint /api/rodeo-resumen exista y sea público.` });
    const data = await resp.json();
    // Tolerante al formato: array directo o envuelto en rodeo/categorias/data
    const lista = Array.isArray(data) ? data : (data.rodeo || data.categorias || data.data || []);
    if (!Array.isArray(lista) || !lista.length) {
      return res.json({ ok: true, actualizados: 0, creados: 0, mensaje: 'ADE no devolvió categorías', crudo: data });
    }
    const getByCatReg = db.prepare("SELECT * FROM stock_ganadero WHERE LOWER(categoria) = LOWER(?) AND LOWER(COALESCE(registro,'GENERAL')) = LOWER(?)");
    const upd = db.prepare("UPDATE stock_ganadero SET cantidad = ?, cantidad_venta = ?, origen = 'ade', updated_at = datetime('now') WHERE id = ?");
    const ins = db.prepare("INSERT INTO stock_ganadero (campo, categoria, registro, cantidad, cantidad_venta, kg_estimado, valor_cabeza, origen) VALUES ('LA AMISTAD', ?, ?, ?, ?, 0, 0, 'ade')");
    let actualizados = 0, creados = 0, totalCab = 0;
    for (const it of lista) {
      const cat = it.categoria || it.category || it.nombre || it.cat;
      const reg = (it.registro || it.pedigree || 'GENERAL').toUpperCase();
      const plantel = parseFloat(it.plantel ?? it.cantidad ?? it.count ?? 0) || 0;
      const venta = parseFloat(it.venta ?? 0) || 0;
      if (!cat) continue;
      totalCab += plantel + venta;
      const ex = getByCatReg.get(cat, reg);
      if (ex) { upd.run(plantel, venta, ex.id); actualizados++; }
      else { ins.run(cat, reg, plantel, venta); creados++; }
    }
    res.json({ ok: true, actualizados, creados, total_cabezas: totalCab, categorias: lista.length });
  } catch (e) {
    res.status(502).json({ error: 'No pude conectar con ADE: ' + String(e.message).slice(0, 150) });
  }
});

// ── API INFORME CICLO ─────────────────────────────────────────────────────────
app.get("/api/informe-ciclo", (req, res) => {
  const cicloStr = req.query.ciclo || getCicloActual().ciclo;
  const informe = getInformeCiclo(cicloStr);
  if (!informe) return res.status(400).json({ error: "Ciclo inválido" });
  res.json({
    ciclo: informe.ciclo.label,
    fecha_desde: informe.ciclo.fecha_desde,
    fecha_hasta: informe.fechaHasta,
    total_egresos: informe.totalEgresos,
    total_ingresos: informe.totalIngresos,
    total_movimientos: informe.totalMovimientos,
    categorias: informe.rows,
    presupuestos: informe.presupuestoMap
  });
});

// ── API INFORME MENSUAL ───────────────────────────────────────────────────────
app.get("/api/informe-mensual", (req, res) => {
  const anio = parseInt(req.query.anio) || new Date().getFullYear();
  const mes = parseInt(req.query.mes) || new Date().getMonth() + 1;
  const informe = getInformeMensual(anio, mes);
  res.json({
    periodo: informe.periodo,
    total_egresos: informe.totalEgresos,
    total_ingresos: informe.totalIngresos,
    categorias: informe.rows,
    presupuestos_mensualizados: informe.presupuestoMap
  });
});

// ── ENVIAR INFORME MANUAL (para testing) ──────────────────────────────────────
app.post("/api/enviar-informe", async (req, res) => {
  const anio = parseInt(req.body.anio) || new Date().getFullYear();
  const mes = parseInt(req.body.mes) || new Date().getMonth();
  if (mes < 1 || mes > 12) return res.status(400).json({ error: "Mes inválido" });
  await enviarInformeMensualWhatsApp(anio, mes);
  res.json({ ok: true, mensaje: `Informe ${mes}/${anio} enviado` });
});

// ── INFORME PDF CICLO ANUAL ───────────────────────────────────────────────────
app.get("/api/informe-pdf", async (req, res) => {
  try {
    const tipo = (req.query.tipo === 'contable') ? 'contable' : 'productivo';
    const cicloStr = req.query.ciclo || getCicloActual(tipo).ciclo;
    const informe = getInformeCiclo(cicloStr, tipo);
    if (!informe) return res.status(400).json({ error: "Ciclo inválido" });

    // ¿Es el ciclo en curso? (define si mostramos cuentas/cheques/inversores — son estado ACTUAL)
    const esActual = informe.ciclo.label === getCicloActual(tipo).label;

    const tc = await getTipoCambio();
    const cuentas = esActual ? getResumenCuentasCorrientes() : [];
    const cheques = esActual ? getChequesPendientes() : [];
    const inversores = esActual ? getInversoresActivos() : [];
    const totalDeudaInv = inversores.reduce((s, i) => s + calcularDeudaInversor(i), 0);

    // Desglose mensual del ciclo (arranca en el mes de inicio del ciclo: mar productivo / jul contable)
    const mesesNombres = ["","Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"];
    const desgloseMensual = [];
    const cicloData = informe.ciclo;
    const anioInicio = parseInt(cicloData.fecha_desde.slice(0, 4));
    const mesInicio = cicloData.mesInicio || 3;
    for (let m = mesInicio; m <= mesInicio + 11; m++) {
      const mesReal = ((m - 1) % 12) + 1;
      const anioReal = anioInicio + Math.floor((m - 1) / 12);
      const periodo = `${anioReal}-${String(mesReal).padStart(2, '0')}`;
      const hoy = new Date().toISOString().slice(0, 7);
      if (periodo > hoy) break;

      const row = db.prepare(`
        SELECT SUM(egreso) as egresos, SUM(ingreso) as ingresos, COUNT(*) as cant
        FROM transacciones WHERE fecha LIKE ?
      `).get(`${periodo}-%`);

      desgloseMensual.push({
        mes: `${mesesNombres[mesReal]} ${anioReal}`,
        egresos: row?.egresos || 0,
        ingresos: row?.ingresos || 0,
        cant: row?.cant || 0
      });
    }

    // ── Generar PDF ──
    const doc = new PDFDocument({ size: 'A4', margin: 50, bufferPages: true });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="IMPROLUX_Ciclo_${cicloStr.replace('/', '-')}.pdf"`);
    doc.pipe(res);

    // Paleta del sistema (azul / dorado / paper)
    const colorVerde = '#0B3D7C';        // "brand" — ahora AZUL (se usa en headers y barras)
    const colorAzulProfundo = '#0F2847'; // banner (como el sidebar)
    const colorGold = '#C9A24B';         // acento dorado
    const colorGris = '#5C6B7E';
    const colorNegro = '#10243F';
    const colorFondo = '#F4F1EA';        // paper (filas alternas)
    const colorLinea = '#D8D0C0';

    // ── ENCABEZADO ──
    doc.rect(0, 0, doc.page.width, 100).fill(colorAzulProfundo);
    doc.rect(0, 100, doc.page.width, 3).fill(colorGold);   // línea dorada (como el subrayado activo)
    doc.fontSize(28).fill('#ffffff').font('Helvetica-Bold')
       .text('IMPROLUX', 50, 30);
    doc.fontSize(13).fill('#e8dcc0').font('Times-Italic')
       .text(`Informe de Ciclo ${informe.ciclo.tipo === 'contable' ? 'Contable' : 'Productivo'} ${informe.ciclo.label}`, 50, 62);
    doc.fontSize(9).fill('#a9bdd6').font('Helvetica')
       .text(`Generado: ${new Date().toLocaleDateString('es-UY')} | ${informe.ciclo.tipo === 'contable' ? 'Contable (jul→jun)' : 'Productivo (mar→feb)'} | Período: ${informe.ciclo.fecha_desde} → ${informe.fechaHasta}`, 50, 82);

    let y = 120;

    // ── RESUMEN EJECUTIVO ──
    doc.fontSize(15).fill(colorVerde).font('Times-Italic')
       .text('Resumen Ejecutivo', 50, y);
    y += 25;

    // Cajas de KPI
    const kpis = [
      { label: 'Total Egresos', valor: `$${fmt(informe.totalEgresos)} USD`, color: '#B83232' },
      { label: 'Total Ingresos', valor: `$${fmt(informe.totalIngresos)} USD`, color: '#1a7a4a' },
      { label: 'Resultado Neto', valor: `$${fmt(informe.totalIngresos - informe.totalEgresos)} USD`, color: (informe.totalIngresos - informe.totalEgresos) >= 0 ? '#1a7a4a' : '#B83232' },
      { label: 'Movimientos', valor: `${informe.totalMovimientos}`, color: colorVerde }
    ];

    const kpiWidth = 120;
    const kpiGap = 10;
    kpis.forEach((kpi, i) => {
      const x = 50 + i * (kpiWidth + kpiGap);
      doc.rect(x, y, kpiWidth, 55).fill(colorFondo).stroke(colorLinea);
      doc.fontSize(8).fill(colorGris).font('Helvetica').text(kpi.label, x + 8, y + 8, { width: kpiWidth - 16 });
      doc.fontSize(14).fill(kpi.color).font('Times-Italic').text(kpi.valor, x + 8, y + 24, { width: kpiWidth - 16 });
    });
    y += 75;

    if (tc) {
      doc.fontSize(8).fill(colorGris).font('Helvetica')
         .text(`TC BROU: $${tc.toFixed(2)} UYU/USD`, 50, y);
      y += 18;
    }

    // ── DESGLOSE POR CATEGORÍA ──
    doc.fontSize(15).fill(colorVerde).font('Times-Italic')
       .text('Desglose por Categoría', 50, y);
    y += 22;

    // Header tabla
    doc.rect(50, y, 495, 18).fill(colorVerde);
    doc.fontSize(8).fill('#ffffff').font('Helvetica-Bold');
    doc.text('Categoría', 55, y + 5, { width: 180 });
    doc.text('Egreso', 240, y + 5, { width: 80, align: 'right' });
    doc.text('Ingreso', 325, y + 5, { width: 80, align: 'right' });
    doc.text('Presup.', 410, y + 5, { width: 65, align: 'right' });
    doc.text('% Ejec.', 480, y + 5, { width: 60, align: 'right' });
    y += 18;

    informe.rows.forEach((r, i) => {
      if (y > 720) { doc.addPage(); y = 50; }
      const bg = i % 2 === 0 ? '#ffffff' : colorFondo;
      doc.rect(50, y, 495, 16).fill(bg);

      const presup = informe.presupuestoMap[r.concepto];
      const pct = presup ? ((r.total_egreso / presup) * 100).toFixed(0) + '%' : '-';
      const excedido = presup && r.total_egreso > presup;

      doc.fontSize(8).fill(colorNegro).font('Helvetica');
      doc.text(r.concepto, 55, y + 4, { width: 180 });
      doc.fill(r.total_egreso > 0 ? '#B83232' : colorGris)
         .text(r.total_egreso > 0 ? `$${fmt(r.total_egreso)}` : '-', 240, y + 4, { width: 80, align: 'right' });
      doc.fill(r.total_ingreso > 0 ? '#1a7a4a' : colorGris)
         .text(r.total_ingreso > 0 ? `$${fmt(r.total_ingreso)}` : '-', 325, y + 4, { width: 80, align: 'right' });
      doc.fill(colorGris)
         .text(presup ? `$${fmt(presup)}` : '-', 410, y + 4, { width: 65, align: 'right' });
      doc.fill(excedido ? '#B83232' : colorNegro).font(excedido ? 'Helvetica-Bold' : 'Helvetica')
         .text(pct + (excedido ? ' ⚠' : ''), 480, y + 4, { width: 60, align: 'right' });
      y += 16;
    });

    // Totales
    doc.rect(50, y, 495, 18).fill(colorVerde);
    doc.fontSize(8).fill('#ffffff').font('Helvetica-Bold');
    doc.text('TOTAL', 55, y + 5, { width: 180 });
    doc.text(`$${fmt(informe.totalEgresos)}`, 240, y + 5, { width: 80, align: 'right' });
    doc.text(`$${fmt(informe.totalIngresos)}`, 325, y + 5, { width: 80, align: 'right' });
    const totalPresup = Object.values(informe.presupuestoMap).reduce((s, v) => s + v, 0);
    doc.text(totalPresup > 0 ? `$${fmt(totalPresup)}` : '-', 410, y + 5, { width: 65, align: 'right' });
    doc.text(totalPresup > 0 ? `${((informe.totalEgresos / totalPresup) * 100).toFixed(0)}%` : '-', 480, y + 5, { width: 60, align: 'right' });
    y += 35;

    // ── DESGLOSE MENSUAL ──
    if (y > 620) { doc.addPage(); y = 50; }
    doc.fontSize(15).fill(colorVerde).font('Times-Italic')
       .text('Evolución Mensual', 50, y);
    y += 22;

    doc.rect(50, y, 495, 18).fill(colorVerde);
    doc.fontSize(8).fill('#ffffff').font('Helvetica-Bold');
    doc.text('Mes', 55, y + 5, { width: 100 });
    doc.text('Egresos', 160, y + 5, { width: 100, align: 'right' });
    doc.text('Ingresos', 265, y + 5, { width: 100, align: 'right' });
    doc.text('Neto', 370, y + 5, { width: 100, align: 'right' });
    doc.text('Mov.', 475, y + 5, { width: 65, align: 'right' });
    y += 18;

    desgloseMensual.forEach((m, i) => {
      if (y > 720) { doc.addPage(); y = 50; }
      const bg = i % 2 === 0 ? '#ffffff' : colorFondo;
      doc.rect(50, y, 495, 16).fill(bg);
      const neto = m.ingresos - m.egresos;
      doc.fontSize(8).fill(colorNegro).font('Helvetica');
      doc.text(m.mes, 55, y + 4, { width: 100 });
      doc.fill('#B83232').text(`$${fmt(m.egresos)}`, 160, y + 4, { width: 100, align: 'right' });
      doc.fill('#1a7a4a').text(`$${fmt(m.ingresos)}`, 265, y + 4, { width: 100, align: 'right' });
      doc.fill(neto >= 0 ? '#1a7a4a' : '#B83232').font('Helvetica-Bold')
         .text(`$${fmt(neto)}`, 370, y + 4, { width: 100, align: 'right' });
      doc.fill(colorGris).font('Helvetica').text(`${m.cant}`, 475, y + 4, { width: 65, align: 'right' });
      y += 16;
    });
    y += 20;

    // ── Nota: en ciclos pasados omitimos cuentas/cheques/inversores (son estado actual, no del cierre) ──
    if (!esActual) {
      if (y > 700) { doc.addPage(); y = 50; }
      doc.fontSize(8).fill(colorGris).font('Helvetica-Oblique')
         .text('Nota: cuentas corrientes, cheques pendientes e inversores se muestran solo en el informe del ciclo en curso, ya que reflejan el estado de hoy y no el del cierre de este ciclo.', 50, y, { width: 495 });
      y += 22;
    }

    // ── CUENTAS CORRIENTES ──
    if (cuentas.length > 0) {
      if (y > 620) { doc.addPage(); y = 50; }
      doc.fontSize(15).fill(colorVerde).font('Times-Italic')
         .text('Cuentas Corrientes', 50, y);
      y += 22;

      doc.rect(50, y, 300, 18).fill(colorVerde);
      doc.fontSize(8).fill('#ffffff').font('Helvetica-Bold');
      doc.text('Proveedor', 55, y + 5, { width: 160 });
      doc.text('Saldo', 220, y + 5, { width: 120, align: 'right' });
      y += 18;

      cuentas.forEach((c, i) => {
        const bg = i % 2 === 0 ? '#ffffff' : colorFondo;
        doc.rect(50, y, 300, 16).fill(bg);
        doc.fontSize(8).fill(colorNegro).font('Helvetica').text(c.proveedor, 55, y + 4, { width: 160 });
        doc.fill(c.saldo > 0 ? '#B83232' : '#1a7a4a').font('Helvetica-Bold')
           .text(`$${fmt(Math.abs(c.saldo))} ${c.saldo > 0 ? '(debemos)' : '(a favor)'}`, 220, y + 4, { width: 120, align: 'right' });
        y += 16;
      });
      const totalCC = cuentas.reduce((s, c) => s + c.saldo, 0);
      doc.fontSize(9).fill(colorNegro).font('Helvetica-Bold')
         .text(`Total adeudado: $${fmt(totalCC)} USD`, 50, y + 5);
      y += 25;
    }

    // ── CHEQUES PENDIENTES ──
    if (cheques.length > 0) {
      if (y > 650) { doc.addPage(); y = 50; }
      doc.fontSize(15).fill(colorVerde).font('Times-Italic')
         .text('Cheques Pendientes', 50, y);
      y += 22;

      cheques.forEach((c, i) => {
        if (y > 720) { doc.addPage(); y = 50; }
        doc.fontSize(8).fill(colorNegro).font('Helvetica');
        const emoji = c.tipo === "EMITIDO" ? "→" : "←";
        doc.text(`${emoji} ${c.tipo} | ${c.proveedor || 'Sin prov.'} | $${fmt(c.monto)} USD | vence: ${c.fecha_cobro || 'sin fecha'}`, 55, y);
        y += 14;
      });
      const totalCheq = cheques.reduce((s, c) => s + c.monto, 0);
      doc.fontSize(9).fill(colorNegro).font('Helvetica-Bold')
         .text(`Total cheques pendientes: $${fmt(totalCheq)} USD`, 50, y + 3);
      y += 25;
    }

    // ── INVERSORES ──
    if (inversores.length > 0) {
      if (y > 620) { doc.addPage(); y = 50; }
      doc.fontSize(15).fill(colorVerde).font('Times-Italic')
         .text('Inversores Activos', 50, y);
      y += 22;

      doc.rect(50, y, 450, 18).fill(colorVerde);
      doc.fontSize(8).fill('#ffffff').font('Helvetica-Bold');
      doc.text('Inversor', 55, y + 5, { width: 120 });
      doc.text('Capital', 180, y + 5, { width: 90, align: 'right' });
      doc.text('Tasa', 275, y + 5, { width: 50, align: 'right' });
      doc.text('Ingreso', 330, y + 5, { width: 80, align: 'right' });
      doc.text('Deuda Actual', 415, y + 5, { width: 80, align: 'right' });
      y += 18;

      inversores.forEach((inv, i) => {
        const bg = i % 2 === 0 ? '#ffffff' : colorFondo;
        doc.rect(50, y, 450, 16).fill(bg);
        const deuda = calcularDeudaInversor(inv);
        doc.fontSize(8).fill(colorNegro).font('Helvetica');
        doc.text(inv.inversor, 55, y + 4, { width: 120 });
        doc.text(`$${fmt(inv.capital)}`, 180, y + 4, { width: 90, align: 'right' });
        doc.text(`${(inv.tasa * 100).toFixed(1)}%`, 275, y + 4, { width: 50, align: 'right' });
        doc.text(inv.fecha_ingreso, 330, y + 4, { width: 80, align: 'right' });
        doc.fill('#B83232').font('Helvetica-Bold')
           .text(`$${fmt(deuda)}`, 415, y + 4, { width: 80, align: 'right' });
        y += 16;
      });
      doc.fontSize(9).fill(colorNegro).font('Helvetica-Bold')
         .text(`Deuda total inversores: $${fmt(totalDeudaInv)} USD`, 50, y + 5);
    }

    // ── GASTOS POR CATEGORÍA (MES A MES) — página apaisada ──
    {
      const mesesAbr = ["","Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"];
      const anioIni = parseInt(informe.ciclo.fecha_desde.slice(0, 4));
      const mIni = informe.ciclo.mesInicio || 3;
      const hoyMes = new Date().toISOString().slice(0, 7);
      const mesesCols = [];
      for (let m = mIni; m <= mIni + 11; m++) {
        const mesReal = ((m - 1) % 12) + 1;
        const anioReal = anioIni + Math.floor((m - 1) / 12);
        const periodo = `${anioReal}-${String(mesReal).padStart(2, '0')}`;
        if (periodo > hoyMes) break;
        mesesCols.push({ periodo, label: `${mesesAbr[mesReal]}${String(anioReal).slice(2)}` });
      }
      const matrizRows = db.prepare(`
        SELECT concepto, substr(fecha,1,7) as periodo, SUM(egreso) as egreso
        FROM transacciones
        WHERE fecha >= ? AND fecha <= ? AND egreso > 0
        GROUP BY concepto, periodo
      `).all(informe.ciclo.fecha_desde, informe.fechaHasta);
      const matriz = {};
      matrizRows.forEach(r => { (matriz[r.concepto] = matriz[r.concepto] || {})[r.periodo] = r.egreso; });
      const cats = informe.rows.filter(r => r.total_egreso > 0).map(r => r.concepto);
      const fmtInt = n => n ? Math.round(n).toLocaleString('es-UY') : '';

      if (cats.length && mesesCols.length) {
        doc.addPage({ size: 'A4', layout: 'landscape', margin: 40 });
        const L = 40, catW = 128, totW = 58;
        const monthsX = L + catW;
        const totX = doc.page.width - L - totW;
        const colW = (totX - monthsX) / mesesCols.length;

        let ly = 45;
        doc.fontSize(15).fill(colorVerde).font('Times-Italic')
           .text('Gastos por Categoría — mes a mes (USD)', L, ly);
        ly += 22;

        const drawHeader = () => {
          doc.rect(L, ly, doc.page.width - 2 * L, 16).fill(colorVerde);
          doc.fontSize(6.5).fill('#ffffff').font('Helvetica-Bold');
          doc.text('Categoría', L + 4, ly + 5, { width: catW - 6, lineBreak: false });
          mesesCols.forEach((mc, i) => doc.text(mc.label, monthsX + i * colW, ly + 5, { width: colW - 2, align: 'right', lineBreak: false }));
          doc.text('Total', totX, ly + 5, { width: totW - 4, align: 'right', lineBreak: false });
          ly += 16;
        };
        drawHeader();

        cats.forEach((cat, idx) => {
          if (ly > 545) { doc.addPage({ size: 'A4', layout: 'landscape', margin: 40 }); ly = 45; drawHeader(); }
          const bg = idx % 2 === 0 ? '#ffffff' : colorFondo;
          doc.rect(L, ly, doc.page.width - 2 * L, 13).fill(bg);
          doc.fontSize(6.5).fill(colorNegro).font('Helvetica').text(cat, L + 4, ly + 3.5, { width: catW - 6, lineBreak: false });
          let rowTot = 0;
          doc.fontSize(6);
          mesesCols.forEach((mc, i) => {
            const v = (matriz[cat] && matriz[cat][mc.periodo]) || 0;
            rowTot += v;
            if (v > 0) doc.fill('#B83232').text(fmtInt(v), monthsX + i * colW, ly + 4, { width: colW - 2, align: 'right', lineBreak: false });
          });
          doc.fill(colorNegro).font('Helvetica-Bold').text(fmtInt(rowTot), totX, ly + 4, { width: totW - 4, align: 'right', lineBreak: false });
          doc.font('Helvetica');
          ly += 13;
        });

        // Ingresos por mes (para las filas resumen)
        const ingMes = {};
        db.prepare(`SELECT substr(fecha,1,7) as periodo, SUM(ingreso) as ing FROM transacciones WHERE fecha >= ? AND fecha <= ? AND ingreso > 0 GROUP BY periodo`)
          .all(informe.ciclo.fecha_desde, informe.fechaHasta)
          .forEach(r => { ingMes[r.periodo] = r.ing; });

        // Filas resumen: Egresos, Ingresos, Neto (por mes)
        const drawResumen = (label, valorFn, granTotFn, opts) => {
          if (ly > 545) { doc.addPage({ size: 'A4', layout: 'landscape', margin: 40 }); ly = 45; drawHeader(); }
          doc.rect(L, ly, doc.page.width - 2 * L, 15).fill(opts.bg || colorVerde);
          doc.fontSize(6.5).fill(opts.fg || '#ffffff').font('Helvetica-Bold').text(label, L + 4, ly + 4.5, { width: catW - 6, lineBreak: false });
          doc.fontSize(6);
          let gt = 0;
          mesesCols.forEach((mc, i) => {
            const v = valorFn(mc.periodo);
            gt += v;
            const col = opts.signColor ? (v >= 0 ? '#1a7a4a' : '#B83232') : (opts.fg || '#ffffff');
            doc.fill(col).text(fmtInt(v), monthsX + i * colW, ly + 4.5, { width: colW - 2, align: 'right', lineBreak: false });
          });
          const gtVal = granTotFn ? granTotFn() : gt;
          doc.fill(opts.signColor ? (gtVal >= 0 ? '#1a7a4a' : '#B83232') : (opts.fg || '#ffffff'))
             .text(fmtInt(gtVal), totX, ly + 4.5, { width: totW - 4, align: 'right', lineBreak: false });
          ly += 15;
        };

        const egMesFn = p => cats.reduce((s, cat) => s + ((matriz[cat] && matriz[cat][p]) || 0), 0);
        drawResumen('TOTAL EGRESOS', egMesFn, () => informe.totalEgresos, { bg: colorVerde, fg: '#ffffff' });
        drawResumen('INGRESOS', p => ingMes[p] || 0, () => informe.totalIngresos, { bg: '#e8f3e8', fg: '#1a7a4a' });
        drawResumen('NETO', p => (ingMes[p] || 0) - egMesFn(p), () => informe.totalIngresos - informe.totalEgresos, { bg: '#f5f7f5', fg: colorNegro, signColor: true });
      }
    }

    // ── PIE DE PÁGINA ──
    const pages = doc.bufferedPageRange();
    for (let i = 0; i < pages.count; i++) {
      doc.switchToPage(i);
      doc.fontSize(7).fill(colorGris).font('Helvetica')
         .text(`IMPROLUX — Informe Ciclo ${informe.ciclo.label} — Página ${i + 1} de ${pages.count}`,
           50, doc.page.height - 30, { width: doc.page.width - 100, align: 'center' });
    }

    doc.end();
  } catch (err) {
    console.error("Error generando PDF:", err);
    res.status(500).json({ error: "Error generando el informe PDF" });
  }
});

// ── INFORME MENSUAL PDF ────────────────────────────────────────────────────────
app.get("/api/informe-mensual-pdf", async (req, res) => {
  try {
    const anio = parseInt(req.query.anio) || new Date().getFullYear();
    const mes = parseInt(req.query.mes) || new Date().getMonth() + 1;
    const mesesNombres = ["","Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"];
    const periodo = `${anio}-${String(mes).padStart(2, '0')}`;

    const informe = getInformeMensual(anio, mes);
    const tc = await getTipoCambio();

    const doc = new PDFDocument({ size: 'A4', margin: 50, bufferPages: true });
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="IMPROLUX_${mesesNombres[mes]}_${anio}.pdf"`);
    doc.pipe(res);

    const colorVerde = '#0B3D7C';
    const colorGris = '#5C6B7E';
    const colorNegro = '#10243F';
    const colorFondo = '#F4F1EA';
    const colorLinea = '#c8d6c8';

    // ── ENCABEZADO ──
    doc.rect(0, 0, doc.page.width, 100).fill(colorVerde);
    doc.fontSize(28).fill('#ffffff').font('Helvetica-Bold')
       .text('IMPROLUX', 50, 30);
    doc.fontSize(12).fill('#c8e6c8').font('Helvetica')
       .text(`Informe Mensual — ${mesesNombres[mes]} ${anio}`, 50, 62);
    doc.fontSize(9).fill('#a0c8a0')
       .text(`Generado: ${new Date().toLocaleDateString('es-UY')}${tc ? ` | TC BROU: $${tc.toFixed(2)} UYU/USD` : ''}`, 50, 80);

    let y = 120;

    // ── RESUMEN ──
    doc.fontSize(15).fill(colorVerde).font('Times-Italic')
       .text('Resumen del Mes', 50, y);
    y += 25;

    const neto = informe.totalIngresos - informe.totalEgresos;
    const kpis = [
      { label: 'Total Egresos', valor: `$${fmt(informe.totalEgresos)} USD`, color: '#B83232' },
      { label: 'Total Ingresos', valor: `$${fmt(informe.totalIngresos)} USD`, color: '#1a7a4a' },
      { label: 'Resultado Neto', valor: `$${fmt(neto)} USD`, color: neto >= 0 ? '#1a7a4a' : '#B83232' },
    ];

    const kpiWidth = 155;
    const kpiGap = 12;
    kpis.forEach((kpi, i) => {
      const x = 50 + i * (kpiWidth + kpiGap);
      doc.rect(x, y, kpiWidth, 55).fill(colorFondo).stroke(colorLinea);
      doc.fontSize(8).fill(colorGris).font('Helvetica').text(kpi.label, x + 10, y + 8, { width: kpiWidth - 20 });
      doc.fontSize(13).fill(kpi.color).font('Helvetica-Bold').text(kpi.valor, x + 10, y + 26, { width: kpiWidth - 20 });
    });
    y += 80;

    // ── DESGLOSE POR CATEGORÍA ──
    doc.fontSize(15).fill(colorVerde).font('Times-Italic')
       .text('Gastos por Categoría', 50, y);
    y += 22;

    // Header
    doc.rect(50, y, 495, 18).fill(colorVerde);
    doc.fontSize(8).fill('#ffffff').font('Helvetica-Bold');
    doc.text('Categoría', 55, y + 5, { width: 180 });
    doc.text('Egreso', 240, y + 5, { width: 80, align: 'right' });
    doc.text('Ingreso', 325, y + 5, { width: 80, align: 'right' });
    doc.text('Presup. Mes', 410, y + 5, { width: 65, align: 'right' });
    doc.text('% Ejec.', 480, y + 5, { width: 60, align: 'right' });
    y += 18;

    const categoriasConGasto = informe.rows.filter(r => r.total_egreso > 0 || r.total_ingreso > 0);
    categoriasConGasto.forEach((r, i) => {
      if (y > 720) { doc.addPage(); y = 50; }
      const bg = i % 2 === 0 ? '#ffffff' : colorFondo;
      doc.rect(50, y, 495, 16).fill(bg);

      const presupMes = informe.presupuestoMap[r.concepto];
      const pct = presupMes ? ((r.total_egreso / presupMes) * 100).toFixed(0) + '%' : '-';
      const excedido = presupMes && r.total_egreso > presupMes;

      doc.fontSize(8).fill(colorNegro).font('Helvetica');
      doc.text(r.concepto, 55, y + 4, { width: 180 });
      doc.fill(r.total_egreso > 0 ? '#B83232' : colorGris)
         .text(r.total_egreso > 0 ? `$${fmt(r.total_egreso)}` : '-', 240, y + 4, { width: 80, align: 'right' });
      doc.fill(r.total_ingreso > 0 ? '#1a7a4a' : colorGris)
         .text(r.total_ingreso > 0 ? `$${fmt(r.total_ingreso)}` : '-', 325, y + 4, { width: 80, align: 'right' });
      doc.fill(colorGris)
         .text(presupMes ? `$${fmt(presupMes)}` : '-', 410, y + 4, { width: 65, align: 'right' });
      doc.fill(excedido ? '#B83232' : colorNegro).font(excedido ? 'Helvetica-Bold' : 'Helvetica')
         .text(pct + (excedido ? ' ⚠' : ''), 480, y + 4, { width: 60, align: 'right' });
      y += 16;
    });

    // Totales
    doc.rect(50, y, 495, 18).fill(colorVerde);
    doc.fontSize(8).fill('#ffffff').font('Helvetica-Bold');
    doc.text('TOTAL', 55, y + 5, { width: 180 });
    doc.text(`$${fmt(informe.totalEgresos)}`, 240, y + 5, { width: 80, align: 'right' });
    doc.text(`$${fmt(informe.totalIngresos)}`, 325, y + 5, { width: 80, align: 'right' });
    const totalPresupMes = Object.values(informe.presupuestoMap).reduce((s, v) => s + v, 0);
    doc.text(totalPresupMes > 0 ? `$${fmt(totalPresupMes)}` : '-', 410, y + 5, { width: 65, align: 'right' });
    doc.text(totalPresupMes > 0 ? `${((informe.totalEgresos / totalPresupMes) * 100).toFixed(0)}%` : '-', 480, y + 5, { width: 60, align: 'right' });
    y += 35;

    // ── TOP 10 MOVIMIENTOS DEL MES ──
    if (y > 550) { doc.addPage(); y = 50; }
    const movimientos = db.prepare(`
      SELECT * FROM transacciones 
      WHERE fecha LIKE ? 
      ORDER BY egreso DESC LIMIT 10
    `).all(`${periodo}-%`);

    if (movimientos.length > 0) {
      doc.fontSize(15).fill(colorVerde).font('Times-Italic')
         .text('Top 10 Gastos del Mes', 50, y);
      y += 22;

      doc.rect(50, y, 495, 18).fill(colorVerde);
      doc.fontSize(8).fill('#ffffff').font('Helvetica-Bold');
      doc.text('Fecha', 55, y + 5, { width: 65 });
      doc.text('Concepto', 125, y + 5, { width: 120 });
      doc.text('Detalle', 250, y + 5, { width: 150 });
      doc.text('Monto', 405, y + 5, { width: 80, align: 'right' });
      y += 18;

      movimientos.forEach((t, i) => {
        if (y > 720) { doc.addPage(); y = 50; }
        const bg = i % 2 === 0 ? '#ffffff' : colorFondo;
        doc.rect(50, y, 495, 16).fill(bg);
        doc.fontSize(7).fill(colorNegro).font('Helvetica');
        doc.text(t.fecha, 55, y + 4, { width: 65 });
        doc.text(t.concepto, 125, y + 4, { width: 120 });
        doc.text((t.detalle || '').substring(0, 30), 250, y + 4, { width: 150 });
        doc.fill('#B83232').font('Helvetica-Bold')
           .text(`$${fmt(t.egreso)}`, 405, y + 4, { width: 80, align: 'right' });
        y += 16;
      });
    }

    // ── PIE DE PÁGINA ──
    const pages = doc.bufferedPageRange();
    for (let i = 0; i < pages.count; i++) {
      doc.switchToPage(i);
      doc.fontSize(7).fill(colorGris).font('Helvetica')
         .text(`IMPROLUX — ${mesesNombres[mes]} ${anio} — Página ${i + 1} de ${pages.count}`,
           50, doc.page.height - 30, { width: doc.page.width - 100, align: 'center' });
    }

    doc.end();
  } catch (err) {
    console.error("Error generando PDF mensual:", err);
    res.status(500).json({ error: "Error generando el informe PDF mensual" });
  }
});

// ── BACKUP CSV ────────────────────────────────────────────────────────────────
app.get("/api/backup", (req, res) => {
  try {
    const hoy = new Date().toISOString().slice(0, 10);
    const rows = db.prepare("SELECT * FROM transacciones ORDER BY fecha ASC, id ASC").all();

    // Header CSV
    const headers = ["id","fecha","concepto","detalle","ingreso","egreso","proveedor","es_cc","tc","fuente","created_at"];
    const csvLines = [headers.join(",")];

    for (const r of rows) {
      const line = headers.map(h => {
        let val = r[h] ?? "";
        val = String(val).replace(/"/g, '""');
        if (String(val).includes(",") || String(val).includes('"') || String(val).includes("\n")) {
          val = `"${val}"`;
        }
        return val;
      });
      csvLines.push(line.join(","));
    }

    const csv = csvLines.join("\n");
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="IMPROLUX_backup_${hoy}.csv"`);
    res.send(csv);
  } catch (err) {
    console.error("Error generando backup:", err);
    res.status(500).json({ error: "Error generando backup" });
  }
});

// Backup de todas las tablas (ZIP-like: múltiples CSVs en una sola descarga)
app.get("/api/backup-completo", (req, res) => {
  try {
    const hoy = new Date().toISOString().slice(0, 10);

    function tableToCsv(tableName) {
      const rows = db.prepare(`SELECT * FROM ${tableName}`).all();
      if (!rows.length) return "";
      const headers = Object.keys(rows[0]);
      const lines = [headers.join(",")];
      for (const r of rows) {
        const line = headers.map(h => {
          let val = r[h] ?? "";
          val = String(val).replace(/"/g, '""');
          if (String(val).includes(",") || String(val).includes('"') || String(val).includes("\n")) val = `"${val}"`;
          return val;
        });
        lines.push(line.join(","));
      }
      return lines.join("\n");
    }

    const tablas = ["transacciones", "cuentas_corrientes", "cheques", "inversores", "presupuestos"];
    const separador = "\n\n========================================\n";
    let contenido = "";

    for (const t of tablas) {
      const csv = tableToCsv(t);
      if (csv) {
        contenido += `=== TABLA: ${t.toUpperCase()} ===\n${csv}${separador}`;
      }
    }

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="IMPROLUX_backup_completo_${hoy}.csv"`);
    res.send(contenido);
  } catch (err) {
    console.error("Error generando backup completo:", err);
    res.status(500).json({ error: "Error generando backup" });
  }
});

// ── HEALTH CHECK ──────────────────────────────────────────────────────────────
app.get("/api/dividendos", (req, res) => {
  const pablo = db.prepare(`SELECT COALESCE(SUM(monto),0) as total FROM dividendos WHERE LOWER(socio) LIKE '%pablo%'`).get().total;
  const joni  = db.prepare(`SELECT COALESCE(SUM(monto),0) as total FROM dividendos WHERE LOWER(socio) LIKE '%jonatan%' OR LOWER(socio) LIKE '%astolfo%'`).get().total;
  const historial = db.prepare(`SELECT * FROM dividendos ORDER BY fecha DESC`).all();
  const porMes = {};
  historial.forEach(d => {
    const mes = d.fecha.slice(0,7);
    if (!porMes[mes]) porMes[mes] = { pablo: 0, joni: 0 };
    if (d.socio.toLowerCase().includes('pablo')) porMes[mes].pablo += d.monto;
    else porMes[mes].joni += d.monto;
  });
  res.json({ pablo, joni, total: pablo + joni, historial, por_mes: porMes });
});

// ── API LOTES Y LABOREOS ──────────────────────────────────────────────────────
app.post("/api/lotes/:id/poligono", (req, res) => {
  const { poligono, hectareas } = req.body;
  const lote = db.prepare("SELECT * FROM lotes WHERE id = ?").get(req.params.id);
  if (!lote) return res.status(404).json({ error: "Lote no encontrado" });
  if (hectareas != null && !isNaN(parseFloat(hectareas))) {
    db.prepare("UPDATE lotes SET poligono = ?, hectareas = ? WHERE id = ?").run(poligono || null, parseFloat(hectareas), req.params.id);
  } else {
    db.prepare("UPDATE lotes SET poligono = ? WHERE id = ?").run(poligono || null, req.params.id);
  }
  res.json({ ok: true });
});

app.get("/api/lotes", (req, res) => {
  const campo = req.query.campo;
  const lotes = campo
    ? db.prepare("SELECT * FROM lotes WHERE campo = ? ORDER BY nombre").all(campo)
    : db.prepare("SELECT * FROM lotes ORDER BY nombre").all();
  res.json(lotes);
});

app.post("/api/lotes", (req, res) => {
  const { nombre, hectareas, ha_sembrables, notas, campo } = req.body;
  if (!nombre) return res.status(400).json({ error: 'Falta nombre' });
  try {
    const r = db.prepare("INSERT INTO lotes (nombre,hectareas,ha_sembrables,notas,campo) VALUES (?,?,?,?,?)").run(nombre.toUpperCase(), hectareas||0, ha_sembrables||0, notas||'', (campo||'LA AMISTAD').toUpperCase());
    res.json({ id: r.lastInsertRowid, nombre: nombre.toUpperCase() });
  } catch(e) {
    if (String(e.message).includes('UNIQUE')) return res.status(400).json({ error: 'Ya existe un lote con ese nombre' });
    res.status(400).json({ error: e.message });
  }
});

app.put("/api/lotes/:id", (req, res) => {
  const { nombre, hectareas, ha_sembrables, notas } = req.body;
  const l = db.prepare("SELECT * FROM lotes WHERE id=?").get(req.params.id);
  if (!l) return res.status(404).json({ error: 'Lote no encontrado' });
  db.prepare("UPDATE lotes SET nombre=?, hectareas=?, ha_sembrables=?, notas=? WHERE id=?")
    .run((nombre||l.nombre).toUpperCase(), hectareas!==undefined?(parseFloat(hectareas)||0):l.hectareas,
      ha_sembrables!==undefined?(parseFloat(ha_sembrables)||0):l.ha_sembrables, notas!==undefined?notas:l.notas, req.params.id);
  res.json({ ok: true });
});

app.delete("/api/lotes/:id", (req, res) => {
  db.prepare("DELETE FROM lotes WHERE id=?").run(req.params.id);
  res.json({ ok: true });
});

// Historial de un lote: todas las órdenes que lo tocan (incluye multi-lote "A + B + C")
app.get("/api/lotes/:nombre/historial", (req, res) => {
  const nombre = req.params.nombre.toUpperCase();
  const todas = db.prepare("SELECT * FROM ordenes_trabajo ORDER BY anio DESC, numero DESC").all();
  const delLote = todas.filter(o => (o.lote||'').toUpperCase().split(' + ').map(s=>s.trim()).includes(nombre))
    .map(o => ({ ...o, numero_display: `${o.numero}/${o.anio}` }));
  res.json(delLote);
});

app.get("/api/laboreos", (req, res) => {
  const { ciclo, lote, estado } = req.query;
  let q = "SELECT l.*, lt.hectareas, lt.ha_sembrables FROM laboreos l LEFT JOIN lotes lt ON lt.nombre = l.lote WHERE 1=1";
  const p = [];
  if (ciclo) { q += " AND l.ciclo = ?"; p.push(ciclo); }
  if (lote) { q += " AND LOWER(l.lote) LIKE ?"; p.push('%'+lote.toLowerCase()+'%'); }
  if (estado) { q += " AND l.estado = ?"; p.push(estado); }
  q += " ORDER BY l.ciclo DESC, l.lote";
  const laboreos = db.prepare(q).all(...p);
  // Add items to each
  const withItems = laboreos.map(lab => ({
    ...lab,
    items: db.prepare("SELECT * FROM laboreo_items WHERE laboreo_id = ? ORDER BY categoria, id").all(lab.id)
  }));
  res.json(withItems);
});

app.post("/api/laboreos", (req, res) => {
  const { lote, tipo, descripcion, ciclo, notas } = req.body;
  if (!lote || !tipo || !ciclo) return res.status(400).json({ error: 'Faltan campos' });
  const r = db.prepare("INSERT INTO laboreos (lote,tipo,descripcion,ciclo,estado,notas) VALUES (?,?,?,?,'PLANIFICADO',?)").run(lote.toUpperCase(), tipo.toUpperCase(), descripcion||'', ciclo, notas||'');
  res.json({ id: r.lastInsertRowid });
});

app.put("/api/laboreos/:id", (req, res) => {
  const { estado, fecha_ejecucion, notas, descripcion } = req.body;
  const fields = {}; 
  if (estado) fields.estado = estado;
  if (fecha_ejecucion) fields.fecha_ejecucion = fecha_ejecucion;
  if (notas !== undefined) fields.notas = notas;
  if (descripcion !== undefined) fields.descripcion = descripcion;
  if (!Object.keys(fields).length) return res.status(400).json({ error: 'Nada que actualizar' });
  const sets = Object.keys(fields).map(k => `${k}=?`).join(',');
  db.prepare(`UPDATE laboreos SET ${sets} WHERE id=?`).run(...Object.values(fields), req.params.id);
  res.json({ ok: true });
});

app.delete("/api/laboreos/:id", (req, res) => {
  db.prepare("DELETE FROM laboreo_items WHERE laboreo_id=?").run(req.params.id);
  db.prepare("DELETE FROM laboreos WHERE id=?").run(req.params.id);
  res.json({ ok: true });
});

app.get("/api/laboreos/:id/items", (req, res) => {
  res.json(db.prepare("SELECT * FROM laboreo_items WHERE laboreo_id=? ORDER BY categoria,id").all(req.params.id));
});

app.post("/api/laboreos/:id/items", (req, res) => {
  const { categoria, descripcion, cantidad, unidad, precio_unitario } = req.body;
  if (!descripcion) return res.status(400).json({ error: 'Falta descripcion' });
  const total = (parseFloat(cantidad)||0) * (parseFloat(precio_unitario)||0);
  const r = db.prepare("INSERT INTO laboreo_items (laboreo_id,categoria,descripcion,cantidad,unidad,precio_unitario,total) VALUES (?,?,?,?,?,?,?)").run(req.params.id, categoria||'INSUMO', descripcion, parseFloat(cantidad)||0, unidad||'kg', parseFloat(precio_unitario)||0, total);
  // Recalc total
  const tot = db.prepare("SELECT COALESCE(SUM(total),0) as t FROM laboreo_items WHERE laboreo_id=?").get(req.params.id).t;
  db.prepare("UPDATE laboreos SET total_presupuestado=? WHERE id=?").run(tot, req.params.id);
  res.json({ id: r.lastInsertRowid, total });
});

app.put("/api/laboreos/:id/items/:itemId", (req, res) => {
  const { descripcion, cantidad, unidad, precio_unitario, categoria, ejecutado, fecha_ejecucion, notas_ejecucion } = req.body;
  const item = db.prepare("SELECT * FROM laboreo_items WHERE id=? AND laboreo_id=?").get(req.params.itemId, req.params.id);
  if (!item) return res.status(404).json({ error: 'Item no encontrado' });

  // Si solo se está ejecutando el item (no editando)
  if (ejecutado !== undefined && Object.keys(req.body).length <= 3) {
    const hoy = new Date().toISOString().slice(0,10);
    db.prepare("UPDATE laboreo_items SET ejecutado=?, fecha_ejecucion=?, notas_ejecucion=? WHERE id=? AND laboreo_id=?")
      .run(ejecutado ? 1 : 0, ejecutado ? (fecha_ejecucion || hoy) : null, notas_ejecucion || null, req.params.itemId, req.params.id);
  } else {
    // Edición completa
    const total = (parseFloat(cantidad||item.cantidad)||0) * (parseFloat(precio_unitario||item.precio_unitario)||0);
    db.prepare("UPDATE laboreo_items SET descripcion=?,cantidad=?,unidad=?,precio_unitario=?,total=?,categoria=?,ejecutado=?,fecha_ejecucion=?,notas_ejecucion=? WHERE id=? AND laboreo_id=?")
      .run(descripcion||item.descripcion, parseFloat(cantidad||item.cantidad)||0, unidad||item.unidad||'kg', parseFloat(precio_unitario||item.precio_unitario)||0, total, categoria||item.categoria||'INSUMO', ejecutado!==undefined?ejecutado:item.ejecutado, fecha_ejecucion||item.fecha_ejecucion, notas_ejecucion||item.notas_ejecucion, req.params.itemId, req.params.id);
  }
  const result = recalcularTotalLaboreo(req.params.id);
  res.json({ ok: true, ...result });
});

// Ejecutar item específico (shortcut)
app.post("/api/laboreos/:id/items/:itemId/ejecutar", (req, res) => {
  const hoy = new Date().toISOString().slice(0,10);
  const { fecha, notas } = req.body;
  db.prepare("UPDATE laboreo_items SET ejecutado=1, fecha_ejecucion=?, notas_ejecucion=? WHERE id=? AND laboreo_id=?")
    .run(fecha || hoy, notas || null, req.params.itemId, req.params.id);
  const result = recalcularTotalLaboreo(req.params.id);
  res.json({ ok: true, ...result });
});

// Desejecutar item
app.post("/api/laboreos/:id/items/:itemId/desejecutar", (req, res) => {
  db.prepare("UPDATE laboreo_items SET ejecutado=0, fecha_ejecucion=NULL, notas_ejecucion=NULL WHERE id=? AND laboreo_id=?")
    .run(req.params.itemId, req.params.id);
  const result = recalcularTotalLaboreo(req.params.id);
  res.json({ ok: true, ...result });
});

app.delete("/api/laboreos/:id/items/:itemId", (req, res) => {
  db.prepare("DELETE FROM laboreo_items WHERE id=? AND laboreo_id=?").run(req.params.itemId, req.params.id);
  const tot = db.prepare("SELECT COALESCE(SUM(total),0) as t FROM laboreo_items WHERE laboreo_id=?").get(req.params.id).t;
  db.prepare("UPDATE laboreos SET total_presupuestado=? WHERE id=?").run(tot, req.params.id);
  res.json({ ok: true });
});

// ══════════════════════════════════════════════════════════════════════════════
// ── API STOCK DE PRODUCTOS (v4.3) ─────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════════
app.get("/api/stock", (req, res) => {
  res.json(getStockValorizado(req.query.campo));
});

app.post("/api/stock", (req, res) => {
  const { nombre, rubro, categoria, unidad, cantidad, precio_unitario, notas, campo } = req.body;
  if (!nombre) return res.status(400).json({ error: 'Falta nombre' });
  try {
    const r = db.prepare("INSERT INTO stock_productos (nombre,rubro,categoria,unidad,cantidad,precio_unitario,notas,campo) VALUES (?,?,?,?,?,?,?,?)")
      .run(nombre.toUpperCase(), (rubro||'AGRICOLA').toUpperCase(), (categoria||'OTRO').toUpperCase(), unidad||'kg', parseFloat(cantidad)||0, parseFloat(precio_unitario)||0, notas||'', (campo||'LA AMISTAD').toUpperCase());
    // Registrar la carga inicial como movimiento ENTRADA (si hay cantidad)
    if ((parseFloat(cantidad)||0) > 0) {
      const hoy = new Date().toISOString().slice(0,10);
      db.prepare("INSERT INTO stock_movimientos (producto_id,fecha,tipo,cantidad,precio_unitario,notas) VALUES (?,?,'ENTRADA',?,?,?)")
        .run(r.lastInsertRowid, hoy, parseFloat(cantidad)||0, parseFloat(precio_unitario)||0, 'Carga inicial');
    }
    res.json({ id: r.lastInsertRowid, nombre: nombre.toUpperCase() });
  } catch(e) {
    if (String(e.message).includes('UNIQUE')) return res.status(400).json({ error: 'Ya existe un producto con ese nombre' });
    res.status(400).json({ error: e.message });
  }
});

app.put("/api/stock/:id", (req, res) => {
  const { nombre, rubro, categoria, unidad, notas } = req.body;
  const p = db.prepare("SELECT * FROM stock_productos WHERE id=?").get(req.params.id);
  if (!p) return res.status(404).json({ error: 'Producto no encontrado' });
  db.prepare("UPDATE stock_productos SET nombre=?,rubro=?,categoria=?,unidad=?,notas=? WHERE id=?")
    .run((nombre||p.nombre).toUpperCase(), (rubro||p.rubro||'AGRICOLA').toUpperCase(), (categoria||p.categoria).toUpperCase(), unidad||p.unidad, notas!==undefined?notas:p.notas, req.params.id);
  res.json({ ok: true });
});

app.delete("/api/stock/:id", (req, res) => {
  db.prepare("DELETE FROM stock_movimientos WHERE producto_id=?").run(req.params.id);
  db.prepare("UPDATE orden_items SET producto_id=NULL WHERE producto_id=?").run(req.params.id);
  db.prepare("DELETE FROM stock_productos WHERE id=?").run(req.params.id);
  res.json({ ok: true });
});

// ENTRADA de stock (compra / reposición) — recalcula costo promedio ponderado
// COMPRA de insumo: genera egreso en el flujo + entrada al stock (un solo paso)
app.post("/api/stock/compra", (req, res) => {
  const r = comprarInsumo(req.body || {});
  if (r.error) return res.status(400).json(r);
  res.json(r);
});

// LISTA de productos de stock (para que ADE elija al aplicar sanidad)
app.get("/api/stock/lista", (req, res) => {
  const campo = req.query.campo;
  const rubro = req.query.rubro;
  let q = "SELECT id, nombre, rubro, categoria, unidad, cantidad, campo FROM stock_productos WHERE 1=1";
  const p = [];
  if (campo) { q += " AND campo = ?"; p.push(campo); }
  if (rubro) { q += " AND rubro = ?"; p.push(rubro.toUpperCase()); }
  q += " ORDER BY rubro, nombre";
  res.json(db.prepare(q).all(...p));
});

// APLICACIÓN: descuenta stock por uso (llamado desde ADE al registrar sanidad).
// Match de nombre: exacto → descuenta; 1 parecido → descuenta y avisa; varios → pregunta; 0 → no encontrado.
// Permite dejar el stock en negativo, pero lo avisa.
app.post("/api/stock/aplicar", (req, res) => {
  const { producto_id, producto, cantidad, fecha, rp, detalle, campo } = req.body;
  const cant = parseFloat(cantidad) || 0;
  if (cant <= 0) return res.status(400).json({ error: "La cantidad aplicada debe ser mayor a 0" });

  const filtroCampo = (rows) => campo ? rows.filter(r => (r.campo || '').toUpperCase() === campo.toUpperCase()) : rows;

  // Ubicar el producto
  let prod = null;
  if (producto_id) prod = db.prepare("SELECT * FROM stock_productos WHERE id = ?").get(producto_id);

  if (!prod && producto) {
    const q = String(producto).trim().toLowerCase();
    // 1) Match exacto
    prod = filtroCampo(db.prepare("SELECT * FROM stock_productos WHERE LOWER(nombre) = ?").all(q))[0] || null;
    // 2) Parecidos: contiene la búsqueda, o la búsqueda contiene el nombre
    if (!prod) {
      const todos = filtroCampo(db.prepare("SELECT * FROM stock_productos").all());
      const parecidos = todos.filter(r => {
        const n = (r.nombre || '').toLowerCase();
        return n.includes(q) || q.includes(n) || n.split(/\s+/)[0] === q.split(/\s+/)[0];
      });
      if (parecidos.length === 1) {
        prod = parecidos[0];
        // se resuelve abajo con aviso de interpretación
        var interpretado = true;
      } else if (parecidos.length > 1) {
        return res.json({
          ok: false, ambiguo: true, buscado: producto,
          opciones: parecidos.map(p => ({ id: p.id, nombre: p.nombre, cantidad: p.cantidad, unidad: p.unidad })),
          mensaje: `Hay varios productos parecidos a "${producto}". ¿Cuál aplicaste?`
        });
      }
    }
  }

  if (!prod) return res.status(404).json({ error: `No encontré "${producto}" ni nada parecido en el stock`, no_encontrado: true, buscado: producto });

  const nuevaCantidad = (prod.cantidad || 0) - cant;
  db.prepare("UPDATE stock_productos SET cantidad = ? WHERE id = ?").run(nuevaCantidad, prod.id);
  const notas = `Aplicación${rp ? ' RP ' + rp : ''}${detalle ? ' · ' + detalle : ''}`;
  db.prepare("INSERT INTO stock_movimientos (producto_id,fecha,tipo,cantidad,precio_unitario,notas) VALUES (?,?,'SALIDA',?,?,?)")
    .run(prod.id, fecha || new Date().toISOString().slice(0, 10), cant, prod.precio_unitario || 0, notas);

  const negativo = nuevaCantidad < 0;
  const interpretadoDe = (typeof interpretado !== 'undefined' && producto && prod.nombre.toLowerCase() !== String(producto).trim().toLowerCase()) ? producto : null;
  const costoUnit = prod.precio_unitario || 0;
  res.json({
    ok: true, producto: prod.nombre, unidad: prod.unidad,
    aplicado: cant, restante: Math.round(nuevaCantidad * 100) / 100,
    // Costo del consumo: ADE lo imputa al animal o al lote sin volver a preguntar.
    costo_unitario: costoUnit,
    costo_total: Math.round(cant * costoUnit * 100) / 100,
    rubro: prod.rubro || 'AGRICOLA', categoria: prod.categoria || null,
    negativo, interpretado_de: interpretadoDe,
    aviso: [
      interpretadoDe ? `📝 Interpreté "${interpretadoDe}" → ${prod.nombre}.` : null,
      negativo ? `⚠️ Stock de ${prod.nombre} quedó en negativo (${Math.round(nuevaCantidad*100)/100} ${prod.unidad}). Registrá una compra para reponer.` : null
    ].filter(Boolean).join(' ') || null
  });
});

// CONSUMO: ADE avisa la ración diaria de un lote. Mismo efecto que aplicar,
// pero pensado para lotes (sin RP) y devolviendo siempre el costo.
app.post("/api/stock/consumo", (req, res) => {
  const { producto, producto_id, cantidad, fecha, lote, detalle, campo } = req.body;
  const cant = parseFloat(cantidad) || 0;
  if (cant <= 0) return res.status(400).json({ error: "La cantidad consumida debe ser mayor a 0" });

  let prod = producto_id ? db.prepare("SELECT * FROM stock_productos WHERE id = ?").get(producto_id) : null;
  if (!prod && producto) {
    const q = String(producto).trim().toLowerCase();
    let rows = db.prepare("SELECT * FROM stock_productos WHERE LOWER(nombre) = ?").all(q);
    if (campo) rows = rows.filter(r => (r.campo || '').toUpperCase() === campo.toUpperCase());
    prod = rows[0] || null;
  }
  if (!prod) return res.status(404).json({ error: `No encontré "${producto}" en el stock`, no_encontrado: true });

  const nueva = (prod.cantidad || 0) - cant;
  const costoUnit = prod.precio_unitario || 0;
  db.prepare("UPDATE stock_productos SET cantidad = ? WHERE id = ?").run(nueva, prod.id);
  db.prepare("INSERT INTO stock_movimientos (producto_id,fecha,tipo,cantidad,precio_unitario,notas) VALUES (?,?,'SALIDA',?,?,?)")
    .run(prod.id, fecha || new Date().toISOString().slice(0, 10), cant, costoUnit,
         `Consumo${lote ? ' lote ' + lote : ''}${detalle ? ' · ' + detalle : ''}`);

  res.json({
    ok: true, producto: prod.nombre, unidad: prod.unidad,
    consumido: cant, restante: Math.round(nueva * 100) / 100,
    costo_unitario: costoUnit, costo_total: Math.round(cant * costoUnit * 100) / 100,
    negativo: nueva < 0,
    aviso: nueva < 0 ? `⚠️ ${prod.nombre} quedó en negativo (${Math.round(nueva*100)/100} ${prod.unidad}). Cargá una compra.` : null
  });
});

app.post("/api/stock/:id/entrada", (req, res) => {
  const { cantidad, precio_unitario, fecha, notas } = req.body;
  if (!cantidad) return res.status(400).json({ error: 'Falta cantidad' });
  const hoy = new Date().toISOString().slice(0,10);
  const r = entradaStock(req.params.id, cantidad, precio_unitario, fecha||hoy, notas);
  if (!r) return res.status(404).json({ error: 'Producto no encontrado' });
  res.json({ ok: true, ...r });
});

// AJUSTE de stock — fija la cantidad a un valor absoluto (recuento físico)
app.post("/api/stock/:id/ajuste", (req, res) => {
  const { cantidad_nueva, notas } = req.body;
  const p = db.prepare("SELECT * FROM stock_productos WHERE id=?").get(req.params.id);
  if (!p) return res.status(404).json({ error: 'Producto no encontrado' });
  const nueva = parseFloat(cantidad_nueva) || 0;
  const diff = nueva - (p.cantidad || 0);
  const hoy = new Date().toISOString().slice(0,10);
  db.prepare("UPDATE stock_productos SET cantidad=? WHERE id=?").run(nueva, req.params.id);
  db.prepare("INSERT INTO stock_movimientos (producto_id,fecha,tipo,cantidad,precio_unitario,notas) VALUES (?,?,'AJUSTE',?,?,?)")
    .run(req.params.id, hoy, diff, p.precio_unitario||0, notas||'Ajuste por recuento');
  res.json({ ok: true, cantidad: nueva });
});

app.get("/api/stock/:id/movimientos", (req, res) => {
  const movs = db.prepare("SELECT * FROM stock_movimientos WHERE producto_id=? ORDER BY id DESC").all(req.params.id);
  res.json(movs);
});

// ══════════════════════════════════════════════════════════════════════════════
// ── API ÓRDENES DE TRABAJO (v4.3) ─────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════════
app.get("/api/ordenes", (req, res) => {
  const { anio, lote, estado, campo } = req.query;
  let q = "SELECT o.*, lt.hectareas as lote_hectareas, lt.ha_sembrables as lote_ha_sembrables FROM ordenes_trabajo o LEFT JOIN lotes lt ON lt.nombre = o.lote WHERE 1=1";
  const p = [];
  if (campo) { q += " AND o.campo = ?"; p.push(campo); }
  if (anio) { q += " AND o.anio = ?"; p.push(parseInt(anio)); }
  if (lote) { q += " AND LOWER(o.lote) LIKE ?"; p.push('%'+lote.toLowerCase()+'%'); }
  if (estado) { q += " AND o.estado = ?"; p.push(estado); }
  q += " ORDER BY o.anio DESC, o.numero DESC";
  const ordenes = db.prepare(q).all(...p);
  const withItems = ordenes.map(o => ({
    ...o,
    numero_display: `${o.numero}/${o.anio}`,
    items: db.prepare(`
      SELECT oi.*, sp.nombre as producto_nombre, sp.cantidad as stock_actual
      FROM orden_items oi LEFT JOIN stock_productos sp ON sp.id = oi.producto_id
      WHERE oi.orden_id = ? ORDER BY oi.tipo, oi.id
    `).all(o.id)
  }));
  res.json(withItems);
});

app.get("/api/ordenes/:id", (req, res) => {
  const det = getOrdenDetalle(req.params.id);
  if (!det) return res.status(404).json({ error: 'Orden no encontrada' });
  res.json(det);
});

app.post("/api/ordenes", (req, res) => {
  const { lote, lotes, titulo, anio, ciclo, numero, notas, hectareas } = req.body;
  const anioFinal = parseInt(anio) || new Date().getFullYear();
  const numFinal = numero ? parseInt(numero) : getNextNumeroOrden(anioFinal);
  let loteStr = '';
  let ha = parseFloat(hectareas);
  if (Array.isArray(lotes) && lotes.length) {
    // Varios lotes: uno el nombre con " + " y sumo las ha aprovechables de cada uno
    const names = lotes.map(n => String(n).toUpperCase().trim()).filter(Boolean);
    loteStr = names.join(' + ');
    if (isNaN(ha) || ha === 0) {
      ha = names.reduce((s, n) => {
        const lt = db.prepare("SELECT ha_sembrables FROM lotes WHERE nombre = ?").get(n);
        return s + (parseFloat(lt?.ha_sembrables) || 0);
      }, 0);
    }
  } else {
    loteStr = lote ? lote.toUpperCase() : '';
    if ((isNaN(ha) || ha === 0) && loteStr) {
      const lt = db.prepare("SELECT ha_sembrables FROM lotes WHERE nombre = ?").get(loteStr);
      ha = parseFloat(lt?.ha_sembrables) || 0;
    }
  }
  ha = ha || 0;
  const r = db.prepare("INSERT INTO ordenes_trabajo (numero,anio,lote,titulo,ciclo,hectareas,estado,notas,campo,tipo_implantacion) VALUES (?,?,?,?,?,?,'PLANIFICADA',?,?,?)")
    .run(numFinal, anioFinal, loteStr, titulo||'', ciclo||'', ha, notas||'', (req.body.campo||'LA AMISTAD').toUpperCase(),
         (req.body.tipo_implantacion||'').toUpperCase()||null);
  res.json({ id: r.lastInsertRowid, numero: numFinal, anio: anioFinal, hectareas: ha, numero_display: `${numFinal}/${anioFinal}` });
});

app.put("/api/ordenes/:id", (req, res) => {
  const { lote, titulo, ciclo, estado, notas, hectareas, anio, numero, tipo_implantacion } = req.body;
  // Vacío = no implanta nada. Se guarda como NULL para que ADE lo saltee.
  if (tipo_implantacion !== undefined) {
    db.prepare("UPDATE ordenes_trabajo SET tipo_implantacion = ? WHERE id = ?")
      .run((tipo_implantacion || '').toUpperCase() || null, req.params.id);
  }
  const o = db.prepare("SELECT * FROM ordenes_trabajo WHERE id=?").get(req.params.id);
  if (!o) return res.status(404).json({ error: 'Orden no encontrada' });
  const fields = {};
  if (lote !== undefined) fields.lote = lote ? lote.toUpperCase() : '';
  if (titulo !== undefined) fields.titulo = titulo;
  if (ciclo !== undefined) fields.ciclo = ciclo;
  if (estado !== undefined) fields.estado = estado;
  if (notas !== undefined) fields.notas = notas;
  if (hectareas !== undefined) fields.hectareas = parseFloat(hectareas) || 0;
  if (anio !== undefined) fields.anio = parseInt(anio) || o.anio;
  if (numero !== undefined) fields.numero = parseInt(numero) || o.numero;
  if (!Object.keys(fields).length) return res.status(400).json({ error: 'Nada que actualizar' });
  const sets = Object.keys(fields).map(k => `${k}=?`).join(',');
  db.prepare(`UPDATE ordenes_trabajo SET ${sets} WHERE id=?`).run(...Object.values(fields), req.params.id);
  // Si cambiaron las ha, recalculo los items cargados por dosis
  if (hectareas !== undefined) { recomputarDosisOrden(req.params.id); recalcularOrden(req.params.id); }
  res.json({ ok: true });
});

app.delete("/api/ordenes/:id", (req, res) => {
  db.prepare("DELETE FROM orden_items WHERE orden_id=?").run(req.params.id);
  db.prepare("DELETE FROM orden_cambios WHERE orden_id=?").run(req.params.id);
  db.prepare("DELETE FROM ordenes_trabajo WHERE id=?").run(req.params.id);
  res.json({ ok: true });
});

// Registrar un cambio manual en la bitácora de la orden
app.post("/api/ordenes/:id/cambios", (req, res) => {
  const { texto } = req.body;
  if (!texto) return res.status(400).json({ error: 'Falta texto' });
  logCambioOrden(req.params.id, texto);
  res.json({ ok: true });
});

// ── ITEMS DE LA ORDEN ──
app.post("/api/ordenes/:id/items", (req, res) => {
  const { tipo, etapa, producto_id, descripcion, cantidad, unidad, precio_unitario, dosis } = req.body;
  const o = db.prepare("SELECT * FROM ordenes_trabajo WHERE id=?").get(req.params.id);
  if (!o) return res.status(404).json({ error: 'Orden no encontrada' });
  const tipoFinal = (tipo || 'INSUMO').toUpperCase();

  // Si es INSUMO con producto vinculado, tomo unidad y precio del stock por defecto
  let desc = descripcion, uni = unidad, precio = parseFloat(precio_unitario) || 0, prodId = producto_id || null;
  if (tipoFinal === 'INSUMO' && prodId) {
    const prod = db.prepare("SELECT * FROM stock_productos WHERE id=?").get(prodId);
    if (prod) {
      if (!desc) desc = prod.nombre;
      if (!uni) uni = prod.unidad;
      if (!precio) precio = prod.precio_unitario || 0;
    }
  }
  if (!desc) return res.status(400).json({ error: 'Falta descripcion o producto' });
  // Cantidad: si viene dosis>0 y la orden tiene ha, cantidad = dosis × ha; si no, la cantidad directa
  const dos = parseFloat(dosis) || 0;
  const ha = parseFloat(o.hectareas) || 0;
  let cant = parseFloat(cantidad) || 0;
  if (dos > 0 && ha > 0) cant = dos * ha;
  const total = cant * precio;
  const etapaFinal = (etapa || 'GENERAL').toUpperCase().trim() || 'GENERAL';
  const r = db.prepare("INSERT INTO orden_items (orden_id,tipo,etapa,producto_id,descripcion,dosis,cantidad,unidad,precio_unitario,total) VALUES (?,?,?,?,?,?,?,?,?,?)")
    .run(req.params.id, tipoFinal, etapaFinal, tipoFinal==='INSUMO'?prodId:null, desc, dos, cant, uni||'kg', precio, total);
  const result = recalcularOrden(req.params.id);
  res.json({ id: r.lastInsertRowid, total, cantidad: cant, ...result });
});

// Editar item — registra el cambio en la bitácora
app.put("/api/ordenes/:id/items/:itemId", (req, res) => {
  const item = db.prepare("SELECT * FROM orden_items WHERE id=? AND orden_id=?").get(req.params.itemId, req.params.id);
  if (!item) return res.status(404).json({ error: 'Item no encontrado' });
  const { tipo, etapa, producto_id, descripcion, cantidad, unidad, precio_unitario, dosis } = req.body;

  const nuevaDesc = descripcion !== undefined ? descripcion : item.descripcion;
  const nuevoPrecio = precio_unitario !== undefined ? (parseFloat(precio_unitario)||0) : item.precio_unitario;
  const nuevaUni = unidad !== undefined ? unidad : item.unidad;
  const nuevoTipo = tipo !== undefined ? tipo.toUpperCase() : item.tipo;
  const nuevoProd = producto_id !== undefined ? (producto_id||null) : item.producto_id;
  const nuevaEtapa = etapa !== undefined ? ((etapa||'GENERAL').toUpperCase().trim() || 'GENERAL') : (item.etapa || 'GENERAL');
  const nuevaDosis = dosis !== undefined ? (parseFloat(dosis)||0) : (item.dosis||0);

  // Cantidad: si hay dosis>0 y la orden tiene ha, recalculo; si no, uso la cantidad provista/actual
  const ord = db.prepare("SELECT hectareas FROM ordenes_trabajo WHERE id=?").get(req.params.id);
  const ha = parseFloat(ord?.hectareas) || 0;
  let nuevaCant = cantidad !== undefined ? (parseFloat(cantidad)||0) : item.cantidad;
  if (nuevaDosis > 0 && ha > 0) nuevaCant = nuevaDosis * ha;
  const total = nuevaCant * nuevoPrecio;

  // Bitácora: registrar el cambio de descripción/producto si cambió
  if (descripcion !== undefined && descripcion !== item.descripcion) {
    logCambioOrden(req.params.id, `Se cambió "${item.descripcion}" por "${descripcion}"`);
  }

  db.prepare("UPDATE orden_items SET tipo=?,etapa=?,producto_id=?,descripcion=?,dosis=?,cantidad=?,unidad=?,precio_unitario=?,total=? WHERE id=?")
    .run(nuevoTipo, nuevaEtapa, nuevoTipo==='INSUMO'?nuevoProd:null, nuevaDesc, nuevaDosis, nuevaCant, nuevaUni, nuevoPrecio, total, req.params.itemId);
  const result = recalcularOrden(req.params.id);
  res.json({ ok: true, total, ...result });
});

app.delete("/api/ordenes/:id/items/:itemId", (req, res) => {
  const item = db.prepare("SELECT * FROM orden_items WHERE id=? AND orden_id=?").get(req.params.itemId, req.params.id);
  if (!item) return res.status(404).json({ error: 'Item no encontrado' });
  // Si estaba ejecutado y descontó stock, revertir la salida
  if (item.ejecutado && item.producto_id && item.cantidad_ejecutada > 0) {
    const hoy = new Date().toISOString().slice(0,10);
    entradaStock(item.producto_id, item.cantidad_ejecutada, 0, hoy, `Reversión por borrado item orden #${req.params.id}`);
  }
  db.prepare("DELETE FROM orden_items WHERE id=? AND orden_id=?").run(req.params.itemId, req.params.id);
  const result = recalcularOrden(req.params.id);
  res.json({ ok: true, ...result });
});

// EJECUTAR item — descuenta stock (si es INSUMO con producto) y marca ejecutado
app.post("/api/ordenes/:id/items/:itemId/ejecutar", (req, res) => {
  const item = db.prepare("SELECT * FROM orden_items WHERE id=? AND orden_id=?").get(req.params.itemId, req.params.id);
  if (!item) return res.status(404).json({ error: 'Item no encontrado' });
  if (item.ejecutado) return res.status(400).json({ error: 'El item ya está ejecutado' });
  const hoy = new Date().toISOString().slice(0,10);
  const { cantidad_ejecutada, fecha, notas } = req.body;
  // Cantidad real usada — por defecto la planificada
  const cantEjec = cantidad_ejecutada !== undefined && cantidad_ejecutada !== null && cantidad_ejecutada !== ''
    ? (parseFloat(cantidad_ejecutada)||0) : item.cantidad;
  const totalEjec = cantEjec * item.precio_unitario;

  db.prepare("UPDATE orden_items SET ejecutado=1, cantidad_ejecutada=?, total_ejecutado=?, fecha_ejecucion=?, notas=? WHERE id=?")
    .run(cantEjec, totalEjec, fecha||hoy, notas!==undefined?notas:item.notas, req.params.itemId);

  // Descontar del stock si es INSUMO vinculado a un producto
  let stockInfo = null;
  if (item.tipo === 'INSUMO' && item.producto_id && cantEjec > 0) {
    stockInfo = salidaStock(item.producto_id, cantEjec, fecha||hoy, `Consumo orden #${req.params.id} — ${item.descripcion}`, req.params.id);
  }
  // Bitácora si la cantidad ejecutada difiere de la planificada
  if (cantEjec !== item.cantidad) {
    logCambioOrden(req.params.id, `Ejecutado "${item.descripcion}": ${cantEjec} ${item.unidad} (planificado ${item.cantidad})`);
  }
  const result = recalcularOrden(req.params.id);
  res.json({ ok: true, stock: stockInfo, ...result });
});

// DESEJECUTAR item — repone el stock descontado
app.post("/api/ordenes/:id/items/:itemId/desejecutar", (req, res) => {
  const item = db.prepare("SELECT * FROM orden_items WHERE id=? AND orden_id=?").get(req.params.itemId, req.params.id);
  if (!item) return res.status(404).json({ error: 'Item no encontrado' });
  if (!item.ejecutado) return res.status(400).json({ error: 'El item no está ejecutado' });
  const hoy = new Date().toISOString().slice(0,10);
  // Reponer stock si había descontado
  if (item.tipo === 'INSUMO' && item.producto_id && item.cantidad_ejecutada > 0) {
    entradaStock(item.producto_id, item.cantidad_ejecutada, 0, hoy, `Reversión ejecución orden #${req.params.id} — ${item.descripcion}`, req.params.id);
  }
  db.prepare("UPDATE orden_items SET ejecutado=0, cantidad_ejecutada=0, total_ejecutado=0, fecha_ejecucion=NULL WHERE id=?").run(req.params.itemId);
  const result = recalcularOrden(req.params.id);
  res.json({ ok: true, ...result });
});

app.get("/", (req, res) => res.json({ status: "IMPROLUX Bot activo 🟢", version: "4.28.1-implantacion" }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`IMPROLUX Bot v4.26.0-pendientes corriendo en puerto ${PORT}`);
  scheduleInformeMensual();
});
