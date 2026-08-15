const express = require("express");
const Anthropic = require("@anthropic-ai/sdk");
const Database = require("better-sqlite3");
const path = require("path");
const fs = require("fs");
let PDFDocument;
try { PDFDocument = require("pdfkit"); } catch(e) { console.log("pdfkit no disponible, informes PDF deshabilitados"); }
let ExcelJS;
try { ExcelJS = require("exceljs"); } catch(e) { console.log("exceljs no disponible, informes Excel deshabilitados"); }
let importador;
let reproduccion;
try { reproduccion = require("./reproduccion.js"); } catch(e) { console.log("reproduccion.js no disponible:", e.message); }
let consistencia;
let campoActualKey = null;   // campo del request en curso
let ultimaRevision = null;   // hallazgos de la última revisión, para poder corregirlos
try { consistencia = require("./consistencia.js"); } catch(e) { console.log("consistencia.js no disponible:", e.message); }
try { importador = require("./importador.js"); }
catch(e) { console.log("⚠️  IMPORTADOR NO DISPONIBLE — no se van a poder leer archivos:", e.message); }
let pdfjsLib;
try { pdfjsLib = require("pdfjs-dist/legacy/build/pdf.js"); } catch(e) { console.log("pdfjs-dist no disponible, lectura PDF deshabilitada"); }
let twilio;
try { twilio = require("twilio"); } catch(e) { console.log("twilio no disponible, WhatsApp deshabilitado"); }

const app = express();
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: false }));

// ── CORS ──────────────────────────────────────────────────────────────────────
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

// ── CONFIGURACIÓN MULTI-CAMPO ─────────────────────────────────────────────────
const CAMPOS = {
  angus_del_este: {
    nombre: "Angus del Este",
    pais: "UY",
    codigo_tipo: "DICOSE",
    codigo: "39100",
    raza: "A. ANGUS",
    whatsapp: process.env.WHATSAPP_ANGUS || "",
    admin: process.env.ADMIN_ANGUS || "",
    bot_financiero: "https://improlux-bot-production.up.railway.app",
    campo_improlux: "LA AMISTAD",
    empresa: "improlux",
    db_file: "angus_del_este.db"
  },
  angus_la_posta: {
    nombre: "Campo Videla",
    pais: "AR",
    codigo_tipo: "RENSPA",
    codigo: "",
    raza: "A. ANGUS",
    whatsapp: process.env.WHATSAPP_POSTA || "",
    admin: process.env.ADMIN_POSTA || "",
    bot_financiero: "https://videla-production.up.railway.app",
    campo_improlux: "AMAKAIK",
    empresa: "amakaik",
    db_file: "angus_la_posta.db"
  },
  las_tranqueras: {
    nombre: "Las Tranqueras",
    pais: "UY",
    codigo_tipo: "DICOSE",
    codigo: "",
    raza: "A. ANGUS",
    whatsapp: process.env.WHATSAPP_TRANQUERAS || "",
    admin: process.env.ADMIN_TRANQUERAS || "",
    bot_financiero: "https://improlux-bot-production.up.railway.app",
    campo_improlux: "LAS TRANQUERAS",
    empresa: "amakaik_srl",
    db_file: "las_tranqueras.db"
  },
  el_triunfo: {
    nombre: "El Triunfo",
    pais: "AR",
    codigo_tipo: "RENSPA",
    codigo: "",
    raza: "A. ANGUS",
    whatsapp: process.env.WHATSAPP_TRIUNFO || "",
    admin: process.env.ADMIN_TRIUNFO || "",
    bot_financiero: "https://videla-production.up.railway.app",
    campo_improlux: "EL TRIUNFO",
    empresa: "amakaik",
    db_file: "el_triunfo.db"
  },
  la_guagua: {
    nombre: "La Guagua",
    pais: "AR",
    codigo_tipo: "RENSPA",
    codigo: "",
    raza: "A. ANGUS",
    whatsapp: process.env.WHATSAPP_GUAGUA || "",
    admin: process.env.ADMIN_GUAGUA || "",
    bot_financiero: "https://videla-production.up.railway.app",
    campo_improlux: "LA GUAGUA",
    empresa: "amakaik",
    db_file: "la_guagua.db"
  }
};

const CAMPO_DEFAULT = "angus_del_este";

// ── SCHEMA SQL (igual para todos los campos) ──────────────────────────────────
const SCHEMA_SQL = `
  CREATE TABLE IF NOT EXISTS animales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chip TEXT UNIQUE,
    rp TEXT NOT NULL,
    fecha_nac TEXT,
    raza TEXT DEFAULT 'A. ANGUS',
    registro TEXT,
    sexo TEXT NOT NULL,
    pelo TEXT,
    categoria TEXT,
    destino TEXT DEFAULT 'PLANTEL',
    madre_rp TEXT,
    madre_hba TEXT,
    padre_rp TEXT,
    padre_hba TEXT,
    fecha_ingreso TEXT,
    estado TEXT DEFAULT 'ACTIVO',
    fecha_salida TEXT,
    motivo_salida TEXT,
    notas TEXT,
    hbu TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE INDEX IF NOT EXISTS idx_animales_rp ON animales(rp);
  CREATE INDEX IF NOT EXISTS idx_animales_chip ON animales(chip);
  CREATE INDEX IF NOT EXISTS idx_animales_categoria ON animales(categoria);

  CREATE TABLE IF NOT EXISTS importaciones_pendientes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    usuario TEXT NOT NULL,
    tipo TEXT NOT NULL,
    archivo TEXT,
    filas TEXT NOT NULL,
    plan TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS pesadas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    animal_id INTEGER NOT NULL,
    fecha TEXT NOT NULL,
    peso REAL NOT NULL,
    contexto TEXT,
    gdp REAL,
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (animal_id) REFERENCES animales(id)
  );
  CREATE INDEX IF NOT EXISTS idx_pesadas_animal ON pesadas(animal_id);

  CREATE TABLE IF NOT EXISTS mediciones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    animal_id INTEGER NOT NULL,
    fecha TEXT NOT NULL,
    tipo TEXT NOT NULL,
    valor REAL,
    valor_texto TEXT,
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (animal_id) REFERENCES animales(id)
  );
  CREATE INDEX IF NOT EXISTS idx_mediciones_animal ON mediciones(animal_id);

  CREATE TABLE IF NOT EXISTS ecografias (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    animal_id INTEGER NOT NULL,
    fecha_medicion TEXT NOT NULL,
    dias_vida INTEGER,
    pct_gi REAL,
    aob REAL,
    gd REAL,
    gc REAL,
    estado TEXT,
    ecografista TEXT,
    interpretador TEXT,
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (animal_id) REFERENCES animales(id)
  );
  CREATE INDEX IF NOT EXISTS idx_ecografias_animal ON ecografias(animal_id);

  CREATE TABLE IF NOT EXISTS servicios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    animal_id INTEGER NOT NULL,
    temporada TEXT,
    tacto_pre TEXT,
    cc_pre REAL,
    tipo_servicio TEXT,
    semen_iatf TEXT,
    fecha_iatf TEXT,
    toro_natural TEXT,
    fecha_ingreso_toro TEXT,
    tacto_servicio TEXT,
    cc_post REAL,
    resultado TEXT,
    fecha_parto TEXT,
    ternero_rp TEXT,
    peso_nacimiento REAL,
    peso_destete REAL,
    sexo_cria TEXT,
    pelo_cria TEXT,
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (animal_id) REFERENCES animales(id)
  );
  CREATE INDEX IF NOT EXISTS idx_servicios_animal ON servicios(animal_id);

  CREATE TABLE IF NOT EXISTS sanidad (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    animal_id INTEGER NOT NULL,
    fecha TEXT NOT NULL,
    tipo TEXT NOT NULL,
    producto TEXT,
    dosis TEXT,
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (animal_id) REFERENCES animales(id)
  );
  CREATE INDEX IF NOT EXISTS idx_sanidad_animal ON sanidad(animal_id);

  CREATE TABLE IF NOT EXISTS toros (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    animal_id INTEGER,
    rp TEXT NOT NULL,
    nombre TEXT,
    breedplan TEXT,
    ce REAL,
    aptitud TEXT,
    fecha_ingreso TEXT,
    fecha_salida TEXT,
    motivo_salida TEXT,
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS lotes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL UNIQUE,
    descripcion TEXT,
    potrero TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS lote_animales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lote_id INTEGER NOT NULL,
    animal_id INTEGER NOT NULL UNIQUE,
    fecha_ingreso TEXT DEFAULT (date('now')),
    FOREIGN KEY (lote_id) REFERENCES lotes(id),
    FOREIGN KEY (animal_id) REFERENCES animales(id)
  );
  CREATE INDEX IF NOT EXISTS idx_lote_animales_lote ON lote_animales(lote_id);
  CREATE INDEX IF NOT EXISTS idx_lote_animales_animal ON lote_animales(animal_id);

  -- Lecturas de control de lotes (Gallagher)
  CREATE TABLE IF NOT EXISTS lecturas_lote (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lote_id INTEGER NOT NULL,
    fecha TEXT NOT NULL,
    total_lote INTEGER,
    leidos INTEGER,
    faltantes TEXT,
    novedades TEXT,
    fuente TEXT DEFAULT 'gallagher',
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (lote_id) REFERENCES lotes(id)
  );

  CREATE TABLE IF NOT EXISTS sesiones (
    usuario TEXT PRIMARY KEY,
    historial TEXT DEFAULT '[]',
    updated_at TEXT DEFAULT (datetime('now'))
  );

  -- Costos por animal individual
  CREATE TABLE IF NOT EXISTS costos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    animal_id INTEGER,
    fecha TEXT NOT NULL,
    concepto TEXT NOT NULL,
    detalle TEXT,
    monto REAL NOT NULL,
    moneda TEXT DEFAULT 'USD',
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (animal_id) REFERENCES animales(id)
  );
  CREATE INDEX IF NOT EXISTS idx_costos_animal ON costos(animal_id);

  -- Costos fijos del campo (alquiler, SRA, etc.)
  CREATE TABLE IF NOT EXISTS costos_campo (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    concepto TEXT NOT NULL,
    monto_mensual REAL DEFAULT 0,
    tipo_reparto TEXT DEFAULT 'POR_CABEZA',
    activo INTEGER DEFAULT 1,
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  -- Stock insumos veterinarios
  CREATE TABLE IF NOT EXISTS stock_insumos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL,
    tipo TEXT DEFAULT 'VETERINARIO',
    unidad TEXT DEFAULT 'dosis',
    stock_actual REAL DEFAULT 0,
    stock_minimo REAL DEFAULT 0,
    costo_unitario REAL DEFAULT 0,
    proveedor TEXT,
    vencimiento TEXT,
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  -- Movimientos de stock (entradas y salidas)
  CREATE TABLE IF NOT EXISTS stock_movimientos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_id INTEGER NOT NULL,
    fecha TEXT NOT NULL,
    tipo TEXT NOT NULL,
    cantidad REAL NOT NULL,
    costo_total REAL,
    detalle TEXT,
    animal_id INTEGER,
    lote_id INTEGER,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (stock_id) REFERENCES stock_insumos(id),
    FOREIGN KEY (animal_id) REFERENCES animales(id)
  );
  CREATE INDEX IF NOT EXISTS idx_stock_mov_stock ON stock_movimientos(stock_id);

  -- Stock genética: embriones y pajuelas
  CREATE TABLE IF NOT EXISTS stock_genetica (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo TEXT NOT NULL,
    toro_nombre TEXT,
    toro_rp TEXT,
    toro_hba TEXT,
    donante_nombre TEXT,
    donante_rp TEXT,
    donante_hba TEXT,
    raza TEXT DEFAULT 'A. ANGUS',
    cantidad INTEGER DEFAULT 0,
    costo_unitario REAL DEFAULT 0,
    proveedor TEXT,
    fecha_colecta TEXT,
    ubicacion TEXT,
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  -- Stock alimento con consumo programado
  CREATE TABLE IF NOT EXISTS stock_alimento (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL,
    tipo TEXT DEFAULT 'RACION',
    unidad TEXT DEFAULT 'kg',
    stock_actual REAL DEFAULT 0,
    costo_por_kg REAL DEFAULT 0,
    proveedor TEXT,
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  -- Eventos de campo (registro libre por animal)
  CREATE TABLE IF NOT EXISTS eventos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    animal_id INTEGER,
    fecha TEXT NOT NULL,
    tipo TEXT DEFAULT 'OBSERVACION',
    descripcion TEXT NOT NULL,
    usuario TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (animal_id) REFERENCES animales(id)
  );
  CREATE INDEX IF NOT EXISTS idx_eventos_animal ON eventos(animal_id);
  CREATE INDEX IF NOT EXISTS idx_eventos_fecha ON eventos(fecha);

  -- Consumos programados de alimento
  CREATE TABLE IF NOT EXISTS consumos_programados (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alimento_id INTEGER NOT NULL,
    lote_nombre TEXT,
    cantidad_animales INTEGER DEFAULT 1,
    kg_por_animal_dia REAL NOT NULL,
    fecha_inicio TEXT NOT NULL,
    fecha_fin TEXT,
    activo INTEGER DEFAULT 1,
    notas TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (alimento_id) REFERENCES stock_alimento(id)
  );

  -- Movimientos de alimento (compras y consumos)
  CREATE TABLE IF NOT EXISTS alimento_movimientos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alimento_id INTEGER NOT NULL,
    fecha TEXT NOT NULL,
    tipo TEXT NOT NULL,
    cantidad REAL NOT NULL,
    costo_total REAL,
    detalle TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (alimento_id) REFERENCES stock_alimento(id)
  );
`;

function runMigrations(database) {
  try { database.exec("ALTER TABLE animales ADD COLUMN destino TEXT DEFAULT 'PLANTEL'"); } catch(e) {}
  try { database.exec("ALTER TABLE animales ADD COLUMN hbu TEXT"); } catch(e) {}
  // Nombre del animal. Un padre se conoce por el nombre, no por el RP:
  // "LUCUMA" dice mucho más que "S249" en un catálogo o un formulario.
  try { database.exec("ALTER TABLE animales ADD COLUMN nombre TEXT"); } catch(e) {}
  // Datos de los padres cuando NO están cargados como animales: semen comprado,
  // vacas de otra cabaña. La familia de la madre y el nombre del padre no se
  // pueden deducir del RP, así que se guardan acá.
  // HBA = registro en Argentina (ERA). HBU = registro en ARU, Uruguay.
  try { database.exec("ALTER TABLE animales ADD COLUMN madre_nombre TEXT"); } catch(e) {}
  try { database.exec("ALTER TABLE animales ADD COLUMN padre_nombre TEXT"); } catch(e) {}
  try { database.exec("ALTER TABLE animales ADD COLUMN madre_hbu TEXT"); } catch(e) {}
  try { database.exec("ALTER TABLE animales ADD COLUMN padre_hbu TEXT"); } catch(e) {}
  try { database.exec("ALTER TABLE animales ADD COLUMN mellizo_de TEXT"); } catch(e) {}
  try { database.exec("ALTER TABLE servicios ADD COLUMN ternero2_rp TEXT"); } catch(e) {}
  try { database.exec("ALTER TABLE servicios ADD COLUMN peso_nacimiento2 REAL"); } catch(e) {}
  try { database.exec("ALTER TABLE servicios ADD COLUMN sexo_cria2 TEXT"); } catch(e) {}
  try { database.exec("ALTER TABLE servicios ADD COLUMN pelo_cria2 TEXT"); } catch(e) {}
  try {
    const conHbuNotas = database.prepare("SELECT id, notas FROM animales WHERE notas LIKE '%HBU:%' AND (hbu IS NULL OR hbu = '')").all();
    for (const a of conHbuNotas) {
      const m = a.notas.match(/HBU:(\w+)/);
      if (m) database.prepare("UPDATE animales SET hbu = ? WHERE id = ?").run(m[1], a.id);
    }
    if (conHbuNotas.length) console.log(`Migrados ${conHbuNotas.length} HBU desde notas`);
  } catch(e) {}
}

// ── INICIALIZAR BASES DE DATOS ────────────────────────────────────────────────
const DB_DIR = process.env.DB_DIR || "/data";
if (!fs.existsSync(DB_DIR)) fs.mkdirSync(DB_DIR, { recursive: true });

// Mapear: la DB actual (angus.db o amakaik.db) → angus_del_este.db
// Si existe el archivo viejo, usarlo; sino crear nuevo
const DB_LEGACY = process.env.DB_PATH || path.join(DB_DIR, "angus.db");

const databases = {};
for (const [key, campo] of Object.entries(CAMPOS)) {
  const dbPath = path.join(DB_DIR, campo.db_file);
  
  // Para angus_del_este: si no existe el nuevo pero existe el viejo, hacer symlink/copy
  if (key === 'angus_del_este' && !fs.existsSync(dbPath)) {
    if (fs.existsSync(DB_LEGACY)) {
      fs.copyFileSync(DB_LEGACY, dbPath);
      console.log(`DB migrada: ${DB_LEGACY} → ${dbPath}`);
    }
  }
  
  // Para angus_la_posta: si no existe pero existe videla.db, migrar
  if (key === 'angus_la_posta' && !fs.existsSync(dbPath)) {
    const videlaPath = path.join(DB_DIR, "videla.db");
    if (fs.existsSync(videlaPath)) {
      fs.copyFileSync(videlaPath, dbPath);
      console.log(`DB migrada: ${videlaPath} → ${dbPath}`);
    }
  }
  
  const database = new Database(dbPath);
  database.pragma("journal_mode = WAL");
  database.pragma("foreign_keys = ON");
  database.exec(SCHEMA_SQL);
  runMigrations(database);
  // Las tablas de los módulos van en TODAS las bases: cada campo tiene la suya.
  try { if (reproduccion) reproduccion.init(database); }
  catch(e) { console.log(`repro init ${key}:`, e.message); }
  databases[key] = database;
  
  const count = database.prepare("SELECT COUNT(*) as n FROM animales").get().n;
  console.log(`Campo ${campo.nombre} (${key}): ${dbPath} — ${count} animales`);
}

// Función para obtener DB por clave de campo
function getDB(campo) {
  return databases[campo] || databases[CAMPO_DEFAULT];
}

// Resolver campo desde número de WhatsApp (To de Twilio)
function campoFromWhatsApp(toNumber) {
  for (const [key, campo] of Object.entries(CAMPOS)) {
    if (campo.whatsapp && toNumber && campo.whatsapp === toNumber) return key;
  }
  return CAMPO_DEFAULT;
}

// Variable global `db` — se setea por request según el campo
let db = databases[CAMPO_DEFAULT];

// ── ANTHROPIC ─────────────────────────────────────────────────────────────────
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// ── TWILIO (multi-cuenta: una por campo) ─────────────────────────────────────
const twilioClients = {};
// Cliente Angus del Este (cuenta principal)
if (process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN) {
  twilioClients.angus_del_este = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN);
  console.log("Twilio Angus del Este configurado");
}
// Cliente Angus la Posta (cuenta secundaria)
if (process.env.TWILIO_SID_POSTA && process.env.TWILIO_TOKEN_POSTA) {
  twilioClients.angus_la_posta = twilio(process.env.TWILIO_SID_POSTA, process.env.TWILIO_TOKEN_POSTA);
  console.log("Twilio Angus la Posta configurado");
}
// Función para obtener el cliente correcto por campo
function getTwilioClient(campoKey) {
  return twilioClients[campoKey] || twilioClients.angus_del_este || null;
}
// Compatibilidad: twilioClient apunta al default
let twilioClient = twilioClients.angus_del_este || null;

// ── MIDDLEWARE: RESOLVER CAMPO ────────────────────────────────────────────────
// Para APIs REST: ?campo=angus_del_este o ?campo=angus_la_posta
// Para WhatsApp: se resuelve por el número To
app.use((req, res, next) => {
  // Si llega una lista (el frontend guarda los campos permitidos separados por
  // coma), se usa el primero.
  const pedido = String(req.query.campo || "").split(",")[0].trim();
  // Un campo desconocido NO puede caer en el default: mostraría datos de otro
  // establecimiento sin avisar. Se rechaza.
  if (pedido && !CAMPOS[pedido]) {
    return res.status(404).json({
      error: `El campo "${pedido}" no existe en este servidor. Campos disponibles: ${Object.keys(CAMPOS).join(", ")}.`
    });
  }
  const campoKey = pedido || CAMPO_DEFAULT;
  req.campoKey = campoKey;
  req.campoDB = getDB(campoKey);
  req.campoInfo = CAMPOS[campoKey] || CAMPOS[CAMPO_DEFAULT];
  // Setear db global para que todos los endpoints existentes funcionen sin cambios
  db = req.campoDB;
  campoActualKey = campoKey;   // lo usa ejecutarAccion para validar el RP único
  next();
});

// ── API CAMPOS ───────────────────────────────────────────────────────────────
app.get("/api/campos", (req, res) => {
  const lista = Object.entries(CAMPOS).map(([key, c]) => ({
    key,
    nombre: c.nombre,
    pais: c.pais,
    codigo_tipo: c.codigo_tipo,
    codigo: c.codigo,
    raza: c.raza,
    animales: databases[key].prepare("SELECT COUNT(*) as n FROM animales").get().n
  }));
  res.json(lista);
});

// ── HELPERS DB ────────────────────────────────────────────────────────────────
function getHistorial(database, usuario) {
  const row = database.prepare("SELECT historial FROM sesiones WHERE usuario = ?").get(usuario);
  return row ? JSON.parse(row.historial) : [];
}

function saveHistorial(database, usuario, historial) {
  // Truncar mensajes largos en el historial para evitar timeouts
  const reciente = historial.slice(-10).map(m => ({
    role: m.role,
    content: typeof m.content === 'string' && m.content.length > 500 ? m.content.substring(0, 500) + '...' : m.content
  }));
  database.prepare(`
    INSERT INTO sesiones (usuario, historial, updated_at) VALUES (?, ?, datetime('now'))
    ON CONFLICT(usuario) DO UPDATE SET historial = excluded.historial, updated_at = excluded.updated_at
  `).run(usuario, JSON.stringify(reciente));
}

function fmt(n) {
  return parseFloat(n || 0).toLocaleString("es-UY", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

// Normalizar chip: quitar espacios, prefijo 858000, ceros iniciales extra
function normalizarChip(chip) {
  if (!chip) return null;
  let c = String(chip).replace(/\s/g, '');
  // Quitar prefijo 858000 o 8580000
  c = c.replace(/^8580{3,4}/, '');
  // Quitar cero inicial si queda (ej: 057051498 → 57051498)
  c = c.replace(/^0+/, '') || c;
  return c;
}

function buscarAnimalPorChip(chip, soloActivos) {
  if (!chip) return null;
  const cn = normalizarChip(chip);
  const where = soloActivos ? "AND estado = 'ACTIVO'" : "";
  // Buscar por chip exacto o normalizado
  const all = db.prepare(`SELECT * FROM animales WHERE 1=1 ${where}`).all();
  return all.find(a => {
    if (!a.chip) return false;
    return normalizarChip(a.chip) === cn;
  }) || null;
}

function buscarAnimal(identificador) {
  if (!identificador) return null;
  let id = String(identificador).trim();
  // Limpiar: sacar "RP", "rp:", "la ", "el ", "vaca ", etc.
  id = id.replace(/^(?:RP[:\s]*|rp[:\s]*|la\s+|el\s+|vaca\s+|toro\s+|ternero\s+)/i, '').trim();
  if (!id) return null;
  
  // 1. RP exacto (activos)
  let animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?) AND estado = 'ACTIVO'").get(id);
  // 2. Con/sin prefijo ADE
  if (!animal && id.toUpperCase().startsWith('ADE')) {
    animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?) AND estado = 'ACTIVO'").get(id.substring(3));
  }
  if (!animal) {
    animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?) AND estado = 'ACTIVO'").get('ADE' + id);
  }
  // 3. Búsqueda parcial (ej: "219" encuentra "S219")
  if (!animal) {
    animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) LIKE LOWER(?) AND estado = 'ACTIVO' ORDER BY id DESC").get(`%${id}`);
  }
  // 4. Por chip normalizado
  if (!animal) animal = buscarAnimalPorChip(id, true);
  // 5. Por nombre de madre/padre (ej: "LUCUMA")
  if (!animal && id.length > 3 && isNaN(id)) {
    animal = db.prepare("SELECT * FROM animales WHERE (LOWER(rp) LIKE LOWER(?) OR LOWER(notas) LIKE LOWER(?)) AND estado = 'ACTIVO'").get(`%${id}%`, `%${id}%`);
  }
  return animal;
}

// Los RP se reusan: un animal dado de baja puede tener el mismo RP que uno
// activo. La baja es solo el registro de que estuvo — no compite con el que
// está en el campo. Por eso SIEMPRE gana el ACTIVO, y entre varios, el más
// reciente. Sin este orden SQLite devuelve el de menor rowid, que es el viejo.
const ORDEN_ANIMAL = " ORDER BY (estado = 'ACTIVO') DESC, id DESC";

function buscarAnimalTodos(identificador) {
  if (!identificador) return null;
  let id = String(identificador).trim();
  id = id.replace(/^(?:RP[:\s]*|rp[:\s]*|la\s+|el\s+|vaca\s+|toro\s+|ternero\s+)/i, '').trim();
  if (!id) return null;

  let animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?)" + ORDEN_ANIMAL).get(id);
  if (!animal && id.toUpperCase().startsWith('ADE')) {
    animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?)" + ORDEN_ANIMAL).get(id.substring(3));
  }
  if (!animal) animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?)" + ORDEN_ANIMAL).get('ADE' + id);
  if (!animal) animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) LIKE LOWER(?)" + ORDEN_ANIMAL).get(`%${id}`);
  if (!animal) animal = buscarAnimalPorChip(id, false);
  // NO buscar por id numérico — causa confusión con RP numéricos
  return animal || null;
}

// Todos los animales que responden a ese RP, activos primero.
// Sirve para avisar cuando hay más de uno antes de hacer algo irreversible.
function animalesConMismoRP(identificador) {
  if (!identificador) return [];
  let id = String(identificador).trim()
    .replace(/^(?:RP[:\s]*|rp[:\s]*|la\s+|el\s+|vaca\s+|toro\s+|ternero\s+)/i, '').trim();
  if (!id) return [];
  const variantes = [id, id.toUpperCase().startsWith('ADE') ? id.substring(3) : 'ADE' + id];
  const vistos = new Set(), out = [];
  for (const v of variantes) {
    db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?)" + ORDEN_ANIMAL).all(v)
      .forEach(a => { if (!vistos.has(a.id)) { vistos.add(a.id); out.push(a); } });
  }
  return out;
}

// Determinar contexto de pesada por edad del animal
function determinarContextoPesada(fechaNac, fechaPesada, categoria) {
  if (!fechaNac) {
    // Sin fecha nac: si es VACA o TORO → ADULTA, sino DESARROLLO
    if (categoria === 'VACA' || categoria === 'TORO') return 'ADULTA';
    return 'DESARROLLO';
  }
  const dias = Math.floor((new Date(fechaPesada) - new Date(fechaNac)) / (1000*60*60*24));
  if (dias <= 3) return 'NACIMIENTO';                     // mismo día o ±3 días
  if (dias >= 160 && dias <= 240) return 'DESTETE';       // 200 ± 40
  if (dias >= 355 && dias <= 445) return 'AÑO';           // 400 ± 45
  if (dias >= 515 && dias <= 605) return '18MESES';       // 560 ± 45 = 515-605
  if (dias > 730) return 'ADULTA';                        // > 2 años = adulta
  return 'DESARROLLO';
}

// Calcular GDP entre primer y último peso
function calcularGDP(animalId) {
  const pesadas = db.prepare("SELECT * FROM pesadas WHERE animal_id = ? ORDER BY fecha ASC").all(animalId);
  if (pesadas.length < 2) return null;
  const primera = pesadas[0];
  const ultima = pesadas[pesadas.length - 1];
  const dias = Math.floor((new Date(ultima.fecha) - new Date(primera.fecha)) / (1000*60*60*24));
  // Mínimo 7 días entre pesadas para GDP confiable
  if (dias < 7) return null;
  const gdp = (ultima.peso - primera.peso) / dias;
  // Rango biológico: -1 a 2.5 kg/día
  if (gdp > 2.5 || gdp < -1) return null;
  return gdp;
}

function getResumenRodeo() {
  const total = db.prepare("SELECT COUNT(*) as n FROM animales WHERE estado = 'ACTIVO'").get();
  const porCat = db.prepare("SELECT categoria, sexo, COUNT(*) as n FROM animales WHERE estado = 'ACTIVO' GROUP BY categoria, sexo ORDER BY categoria").all();
  const porPelo = db.prepare("SELECT pelo, COUNT(*) as n FROM animales WHERE estado = 'ACTIVO' GROUP BY pelo ORDER BY n DESC").all();
  return { total: total.n, por_categoria: porCat, por_pelo: porPelo };
}

// ── CONTEXTO IA ───────────────────────────────────────────────────────────────
function buildContexto() {
  const resumen = getResumenRodeo();
  const ultimasPesadas = db.prepare(`
    SELECT p.*, a.rp, a.categoria FROM pesadas p 
    JOIN animales a ON a.id = p.animal_id 
    ORDER BY p.created_at DESC LIMIT 10
  `).all();
  const ultimosSanidad = db.prepare(`
    SELECT s.*, a.rp FROM sanidad s 
    JOIN animales a ON a.id = s.animal_id 
    ORDER BY s.created_at DESC LIMIT 5
  `).all();

  // Stats reproductivas para contexto
  const serviciosStats = db.prepare(`
    SELECT temporada, resultado, tipo_servicio, COUNT(*) as n FROM servicios 
    GROUP BY temporada, resultado, tipo_servicio ORDER BY temporada DESC
  `).all();
  const lotesResumen = db.prepare(`
    SELECT l.nombre, l.potrero, COUNT(la.id) as n FROM lotes l 
    LEFT JOIN lote_animales la ON la.lote_id = l.id 
    LEFT JOIN animales a ON a.id = la.animal_id AND a.estado = 'ACTIVO'
    GROUP BY l.id ORDER BY l.nombre
  `).all();

  return `Asistente ganadero Angus del Este / Angus la Posta (UY). Español rioplatense, conciso.
HOY: ${new Date().toISOString().slice(0,10)}

ACCIONES (respondé SOLO JSON sin texto):
{"accion":"registrar_animal","rp":"","chip":"","fecha_nac":"","sexo":"M/H","pelo":"NEGRO/COLORADO","categoria":"TERNERO/RECRIA/VAQUILLONA/VACA/TORO","registro":"PP/SA/GENERAL","destino":"PLANTEL/VENTA","madre_rp":"","padre_rp":""}
{"accion":"registrar_pesada","rp":"","peso":0,"fecha":"YYYY-MM-DD"}
{"accion":"registrar_medicion","rp":"","tipo":"CE/CC/FRAME","valor":0,"fecha":"YYYY-MM-DD"}
{"accion":"registrar_servicio","rp":"","temporada":"2025","tipo_servicio":"IATF/NATURAL","semen_iatf":"","fecha_iatf":"","toro_natural":"","fecha_ingreso_toro":"","cc_pre":0}
{"accion":"resultado_tacto","rp":"","resultado":"PREÑADA_IATF/PREÑADA_TORO/VACIA","fecha_tacto":"","temporada":"2025"}
{"accion":"registrar_parto","madre_rp":"","ternero_rp":"","peso_nac":0,"sexo":"M/H","pelo":"","fecha":""}
{"accion":"registrar_sanidad","rp":"","lote":"","tipo":"VACUNA/TRATAMIENTO/DESPARASITACION","fecha":"","productos":[{"producto":"","dosis":0}]}
// SANIDAD — REGLAS DURAS, no se pueden saltear:
//  1. SIEMPRE rp (uno o varios separados por coma) o lote. Sin destinatario NO se registra: preguntá a quién.
//  2. SIEMPRE dosis numérica por producto (cc/ml/dosis). Sin dosis NO se registra: preguntá cuánto.
//  3. UN RENGLÓN POR PRODUCTO. Varios productos en el mismo mensaje → varios objetos en "productos", cada uno con su dosis.
//  4. Usá el nombre EXACTO de "Veterinarios en stock". Si no coincide ninguno, listá los disponibles y no registres.
// Ej: "al 2015 le di 5cc ivermectina, 5cc vit ADE y 5cc vit min" →
// {"accion":"registrar_sanidad","rp":"2015","tipo":"TRATAMIENTO","productos":[{"producto":"Ivermectina","dosis":5},{"producto":"Vitamina ADE","dosis":5},{"producto":"Vitamina Mineral","dosis":5}]}
// Ej: "desparasité el lote recria con 8cc de ivermectina" →
// {"accion":"registrar_sanidad","lote":"RECRIA","tipo":"DESPARASITACION","productos":[{"producto":"Ivermectina","dosis":8}]}
{"accion":"dar_baja","rp":"","motivo":"VENTA/MUERTE","fecha":""}
{"accion":"baja_lote","rps":["S219"],"motivo":"VENTA","fecha":""}
{"accion":"borrar_pesada","id":0}
{"accion":"borrar_sanidad","id":0}
{"accion":"borrar_servicio","id":0}
{"accion":"ficha_animal","rp":""}
{"accion":"ver_rodeo"}
{"accion":"ver_ultimos"}

CONSULTAS: {"accion":"consulta","tipo":"TIPO","temporada":"2025","anio":"2025","filtros":{}}
Tipos: servicio_resumen, servicio_detalle, servicio_por_toro, prenadas_hoy, tacto_resumen, tacto_detalle, vacias, fpp, paricion_resumen, paricion_detalle, evaluacion_toros, destete_resumen, destete_ranking, peso_por_padre, hijos_de_padre, hijos_de_madre, recria_estado, rodeo_composicion, lotes_estado, sin_lote, sanidad_cobertura, sanidad_historial, reproductivo_ciclo
- "hijos de MALAL" / "qué hijos tiene X" / "progenie de X" → hijos_de_padre con filtros.padre="MALAL" (LISTA los animales, NO promedios)
- "peso promedio por padre" / "ranking peso por padre" → peso_por_padre (PROMEDIOS)
- Respondé EXACTAMENTE lo que se pregunta, no cambies a otra consulta parecida.
temporada=año servicio/tacto. anio=año calendario pesadas/partos.

STOCK (insumos y alimento viven en IMPROLUX; acá solo genética y ración por lote):
{"accion":"crear_genetica","tipo":"PAJUELA","toro_nombre":"","cantidad":0,"costo_unitario":0}
{"accion":"usar_genetica","nombre":"","cantidad":1}
{"accion":"registrar_costo","rp":"","concepto":"SANIDAD","monto":0,"detalle":""}
{"accion":"ver_stock"}
{"accion":"asignar_racion","lote":"","producto":"","kg_dia":0,"modo":"TOTAL","hasta":""}
{"accion":"cortar_racion","lote":""}
{"accion":"ver_racion"}
{"accion":"ver_alertas"}
{"accion":"registrar_evento","rp":"","descripcion":"","tipo":"OBSERVACION"}
{"accion":"ver_eventos","rp":""}
{"accion":"texto","mensaje":"respuesta"}

REGLAS: ADE es prefijo (ADE2=2). Fecha YYYY-MM-DD, sin fecha→HOY. Tacto: PREÑADA_IATF/PREÑADA_TORO/VACIA. Se vincula al último servicio sin resultado. Parto: ±15d IATF+282=IATF, sino repaso. Solo hembras 12+m en servicio. Varios animales→1 JSON con array. Pregunta→consulta. Sin entender→texto preguntar. RACIÓN: "40 kg de racion al corral"→asignar_racion modo=TOTAL. "2 kg por cabeza"→modo=POR_ANIMAL. El producto sale del stock de IMPROLUX; si no lo nombran, usar el único alimento que haya. SANIDAD: usá el nombre EXACTO de \"Veterinarios en stock\". Si lo que dicen no coincide con ninguno, respondé texto listando los disponibles en vez de inventar el nombre.

DATOS:
Rodeo: ${JSON.stringify(resumen)}
Lotes: ${JSON.stringify(lotesResumen)}
Alimentos: ${JSON.stringify((() => { try { return db.prepare("SELECT producto,unidad,costo_unitario,stock_improlux FROM costeo_productos WHERE tipo='ALIMENTO'").all(); } catch(e) { return []; } })())}
Raciones activas: ${JSON.stringify((() => { try { return db.prepare(`SELECT l.nombre lote, d.producto, d.modo, d.kg_dia FROM costeo_dietas d JOIN lotes l ON l.id = d.lote_id WHERE d.activo = 1`).all(); } catch(e) { return []; } })())}
Veterinarios en stock: ${JSON.stringify((() => { try { return db.prepare("SELECT producto,unidad,stock_improlux FROM costeo_productos WHERE tipo='VETERINARIO'").all(); } catch(e) { return []; } })())}`;

}

// ── ESQUEMA PARA TOOL-USE (consultas libres a la DB) ──────────────────────────
const DB_SCHEMA_DOC = `ESQUEMA DE LA BASE (SQLite). Usá la herramienta consultar_datos para responder CUALQUIER pregunta analítica leyendo estos datos:

animales(id, rp, chip, fecha_nac, raza, registro[PP/SA/GENERAL/SENANGUS], sexo[MACHO/HEMBRA], pelo[NEGRO/COLORADO], categoria[TERNERO/RECRIA/VAQUILLONA/VACA/TORO/NOVILLO], destino[PLANTEL/VENTA], madre_rp, padre_rp, estado[ACTIVO/VENDIDO/MUERTO], fecha_salida, motivo_salida, hbu, notas)
pesadas(id, animal_id, fecha, peso, contexto[NACIMIENTO/DESTETE/AÑO/18MESES/DESARROLLO/ADULTA], gdp)
mediciones(id, animal_id, fecha, tipo[CE/ALTURA/CC/FRAME/DOCILIDAD], valor)
PROTOCOLO DE SERVICIO (módulo nuevo, tablas repro_*): las cuatro etapas son programación de toros (con RP, nombre y registro HBU/HBA), ejecución (sincronización, IATF, entrada y salida del toro de repaso), ecografía precoz que confirma la IATF, y ecografía final que cierra el servicio y adjudica el toro. Al nacer, el padre se asigna solo: hasta 15 días después de la fecha probable de la IATF es del toro de IATF, después es del repaso.
- Para registrar un parto por este protocolo usá {"accion":"registrar_parto_protocolo","madre_rp":"","rp_cria":"","fecha":"","peso_nac":0,"pelo":"","sexo":"M/H"}. Peso, pelo, RP y madre son OBLIGATORIOS: si falta alguno, pedíselo al usuario antes de registrar.
- Para ver cómo viene el servicio usá {"accion":"estado_protocolo"}.

REVISIÓN DE CONSISTENCIA: si el usuario pregunta por errores, inconsistencias, datos raros o si algo "no cuadra" en servicios, partos o crías, respondé con la acción {"accion":"revisar_consistencia"}. Si después de una revisión el usuario pide corregir, respondé {"accion":"corregir_inconsistencias"}. Detecta crías anotadas que no existen, vacas que ya parieron pero figuran preñadas, madres o padres contradictorios y fechas imposibles.

servicios(id, animal_id, temporada, tipo_servicio[IATF/NATURAL], semen_iatf, fecha_iatf, toro_natural, fecha_ingreso_toro, resultado[PREÑADA_IATF/PREÑADA_TORO/VACIA], cc_pre, cc_post, tacto_servicio, fecha_parto, ternero_rp, peso_nacimiento, peso_destete, sexo_cria, pelo_cria)
sanidad(id, animal_id, fecha, tipo[VACUNA/TRATAMIENTO/DESPARASITACION], producto, dosis)
lotes(id, nombre, descripcion, potrero)
lote_animales(id, lote_id, animal_id)
lecturas_lote(id, lote_id, fecha, total_lote, leidos, faltantes, novedades)
stock_genetica(id, tipo, toro_nombre, donante_nombre, cantidad)
costos(id, animal_id, concepto, monto, fecha, detalle)
eventos(id, animal_id, fecha, tipo, descripcion)

REGLAS PARA QUERIES:
- Solo SELECT. Para relacionar animal usá JOIN animales a ON a.id = X.animal_id.
- Para "hijos de un toro": WHERE a.padre_rp = 'NOMBRE'. Para "% preñez": contar servicios con MAX(id) GROUP BY animal_id.
- temporada = año de servicio (columna temporada en servicios). anio = año calendario (substr(fecha,1,4)).
- GDP válido: p.gdp > 0 AND p.gdp <= 2.5.
- Solo animales estado='ACTIVO' salvo que pregunten por vendidos/muertos.
- Devolvé la query lista para ejecutar, sin explicación.`;

// Guard: solo permite SELECT de lectura
function esQuerySegura(sql) {
  const s = sql.trim().toLowerCase();
  if (!s.startsWith("select") && !s.startsWith("with")) return false;
  // Bloquear cualquier statement de escritura o peligroso
  const prohibidas = /\b(insert|update|delete|drop|alter|create|replace|truncate|pragma|attach|detach|vacuum)\b/i;
  if (prohibidas.test(sql)) return false;
  // Bloquear multi-statement
  if (sql.replace(/;\s*$/, '').includes(';')) return false;
  return true;
}

function ejecutarConsultaSQL(sql) {
  if (!esQuerySegura(sql)) return { error: "Query no permitida (solo SELECT de lectura, sin múltiples statements)" };
  try {
    let q = sql.trim().replace(/;\s*$/, '');
    // Cap de filas para no explotar el contexto
    if (!/\blimit\b/i.test(q)) q += " LIMIT 200";
    const rows = db.prepare(q).all();
    return { filas: rows.length, datos: rows };
  } catch(e) {
    return { error: e.message };
  }
}

// Definición de la herramienta para la API
const HERRAMIENTA_CONSULTA = {
  name: "consultar_datos",
  description: "Ejecuta una query SQL de SOLO LECTURA (SELECT) sobre la base de datos ganadera para responder preguntas analíticas del usuario. Usala siempre que necesites datos que no estén en el contexto (rankings, listados, promedios, conteos, historiales, etc).",
  input_schema: {
    type: "object",
    properties: {
      sql: { type: "string", description: "Query SELECT válida de SQLite. Una sola sentencia, sin punto y coma final." }
    },
    required: ["sql"]
  }
};


// El diario (lluvias, acontecimientos) vive en el sistema financiero de la
// empresa. Se lo damos a la IA como herramienta en vez de adivinar con
// palabras clave cuándo el usuario está hablando de lluvia.
const HERRAMIENTA_DIARIO_LEER = {
  name: "consultar_diario",
  description: "Lee el diario de campo de esta empresa: lluvias registradas (con fecha y milímetros) y acontecimientos. Usala para CUALQUIER pregunta sobre lluvia, precipitaciones, milímetros, agua caída o el diario del campo, sin importar cómo esté formulada. Devuelve los registros del año pedido y el total por mes.",
  input_schema: {
    type: "object",
    properties: {
      anio: { type: "string", description: "Año a consultar, ej '2026'. Si no lo aclaran, el año en curso." }
    }
  }
};

const HERRAMIENTA_DIARIO_ESCRIBIR = {
  name: "registrar_diario",
  description: "Registra una lluvia o un acontecimiento en el diario de campo. Usala cuando el usuario informe milímetros caídos o quiera anotar algo del campo. Si ya hay una lluvia cargada ese día, la corrige en vez de duplicarla.",
  input_schema: {
    type: "object",
    properties: {
      tipo: { type: "string", enum: ["LLUVIA", "ACONTECIMIENTO"], description: "LLUVIA si informan milímetros." },
      mm: { type: "number", description: "Milímetros caídos. Sólo para tipo LLUVIA." },
      fecha: { type: "string", description: "Fecha AAAA-MM-DD. Si dicen 'ayer' calculala; si no aclaran, hoy." },
      detalle: { type: "string", description: "Texto libre opcional." }
    },
    required: ["tipo"]
  }
};

// Ejecuta las herramientas del diario y devuelve texto para el modelo.
async function ejecutarHerramientaDiario(nombre, input, campoKey) {
  try {
    if (nombre === "consultar_diario") {
      const anio = input.anio || new Date().getFullYear().toString();
      const d = await leerDiario(campoKey, anio);
      const lluvias = d.registros.filter(r => r.tipo === "LLUVIA" && r.mm)
        .map(r => ({ fecha: r.fecha, mm: r.mm }));
      const otros = d.registros.filter(r => r.tipo !== "LLUVIA")
        .map(r => ({ fecha: r.fecha, titulo: r.titulo, detalle: r.detalle }));
      return JSON.stringify({
        campo: d.campo, anio,
        lluvias, total_mm_anio: d.total, por_mes: d.porMes,
        acontecimientos: otros.slice(0, 30),
        hoy: new Date().toISOString().slice(0, 10)
      });
    }
    if (nombre === "registrar_diario") {
      if (input.tipo === "LLUVIA") {
        if (!(input.mm > 0)) return JSON.stringify({ error: "Falta la cantidad de milímetros" });
        return JSON.stringify({ ok: true, mensaje: await registrarLluviaImprolux(input.mm, campoKey, input.detalle || "", input.fecha) });
      }
      const { url, campo } = await resolverCampo(campoKey);
      const f = input.fecha || new Date().toISOString().slice(0, 10);
      const r = await fetch(`${url}/api/diario`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ campo, fecha: f, tipo: "ACONTECIMIENTO", titulo: input.detalle || "", detalle: input.detalle || "" })
      });
      const d = await r.json().catch(() => ({}));
      return JSON.stringify(d.ok || d.id
        ? { ok: true, mensaje: `Anotado en el diario de ${campo} (${f}).` }
        : { error: "No se pudo guardar" });
    }
  } catch (e) {
    return JSON.stringify({ error: String(e.message).slice(0, 120) });
  }
  return JSON.stringify({ error: "Herramienta desconocida" });
}

const HERRAMIENTAS = [HERRAMIENTA_CONSULTA, HERRAMIENTA_DIARIO_LEER, HERRAMIENTA_DIARIO_ESCRIBIR];

// ── EJECUTAR ACCIÓN ───────────────────────────────────────────────────────────
const IMPROLUX_URL = (process.env.IMPROLUX_URL || "https://improlux-bot-production.up.railway.app").replace(/\/$/, "");

// Detecta "llovió X mm" / "cayeron X" / "lluvia de X" y devuelve {mm, fecha} o null.
function detectarLluvia(body) {
  const low = (body || "").toLowerCase();
  if (!/(lluvia|llovi[óo]|llovieron|cayeron|cay[óo]|precipit|mm\b|mil[ií]metros)/.test(low)) return null;
  const m = body.match(/(\d+(?:[.,]\d+)?)\s*mm\b/i)
         || body.match(/(?:llovi[óo]|llovieron|cayeron|cay[óo]|lluvia(?:\s+de)?|precipit\w*)\s*(?:de\s*)?(\d+(?:[.,]\d+)?)/i);
  if (!m) return null;
  const mm = parseFloat(m[1].replace(',', '.'));
  if (!(mm > 0)) return null;
  let fecha = new Date();
  if (/\bayer\b/.test(low)) fecha = new Date(Date.now() - 86400000);
  return { mm, fecha: fecha.toISOString().slice(0, 10) };
}


// ── CAMPO ACTIVO POR CONVERSACIÓN ────────────────────────────────────────────
// Con varios campos en la misma empresa, el bot tiene que saber en cuál está
// trabajando. Se recuerda por usuario y se puede cambiar hablando.

function tablaCampoSesion(base) {
  base.exec(`CREATE TABLE IF NOT EXISTS sesion_campo (
    usuario TEXT PRIMARY KEY, campo TEXT NOT NULL, updated_at TEXT DEFAULT (datetime('now')))`);
}

function campoDeSesion(usuario) {
  try {
    const base = databases[CAMPO_DEFAULT];
    tablaCampoSesion(base);
    const r = base.prepare("SELECT campo FROM sesion_campo WHERE usuario=?").get(usuario);
    return r ? r.campo : null;
  } catch (e) { return null; }
}

function fijarCampoSesion(usuario, campoKey) {
  try {
    const base = databases[CAMPO_DEFAULT];
    tablaCampoSesion(base);
    base.prepare(`INSERT INTO sesion_campo (usuario,campo,updated_at) VALUES (?,?,datetime('now'))
                  ON CONFLICT(usuario) DO UPDATE SET campo=excluded.campo, updated_at=datetime('now')`)
      .run(usuario, campoKey);
  } catch (e) {}
}

// Busca el nombre de un campo dentro del mensaje. Tolera acentos y artículos:
// "el triunfo", "triunfo", "la guagua", "guagua".
function campoMencionado(texto, permitidos) {
  const limpio = String(texto || "").toLowerCase()
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  for (const key of permitidos) {
    const cfg = CAMPOS[key];
    if (!cfg) continue;
    const nom = cfg.nombre.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
    if (limpio.includes(nom)) return key;
    // Sin artículo: "El Triunfo" → "triunfo"
    const corto = nom.replace(/^(el|la|los|las|campo)\s+/, "");
    if (corto.length > 3 && new RegExp(`\\b${corto}\\b`).test(limpio)) return key;
  }
  return null;
}

// Un mensaje que escribe datos y no dice el campo es peligroso: se pregunta.
function esOperacionDeCarga(texto) {
  return /(carg|anot|registr|dar de alta|nuevo animal|nacio|nació|pes[oó]|destet|vacun|apliqu|servicio|parto|compr|vend)/i
    .test(String(texto || ""));
}


// ── RP ÚNICO POR EMPRESA ─────────────────────────────────────────────────────
// Cada campo tiene su propia base, así que sin esto el mismo RP podría existir
// en dos campos de la misma empresa. Al mover hacienda o al buscar un animal,
// eso vuelve ambiguo el dato.

function camposHermanos(campoKey) {
  const emp = (CAMPOS[campoKey] || {}).empresa;
  if (!emp) return [];
  return Object.keys(CAMPOS).filter(k => k !== campoKey && CAMPOS[k].empresa === emp);
}

// Devuelve en qué campo hermano ya existe ese RP, o null.
function rpEnOtroCampo(campoKey, rp) {
  if (!rp) return null;
  for (const otro of camposHermanos(campoKey)) {
    try {
      const base = getDB(otro);
      const a = base.prepare("SELECT rp, estado FROM animales WHERE upper(rp) = upper(?)").get(String(rp).trim());
      if (a) return { campo: otro, nombre: CAMPOS[otro].nombre, estado: a.estado };
    } catch (e) {}
  }
  return null;
}

// Para importaciones: revisa muchos de una vez.
function rpsEnOtrosCampos(campoKey, rps) {
  const choques = [];
  const hermanos = camposHermanos(campoKey);
  if (!hermanos.length) return choques;
  for (const otro of hermanos) {
    try {
      const base = getDB(otro);
      const st = base.prepare("SELECT rp FROM animales WHERE upper(rp) = upper(?)");
      for (const rp of rps) {
        if (!rp) continue;
        if (st.get(String(rp).trim())) choques.push({ rp, campo: otro, nombre: CAMPOS[otro].nombre });
      }
    } catch (e) {}
  }
  return choques;
}

// ── DIARIO DE CAMPO ──────────────────────────────────────────────────────────
// El diario (lluvias, acontecimientos) vive en el sistema financiero de cada
// empresa. El ganadero no lo duplica: lo lee y lo escribe por API, así el dato
// queda en un solo lugar y da igual desde qué lado preguntes.

const _campoCache = {};

function urlDiario(campoKey) {
  const cfg = CAMPOS[campoKey] || CAMPOS[CAMPO_DEFAULT];
  return {
    url: (cfg.bot_financiero || IMPROLUX_URL).replace(/\/$/, ""),
    campo: cfg.campo_improlux || "LA AMISTAD"
  };
}

// El nombre del campo tiene que coincidir con el que usa el sistema financiero,
// si no el dato se guarda donde nadie lo mira. Se pregunta y se cachea.
async function resolverCampo(campoKey) {
  const { url, campo } = urlDiario(campoKey);
  const cache = _campoCache[campoKey];
  if (cache && Date.now() - cache.t < 600000) return { url, campo: cache.campo };
  try {
    const r = await fetch(`${url}/api/campos`);
    const lista = await r.json();
    if (Array.isArray(lista)) {
      const exacto = lista.find(c => String(c.nombre).toUpperCase() === campo.toUpperCase());
      if (exacto) {
        _campoCache[campoKey] = { campo: exacto.nombre, t: Date.now() };
        return { url, campo: exacto.nombre };
      }
      // No existe: se crea. Caer en el primero mezclaría el stock y el diario
      // de dos establecimientos distintos, que es peor que fallar.
      try {
        const cr = await fetch(`${url}/api/campos`, {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ nombre: campo })
        });
        const d = await cr.json().catch(() => ({}));
        if (d.nombre || d.id) {
          console.log(`[financiero] campo "${campo}" creado`);
          _campoCache[campoKey] = { campo, t: Date.now() };
          return { url, campo };
        }
      } catch (e) {}
      console.warn(`[financiero] no pude crear el campo "${campo}"`);
    }
  } catch (e) {}
  return { url, campo };
}

async function leerDiario(campoKey, anio) {
  const { url, campo } = await resolverCampo(campoKey);
  const q = new URL(`${url}/api/diario`);
  q.searchParams.set("campo", campo);
  if (anio) q.searchParams.set("anio", anio);
  const r = await fetch(q.toString());
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const d = await r.json();
  return {
    campo,
    registros: d.registros || [],
    porMes: d.lluvias_por_mes || [],
    total: d.total_mm || 0
  };
}

const MESES = ["enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre"];

// Responde cuánto llovió, sin depender de que la IA sepa consultar otro sistema.
async function consultarLluvia(campoKey, texto) {
  const hoy = new Date();
  const anio = hoy.getFullYear();
  let d;
  try { d = await leerDiario(campoKey, anio); }
  catch (e) { return `⚠️ No pude leer el diario de campo (${String(e.message).slice(0, 50)}).`; }

  const lluvias = d.registros.filter(r => r.tipo === "LLUVIA" && r.mm);
  if (!lluvias.length) return `🌧️ No hay lluvias registradas en ${d.campo} este año.`;

  const bajo = (texto || "").toLowerCase();
  const mesPedido = MESES.findIndex(m => bajo.includes(m));
  const mesClave = mesPedido >= 0
    ? `${anio}-${String(mesPedido + 1).padStart(2, "0")}`
    : hoy.toISOString().slice(0, 7);

  const delMes = lluvias.filter(r => String(r.fecha).startsWith(mesClave));
  const nombreMes = MESES[parseInt(mesClave.slice(5, 7)) - 1];

  // ¿Pregunta por la última, nomás?
  if (/[uú]ltim|reciente|cu[aá]ndo llovi/.test(bajo)) {
    const u = lluvias[0];
    const dias = Math.round((Date.now() - new Date(u.fecha).getTime()) / 86400000);
    const cuando = dias <= 0 ? "hoy" : dias === 1 ? "ayer" : `hace ${dias} días`;
    return `🌧️ La última lluvia en ${d.campo} fue el ${u.fecha} (${cuando}): ${u.mm} mm.`;
  }

  // ¿Pide el año, o un mes puntual?
  if (/\ba[ñn]o\b|anual|acumulado del a[ñn]o|en el a[ñn]o/.test(bajo)) {
    const total = lluvias.reduce((a, b) => a + Number(b.mm || 0), 0);
    let msg = `🌧️ *${d.campo} — ${anio}*\nTotal: ${total.toFixed(0)} mm en ${lluvias.length} días.\n`;
    d.porMes.filter(m => m.mm > 0).forEach(m => {
      msg += `\n${MESES[parseInt(m.mes.slice(5, 7)) - 1]}: ${Number(m.mm).toFixed(0)} mm (${m.dias} día${m.dias > 1 ? "s" : ""})`;
    });
    return msg;
  }

  if (!delMes.length) {
    const ult = lluvias[0];
    return `🌧️ En ${nombreMes} todavía no llovió en ${d.campo}.\nLa última fue el ${ult.fecha}: ${ult.mm} mm.`;
  }

  const total = delMes.reduce((a, b) => a + Number(b.mm || 0), 0);
  let msg = `🌧️ *${d.campo} — ${nombreMes}*\n${total.toFixed(0)} mm en ${delMes.length} día${delMes.length > 1 ? "s" : ""}.\n`;
  delMes.slice(0, 12).forEach(r => { msg += `\n${r.fecha}: ${r.mm} mm`; });
  if (delMes.length > 12) msg += `\n…y ${delMes.length - 12} más`;
  return msg;
}

async function registrarLluviaImprolux(mm, campoKey, detalle, fecha) {
  const { url, campo } = await resolverCampo(campoKey);
  const f = fecha || new Date().toISOString().slice(0, 10);
  let corregido = null;
  try {
    // Si ya había una lluvia ese día, se corrige en vez de sumar otra fila.
    try {
      const previo = await leerDiario(campoKey, f.slice(0, 4));
      const dup = previo.registros.find(r => r.tipo === "LLUVIA" && r.fecha === f);
      if (dup) {
        corregido = dup.mm;
        await fetch(`${url}/api/diario/${dup.id}`, { method: "DELETE" });
      }
    } catch (e) {}

    const resp = await fetch(`${url}/api/diario`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ campo, fecha: f, tipo: "LLUVIA", mm, detalle: detalle || "" })
    });
    const d = await resp.json().catch(() => ({}));
    if (!(d.ok || d.id)) return `⚠️ No pude registrar la lluvia: ${d.error || ("HTTP " + resp.status)}`;

    let extra = "";
    try {
      const dj = await leerDiario(campoKey, f.slice(0, 4));
      const m = dj.porMes.find(x => x.mes === f.slice(0, 7));
      if (m && m.mm) extra = `\n📊 Van ${Number(m.mm).toFixed(0)} mm en el mes, en ${m.dias} día${m.dias > 1 ? "s" : ""}.`;
    } catch (e) {}

    const cabeza = corregido !== null && corregido !== mm
      ? `🌧️ Corregí la lluvia del ${f} en ${campo}: ${corregido} → ${mm} mm.`
      : `🌧️ Lluvia registrada en el diario de ${campo}: ${mm} mm (${f}).`;
    return `${cabeza}${extra}`;
  } catch (e) {
    return `⚠️ No pude conectar con el diario de ${campo} (${String(e.message).slice(0, 60)}).`;
  }
}

// Una consulta de lluvia no trae milímetros; un registro sí. Esa es la señal.
// Si habla de lluvia y no trae milímetros, es una pregunta. Antes se exigía
// además alguna palabra tipo "cuánta" o "acumulado", y algo tan común como
// "lluvias de agosto?" quedaba afuera y terminaba contestando que no sabe.
function esConsultaLluvia(body) {
  const b = (body || "").toLowerCase();
  if (!/(lluvia|llovi|precipitac|pluviom|mil[ií]metro|\bmm\b|agua ca[ií]da)/.test(b)) return false;
  return !detectarLluvia(body);   // si trae mm, es un registro
}

// Descuenta del stock de IMPROLUX esperando la respuesta (para interpretar/repreguntar).
async function descontarStockImprolux(accion) {
  const dosisNum = parseFloat(accion.dosis);
  if (!accion.producto || !(dosisNum > 0)) return "";
  try {
    const resp = await fetch(`${IMPROLUX_URL}/api/stock/aplicar`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ producto: accion.producto, cantidad: dosisNum, rp: accion.rp, fecha: accion.fecha, detalle: accion.tipo })
    });
    const d = await resp.json().catch(() => ({}));
    if (d.ambiguo) {
      const ops = (d.opciones || []).map(o => `${o.nombre} (${o.cantidad} ${o.unidad})`).join(" · ");
      return `\n\n⚠️ Hay varios parecidos a "${accion.producto}": ${ops}.\nRepetí la sanidad con el nombre exacto para descontar bien.`;
    }
    if (d.ok) {
      let s = `\n📦 Stock: -${dosisNum} de ${d.producto} (quedan ${d.restante} ${d.unidad})`;
      // Si IMPROLUX devuelve el costo, se carga contra el animal en el acto.
      // Si no lo devuelve, la valorización queda a cargo de la lista de precios
      // sincronizada desde IMPROLUX (Costos → Ración y sanidad).
      const costoTotal = (d.costo_total != null) ? parseFloat(d.costo_total)
        : (d.costo_unitario != null) ? parseFloat(d.costo_unitario) * dosisNum
        : (d.costo != null) ? parseFloat(d.costo) * dosisNum : null;
      if (costoTotal > 0 && accion.rp) {
        try {
          const an = buscarAnimalTodos(accion.rp);
          if (an) {
            const marca = `costeo:improlux ${d.producto} ${dosisNum}${d.unidad || ''}`;
            db.prepare("INSERT INTO costos (animal_id,fecha,concepto,detalle,monto,moneda) VALUES (?,?,'SANIDAD',?,?,'USD')")
              .run(an.id, accion.fecha || new Date().toISOString().slice(0,10), marca, costoTotal);
            s += `\n💰 Costo cargado a ${an.rp}: US$ ${costoTotal.toFixed(2)}`;
          }
        } catch (e) { console.error('costo sanidad:', e.message); }
      }
      if (d.aviso) s += `\n${d.aviso}`;
      return s;
    }
    return `\n⚠️ No descontó del stock: ${d.error || "producto no encontrado"}`;
  } catch (e) {
    return `\n⚠️ No pude conectar con el stock de IMPROLUX (${String(e.message).slice(0, 60)}).`;
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// SANIDAD — un renglón por producto, siempre contra animales concretos.
// Sin destinatario o sin dosis no se registra nada: el costo tiene que caer
// sobre alguien, si no el costeo por animal queda incompleto.
// ══════════════════════════════════════════════════════════════════════════════
function normalizarProductos(a) {
  if (Array.isArray(a.productos) && a.productos.length) {
    return a.productos.map(p => ({
      producto: String(p.producto || p.nombre || "").trim(),
      dosis: parseFloat(p.dosis) || 0
    }));
  }
  if (a.producto) return [{ producto: String(a.producto).trim(), dosis: parseFloat(a.dosis) || 0 }];
  return [];
}

function resolverDestinatarios(a) {
  // Lote entero
  if (a.lote) {
    const l = db.prepare("SELECT * FROM lotes WHERE LOWER(nombre) = LOWER(?)").get(String(a.lote).trim())
           || db.prepare("SELECT * FROM lotes WHERE LOWER(nombre) LIKE LOWER(?)").get(`%${String(a.lote).trim()}%`);
    if (!l) {
      const todos = db.prepare("SELECT nombre FROM lotes ORDER BY nombre").all().map(x => x.nombre);
      return { error: `❌ No encontré el lote "${a.lote}".\nLotes: ${todos.join(", ") || "ninguno"}` };
    }
    const an = db.prepare(`SELECT a.id, a.rp FROM animales a JOIN lote_animales la ON la.animal_id = a.id
      WHERE la.lote_id = ? AND a.estado = 'ACTIVO'`).all(l.id);
    if (!an.length) return { error: `❌ El lote ${l.nombre} está vacío.` };
    return { animales: an, destino: `lote ${l.nombre}` };
  }
  // RP sueltos, o la lista de registros del formato viejo
  let rps = [];
  if (Array.isArray(a.registros)) rps = a.registros.map(r => r.rp).filter(Boolean);
  else if (Array.isArray(a.rps)) rps = a.rps;
  else if (a.rp) rps = String(a.rp).split(/[,\s]+/).filter(Boolean);
  if (!rps.length) return { error: "❌ ¿A qué animal o lote? Necesito el RP o el nombre del lote para poder cargar el costo." };

  const animales = [], faltan = [];
  for (const rp of rps) {
    const an = buscarAnimalTodos(String(rp).trim());
    if (an) animales.push({ id: an.id, rp: an.rp }); else faltan.push(rp);
  }
  if (!animales.length) return { error: `❌ No encontré: ${faltan.join(", ")}` };
  return { animales, faltan, destino: animales.length === 1 ? `RP ${animales[0].rp}` : `${animales.length} animales` };
}

async function procesarSanidad(accion) {
  const hoy = new Date().toISOString().split("T")[0];
  const fecha = accion.fecha || hoy;
  const tipo = accion.tipo || "TRATAMIENTO";

  const dest = resolverDestinatarios(accion);
  if (dest.error) return dest.error;

  const productos = normalizarProductos(accion);
  if (!productos.length) return "❌ ¿Qué producto aplicaste?";
  const sinDosis = productos.filter(p => !(p.dosis > 0)).map(p => p.producto || "?");
  if (sinDosis.length)
    return `❌ Falta la dosis de: ${sinDosis.join(", ")}.\nSin dosis no puedo descontar stock ni calcular el costo por animal.`;

  const ins = db.prepare("INSERT INTO sanidad (animal_id, fecha, tipo, producto, dosis, notas) VALUES (?,?,?,?,?,?)");
  const insCosto = db.prepare("INSERT INTO costos (animal_id,fecha,concepto,detalle,monto,moneda) VALUES (?,?,'SANIDAD',?,?,'USD')");

  const lineas = [];
  let costoTotalGeneral = 0;

  for (const p of productos) {
    // Un registro de sanidad por animal y por producto
    const tx = db.transaction(() => {
      dest.animales.forEach(a => ins.run(a.id, fecha, tipo, p.producto, String(p.dosis), dest.destino));
    });
    tx();

    const totalAplicado = p.dosis * dest.animales.length;
    let linea = `  • ${p.producto} · ${p.dosis} × ${dest.animales.length}`;

    try {
      const resp = await fetch(`${IMPROLUX_URL}/api/stock/aplicar`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ producto: p.producto, cantidad: totalAplicado, fecha,
          rp: dest.animales.length === 1 ? dest.animales[0].rp : undefined,
          detalle: `${tipo} · ${dest.destino}` })
      });
      const d = await resp.json().catch(() => ({}));

      if (d.ambiguo) {
        linea += `\n    ⚠️ Varios parecidos: ${(d.opciones || []).map(o => o.nombre).join(" · ")}. No descontó.`;
      } else if (d.ok) {
        const costoTotal = (d.costo_total != null) ? parseFloat(d.costo_total)
          : (d.costo_unitario != null) ? parseFloat(d.costo_unitario) * totalAplicado : 0;
        if (costoTotal > 0) {
          const porCabeza = costoTotal / dest.animales.length;
          const tx2 = db.transaction(() => {
            dest.animales.forEach(a => insCosto.run(a.id, fecha,
              `costeo:improlux ${d.producto} ${p.dosis}${d.unidad || ''}`, porCabeza));
          });
          tx2();
          costoTotalGeneral += costoTotal;
          linea += ` = ${totalAplicado} ${d.unidad || ''} · US$ ${costoTotal.toFixed(2)} (US$ ${porCabeza.toFixed(2)} c/u)`;
        } else {
          linea += ` = ${totalAplicado} ${d.unidad || ''} · sin precio cargado`;
        }
        linea += `\n    📦 Quedan ${d.restante} ${d.unidad || ''}`;
        if (d.negativo) linea += " ⚠️ NEGATIVO";
        if (d.interpretado_de) linea += `\n    📝 Interpreté "${d.interpretado_de}" → ${d.producto}`;
      } else {
        linea += `\n    ⚠️ No está en el stock de IMPROLUX (${d.error || "no encontrado"}). Se registró igual, sin costo.`;
      }
    } catch (e) {
      linea += `\n    ⚠️ Sin conexión con IMPROLUX (${String(e.message).slice(0, 40)}). Se registró sin costo.`;
    }
    lineas.push(linea);
  }

  let r = `💉 *${tipo}* — ${dest.destino} · ${fecha}\n${lineas.join("\n")}`;
  if (productos.length > 1 && costoTotalGeneral > 0)
    r += `\n\n💰 Total: US$ ${costoTotalGeneral.toFixed(2)} · US$ ${(costoTotalGeneral / dest.animales.length).toFixed(2)} por animal`;
  if (dest.faltan && dest.faltan.length) r += `\n\n⚠️ No encontré: ${dest.faltan.join(", ")}`;
  return r;
}

async function ejecutarAccionAsync(accion) {
  if (accion.accion === "registrar_sanidad" || accion.accion === "sanidad_lote")
    return await procesarSanidad(accion);
  return ejecutarAccion(accion);
}

function ejecutarAccion(accion) {
  const hoy = new Date().toISOString().split("T")[0];

  // REGISTRAR ANIMAL
  if (accion.accion === "registrar_animal") {
    const { rp, chip, fecha_nac, sexo, pelo, categoria, registro, destino, madre_rp, padre_rp, notas } = accion;
    if (!rp || !sexo) return "❌ Faltan datos: necesito al menos RP y sexo.";
    // El RP tiene que ser único en toda la empresa, no sólo en este campo.
    {
      const ch = rpEnOtroCampo(campoActualKey, rp);
      if (ch) return `❌ El RP ${rp} ya existe en *${ch.nombre}*${ch.estado !== 'ACTIVO' ? ` (${ch.estado})` : ''}.\nUsá otro RP o movelo desde allá.`;
    }
    try {
      const r = db.prepare(`
        INSERT INTO animales (chip, rp, fecha_nac, sexo, pelo, categoria, registro, destino, madre_rp, padre_rp, notas, fecha_ingreso)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(chip || null, rp, fecha_nac || null, sexo.toUpperCase(), pelo || null, categoria || "RECRIA", registro || null, destino || "PLANTEL", madre_rp || null, padre_rp || null, notas || null, hoy);
      return `✅ Animal registrado!\n🏷️ RP: ${rp}${chip ? ` | CHIP: ${chip}` : ""}\n${sexo} ${pelo || ""} | ${categoria || "RECRIA"} | ${registro || ""} | ${destino || "PLANTEL"}\n${madre_rp ? `👩 Madre: ${madre_rp}` : ""}${padre_rp ? ` | 👨 Padre: ${padre_rp}` : ""}`;
    } catch (e) {
      if (e.message.includes("UNIQUE")) return `⚠️ Ya existe un animal con chip ${chip}.`;
      return `❌ Error: ${e.message}`;
    }
  }

  // REGISTRAR PESADA
  if (accion.accion === "registrar_pesada") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    const peso = parseFloat(accion.peso);
    if (!peso) return "❌ Falta el peso.";

    // Determinar fecha: si es NACIMIENTO usar fecha_nac del animal
    let fecha = accion.fecha || hoy;
    if (accion.contexto === 'NACIMIENTO' && animal.fecha_nac && (!accion.fecha || accion.fecha === hoy)) {
      fecha = animal.fecha_nac;
    }

    // Auto-detectar contexto por edad
    let contexto = accion.contexto || determinarContextoPesada(animal.fecha_nac, fecha, animal.categoria);

    // Anti-duplicado
    const existe = db.prepare("SELECT id FROM pesadas WHERE animal_id = ? AND fecha = ? AND peso = ?").get(animal.id, fecha, peso);
    if (existe) return `⚠️ Ya existe pesada de ${peso}kg el ${fecha} para RP ${animal.rp}.`;

    db.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto, notas) VALUES (?, ?, ?, ?, ?)")
      .run(animal.id, fecha, peso, contexto, accion.notas || null);

    const gdp = calcularGDP(animal.id);
    if (gdp !== null) {
      const lastId = db.prepare("SELECT id FROM pesadas WHERE animal_id = ? ORDER BY created_at DESC LIMIT 1").get(animal.id);
      if (lastId) db.prepare("UPDATE pesadas SET gdp = ? WHERE id = ?").run(gdp, lastId.id);
    }

    let resp = `✅ Pesada registrada!\n🏷️ RP ${animal.rp} | ${fmt(peso)} kg\n📅 ${fecha} | 📋 ${contexto}`;
    if (gdp !== null) resp += `\n📈 GDP promedio: ${fmt(gdp * 1000)} g/día`;
    return resp;
  }

  // PESADA LOTE
  if (accion.accion === "pesada_lote") {
    if (!Array.isArray(accion.pesadas)) return "❌ Formato inválido.";
    let ok = 0, errores = [];
    const stmt = db.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto, notas) VALUES (?, ?, ?, ?, ?)");
    for (const p of accion.pesadas) {
      const animal = buscarAnimal(p.rp);
      if (!animal) { errores.push(p.rp); continue; }
      stmt.run(animal.id, hoy, parseFloat(p.peso), accion.contexto || "DESARROLLO", accion.notas || null);
      ok++;
    }
    let resp = `✅ Pesada de lote: ${ok} registradas`;
    if (errores.length) resp += `\n⚠️ No encontrados: ${errores.join(", ")}`;
    return resp;
  }

  // REGISTRAR MEDICIÓN
  if (accion.accion === "registrar_medicion") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    const fecha = accion.fecha || hoy;
    db.prepare("INSERT INTO mediciones (animal_id, fecha, tipo, valor, notas) VALUES (?, ?, ?, ?, ?)")
      .run(animal.id, fecha, accion.tipo, parseFloat(accion.valor), accion.notas || null);
    const unidades = { CE: "cm", ALTURA: "cm", FRAME: "", DOCILIDAD: "/5", CC: "/10" };
    return `✅ Medición registrada!\n🏷️ RP ${animal.rp} | ${accion.tipo}: ${accion.valor}${unidades[accion.tipo] || ""} | 📅 ${fecha}`;
  }

  // REGISTRAR ECOGRAFÍA
  if (accion.accion === "registrar_ecografia") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    db.prepare(`
      INSERT INTO ecografias (animal_id, fecha_medicion, dias_vida, pct_gi, aob, gd, gc, estado, notas)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(animal.id, accion.fecha_medicion || hoy, accion.dias_vida || null, accion.pct_gi || null,
           accion.aob || null, accion.gd || null, accion.gc || null, accion.estado || null, accion.notas || null);
    return `✅ Ecografía registrada!\n🏷️ RP ${animal.rp}\n🥩 AOB: ${accion.aob} cm² | GD: ${accion.gd}mm | GC: ${accion.gc}mm | %GI: ${accion.pct_gi}%\n📋 ${accion.estado || ""}`;
  }

  // REGISTRAR SERVICIO
  if (accion.accion === "registrar_servicio") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    db.prepare(`
      INSERT INTO servicios (animal_id, temporada, tipo_servicio, semen_iatf, toro_natural, fecha_iatf, tacto_pre, cc_pre)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(animal.id, accion.temporada || null, accion.tipo_servicio || null, accion.semen_iatf || null,
           accion.toro_natural || null, accion.fecha_iatf || null, accion.tacto_pre || null, accion.cc_pre || null);
    return `✅ Servicio registrado!\n🏷️ RP ${animal.rp} | ${accion.tipo_servicio || ""}${accion.semen_iatf ? ` | Semen: ${accion.semen_iatf}` : ""}${accion.toro_natural ? ` | Toro: ${accion.toro_natural}` : ""}`;
  }

  // RESULTADO TACTO — vincula con el servicio correcto
  if (accion.accion === "resultado_tacto") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    
    const fechaTacto = accion.fecha_tacto || hoy;
    const temporadaBuscada = accion.temporada || null;
    let resultado = (accion.resultado || "").toUpperCase();
    
    // Normalizar: aceptar PREÑADA sola como legado
    if (resultado === 'PREÑADA') resultado = 'PREÑADA_IATF';
    
    // ── BUSCAR SERVICIO CORRECTO ──
    // Prioridad: 1) temporada explícita, 2) último sin resultado, 3) último servicio
    let serv = null;
    if (temporadaBuscada) {
      serv = db.prepare("SELECT * FROM servicios WHERE animal_id = ? AND temporada = ? ORDER BY created_at DESC LIMIT 1")
        .get(animal.id, temporadaBuscada);
    }
    if (!serv) {
      // Buscar el último servicio SIN resultado (pendiente de tacto)
      serv = db.prepare("SELECT * FROM servicios WHERE animal_id = ? AND (resultado IS NULL OR resultado = '') ORDER BY created_at DESC LIMIT 1")
        .get(animal.id);
    }
    if (!serv) {
      // Fallback: último servicio (puede ser para corregir un tacto anterior)
      serv = db.prepare("SELECT * FROM servicios WHERE animal_id = ? ORDER BY created_at DESC LIMIT 1")
        .get(animal.id);
    }
    if (!serv) return `❌ No hay servicio registrado para RP ${accion.rp}. Primero registrá el servicio.`;
    
    // ── VALIDAR COHERENCIA ──
    // Si el servicio ya tiene resultado y no están pidiendo corrección explícita
    let correccion = '';
    if (serv.resultado && serv.resultado !== resultado) {
      correccion = `\n⚠️ Corregido: ${serv.resultado} → ${resultado} (temporada ${serv.temporada||'?'})`;
    }
    
    // ── VACÍA ──
    if (resultado === 'VACIA') {
      db.prepare("UPDATE servicios SET resultado = 'VACIA', tacto_servicio = ? WHERE id = ?")
        .run(fechaTacto, serv.id);
      let resp = `⚪ Tacto registrado!\n🏷️ RP ${animal.rp} | VACIA\n📅 ${fechaTacto}\n📋 Servicio temporada ${serv.temporada||'?'}`;
      if (serv.semen_iatf) resp += ` | Semen: ${serv.semen_iatf}`;
      if (serv.toro_natural) resp += ` | Toro: ${serv.toro_natural}`;
      resp += correccion;
      return resp;
    }
    
    // ── PREÑADA_IATF o PREÑADA_TORO ──
    let padre = null;
    let fpp = null;
    
    if (resultado === 'PREÑADA_IATF') {
      padre = serv.semen_iatf;
      if (serv.fecha_iatf) {
        const d = new Date(serv.fecha_iatf);
        d.setDate(d.getDate() + 282);
        fpp = d.toISOString().slice(0, 10);
      }
    } else if (resultado === 'PREÑADA_TORO') {
      padre = serv.toro_natural;
      if (serv.fecha_ingreso_toro) {
        const d = new Date(serv.fecha_ingreso_toro);
        d.setDate(d.getDate() + 282);
        fpp = d.toISOString().slice(0, 10);
      }
    }
    
    // Guardar resultado, fecha tacto, FPP y padre en el servicio
    db.prepare(`UPDATE servicios SET resultado = ?, tacto_servicio = ?, 
      notas = COALESCE(notas,'') || ? WHERE id = ?`)
      .run(resultado, fechaTacto,
           `${fpp ? ' | FPP: ' + fpp : ''}${padre ? ' | Padre: ' + padre : ''}`, serv.id);
    
    // Si es preñada → actualizar categoría a VACA si corresponde
    if (animal.categoria === 'VAQUILLONA' || (animal.categoria === 'RECRIA' && animal.sexo === 'HEMBRA')) {
      db.prepare("UPDATE animales SET categoria = 'VACA' WHERE id = ?").run(animal.id);
    }
    
    let resp = `🤰 Tacto registrado!\n🏷️ RP ${animal.rp} | ${resultado}`;
    resp += `\n📋 Servicio temporada ${serv.temporada||'?'}`;
    if (serv.semen_iatf) resp += ` | Semen: ${serv.semen_iatf}`;
    if (serv.toro_natural) resp += ` | Toro: ${serv.toro_natural}`;
    if (padre) resp += `\n🐂 Padre: ${padre}`;
    if (fpp) resp += `\n📅 Fecha probable parto: ${fpp}`;
    resp += `\n📅 Tacto: ${fechaTacto}`;
    resp += correccion;
    return resp;
  }

  // REGISTRAR PARTO
  if (accion.accion === "registrar_parto") {
    const madre = buscarAnimal(accion.madre_rp);
    if (!madre) return `❌ No encontré madre con RP "${accion.madre_rp}".`;
    
    const fechaParto = accion.fecha || hoy;
    
    // ── ASIGNAR PADRE AUTOMÁTICAMENTE ──
    // Buscar último servicio de la madre
    const serv = db.prepare("SELECT * FROM servicios WHERE animal_id = ? ORDER BY created_at DESC LIMIT 1").get(madre.id);
    let padre_rp = null;
    let padre_origen = "";
    
    if (serv && serv.fecha_iatf) {
      // Calcular días entre IATF y parto
      const diasGestacion = Math.floor((new Date(fechaParto) - new Date(serv.fecha_iatf)) / (1000*60*60*24));
      // Gestación bovina: 282 días ± 15 para determinar padre
      if (diasGestacion >= 267 && diasGestacion <= 297) {
        // Parto dentro del rango de IATF → padre = toro de inseminación
        padre_rp = serv.semen_iatf || serv.toro_natural;
        padre_origen = `IATF (${diasGestacion}d gestación)`;
      } else if (serv.toro_natural) {
        // Fuera de rango IATF → padre = toro de repaso
        padre_rp = serv.toro_natural;
        padre_origen = `REPASO (${diasGestacion}d desde IATF, fuera de rango ±15d)`;
      } else {
        padre_rp = serv.semen_iatf;
        padre_origen = `Estimado (${diasGestacion}d)`;
      }
      
      // Si el tacto decía PREÑADA_IATF pero el parto dice REPASO (o viceversa) → corregir
      if (serv.resultado === 'PREÑADA_IATF' && padre_origen.startsWith('REPASO')) {
        db.prepare("UPDATE servicios SET resultado = 'PREÑADA_TORO', notas = COALESCE(notas,'') || ? WHERE id = ?")
          .run(` | CORREGIDO por fecha parto: era IATF → TORO (${diasGestacion}d)`, serv.id);
        padre_origen += ' ⚠️ Corregido de IATF a TORO';
      } else if (serv.resultado === 'PREÑADA_TORO' && padre_origen.startsWith('IATF')) {
        db.prepare("UPDATE servicios SET resultado = 'PREÑADA_IATF', notas = COALESCE(notas,'') || ? WHERE id = ?")
          .run(` | CORREGIDO por fecha parto: era TORO → IATF (${diasGestacion}d)`, serv.id);
        padre_origen += ' ⚠️ Corregido de TORO a IATF';
      }
    } else if (serv && serv.toro_natural) {
      padre_rp = serv.toro_natural;
      padre_origen = "NATURAL";
    }
    
    // Registrar ternero como nuevo animal
    const terneroRp = accion.ternero_rp || `T${Date.now().toString().slice(-4)}`;
    try {
      db.prepare(`
        INSERT INTO animales (rp, fecha_nac, sexo, pelo, categoria, madre_rp, padre_rp, destino, fecha_ingreso, notas)
        VALUES (?, ?, ?, ?, 'TERNERO', ?, ?, 'PLANTEL', ?, ?)
      `).run(terneroRp, fechaParto, accion.sexo || "MACHO", accion.pelo || null, 
             accion.madre_rp, padre_rp, hoy, padre_origen ? `Padre: ${padre_origen}` : null);
    } catch(e) { /* ya existe */ }

    // Registrar peso nacimiento
    const ternero = buscarAnimal(terneroRp);
    if (ternero && accion.peso_nac) {
      db.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto) VALUES (?, ?, ?, 'NACIMIENTO')")
        .run(ternero.id, fechaParto, parseFloat(accion.peso_nac));
    }

    // Actualizar servicio de la madre
    if (serv) {
      db.prepare("UPDATE servicios SET ternero_rp = ?, peso_nacimiento = ?, sexo_cria = ?, fecha_parto = ?, resultado = 'PREÑADA' WHERE id = ?")
        .run(terneroRp, accion.peso_nac || null, accion.sexo || null, fechaParto, serv.id);
    }

    let resp = `🐄 Parto registrado!\n👩 Madre: RP ${accion.madre_rp}\n🐮 Ternero: RP ${terneroRp} | ${accion.sexo || ""} ${accion.pelo || ""}`;
    if (accion.peso_nac) resp += `\n⚖️ Peso nac: ${accion.peso_nac} kg`;
    if (padre_rp) resp += `\n🐂 Padre asignado: ${padre_rp} (${padre_origen})`;
    else resp += `\n⚠️ No se pudo determinar padre (sin servicio registrado)`;
    return resp;
  }

  // SANIDAD → la maneja procesarSanidad (async, descuenta stock y costea)
  if (accion.accion === "registrar_sanidad" || accion.accion === "sanidad_lote") {
    return "⏳";
  }

  if (accion.accion === "__nunca__") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    const fecha = accion.fecha || hoy;
    db.prepare("INSERT INTO sanidad (animal_id, fecha, tipo, producto, dosis, notas) VALUES (?, ?, ?, ?, ?, ?)")
      .run(animal.id, fecha, accion.tipo, accion.producto || null, accion.dosis || null, accion.notas || null);
    return `💉 Sanidad registrada!\n🏷️ RP ${animal.rp} | ${accion.tipo}\n💊 ${accion.producto || ""} | 📅 ${fecha}`;
  }

  // SANIDAD LOTE
  if (accion.accion === "sanidad_lote") {
    if (!Array.isArray(accion.registros)) return "❌ Formato inválido.";
    const fecha = accion.fecha || hoy;
    let ok = 0, errores = [];
    const stmt = db.prepare("INSERT INTO sanidad (animal_id, fecha, tipo, producto, dosis, notas) VALUES (?, ?, ?, ?, ?, ?)");
    for (const r of accion.registros) {
      const animal = buscarAnimal(r.rp);
      if (!animal) { errores.push(r.rp); continue; }
      stmt.run(animal.id, fecha, accion.tipo || "TRATAMIENTO", r.producto || accion.producto || null, r.dosis || null, accion.notas || null);
      ok++;
    }
    let resp = `💉 Sanidad lote: ${ok} registrados con ${accion.producto || accion.tipo || 'tratamiento'} (${fecha})`;
    if (errores.length) resp += `\n⚠️ No encontrados: ${errores.join(", ")}`;
    return resp;
  }

  // DAR BAJA
  if (accion.accion === "dar_baja") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    const motivo = (accion.motivo || "VENTA").toUpperCase();
    const estado = motivo === 'MUERTE' ? 'MUERTO' : 'VENDIDO';
    const fecha = accion.fecha || hoy;
    db.prepare("UPDATE animales SET estado = ?, fecha_salida = ?, motivo_salida = ? WHERE id = ?")
      .run(estado, fecha, motivo, animal.id);
    const emoji = motivo === 'MUERTE' ? '🪦' : '📤';
    return `${emoji} Baja registrada!\n🏷️ RP ${animal.rp} | ${motivo}\n📅 ${fecha}`;
  }

  // BAJA LOTE
  if (accion.accion === "baja_lote") {
    if (!Array.isArray(accion.rps)) return "❌ Formato inválido.";
    const motivo = (accion.motivo || "VENTA").toUpperCase();
    const estado = motivo === 'MUERTE' ? 'MUERTO' : 'VENDIDO';
    const fecha = accion.fecha || hoy;
    let ok = 0, errores = [];
    for (const rp of accion.rps) {
      const animal = buscarAnimal(rp);
      if (!animal) { errores.push(rp); continue; }
      db.prepare("UPDATE animales SET estado = ?, fecha_salida = ?, motivo_salida = ? WHERE id = ?").run(estado, fecha, motivo, animal.id);
      ok++;
    }
    let resp = `📤 Baja masiva: ${ok} animales (${motivo}) | 📅 ${fecha}`;
    if (errores.length) resp += `\n⚠️ No encontrados: ${errores.join(', ')}`;
    return resp;
  }

  // CAMBIAR CATEGORÍA
  if (accion.accion === "cambiar_categoria") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    db.prepare("UPDATE animales SET categoria = ? WHERE id = ?").run(accion.nueva_categoria, animal.id);
    return `✅ Categoría actualizada!\n🏷️ RP ${animal.rp} | ${animal.categoria} → ${accion.nueva_categoria}`;
  }

  // CAMBIAR CATEGORÍA LOTE
  if (accion.accion === "cambiar_categoria_lote") {
    if (!Array.isArray(accion.rps)) return "❌ Formato inválido.";
    let ok = 0, errores = [];
    for (const rp of accion.rps) {
      const animal = buscarAnimal(rp);
      if (!animal) { errores.push(rp); continue; }
      db.prepare("UPDATE animales SET categoria = ? WHERE id = ?").run(accion.nueva_categoria, animal.id);
      ok++;
    }
    let resp = `✅ Categoría masiva: ${ok} animales → ${accion.nueva_categoria}`;
    if (errores.length) resp += `\n⚠️ No encontrados: ${errores.join(', ')}`;
    return resp;
  }

  // FICHA ANIMAL
  if (accion.accion === "ficha_animal") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;

    const pesadas = db.prepare("SELECT * FROM pesadas WHERE animal_id = ? ORDER BY fecha DESC LIMIT 5").all(animal.id);
    const mediciones = db.prepare("SELECT * FROM mediciones WHERE animal_id = ? ORDER BY fecha DESC").all(animal.id);
    const ecografias = db.prepare("SELECT * FROM ecografias WHERE animal_id = ? ORDER BY fecha_medicion DESC").all(animal.id);
    const servicios = db.prepare("SELECT * FROM servicios WHERE animal_id = ? ORDER BY created_at DESC LIMIT 3").all(animal.id);
    const sanidadRec = db.prepare("SELECT * FROM sanidad WHERE animal_id = ? ORDER BY fecha DESC LIMIT 5").all(animal.id);

    // Calcular edad
    let edad = "";
    if (animal.fecha_nac) {
      const dias = Math.floor((new Date() - new Date(animal.fecha_nac)) / (1000*60*60*24));
      const meses = Math.floor(dias / 30.44);
      edad = meses >= 12 ? `${Math.floor(meses/12)}a ${meses%12}m` : `${meses}m`;
    }

    // Hijos
    const hijos = db.prepare("SELECT rp, sexo, fecha_nac FROM animales WHERE madre_rp = ? ORDER BY fecha_nac DESC").all(animal.rp);

    let ficha = `📋 *FICHA — RP ${animal.rp}*\n`;
    ficha += `${animal.sexo} ${animal.pelo || ""} | ${animal.categoria} | ${animal.raza}\n`;
    if (animal.chip) ficha += `🔖 CHIP: ${animal.chip}\n`;
    if (edad) ficha += `📅 Nac: ${animal.fecha_nac} (${edad})\n`;
    if (animal.madre_rp) ficha += `👩 Madre: ${animal.madre_rp}`;
    if (animal.padre_rp) ficha += ` | 👨 Padre: ${animal.padre_rp}`;
    if (animal.madre_rp || animal.padre_rp) ficha += "\n";

    if (pesadas.length) {
      ficha += `\n⚖️ *Pesadas:*\n`;
      pesadas.forEach(p => ficha += `  ${p.fecha}: ${fmt(p.peso)}kg (${p.contexto})${p.gdp ? ` GDP:${fmt(p.gdp*1000)}g/d` : ""}\n`);
    }

    if (mediciones.length) {
      ficha += `\n📐 *Mediciones:*\n`;
      mediciones.forEach(m => ficha += `  ${m.fecha}: ${m.tipo} = ${m.valor}\n`);
    }

    if (ecografias.length) {
      ficha += `\n🥩 *Ecografías:*\n`;
      ecografias.forEach(e => ficha += `  ${e.fecha_medicion}: AOB=${e.aob}cm² GD=${e.gd}mm GC=${e.gc}mm %GI=${e.pct_gi} (${e.estado || ""})\n`);
    }

    if (servicios.length) {
      ficha += `\n🔄 *Servicios:*\n`;
      servicios.forEach(s => {
        ficha += `  ${s.temporada || ""}: ${s.tipo_servicio || ""}${s.semen_iatf ? ` Semen:${s.semen_iatf}` : ""}${s.toro_natural ? ` Toro:${s.toro_natural}` : ""} → ${s.resultado || "pendiente"}`;
        if (s.ternero_rp) ficha += ` | Cría: ${s.ternero_rp}`;
        if (s.ternero2_rp) ficha += ` + Mellizo: ${s.ternero2_rp} (${s.peso_nacimiento2||'?'}kg)`;
        ficha += "\n";
      });
    }

    if (hijos.length) {
      ficha += `\n👶 *Crías (${hijos.length}):*\n`;
      hijos.forEach(h => {
        const mellTag = h.mellizo_de ? ` 👯 mellizo de ${h.mellizo_de}` : '';
        ficha += `  RP ${h.rp} | ${h.sexo} | ${h.fecha_nac || ""}${mellTag}\n`;
      });
    }

    if (sanidadRec.length) {
      ficha += `\n💉 *Últimos tratamientos:*\n`;
      sanidadRec.forEach(s => ficha += `  ${s.fecha}: ${s.producto || s.tipo}${s.dosis ? ` (${s.dosis})` : ""}\n`);
    }

    // Eventos de campo
    try {
      const eventos = db.prepare("SELECT * FROM eventos WHERE animal_id = ? ORDER BY fecha DESC LIMIT 10").all(animal.id);
      if (eventos.length) {
        ficha += `\n📋 *Eventos:*\n`;
        eventos.forEach(e => ficha += `  ${e.fecha}: ${e.descripcion}\n`);
      }
    } catch(e) {}

    return ficha;
  }

  // VER RODEO
  if (accion.accion === "ver_rodeo") {
    const resumen = getResumenRodeo();
    let resp = `🐄 *Rodeo AMAKAIK — ${resumen.total} cabezas activas*\n\n`;
    const cats = {};
    resumen.por_categoria.forEach(c => {
      const key = c.categoria || "SIN CAT";
      if (!cats[key]) cats[key] = { total: 0, detalle: [] };
      cats[key].total += c.n;
      cats[key].detalle.push(`${c.n} ${c.sexo || ""}`);
    });
    Object.entries(cats).forEach(([cat, data]) => {
      resp += `  ${cat}: ${data.total} (${data.detalle.join(", ")})\n`;
    });
    if (resumen.por_pelo.length) {
      resp += `\nPor pelo: ${resumen.por_pelo.map(p => `${p.pelo || "s/d"}: ${p.n}`).join(" | ")}`;
    }
    return resp;
  }

  // VER LOTE
  if (accion.accion === "ver_lote") {
    let where = "estado = 'ACTIVO'";
    const params = [];
    if (accion.categoria) { where += " AND UPPER(categoria) = UPPER(?)"; params.push(accion.categoria); }
    if (accion.sexo) { where += " AND UPPER(sexo) = UPPER(?)"; params.push(accion.sexo); }
    const animales = db.prepare(`SELECT * FROM animales WHERE ${where} ORDER BY rp`).all(...params);
    if (!animales.length) return "📋 No hay animales con esos filtros.";

    let resp = `📋 *Lote: ${accion.categoria || "TODOS"} ${accion.sexo || ""} — ${animales.length} cabezas*\n\n`;
    animales.slice(0, 30).forEach(a => {
      resp += `  🏷️ ${a.rp} | ${a.sexo} ${a.pelo || ""} | ${a.categoria}${a.fecha_nac ? ` | Nac: ${a.fecha_nac}` : ""}\n`;
    });
    if (animales.length > 30) resp += `\n... y ${animales.length - 30} más`;
    return resp;
  }

  // VER PESADAS
  if (accion.accion === "ver_pesadas") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    const pesadas = db.prepare("SELECT * FROM pesadas WHERE animal_id = ? ORDER BY fecha ASC").all(animal.id);
    if (!pesadas.length) return `📋 No hay pesadas para RP ${accion.rp}.`;
    let resp = `⚖️ *Pesadas RP ${animal.rp}:*\n\n`;
    pesadas.forEach(p => resp += `  ${p.fecha}: ${fmt(p.peso)}kg (${p.contexto})${p.gdp ? ` | GDP: ${fmt(p.gdp*1000)}g/d` : ""}\n`);
    return resp;
  }

  // VER SERVICIOS
  if (accion.accion === "ver_servicios") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    const servicios = db.prepare("SELECT * FROM servicios WHERE animal_id = ? ORDER BY created_at DESC").all(animal.id);
    if (!servicios.length) return `📋 No hay servicios para RP ${accion.rp}.`;
    let resp = `🔄 *Servicios RP ${animal.rp}:*\n\n`;
    servicios.forEach(s => {
      resp += `  ${s.temporada || ""}: ${s.tipo_servicio || ""}`;
      if (s.semen_iatf) resp += ` | Semen: ${s.semen_iatf}`;
      if (s.toro_natural) resp += ` | Toro: ${s.toro_natural}`;
      resp += ` → ${s.resultado || "pendiente"}`;
      if (s.ternero_rp) resp += ` | Cría: ${s.ternero_rp} (${s.peso_nacimiento || "?"}kg)`;
      resp += "\n";
    });
    return resp;
  }

  // VER ECOGRAFÍAS
  if (accion.accion === "ver_ecografias") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    const ecos = db.prepare("SELECT * FROM ecografias WHERE animal_id = ? ORDER BY fecha_medicion DESC").all(animal.id);
    if (!ecos.length) return `📋 No hay ecografías para RP ${accion.rp}.`;
    let resp = `🥩 *Ecografías RP ${animal.rp}:*\n\n`;
    ecos.forEach(e => resp += `  ${e.fecha_medicion}: AOB=${e.aob}cm² | GD=${e.gd}mm | GC=${e.gc}mm | %GI=${e.pct_gi}% | ${e.estado || ""}\n`);
    return resp;
  }

  // VER SANIDAD
  if (accion.accion === "ver_sanidad") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    const registros = db.prepare("SELECT * FROM sanidad WHERE animal_id = ? ORDER BY fecha DESC").all(animal.id);
    if (!registros.length) return `📋 No hay registros sanitarios para RP ${accion.rp}.`;
    let resp = `💉 *Sanidad RP ${animal.rp}:*\n\n`;
    registros.forEach(s => resp += `  ${s.fecha}: ${s.tipo} | ${s.producto || ""}${s.dosis ? ` (${s.dosis})` : ""}${s.notas ? ` — ${s.notas}` : ""}\n`);
    return resp;
  }

  // BUSCAR
  if (accion.accion === "buscar") {
    const term = `%${accion.termino}%`;
    const animales = db.prepare(`
      SELECT * FROM animales WHERE estado = 'ACTIVO' AND 
      (rp LIKE ? OR chip LIKE ? OR notas LIKE ? OR madre_rp LIKE ? OR padre_rp LIKE ?)
      LIMIT 20
    `).all(term, term, term, term, term);
    if (!animales.length) return `🔍 No encontré animales con "${accion.termino}".`;
    let resp = `🔍 *Resultados para "${accion.termino}" — ${animales.length}:*\n\n`;
    animales.forEach(a => resp += `  🏷️ ${a.rp}${a.chip ? ` (${a.chip})` : ""} | ${a.sexo} ${a.pelo || ""} | ${a.categoria}\n`);
    return resp;
  }

  // RANKING PESO
  if (accion.accion === "ranking_peso") {
    let query = `
      SELECT a.rp, a.categoria, a.sexo, p.peso, p.fecha, p.contexto
      FROM pesadas p JOIN animales a ON a.id = p.animal_id
      WHERE a.estado = 'ACTIVO'
    `;
    const params = [];
    if (accion.categoria) { query += " AND UPPER(a.categoria) = UPPER(?)"; params.push(accion.categoria); }
    query += " ORDER BY p.peso DESC LIMIT ?";
    params.push(accion.limite || 10);
    const rows = db.prepare(query).all(...params);
    if (!rows.length) return "📋 No hay pesadas registradas.";
    let resp = `🏆 *Top ${rows.length} por peso${accion.categoria ? ` (${accion.categoria})` : ""}:*\n\n`;
    rows.forEach((r, i) => resp += `  ${i+1}. RP ${r.rp}: ${fmt(r.peso)}kg | ${r.contexto} (${r.fecha})\n`);
    return resp;
  }

  // ESTADÍSTICAS ECOGRAFÍA
  if (accion.accion === "estadisticas_ecografia") {
    const stats = db.prepare(`
      SELECT a.sexo, COUNT(*) as n, 
        AVG(e.pct_gi) as avg_gi, AVG(e.aob) as avg_aob, AVG(e.gd) as avg_gd, AVG(e.gc) as avg_gc,
        MIN(e.aob) as min_aob, MAX(e.aob) as max_aob
      FROM ecografias e JOIN animales a ON a.id = e.animal_id
      GROUP BY a.sexo
    `).all();
    if (!stats.length) return "📋 No hay ecografías registradas.";
    let resp = `🥩 *Estadísticas Ecográficas:*\n\n`;
    stats.forEach(s => {
      resp += `${s.sexo} (${s.n} animales):\n`;
      resp += `  %GI: ${fmt(s.avg_gi)} | AOB: ${fmt(s.avg_aob)}cm² (${fmt(s.min_aob)}-${fmt(s.max_aob)})\n`;
      resp += `  GD: ${fmt(s.avg_gd)}mm | GC: ${fmt(s.avg_gc)}mm\n\n`;
    });
    return resp;
  }

  // RESUMEN SERVICIOS
  if (accion.accion === "resumen_servicios") {
    const rows = db.prepare(`
      SELECT resultado, COUNT(*) as n FROM servicios 
      ${accion.temporada ? "WHERE temporada = ?" : ""}
      GROUP BY resultado
    `).all(...(accion.temporada ? [accion.temporada] : []));
    if (!rows.length) return "📋 No hay servicios registrados.";
    const total = rows.reduce((s, r) => s + r.n, 0);
    const prenadas = rows.find(r => r.resultado === "PREÑADA")?.n || 0;
    let resp = `🔄 *Resumen Servicios${accion.temporada ? ` ${accion.temporada}` : ""}:*\n\n`;
    rows.forEach(r => resp += `  ${r.resultado || "PENDIENTE"}: ${r.n}\n`);
    resp += `\n📊 Total: ${total} | % Preñez: ${total ? ((prenadas/total)*100).toFixed(1) : 0}%`;
    return resp;
  }

  // VER ÚLTIMOS
  if (accion.accion === "ver_ultimos") {
    const ultAnimales = db.prepare("SELECT * FROM animales ORDER BY created_at DESC LIMIT 5").all();
    const ultPesadas = db.prepare("SELECT p.*, a.rp FROM pesadas p JOIN animales a ON a.id = p.animal_id ORDER BY p.created_at DESC LIMIT 5").all();
    let resp = "📋 *Últimos registros:*\n\n";
    if (ultAnimales.length) {
      resp += "🐄 Animales:\n";
      ultAnimales.forEach(a => resp += `  ${a.rp} | ${a.sexo} ${a.pelo || ""} | ${a.categoria}\n`);
    }
    if (ultPesadas.length) {
      resp += "\n⚖️ Pesadas:\n";
      ultPesadas.forEach(p => resp += `  ${p.rp}: ${fmt(p.peso)}kg (${p.fecha})\n`);
    }
    return resp;
  }

  // IMPORTAR ANIMALES
  if (accion.accion === "importar_animales") {
    if (!Array.isArray(accion.animales)) return "❌ Formato inválido.";
    let ok = 0, errores = 0;
    const stmt = db.prepare(`
      INSERT OR IGNORE INTO animales (chip, rp, fecha_nac, raza, registro, sexo, pelo, categoria, destino, madre_rp, madre_hba, padre_rp, padre_hba, estado, fecha_salida, motivo_salida, notas, fecha_ingreso, hbu, mellizo_de)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    for (const a of accion.animales) {
      try {
        stmt.run(a.chip||null, a.rp, a.fecha_nac||null, a.raza||'A. ANGUS', a.registro||null,
                 a.sexo||'HEMBRA', a.pelo||null, a.categoria||'RECRIA', a.destino||'PLANTEL',
                 a.madre_rp||null, a.madre_hba||null, a.padre_rp||null, a.padre_hba||null,
                 a.estado||'ACTIVO', a.fecha_salida||null, a.motivo_salida||null, a.notas||null,
                 a.fecha_ingreso||new Date().toISOString().slice(0,10), a.hbu||null, a.mellizo_de||null);
        ok++;
      } catch(e) { errores++; }
    }
    return `✅ Importación: ${ok} animales cargados, ${errores} errores.`;
  }

  // IMPORTAR ECOGRAFÍAS
  if (accion.accion === "importar_ecografias") {
    if (!Array.isArray(accion.ecografias)) return "❌ Formato inválido.";
    let ok = 0, errores = 0;
    for (const e of accion.ecografias) {
      const animal = buscarAnimalTodos(e.rp);
      if (!animal) { errores++; continue; }
      try {
        db.prepare(`INSERT INTO ecografias (animal_id, fecha_medicion, dias_vida, pct_gi, aob, gd, gc, estado) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
          .run(animal.id, e.fecha_medicion||null, e.dias_vida||null, e.pct_gi||null, e.aob||null, e.gd||null, e.gc||null, e.estado||null);
        ok++;
      } catch(err) { errores++; }
    }
    return `✅ Ecografías importadas: ${ok} cargadas, ${errores} errores.`;
  }

  // IMPORTAR PESADAS
  if (accion.accion === "importar_pesadas") {
    if (!Array.isArray(accion.pesadas)) return "❌ Formato inválido.";
    let ok = 0, errores = 0;
    for (const p of accion.pesadas) {
      const animal = buscarAnimalTodos(p.rp);
      if (!animal) { errores++; continue; }
      try {
        db.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto) VALUES (?, ?, ?, ?)")
          .run(animal.id, p.fecha||new Date().toISOString().slice(0,10), p.peso, p.contexto||"DESARROLLO");
        ok++;
      } catch(err) { errores++; }
    }
    return `✅ Pesadas importadas: ${ok} cargadas, ${errores} errores.`;
  }

  // BORRAR PESADA
  if (accion.accion === "borrar_pesada") {
    const p = db.prepare("SELECT * FROM pesadas WHERE id = ?").get(accion.id);
    if (!p) return `❌ No encontré pesada #${accion.id}.`;
    db.prepare("DELETE FROM pesadas WHERE id = ?").run(accion.id);
    return `🗑️ Pesada #${accion.id} eliminada (${p.peso}kg del ${p.fecha})`;
  }

  // EDITAR PESADA
  if (accion.accion === "editar_pesada") {
    const p = db.prepare("SELECT * FROM pesadas WHERE id = ?").get(accion.id);
    if (!p) return `❌ No encontré pesada #${accion.id}.`;
    if (accion.peso) db.prepare("UPDATE pesadas SET peso = ? WHERE id = ?").run(parseFloat(accion.peso), accion.id);
    if (accion.fecha) db.prepare("UPDATE pesadas SET fecha = ? WHERE id = ?").run(accion.fecha, accion.id);
    if (accion.contexto) db.prepare("UPDATE pesadas SET contexto = ? WHERE id = ?").run(accion.contexto, accion.id);
    return `✅ Pesada #${accion.id} actualizada${accion.peso ? ` → ${accion.peso}kg` : ''}`;
  }

  // BORRAR SANIDAD
  if (accion.accion === "borrar_sanidad") {
    db.prepare("DELETE FROM sanidad WHERE id = ?").run(accion.id);
    return `🗑️ Registro sanitario #${accion.id} eliminado`;
  }

  // BORRAR SERVICIO
  if (accion.accion === "borrar_servicio") {
    db.prepare("DELETE FROM servicios WHERE id = ?").run(accion.id);
    return `🗑️ Servicio #${accion.id} eliminado`;
  }

  // BORRAR MEDICIÓN
  if (accion.accion === "borrar_medicion") {
    db.prepare("DELETE FROM mediciones WHERE id = ?").run(accion.id);
    return `🗑️ Medición #${accion.id} eliminada`;
  }

  // ── MOTOR DE CONSULTAS / INFORMES ──────────────────────────────────────────
  if (accion.accion === "consulta") {
    const tipo = (accion.tipo || "").toLowerCase();
    const f = accion.filtros || {};
    const temporada = accion.temporada || f.temporada;
    
    // ── SERVICIO RESUMEN ──
    if (tipo === "servicio_resumen") {
      let where = "1=1"; const params = [];
      if (temporada) { where += " AND s.temporada = ?"; params.push(temporada); }
      const total = db.prepare(`SELECT COUNT(*) as n FROM servicios s WHERE ${where}`).get(...params);
      const porTipo = db.prepare(`SELECT tipo_servicio, COUNT(*) as n FROM servicios s WHERE ${where} GROUP BY tipo_servicio`).all(...params);
      const porToro = db.prepare(`SELECT COALESCE(semen_iatf, toro_natural, 'Sin dato') as toro, COUNT(*) as n FROM servicios s WHERE ${where} GROUP BY toro ORDER BY n DESC`).all(...params);
      const conCC = db.prepare(`SELECT AVG(cc_pre) as prom, MIN(cc_pre) as min, MAX(cc_pre) as max FROM servicios s WHERE ${where} AND cc_pre > 0`).get(...params);
      
      let resp = `📊 *Resumen de Servicio${temporada ? ` — Temporada ${temporada}` : ''}*\n\n`;
      resp += `🐄 Total servidas: ${total.n}\n`;
      porTipo.forEach(t => resp += `  ${t.tipo_servicio || 'Sin tipo'}: ${t.n} (${total.n ? ((t.n/total.n)*100).toFixed(0) : 0}%)\n`);
      resp += `\n🐂 Por toro/semen:\n`;
      porToro.forEach(t => resp += `  ${t.toro}: ${t.n} vacas\n`);
      if (conCC && conCC.prom) resp += `\n📊 CC pre-servicio: promedio ${fmt(conCC.prom)} (${fmt(conCC.min)}-${fmt(conCC.max)})`;
      return resp;
    }
    
    // ── SERVICIO DETALLE ──
    if (tipo === "servicio_detalle") {
      let where = "1=1"; const params = [];
      if (temporada) { where += " AND s.temporada = ?"; params.push(temporada); }
      if (f.toro) { where += " AND (LOWER(s.semen_iatf) LIKE LOWER(?) OR LOWER(s.toro_natural) LIKE LOWER(?))"; params.push(`%${f.toro}%`, `%${f.toro}%`); }
      const rows = db.prepare(`SELECT s.*, a.rp, a.registro, a.categoria FROM servicios s JOIN animales a ON a.id = s.animal_id WHERE ${where} ORDER BY a.rp`).all(...params);
      if (!rows.length) return `📋 No hay servicios${temporada ? ` en temporada ${temporada}` : ''}.`;
      let resp = `📋 *Detalle Servicio${temporada ? ` ${temporada}` : ''} — ${rows.length} vacas*\n\n`;
      rows.forEach(s => {
        resp += `🏷️ ${s.rp} | ${s.tipo_servicio||'—'} | Semen: ${s.semen_iatf||'—'} | Toro: ${s.toro_natural||'—'}`;
        if (s.fecha_iatf) resp += ` | IATF: ${s.fecha_iatf}`;
        if (s.cc_pre) resp += ` | CC: ${s.cc_pre}`;
        resp += ` → ${s.resultado || 'pendiente'}\n`;
      });
      return resp;
    }
    
    // ── SERVICIO POR TORO ──
    if (tipo === "servicio_por_toro") {
      let where = "1=1"; const params = [];
      if (temporada) { where += " AND s.temporada = ?"; params.push(temporada); }
      const iatf = db.prepare(`SELECT semen_iatf as toro, COUNT(*) as n, SUM(CASE WHEN resultado='PREÑADA_IATF' THEN 1 ELSE 0 END) as prenadas FROM servicios s WHERE ${where} AND semen_iatf IS NOT NULL GROUP BY semen_iatf ORDER BY n DESC`).all(...params);
      const natural = db.prepare(`SELECT toro_natural as toro, COUNT(*) as n, SUM(CASE WHEN resultado='PREÑADA_TORO' THEN 1 ELSE 0 END) as prenadas FROM servicios s WHERE ${where} AND toro_natural IS NOT NULL GROUP BY toro_natural ORDER BY n DESC`).all(...params);
      let resp = `🐂 *Distribución por Toro${temporada ? ` — ${temporada}` : ''}*\n\n`;
      if (iatf.length) {
        resp += `🧬 IATF (semen):\n`;
        iatf.forEach(t => resp += `  ${t.toro}: ${t.n} servidas → ${t.prenadas} preñadas (${t.n ? ((t.prenadas/t.n)*100).toFixed(0) : 0}%)\n`);
      }
      if (natural.length) {
        resp += `\n🐂 Repaso (natural):\n`;
        natural.forEach(t => resp += `  ${t.toro}: ${t.n} servidas → ${t.prenadas} preñadas\n`);
      }
      return resp;
    }
    
    // ── TACTO RESUMEN ──
    if (tipo === "tacto_resumen") {
      // Usar subquery para tomar SOLO el último servicio de cada animal
      let whereTemp = ''; const params = [];
      if (temporada) { whereTemp = " AND temporada = ?"; params.push(temporada); }
      const sql = `SELECT s.resultado, COUNT(*) as n FROM servicios s
        INNER JOIN (SELECT animal_id, MAX(id) as max_id FROM servicios WHERE 1=1${whereTemp} GROUP BY animal_id) ult
        ON s.id = ult.max_id
        WHERE s.resultado IS NOT NULL GROUP BY s.resultado`;
      const porRes = db.prepare(sql).all(...params);
      const totalDiagQ = `SELECT COUNT(*) as n FROM servicios s
        INNER JOIN (SELECT animal_id, MAX(id) as max_id FROM servicios WHERE 1=1${whereTemp} GROUP BY animal_id) ult
        ON s.id = ult.max_id WHERE s.resultado IS NOT NULL`;
      const total = db.prepare(totalDiagQ).get(...params);
      const totalServQ = `SELECT COUNT(DISTINCT animal_id) as n FROM servicios WHERE 1=1${whereTemp}`;
      const totalServ = db.prepare(totalServQ).get(...params);
      
      const prenIatf = porRes.find(r => r.resultado === 'PREÑADA_IATF')?.n || 0;
      const prenToro = porRes.find(r => r.resultado === 'PREÑADA_TORO')?.n || 0;
      const prenLegacy = porRes.find(r => r.resultado === 'PREÑADA')?.n || 0;
      const vacias = porRes.find(r => r.resultado === 'VACIA')?.n || 0;
      const totalPren = prenIatf + prenToro + prenLegacy;
      const totalDiag = total.n || 1;
      
      let resp = `📊 *Diagnóstico de Preñez${temporada ? ` — ${temporada}` : ''}*\n\n`;
      resp += `🐄 Vacas servidas (únicas): ${totalServ.n}\n`;
      resp += `🔍 Diagnosticadas: ${total.n}\n`;
      resp += `🤰 Preñadas: ${totalPren} (${((totalPren/totalDiag)*100).toFixed(1)}%)\n`;
      resp += `  🧬 IATF: ${prenIatf} (${((prenIatf/totalDiag)*100).toFixed(1)}%)\n`;
      resp += `  🐂 Toro: ${prenToro} (${((prenToro/totalDiag)*100).toFixed(1)}%)\n`;
      if (prenLegacy) resp += `  ❓ Sin tipo (dato viejo): ${prenLegacy}\n`;
      resp += `⚪ Vacías: ${vacias} (${((vacias/totalDiag)*100).toFixed(1)}%)\n`;
      resp += `⏳ Sin diagnosticar: ${totalServ.n - total.n}`;
      return resp;
    }
    
    // ── PREÑADAS HOY (estado actual de cada vaca) ──
    if (tipo === "prenadas_hoy" || tipo === "preñadas_hoy" || tipo === "prenadas_actual") {
      // Solo vacas activas cuyo ÚLTIMO servicio tiene resultado preñada
      const rows = db.prepare(`
        SELECT s.*, a.rp, a.categoria, a.registro, a.fecha_nac FROM servicios s
        JOIN animales a ON a.id = s.animal_id
        INNER JOIN (SELECT animal_id, MAX(id) as max_id FROM servicios GROUP BY animal_id) ult
        ON s.id = ult.max_id
        WHERE a.estado = 'ACTIVO' AND s.resultado IN ('PREÑADA_IATF','PREÑADA_TORO','PREÑADA')
        ORDER BY a.rp
      `).all();
      if (!rows.length) return "📋 No hay vacas preñadas activas actualmente.";
      let resp = `🤰 *Vacas Preñadas Actualmente — ${rows.length} cabezas*\n\n`;
      rows.forEach(s => {
        let padre = '', fpp = '';
        if (s.resultado === 'PREÑADA_IATF') {
          padre = s.semen_iatf || '?';
          if (s.fecha_iatf) { const d = new Date(s.fecha_iatf); d.setDate(d.getDate()+282); fpp = d.toISOString().slice(0,10); }
        } else if (s.resultado === 'PREÑADA_TORO') {
          padre = s.toro_natural || '?';
          if (s.fecha_ingreso_toro) { const d = new Date(s.fecha_ingreso_toro); d.setDate(d.getDate()+282); fpp = d.toISOString().slice(0,10); }
        } else {
          padre = s.semen_iatf || s.toro_natural || 'sin dato';
        }
        const tipoLabel = s.resultado === 'PREÑADA_IATF' ? '🧬IATF' : s.resultado === 'PREÑADA_TORO' ? '🐂TORO' : '❓';
        resp += `🏷️ ${s.rp} | ${tipoLabel} | Padre: ${padre}${fpp ? ` | FPP: ${fpp}` : ''} | ${s.temporada||''}\n`;
      });
      return resp;
    }
    
    // ── TACTO DETALLE ──
    if (tipo === "tacto_detalle") {
      let where = "resultado IS NOT NULL"; const params = [];
      if (temporada) { where += " AND s.temporada = ?"; params.push(temporada); }
      const rows = db.prepare(`SELECT s.*, a.rp FROM servicios s JOIN animales a ON a.id = s.animal_id WHERE ${where} ORDER BY s.resultado, a.rp`).all(...params);
      if (!rows.length) return "📋 No hay resultados de tacto registrados.";
      let resp = `📋 *Detalle Tacto${temporada ? ` ${temporada}` : ''} — ${rows.length} diagnosticadas*\n\n`;
      rows.forEach(s => {
        let fpp = '', padre = '';
        if (s.resultado === 'PREÑADA_IATF') {
          padre = s.semen_iatf || '?';
          if (s.fecha_iatf) { const d = new Date(s.fecha_iatf); d.setDate(d.getDate()+282); fpp = ` → FPP: ${d.toISOString().slice(0,10)}`; }
        } else if (s.resultado === 'PREÑADA_TORO') {
          padre = s.toro_natural || '?';
          if (s.fecha_ingreso_toro) { const d = new Date(s.fecha_ingreso_toro); d.setDate(d.getDate()+282); fpp = ` → FPP: ~${d.toISOString().slice(0,10)}`; }
        } else if (s.resultado === 'PREÑADA') {
          // Formato viejo — intentar determinar padre
          padre = s.semen_iatf || s.toro_natural || 'sin dato';
        } else if (s.resultado === 'VACIA') {
          padre = s.semen_iatf || s.toro_natural || '—';
        }
        const emoji = s.resultado === 'VACIA' ? '⚪' : '🤰';
        resp += `${emoji} ${s.rp} | ${s.resultado} | ${padre}${fpp}\n`;
      });
      return resp;
    }
    
    // ── VACÍAS ──
    if (tipo === "vacias") {
      let where = "resultado = 'VACIA'"; const params = [];
      if (temporada) { where += " AND s.temporada = ?"; params.push(temporada); }
      const rows = db.prepare(`SELECT s.*, a.rp, a.categoria, a.registro, a.fecha_nac FROM servicios s JOIN animales a ON a.id = s.animal_id WHERE ${where} AND a.estado = 'ACTIVO' ORDER BY a.rp`).all(...params);
      if (!rows.length) return "✅ No hay vacías registradas.";
      let resp = `⚪ *Vacías${temporada ? ` ${temporada}` : ''} — ${rows.length} animales*\n\n`;
      rows.forEach(s => {
        const edad = s.fecha_nac ? Math.floor((new Date() - new Date(s.fecha_nac)) / (1000*60*60*24*30.44)) : '?';
        resp += `🏷️ ${s.rp} | ${s.categoria} | ${s.registro||'—'} | ${edad} meses | Servida: ${s.tipo_servicio||'—'} ${s.semen_iatf||s.toro_natural||''}\n`;
      });
      resp += `\n💡 Opciones: re-servir, descartar (baja), o mantener para próxima temporada.`;
      return resp;
    }
    
    // ── FPP (fechas probables de parto) ──
    if (tipo === "fpp") {
      let where = "resultado IN ('PREÑADA_IATF','PREÑADA_TORO','PREÑADA')"; const params = [];
      if (temporada) { where += " AND s.temporada = ?"; params.push(temporada); }
      const rows = db.prepare(`SELECT s.*, a.rp FROM servicios s JOIN animales a ON a.id = s.animal_id WHERE ${where} AND a.estado = 'ACTIVO' ORDER BY s.fecha_iatf`).all(...params);
      if (!rows.length) return "📋 No hay preñadas con fecha para calcular FPP.";
      let resp = `📅 *Fechas Probables de Parto${temporada ? ` ${temporada}` : ''}*\n\n`;
      const fpps = [];
      rows.forEach(s => {
        let fpp = null, padre = '';
        if (s.resultado === 'PREÑADA_IATF' && s.fecha_iatf) {
          const d = new Date(s.fecha_iatf); d.setDate(d.getDate()+282);
          fpp = d.toISOString().slice(0,10); padre = s.semen_iatf || '—';
        } else if (s.resultado === 'PREÑADA_TORO' && s.fecha_ingreso_toro) {
          const d = new Date(s.fecha_ingreso_toro); d.setDate(d.getDate()+282);
          fpp = d.toISOString().slice(0,10); padre = s.toro_natural || '—';
        } else if (s.resultado === 'PREÑADA') {
          // Legacy: intentar con fecha_iatf primero, sino fecha_ingreso_toro
          padre = s.semen_iatf || s.toro_natural || 'sin dato';
          if (s.fecha_iatf) { const d = new Date(s.fecha_iatf); d.setDate(d.getDate()+282); fpp = d.toISOString().slice(0,10); }
          else if (s.fecha_ingreso_toro) { const d = new Date(s.fecha_ingreso_toro); d.setDate(d.getDate()+282); fpp = d.toISOString().slice(0,10); }
        }
        if (fpp) fpps.push({ rp: s.rp, fpp, padre, tipo: s.resultado });
      });
      fpps.sort((a,b) => a.fpp.localeCompare(b.fpp));
      fpps.forEach(f => {
        const tipoLabel = f.tipo === 'PREÑADA_IATF' ? '🧬 IATF' : f.tipo === 'PREÑADA_TORO' ? '🐂 Toro' : '❓ s/tipo';
        resp += `📅 ${f.fpp} | ${f.rp} | ${tipoLabel}: ${f.padre}\n`;
      });
      const hoy = new Date().toISOString().slice(0,10);
      const vencidas = fpps.filter(f => f.fpp < hoy).length;
      if (vencidas) resp += `\n⚠️ ${vencidas} vacas ya pasaron su FPP y no registraron parto.`;
      return resp;
    }
    
    // ── PARICIÓN RESUMEN ──
    if (tipo === "paricion_resumen") {
      let where = "1=1"; const params = [];
      if (temporada) { where += " AND s.temporada = ?"; params.push(temporada); }
      const anio = f.anio || accion.anio;
      if (anio) { where += " AND s.fecha_parto LIKE ?"; params.push(`${anio}-%`); }
      const conParto = db.prepare(`SELECT COUNT(*) as n, AVG(s.peso_nacimiento) as peso_prom FROM servicios s WHERE ${where} AND s.fecha_parto IS NOT NULL`).get(...params);
      const porOrigen = db.prepare(`SELECT resultado, COUNT(*) as n FROM servicios s WHERE ${where} AND s.fecha_parto IS NOT NULL GROUP BY resultado`).all(...params);
      const porSexo = db.prepare(`SELECT sexo_cria, COUNT(*) as n FROM servicios s WHERE ${where} AND sexo_cria IS NOT NULL GROUP BY sexo_cria`).all(...params);
      const label = anio ? ` ${anio}` : (temporada ? ` ${temporada}` : '');
      let resp = `🐄 *Resumen Parición${label}*\n\n`;
      resp += `🐮 Total partos: ${conParto.n}\n`;
      if (conParto.peso_prom) resp += `⚖️ Peso promedio nacimiento: ${fmt(conParto.peso_prom)} kg\n`;
      porOrigen.forEach(o => {
        const lab = o.resultado === 'PREÑADA_IATF' ? '🧬 IATF' : o.resultado === 'PREÑADA_TORO' ? '🐂 Toro' : o.resultado || '—';
        resp += `  ${lab}: ${o.n}\n`;
      });
      if (porSexo.length) {
        resp += `\nPor sexo:\n`;
        porSexo.forEach(s => resp += `  ${s.sexo_cria}: ${s.n}\n`);
      }
      return resp;
    }
    
    // ── PARICIÓN DETALLE ──
    if (tipo === "paricion_detalle") {
      let where = "s.fecha_parto IS NOT NULL"; const params = [];
      if (temporada) { where += " AND s.temporada = ?"; params.push(temporada); }
      const anio = f.anio || accion.anio;
      if (anio) { where += " AND s.fecha_parto LIKE ?"; params.push(`${anio}-%`); }
      const rows = db.prepare(`SELECT s.*, a.rp as madre_rp FROM servicios s JOIN animales a ON a.id = s.animal_id WHERE ${where} ORDER BY s.fecha_parto DESC`).all(...params);
      const label = anio ? ` ${anio}` : (temporada ? ` ${temporada}` : '');
      if (!rows.length) return `📋 No hay partos registrados${label}.`;
      let resp = `📋 *Detalle Parición${label} — ${rows.length} partos*\n\n`;
      rows.forEach(s => {
        const padre = s.resultado === 'PREÑADA_IATF' ? `🧬 ${s.semen_iatf||'?'}` : s.resultado === 'PREÑADA_TORO' ? `🐂 ${s.toro_natural||'?'}` : `${s.semen_iatf||s.toro_natural||'?'}`;
        resp += `📅 ${s.fecha_parto} | Madre: ${s.madre_rp} → Cría: ${s.ternero_rp||'?'} ${s.sexo_cria||''} | ${fmt(s.peso_nacimiento||0)}kg | Padre: ${padre}\n`;
      });
      return resp;
    }
    
    // ── EVALUACIÓN DE TOROS ──
    if (tipo === "evaluacion_toros") {
      const anio = f.anio || accion.anio;
      let whereExtra = ''; const params2 = [];
      if (anio) { whereExtra = " AND a.fecha_nac LIKE ?"; params2.push(`${anio}-%`); }
      const toros = db.prepare(`
        SELECT a.padre_rp as toro, COUNT(*) as crias, 
          AVG(pn.peso) as peso_nac_prom, AVG(pd.peso) as peso_dest_prom
        FROM animales a 
        LEFT JOIN pesadas pn ON pn.animal_id = a.id AND pn.contexto = 'NACIMIENTO'
        LEFT JOIN pesadas pd ON pd.animal_id = a.id AND pd.contexto = 'DESTETE'
        WHERE a.padre_rp IS NOT NULL AND a.padre_rp != ''${whereExtra}
        GROUP BY a.padre_rp ORDER BY crias DESC
      `).all(...params2);
      if (!toros.length) return `📋 No hay datos de crías${anio ? ` nacidas en ${anio}` : ''} con padre asignado.`;
      let resp = `🐂 *Evaluación de Toros${anio ? ` — Crías ${anio}` : ''} — Ranking por progenie*\n\n`;
      toros.forEach((t, i) => {
        resp += `${i+1}. ${t.toro}: ${t.crias} crías`;
        if (t.peso_nac_prom) resp += ` | Nac: ${fmt(t.peso_nac_prom)}kg`;
        if (t.peso_dest_prom) resp += ` | Dest: ${fmt(t.peso_dest_prom)}kg`;
        resp += `\n`;
      });
      return resp;
    }
    
    // ── DESTETE RESUMEN ──
    if (tipo === "destete_resumen") {
      const anio = f.anio || accion.anio;
      let whereExtra = ''; const params2 = [];
      if (anio) { whereExtra = " AND p.fecha LIKE ?"; params2.push(`${anio}-%`); }
      const stats = db.prepare(`
        SELECT a.sexo, COUNT(*) as n, AVG(p.peso) as prom, MIN(p.peso) as min, MAX(p.peso) as max, AVG(p.gdp) as gdp_prom
        FROM pesadas p JOIN animales a ON a.id = p.animal_id 
        WHERE p.contexto = 'DESTETE'${whereExtra} GROUP BY a.sexo
      `).all(...params2);
      if (!stats.length) return `📋 No hay pesadas de destete${anio ? ` en ${anio}` : ''}.`;
      let resp = `⚖️ *Resumen Destete${anio ? ` ${anio}` : ''}*\n\n`;
      let totalN = 0;
      stats.forEach(s => {
        totalN += s.n;
        resp += `${s.sexo}: ${s.n} terneros\n`;
        resp += `  Peso: ${fmt(s.prom)}kg (${fmt(s.min)}-${fmt(s.max)})\n`;
        if (s.gdp_prom && s.gdp_prom < 5) resp += `  GDP promedio: ${fmt(s.gdp_prom*1000)} g/día\n`;
      });
      resp += `\nTotal: ${totalN} terneros destetados`;
      return resp;
    }
    
    // ── DESTETE RANKING ──
    if (tipo === "destete_ranking") {
      const anio = f.anio || accion.anio;
      let whereExtra = ''; const params2 = [];
      if (anio) { whereExtra = " AND p.fecha LIKE ?"; params2.push(`${anio}-%`); }
      const rows = db.prepare(`
        SELECT a.rp, a.sexo, a.pelo, a.padre_rp, p.peso, p.gdp, p.fecha
        FROM pesadas p JOIN animales a ON a.id = p.animal_id 
        WHERE p.contexto = 'DESTETE'${whereExtra} ORDER BY p.peso DESC LIMIT 30
      `).all(...params2);
      if (!rows.length) return `📋 No hay pesadas de destete${anio ? ` en ${anio}` : ''}.`;
      let resp = `🏆 *Ranking Destete${anio ? ` ${anio}` : ''} — Top ${rows.length}*\n\n`;
      rows.forEach((r, i) => {
        resp += `${i+1}. ${r.rp} | ${r.sexo} ${r.pelo||''} | ${fmt(r.peso)}kg | ${r.fecha}`;
        if (r.gdp && r.gdp < 5) resp += ` | GDP: ${fmt(r.gdp*1000)}g/d`;
        if (r.padre_rp) resp += ` | Padre: ${r.padre_rp}`;
        resp += `\n`;
      });
      return resp;
    }
    
    // ── HIJOS DE UN PADRE O MADRE ──
    if (tipo === "hijos_de_padre" || tipo === "hijos_de_madre" || tipo === "hijos") {
      const progenitor = f.padre || f.madre || f.rp || accion.rp || f.nombre;
      if (!progenitor) return "❓ ¿De qué padre o madre querés ver los hijos?";
      const esMadre = tipo === "hijos_de_madre" || f.madre;
      const campo = esMadre ? 'madre_rp' : 'padre_rp';
      // Búsqueda flexible del progenitor
      const hijos = db.prepare(`
        SELECT a.rp, a.sexo, a.pelo, a.categoria, a.fecha_nac, a.estado,
          (SELECT p.peso FROM pesadas p WHERE p.animal_id = a.id AND p.contexto='NACIMIENTO' LIMIT 1) as peso_nac,
          (SELECT p.peso FROM pesadas p WHERE p.animal_id = a.id AND p.contexto='DESTETE' LIMIT 1) as peso_dest
        FROM animales a 
        WHERE (LOWER(a.${campo}) = LOWER(?) OR LOWER(a.${campo}) LIKE LOWER(?))
        ORDER BY a.fecha_nac DESC
      `).all(progenitor, `%${progenitor}%`);
      if (!hijos.length) return `📋 No encontré hijos de "${progenitor}".`;
      const activos = hijos.filter(h => h.estado === 'ACTIVO').length;
      let resp = `👨‍👧 *Hijos de ${progenitor}* — ${hijos.length} (${activos} activos)\n\n`;
      hijos.forEach(h => {
        resp += `🏷️ ${h.rp} | ${h.sexo||'—'} ${h.pelo||''} | ${h.categoria||''}`;
        if (h.fecha_nac) resp += ` | nac ${h.fecha_nac}`;
        if (h.peso_nac) resp += ` | PN:${fmt(h.peso_nac)}`;
        if (h.peso_dest) resp += ` | PD:${fmt(h.peso_dest)}`;
        if (h.estado !== 'ACTIVO') resp += ` | ${h.estado}`;
        resp += `\n`;
      });
      return resp;
    }
    
    // ── PESO POR PADRE ──
    if (tipo === "peso_por_padre") {
      const ctx = f.contexto_pesada || 'DESTETE';
      const anio = f.anio || accion.anio;
      let whereExtra = ''; const params2 = [ctx];
      if (anio) { whereExtra = " AND p.fecha LIKE ?"; params2.push(`${anio}-%`); }
      const rows = db.prepare(`
        SELECT a.padre_rp, COUNT(*) as n, AVG(p.peso) as prom, MIN(p.peso) as min, MAX(p.peso) as max
        FROM pesadas p JOIN animales a ON a.id = p.animal_id 
        WHERE p.contexto = ? AND a.padre_rp IS NOT NULL AND a.padre_rp != ''${whereExtra}
        GROUP BY a.padre_rp ORDER BY prom DESC
      `).all(...params2);
      if (!rows.length) return `📋 No hay pesadas ${ctx}${anio ? ` en ${anio}` : ''} con padre asignado.`;
      let resp = `⚖️ *Peso promedio por Padre — ${ctx}${anio ? ` ${anio}` : ''}*\n\n`;
      rows.forEach((r, i) => {
        resp += `${i+1}. ${r.padre_rp}: ${fmt(r.prom)}kg promedio (${r.n} crías, rango ${fmt(r.min)}-${fmt(r.max)})\n`;
      });
      return resp;
    }
    
    // ── RECRÍA ESTADO ──
    if (tipo === "recria_estado") {
      const animales = db.prepare(`
        SELECT a.*, 
          (SELECT p.peso FROM pesadas p WHERE p.animal_id = a.id ORDER BY p.fecha DESC LIMIT 1) as ult_peso,
          (SELECT p.fecha FROM pesadas p WHERE p.animal_id = a.id ORDER BY p.fecha DESC LIMIT 1) as ult_peso_fecha,
          (SELECT m.valor FROM mediciones m WHERE m.animal_id = a.id AND m.tipo = 'CE' ORDER BY m.fecha DESC LIMIT 1) as ce,
          (SELECT m.valor FROM mediciones m WHERE m.animal_id = a.id AND m.tipo = 'FRAME' ORDER BY m.fecha DESC LIMIT 1) as frame
        FROM animales a WHERE a.estado = 'ACTIVO' AND a.categoria = 'RECRIA' ORDER BY a.rp
      `).all();
      if (!animales.length) return "📋 No hay animales en recría.";
      let resp = `📊 *Estado Recría — ${animales.length} animales*\n\n`;
      animales.forEach(a => {
        const edad = a.fecha_nac ? Math.floor((new Date() - new Date(a.fecha_nac)) / (1000*60*60*24*30.44)) : '?';
        resp += `🏷️ ${a.rp} | ${a.sexo} ${a.pelo||''} | ${edad}m | ${a.destino}`;
        if (a.ult_peso) resp += ` | ${fmt(a.ult_peso)}kg (${a.ult_peso_fecha})`;
        if (a.ce) resp += ` | CE:${a.ce}`;
        if (a.frame) resp += ` | Frame:${a.frame}`;
        resp += `\n`;
      });
      return resp;
    }
    
    // ── RODEO COMPOSICIÓN ──
    if (tipo === "rodeo_composicion") {
      const resumen = getResumenRodeo();
      const porReg = db.prepare("SELECT registro, COUNT(*) as n FROM animales WHERE estado='ACTIVO' GROUP BY registro ORDER BY n DESC").all();
      const porDest = db.prepare("SELECT destino, COUNT(*) as n FROM animales WHERE estado='ACTIVO' GROUP BY destino").all();
      const edades = db.prepare("SELECT categoria, AVG(CAST((julianday('now') - julianday(fecha_nac))/30.44 AS INTEGER)) as edad_prom FROM animales WHERE estado='ACTIVO' AND fecha_nac IS NOT NULL GROUP BY categoria").all();
      let resp = `🐄 *Composición del Rodeo — ${resumen.total} cabezas*\n\n`;
      resp += `Por categoría:\n`;
      const catMap = {};
      resumen.por_categoria.forEach(c => { if(!catMap[c.categoria]) catMap[c.categoria]={m:0,h:0}; if(c.sexo==='MACHO') catMap[c.categoria].m=c.n; else catMap[c.categoria].h=c.n; });
      Object.entries(catMap).forEach(([cat,d]) => {
        const edadInfo = edades.find(e => e.categoria === cat);
        resp += `  ${cat}: ${d.m+d.h} (${d.m}M/${d.h}H)${edadInfo ? ` — edad prom: ${Math.round(edadInfo.edad_prom)}m` : ''}\n`;
      });
      resp += `\nPor registro: ${porReg.map(r => `${r.registro||'s/d'}: ${r.n}`).join(' | ')}`;
      resp += `\nPor destino: ${porDest.map(d => `${d.destino||'s/d'}: ${d.n}`).join(' | ')}`;
      resp += `\nPor pelo: ${resumen.por_pelo.map(p => `${p.pelo||'s/d'}: ${p.n}`).join(' | ')}`;
      return resp;
    }
    
    // ── LOTES ESTADO ──
    if (tipo === "lotes_estado") {
      const lotes = db.prepare("SELECT * FROM lotes ORDER BY nombre").all();
      if (!lotes.length) return "📋 No hay lotes creados.";
      let resp = `🏷️ *Estado de Lotes*\n\n`;
      for (const l of lotes) {
        const animales = db.prepare(`SELECT a.rp, a.categoria, a.sexo FROM animales a JOIN lote_animales la ON la.animal_id = a.id WHERE la.lote_id = ? AND a.estado = 'ACTIVO' ORDER BY a.rp`).all(l.id);
        resp += `📍 ${l.nombre}${l.potrero ? ` — ${l.potrero}` : ''}: ${animales.length} cabezas\n`;
        if (l.descripcion) resp += `   ${l.descripcion}\n`;
        if (animales.length) {
          const cats = {};
          animales.forEach(a => { cats[a.categoria] = (cats[a.categoria]||0)+1; });
          resp += `   ${Object.entries(cats).map(([c,n]) => `${c}: ${n}`).join(' | ')}\n`;
          resp += `   RPs: ${animales.map(a=>a.rp).join(', ')}\n`;
        }
        resp += `\n`;
      }
      // Animales sin lote
      const sinLote = db.prepare("SELECT COUNT(*) as n FROM animales WHERE estado='ACTIVO' AND id NOT IN (SELECT animal_id FROM lote_animales)").get();
      if (sinLote.n) resp += `⚠️ ${sinLote.n} animales sin lote asignado`;
      return resp;
    }
    
    // ── ANIMALES SIN LOTE ──
    if (tipo === "sin_lote") {
      const sinLote = db.prepare(`
        SELECT rp, categoria, sexo, pelo, registro FROM animales 
        WHERE estado='ACTIVO' AND id NOT IN (SELECT animal_id FROM lote_animales)
        ORDER BY categoria, rp
      `).all();
      if (!sinLote.length) return "✅ Todos los animales activos están asignados a un lote.";
      let resp = `⚠️ *Animales sin lote — ${sinLote.length} cabezas*\n\n`;
      sinLote.forEach(a => resp += `🏷️ ${a.rp} | ${a.categoria} | ${a.sexo} | ${a.pelo||'—'} | ${a.registro||'—'}\n`);
      return resp;
    }
    
    // ── SANIDAD COBERTURA ──
    if (tipo === "sanidad_cobertura") {
      const producto = f.producto || f.vacuna;
      if (!producto) {
        // Mostrar resumen de todos los productos
        const prods = db.prepare("SELECT producto, tipo, COUNT(DISTINCT animal_id) as animales, MAX(fecha) as ult_fecha FROM sanidad GROUP BY producto, tipo ORDER BY ult_fecha DESC").all();
        let resp = `💉 *Cobertura Sanitaria*\n\n`;
        prods.forEach(p => resp += `${p.tipo}: ${p.producto||'—'} | ${p.animales} animales | Última: ${p.ult_fecha}\n`);
        return resp;
      }
      // Cobertura de producto específico
      const vacunados = db.prepare(`SELECT DISTINCT a.rp FROM sanidad s JOIN animales a ON a.id = s.animal_id WHERE a.estado='ACTIVO' AND LOWER(s.producto) LIKE LOWER(?)`).all(`%${producto}%`);
      const totalActivos = db.prepare("SELECT COUNT(*) as n FROM animales WHERE estado='ACTIVO'").get();
      const sinVacunar = db.prepare(`SELECT a.rp, a.categoria FROM animales a WHERE a.estado='ACTIVO' AND a.id NOT IN (SELECT DISTINCT animal_id FROM sanidad WHERE LOWER(producto) LIKE LOWER(?))`).all(`%${producto}%`);
      let resp = `💉 *Cobertura: ${producto}*\n\n`;
      resp += `✅ Vacunados: ${vacunados.length}/${totalActivos.n} (${((vacunados.length/totalActivos.n)*100).toFixed(0)}%)\n`;
      if (sinVacunar.length && sinVacunar.length <= 30) {
        resp += `\n❌ Sin ${producto}:\n`;
        sinVacunar.forEach(a => resp += `  ${a.rp} (${a.categoria})\n`);
      } else if (sinVacunar.length > 30) {
        resp += `\n❌ ${sinVacunar.length} animales sin ${producto}`;
      }
      return resp;
    }
    
    // ── SANIDAD HISTORIAL ──
    if (tipo === "sanidad_historial") {
      let where = "1=1"; const params = [];
      if (f.fecha_desde) { where += " AND s.fecha >= ?"; params.push(f.fecha_desde); }
      if (f.fecha_hasta) { where += " AND s.fecha <= ?"; params.push(f.fecha_hasta); }
      const rows = db.prepare(`SELECT s.*, a.rp, a.categoria FROM sanidad s JOIN animales a ON a.id = s.animal_id WHERE ${where} ORDER BY s.fecha DESC LIMIT 100`).all(...params);
      if (!rows.length) return "📋 No hay registros sanitarios en ese período.";
      let resp = `💉 *Historial Sanitario — ${rows.length} registros*\n\n`;
      rows.forEach(s => resp += `${s.fecha} | ${s.rp} | ${s.tipo} | ${s.producto||'—'}${s.dosis ? ` (${s.dosis})` : ''}\n`);
      return resp;
    }
    
    // ── CICLO REPRODUCTIVO COMPLETO ──
    if (tipo === "reproductivo_ciclo") {
      if (!temporada) return "❌ Necesito la temporada (ej: 2025) para el ciclo completo.";
      const totalServ = db.prepare("SELECT COUNT(*) as n FROM servicios WHERE temporada = ?").get(temporada);
      const porRes = db.prepare("SELECT resultado, COUNT(*) as n FROM servicios WHERE temporada = ? GROUP BY resultado").all(temporada);
      const conParto = db.prepare("SELECT COUNT(*) as n, AVG(peso_nacimiento) as peso_prom FROM servicios WHERE temporada = ? AND fecha_parto IS NOT NULL").get(temporada);
      const porToro = db.prepare("SELECT COALESCE(semen_iatf,'—') as toro, COUNT(*) as n FROM servicios WHERE temporada = ? AND semen_iatf IS NOT NULL GROUP BY semen_iatf").all(temporada);
      
      const prenIatf = porRes.find(r=>r.resultado==='PREÑADA_IATF')?.n||0;
      const prenToro = porRes.find(r=>r.resultado==='PREÑADA_TORO')?.n||0;
      const prenLegacy = porRes.find(r=>r.resultado==='PREÑADA')?.n||0;
      const vacias = porRes.find(r=>r.resultado==='VACIA')?.n||0;
      const totalPren = prenIatf + prenToro + prenLegacy;
      const totalDiag = totalPren + vacias || 1;
      
      let resp = `📊 *Ciclo Reproductivo Completo — Temporada ${temporada}*\n\n`;
      resp += `1️⃣ SERVICIO\n  Total servidas: ${totalServ.n}\n`;
      if (porToro.length) { resp += `  Toros IATF: ${porToro.map(t=>`${t.toro}(${t.n})`).join(', ')}\n`; }
      resp += `\n2️⃣ DIAGNÓSTICO\n`;
      resp += `  🤰 Preñadas: ${totalPren} (${(totalPren/totalDiag*100).toFixed(0)}%)\n`;
      resp += `    IATF: ${prenIatf} | Toro: ${prenToro}${prenLegacy ? ` | Sin tipo: ${prenLegacy}` : ''}\n`;
      resp += `  ⚪ Vacías: ${vacias} (${(vacias/totalDiag*100).toFixed(0)}%)\n`;
      resp += `\n3️⃣ PARICIÓN\n`;
      resp += `  Partos registrados: ${conParto.n}\n`;
      if (conParto.peso_prom) resp += `  Peso nac. promedio: ${fmt(conParto.peso_prom)}kg\n`;
      return resp;
    }
    
    return `❌ Tipo de consulta "${tipo}" no reconocido. Tipos disponibles: servicio_resumen, tacto_resumen, vacias, fpp, evaluacion_toros, destete_resumen, rodeo_composicion, lotes_estado, sanidad_cobertura, reproductivo_ciclo`;
  }

  // ── STOCK GENÉTICA ──
  if (accion.accion === "crear_genetica") {
    const { tipo, toro_nombre, toro_rp, donante_nombre, donante_rp, cantidad, costo_unitario } = accion;
    db.prepare("INSERT INTO stock_genetica (tipo,toro_nombre,toro_rp,donante_nombre,donante_rp,cantidad,costo_unitario) VALUES (?,?,?,?,?,?,?)")
      .run(tipo||'PAJUELA', toro_nombre||'', toro_rp||'', donante_nombre||'', donante_rp||'', cantidad||0, costo_unitario||0);
    return `✅ ${tipo||'PAJUELA'}: ${toro_nombre||''} × ${cantidad}\n💰 $${costo_unitario||0} c/u`;
  }

  if (accion.accion === "usar_genetica") {
    const { nombre, cantidad } = accion;
    const item = db.prepare("SELECT * FROM stock_genetica WHERE (LOWER(toro_nombre) LIKE LOWER(?) OR LOWER(donante_nombre) LIKE LOWER(?)) AND cantidad > 0").get(`%${nombre}%`, `%${nombre}%`);
    if (!item) return `❌ No encontré genética de "${nombre}" con stock.`;
    const uso = cantidad || 1;
    if (item.cantidad < uso) return `❌ Solo quedan ${item.cantidad} de ${item.toro_nombre||item.donante_nombre}.`;
    db.prepare("UPDATE stock_genetica SET cantidad = cantidad - ? WHERE id = ?").run(uso, item.id);
    return `✅ Usado: ${uso} ${item.tipo} de ${item.toro_nombre||item.donante_nombre}\n📦 Quedan: ${item.cantidad - uso}`;
  }

  // ── COSTOS FIJOS CAMPO ──
  if (accion.accion === "crear_costo_fijo") {
    const { concepto, monto_mensual, notas } = accion;
    if (!concepto || !monto_mensual) return "❌ Falta concepto y monto mensual.";
    db.prepare("INSERT INTO costos_campo (concepto, monto_mensual, notas) VALUES (?,?,?)")
      .run(concepto, parseFloat(monto_mensual), notas||'');
    const nAnim = db.prepare("SELECT COUNT(*) as n FROM animales WHERE estado = 'ACTIVO'").get().n;
    const porAnimal = nAnim ? (parseFloat(monto_mensual) / nAnim).toFixed(2) : '?';
    return `✅ Costo fijo: ${concepto} $${monto_mensual}/mes\n📊 = $${porAnimal}/animal/mes (${nAnim} cab.)`;
  }

  // ── COSTOS ──
  if (accion.accion === "registrar_costo") {
    const { rp, concepto, monto, detalle, fecha } = accion;
    const animal = buscarAnimalTodos(rp);
    if (!animal) return `❌ No encontré animal ${rp}.`;
    db.prepare("INSERT INTO costos (animal_id,fecha,concepto,detalle,monto) VALUES (?,?,?,?,?)")
      .run(animal.id, fecha||hoy, concepto||'OTROS', detalle||'', parseFloat(monto)||0);
    const total = db.prepare("SELECT SUM(monto) as t FROM costos WHERE animal_id = ?").get(animal.id);
    return `✅ Costo registrado: ${rp} → $${monto} (${concepto})\n💰 Costo acumulado: $${(total.t||0).toFixed(2)}`;
  }

  if (accion.accion === "costo_masivo") {
    const { rps, concepto, monto, detalle, fecha } = accion;
    if (!Array.isArray(rps)) return "❌ Formato inválido.";
    let ok = 0;
    for (const rp of rps) {
      const a = buscarAnimalTodos(rp);
      if (!a) continue;
      db.prepare("INSERT INTO costos (animal_id,fecha,concepto,detalle,monto) VALUES (?,?,?,?,?)")
        .run(a.id, fecha||hoy, concepto||'OTROS', detalle||'', parseFloat(monto)||0);
      ok++;
    }
    return `✅ Costo $${monto} registrado a ${ok} animales (${concepto})`;
  }

  if (accion.accion === "ver_costos") {
    const { rp } = accion;
    if (rp) {
      const animal = buscarAnimalTodos(rp);
      if (!animal) return `❌ No encontré ${rp}.`;
      const costos = db.prepare("SELECT * FROM costos WHERE animal_id = ? ORDER BY fecha DESC LIMIT 10").all(animal.id);
      const total = db.prepare("SELECT SUM(monto) as t FROM costos WHERE animal_id = ?").get(animal.id);
      const ultimoPeso = db.prepare("SELECT peso FROM pesadas WHERE animal_id = ? ORDER BY fecha DESC LIMIT 1").get(animal.id);
      if (!costos.length) return `📊 ${rp}: sin costos registrados.`;
      let resp = `📊 Costos de ${rp}:\n`;
      costos.forEach(c => resp += `  ${c.fecha}: ${c.concepto} $${c.monto} ${c.detalle||''}\n`);
      resp += `\n💰 Total: $${(total.t||0).toFixed(2)}`;
      if (ultimoPeso) resp += `\n📐 Último peso: ${ultimoPeso.peso}kg → Costo/kg: $${((total.t||0)/ultimoPeso.peso).toFixed(2)}`;
      return resp;
    }
    const resumen = db.prepare(`
      SELECT a.rp, a.categoria, SUM(c.monto) as total FROM costos c 
      JOIN animales a ON a.id = c.animal_id GROUP BY c.animal_id ORDER BY total DESC LIMIT 15
    `).all();
    if (!resumen.length) return "📊 No hay costos registrados.";
    let resp = "📊 Top costos por animal:\n";
    resumen.forEach(r => resp += `  ${r.rp} (${r.categoria}): $${r.total.toFixed(2)}\n`);
    return resp;
  }

  // ── VER STOCK ──
  // ── RACIÓN POR LOTE ──
  if (accion.accion === "asignar_racion") {
    const { lote, producto, kg_dia, modo, hasta, desde } = accion;
    if (!lote) return "❌ ¿A qué lote? Decime el nombre.";
    if (!(parseFloat(kg_dia) > 0)) return "❌ ¿Cuántos kg por día?";

    const l = db.prepare("SELECT * FROM lotes WHERE LOWER(nombre) = LOWER(?)").get(String(lote).trim())
           || db.prepare("SELECT * FROM lotes WHERE LOWER(nombre) LIKE LOWER(?)").get(`%${String(lote).trim()}%`);
    if (!l) {
      const todos = db.prepare("SELECT nombre FROM lotes ORDER BY nombre").all().map(x => x.nombre);
      return `❌ No encontré el lote "${lote}".\nLotes: ${todos.join(", ") || "ninguno"}`;
    }
    const n = db.prepare("SELECT COUNT(*) n FROM lote_animales WHERE lote_id = ?").get(l.id).n;
    if (!n) return `❌ El lote ${l.nombre} está vacío.`;

    // Producto: el nombrado, o el único alimento si hay uno solo
    const alims = db.prepare("SELECT * FROM costeo_productos WHERE tipo = 'ALIMENTO' ORDER BY producto").all();
    let prod = null;
    if (producto) {
      const q = String(producto).trim().toLowerCase();
      prod = alims.find(a => a.producto.toLowerCase() === q)
          || alims.find(a => a.producto.toLowerCase().includes(q) || q.includes(a.producto.toLowerCase()));
    }
    if (!prod && alims.length === 1) prod = alims[0];
    if (!prod) {
      if (!alims.length) return "❌ No hay alimentos sincronizados desde IMPROLUX.\nCargalo allá con rubro *Alimento* y volvé a intentar.";
      return `❓ ¿Cuál alimento?\n${alims.map(a => `  • ${a.producto} (${Math.round(a.stock_improlux)} ${a.unidad})`).join("\n")}`;
    }

    const md = String(modo || "TOTAL").toUpperCase() === "POR_ANIMAL" ? "POR_ANIMAL" : "TOTAL";
    const kd = parseFloat(kg_dia);
    db.prepare("UPDATE costeo_dietas SET activo = 0, fecha_hasta = ? WHERE lote_id = ? AND activo = 1").run(hoy, l.id);
    db.prepare(`INSERT INTO costeo_dietas (lote_id,producto,modo,kg_dia,fecha_desde,fecha_hasta)
      VALUES (?,?,?,?,?,?)`).run(l.id, prod.producto, md, kd, desde || hoy, hasta || null);

    const kgTot = md === "POR_ANIMAL" ? kd * n : kd;
    const usd = kgTot * (prod.costo_unitario || 0);
    const dias = kgTot > 0 && prod.stock_improlux > 0 ? Math.floor(prod.stock_improlux / kgTot) : null;
    return `✅ *${l.nombre}* → ${prod.producto}\n` +
      `🌾 ${md === "POR_ANIMAL" ? `${kd} kg/cabeza` : `${kd} kg al lote`} = *${kgTot.toFixed(0)} kg/día* entre ${n} cab.\n` +
      (prod.costo_unitario ? `💰 US$ ${usd.toFixed(2)}/día · US$ ${(usd / n).toFixed(3)} por cabeza\n` : `⚠️ Sin precio en IMPROLUX\n`) +
      (dias !== null ? `📦 Stock alcanza ${dias} días` : "") +
      (hasta ? `\n📅 Hasta ${hasta}` : "");
  }

  if (accion.accion === "cortar_racion") {
    const { lote } = accion;
    if (!lote) return "❌ ¿De qué lote?";
    const l = db.prepare("SELECT * FROM lotes WHERE LOWER(nombre) LIKE LOWER(?)").get(`%${String(lote).trim()}%`);
    if (!l) return `❌ No encontré el lote "${lote}".`;
    const r = db.prepare("UPDATE costeo_dietas SET activo = 0, fecha_hasta = ? WHERE lote_id = ? AND activo = 1").run(hoy, l.id);
    return r.changes ? `✅ Ración cortada en ${l.nombre}. Deja de consumir y de sumar costo.`
                     : `${l.nombre} no tenía ración asignada.`;
  }

  if (accion.accion === "ver_racion") {
    const ds = db.prepare(`SELECT l.nombre lote, d.producto, d.modo, d.kg_dia, d.fecha_hasta,
        (SELECT COUNT(*) FROM lote_animales la WHERE la.lote_id = d.lote_id) n,
        (SELECT costo_unitario FROM costeo_productos p WHERE UPPER(TRIM(p.producto)) = UPPER(TRIM(d.producto))) costo,
        (SELECT stock_improlux FROM costeo_productos p WHERE UPPER(TRIM(p.producto)) = UPPER(TRIM(d.producto))) stock
      FROM costeo_dietas d JOIN lotes l ON l.id = d.lote_id WHERE d.activo = 1 ORDER BY l.nombre`).all();
    if (!ds.length) return "🌾 Ningún lote tiene ración asignada. Todo a pasto.";
    let kgTot = 0, usdTot = 0, r = "🌾 *RACIÓN POR LOTE*\n";
    ds.forEach(d => {
      const kg = d.modo === "POR_ANIMAL" ? d.kg_dia * d.n : d.kg_dia;
      const usd = kg * (d.costo || 0);
      kgTot += kg; usdTot += usd;
      r += `\n*${d.lote}* (${d.n} cab.)\n  ${d.producto} · ${kg.toFixed(0)} kg/día`;
      r += d.costo ? ` · US$ ${usd.toFixed(2)}/día` : " · ⚠️ sin precio";
      if (d.fecha_hasta) r += `\n  📅 hasta ${d.fecha_hasta}`;
      r += "\n";
    });
    r += `\n📊 *Total: ${kgTot.toFixed(0)} kg/día · US$ ${usdTot.toFixed(2)}/día*`;
    r += `\n   US$ ${(usdTot * 30).toFixed(0)} por mes`;
    return r;
  }

  if (accion.accion === "ver_stock") {
    // Insumos y alimento se consultan en IMPROLUX. Acá: genética y ración por lote.
    let resp = "📦 *STOCK ADE*\n";
    const genetica = db.prepare("SELECT * FROM stock_genetica WHERE cantidad > 0 ORDER BY tipo, toro_nombre").all();
    if (genetica.length) {
      resp += "\n🧬 *Genética:*\n";
      genetica.forEach(g => resp += `  ${g.tipo}: ${g.toro_nombre}${g.donante_nombre ? ` x ${g.donante_nombre}` : ''} → ${g.cantidad}u\n`);
    }
    try {
      const ds = db.prepare(`SELECT l.nombre lote, d.producto, d.modo, d.kg_dia,
          (SELECT COUNT(*) FROM lote_animales la WHERE la.lote_id = d.lote_id) n,
          (SELECT stock_improlux FROM costeo_productos p WHERE UPPER(TRIM(p.producto)) = UPPER(TRIM(d.producto))) stock
        FROM costeo_dietas d JOIN lotes l ON l.id = d.lote_id WHERE d.activo = 1`).all();
      if (ds.length) {
        let tot = 0;
        resp += "\n🌾 *Ración por lote:*\n";
        ds.forEach(d => {
          const kg = d.modo === "POR_ANIMAL" ? d.kg_dia * d.n : d.kg_dia;
          tot += kg;
          resp += `  ${d.lote}: ${kg.toFixed(0)} kg/día (${d.producto})\n`;
        });
        const st = ds[0] && ds[0].stock ? ds[0].stock : 0;
        resp += `  Total: ${tot.toFixed(0)} kg/día${st && tot ? ` → ${Math.floor(st / tot)} días de stock` : ''}\n`;
      }
    } catch (e) {}
    resp += "\n💊 Insumos y alimento: se consultan en IMPROLUX.";
    return resp;
  }

  // ── VER ALERTAS ──
  if (accion.accion === "ver_alertas") {
    const alertas = [];
    // Terneros pendientes destete
    const ternDest = db.prepare(`SELECT a.rp, CAST((julianday(?) - julianday(a.fecha_nac)) AS INTEGER) as dias
      FROM animales a WHERE a.estado='ACTIVO' AND a.fecha_nac IS NOT NULL
      AND CAST((julianday(?)-julianday(a.fecha_nac)) AS INTEGER) BETWEEN 160 AND 300
      AND (SELECT COUNT(*) FROM pesadas WHERE animal_id=a.id AND contexto='DESTETE')=0`).all(hoy, hoy);
    if (ternDest.length) alertas.push(`🍼 ${ternDest.length} terneros pendientes de destete: ${ternDest.map(t=>`${t.rp}(${t.dias}d)`).join(', ')}`);
    // Ración: días de stock según lo que consume cada lote (stock real en IMPROLUX)
    try {
      const ds = db.prepare(`SELECT d.producto,
          SUM(CASE WHEN d.modo='POR_ANIMAL'
            THEN d.kg_dia * (SELECT COUNT(*) FROM lote_animales la WHERE la.lote_id = d.lote_id)
            ELSE d.kg_dia END) kg_dia,
          (SELECT stock_improlux FROM costeo_productos p WHERE UPPER(TRIM(p.producto)) = UPPER(TRIM(d.producto))) stock
        FROM costeo_dietas d WHERE d.activo = 1 GROUP BY UPPER(TRIM(d.producto))`).all();
      ds.forEach(x => {
        if (x.kg_dia > 0 && x.stock > 0) {
          const d = Math.floor(x.stock / x.kg_dia);
          if (d < 15) alertas.push(`${d < 7 ? '🚨' : '⚠️'} ${x.producto}: ${d} días de stock (${Math.round(x.stock)} kg en IMPROLUX)`);
        }
      });
    } catch (e) {}
    if (!alertas.length) return "✅ No hay alertas pendientes. Todo en orden.";
    return `🔔 *ALERTAS (${alertas.length}):*\n\n${alertas.join('\n')}`;
  }

  // ── VER PRODUCTIVIDAD ──
  if (accion.accion === "ver_productividad") {
    let resp = "📊 *PRODUCTIVIDAD*\n";
    const gdp = db.prepare(`SELECT a.padre_rp as padre, COUNT(DISTINCT a.id) as n, AVG(p.gdp) as gdp
      FROM animales a JOIN pesadas p ON p.animal_id=a.id WHERE a.padre_rp IS NOT NULL AND a.padre_rp!='' AND p.gdp>0 AND p.gdp<=2.5
      GROUP BY a.padre_rp HAVING n>=2 ORDER BY gdp DESC LIMIT 10`).all();
    if (gdp.length) {
      resp += "\n🐂 *GDP por padre:*\n";
      gdp.forEach(g => resp += `  ${g.padre}: ${(g.gdp*1000).toFixed(0)}g/d (${g.n} hijos)\n`);
    }
    const ppd = db.prepare(`SELECT a.rp, AVG(CASE WHEN p2.contexto='DESTETE' THEN p2.peso END) as ppd,
      COUNT(CASE WHEN p2.contexto='DESTETE' THEN 1 END) as n
      FROM animales a JOIN animales h ON h.madre_rp=a.rp JOIN pesadas p2 ON p2.animal_id=h.id
      WHERE a.categoria='VACA' GROUP BY a.id HAVING n>=1 ORDER BY ppd DESC LIMIT 10`).all();
    if (ppd.length) {
      resp += "\n🏆 *Ranking vacas por PPD:*\n";
      ppd.forEach(v => resp += `  ${v.rp}: ${v.ppd?.toFixed(0)||'?'}kg (${v.n} crías)\n`);
    }
    return resp;
  }

  // ── REPRODUCCIÓN (protocolo nuevo) ──
  if (accion.accion === "registrar_parto_protocolo") {
    if (!reproduccion) return "El módulo de reproducción no está disponible.";
    const r = reproduccion.registrarParto(db, {
      verificarRpUnico: rp => rpEnOtroCampo(campoActualKey || CAMPO_DEFAULT, rp),
      madre_rp: accion.madre_rp, rp_cria: accion.rp_cria, fecha: accion.fecha || hoy,
      peso_nac: accion.peso_nac, pelo: accion.pelo, sexo: accion.sexo, notas: accion.notas
    });
    return r.ok ? r.mensaje : `❌ ${r.error}`;
  }

  if (accion.accion === "estado_protocolo") {
    if (!reproduccion) return "El módulo de reproducción no está disponible.";
    const abiertos = db.prepare("SELECT id FROM repro_protocolos WHERE cerrado=0 ORDER BY id DESC").all();
    if (!abiertos.length) return "No hay protocolos de servicio abiertos.";
    let resp = "";
    for (const a of abiertos) {
      const e = reproduccion.estado(db, a.id);
      resp += `🐄 *${e.nombre} — ${e.temporada}*\nPaso ${e.paso} de ${e.de}: ${e.etapa}\n`;
      if (e.toro_iatf) resp += `IATF: ${e.toro_iatf.nombre}${e.fpp_iatf ? ` · FPP ${e.fpp_iatf}` : ""}\n`;
      if (e.toro_repaso) resp += `Repaso: ${e.toro_repaso.nombre}\n`;
      resp += `${e.total_vacas} vacas`;
      if (e.prenadas_iatf || e.prenadas_repaso) resp += ` → ${e.prenadas_iatf} IATF, ${e.prenadas_repaso} repaso, ${e.vacias} vacías`;
      if (e.pendientes.length) resp += `\n⏳ Falta ${e.pendientes.join("; ")}`;
      resp += "\n\n";
    }
    return resp.trim();
  }

  // ── CONSISTENCIA ──
  if (accion.accion === "revisar_consistencia") {
    if (!consistencia) return "El control de consistencia no está disponible.";
    const h = consistencia.revisar(db);
    ultimaRevision = h;
    return consistencia.resumir(h);
  }

  if (accion.accion === "corregir_inconsistencias") {
    if (!consistencia) return "El control de consistencia no está disponible.";
    const pendientes = (ultimaRevision || []).filter(x => x.correccion);
    if (!pendientes.length) return "No hay nada que pueda corregir solo. Pedime primero que revise los datos.";
    const hechas = [];
    for (const p of pendientes) {
      const r = consistencia.corregir(db, p.correccion);
      hechas.push(r.ok ? `✅ ${r.mensaje}` : `⚠️ ${r.error}`);
    }
    ultimaRevision = null;
    return `Corregí ${hechas.filter(x => x.startsWith("✅")).length} de ${pendientes.length}:\n\n${hechas.join("\n")}`;
  }

  // ── EVENTOS DE CAMPO ──
  if (accion.accion === "registrar_evento") {
    const { rp, descripcion, tipo } = accion;
    if (!rp || !descripcion) return "❌ Falta RP y descripción del evento.";
    const animal = buscarAnimalTodos(rp);
    if (!animal) return `❌ No encontré animal ${rp}.`;
    db.prepare("INSERT INTO eventos (animal_id, fecha, tipo, descripcion) VALUES (?, ?, ?, ?)")
      .run(animal.id, hoy, tipo || 'OBSERVACION', descripcion);
    return `✅ Evento registrado para ${rp}:\n📋 ${descripcion}\n📅 ${hoy}`;
  }

  if (accion.accion === "ver_eventos") {
    const { rp } = accion;
    if (!rp) return "❌ Falta RP.";
    const animal = buscarAnimalTodos(rp);
    if (!animal) return `❌ No encontré animal ${rp}.`;
    const eventos = db.prepare("SELECT * FROM eventos WHERE animal_id = ? ORDER BY fecha DESC LIMIT 20").all(animal.id);
    if (!eventos.length) return `📋 ${rp}: sin eventos registrados.`;
    let resp = `📋 *Eventos de ${rp}:*\n\n`;
    eventos.forEach(e => resp += `  ${e.fecha} · ${e.tipo} · ${e.descripcion}\n`);
    return resp;
  }

  if (accion.accion === "texto") return accion.mensaje;
  return "No entendí eso. Intentá de nuevo.";
}

// ── WEBHOOK WHATSAPP (Twilio) ─────────────────────────────────────────────────
app.post("/webhook", async (req, res) => {
  if (!twilio) return res.status(500).send("Twilio no configurado");
  const twiml = new twilio.twiml.MessagingResponse();
  try {
    const body = (req.body.Body || "").trim();
    const from = req.body.From || "";
    const to = req.body.To || "";
    
    // Resolver campo por número de WhatsApp
    const campoKey = campoFromWhatsApp(to);
    const campoDb = getDB(campoKey);
    const campoInfo = CAMPOS[campoKey] || CAMPOS[CAMPO_DEFAULT];
    // Override req y db global
    req.campoDB = campoDb;
    req.campoKey = campoKey;
    req.campoInfo = campoInfo;
    db = campoDb;
    
    const usuario = `whatsapp-${campoKey}`;
    console.log(`[WA] ${from} → ${to} → campo: ${campoKey} (${campoInfo.nombre})`);
    if (!body) { twiml.message("Escribí algo para comenzar."); return res.type("text/xml").send(twiml.toString()); }

    // ── INTERCEPT 1: SERVICIO ──
    if (/servicio|iatf.*fecha|toro\s*repaso/i.test(body) && /RP\s+[A-Za-z0-9]/i.test(body)) {
      const hoy = new Date().toISOString().split("T")[0];
      const rpM = body.match(/RP\s+([A-Za-z0-9]+)/i);
      if (rpM) {
        const animal = buscarAnimal(rpM[1]);
        if (animal) {
          const tempM = body.match(/temporada\s+(\d{4})/i);
          const iatfM = body.match(/IATF\s+([A-Z][A-Za-z0-9]+)/i);
          const fechas = body.match(/(\d{4}-\d{2}-\d{2})/g) || [];
          const repasoM = body.match(/repaso\s+([A-Za-z0-9]+)/i);
          const ccM = body.match(/CC\s+(\d+\.?\d*)/i);
          const temporada = tempM ? tempM[1] : new Date().getFullYear().toString();
          db.prepare("INSERT INTO servicios (animal_id,temporada,tipo_servicio,semen_iatf,fecha_iatf,toro_natural,fecha_ingreso_toro,cc_pre,notas) VALUES (?,?,?,?,?,?,?,?,?)")
            .run(animal.id, temporada, iatfM?'IATF':'NATURAL', iatfM?iatfM[1]:null, fechas[0]||null, repasoM?repasoM[1]:null, fechas[1]||null, ccM?parseFloat(ccM[1]):null, 'WhatsApp');
          let resp = `✅ Servicio registrado!\n🏷️ RP ${animal.rp} | Temporada ${temporada}`;
          if (iatfM) resp += `\n🧬 IATF: ${iatfM[1]}${fechas[0]?' ('+fechas[0]+')':''}`;
          if (repasoM) resp += `\n🐂 Repaso: ${repasoM[1]}${fechas[1]?' (desde '+fechas[1]+')':''}`;
          twiml.message(resp);
          return res.type("text/xml").send(twiml.toString());
        }
      }
    }

    // ── INTERCEPT 2: MASIVOS ──
    const rpListMatch = body.match(/RP[:\s]+(.+?)$/i);
    const rpList = rpListMatch ? rpListMatch[1].split(/[,\s]+/).map(r => r.trim()).filter(r => r && /^[A-Za-z0-9]+$/.test(r)) : [];
    if (rpList.length >= 2) {
      let fecha = new Date().toISOString().split("T")[0];
      if (/baja\s+(?:por\s+)?(venta|muerte)/i.test(body)) {
        const motivo = (body.match(/(venta|muerte)/i)||['','VENTA'])[1].toUpperCase();
        const estado = motivo === 'MUERTE' ? 'MUERTO' : 'VENDIDO';
        let ok=0, errs=[];
        for (const rp of rpList) { const a=buscarAnimal(rp); if(!a){errs.push(rp);continue;} db.prepare("UPDATE animales SET estado=?,fecha_salida=?,motivo_salida=? WHERE id=?").run(estado,fecha,motivo,a.id); ok++; }
        let resp = `📤 Baja masiva: ${ok} animales (${motivo})`;
        if (errs.length) resp += `\n⚠️ No encontrados: ${errs.join(', ')}`;
        twiml.message(resp);
        return res.type("text/xml").send(twiml.toString());
      }
      if (/(?:vacun[eéa]|apliqu[eé]|aplicar|desparasit[eé])/i.test(body) && !/servicio|iatf|toro\s*repaso/i.test(body)) {
        const pm = body.match(/(?:vacun[eéa]\w*|apliqu[eé]|aplicar|desparasit[eé]\w*)\s+(?:con\s+)?(.+?)\s+(?:a\s+|para\s+)/i);
        const producto = pm ? pm[1].trim() : 'Tratamiento';
        let ok=0, errs=[];
        for (const rp of rpList) { const a=buscarAnimal(rp); if(!a){errs.push(rp);continue;} db.prepare("INSERT INTO sanidad (animal_id,fecha,tipo,producto,notas) VALUES(?,?,'TRATAMIENTO',?,'WhatsApp')").run(a.id,fecha,producto); ok++; }
        let resp = `💉 Sanidad: ${ok} registrados con ${producto}`;
        if (errs.length) resp += `\n⚠️ No encontrados: ${errs.join(', ')}`;
        twiml.message(resp);
        return res.type("text/xml").send(twiml.toString());
      }
    }

    // ── Flujo normal: ACK inmediato + procesamiento asincrónico con tool-use ──
    // Respondemos YA a Twilio (vacío) para no chocar con timeout de ~15s,
    // y enviamos la respuesta real por API cuando termine.
    res.type("text/xml").send("<Response></Response>");

    // Procesar en background
    procesarMensajeAsync(campoDb, campoKey, usuario, body, from, to).catch(err => {
      console.error("Error procesamiento async:", err);
      enviarWhatsApp(from, to, "❌ Error procesando. Intentá de nuevo.", campoKey);
    });
    return;
  } catch (err) {
    console.error("Error webhook WhatsApp:", err);
    twiml.message("❌ Error. Intentá de nuevo.");
  }
  res.type("text/xml").send(twiml.toString());
});

// Enviar mensaje por API de Twilio (parte mensajes largos)
async function enviarWhatsApp(to, from, texto, campoKey) {
  const client = getTwilioClient(campoKey) || twilioClient;
  if (!client) { console.error("twilioClient no configurado"); return; }
  // to = destinatario (el "from" original del usuario), from = nuestro número (el "to" original)
  const partes = [];
  let t = texto || "Sin respuesta.";
  while (t.length > 1500) {
    let corte = t.lastIndexOf("\n", 1500);
    if (corte < 500) corte = 1500;
    partes.push(t.substring(0, corte));
    t = t.substring(corte).trim();
  }
  if (t) partes.push(t);
  for (const parte of partes) {
    try {
      await client.messages.create({ from, to, body: parte });
    } catch(e) { console.error("Error enviando WhatsApp:", e.message); }
  }
}

// Procesar mensaje con tool-use loop (Sonnet consulta la base libremente)
// Motor de respuesta: prompt + esquema + herramientas + ejecución de acciones.
// Devuelve el texto final. Lo usan el webhook de WhatsApp y el webhook interno.
async function responderConIA(campoDb, campoKey, usuario, body) {
  db = campoDb;
  let historial = getHistorial(campoDb, usuario);
  historial.push({ role: "user", content: body });

  // ── INTERCEPT LLUVIA → diario del sistema financiero ──
  if (esConsultaLluvia(body)) {
    const r = await consultarLluvia(campoKey, body);
    historial.push({ role: "assistant", content: r });
    saveHistorial(campoDb, usuario, historial);
    return r;
  }
  {
    const ll = detectarLluvia(body);
    if (ll) {
      const r = await registrarLluviaImprolux(ll.mm, campoKey, "", ll.fecha);
      historial.push({ role: "assistant", content: r });
      saveHistorial(campoDb, usuario, historial);
      return r;
    }
  }

  const contexto = buildContexto() + "\n\n" + DB_SCHEMA_DOC;
  let respuesta = "";
  let mensajes = historial.slice(-6);

  // Loop de tool-use: mientras el modelo pida consultar datos, ejecutamos y devolvemos
  let iteraciones = 0;
  const MAX_ITER = 6;
  try {
    while (iteraciones < MAX_ITER) {
      iteraciones++;
      const result = await anthropic.messages.create({
        model: "claude-sonnet-4-6",
        max_tokens: 1500,
        system: contexto,
        tools: [HERRAMIENTA_CONSULTA],
        messages: mensajes,
      });

      if (result.stop_reason === "tool_use") {
        // Agregar la respuesta del asistente (con el tool_use) al historial de la conversación
        mensajes.push({ role: "assistant", content: result.content });
        const toolResults = [];
        for (const block of result.content) {
          if (block.type === "tool_use" && block.name === "consultar_datos") {
            const resultado = ejecutarConsultaSQL(block.input.sql);
            console.log(`[SQL] ${block.input.sql} → ${resultado.filas ?? resultado.error}`);
            toolResults.push({
              type: "tool_result",
              tool_use_id: block.id,
              content: JSON.stringify(resultado).substring(0, 8000)
            });
          }
        }
        mensajes.push({ role: "user", content: toolResults });
        continue; // volver a llamar al modelo con los resultados
      }

      // stop_reason normal: extraer texto o acción JSON
      const textBlock = result.content.find(b => b.type === "text");
      const rawRespuesta = textBlock ? textBlock.text.trim() : "";

      // ¿Es una acción JSON de registro? (registrar pesada, servicio, etc.)
      const limpio = rawRespuesta.replace(/```json\s*/gi, "").replace(/```\s*/g, "").trim();
      const jsonObjects = [];
      let depth = 0, start = -1;
      for (let i = 0; i < limpio.length; i++) {
        if (limpio[i] === '{') { if (depth === 0) start = i; depth++; }
        else if (limpio[i] === '}') { depth--; if (depth === 0 && start >= 0) { jsonObjects.push(limpio.substring(start, i + 1)); start = -1; } }
      }
      const acciones = [];
      for (const js of jsonObjects) {
        try { const a = JSON.parse(js); if (a && a.accion) acciones.push(a); } catch {}
      }
      if (acciones.length >= 1) {
        const res = [];
        for (const a of acciones) res.push(await ejecutarAccionAsync(a));
        respuesta = res.join("\n\n");
      } else {
        respuesta = limpio || rawRespuesta;
      }
      // Guardar en historial la respuesta final en texto
      historial.push({ role: "assistant", content: respuesta });
      break;
    }
  } catch(apiErr) {
    console.error("Error en tool-use loop:", apiErr.message);
    // Retry simple sin tools ni historial
    try {
      const r2 = await anthropic.messages.create({
        model: "claude-sonnet-4-6",
        max_tokens: 800,
        system: contexto,
        messages: [{ role: "user", content: body }],
      });
      respuesta = (r2.content.find(b=>b.type==="text")?.text || "").trim();
      saveHistorial(campoDb, usuario, []);
    } catch(e2) {
      respuesta = "❌ Error de conexión con el asistente. Intentá de nuevo.";
    }
  }

  saveHistorial(campoDb, usuario, historial);
  return respuesta || "No entendí. Intentá de nuevo.";
}

// El webhook de Twilio: usa el motor y manda la respuesta.
async function procesarMensajeAsync(campoDb, campoKey, usuario, body, from, to) {
  const respuesta = await responderConIA(campoDb, campoKey, usuario, body);
  await enviarWhatsApp(from, to, respuesta, campoKey);
}

// ── WEBHOOK INTERNO (bot web) ─────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// IMPORTACIÓN INTELIGENTE DE ARCHIVOS
// La IA interpreta el archivo y propone; el usuario confirma; recién ahí se
// escribe. Ninguna planilla entra a la base sin que alguien haya dicho que sí.
// ─────────────────────────────────────────────────────────────────────────────

function guardarPendienteImport(usuario, tipo, archivo, filas, plan) {
  db.prepare("DELETE FROM importaciones_pendientes WHERE usuario = ?").run(usuario);
  db.prepare(`INSERT INTO importaciones_pendientes (usuario,tipo,archivo,filas,plan)
              VALUES (?,?,?,?,?)`)
    .run(usuario, tipo, archivo || "", JSON.stringify(filas), JSON.stringify(plan || {}));
}

function leerPendienteImport(usuario) {
  const r = db.prepare(`SELECT * FROM importaciones_pendientes
                        WHERE usuario = ? AND created_at > datetime('now','-2 hours')
                        ORDER BY id DESC LIMIT 1`).get(usuario);
  if (!r) return null;
  try { return { ...r, filas: JSON.parse(r.filas), plan: JSON.parse(r.plan || "{}") }; }
  catch (e) { return null; }
}

function borrarPendienteImport(usuario) {
  db.prepare("DELETE FROM importaciones_pendientes WHERE usuario = ?").run(usuario);
}

// Escribe lo confirmado. Todo dentro de una transacción: si algo revienta a la
// mitad, no queda media planilla cargada.
function ejecutarImport(tipo, filas) {
  let ok = 0, sinAnimal = [], errores = [];
  const hoy = new Date().toISOString().slice(0, 10);

  const correr = db.transaction(() => {
    for (const f of filas) {
      try {
        if (tipo === "animales") {
          const ya = buscarAnimal(f.rp);
          if (ya) { errores.push(`${f.rp} ya existe`); continue; }
          db.prepare(`INSERT INTO animales (rp,chip,sexo,fecha_nac,raza,registro,categoria,madre_rp,padre_rp,hbu,notas,fecha_ingreso)
                      VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`)
            .run(f.rp, f.chip || null, (f.sexo || "H").toUpperCase().startsWith("M") ? "M" : "H",
                 f.fecha_nac || null, f.raza || "A. ANGUS", f.registro || null, f.categoria || null,
                 f.madre_rp || null, f.padre_rp || null, f.hbu || null,
                 f.notas || "Importado", f.fecha_ingreso || hoy);
          ok++;
          continue;
        }

        const animal = buscarAnimal(f.rp);
        if (!animal) { sinAnimal.push(f.rp); continue; }

        if (tipo === "pesadas") {
          const info = db.prepare("INSERT INTO pesadas (animal_id,fecha,peso,contexto,notas) VALUES (?,?,?,?,?)")
            .run(animal.id, f.fecha || hoy, f.peso, f.contexto || null, f.notas || "Importado");
          const gdp = calcularGDP(animal.id);
          if (gdp !== null && gdp !== undefined) {
            db.prepare("UPDATE pesadas SET gdp = ? WHERE id = ?").run(gdp, info.lastInsertRowid);
          }
        } else if (tipo === "mediciones") {
          db.prepare("INSERT INTO mediciones (animal_id,fecha,tipo,valor,notas) VALUES (?,?,?,?,?)")
            .run(animal.id, f.fecha || hoy, (f.tipo || "CE").toUpperCase(), f.valor, f.notas || "Importado");
        } else if (tipo === "sanidad") {
          db.prepare("INSERT INTO sanidad (animal_id,fecha,tipo,producto,dosis,notas) VALUES (?,?,?,?,?,?)")
            .run(animal.id, f.fecha || hoy, (f.tipo || "TRATAMIENTO").toUpperCase(),
                 f.producto || null, f.dosis || null, f.notas || "Importado");
        }
        ok++;
      } catch (e) {
        errores.push(`${f.rp}: ${e.message}`);
      }
    }
  });
  correr();

  return { ok, sinAnimal, errores };
}

function esConfirmacion(texto) {
  return /^\s*(s[ií]|si+|dale|confirmo?|confirmar|ok|okey|adelante|carg[aá](los|las)?|proced[eé])\s*[.!]?\s*$/i.test(texto);
}
function esNegativa(texto) {
  return /^\s*(no+|cancel[aá]r?|dej[aá]|par[aá]|olvidalo|nada)\s*[.!]?\s*$/i.test(texto);
}

app.post("/webhook-interno", async (req, res) => {
  try {
    const body = (req.body.Body || "").trim();
    // Cada usuario de WhatsApp tiene su propia importación pendiente; el chat
    // web comparte una sola, como hasta ahora.
    const usuario = req.body.usuario || "amakaik-web";
    const fileData = req.body.FileData;
    const fileName = req.body.FileName || "";

    // ── ¿EN QUÉ CAMPO TRABAJA? ──
    // Con varios campos por empresa, cargar en el equivocado es un problema
    // serio: el bot interpreta lo que le digan, lo recuerda, y si no está
    // claro pregunta antes de escribir nada.
    const permitidos = String(req.query.campos_permitidos || req.body.campos_permitidos || "")
      .split(",").map(x => x.trim()).filter(x => x && CAMPOS[x]);

    if (permitidos.length > 1) {
      const nombresCampos = permitidos.map(k => CAMPOS[k].nombre);

      // 1. ¿Lo nombró en el mensaje?
      const mencionado = campoMencionado(body, permitidos);
      if (mencionado) {
        fijarCampoSesion(usuario, mencionado);
        req.campoKey = mencionado;
        req.campoDB = getDB(mencionado);
        db = req.campoDB;
        // Si sólo pidió cambiar, se confirma y listo.
        const sinTilde = body.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
        if (/^\s*(cambi|pasa|anda|ir a|vamos|trabaj|usa|selecc|elegi|poner?te?)/i.test(sinTilde) && body.length < 60) {
          return res.json({ respuesta: `📍 Listo, ahora trabajo en *${CAMPOS[mencionado].nombre}*.` });
        }
      } else {
        // 2. ¿Quedó uno de antes en esta conversación?
        const recordado = campoDeSesion(usuario);
        if (recordado && permitidos.includes(recordado)) {
          req.campoKey = recordado;
          req.campoDB = getDB(recordado);
          db = req.campoDB;
        } else if (esOperacionDeCarga(body) || fileData || (req.body.adjuntos || []).length) {
          // 3. Va a escribir datos y no sabemos dónde: se pregunta.
          return res.json({ respuesta:
            `📍 ¿En qué campo lo cargo?\n\n` +
            nombresCampos.map((n, i) => `${i + 1}️⃣ ${n}`).join("\n") +
            `\n\nRespondé con el nombre o el número. Lo recuerdo para el resto de la charla.` });
        }
      }
    }

    // Respuesta corta con un número, contestando la pregunta de arriba.
    if (permitidos.length > 1 && /^\s*[1-9]\s*$/.test(body)) {
      const elegido = permitidos[parseInt(body.trim()) - 1];
      if (elegido) {
        fijarCampoSesion(usuario, elegido);
        return res.json({ respuesta: `📍 Listo, trabajo en *${CAMPOS[elegido].nombre}*. Repetime lo que querías cargar.` });
      }
    }

    // ── ¿Está contestando una propuesta de importación? ──
    const pendiente = leerPendienteImport(usuario);
    if (pendiente && !fileData) {
      if (esConfirmacion(body)) {
        const r = ejecutarImport(pendiente.tipo, pendiente.filas);
        borrarPendienteImport(usuario);
        let msg = `✅ Cargué ${r.ok} de ${pendiente.filas.length}.`;
        if (r.sinAnimal.length) {
          const m = [...new Set(r.sinAnimal)].slice(0, 10).join(", ");
          msg += `\n\n⚠️ ${r.sinAnimal.length} no están en el inventario: ${m}${r.sinAnimal.length > 10 ? "…" : ""}`;
          msg += `\nSi querés darlos de alta, mandame la planilla de animales.`;
        }
        if (r.errores.length) msg += `\n\n⚠️ ${r.errores.length} con error: ${r.errores.slice(0, 3).join(" | ")}`;
        return res.json({ respuesta: msg });
      }
      if (esNegativa(body)) {
        borrarPendienteImport(usuario);
        return res.json({ respuesta: "Listo, no cargué nada. El archivo queda descartado." });
      }
      // Cualquier otra cosa: sigue su curso normal y la propuesta queda esperando.
    }

    // ── ¿Vino una foto? Vision la lee ──
    const adjuntos = Array.isArray(req.body.adjuntos) ? req.body.adjuntos : [];
    const foto = adjuntos.find(a => a.clase === "imagen" && a.base64);
    if (foto && importador && !fileData) {
      try {
        const rps = new Set(db.prepare("SELECT rp FROM animales WHERE estado='ACTIVO'").all().map(a => a.rp));
        const r = await importador.analizarImagen({
          base64: foto.base64, tipo: foto.tipo, instruccion: body, anthropic, rpsExistentes: rps
        });
        if (!r.ok || !r.filas.length) return res.json({ respuesta: r.mensaje });
        guardarPendienteImport(usuario, r.tipo, foto.nombre || "foto", r.filas, r.plan);
        console.log(`[IMPORT] foto: ${r.tipo}, ${r.filas.length} filas, esperando confirmación de ${usuario}`);
        return res.json({ respuesta: r.mensaje });
      } catch (e) {
        console.error("[IMPORT foto]", e);
        return res.json({ respuesta: `No pude leer la foto: ${e.message}` });
      }
    }

    // ── ¿Vino un archivo? La IA lo lee y propone qué hacer ──
    if (fileData && importador) {
      try {
        const buf = Buffer.from(fileData, "base64");
        const rps = new Set(db.prepare("SELECT rp FROM animales WHERE estado='ACTIVO'").all().map(a => a.rp));
        const r = await importador.analizar({
          buffer: buf,
          nombre: fileName,
          instruccion: body,
          anthropic,
          rpsExistentes: rps
        });

        if (!r.ok) return res.json({ respuesta: r.mensaje });
        if (!r.filas.length) return res.json({ respuesta: r.mensaje });

        guardarPendienteImport(usuario, r.tipo, fileName, r.filas, r.plan);
        console.log(`[IMPORT] ${fileName}: ${r.tipo}, ${r.filas.length} filas, esperando confirmación de ${usuario}`);
        return res.json({ respuesta: r.mensaje });

      } catch (e) {
        console.error("[IMPORT]", e);
        return res.json({ respuesta: `No pude leer el archivo: ${e.message}` });
      }
    }

    if (!body) return res.json({ respuesta: "Escribí algo para comenzar." });

    // ── INTERCEPT 0: LLUVIA → diario del sistema financiero ──
    {
      const campoKeyLl = req.body.campo || req.query.campo || CAMPO_DEFAULT;
      if (esConsultaLluvia(body)) {
        return res.json({ respuesta: await consultarLluvia(campoKeyLl, body) });
      }
      const ll = detectarLluvia(body);
      if (ll) {
        return res.json({ respuesta: await registrarLluviaImprolux(ll.mm, campoKeyLl, "", ll.fecha) });
      }
    }


    // ── INTERCEPT 1: SERVICIO (prioridad máxima, antes de todo) ──
    if (/servicio|iatf.*fecha|toro\s*repaso/i.test(body) && /RP\s+[A-Za-z0-9]/i.test(body)) {
      const hoy = new Date().toISOString().split("T")[0];
      const rpM = body.match(/RP\s+([A-Za-z0-9]+)/i);
      if (rpM) {
        const animal = buscarAnimal(rpM[1]);
        if (animal) {
          const tempM = body.match(/temporada\s+(\d{4})/i);
          const iatfM = body.match(/IATF\s+([A-Z][A-Za-z0-9]+)/i);
          const fechas = body.match(/(\d{4}-\d{2}-\d{2})/g) || [];
          const repasoM = body.match(/repaso\s+([A-Za-z0-9]+)/i);
          const ccM = body.match(/CC\s+(\d+\.?\d*)/i);
          const temporada = tempM ? tempM[1] : new Date().getFullYear().toString();

          db.prepare("INSERT INTO servicios (animal_id,temporada,tipo_servicio,semen_iatf,fecha_iatf,toro_natural,fecha_ingreso_toro,cc_pre,notas) VALUES (?,?,?,?,?,?,?,?,?)")
            .run(animal.id, temporada, iatfM?'IATF':'NATURAL', iatfM?iatfM[1]:null, fechas[0]||null, repasoM?repasoM[1]:null, fechas[1]||null, ccM?parseFloat(ccM[1]):null, 'Manual');

          let resp = `✅ Servicio registrado!\n🏷️ RP ${animal.rp} | Temporada ${temporada}`;
          if (iatfM) resp += `\n🧬 IATF: ${iatfM[1]}${fechas[0]?' ('+fechas[0]+')':''}`;
          if (repasoM) resp += `\n🐂 Repaso: ${repasoM[1]}${fechas[1]?' (desde '+fechas[1]+')':''}`;
          if (ccM) resp += `\n📊 CC: ${ccM[1]}`;
          return res.json({ respuesta: resp });
        } else {
          return res.json({ respuesta: `❌ No encontré animal con RP "${rpM[1]}"` });
        }
      }
    }

    // ── INTERCEPT 2: operaciones masivas directo sin Haiku ──
    // Extraer RPs del mensaje — buscar patrones como "RP: S510, S512, S514" o "S510 S512 S514"
    let rpList = [];
    const rpListMatch = body.match(/RP[:\s]+([A-Za-z0-9,\s]+?)(?:\s+(?:se\s|baja|vacun|apliqu|desparasit|alicort|inyect|tratamiento|ivermec|producto|con\s|$))/i);
    if (rpListMatch) {
      rpList = rpListMatch[1].split(/[,\s]+/).map(r => r.trim()).filter(r => r && /^[A-Za-z0-9]+$/.test(r));
    }
    // Fallback: "Registrar X para los RP: S510, S512" → RPs al final
    if (rpList.length < 2) {
      const rpFinalMatch = body.match(/RP[:\s]+([A-Za-z0-9][A-Za-z0-9,\s]+)$/i);
      if (rpFinalMatch) {
        rpList = rpFinalMatch[1].split(/[,\s]+/).map(r => r.trim()).filter(r => r && /^[A-Za-z0-9]+$/.test(r));
      }
    }

    if (rpList.length >= 2) {
      // Extraer fecha
      let fecha = new Date().toISOString().split("T")[0];
      const fm = body.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
      if (fm) { let [_,d,m,y]=fm; if(y.length===2) y=(parseInt(y)>50?'19':'20')+y; fecha=parseInt(d)>31?`${d}-${m.padStart(2,'0')}-${y}`:`${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`; }

      // Baja masiva
      if (/baja\s+(?:por\s+)?(venta|muerte)/i.test(body)) {
        const motivo = (body.match(/(venta|muerte)/i)||['','VENTA'])[1].toUpperCase();
        const estado = motivo === 'MUERTE' ? 'MUERTO' : 'VENDIDO';
        let ok=0, errs=[];
        for (const rp of rpList) { const a=buscarAnimal(rp); if(!a){errs.push(rp);continue;} db.prepare("UPDATE animales SET estado=?,fecha_salida=?,motivo_salida=? WHERE id=?").run(estado,fecha,motivo,a.id); ok++; }
        let resp = `📤 Baja masiva: ${ok} animales (${motivo}) | 📅 ${fecha}`;
        if (errs.length) resp += `\n⚠️ No encontrados: ${errs.join(', ')}`;
        return res.json({ respuesta: resp });
      }

      // Sanidad masiva — detectar por producto o acción de sanidad en el texto
      const textoSinRPs = body.replace(/RP[:\s]+[A-Za-z0-9,\s]+/i, '').trim();
      const esSanidad = /(?:vacun[eéa]|apliqu[eé]|aplicar|desparasit[eé]|alicort|inyect|dosifi|ivermec|dectomax|bagomec|pour.?on|antiparasit|tratamiento|sanidad|registrar\s)/i.test(body) && !/servicio|iatf|toro\s*repaso/i.test(body);
      if (esSanidad) {
        let producto = 'Tratamiento', dosis = null;
        
        // Buscar dosis (Xcc, X ml)
        const dosisMatch = body.match(/(\d+\.?\d*)\s*(?:cc|ml|cm3)/i);
        if (dosisMatch) dosis = dosisMatch[0].trim();
        
        // Buscar producto conocido
        const prodMatch = body.match(/(?:ivermectin\w*[\s.]*[\d.]*|dectomax|bagomec\w*|pour.?on|doramectin\w*|aftosa|brucelosis|clostridi\w*|carbuncl\w*|vitamina\w*)/i);
        if (prodMatch) producto = prodMatch[0].trim();
        
        // Si no encontró, buscar después de "de" o "con"
        if (producto === 'Tratamiento') {
          const deMatch = body.match(/(?:de|con)\s+([a-záéíóúA-Z][a-záéíóúA-Z0-9\s.]+?)(?:\s*$|\s+(?:para|a\s+los))/i);
          if (deMatch) producto = deMatch[1].trim();
        }
        
        // Si aún no, buscar "Registrar X para"
        if (producto === 'Tratamiento') {
          const regMatch = body.match(/(?:registrar|aplicar)\s+(.+?)\s+(?:para|a)\s+/i);
          if (regMatch) producto = regMatch[1].trim();
        }
        
        let ok=0, errs=[];
        for (const rp of rpList) { const a=buscarAnimal(rp); if(!a){errs.push(rp);continue;} db.prepare("INSERT INTO sanidad (animal_id,fecha,tipo,producto,dosis,notas) VALUES(?,?,'TRATAMIENTO',?,?,'Masivo')").run(a.id,fecha,producto,dosis); ok++; }
        let resp = `💉 Sanidad: ${ok} registrados | ${producto}${dosis ? ' ('+dosis+')' : ''} | ${fecha}`;
        if (errs.length) resp += `\n⚠️ No encontrados: ${errs.join(', ')}`;
        return res.json({ respuesta: resp });
      }
    }

    // ── INTERCEPT: baja individual directo ──
    if (/baja\s+(?:por\s+)?(venta|muerte)/i.test(body) && !rpListMatch) {
      const rpM = body.match(/RP\s+([A-Za-z0-9]+)/i);
      if (rpM) {
        const animal = buscarAnimal(rpM[1]);
        if (animal) {
          const motivo = (body.match(/(venta|muerte)/i)||['','VENTA'])[1].toUpperCase();
          const estado = motivo === 'MUERTE' ? 'MUERTO' : 'VENDIDO';
          let fecha = new Date().toISOString().split("T")[0];
          const fM = body.match(/(\d{4}-\d{2}-\d{2})/); if (fM) fecha = fM[1];
          db.prepare("UPDATE animales SET estado=?,fecha_salida=?,motivo_salida=? WHERE id=?").run(estado,fecha,motivo,animal.id);
          return res.json({ respuesta: `📤 Baja: RP ${animal.rp} | ${motivo} | 📅 ${fecha}` });
        }
      }
    }

    // ── ARCHIVO ADJUNTO sin el importador inteligente ──
    // El parser viejo sólo lee RP y peso: cargaría animales con sexo, fecha,
    // madre y padre vacíos. Mejor frenar y decir qué falta.
    if (fileData && !importador) {
      return res.json({ respuesta:
        "⚠️ No puedo leer archivos: falta el módulo importador.js en el servidor.\n\n" +
        "Subilo al repositorio, al lado de server.js, y volvé a intentar. " +
        "Sin él sólo podría cargar el RP y el peso, dejando el resto de los datos vacíos." });
    }

    if (false) {
      try {
        const buf = Buffer.from(fileData, 'base64');
        let datos = [];
        
        if (fileName.endsWith('.xlsx') || fileName.endsWith('.xls')) {
          if (!ExcelJS) return res.json({ respuesta: "❌ ExcelJS no disponible." });
          const wb = new ExcelJS.Workbook();
          await wb.xlsx.load(buf);
          const ws = wb.getWorksheet(1);
          let headerRow = 1;
          const firstVal = ws.getCell(1, 1).value;
          if (firstVal && String(firstVal).includes('Sesion')) headerRow = 2;
          const headers = [];
          ws.getRow(headerRow).eachCell((cell, col) => {
            headers[col] = String(cell.value || '').toLowerCase().trim()
              .replace(/[áà]/g,'a').replace(/[éè]/g,'e').replace(/[íì]/g,'i').replace(/[óò]/g,'o').replace(/[úù]/g,'u')
              .replace(/\s+/g,'_');
          });
          for (let r = headerRow + 1; r <= ws.rowCount; r++) {
            const row = ws.getRow(r);
            const obj = {};
            let hasData = false;
            headers.forEach((h, col) => {
              if (h && row.getCell(col).value !== null && row.getCell(col).value !== undefined) {
                obj[h] = row.getCell(col).value;
                hasData = true;
              }
            });
            if (hasData) datos.push(obj);
          }
        } else if (fileName.endsWith('.csv')) {
          const lines = buf.toString('utf-8').split('\n').filter(l => l.trim());
          if (lines.length < 2) return res.json({ respuesta: "❌ CSV vacío." });
          const hdrs = lines[0].split(',').map(h => h.trim().toLowerCase().replace(/\s+/g,'_'));
          for (let i = 1; i < lines.length; i++) {
            const vals = lines[i].split(',');
            const obj = {};
            hdrs.forEach((h, j) => { if (vals[j]?.trim()) obj[h] = vals[j].trim(); });
            if (Object.keys(obj).length) datos.push(obj);
          }
        } else if (fileName.endsWith('.pdf') && pdfjsLib) {
          // Parsear PDF: extraer texto y convertir en tabla
          const pdfDoc = await pdfjsLib.getDocument({data: new Uint8Array(buf)}).promise;
          let allText = '';
          for (let i = 1; i <= pdfDoc.numPages; i++) {
            const page = await pdfDoc.getPage(i);
            const content = await page.getTextContent();
            allText += content.items.map(item => item.str).join(' ') + '\n';
          }
          
          // Separar por múltiples espacios → array de valores
          const parts = allText.split(/\s{2,}/).map(p => p.trim()).filter(p => p);
          
          // Detectar headers: buscar fila con RP, PESO, SEXO, etc
          let headerStart = -1;
          let headerEnd = -1;
          for (let i = 0; i < Math.min(parts.length, 30); i++) {
            if (/^RP$/i.test(parts[i])) { headerStart = i; break; }
          }
          
          if (headerStart >= 0) {
            // Buscar fin de headers: hasta encontrar un valor que parece dato (número, fecha, nombre de animal)
            const headers = [];
            for (let i = headerStart; i < parts.length; i++) {
              const p = parts[i];
              // Si es un valor numérico o fecha, terminaron los headers
              if (/^\d{1,2}\s+(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)/i.test(p)) { headerEnd = i; break; }
              if (i > headerStart && /^\d+$/.test(p) && parseInt(p) < 200) { headerEnd = i; break; }
              if (i > headerStart && /^[A-Z]\d{3,}$/i.test(p)) { headerEnd = i; break; }
              headers.push(p.toLowerCase().replace(/\s+/g,'_').replace(/[áà]/g,'a').replace(/[éè]/g,'e').replace(/[íì]/g,'i').replace(/[óò]/g,'o').replace(/[úù]/g,'u'));
            }
            
            if (headerEnd < 0) headerEnd = headerStart + headers.length;
            const numCols = headers.length;
            console.log(`[PDF] Headers (${numCols}): ${headers.join(', ')}`);
            
            // Leer datos en bloques de numCols
            const dataValues = parts.slice(headerEnd);
            for (let i = 0; i + numCols - 1 < dataValues.length; i += numCols) {
              const obj = {};
              let hasData = false;
              for (let j = 0; j < numCols; j++) {
                if (dataValues[i + j]) {
                  obj[headers[j]] = dataValues[i + j];
                  hasData = true;
                }
              }
              if (hasData) datos.push(obj);
            }
          } else {
            // No encontré headers, intentar formato libre línea por línea
            const lines = allText.split('\n').filter(l => l.trim());
            for (const line of lines) {
              const vals = line.split(/\s{2,}/).map(v => v.trim()).filter(v => v);
              if (vals.length >= 3) {
                datos.push({ rp: vals[0], valor1: vals[1], valor2: vals[2], extra: vals.slice(3).join(' ') });
              }
            }
          }
        }
        
        if (!datos.length) return res.json({ respuesta: `❌ No pude leer datos de ${fileName}.` });
        
        const sample = datos[0];
        const keys = Object.keys(sample);
        const rpKey = keys.find(k => /rp|numero.*etiqueta|arete|tag|id_animal/i.test(k));
        const pesoKey = keys.find(k => /peso|weight|kg/i.test(k));
        const ceKey = keys.find(k => /c\.?e\.?|circ.*escrot|scrotal/i.test(k));
        const fechaKey = keys.find(k => /fecha|date/i.test(k));
        
        console.log(`[FILE] ${fileName}: ${datos.length} filas, keys=${keys.join(',')}, rp=${rpKey}, peso=${pesoKey}, ce=${ceKey}`);
        
        const instruccion = body.toLowerCase();
        const soloExistentes = /solo.*exist|que.*est[eé]n|solo.*cargados|que ya|solo los que/i.test(instruccion);
        const esDestete = /destete/i.test(instruccion);
        const esAno = /a[ñn]o|12.*mes/i.test(instruccion);
        const es18m = /18\s*mes/i.test(instruccion);
        const esNac = /nacimiento|nac\b/i.test(instruccion);
        const esAdulta = /adulta|vaca/i.test(instruccion);
        let ctxForzado = esDestete?'DESTETE':esAno?'AÑO':es18m?'18MESES':esNac?'NACIMIENTO':esAdulta?'ADULTA':null;
        
        if (rpKey && pesoKey) {
          let okPes=0, okCe=0, noEnc=[], errores=[];
          const fecha = fechaKey ? String(datos[0][fechaKey]).slice(0,10) : new Date().toISOString().slice(0,10);
          
          const rpsEnDB = new Set();
          if (soloExistentes) {
            db.prepare("SELECT rp FROM animales").all().forEach(a => rpsEnDB.add(a.rp.toUpperCase()));
          }
          
          for (const d of datos) {
            let rp = String(d[rpKey]||'').trim();
            if (!rp) {
              const notas = keys.find(k => /notas|notes/i.test(k));
              if (notas && d[notas] && /NuevoArete:|Nuevo Arete:/i.test(String(d[notas]))) {
                rp = String(d[notas]).split(':')[1].trim();
              }
            }
            if (!rp) continue;
            if (soloExistentes && !rpsEnDB.has(rp.toUpperCase())) { noEnc.push(rp); continue; }
            
            const animal = buscarAnimalTodos(rp);
            if (!animal) { noEnc.push(rp); continue; }
            
            const peso = parseFloat(d[pesoKey]);
            if (!peso || isNaN(peso)) continue;
            const fP = d[fechaKey] ? String(d[fechaKey]).slice(0,10) : fecha;
            let ctx = ctxForzado || determinarContextoPesada(animal.fecha_nac, fP, animal.categoria);
            
            try {
              db.prepare("INSERT INTO pesadas (animal_id,fecha,peso,contexto) VALUES (?,?,?,?)").run(animal.id,fP,peso,ctx);
              okPes++;
            } catch(e) { errores.push(rp); }
            
            if (ceKey && d[ceKey]) {
              const ce = parseFloat(d[ceKey]);
              if (ce && !isNaN(ce)) {
                try { db.prepare("INSERT INTO mediciones (animal_id,fecha,tipo,valor) VALUES (?,?,'CE',?)").run(animal.id,fP,ce); okCe++; } catch(e) {}
              }
            }
          }
          
          let resp = `📊 ${fileName}\n⚖️ ${okPes} pesadas (${ctxForzado||'auto'}) | 📅 ${fecha}`;
          if (okCe) resp += `\n📐 ${okCe} mediciones CE`;
          if (noEnc.length) resp += `\n⚠️ ${noEnc.length} ignorados${soloExistentes?' (no existen)':''}: ${noEnc.slice(0,10).join(', ')}${noEnc.length>10?'...':''}`;
          return res.json({ respuesta: resp });
        }
        
        return res.json({ respuesta: `📄 ${fileName}: ${datos.length} filas\n📋 Columnas: ${keys.join(', ')}\n\nDecime qué querés cargar (ej: "cargá pesadas al destete", "solo los que ya están").` });
      } catch(e) {
        console.error("Error archivo:", e);
        return res.json({ respuesta: `❌ Error: ${e.message}` });
      }
    }

    // ── Flujo normal: el MISMO motor que el webhook de WhatsApp ──
    // Antes esta ruta llamaba a la IA sin herramientas, así que para contestar
    // sólo podía elegir una "acción" enlatada: por eso una pregunta puntual
    // ("preñadas por LUCUMA") devolvía el listado completo. Con el motor, la
    // IA consulta la base por SQL y responde exactamente lo que se le pide.
    const campoKeyIA = req.body.campo || req.query.campo || CAMPO_DEFAULT;
    const respuestaIA = await responderConIA(req.campoDB, campoKeyIA, usuario, body);
    return res.json({ respuesta: respuestaIA });

  } catch (err) {
    console.error("Error webhook-interno:", err);
    res.json({ respuesta: `❌ Error: ${err.message || 'Intentá de nuevo.'}` });
  }
});

// ── API REST ──────────────────────────────────────────────────────────────────
// Exportar todos los animales activos a CSV
// Exportación completa para catálogos. Acepta una lista de RPs (o de chips)
// y devuelve todo lo que hay de cada animal: pedigrí con nombres y HBU, pesos
// por etapa, mediciones, estado reproductivo y costeo. Pensado para bajarlo y
// trabajarlo afuera.
app.post("/api/exportar/seleccion", (req, res) => {
  const refs = (req.body.animales || []).map(x => String(x).trim()).filter(Boolean);
  const formato = (req.body.formato || "csv").toLowerCase();
  if (!refs.length) return res.status(400).json({ error: "Sin animales" });

  const ph = refs.map(() => "?").join(",");
  const rows = db.prepare(`
    SELECT a.id, a.rp, a.nombre, a.chip, a.hbu, a.fecha_nac, a.sexo, a.pelo,
           a.categoria, a.registro, a.raza, a.destino, a.estado, a.notas,
           a.madre_rp, a.padre_rp,
      COALESCE(a.madre_nombre, (SELECT m.nombre FROM animales m WHERE m.rp = a.madre_rp LIMIT 1)) as madre_nombre,
      COALESCE(a.madre_hbu,    (SELECT m.hbu    FROM animales m WHERE m.rp = a.madre_rp LIMIT 1)) as madre_hbu,
      COALESCE(a.padre_nombre, (SELECT p.nombre FROM animales p WHERE p.rp = a.padre_rp LIMIT 1)) as padre_nombre,
      COALESCE(a.padre_hbu,    (SELECT p.hbu    FROM animales p WHERE p.rp = a.padre_rp LIMIT 1)) as padre_hbu,
      a.madre_hba, a.padre_hba,
      (SELECT l.nombre FROM lote_animales la JOIN lotes l ON l.id=la.lote_id WHERE la.animal_id=a.id) as lote,
      (SELECT p.peso  FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='NACIMIENTO' ORDER BY p.fecha LIMIT 1) as peso_nac,
      (SELECT p.peso  FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='DESTETE'    ORDER BY p.fecha LIMIT 1) as peso_dest,
      (SELECT p.fecha FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='DESTETE'    ORDER BY p.fecha LIMIT 1) as fecha_dest,
      (SELECT p.peso  FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='AÑO'        ORDER BY p.fecha LIMIT 1) as peso_año,
      (SELECT p.peso  FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='18MESES'    ORDER BY p.fecha LIMIT 1) as peso_18m,
      (SELECT AVG(p.peso) FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='ADULTA') as pva,
      (SELECT p.peso  FROM pesadas p WHERE p.animal_id=a.id ORDER BY p.fecha DESC LIMIT 1) as ultimo_peso,
      (SELECT p.fecha FROM pesadas p WHERE p.animal_id=a.id ORDER BY p.fecha DESC LIMIT 1) as fecha_ultimo_peso,
      (SELECT p.gdp   FROM pesadas p WHERE p.animal_id=a.id AND p.gdp IS NOT NULL ORDER BY p.fecha DESC LIMIT 1) as gdp,
      (SELECT COUNT(*) FROM pesadas p WHERE p.animal_id=a.id) as n_pesadas,
      (SELECT m.valor FROM mediciones m WHERE m.animal_id=a.id AND m.tipo='CE'        ORDER BY m.fecha DESC LIMIT 1) as ce,
      (SELECT m.fecha FROM mediciones m WHERE m.animal_id=a.id AND m.tipo='CE'        ORDER BY m.fecha DESC LIMIT 1) as fecha_ce,
      (SELECT m.valor FROM mediciones m WHERE m.animal_id=a.id AND m.tipo='ALTURA'    ORDER BY m.fecha DESC LIMIT 1) as altura,
      (SELECT m.valor FROM mediciones m WHERE m.animal_id=a.id AND m.tipo='CC'        ORDER BY m.fecha DESC LIMIT 1) as cc,
      (SELECT m.valor FROM mediciones m WHERE m.animal_id=a.id AND m.tipo='FRAME'     ORDER BY m.fecha DESC LIMIT 1) as frame,
      (SELECT m.valor FROM mediciones m WHERE m.animal_id=a.id AND m.tipo='DOCILIDAD' ORDER BY m.fecha DESC LIMIT 1) as docilidad,
      (SELECT COUNT(*) FROM servicios s WHERE s.animal_id=a.id) as n_servicios,
      (SELECT s.resultado FROM servicios s WHERE s.animal_id=a.id ORDER BY s.created_at DESC, s.id DESC LIMIT 1) as ultimo_resultado,
      (SELECT s.semen_iatf FROM servicios s WHERE s.animal_id=a.id AND s.semen_iatf IS NOT NULL ORDER BY s.id DESC LIMIT 1) as ultimo_semen,
      (SELECT s.fecha_parto FROM servicios s WHERE s.animal_id=a.id AND s.fecha_parto IS NOT NULL ORDER BY s.fecha_parto DESC LIMIT 1) as ultimo_parto,
      (SELECT COUNT(*) FROM animales h WHERE h.madre_rp = a.rp) as n_hijos,
      (SELECT COUNT(*) FROM sanidad sa WHERE sa.animal_id=a.id) as n_sanidad
    FROM animales a
    WHERE a.rp IN (${ph}) OR a.chip IN (${ph})
    ORDER BY a.categoria, a.rp`).all(...refs, ...refs);

  // Saldo en kgNE y costos, si el módulo de costeo está activo
  for (const r of rows) {
    try {
      const k = db.prepare(`SELECT kgne_saldo, etapa FROM costeo_kgne
        WHERE animal_id = ? ORDER BY fecha DESC, id DESC LIMIT 1`).get(r.id);
      if (k) { r.kgne_saldo = Math.round(k.kgne_saldo); r.etapa = k.etapa; }
      const c = db.prepare("SELECT COALESCE(SUM(monto),0) t FROM costos WHERE animal_id = ?").get(r.id);
      if (c) r.costos_usd = Math.round(c.t * 100) / 100;
    } catch (e) {}
    if (r.fecha_nac) {
      const d = Math.floor((Date.now() - new Date(r.fecha_nac)) / 86400000);
      r.edad_dias = d; r.edad_meses = Math.round(d / 30.44 * 10) / 10;
    }
  }

  if (formato === "json") return res.json({ animales: rows, total: rows.length });

  const cols = [
    ["rp","RP"],["nombre","Nombre"],["chip","Chip"],["hbu","HBU"],
    ["fecha_nac","Fecha Nac"],["edad_meses","Edad meses"],
    ["sexo","Sexo"],["pelo","Pelo"],["categoria","Categoria"],["registro","Registro"],
    ["raza","Raza"],["destino","Destino"],["estado","Estado"],["lote","Lote"],
    ["padre_rp","Padre RP"],["padre_nombre","Padre nombre"],["padre_hbu","Padre HBU"],["padre_hba","Padre HBA"],
    ["madre_rp","Madre RP"],["madre_nombre","Madre familia"],["madre_hbu","Madre HBU"],["madre_hba","Madre HBA"],
    ["peso_nac","Peso nac"],["peso_dest","Peso destete"],["fecha_dest","Fecha destete"],
    ["peso_año","Peso año"],["peso_18m","Peso 18m"],["pva","PVA"],
    ["ultimo_peso","Ultimo peso"],["fecha_ultimo_peso","Fecha ult peso"],["gdp","GDP"],["n_pesadas","N pesadas"],
    ["ce","CE"],["fecha_ce","Fecha CE"],["altura","Altura"],["cc","CC"],["frame","Frame"],["docilidad","Docilidad"],
    ["n_servicios","N servicios"],["ultimo_resultado","Ultimo resultado"],["ultimo_semen","Ultimo semen"],
    ["ultimo_parto","Ultimo parto"],["n_hijos","N hijos"],["n_sanidad","N sanidad"],
    ["etapa","Etapa costeo"],["kgne_saldo","Saldo kgNE"],["costos_usd","Costos USD"],
    ["notas","Notas"]
  ];
  const esc = v => {
    if (v === null || v === undefined) return '';
    const t = String(v);
    return /[",\n]/.test(t) ? `"${t.replace(/"/g,'""')}"` : t;
  };
  const r1 = v => v != null ? Math.round(v * 10) / 10 : '';
  let csv = cols.map(c => c[1]).join(',') + '\n';
  for (const r of rows) {
    r.pva = r1(r.pva); r.cc = r1(r.cc); r.gdp = r.gdp != null ? Math.round(r.gdp * 1000) / 1000 : '';
    csv += cols.map(c => esc(r[c[0]])).join(',') + '\n';
  }
  const fecha = new Date().toISOString().slice(0,10);
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="catalogo_${rows.length}animales_${fecha}.csv"`);
  res.send('\ufeff' + csv);
});

app.get("/api/exportar/animales", (req, res) => {
  const soloActivos = req.query.todos !== '1';
  const rows = db.prepare(`
    SELECT a.rp, a.chip, a.fecha_nac, a.sexo, a.pelo, a.categoria, a.registro, a.destino,
      a.madre_rp, a.padre_rp, a.hbu, a.estado,
      (SELECT l.nombre FROM lote_animales la JOIN lotes l ON l.id=la.lote_id WHERE la.animal_id=a.id) as lote,
      (SELECT p.peso FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='NACIMIENTO' LIMIT 1) as peso_nac,
      (SELECT p.peso FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='DESTETE' LIMIT 1) as peso_dest,
      (SELECT AVG(p.peso) FROM pesadas p WHERE p.animal_id=a.id AND p.contexto='ADULTA') as pva,
      (SELECT p.peso FROM pesadas p WHERE p.animal_id=a.id ORDER BY p.fecha DESC LIMIT 1) as ultimo_peso,
      (SELECT p.fecha FROM pesadas p WHERE p.animal_id=a.id ORDER BY p.fecha DESC LIMIT 1) as fecha_ultimo_peso,
      (SELECT m.valor FROM mediciones m WHERE m.animal_id=a.id AND m.tipo='CE' ORDER BY m.fecha DESC LIMIT 1) as ce,
      (SELECT AVG(m.valor) FROM mediciones m WHERE m.animal_id=a.id AND m.tipo='CC') as cc,
      (SELECT COUNT(*) FROM servicios s WHERE s.animal_id=a.id) as n_servicios
    FROM animales a
    ${soloActivos ? "WHERE a.estado='ACTIVO'" : ""}
    ORDER BY a.categoria, a.rp
  `).all();

  const cols = ['rp','chip','fecha_nac','sexo','pelo','categoria','registro','destino','madre_rp','padre_rp','hbu','lote','peso_nac','peso_dest','pva','ultimo_peso','fecha_ultimo_peso','ce','cc','n_servicios','estado'];
  const header = ['RP','Chip','Fecha Nac','Sexo','Pelo','Categoria','Registro','Destino','Madre RP','Padre RP','HBU','Lote','Peso Nac','Peso Destete','PVA','Ultimo Peso','Fecha Ult Peso','CE','CC','Servicios','Estado'];
  
  const esc = (v) => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return s.includes(',') || s.includes('"') ? `"${s.replace(/"/g,'""')}"` : s;
  };
  const fmt2 = (v) => v != null ? Math.round(v*10)/10 : '';
  
  let csv = header.join(',') + '\n';
  for (const r of rows) {
    r.pva = fmt2(r.pva); r.cc = fmt2(r.cc);
    csv += cols.map(c => esc(r[c])).join(',') + '\n';
  }
  
  const campo = req.query.campo || CAMPO_DEFAULT;
  const fecha = new Date().toISOString().slice(0,10);
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="animales_${campo}_${fecha}.csv"`);
  res.send('\ufeff' + csv); // BOM para que Excel abra bien los acentos
});

// ── RODEO RESUMEN (para sincronización con IMPROLUX) ─────────────────────────
// Usa la categoría que ADE ya tiene guardada (VACA/TORO/VAQUILLONA/TERNERO/RECRIA/
// NOVILLO). Divide Terneros/Terneras por sexo. Agrupa por categoría × pedigree
// (PP/SA/GENERAL) contando PLANTEL y VENTA por separado (destino).
//   → rodeo: [{ categoria, registro, plantel, venta }]
app.get("/api/rodeo-resumen", (req, res) => {
  const animales = db.prepare("SELECT categoria, registro, destino, sexo FROM animales WHERE estado = 'ACTIVO'").all();
  const grupos = {};
  const normReg = (r) => {
    const u = (r || '').toUpperCase().trim();
    return (u === 'PP' || u === 'SA') ? u : 'GENERAL';
  };
  const add = (categoria, registro, destino) => {
    const reg = normReg(registro);
    const k = categoria + '|' + reg;
    if (!grupos[k]) grupos[k] = { categoria, registro: reg, plantel: 0, venta: 0 };
    if ((destino || '').toUpperCase().trim() === 'VENTA') grupos[k].venta++; else grupos[k].plantel++;
  };
  for (const a of animales) {
    const cs = (a.categoria || '').toUpperCase().trim();
    const sx = (a.sexo || '').toUpperCase().trim();
    let cat;
    if (cs === 'VACA') cat = 'Vacas';
    else if (cs === 'TORO') cat = 'Toros';
    else if (cs === 'NOVILLO') cat = 'Novillos';
    else if (cs === 'VAQUILLONA') cat = 'Vaquillonas';
    else if (cs === 'TERNERO' || cs === 'RECRIA') cat = (sx === 'HEMBRA') ? 'Terneras' : 'Terneros';
    else cat = a.categoria || 'Sin categoría';
    add(cat, a.registro, a.destino);
  }
  const ordenCat = { 'Toros': 1, 'Vacas': 2, 'Vaquillonas': 3, 'Recría': 4, 'Terneros': 5, 'Terneras': 6, 'Novillos': 7, 'Sin categoría': 9 };
  const ordenReg = { 'PP': 1, 'SA': 2, 'GENERAL': 3 };
  const rows = Object.values(grupos).sort((a, b) =>
    (ordenReg[a.registro] || 4) - (ordenReg[b.registro] || 4) || (ordenCat[a.categoria] || 8) - (ordenCat[b.categoria] || 8)
  );
  res.json({
    campo: req.campoKey,
    total: rows.reduce((s, r) => s + r.plantel + r.venta, 0),
    rodeo: rows
  });
});

app.get("/api/animales", (req, res) => {
  const { categoria, sexo, estado, limite, buscar } = req.query;
  // Búsqueda por texto
  if (buscar) {
    const term = `%${buscar}%`;
    const rows = db.prepare(`
      SELECT * FROM animales WHERE estado = 'ACTIVO' AND 
      (rp LIKE ? OR chip LIKE ? OR madre_rp LIKE ? OR padre_rp LIKE ? OR notas LIKE ?)
      ORDER BY rp LIMIT 50
    `).all(term, term, term, term, term);
    return res.json(rows);
  }
  let where = "1=1";
  const params = [];
  if (estado) { where += " AND UPPER(estado) = UPPER(?)"; params.push(estado); }
  else { where += " AND estado = 'ACTIVO'"; }
  if (categoria) { where += " AND UPPER(categoria) = UPPER(?)"; params.push(categoria); }
  if (sexo) { where += " AND UPPER(sexo) = UPPER(?)"; params.push(sexo); }
  params.push(parseInt(limite) || 500);
  const rows = db.prepare(`SELECT * FROM animales WHERE ${where} ORDER BY rp LIMIT ?`).all(...params);
  res.json(rows);
});

// ── BAJA MASIVA (debe ir ANTES de /api/animales/:rp) ──
app.post("/api/animales/baja", (req, res) => {
  const { rps, motivo, fecha } = req.body;
  if (!Array.isArray(rps) || !rps.length) return res.status(400).json({ error: "Enviá al menos un RP" });
  if (!motivo) return res.status(400).json({ error: "Falta motivo (VENTA o MUERTE)" });
  
  const estado = motivo.toUpperCase() === 'MUERTE' ? 'MUERTO' : 'VENDIDO';
  const fechaBaja = fecha || new Date().toISOString().slice(0, 10);
  let ok = 0, errores = [];
  
  for (const rp of rps) {
    const animal = buscarAnimalTodos(rp);
    if (!animal) { errores.push(rp); continue; }
    db.prepare("UPDATE animales SET estado = ?, fecha_salida = ?, motivo_salida = ? WHERE id = ?")
      .run(estado, fechaBaja, motivo.toUpperCase(), animal.id);
    db.prepare("DELETE FROM lote_animales WHERE animal_id = ?").run(animal.id);
    ok++;
  }
  
  res.json({ mensaje: `✅ ${ok} animales dados de baja (${estado}). ${errores.length ? 'No encontrados: ' + errores.join(', ') : ''}`, ok, errores });
});

// ── LISTAR BAJAS ──
app.get("/api/animales/bajas", (req, res) => {
  const rows = db.prepare(`
    SELECT * FROM animales 
    WHERE estado IN ('VENDIDO', 'MUERTO', 'BAJA') 
    ORDER BY fecha_salida DESC, motivo_salida
  `).all();
  res.json(rows);
});

// ── REVERTIR BAJA ──
app.post("/api/animales/revertir-baja", (req, res) => {
  const { id } = req.body;
  if (!id) return res.status(400).json({ error: "Falta ID del animal" });
  const animal = db.prepare("SELECT * FROM animales WHERE id = ?").get(id);
  if (!animal) return res.status(404).json({ error: "Animal no encontrado" });
  if (animal.estado === 'ACTIVO') return res.json({ mensaje: `⚠️ ${animal.rp} ya está activo` });
  
  db.prepare("UPDATE animales SET estado = 'ACTIVO', fecha_salida = NULL, motivo_salida = NULL WHERE id = ?").run(id);
  res.json({ mensaje: `✅ ${animal.rp} reactivado. Estado: ACTIVO` });
});

app.get("/api/animales/:rp", (req, res) => {
  let animal = buscarAnimal(req.params.rp);
  if (!animal) animal = buscarAnimalTodos(req.params.rp);
  if (!animal) return res.status(404).json({ error: "No encontrado" });
  const pesadas = db.prepare("SELECT * FROM pesadas WHERE animal_id = ? ORDER BY fecha DESC").all(animal.id);
  const mediciones = db.prepare("SELECT * FROM mediciones WHERE animal_id = ? ORDER BY fecha DESC").all(animal.id);
  const ecografias = db.prepare("SELECT * FROM ecografias WHERE animal_id = ? ORDER BY fecha_medicion DESC").all(animal.id);
  const servicios = db.prepare("SELECT * FROM servicios WHERE animal_id = ? ORDER BY created_at DESC").all(animal.id);
  const sanidad = db.prepare("SELECT * FROM sanidad WHERE animal_id = ? ORDER BY fecha DESC").all(animal.id);
  const hijos = db.prepare("SELECT * FROM animales WHERE madre_rp = ? ORDER BY fecha_nac DESC").all(animal.rp);
  const hijos_padre = db.prepare("SELECT * FROM animales WHERE padre_rp = ? ORDER BY fecha_nac DESC").all(animal.rp);
  const lote = db.prepare("SELECT l.* FROM lotes l JOIN lote_animales la ON la.lote_id = l.id WHERE la.animal_id = ?").get(animal.id);
  let eventos = [];
  try { eventos = db.prepare("SELECT * FROM eventos WHERE animal_id = ? ORDER BY fecha DESC").all(animal.id); } catch(e) {}
  res.json({ ...animal, pesadas, mediciones, ecografias, servicios, sanidad, hijos, hijos_padre, lote, eventos });
});

// ══════════════════════════════════════════════════════════════════════════════
// ── BORRADO DE ANIMALES Y REGISTROS ──────────────────────────────────────────
// Baja  = el animal se fue del campo (vendido/muerto). Queda en el historial.
// Borrar = el animal nunca debió existir (carga errónea, duplicado). Se va todo.
// ══════════════════════════════════════════════════════════════════════════════

// Tablas que cuelgan de animal_id. Se recorren para el preview y el borrado.
const TABLAS_DEPENDIENTES = [
  ["pesadas",        "Pesadas"],
  ["mediciones",     "Mediciones"],
  ["ecografias",     "Ecografías"],
  ["servicios",      "Servicios"],
  ["sanidad",        "Sanidad"],
  ["costos",         "Costos"],
  ["eventos",        "Eventos"],
  ["lote_animales",  "Asignación a lote"],
  ["toros",          "Ficha de toro"],
  ["costeo_kgne",        "Asientos kgNE"],
  ["costeo_etapas",      "Etapas de costeo"],
  ["costeo_permanencia", "Permanencia"]
];

function contarDependencias(animalId) {
  const out = [];
  for (const [tabla, label] of TABLAS_DEPENDIENTES) {
    try {
      const n = db.prepare(`SELECT COUNT(*) n FROM ${tabla} WHERE animal_id = ?`).get(animalId).n;
      if (n > 0) out.push({ tabla, label, n });
    } catch (e) { /* la tabla puede no existir todavía */ }
  }
  return out;
}

// Preview: qué se borraría
app.get("/api/animales/:id/dependencias", (req, res) => {
  const ref = String(req.params.id);
  const porId = /^\d+$/.test(ref) ? db.prepare("SELECT * FROM animales WHERE id = ?").get(parseInt(ref, 10)) : null;
  const a = porId || buscarAnimalTodos(ref);
  if (!a) return res.status(404).json({ error: "Animal no encontrado" });
  const homonimos = animalesConMismoRP(a.rp).filter(x => x.id !== a.id);
  const deps = contarDependencias(a.id);
  const hijos = db.prepare("SELECT COUNT(*) n FROM animales WHERE madre_rp = ? OR padre_rp = ?")
    .get(a.rp, a.rp).n;
  res.json({
    animal: { id: a.id, rp: a.rp, chip: a.chip, categoria: a.categoria,
              sexo: a.sexo, estado: a.estado, fecha_nac: a.fecha_nac },
    homonimos: homonimos.map(h => ({ id: h.id, rp: h.rp, chip: h.chip,
      fecha_nac: h.fecha_nac, estado: h.estado, fecha_salida: h.fecha_salida })),
    dependencias: deps,
    total_registros: deps.reduce((t, d) => t + d.n, 0),
    hijos_referenciados: hijos
  });
});

// Borrado real con cascada
app.delete("/api/animales/:id", (req, res) => {
  const ref = String(req.params.id);
  const porId = /^\d+$/.test(ref) ? db.prepare("SELECT * FROM animales WHERE id = ?").get(parseInt(ref, 10)) : null;

  /* Si no vino el id interno y hay varios con ese RP, NO se elige por el
     sistema: se devuelven las opciones y decide el usuario. Borrar el que no
     era es irreversible. */
  if (!porId) {
    const cands = animalesConMismoRP(ref);
    if (cands.length > 1) {
      return res.status(409).json({
        error: `Hay ${cands.length} animales con el RP "${ref}". Elegí cuál borrar.`,
        requiere_id: true,
        opciones: cands.map(c => ({
          id: c.id, rp: c.rp, chip: c.chip, sexo: c.sexo, categoria: c.categoria,
          fecha_nac: c.fecha_nac, estado: c.estado, fecha_salida: c.fecha_salida,
          motivo_salida: c.motivo_salida
        }))
      });
    }
  }

  const a = porId || buscarAnimalTodos(ref);
  if (!a) return res.status(404).json({ error: "Animal no encontrado" });

  const hijos = db.prepare("SELECT rp FROM animales WHERE madre_rp = ? OR padre_rp = ?")
    .all(a.rp, a.rp).map(h => h.rp);
  if (hijos.length && req.query.forzar !== "1") {
    return res.status(409).json({
      error: `RP ${a.rp} figura como padre o madre de ${hijos.length} animal(es). ` +
             `Si lo borrás, esos animales quedan sin ese progenitor.`,
      hijos: hijos.slice(0, 20), requiere_forzar: true
    });
  }

  const deps = contarDependencias(a.id);
  const tx = db.transaction(() => {
    for (const [tabla] of TABLAS_DEPENDIENTES) {
      try { db.prepare(`DELETE FROM ${tabla} WHERE animal_id = ?`).run(a.id); } catch (e) {}
    }
    if (req.query.forzar === "1" && hijos.length) {
      db.prepare("UPDATE animales SET madre_rp = NULL WHERE madre_rp = ?").run(a.rp);
      db.prepare("UPDATE animales SET padre_rp = NULL WHERE padre_rp = ?").run(a.rp);
    }
    db.prepare("DELETE FROM animales WHERE id = ?").run(a.id);
  });
  tx();

  res.json({
    mensaje: `🗑️ RP ${a.rp} eliminado junto con ${deps.reduce((t, d) => t + d.n, 0)} registro(s)`,
    borrado: deps, hijos_desvinculados: req.query.forzar === "1" ? hijos.length : 0
  });
});

// Borrado masivo por lista de RP — para importaciones que salieron mal
app.post("/api/animales/eliminar-masivo", (req, res) => {
  const { rps, forzar } = req.body;
  if (!Array.isArray(rps) || !rps.length)
    return res.status(400).json({ error: "Mandá una lista de RP" });

  const okList = [], fallos = [];
  const tx = db.transaction(() => {
    for (const rp of rps) {
      const cands = animalesConMismoRP(String(rp).trim());
      if (cands.length > 1) {
        fallos.push({ rp, motivo: `hay ${cands.length} con ese RP — borralo desde su ficha` });
        continue;
      }
      const a = buscarAnimalTodos(String(rp).trim());
      if (!a) { fallos.push({ rp, motivo: "no encontrado" }); continue; }
      const hijos = db.prepare("SELECT COUNT(*) n FROM animales WHERE madre_rp = ? OR padre_rp = ?")
        .get(a.rp, a.rp).n;
      if (hijos && !forzar) { fallos.push({ rp: a.rp, motivo: `es progenitor de ${hijos}` }); continue; }
      for (const [tabla] of TABLAS_DEPENDIENTES) {
        try { db.prepare(`DELETE FROM ${tabla} WHERE animal_id = ?`).run(a.id); } catch (e) {}
      }
      if (forzar) {
        db.prepare("UPDATE animales SET madre_rp = NULL WHERE madre_rp = ?").run(a.rp);
        db.prepare("UPDATE animales SET padre_rp = NULL WHERE padre_rp = ?").run(a.rp);
      }
      db.prepare("DELETE FROM animales WHERE id = ?").run(a.id);
      okList.push(a.rp);
    }
  });
  tx();
  res.json({ mensaje: `🗑️ ${okList.length} animal(es) eliminado(s)`,
             eliminados: okList, fallos });
});

// ══════════════════════════════════════════════════════════════════════════════
// DIAGNÓSTICO — busca lo que rompe el costeo antes de que dé números raros
// ══════════════════════════════════════════════════════════════════════════════
app.get("/api/diagnostico", (req, res) => {
  const q = (sql, ...a) => { try { return db.prepare(sql).all(...a); } catch (e) { return []; } };
  const n = (sql, ...a) => { try { return db.prepare(sql).get(...a).n; } catch (e) { return 0; } };
  const hoy = new Date().toISOString().slice(0, 10);

  const activos = n("SELECT COUNT(*) n FROM animales WHERE estado = 'ACTIVO'");

  const chequeos = [];
  const agregar = (clave, titulo, detalle, filas, arreglo, gravedad) =>
    chequeos.push({ clave, titulo, detalle, n: filas.length, filas: filas.slice(0, 300),
                    arreglo: arreglo || null, gravedad: gravedad || "media" });

  // 1. RP con más de un ACTIVO — el sistema no puede saber cuál es
  agregar("rp_duplicados", "RP con más de un animal activo",
    "El sistema elige uno y puede equivocarse. Hay que corregir el RP de alguno.",
    q(`SELECT rp, COUNT(*) n, GROUP_CONCAT(id) ids, GROUP_CONCAT(COALESCE(chip,'sin chip')) chips
       FROM animales WHERE estado = 'ACTIVO' GROUP BY LOWER(TRIM(rp)) HAVING n > 1`),
    null, "alta");

  // 2. Sin fecha de nacimiento — no se puede saber si es cría o recría
  agregar("sin_fecha_nac", "Activos sin fecha de nacimiento",
    "Sin edad caen todos en el sector de cría y el piso queda mal repartido.",
    q(`SELECT id, rp, chip, sexo, categoria FROM animales
       WHERE estado = 'ACTIVO' AND (fecha_nac IS NULL OR fecha_nac = '')`),
    "fecha_nac", "alta");

  // 3. Terneros sin madre — la vaca nunca cobra ese destete
  agregar("sin_madre", "Jóvenes sin madre cargada",
    "Sin el RP de la madre, la vaca no cobra el destete y da negativo.",
    q(`SELECT a.id, a.rp, a.chip, a.fecha_nac, a.categoria FROM animales a
       WHERE a.estado = 'ACTIVO' AND (a.madre_rp IS NULL OR a.madre_rp = '')
         AND a.fecha_nac >= date('now','-3 years')
         AND UPPER(COALESCE(a.categoria,'')) IN ('TERNERO','TERNERA','RECRIA','VAQUILLONA')`),
    "madre_rp", "alta");

  // 4. Madre cargada que no existe
  agregar("madre_inexistente", "Madre que no está en el sistema",
    "El RP de la madre no corresponde a ningún animal: ese destete se pierde.",
    q(`SELECT a.id, a.rp, a.madre_rp FROM animales a
       WHERE a.estado = 'ACTIVO' AND a.madre_rp IS NOT NULL AND a.madre_rp != ''
         AND NOT EXISTS (SELECT 1 FROM animales m WHERE m.rp = a.madre_rp)`),
    null, "media");

  // 5. Pesadas sin contexto — no se sabe si es destete
  agregar("pesadas_sin_contexto", "Pesadas sin contexto",
    "Si es la del destete y no lo dice, ni el ternero abre ficha ni la madre cobra.",
    q(`SELECT p.id, p.fecha, p.peso, a.rp, a.categoria,
              (julianday(p.fecha) - julianday(a.fecha_nac)) / 30.44 edad_meses
       FROM pesadas p JOIN animales a ON a.id = p.animal_id
       WHERE (p.contexto IS NULL OR p.contexto = '') AND a.estado = 'ACTIVO'
       ORDER BY p.fecha DESC LIMIT 300`),
    "contexto_pesada", "media");

  // 6. Sin ficha de costeo
  agregar("sin_ficha", "Activos sin ficha de costeo",
    "No acumulan ni costo ni producción. Abrí el inventario o sincronizá.",
    q(`SELECT a.id, a.rp, a.categoria, a.fecha_nac FROM animales a
       WHERE a.estado = 'ACTIVO'
         AND NOT EXISTS (SELECT 1 FROM costeo_kgne k WHERE k.animal_id = a.id)`),
    null, activos ? "alta" : "baja");

  // 7. Sin permanencia
  agregar("sin_permanencia", "Activos sin permanencia en lote",
    "No entran en el prorrateo por días-animal.",
    q(`SELECT a.id, a.rp, a.categoria FROM animales a
       WHERE a.estado = 'ACTIVO'
         AND NOT EXISTS (SELECT 1 FROM costeo_permanencia p WHERE p.animal_id = a.id)`),
    null, "media");

  // 8. Fuera de lote
  agregar("sin_lote", "Activos que no están en ningún lote",
    "Sin lote no reciben ración ni se los puede ubicar.",
    q(`SELECT a.id, a.rp, a.categoria FROM animales a
       WHERE a.estado = 'ACTIVO'
         AND NOT EXISTS (SELECT 1 FROM lote_animales la WHERE la.animal_id = a.id)`),
    null, "baja");

  // 9. Lotes de campo sin superficie
  agregar("lotes_sin_ha", "Lotes de campo sin hectáreas",
    "Sin superficie no generan costo de tierra.",
    q(`SELECT id, nombre, sector FROM costeo_lotes_campo
       WHERE activo = 1 AND COALESCE(ha_totales,0) = 0`),
    null, "media");

  // 10. Producción sin costo y viceversa
  agregar("produccion_sin_costo", "Con producción pero sin ningún costo",
    "Falta devengar piso, ración o sanidad: el margen sale inflado.",
    q(`SELECT a.id, a.rp, a.categoria,
              ROUND(SUM(k.kgne_produccion)) kgne
       FROM costeo_kgne k JOIN animales a ON a.id = k.animal_id
       WHERE a.estado = 'ACTIVO'
         AND NOT EXISTS (SELECT 1 FROM costos c WHERE c.animal_id = a.id)
       GROUP BY a.id HAVING kgne > 0`),
    null, "media");

  /* Los sin producción no son todos iguales. Hay que separar los que están a
     mitad de camino de los que de verdad les falta algo. */
  const sinProd = q(`SELECT a.id, a.rp, a.sexo, a.categoria, a.fecha_nac,
      ROUND(SUM(c.monto),2) costo,
      (SELECT etapa FROM costeo_kgne k WHERE k.animal_id = a.id ORDER BY fecha DESC, id DESC LIMIT 1) etapa,
      (SELECT MIN(fecha) FROM costeo_kgne k WHERE k.animal_id = a.id) apertura,
      (SELECT COUNT(*) FROM pesadas p WHERE p.animal_id = a.id) pesadas,
      (SELECT MAX(p.fecha) FROM pesadas p WHERE p.animal_id = a.id) ult_pesada,
      (SELECT MAX(s.fecha_parto) FROM servicios s WHERE s.animal_id = a.id) ult_parto,
      (SELECT s.resultado FROM servicios s WHERE s.animal_id = a.id
        ORDER BY COALESCE(s.fecha_iatf, s.fecha_ingreso_toro, s.created_at) DESC LIMIT 1) ult_servicio
    FROM costos c JOIN animales a ON a.id = c.animal_id
    WHERE a.estado = 'ACTIVO'
      AND COALESCE((SELECT SUM(k.kgne_produccion) FROM costeo_kgne k WHERE k.animal_id = a.id),0) <= 0
    GROUP BY a.id`);

  const ETAPAS_PESO = new Set(["RECRIA", "ENGORDE", "TERMINACION", "PREPARACION"]);
  const gestando = [], pesoFaltante = [], sinPesar = [], resto = [];
  for (const x of sinProd) {
    const prenada = String(x.ult_servicio || "").toUpperCase().includes("PRE");
    /* Parió hace menos de 8 meses: el ternero todavía está al pie */
    const partoReciente = x.ult_parto && (Date.now() - new Date(x.ult_parto)) / 86400000 < 250;
    if (ETAPAS_PESO.has(x.etapa)) {
      if (!x.pesadas) sinPesar.push(x);
      else if (x.ult_pesada && x.apertura && x.ult_pesada <= x.apertura) pesoFaltante.push(x);
      else resto.push(x);
    } else if (prenada || partoReciente) gestando.push(x);
    else resto.push(x);
  }

  agregar("gestando", "Vientres a mitad de ciclo",
    "Preñadas o con parto reciente: todavía no destetaron. No es un error, es que su producción viene después.",
    gestando, null, "baja");

  agregar("peso_previo_apertura", "En etapa de peso, con pesadas anteriores a la apertura",
    "Su última pesada es previa al corte, así que la ganancia quedó adentro del saldo inicial. Reabrí el inventario a una fecha anterior.",
    pesoFaltante, "reabrir", "alta");

  agregar("sin_pesar", "En etapa de peso y sin ninguna pesada",
    "No hay con qué medir lo que ganaron. Falta subir el CSV de la balanza.",
    sinPesar, null, "media");

  agregar("costo_sin_produccion", "Con costo y sin producción, sin motivo claro",
    "Ni gestando ni en etapa de peso. Casi siempre es la vaca sin destete acreditado.",
    resto, null, "alta");

  const precios = (() => { try {
    return db.prepare("SELECT semana, created_at FROM costeo_precios ORDER BY id DESC LIMIT 1").get();
  } catch (e) { return null; } })();

  res.json({
    fecha: hoy, animales_activos: activos,
    precios_ultima_carga: precios || null,
    problemas: chequeos.filter(c => c.n > 0).length,
    graves: chequeos.filter(c => c.n > 0 && c.gravedad === "alta").length,
    chequeos
  });
});

// Corrección en bloque de un campo de animales
app.post("/api/animales/corregir-masivo", (req, res) => {
  const { cambios } = req.body;   // [{ id, campo, valor }]
  if (!Array.isArray(cambios) || !cambios.length)
    return res.status(400).json({ error: "Mandá una lista de cambios" });
  const permitidos = new Set(["fecha_nac", "madre_rp", "padre_rp", "categoria", "sexo", "registro", "destino", "rp"]);
  const ok = [], fallos = [];
  const tx = db.transaction(() => {
    for (const c of cambios) {
      if (!permitidos.has(c.campo)) { fallos.push({ ...c, motivo: "campo no permitido" }); continue; }
      const a = db.prepare("SELECT * FROM animales WHERE id = ?").get(c.id);
      if (!a) { fallos.push({ ...c, motivo: "no encontrado" }); continue; }
      try {
        db.prepare(`UPDATE animales SET ${c.campo} = ? WHERE id = ?`).run(c.valor || null, c.id);
        ok.push({ id: c.id, rp: a.rp, campo: c.campo, valor: c.valor });
      } catch (e) { fallos.push({ ...c, motivo: e.message }); }
    }
  });
  tx();
  res.json({ mensaje: `✅ ${ok.length} corregido(s)`, corregidos: ok.length, aplicados: ok, fallos });
});

// Separa los padres que quedaron con el nombre y el RP en un solo campo:
// "CINTHIA 9685" → familia CINTHIA + RP 9685. Solo toca los que tienen texto
// y números juntos; los que ya están limpios no se tocan.
app.post("/api/animales/separar-padres", (req, res) => {
  const dry = !!req.body.dryRun;
  const filas = db.prepare(`SELECT id, rp, madre_rp, padre_rp, madre_nombre, padre_nombre
    FROM animales WHERE madre_rp IS NOT NULL OR padre_rp IS NOT NULL`).all();

  // Texto + número al final: el número es el RP, lo de antes es el nombre.
  const partir = (v) => {
    if (!v) return null;
    const t = String(v).trim();
    const m = t.match(/^(.*?[A-Za-zÁÉÍÓÚÑáéíóúñ].*?)\s+(\d[\d\-\/]*)$/);
    if (!m) return null;
    return { nombre: m[1].trim(), rp: m[2].trim() };
  };

  const out = { revisados: filas.length, separados: 0, muestra: [], sin_cambio: 0 };
  const up = db.prepare(`UPDATE animales SET madre_rp=?, madre_nombre=?, padre_rp=?, padre_nombre=? WHERE id=?`);
  const tx = db.transaction(() => {
    for (const f of filas) {
      const m = f.madre_nombre ? null : partir(f.madre_rp);
      const p = f.padre_nombre ? null : partir(f.padre_rp);
      if (!m && !p) { out.sin_cambio++; continue; }
      if (!dry) up.run(m ? m.rp : f.madre_rp, m ? m.nombre : f.madre_nombre,
                       p ? p.rp : f.padre_rp, p ? p.nombre : f.padre_nombre, f.id);
      out.separados++;
      if (out.muestra.length < 15) out.muestra.push({ rp: f.rp,
        madre: m ? `${f.madre_rp} → ${m.nombre} · RP ${m.rp}` : null,
        padre: p ? `${f.padre_rp} → ${p.nombre} · RP ${p.rp}` : null });
    }
  });
  tx();
  out.dry = dry;
  res.json(out);
});

// Deduce el contexto de todas las pesadas que no lo tengan, por la edad del
// animal en cada pesaje. Es la misma regla que se aplica al cargar de a una.
app.post("/api/pesadas/deducir-contexto", (req, res) => {
  const soloVacias = req.body.solo_vacias !== false;
  const filas = db.prepare(`SELECT p.id, p.fecha, p.contexto, a.fecha_nac, a.categoria, a.rp
    FROM pesadas p JOIN animales a ON a.id = p.animal_id
    ${soloVacias ? "WHERE p.contexto IS NULL OR p.contexto = ''" : ""}`).all();

  const res2 = { revisadas: filas.length, actualizadas: 0, sin_fecha_nac: 0, por_contexto: {}, muestra: [] };
  const up = db.prepare("UPDATE pesadas SET contexto = ? WHERE id = ?");
  const tx = db.transaction(() => {
    for (const f of filas) {
      if (!f.fecha_nac) { res2.sin_fecha_nac++; continue; }
      const ctx = determinarContextoPesada(f.fecha_nac, f.fecha, f.categoria);
      if (!ctx || ctx === f.contexto) continue;
      if (!req.body.dryRun) up.run(ctx, f.id);
      res2.actualizadas++;
      res2.por_contexto[ctx] = (res2.por_contexto[ctx] || 0) + 1;
      if (res2.muestra.length < 20) res2.muestra.push({ rp: f.rp, fecha: f.fecha, contexto: ctx,
        dias: Math.floor((new Date(f.fecha) - new Date(f.fecha_nac)) / 86400000) });
    }
  });
  tx();
  res2.dry = !!req.body.dryRun;
  res.json(res2);
});

// Corrección en bloque del contexto de pesadas
app.post("/api/pesadas/corregir-masivo", (req, res) => {
  const { cambios } = req.body;   // [{ id, contexto }]
  if (!Array.isArray(cambios) || !cambios.length)
    return res.status(400).json({ error: "Mandá una lista de cambios" });
  let n = 0;
  const tx = db.transaction(() => {
    for (const c of cambios) {
      const r = db.prepare("UPDATE pesadas SET contexto = ? WHERE id = ?")
        .run(String(c.contexto || "").toUpperCase() || null, c.id);
      n += r.changes;
    }
  });
  tx();
  res.json({ mensaje: `✅ ${n} pesada(s) actualizadas`, corregidos: n });
});

// Auditoría: qué RP están repetidos y en qué estado está cada uno.
// Sirve para revisar antes de que un RP repetido cause un error.
app.get("/api/animales/rp-duplicados", (req, res) => {
  const grupos = db.prepare(`
    SELECT rp, COUNT(*) n,
           SUM(CASE WHEN estado = 'ACTIVO' THEN 1 ELSE 0 END) activos
    FROM animales GROUP BY LOWER(TRIM(rp)) HAVING n > 1 ORDER BY activos DESC, n DESC
  `).all();
  const detalle = grupos.map(g => ({
    ...g,
    animales: db.prepare(`SELECT id, rp, chip, sexo, categoria, fecha_nac, estado,
      fecha_salida, motivo_salida FROM animales
      WHERE LOWER(TRIM(rp)) = LOWER(TRIM(?))
      ORDER BY (estado = 'ACTIVO') DESC, id DESC`).all(g.rp)
  }));
  res.json({
    grupos: detalle.length,
    conflictivos: detalle.filter(g => g.activos > 1).length,
    detalle
  });
});

// Faltaba el borrado de ecografías
app.delete("/api/ecografias/:id", (req, res) => {
  db.prepare("DELETE FROM ecografias WHERE id = ?").run(req.params.id);
  res.json({ mensaje: "✅ Ecografía eliminada" });
});

// Borrado masivo de registros de un tipo para un animal
app.delete("/api/animales/:id/registros/:tipo", (req, res) => {
  const permitidas = ["pesadas","mediciones","ecografias","servicios","sanidad","costos","eventos",
                      "costeo_kgne","costeo_etapas","costeo_permanencia"];
  const t = req.params.tipo;
  if (!permitidas.includes(t)) return res.status(400).json({ error: "Tipo no permitido" });
  const a = db.prepare("SELECT * FROM animales WHERE id = ?").get(req.params.id)
        || buscarAnimalTodos(req.params.id);
  if (!a) return res.status(404).json({ error: "Animal no encontrado" });
  const r = db.prepare(`DELETE FROM ${t} WHERE animal_id = ?`).run(a.id);
  res.json({ mensaje: `✅ ${r.changes} registro(s) de ${t} eliminados de RP ${a.rp}` });
});

app.put("/api/animales/:id", (req, res) => {
  const id = parseInt(req.params.id);
  const animal = db.prepare("SELECT * FROM animales WHERE id = ?").get(id);
  if (!animal) return res.status(404).json({ error: "No encontrado" });

  const { nombre, rp, chip, fecha_nac, sexo, pelo, registro, categoria, destino, madre_rp, padre_rp } = req.body;
  const b = req.body;
  const opt = (k) => b[k] !== undefined ? (b[k] || null) : animal[k];
  try {
    db.prepare(`
      UPDATE animales SET nombre=?, rp=?, chip=?, fecha_nac=?, sexo=?, pelo=?, registro=?,
             categoria=?, destino=?, madre_rp=?, padre_rp=?, hbu=?,
             madre_nombre=?, padre_nombre=?, madre_hbu=?, padre_hbu=?,
             madre_hba=?, padre_hba=? WHERE id=?
    `).run(nombre !== undefined ? (nombre || null) : animal.nombre,
      rp || animal.rp, chip || animal.chip, fecha_nac || animal.fecha_nac,
      sexo || animal.sexo, pelo || animal.pelo, registro || animal.registro,
      categoria || animal.categoria, destino || animal.destino,
      madre_rp !== undefined ? madre_rp : animal.madre_rp,
      padre_rp !== undefined ? padre_rp : animal.padre_rp,
      req.body.hbu !== undefined ? (req.body.hbu || null) : animal.hbu,
      opt('madre_nombre'), opt('padre_nombre'),
      opt('madre_hbu'), opt('padre_hbu'),
      opt('madre_hba'), opt('padre_hba'),
      id
    );
    res.json({ mensaje: `✅ Animal ${rp || animal.rp} actualizado.` });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/api/pesadas", (req, res) => {
  const rows = db.prepare(`
    SELECT p.*, a.rp, a.categoria, a.sexo FROM pesadas p
    JOIN animales a ON a.id = p.animal_id ORDER BY p.fecha DESC LIMIT ?
  `).all(parseInt(req.query.limite) || 200);
  res.json(rows);
});

// ── DELETE/PUT para registros individuales ──
app.delete("/api/pesadas/:id", (req, res) => {
  const r = db.prepare("SELECT * FROM pesadas WHERE id = ?").get(req.params.id);
  if (!r) return res.status(404).json({ error: "No encontrada" });
  db.prepare("DELETE FROM pesadas WHERE id = ?").run(req.params.id);
  res.json({ mensaje: `✅ Pesada #${req.params.id} eliminada (${r.peso}kg ${r.fecha})` });
});

app.put("/api/pesadas/:id", (req, res) => {
  const { peso, fecha, contexto } = req.body;
  db.prepare("UPDATE pesadas SET peso=COALESCE(?,peso), fecha=COALESCE(?,fecha), contexto=COALESCE(?,contexto) WHERE id=?")
    .run(peso||null, fecha||null, contexto||null, req.params.id);
  res.json({ mensaje: `✅ Pesada #${req.params.id} actualizada` });
});

app.delete("/api/mediciones/:id", (req, res) => {
  db.prepare("DELETE FROM mediciones WHERE id = ?").run(req.params.id);
  res.json({ mensaje: `✅ Medición eliminada` });
});

app.post("/api/mediciones", (req, res) => {
  const { rp, animal_id, fecha, tipo, valor } = req.body;
  let aid = animal_id;
  if (!aid && rp) {
    const a = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?) AND estado = 'ACTIVO'").get(rp);
    if (a) aid = a.id;
  }
  if (!aid) return res.status(400).json({ error: "Animal no encontrado" });
  db.prepare("INSERT INTO mediciones (animal_id, fecha, tipo, valor) VALUES (?, ?, ?, ?)")
    .run(aid, fecha || new Date().toISOString().slice(0,10), tipo || 'CE', parseFloat(valor) || 0);
  res.json({ ok: true, mensaje: `✅ ${tipo} = ${valor} registrado` });
});

app.delete("/api/sanidad/:id", (req, res) => {
  db.prepare("DELETE FROM sanidad WHERE id = ?").run(req.params.id);
  res.json({ mensaje: `✅ Registro sanitario eliminado` });
});

app.delete("/api/servicios/:id", (req, res) => {
  db.prepare("DELETE FROM servicios WHERE id = ?").run(req.params.id);
  res.json({ mensaje: `✅ Servicio eliminado` });
});

app.get("/api/ecografias", (req, res) => {
  const rows = db.prepare(`
    SELECT e.*, a.rp, a.sexo, a.categoria FROM ecografias e
    JOIN animales a ON a.id = e.animal_id ORDER BY e.fecha_medicion DESC LIMIT ?
  `).all(parseInt(req.query.limite) || 200);
  res.json(rows);
});

app.get("/api/servicios", (req, res) => {
  const rows = db.prepare(`
    SELECT s.*, a.rp, a.categoria FROM servicios s
    JOIN animales a ON a.id = s.animal_id ORDER BY s.created_at DESC LIMIT ?
  `).all(parseInt(req.query.limite) || 200);
  res.json(rows);
});

app.get("/api/sanidad", (req, res) => {
  const rows = db.prepare(`
    SELECT s.*, a.rp FROM sanidad s
    JOIN animales a ON a.id = s.animal_id ORDER BY s.fecha DESC LIMIT ?
  `).all(parseInt(req.query.limite) || 200);
  res.json(rows);
});

app.get("/api/resumen", (req, res) => {
  const resumen = getResumenRodeo();
  const ultPesada = db.prepare("SELECT MAX(fecha) as f FROM pesadas").get();
  const totalEco = db.prepare("SELECT COUNT(*) as n FROM ecografias").get();
  const totalServ = db.prepare("SELECT COUNT(*) as n FROM servicios").get();
  const totalSanidad = db.prepare("SELECT COUNT(*) as n FROM sanidad").get();
  res.json({
    ...resumen,
    ultima_pesada: ultPesada?.f,
    total_ecografias: totalEco?.n || 0,
    total_servicios: totalServ?.n || 0,
    total_sanidad: totalSanidad?.n || 0,
  });
});

// Importación masiva REST
app.post("/api/importar/animales", (req, res) => {
  const { animales } = req.body;
  if (!Array.isArray(animales)) return res.status(400).json({ error: "Formato inválido" });
  const result = ejecutarAccion({ accion: "importar_animales", animales });
  res.json({ mensaje: result });
});

app.post("/api/importar/ecografias", (req, res) => {
  const { ecografias } = req.body;
  if (!Array.isArray(ecografias)) return res.status(400).json({ error: "Formato inválido" });
  const result = ejecutarAccion({ accion: "importar_ecografias", ecografias });
  res.json({ mensaje: result });
});

app.post("/api/importar/pesadas", (req, res) => {
  const { pesadas } = req.body;
  if (!Array.isArray(pesadas)) return res.status(400).json({ error: "Formato inválido" });
  const result = ejecutarAccion({ accion: "importar_pesadas", pesadas });
  res.json({ mensaje: result });
});

app.post("/api/importar/servicios", (req, res) => {
  // Usa buscarAnimalTodos para incluir vendidos
  const { servicios } = req.body;
  if (!Array.isArray(servicios)) return res.status(400).json({ error: "Formato inválido" });
  let ok = 0, errores = 0;
  for (const s of servicios) {
    const animal = buscarAnimalTodos(s.rp);
    if (!animal) { errores++; continue; }
    try {
      db.prepare(`
        INSERT INTO servicios (animal_id, temporada, tacto_pre, cc_pre, tipo_servicio, semen_iatf, fecha_iatf, toro_natural, fecha_ingreso_toro, tacto_servicio, cc_post, resultado, fecha_parto, ternero_rp, peso_nacimiento, peso_destete, sexo_cria, pelo_cria, ternero2_rp, peso_nacimiento2, sexo_cria2, pelo_cria2, notas)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(animal.id, s.temporada||null, s.tacto_pre||null, s.cc_pre||null, s.tipo_servicio||null, s.semen_iatf||null, s.fecha_iatf||null, s.toro_natural||null, s.fecha_ingreso_toro||null, s.tacto_servicio||null, s.cc_post||null, s.resultado||null, s.fecha_parto||null, s.ternero_rp||null, s.peso_nacimiento||null, s.peso_destete||null, s.sexo_cria||null, s.pelo_cria||null, s.ternero2_rp||null, s.peso_nacimiento2||null, s.sexo_cria2||null, s.pelo_cria2||null, s.notas||null);
      // Si hay mellizo, marcar ambos terneros como mellizos entre sí
      if (s.ternero_rp && s.ternero2_rp) {
        const t1 = buscarAnimalTodos(s.ternero_rp);
        const t2 = buscarAnimalTodos(s.ternero2_rp);
        if (t1) db.prepare("UPDATE animales SET mellizo_de = ? WHERE id = ?").run(s.ternero2_rp, t1.id);
        if (t2) db.prepare("UPDATE animales SET mellizo_de = ? WHERE id = ?").run(s.ternero_rp, t2.id);
      }
      ok++;
    } catch(e) { errores++; }
  }
  res.json({ mensaje: `✅ Servicios importados: ${ok} cargados, ${errores} errores.` });
});

app.post("/api/importar/sanidad", (req, res) => {
  const { sanidad } = req.body;
  if (!Array.isArray(sanidad)) return res.status(400).json({ error: "Formato inválido" });
  let ok = 0, errores = 0;
  for (const s of sanidad) {
    const animal = buscarAnimalTodos(s.rp);
    if (!animal) { errores++; continue; }
    try {
      db.prepare("INSERT INTO sanidad (animal_id, fecha, tipo, producto, dosis, notas) VALUES (?, ?, ?, ?, ?, ?)")
        .run(animal.id, s.fecha||new Date().toISOString().slice(0,10), s.tipo||'TRATAMIENTO', s.producto||null, s.dosis||null, s.notas||null);
      ok++;
    } catch(e) { errores++; }
  }
  res.json({ mensaje: `✅ Sanidad importada: ${ok} cargados, ${errores} errores.` });
});

app.post("/api/importar/mediciones", (req, res) => {
  const { mediciones } = req.body;
  if (!Array.isArray(mediciones)) return res.status(400).json({ error: "Formato inválido" });
  let ok = 0, errores = 0;
  for (const m of mediciones) {
    const animal = buscarAnimalTodos(m.rp);
    if (!animal) { errores++; continue; }
    try {
      db.prepare("INSERT INTO mediciones (animal_id, fecha, tipo, valor, notas) VALUES (?, ?, ?, ?, ?)")
        .run(animal.id, m.fecha||new Date().toISOString().slice(0,10), m.tipo||'CE', m.valor||null, m.notas||null);
      ok++;
    } catch(e) { errores++; }
  }
  res.json({ mensaje: `✅ Mediciones importadas: ${ok} cargados, ${errores} errores.` });
});

// ── IMPORTAR GALLAGHER CSV ────────────────────────────────────────────────────
app.post("/api/importar/gallagher", (req, res) => {
  const { registros, nombre_sesion } = req.body;
  if (!Array.isArray(registros)) return res.status(400).json({ error: "Formato inválido" });

  const resumen = { pesadas:0, animales_nuevos:0, mediciones:0, servicios:0, sanidad:0, updates:0, errores:0, no_encontrados:[], _leidos:[] };
  const hoy = new Date().toISOString().split("T")[0];

  for (const r of registros) {
    try {
      const rp = r.rp ? String(r.rp).trim() : null;
      const chipRaw = r.chip ? String(r.chip).replace(/\s/g, '') : null;
      // Normalizar chip: quitar prefijo 858000 si existe
      const chip = chipRaw ? chipRaw.replace(/^858000/, '') : null;
      const fecha = r.fecha || hoy;

      // ── BUSCAR ANIMAL (CHIP prioritario, luego RP) ──
      let animal = null;
      let esAnimalNuevoPorChip = false;
      
      // 1. Siempre buscar primero por CHIP (es único e inequívoco)
      if (chip) {
        animal = buscarAnimalPorChip(chip, false);
        if (!animal && chipRaw) animal = buscarAnimalPorChip(chipRaw, false);
        // Si tiene chip pero NO se encontró → es animal NUEVO seguro
        if (!animal) esAnimalNuevoPorChip = true;
      }
      
      // 2. Si no encontró por chip, buscar por RP
      if (!animal && rp) {
        const matches = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?)").all(rp);
        if (matches.length === 1) {
          animal = matches[0];
          // Si tenía chip y el animal no tiene → actualizar chip
          if (chip && !animal.chip) {
            try { db.prepare("UPDATE animales SET chip = ? WHERE id = ?").run(chip, animal.id); resumen.updates++; } catch(e) {}
          }
        } else if (matches.length > 1) {
          resumen.no_encontrados.push(`${rp} (RP duplicado, usar chip)`);
          continue;
        } else {
          // Buscar con lógica ADE
          animal = buscarAnimalTodos(rp);
          if (animal && chip && !animal.chip) {
            try { db.prepare("UPDATE animales SET chip = ? WHERE id = ?").run(chip, animal.id); resumen.updates++; } catch(e) {}
          }
        }
      }

      // Si no existe → según modo: crear o solo reportar
      if (!animal && (rp || chip)) {
        if (req.body.auto_crear) {
          const nuevoRp = rp || `G${(chip || Date.now().toString()).slice(-4)}`;
          const pelo = r.color ? (r.color.toLowerCase()==='black'?'NEGRO':r.color.toLowerCase()==='red'?'COLORADO':null) : (r.pelo || null);
          // El sexo puede venir M/H, MACHO/HEMBRA o MALE/FEMALE.
          const sx = String(r.sexo || '').trim().toUpperCase();
          const sexo = sx ? (/^(M|MACHO|MALE|TORO)/.test(sx) ? 'MACHO' : 'HEMBRA') : 'HEMBRA';
          // Alias de columnas: distintas planillas las nombran distinto.
          const fechaNac = r.fecha_nac || r.fecha_nacimiento || r.nacimiento || r.fnac || null;
          const madreRp = r.madre || r.madre_rp || r.rp_madre || r.dam || null;
          const padreRp = r.padre || r.padre_rp || r.rp_padre || r.sire || null;
          const registro = r.registro || r.reg || null;
          const raza = r.raza || null;
          
          // Auto-detectar categoría por edad
          let cat = (r.categoria || '').toUpperCase() || 'RECRIA';
          if (!r.categoria && fechaNac) {
            const edadMeses = Math.floor((new Date() - new Date(fechaNac)) / (1000*60*60*24*30.44));
            if (edadMeses < 7) cat = 'TERNERO';
            else if (edadMeses < 15) cat = 'RECRIA';
            else if (edadMeses < 24 && sexo === 'HEMBRA') cat = 'VAQUILLONA';
          }
          
          // El RP no puede repetirse en otro campo de la misma empresa.
          const enOtro = rpEnOtroCampo(req.campoKey || CAMPO_DEFAULT, nuevoRp);
          if (enOtro) {
            resumen.no_encontrados.push(`${nuevoRp} (ya existe en ${enOtro.nombre}, no se creó)`);
            continue;
          }
          // Verificar si ya existe ese RP (puede ser embrión/mellizo de misma madre)
          const rpExiste = db.prepare("SELECT id FROM animales WHERE LOWER(rp) = LOWER(?)").get(nuevoRp);
          let rpFinal = nuevoRp;
          if (rpExiste) {
            // RP duplicado: agregar sufijo con chip
            rpFinal = chip ? `${nuevoRp}-${chip.slice(-3)}` : `${nuevoRp}-${Date.now().toString().slice(-3)}`;
            resumen.no_encontrados.push(`${nuevoRp} (RP duplicado, creado como ${rpFinal})`);
          }
          
          try {
            db.prepare(`
              INSERT INTO animales (chip, rp, sexo, pelo, categoria, destino, estado, fecha_nac, fecha_ingreso, madre_rp, padre_rp, raza, registro, notas)
              VALUES (?, ?, ?, ?, ?, 'PLANTEL', 'ACTIVO', ?, ?, ?, ?, ?, ?, ?)
            `).run(chip||null, rpFinal, sexo, pelo, cat, fechaNac, fecha, madreRp, padreRp,
                   raza, registro, `Importado: ${nombre_sesion||''}`);
            animal = buscarAnimalTodos(rpFinal);
            resumen.animales_nuevos++;
          } catch(e) {
            if (chip) animal = buscarAnimalPorChip(chip, false);
          }
        }
      }

      if (!animal) {
        resumen.errores++;
        const info = rp || `chip:${chip||'?'}`;
        resumen.no_encontrados.push(info);
        continue;
      }

      // ── PESO VIVO → pesada ──
      const peso = r.peso != null && r.peso !== '' ? parseFloat(r.peso) : null;
      const gdpProm = r.gdp_promedio != null && r.gdp_promedio !== '' ? parseFloat(r.gdp_promedio) : null;
      const gdpGral = r.gdp_general != null && r.gdp_general !== '' ? parseFloat(r.gdp_general) : null;

      if (peso && peso > 0) {
        // Anti-duplicado: no insertar si ya existe pesada del mismo animal, misma fecha, mismo peso
        const existe = db.prepare("SELECT id FROM pesadas WHERE animal_id = ? AND fecha = ? AND peso = ?").get(animal.id, fecha, peso);
        if (!existe) {
          const contexto = determinarContextoPesada(animal.fecha_nac, fecha, animal.categoria);
          db.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto, gdp, notas) VALUES (?,?,?,?,?,?)")
            .run(animal.id, fecha, peso, contexto, gdpGral||gdpProm||null, `${nombre_sesion||'Gallagher'}`);
          // Recalcular GDP después de insertar
          const gdpCalc = calcularGDP(animal.id);
          if (gdpCalc !== null) {
            const lastP = db.prepare("SELECT id FROM pesadas WHERE animal_id = ? ORDER BY created_at DESC LIMIT 1").get(animal.id);
            if (lastP) db.prepare("UPDATE pesadas SET gdp = ? WHERE id = ?").run(gdpCalc, lastP.id);
          }
          resumen.pesadas++;
        }
      } else if (gdpProm || gdpGral) {
        db.prepare("INSERT INTO mediciones (animal_id, fecha, tipo, valor, notas) VALUES (?,?,'GDP',?,?)")
          .run(animal.id, fecha, gdpGral||gdpProm, `GDP_P:${gdpProm||''} GDP_G:${gdpGral||''} | ${nombre_sesion||''}`);
        resumen.mediciones++;
      }

      // ── C.E. (circunferencia escrotal) ──
      if (r.ce != null && r.ce !== '') {
        const ex = db.prepare("SELECT id FROM mediciones WHERE animal_id=? AND fecha=? AND tipo='CE'").get(animal.id, fecha);
        if (!ex) { db.prepare("INSERT INTO mediciones (animal_id, fecha, tipo, valor, notas) VALUES (?,?,'CE',?,?)").run(animal.id, fecha, parseFloat(r.ce), nombre_sesion||'Gallagher'); resumen.mediciones++; }
      }

      // ── ALTURA ──
      if (r.altura != null && r.altura !== '') {
        const ex = db.prepare("SELECT id FROM mediciones WHERE animal_id=? AND fecha=? AND tipo='ALTURA'").get(animal.id, fecha);
        if (!ex) { db.prepare("INSERT INTO mediciones (animal_id, fecha, tipo, valor, notas) VALUES (?,?,'ALTURA',?,?)").run(animal.id, fecha, parseFloat(r.altura), nombre_sesion||'Gallagher'); resumen.mediciones++; }
      }

      // ── CARTEL (frame score) ──
      if (r.cartel != null && r.cartel !== '') {
        const ex = db.prepare("SELECT id FROM mediciones WHERE animal_id=? AND fecha=? AND tipo='FRAME'").get(animal.id, fecha);
        if (!ex) { db.prepare("INSERT INTO mediciones (animal_id, fecha, tipo, valor, notas) VALUES (?,?,'FRAME',?,?)").run(animal.id, fecha, parseFloat(r.cartel), nombre_sesion||'Gallagher'); resumen.mediciones++; }
      }

      // ── CONDICIÓN CORPORAL ──
      if (r.condicion != null && r.condicion !== '') {
        const ex = db.prepare("SELECT id FROM mediciones WHERE animal_id=? AND fecha=? AND tipo='CC'").get(animal.id, fecha);
        if (!ex) { db.prepare("INSERT INTO mediciones (animal_id, fecha, tipo, valor, notas) VALUES (?,?,'CC',?,?)").run(animal.id, fecha, parseFloat(r.condicion), nombre_sesion||'Gallagher'); resumen.mediciones++; }
      }

      // ── COLOR / PELO → actualizar animal ──
      const colorPelo = r.color ? (r.color.toLowerCase()==='black'?'NEGRO':r.color.toLowerCase()==='red'?'COLORADO':null) : (r.pelo||null);
      if (colorPelo && !animal.pelo) {
        db.prepare("UPDATE animales SET pelo = ? WHERE id = ? AND (pelo IS NULL OR pelo = '')").run(colorPelo, animal.id);
        resumen.updates++;
      }

      // ── SEXO → actualizar si no tiene ──
      if (r.sexo && !animal.sexo) {
        db.prepare("UPDATE animales SET sexo = ? WHERE id = ?").run(r.sexo.toUpperCase(), animal.id);
        resumen.updates++;
      }

      // ── FECHA NACIMIENTO → actualizar si no tiene ──
      if (r.fecha_nac && !animal.fecha_nac) {
        db.prepare("UPDATE animales SET fecha_nac = ? WHERE id = ? AND fecha_nac IS NULL").run(r.fecha_nac, animal.id);
        resumen.updates++;
      }

      // ── RAZA → actualizar si viene ──
      if (r.raza && (!animal.raza || animal.raza === 'A. ANGUS')) {
        db.prepare("UPDATE animales SET raza = ? WHERE id = ?").run(r.raza, animal.id);
      }

      // ── MADRE → actualizar si no tiene ──
      if (r.madre && !animal.madre_rp) {
        db.prepare("UPDATE animales SET madre_rp = ? WHERE id = ? AND madre_rp IS NULL").run(r.madre, animal.id);
        resumen.updates++;
      }

      // ── PADRE / PROGENITOR MACHO → toro de servicio o padre genético ──
      if (r.padre) {
        if (!animal.padre_rp) {
          db.prepare("UPDATE animales SET padre_rp = ? WHERE id = ? AND padre_rp IS NULL").run(r.padre, animal.id);
        }
        // También registrar como servicio si es hembra
        if (animal.sexo === 'HEMBRA') {
          const temporada = fecha.slice(0,4);
          const existeServ = db.prepare("SELECT id FROM servicios WHERE animal_id = ? AND temporada = ?").get(animal.id, temporada);
          if (!existeServ) {
            db.prepare("INSERT INTO servicios (animal_id, temporada, tipo_servicio, semen_iatf, notas) VALUES (?,?,'IATF',?,?)")
              .run(animal.id, temporada, r.padre, `Gallagher ${nombre_sesion||''}`);
            resumen.servicios++;
          }
        }
      }

      // ── PREÑEZ → resultado tacto ──
      if (r.prenez) {
        const temporada = fecha.slice(0,4);
        let resultado = null, tipo = null;
        const pren = r.prenez.toUpperCase();
        if (pren === 'IATF') { resultado = 'PREÑADA'; tipo = 'IATF'; }
        else if (pren === 'CABEZA' || pren === 'NATURAL') { resultado = 'PREÑADA'; tipo = 'NATURAL'; }
        else if (pren === 'VACIA') { resultado = 'VACIA'; }

        if (resultado) {
          const existeServ = db.prepare("SELECT id FROM servicios WHERE animal_id = ? AND temporada = ?").get(animal.id, temporada);
          if (existeServ) {
            db.prepare("UPDATE servicios SET resultado = ?, tipo_servicio = COALESCE(?, tipo_servicio) WHERE id = ?")
              .run(resultado, tipo, existeServ.id);
          } else {
            db.prepare("INSERT INTO servicios (animal_id, temporada, tipo_servicio, resultado, notas) VALUES (?,?,?,?,?)")
              .run(animal.id, temporada, tipo, resultado, `Tacto Gallagher ${nombre_sesion||''}`);
          }
          resumen.servicios++;
          // Preñada → VAQUILLONA/RECRIA pasa a VACA
          if (resultado === 'PREÑADA' && (animal.categoria === 'VAQUILLONA' || (animal.categoria === 'RECRIA' && animal.sexo === 'HEMBRA'))) {
            db.prepare("UPDATE animales SET categoria = 'VACA' WHERE id = ?").run(animal.id);
          }
        }
      }

      // ── TORO PREÑEZ → registrar qué toro preñó ──
      if (r.toro_prenez) {
        const temporada = fecha.slice(0,4);
        const existeServ = db.prepare("SELECT id FROM servicios WHERE animal_id = ? AND temporada = ?").get(animal.id, temporada);
        if (existeServ) {
          db.prepare("UPDATE servicios SET toro_natural = COALESCE(toro_natural, ?), semen_iatf = COALESCE(semen_iatf, ?) WHERE id = ?")
            .run(r.toro_prenez, r.toro_prenez, existeServ.id);
        }
      }

      // ── TACTO PRE → registrar ──
      if (r.tacto_pre) {
        const temporada = fecha.slice(0,4);
        const existeServ = db.prepare("SELECT id FROM servicios WHERE animal_id = ? AND temporada = ?").get(animal.id, temporada);
        if (existeServ) {
          db.prepare("UPDATE servicios SET tacto_pre = ? WHERE id = ?").run(r.tacto_pre, existeServ.id);
        } else {
          db.prepare("INSERT INTO servicios (animal_id, temporada, tacto_pre, notas) VALUES (?,?,?,?)")
            .run(animal.id, temporada, r.tacto_pre, `Gallagher ${nombre_sesion||''}`);
          resumen.servicios++;
        }
      }

      // ── LACTANCIA → medición ──
      if (r.lactancia != null && r.lactancia !== '') {
        db.prepare("INSERT INTO mediciones (animal_id, fecha, tipo, valor_texto, notas) VALUES (?,?,'LACTANCIA',?,?)")
          .run(animal.id, fecha, r.lactancia, nombre_sesion||'Gallagher');
        resumen.mediciones++;
      }

      // ── VACUNA → sanidad ──
      if (r.vacuna) {
        db.prepare("INSERT INTO sanidad (animal_id, fecha, tipo, producto, notas) VALUES (?,?,'VACUNA',?,?)")
          .run(animal.id, fecha, r.vacuna, `Gallagher ${nombre_sesion||''}`);
        resumen.sanidad++;
      }

      // ── GRUPO → actualizar notas ──
      if (r.grupo) {
        db.prepare("UPDATE animales SET notas = CASE WHEN notas IS NULL THEN ? ELSE notas || ' | ' || ? END WHERE id = ?")
          .run(`GRUPO:${r.grupo}`, `GRUPO:${r.grupo}`, animal.id);
      }

      // ── ESTADO → actualizar ──
      if (r.estado) {
        db.prepare("UPDATE animales SET estado = ? WHERE id = ?").run(r.estado.toUpperCase(), animal.id);
      }

      // ── NOTAS: Nuevo Arete → actualizar RP ──
      if (r.notas && r.notas.toLowerCase().includes('nuevo arete')) {
        const nuevoRp = r.notas.replace(/Nuevo Arete:/i,'').trim();
        if (nuevoRp && nuevoRp !== animal.rp) {
          try { db.prepare("UPDATE animales SET rp = ? WHERE id = ?").run(nuevoRp, animal.id); resumen.updates++; } catch(e) {}
        }
      }

      // ── CHIP → actualizar si no tiene ──
      if (chip && !animal.chip) {
        db.prepare("UPDATE animales SET chip = ? WHERE id = ? AND (chip IS NULL OR chip = '')").run(chip, animal.id);
      }

      // Track para control de lote
      if (animal) resumen._leidos.push(animal.id);

    } catch(e) {
      resumen.errores++;
    }
  }

  // ── CONTROL DE LOTE AUTOMÁTICO ──
  let controlMsg = '';
  try {
    if (resumen._leidos.length >= 3) {
      // Detectar lote dominante entre los leídos
      const placeholders = resumen._leidos.map(()=>'?').join(',');
      const lotesLeidos = db.prepare(`
        SELECT la.lote_id, l.nombre, COUNT(*) as n FROM lote_animales la
        JOIN lotes l ON l.id = la.lote_id
        WHERE la.animal_id IN (${placeholders})
        GROUP BY la.lote_id ORDER BY n DESC
      `).all(...resumen._leidos);
      
      if (lotesLeidos.length) {
        const dominante = lotesLeidos[0];
        const totalLote = db.prepare(`
          SELECT COUNT(*) as n FROM lote_animales la JOIN animales a ON a.id = la.animal_id
          WHERE la.lote_id = ? AND a.estado = 'ACTIVO'
        `).get(dominante.lote_id);
        
        // Solo registrar control si leyó al menos 30% del lote
        if (dominante.n >= totalLote.n * 0.3) {
          // Faltantes: del lote dominante que NO están en la lectura
          const faltantes = db.prepare(`
            SELECT a.rp FROM lote_animales la JOIN animales a ON a.id = la.animal_id
            WHERE la.lote_id = ? AND a.estado = 'ACTIVO' AND a.id NOT IN (${placeholders})
            ORDER BY a.rp
          `).all(dominante.lote_id, ...resumen._leidos).map(x=>x.rp);
          
          // Novedades: leídos que NO son del lote dominante
          const novedades = db.prepare(`
            SELECT a.rp, COALESCE(l.nombre,'SIN LOTE') as lote FROM animales a
            LEFT JOIN lote_animales la ON la.animal_id = a.id
            LEFT JOIN lotes l ON l.id = la.lote_id
            WHERE a.id IN (${placeholders}) AND (la.lote_id IS NULL OR la.lote_id != ?)
            ORDER BY a.rp
          `).all(...resumen._leidos, dominante.lote_id).map(x=>`${x.rp} (${x.lote})`);
          
          const fechaLectura = registros[0]?.fecha || hoy;
          db.prepare(`INSERT INTO lecturas_lote (lote_id, fecha, total_lote, leidos, faltantes, novedades)
            VALUES (?,?,?,?,?,?)`)
            .run(dominante.lote_id, fechaLectura, totalLote.n, dominante.n,
                 JSON.stringify(faltantes), JSON.stringify(novedades));
          
          controlMsg = `\n📋 *Control lote ${dominante.nombre}*: ${dominante.n}/${totalLote.n} leídos`;
          if (faltantes.length) controlMsg += `\n⚠️ Faltaron: ${faltantes.join(', ')}`;
          else controlMsg += `\n✅ Pasaron todos`;
          if (novedades.length) controlMsg += `\n🆕 Novedades (de otro lote): ${novedades.join(', ')}`;
        }
      }
    }
  } catch(e) { console.error('Error control lote:', e.message); }

  let msg = `✅ Sesión Gallagher: ${nombre_sesion || ''}\n📊 ${registros.length} lecturas\n`;
  if (resumen.pesadas) msg += `⚖️ ${resumen.pesadas} pesadas\n`;
  if (resumen.mediciones) msg += `📐 ${resumen.mediciones} mediciones (CE/Altura/CC/GDP)\n`;
  if (resumen.animales_nuevos) msg += `🐄 ${resumen.animales_nuevos} animales nuevos\n`;
  if (resumen.servicios) msg += `🔄 ${resumen.servicios} servicios/tactos\n`;
  if (resumen.sanidad) msg += `💉 ${resumen.sanidad} vacunas/sanidad\n`;
  if (resumen.updates) msg += `🔄 ${resumen.updates} datos actualizados\n`;
  if (resumen.errores) msg += `⚠️ ${resumen.errores} errores\n`;
  if (resumen.no_encontrados.length) msg += `❓ No encontrados: ${resumen.no_encontrados.slice(0,10).join(', ')}`;
  msg += controlMsg;

  delete resumen._leidos;
  res.json({ mensaje: msg, resumen });
});

// ── EDITAR SERVICIO ──
app.put("/api/servicios/:id", (req, res) => {
  const id = parseInt(req.params.id);
  const s = db.prepare("SELECT * FROM servicios WHERE id = ?").get(id);
  if (!s) return res.status(404).json({ error: "No encontrado" });
  const { temporada, tipo_servicio, semen_iatf, fecha_iatf, toro_natural, fecha_ingreso_toro, tacto_pre, cc_pre, resultado, notas } = req.body;
  db.prepare(`
    UPDATE servicios SET temporada=COALESCE(?,temporada), tipo_servicio=COALESCE(?,tipo_servicio), 
    semen_iatf=COALESCE(?,semen_iatf), fecha_iatf=COALESCE(?,fecha_iatf),
    toro_natural=COALESCE(?,toro_natural), fecha_ingreso_toro=COALESCE(?,fecha_ingreso_toro),
    tacto_pre=COALESCE(?,tacto_pre), cc_pre=COALESCE(?,cc_pre), resultado=COALESCE(?,resultado),
    notas=COALESCE(?,notas) WHERE id=?
  `).run(temporada||null, tipo_servicio||null, semen_iatf||null, fecha_iatf||null,
         toro_natural||null, fecha_ingreso_toro||null, tacto_pre||null, cc_pre||null,
         resultado||null, notas||null, id);
  res.json({ mensaje: `✅ Servicio #${id} actualizado` });
});

// ── IMPORTAR SERVICIOS CSV ──
app.post("/api/importar/servicios-csv", (req, res) => {
  const { registros } = req.body;
  if (!Array.isArray(registros)) return res.status(400).json({ error: "Formato inválido" });
  let ok = 0, errores = 0, noEncontrados = [];
  for (const r of registros) {
    const animal = buscarAnimalTodos(r.rp);
    if (!animal) { errores++; noEncontrados.push(r.rp || '?'); continue; }
    try {
      // Anti-duplicado: misma temporada + mismo animal
      const existe = db.prepare("SELECT id FROM servicios WHERE animal_id = ? AND temporada = ? AND tipo_servicio = ?")
        .get(animal.id, r.temporada || '', r.tipo_servicio || '');
      if (existe) {
        // Actualizar existente
        db.prepare(`UPDATE servicios SET semen_iatf=COALESCE(?,semen_iatf), fecha_iatf=COALESCE(?,fecha_iatf),
          toro_natural=COALESCE(?,toro_natural), fecha_ingreso_toro=COALESCE(?,fecha_ingreso_toro),
          cc_pre=COALESCE(?,cc_pre), resultado=COALESCE(?,resultado) WHERE id=?`)
          .run(r.semen_iatf||null, r.fecha_iatf||null, r.toro_natural||null, r.fecha_ingreso_toro||null,
               r.cc_pre||null, r.resultado||null, existe.id);
      } else {
        db.prepare(`INSERT INTO servicios (animal_id,temporada,tipo_servicio,semen_iatf,fecha_iatf,toro_natural,fecha_ingreso_toro,cc_pre,resultado,notas)
          VALUES (?,?,?,?,?,?,?,?,?,?)`)
          .run(animal.id, r.temporada||null, r.tipo_servicio||null, r.semen_iatf||null, r.fecha_iatf||null,
               r.toro_natural||null, r.fecha_ingreso_toro||null, r.cc_pre||null, r.resultado||null, r.notas||'CSV');
      }
      ok++;
    } catch(e) { errores++; }
  }
  let msg = `✅ Servicios CSV: ${ok} procesados, ${errores} errores.`;
  if (noEncontrados.length) msg += `\n❓ No encontrados: ${noEncontrados.slice(0,10).join(', ')}`;
  res.json({ mensaje: msg });
});

// ── SERVICIO MASIVO ──────────────────────────────────────────────────────────
app.post("/api/servicios/masivo", (req, res) => {
  const { rps, temporada, tipo_servicio, semen_iatf, fecha_iatf, toro_natural, fecha_ingreso_toro, cc_pre } = req.body;
  if (!Array.isArray(rps) || !rps.length) return res.status(400).json({ error: "Faltan RPs" });
  
  let ok = 0, errores = [], menores = [];
  const stmt = db.prepare(`
    INSERT INTO servicios (animal_id, temporada, tipo_servicio, semen_iatf, fecha_iatf, toro_natural, fecha_ingreso_toro, cc_pre, notas)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Masivo')
  `);
  
  for (const rp of rps) {
    const animal = buscarAnimal(rp);
    if (!animal) { errores.push(rp); continue; }
    
    // Validar edad: solo hembras de 12+ meses
    if (animal.sexo !== 'HEMBRA') { errores.push(`${rp} (macho)`); continue; }
    if (animal.fecha_nac) {
      const edadMeses = Math.floor((new Date() - new Date(animal.fecha_nac)) / (1000*60*60*24*30.44));
      if (edadMeses < 12) { menores.push(`${rp} (${edadMeses}m)`); continue; }
    }
    
    // Anti-duplicado: si ya tiene servicio de esta temporada, no duplicar
    const existe = db.prepare("SELECT id FROM servicios WHERE animal_id = ? AND temporada = ?").get(animal.id, temporada);
    if (existe) { errores.push(`${rp} (ya servida ${temporada})`); continue; }
    
    try {
      stmt.run(animal.id, temporada || null, tipo_servicio || null, semen_iatf || null,
               fecha_iatf || null, toro_natural || null, fecha_ingreso_toro || null, cc_pre || null);
      ok++;
    } catch(e) { errores.push(rp); }
  }
  
  let msg = `✅ Servicio masivo: ${ok} registrados | Temporada ${temporada || '?'}`;
  if (semen_iatf) msg += `\n🧬 IATF: ${semen_iatf}${fecha_iatf ? ` (${fecha_iatf})` : ''}`;
  if (toro_natural) msg += `\n🐂 Repaso: ${toro_natural}${fecha_ingreso_toro ? ` (desde ${fecha_ingreso_toro})` : ''}`;
  if (menores.length) msg += `\n⚠️ Menores de 12 meses (no servidas): ${menores.join(', ')}`;
  if (errores.length) msg += `\n❌ Errores: ${errores.join(', ')}`;
  res.json({ mensaje: msg });
});

// ── LOTES CRUD ───────────────────────────────────────────────────────────────
app.get("/api/lotes", (req, res) => {
  const lotes = db.prepare("SELECT * FROM lotes ORDER BY nombre").all();
  const result = lotes.map(l => {
    const animales = db.prepare(`
      SELECT a.* FROM animales a JOIN lote_animales la ON la.animal_id = a.id
      WHERE la.lote_id = ? AND a.estado = 'ACTIVO' ORDER BY a.rp
    `).all(l.id);
    const ultLectura = db.prepare("SELECT fecha, leidos, total_lote, faltantes, novedades FROM lecturas_lote WHERE lote_id = ? ORDER BY fecha DESC, id DESC LIMIT 1").get(l.id);
    return { ...l, animales, cantidad: animales.length, ultima_lectura: ultLectura || null };
  });
  res.json(result);
});

// Control manual de lote (con lista de RPs/chips leídos)
app.post("/api/lotes/:id/control", (req, res) => {
  const loteId = parseInt(req.params.id);
  const { rps, lecturas, fecha } = req.body;
  // Aceptar formato nuevo (lecturas: [{rp, chip}]) o viejo (rps: [...])
  const items = Array.isArray(lecturas) ? lecturas : (Array.isArray(rps) ? rps.map(r => ({ rp: r })) : []);
  if (!items.length) return res.status(400).json({ error: "Faltan lecturas" });
  
  const lote = db.prepare("SELECT * FROM lotes WHERE id = ?").get(loteId);
  if (!lote) return res.status(404).json({ error: "Lote no encontrado" });
  
  // Resolver por RP y/o chip
  const leidosIds = [];
  const noEncontrados = [];
  for (const item of items) {
    let a = null;
    if (item.rp) a = buscarAnimal(item.rp);
    if (!a && item.chip) {
      const chipNorm = String(item.chip).replace(/\s/g, '').replace(/^858000/, '');
      a = buscarAnimalPorChip(chipNorm, true);
    }
    if (a) {
      if (!leidosIds.includes(a.id)) leidosIds.push(a.id);
    } else {
      noEncontrados.push(item.rp || item.chip || '?');
    }
  }
  
  const totalLote = db.prepare(`SELECT COUNT(*) as n FROM lote_animales la JOIN animales a ON a.id = la.animal_id WHERE la.lote_id = ? AND a.estado = 'ACTIVO'`).get(loteId);
  
  const ph = leidosIds.length ? leidosIds.map(()=>'?').join(',') : 'NULL';
  const faltantes = db.prepare(`
    SELECT a.rp FROM lote_animales la JOIN animales a ON a.id = la.animal_id
    WHERE la.lote_id = ? AND a.estado = 'ACTIVO' ${leidosIds.length ? `AND a.id NOT IN (${ph})` : ''} ORDER BY a.rp
  `).all(loteId, ...leidosIds).map(x=>x.rp);
  
  const novedades = leidosIds.length ? db.prepare(`
    SELECT a.rp, COALESCE(l.nombre,'SIN LOTE') as lote FROM animales a
    LEFT JOIN lote_animales la ON la.animal_id = a.id
    LEFT JOIN lotes l ON l.id = la.lote_id
    WHERE a.id IN (${ph}) AND (la.lote_id IS NULL OR la.lote_id != ?) ORDER BY a.rp
  `).all(...leidosIds, loteId).map(x=>`${x.rp} (${x.lote})`) : [];
  
  const f = fecha || new Date().toISOString().split("T")[0];
  db.prepare("INSERT INTO lecturas_lote (lote_id, fecha, total_lote, leidos, faltantes, novedades, fuente) VALUES (?,?,?,?,?,?,'manual')")
    .run(loteId, f, totalLote.n, leidosIds.length, JSON.stringify(faltantes), JSON.stringify(novedades));
  
  let msg = `📋 Control ${lote.nombre} (${f})\n✅ Leídos: ${leidosIds.length}/${totalLote.n}`;
  if (faltantes.length) msg += `\n⚠️ Faltaron: ${faltantes.join(', ')}`;
  else msg += `\n✅ Pasaron todos`;
  if (novedades.length) msg += `\n🆕 Novedades: ${novedades.join(', ')}`;
  if (noEncontrados.length) msg += `\n❓ RPs no encontrados: ${noEncontrados.join(', ')}`;
  res.json({ mensaje: msg });
});

// Historial de lecturas de un lote
app.get("/api/lotes/:id/lecturas", (req, res) => {
  const rows = db.prepare("SELECT * FROM lecturas_lote WHERE lote_id = ? ORDER BY fecha DESC, id DESC LIMIT 20").all(req.params.id);
  res.json(rows.map(r => ({ ...r, faltantes: JSON.parse(r.faltantes||'[]'), novedades: JSON.parse(r.novedades||'[]') })));
});

app.post("/api/lotes", (req, res) => {
  const { nombre, descripcion, potrero } = req.body;
  if (!nombre) return res.status(400).json({ error: "Falta nombre" });
  try {
    const r = db.prepare("INSERT INTO lotes (nombre, descripcion, potrero) VALUES (?, ?, ?)")
      .run(nombre.toUpperCase(), descripcion || null, potrero || null);
    res.json({ id: r.lastInsertRowid, mensaje: `✅ Lote "${nombre}" creado` });
  } catch(e) {
    if (e.message.includes("UNIQUE")) return res.status(400).json({ error: `Lote "${nombre}" ya existe` });
    res.status(500).json({ error: e.message });
  }
});

app.put("/api/lotes/:id", (req, res) => {
  const { nombre, descripcion, potrero } = req.body;
  db.prepare("UPDATE lotes SET nombre=COALESCE(?,nombre), descripcion=COALESCE(?,descripcion), potrero=COALESCE(?,potrero) WHERE id=?")
    .run(nombre||null, descripcion||null, potrero||null, req.params.id);
  res.json({ mensaje: "✅ Lote actualizado" });
});

app.delete("/api/lotes/:id", (req, res) => {
  db.prepare("DELETE FROM lote_animales WHERE lote_id = ?").run(req.params.id);
  db.prepare("DELETE FROM lotes WHERE id = ?").run(req.params.id);
  res.json({ mensaje: "✅ Lote eliminado" });
});

// Agregar animales a un lote
app.post("/api/lotes/:id/animales", (req, res) => {
  const loteId = parseInt(req.params.id);
  const { rps } = req.body;
  if (!Array.isArray(rps)) return res.status(400).json({ error: "Formato: { rps: ['S219','S211'] }" });
  let ok = 0, errores = [];
  for (const rp of rps) {
    const animal = buscarAnimal(rp);
    if (!animal) { errores.push(rp); continue; }
    try {
      // Quitar de lote anterior si estaba en uno
      db.prepare("DELETE FROM lote_animales WHERE animal_id = ?").run(animal.id);
      db.prepare("INSERT INTO lote_animales (lote_id, animal_id) VALUES (?, ?)").run(loteId, animal.id);
      ok++;
    } catch(e) { errores.push(rp); }
  }
  let msg = `✅ ${ok} animales agregados al lote`;
  if (errores.length) msg += `\n⚠️ No encontrados: ${errores.join(', ')}`;
  res.json({ mensaje: msg });
});

// Quitar animales de un lote
app.delete("/api/lotes/:id/animales", (req, res) => {
  const { rps } = req.body;
  if (!Array.isArray(rps)) return res.status(400).json({ error: "Formato: { rps: ['S219'] }" });
  let ok = 0;
  for (const rp of rps) {
    const animal = buscarAnimal(rp);
    if (!animal) continue;
    db.prepare("DELETE FROM lote_animales WHERE lote_id = ? AND animal_id = ?").run(req.params.id, animal.id);
    ok++;
  }
  res.json({ mensaje: `✅ ${ok} animales removidos del lote` });
});

// Mover animales entre lotes
app.post("/api/lotes/mover", (req, res) => {
  const { rps, lote_destino_id } = req.body;
  if (!Array.isArray(rps) || !lote_destino_id) return res.status(400).json({ error: "Faltan datos" });
  let ok = 0, errores = [];
  for (const rp of rps) {
    const animal = buscarAnimal(rp);
    if (!animal) { errores.push(rp); continue; }
    db.prepare("DELETE FROM lote_animales WHERE animal_id = ?").run(animal.id);
    db.prepare("INSERT INTO lote_animales (lote_id, animal_id) VALUES (?, ?)").run(lote_destino_id, animal.id);
    ok++;
  }
  let msg = `✅ ${ok} animales movidos`;
  if (errores.length) msg += `\n⚠️ No encontrados: ${errores.join(', ')}`;
  res.json({ mensaje: msg });
});

// Acción masiva sobre un lote (sanidad, pesada)
// Ambos endpoints usan el mismo motor que el bot: un renglón por producto,
// descuento en IMPROLUX y costo repartido entre los animales tratados.
app.post("/api/lotes/:id/sanidad", async (req, res) => {
  const l = db.prepare("SELECT nombre FROM lotes WHERE id = ?").get(parseInt(req.params.id));
  if (!l) return res.status(404).json({ error: "Lote no encontrado" });
  try {
    res.json({ mensaje: await procesarSanidad({ ...req.body, lote: l.nombre }) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/api/sanidad/masiva", async (req, res) => {
  const { rps } = req.body;
  if (!Array.isArray(rps) || !rps.length) return res.status(400).json({ error: "Mandá una lista de RP" });
  try {
    res.json({ mensaje: await procesarSanidad({ ...req.body, rps }) });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── INFORMES PDF ─────────────────────────────────────────────────────────────

app.get("/api/informes/rodeo", (req, res) => {
  if (!PDFDocument) return res.status(500).json({ error: "pdfkit no instalado" });
  
  const resumen = getResumenRodeo();
  const animales = db.prepare("SELECT * FROM animales WHERE estado = 'ACTIVO' ORDER BY categoria, rp").all();
  
  const doc = new PDFDocument({ size: 'A4', margin: 50 });
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename=Rodeo_ADE_${new Date().toISOString().slice(0,10)}.pdf`);
  doc.pipe(res);
  
  // Header
  doc.fontSize(22).font('Helvetica-Bold').text('ANGUS DEL ESTE', { align: 'center' });
  doc.fontSize(10).font('Helvetica').fillColor('#666').text('Ganadería de Precisión — Informe de Rodeo', { align: 'center' });
  doc.moveDown(0.3);
  doc.fontSize(9).text(`Fecha: ${new Date().toISOString().slice(0,10)}`, { align: 'center' });
  doc.moveDown(1);
  doc.moveTo(50, doc.y).lineTo(545, doc.y).stroke('#ddd');
  doc.moveDown(0.5);
  
  // Resumen general
  doc.fontSize(14).font('Helvetica-Bold').fillColor('#000').text('Resumen General');
  doc.moveDown(0.3);
  doc.fontSize(10).font('Helvetica').text(`Total cabezas activas: ${resumen.total}`);
  doc.moveDown(0.3);
  
  // Tabla categorías
  const catMap = {};
  resumen.por_categoria.forEach(c => {
    const key = c.categoria || 'SIN CAT';
    if (!catMap[key]) catMap[key] = { machos: 0, hembras: 0 };
    if (c.sexo === 'MACHO') catMap[key].machos = c.n;
    else catMap[key].hembras = c.n;
  });
  
  doc.fontSize(9).font('Helvetica-Bold');
  let tableY = doc.y;
  doc.text('Categoría', 50, tableY, { width: 130 });
  doc.text('Machos', 180, tableY, { width: 80, align: 'right' });
  doc.text('Hembras', 260, tableY, { width: 80, align: 'right' });
  doc.text('Total', 340, tableY, { width: 80, align: 'right' });
  doc.moveDown(0.3);
  doc.moveTo(50, doc.y).lineTo(420, doc.y).stroke('#eee');
  doc.moveDown(0.2);
  
  doc.font('Helvetica').fontSize(9);
  for (const [cat, data] of Object.entries(catMap)) {
    tableY = doc.y;
    doc.text(cat, 50, tableY, { width: 130 });
    doc.text(String(data.machos || 0), 180, tableY, { width: 80, align: 'right' });
    doc.text(String(data.hembras || 0), 260, tableY, { width: 80, align: 'right' });
    doc.text(String((data.machos||0) + (data.hembras||0)), 340, tableY, { width: 80, align: 'right' });
    doc.moveDown(0.3);
  }
  
  // Pelo
  doc.moveDown(0.5);
  doc.fontSize(10).font('Helvetica-Bold').text('Distribución por Pelo');
  doc.moveDown(0.3);
  doc.fontSize(9).font('Helvetica');
  resumen.por_pelo.forEach(p => doc.text(`${p.pelo || 'Sin dato'}: ${p.n} cabezas`));
  
  // Listado por categoría
  const categorias = [...new Set(animales.map(a => a.categoria))];
  for (const cat of categorias) {
    doc.addPage();
    doc.fontSize(14).font('Helvetica-Bold').text(`${cat} — Listado`);
    doc.moveDown(0.3);
    doc.moveTo(50, doc.y).lineTo(545, doc.y).stroke('#ddd');
    doc.moveDown(0.3);
    
    const catAnimales = animales.filter(a => a.categoria === cat);
    doc.fontSize(8).font('Helvetica-Bold');
    tableY = doc.y;
    doc.text('RP', 50, tableY, {width:50});
    doc.text('Chip', 100, tableY, {width:80});
    doc.text('Sexo', 180, tableY, {width:50});
    doc.text('Pelo', 230, tableY, {width:60});
    doc.text('Nac.', 290, tableY, {width:70});
    doc.text('Registro', 360, tableY, {width:50});
    doc.text('Destino', 410, tableY, {width:50});
    doc.text('Madre', 460, tableY, {width:40});
    doc.text('Padre', 500, tableY, {width:45});
    doc.moveDown(0.2);
    doc.moveTo(50, doc.y).lineTo(545, doc.y).stroke('#eee');
    doc.y += 3;
    
    const ROW_H = 12;
    doc.font('Helvetica').fontSize(7);
    for (const a of catAnimales) {
      if (doc.y + ROW_H > 780) { doc.addPage(); doc.moveDown(0.5); }
      tableY = doc.y;
      doc.text(a.rp || '', 50, tableY, {width:50});
      doc.text(a.chip || '', 100, tableY, {width:80});
      doc.text(a.sexo || '', 180, tableY, {width:50});
      doc.text(a.pelo || '', 230, tableY, {width:60});
      doc.text(a.fecha_nac || '', 290, tableY, {width:70});
      doc.text(a.registro || '', 360, tableY, {width:50});
      doc.text(a.destino || '', 410, tableY, {width:50});
      doc.text(a.madre_rp || '', 460, tableY, {width:40});
      doc.text(a.padre_rp || '', 500, tableY, {width:45});
      doc.y = tableY + ROW_H;
    }
    doc.moveDown(0.3);
    doc.fontSize(9).font('Helvetica').fillColor('#666').text(`Total ${cat}: ${catAnimales.length} cabezas`);
    doc.fillColor('#000');
  }
  
  // Lotes
  const lotes = db.prepare("SELECT * FROM lotes ORDER BY nombre").all();
  if (lotes.length) {
    doc.addPage();
    doc.fontSize(14).font('Helvetica-Bold').text('Lotes');
    doc.moveDown(0.5);
    for (const l of lotes) {
      const animalesLote = db.prepare(`
        SELECT a.rp, a.categoria, a.sexo FROM animales a JOIN lote_animales la ON la.animal_id = a.id
        WHERE la.lote_id = ? AND a.estado = 'ACTIVO' ORDER BY a.rp
      `).all(l.id);
      doc.fontSize(11).font('Helvetica-Bold').text(`${l.nombre} — ${animalesLote.length} cabezas`);
      if (l.potrero) doc.fontSize(8).font('Helvetica').fillColor('#666').text(`Potrero: ${l.potrero}`);
      if (l.descripcion) doc.fontSize(8).text(l.descripcion);
      doc.fillColor('#000').moveDown(0.2);
      doc.fontSize(8).font('Helvetica');
      const rpList = animalesLote.map(a => a.rp).join(', ');
      doc.text(rpList || 'Sin animales asignados');
      doc.moveDown(0.5);
    }
  }
  
  doc.end();
});

// Informe de pesadas
app.get("/api/informes/pesadas", (req, res) => {
  if (!PDFDocument) return res.status(500).json({ error: "pdfkit no instalado" });
  
  const pesadas = db.prepare(`
    SELECT p.*, a.rp, a.categoria, a.sexo, a.pelo FROM pesadas p
    JOIN animales a ON a.id = p.animal_id
    ORDER BY p.fecha DESC LIMIT 500
  `).all();
  
  const doc = new PDFDocument({ size: 'A4', margin: 40, layout: 'landscape' });
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename=Pesadas_ADE_${new Date().toISOString().slice(0,10)}.pdf`);
  doc.pipe(res);
  
  doc.fontSize(16).font('Helvetica-Bold').text('ANGUS DEL ESTE — Informe de Pesadas', { align: 'center' });
  doc.fontSize(9).font('Helvetica').fillColor('#666').text(`Fecha: ${new Date().toISOString().slice(0,10)} | ${pesadas.length} registros`, { align: 'center' });
  doc.moveDown(0.3);
  doc.moveTo(40, doc.y).lineTo(800, doc.y).stroke('#ccc');
  doc.moveDown(0.3);
  
  const ROW_H = 12;
  const cols = [
    {x:40,w:55,h:'RP'},{x:95,w:60,h:'Cat.'},{x:155,w:50,h:'Sexo'},
    {x:205,w:60,h:'Peso (kg)',a:'right'},{x:275,w:70,h:'Fecha'},
    {x:345,w:70,h:'Contexto'},{x:425,w:60,h:'GDP (g/d)',a:'right'}
  ];
  
  function drawHeader() {
    doc.fontSize(8).font('Helvetica-Bold').fillColor('#333');
    const ty = doc.y;
    cols.forEach(c => doc.text(c.h, c.x, ty, {width:c.w, align:c.a||'left'}));
    doc.moveDown(0.1);
    doc.moveTo(40, doc.y).lineTo(490, doc.y).stroke('#ddd');
    doc.y += 3;
  }
  
  drawHeader();
  doc.font('Helvetica').fontSize(7).fillColor('#000');
  
  for (const p of pesadas) {
    if (doc.y + ROW_H > 545) { doc.addPage(); drawHeader(); doc.font('Helvetica').fontSize(7).fillColor('#000'); }
    const ty = doc.y;
    doc.text(p.rp||'', cols[0].x, ty, {width:cols[0].w});
    doc.text(p.categoria||'', cols[1].x, ty, {width:cols[1].w});
    doc.text(p.sexo||'', cols[2].x, ty, {width:cols[2].w});
    doc.text(fmt(p.peso), cols[3].x, ty, {width:cols[3].w, align:'right'});
    doc.text(p.fecha||'', cols[4].x, ty, {width:cols[4].w});
    doc.text(p.contexto||'', cols[5].x, ty, {width:cols[5].w});
    doc.text(p.gdp ? fmt(p.gdp*1000) : '', cols[6].x, ty, {width:cols[6].w, align:'right'});
    doc.y = ty + ROW_H;
  }
  doc.end();
});

// Informe de servicios reproductivos
app.get("/api/informes/servicios", (req, res) => {
  if (!PDFDocument) return res.status(500).json({ error: "pdfkit no instalado" });
  
  const temporada = req.query.temporada;
  let query = `SELECT s.*, a.rp, a.categoria FROM servicios s JOIN animales a ON a.id = s.animal_id`;
  const params = [];
  if (temporada) { query += " WHERE s.temporada = ?"; params.push(temporada); }
  query += " ORDER BY s.temporada DESC, a.rp";
  const servicios = db.prepare(query).all(...params);
  
  const doc = new PDFDocument({ size: 'A4', margin: 40, layout: 'landscape' });
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename=Servicios_ADE_${temporada||'todos'}_${new Date().toISOString().slice(0,10)}.pdf`);
  doc.pipe(res);
  
  doc.fontSize(16).font('Helvetica-Bold').text(`ANGUS DEL ESTE — Servicios${temporada ? ` Temporada ${temporada}` : ''}`, { align: 'center' });
  doc.fontSize(9).font('Helvetica').fillColor('#666').text(`${servicios.length} registros | ${new Date().toISOString().slice(0,10)}`, { align: 'center' });
  doc.moveDown(0.3);
  
  const prenadas = servicios.filter(s => s.resultado && s.resultado.includes('PREÑADA')).length;
  const vacias = servicios.filter(s => s.resultado === 'VACIA').length;
  const pendientes = servicios.filter(s => !s.resultado).length;
  doc.fontSize(9).font('Helvetica-Bold').fillColor('#000');
  doc.text(`Preñadas: ${prenadas} | Vacías: ${vacias} | Pendientes: ${pendientes} | % Preñez: ${(prenadas+vacias) ? ((prenadas/(prenadas+vacias))*100).toFixed(1) : 0}%`);
  doc.moveDown(0.3);
  doc.moveTo(40, doc.y).lineTo(800, doc.y).stroke('#ccc');
  doc.moveDown(0.3);
  
  const ROW_H = 12;
  const cols = [
    {x:40,w:40,h:'RP'},{x:80,w:35,h:'Temp.'},{x:115,w:45,h:'Tipo'},
    {x:160,w:80,h:'Semen IATF'},{x:240,w:58,h:'F.IATF'},
    {x:298,w:80,h:'Toro Repaso'},{x:378,w:58,h:'F.Toro'},
    {x:436,w:22,h:'CC'},{x:458,w:70,h:'Resultado'},
    {x:528,w:40,h:'Cría'},{x:568,w:30,h:'P.Nac'}
  ];
  
  function drawHeader() {
    doc.fontSize(7).font('Helvetica-Bold').fillColor('#333');
    const ty = doc.y;
    cols.forEach(c => doc.text(c.h, c.x, ty, {width:c.w}));
    doc.moveDown(0.1);
    doc.moveTo(40, doc.y).lineTo(600, doc.y).stroke('#ddd');
    doc.y += 3;
  }
  
  drawHeader();
  doc.font('Helvetica').fontSize(6.5).fillColor('#000');
  
  for (const s of servicios) {
    if (doc.y + ROW_H > 545) { doc.addPage(); drawHeader(); doc.font('Helvetica').fontSize(6.5).fillColor('#000'); }
    const ty = doc.y;
    doc.text(s.rp||'', cols[0].x, ty, {width:cols[0].w});
    doc.text(s.temporada||'', cols[1].x, ty, {width:cols[1].w});
    doc.text(s.tipo_servicio||'', cols[2].x, ty, {width:cols[2].w});
    doc.text(s.semen_iatf||'', cols[3].x, ty, {width:cols[3].w});
    doc.text(s.fecha_iatf||'', cols[4].x, ty, {width:cols[4].w});
    doc.text(s.toro_natural||'', cols[5].x, ty, {width:cols[5].w});
    doc.text(s.fecha_ingreso_toro||'', cols[6].x, ty, {width:cols[6].w});
    doc.text(s.cc_pre ? String(s.cc_pre) : '', cols[7].x, ty, {width:cols[7].w});
    doc.text(s.resultado||'—', cols[8].x, ty, {width:cols[8].w});
    doc.text(s.ternero_rp||'', cols[9].x, ty, {width:cols[9].w});
    doc.text(s.peso_nacimiento ? String(s.peso_nacimiento) : '', cols[10].x, ty, {width:cols[10].w});
    doc.y = ty + ROW_H;
  }
  doc.end();
});

// Informe de sanidad
app.get("/api/informes/sanidad", (req, res) => {
  if (!PDFDocument) return res.status(500).json({ error: "pdfkit no instalado" });
  
  const sanidad = db.prepare(`
    SELECT s.*, a.rp, a.categoria FROM sanidad s 
    JOIN animales a ON a.id = s.animal_id ORDER BY s.fecha DESC LIMIT 500
  `).all();
  
  const doc = new PDFDocument({ size: 'A4', margin: 40 });
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename=Sanidad_ADE_${new Date().toISOString().slice(0,10)}.pdf`);
  doc.pipe(res);
  
  doc.fontSize(16).font('Helvetica-Bold').text('ANGUS DEL ESTE — Informe Sanitario', { align: 'center' });
  doc.fontSize(9).font('Helvetica').fillColor('#666').text(`${sanidad.length} registros | ${new Date().toISOString().slice(0,10)}`, { align: 'center' });
  doc.moveDown(0.3); doc.moveTo(40, doc.y).lineTo(555, doc.y).stroke('#ccc'); doc.moveDown(0.3);
  
  const ROW_H = 13;
  const cols = [
    {x:40,w:65,h:'Fecha'},{x:105,w:50,h:'RP'},{x:155,w:60,h:'Cat.'},
    {x:215,w:80,h:'Tipo'},{x:295,w:130,h:'Producto'},{x:425,w:70,h:'Dosis'}
  ];
  
  function drawHeader() {
    doc.fontSize(8).font('Helvetica-Bold').fillColor('#333');
    const ty = doc.y;
    cols.forEach(c => doc.text(c.h, c.x, ty, {width:c.w}));
    doc.moveDown(0.1);
    doc.moveTo(40, doc.y).lineTo(500, doc.y).stroke('#ddd');
    doc.y += 3;
  }
  
  drawHeader();
  doc.font('Helvetica').fontSize(7).fillColor('#000');
  
  for (const s of sanidad) {
    if (doc.y + ROW_H > 780) { doc.addPage(); drawHeader(); doc.font('Helvetica').fontSize(7).fillColor('#000'); }
    const ty = doc.y;
    doc.text(s.fecha||'', cols[0].x, ty, {width:cols[0].w});
    doc.text(s.rp||'', cols[1].x, ty, {width:cols[1].w});
    doc.text(s.categoria||'', cols[2].x, ty, {width:cols[2].w});
    doc.text(s.tipo||'', cols[3].x, ty, {width:cols[3].w});
    doc.text(s.producto||'', cols[4].x, ty, {width:cols[4].w});
    doc.text(s.dosis||'', cols[5].x, ty, {width:cols[5].w});
    doc.y = ty + ROW_H;
  }
  doc.end();
});

// ── STATS VACAS (PVA, peso destete crías, CC promedio) ───────────────────────
app.get("/api/stats/vacas", (req, res) => {
  try {
    const vacas = db.prepare("SELECT id, rp FROM animales WHERE estado = 'ACTIVO' AND (categoria = 'VACA' OR categoria = 'VAQUILLONA')").all();
    const stats = {};
    
    for (const v of vacas) {
      // PVA: promedio de pesadas ADULTA. Si no hay, usar último peso (no NAC/DEST)
      let pva = null;
      try {
        const pvaRow = db.prepare("SELECT AVG(peso) as pva FROM pesadas WHERE animal_id = ? AND contexto = 'ADULTA'").get(v.id);
        pva = (pvaRow && pvaRow.pva) ? pvaRow.pva : null;
        if (!pva) {
          const fallback = db.prepare("SELECT peso FROM pesadas WHERE animal_id = ? AND contexto NOT IN ('NACIMIENTO','DESTETE') ORDER BY fecha DESC LIMIT 1").get(v.id);
          pva = (fallback && fallback.peso) ? fallback.peso : null;
        }
      } catch(e) {}
      
      // CC promedio de la vaca
      let cc_prom = null;
      try {
        const ccRow = db.prepare("SELECT AVG(valor) as cc FROM mediciones WHERE animal_id = ? AND tipo = 'CC' AND valor > 0").get(v.id);
        cc_prom = (ccRow && ccRow.cc) ? ccRow.cc : null;
      } catch(e) {}
      
      // Peso promedio al destete de sus hijos
      const rp = v.rp;
      const rpSinADE = rp.toUpperCase().startsWith('ADE') ? rp.substring(3) : rp;
      const rpConADE = rp.toUpperCase().startsWith('ADE') ? rp : 'ADE' + rp;
      
      let peso_dest_crias = null, n_crias_destete = 0;
      try {
        const hijosDestete = db.prepare(`
          SELECT AVG(p.peso) as prom, COUNT(*) as n FROM pesadas p
          JOIN animales a ON a.id = p.animal_id
          WHERE (LOWER(a.madre_rp) = LOWER(?) OR LOWER(a.madre_rp) = LOWER(?) OR LOWER(a.madre_rp) = LOWER(?))
          AND p.contexto = 'DESTETE' AND p.peso > 0
        `).get(rp, rpSinADE, rpConADE);
        peso_dest_crias = (hijosDestete && hijosDestete.prom) ? hijosDestete.prom : null;
        n_crias_destete = (hijosDestete && hijosDestete.n) ? hijosDestete.n : 0;
      } catch(e) {}
      
      stats[v.rp] = { pva, cc_prom, peso_dest_crias, n_crias_destete };
    }
    
    res.json(stats);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ── RECALCULAR CONTEXTOS, GDP Y CATEGORÍAS ───────────────────────────────────
app.post("/api/recalcular", (req, res) => {
  try {
    const pesadas = db.prepare(`
      SELECT p.id, p.animal_id, p.fecha, p.peso, p.contexto, a.fecha_nac, a.categoria 
      FROM pesadas p JOIN animales a ON a.id = p.animal_id
    `).all();
    
    let contextos = 0, gdps = 0, categorias = 0;
    
    for (const p of pesadas) {
      const nuevoCtx = determinarContextoPesada(p.fecha_nac, p.fecha, p.categoria);
      if (nuevoCtx !== p.contexto) {
        db.prepare("UPDATE pesadas SET contexto = ? WHERE id = ?").run(nuevoCtx, p.id);
        contextos++;
      }
    }
    
    // Recalcular GDP
    const animalIds = db.prepare("SELECT DISTINCT animal_id FROM pesadas").all();
    for (const { animal_id } of animalIds) {
      const gdp = calcularGDP(animal_id);
      if (gdp !== null) {
        const ultima = db.prepare("SELECT id FROM pesadas WHERE animal_id = ? ORDER BY fecha DESC LIMIT 1").get(animal_id);
        if (ultima) { db.prepare("UPDATE pesadas SET gdp = ? WHERE id = ?").run(gdp, ultima.id); gdps++; }
      }
    }
    
    // ── ACTUALIZAR CATEGORÍAS POR EDAD Y ESTADO REPRODUCTIVO ──
    const animales = db.prepare("SELECT * FROM animales WHERE estado = 'ACTIVO' AND fecha_nac IS NOT NULL").all();
    for (const a of animales) {
      const edadMeses = Math.floor((new Date() - new Date(a.fecha_nac)) / (1000*60*60*24*30.44));
      let nuevaCat = a.categoria;
      
      // VACA y TORO confirmados no se tocan
      if (a.categoria === 'VACA' || a.categoria === 'TORO') continue;
      
      if (edadMeses < 7) {
        nuevaCat = 'TERNERO';
      } else if (edadMeses < 13) {
        nuevaCat = 'RECRIA';  // Recría 1 año
      } else if (edadMeses < 19) {
        nuevaCat = 'RECRIA';  // Recría 2 años
      } else if (a.sexo === 'MACHO') {
        // Macho > 19 meses → TORO (el usuario decide plantel/venta desde la ficha)
        nuevaCat = 'TORO';
      } else if (a.sexo === 'HEMBRA') {
        // Hembra > 19 meses → verificar si tiene tacto
        const tacto = db.prepare("SELECT resultado FROM servicios WHERE animal_id = ? AND resultado IS NOT NULL ORDER BY created_at DESC LIMIT 1").get(a.id);
        if (tacto && tacto.resultado === 'PREÑADA') {
          nuevaCat = 'VACA';
        } else {
          nuevaCat = 'VAQUILLONA';
        }
      }
      
      if (nuevaCat !== a.categoria) {
        db.prepare("UPDATE animales SET categoria = ? WHERE id = ?").run(nuevaCat, a.id);
        categorias++;
      }
    }
    
    res.json({ mensaje: `✅ Recalculado:\n📋 ${contextos} contextos pesadas\n📈 ${gdps} GDP\n🏷️ ${categorias} categorías actualizadas` });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ── RESET DB ──────────────────────────────────────────────────────────────────
app.post("/api/reset", (req, res) => {
  try {
    db.exec("DELETE FROM sanidad");
    db.exec("DELETE FROM mediciones");
    db.exec("DELETE FROM ecografias");
    db.exec("DELETE FROM servicios");
    db.exec("DELETE FROM pesadas");
    db.exec("DELETE FROM toros");
    db.exec("DELETE FROM lote_animales");
    db.exec("DELETE FROM lotes");
    db.exec("DELETE FROM animales");
    db.exec("DELETE FROM sesiones");
    res.json({ mensaje: "✅ Base de datos limpia." });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});


// ── LIMPIAR HISTORIAL CHAT ──
app.post("/api/limpiar-historial", (req, res) => {
  db.prepare("DELETE FROM sesiones").run();
  res.json({ mensaje: "✅ Historial limpiado" });
});

// ── HBU MANAGEMENT ───────────────────────────────────────────────────────────
// GET: listar animales con/sin HBU (para la vista INIA)
app.get("/api/hbu", (req, res) => {
  const animales = db.prepare(`
    SELECT id, rp, hbu, registro, categoria, sexo, madre_rp, madre_hba, padre_rp, padre_hba 
    FROM animales WHERE estado = 'ACTIVO' 
    ORDER BY registro DESC, rp
  `).all();
  res.json(animales);
});

// PUT: actualizar HBU de un animal
app.put("/api/hbu/:id", (req, res) => {
  const { hbu, madre_hba, padre_hba, mellizo_de } = req.body;
  const sets = [];
  const vals = [];
  if (hbu !== undefined) { sets.push("hbu = ?"); vals.push(hbu || null); }
  if (madre_hba !== undefined) { sets.push("madre_hba = ?"); vals.push(madre_hba || null); }
  if (padre_hba !== undefined) { sets.push("padre_hba = ?"); vals.push(padre_hba || null); }
  if (mellizo_de !== undefined) { sets.push("mellizo_de = ?"); vals.push(mellizo_de || null); }
  if (!sets.length) return res.status(400).json({ error: "No hay campos para actualizar" });
  vals.push(req.params.id);
  db.prepare(`UPDATE animales SET ${sets.join(', ')} WHERE id = ?`).run(...vals);
  res.json({ ok: true });
});

// POST: carga masiva de HBU [{rp, hbu, madre_hba, padre_hba}]
app.post("/api/hbu/importar", (req, res) => {
  const { registros } = req.body;
  if (!Array.isArray(registros)) return res.status(400).json({ error: "Formato inválido" });
  let ok = 0, errores = [];
  for (const r of registros) {
    const animal = buscarAnimal(r.rp);
    if (!animal) { errores.push(r.rp); continue; }
    const sets = [];
    const vals = [];
    if (r.hbu) { sets.push("hbu = ?"); vals.push(r.hbu); }
    if (r.madre_hba) { sets.push("madre_hba = ?"); vals.push(r.madre_hba); }
    if (r.padre_hba) { sets.push("padre_hba = ?"); vals.push(r.padre_hba); }
    if (sets.length) {
      vals.push(animal.id);
      db.prepare(`UPDATE animales SET ${sets.join(', ')} WHERE id = ?`).run(...vals);
      ok++;
    }
  }
  res.json({ ok, errores, total: registros.length });
});

// ══════════════════════════════════════════════════════════════════════════════
// ── MÓDULO: COSTOS POR ANIMAL ────────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════════

// Costos fijos del campo
app.get("/api/costos/campo", (req, res) => {
  try {
    const rows = db.prepare("SELECT * FROM costos_campo ORDER BY concepto").all();
    res.json(rows);
  } catch(e) { res.json([]); }
});

app.post("/api/costos/campo", (req, res) => {
  const { concepto, monto_mensual, notas } = req.body;
  if (!concepto || !monto_mensual) return res.status(400).json({ error: "Falta concepto y monto mensual" });
  db.prepare("INSERT INTO costos_campo (concepto, monto_mensual, notas) VALUES (?,?,?)")
    .run(concepto, parseFloat(monto_mensual), notas||'');
  res.json({ mensaje: `✅ Costo fijo creado: ${concepto} $${monto_mensual}/mes` });
});

app.delete("/api/costos/campo/:id", (req, res) => {
  db.prepare("DELETE FROM costos_campo WHERE id = ?").run(req.params.id);
  res.json({ mensaje: "✅ Eliminado" });
});

// Repartir costos fijos del mes entre animales activos
app.post("/api/costos/repartir-mensual", (req, res) => {
  const mes = req.body.mes || new Date().toISOString().slice(0, 7); // YYYY-MM
  
  // Verificar si ya se repartió este mes
  const yaRepartido = db.prepare("SELECT COUNT(*) as n FROM costos WHERE fecha LIKE ? AND concepto LIKE 'ALQUILER%' AND detalle LIKE '%reparto mensual%'").get(`${mes}-%`);
  if (yaRepartido.n > 0) return res.json({ mensaje: `⚠️ Los costos fijos de ${mes} ya fueron repartidos.` });
  
  // Sólo costos fijos del campo (alquiler, sueldos, estructura). Lo que se
  // consume en una etapa del protocolo ya se imputó a cada vaca al ejecutarla:
  // repartirlo de nuevo sería contarlo dos veces.
  const costosFijos = db.prepare("SELECT * FROM costos_campo WHERE activo = 1").all();
  if (!costosFijos.length) return res.json({ mensaje: "⚠️ No hay costos fijos activos." });
  
  const animalesActivos = db.prepare("SELECT id, rp FROM animales WHERE estado = 'ACTIVO'").all();
  if (!animalesActivos.length) return res.json({ mensaje: "⚠️ No hay animales activos." });
  
  const nAnimales = animalesActivos.length;
  let totalRepartido = 0;
  const fecha = `${mes}-01`;
  
  for (const cf of costosFijos) {
    const costoPorAnimal = cf.monto_mensual / nAnimales;
    for (const a of animalesActivos) {
      db.prepare("INSERT INTO costos (animal_id, fecha, concepto, detalle, monto) VALUES (?,?,?,?,?)")
        .run(a.id, fecha, cf.concepto, `reparto mensual ${mes} ($${cf.monto_mensual}/${nAnimales} cab.)`, costoPorAnimal);
    }
    totalRepartido += cf.monto_mensual;
  }
  
  res.json({ mensaje: `✅ Costos fijos de ${mes} repartidos:\n💰 $${totalRepartido.toFixed(2)} total → $${(totalRepartido/nAnimales).toFixed(2)}/animal (${nAnimales} cab.)` });
});

app.get("/api/costos", (req, res) => {
  const animal_id = req.query.animal_id;
  if (animal_id) {
    const rows = db.prepare("SELECT * FROM costos WHERE animal_id = ? ORDER BY fecha DESC").all(animal_id);
    const total = db.prepare("SELECT SUM(monto) as total FROM costos WHERE animal_id = ?").get(animal_id);
    res.json({ costos: rows, total: total?.total || 0 });
  } else {
    // Resumen: costo total por animal
    const rows = db.prepare(`
      SELECT a.rp, a.categoria, a.sexo, SUM(c.monto) as costo_total, COUNT(c.id) as n_registros,
        (SELECT peso FROM pesadas WHERE animal_id = a.id ORDER BY fecha DESC LIMIT 1) as ultimo_peso
      FROM costos c JOIN animales a ON a.id = c.animal_id
      GROUP BY c.animal_id ORDER BY costo_total DESC
    `).all();
    res.json(rows);
  }
});

app.post("/api/costos", (req, res) => {
  const { animal_id, rp, fecha, concepto, detalle, monto, moneda, notas } = req.body;
  let aid = animal_id;
  if (!aid && rp) { const a = buscarAnimalTodos(rp); if (a) aid = a.id; }
  if (!aid) return res.status(400).json({ error: "Animal no encontrado" });
  db.prepare("INSERT INTO costos (animal_id, fecha, concepto, detalle, monto, moneda, notas) VALUES (?,?,?,?,?,?,?)")
    .run(aid, fecha || new Date().toISOString().slice(0,10), concepto, detalle||'', parseFloat(monto)||0, moneda||'USD', notas||'');
  res.json({ mensaje: "✅ Costo registrado" });
});

app.post("/api/costos/masivo", (req, res) => {
  const { rps, fecha, concepto, detalle, monto, notas } = req.body;
  if (!Array.isArray(rps)) return res.status(400).json({ error: "Formato inválido" });
  let ok = 0;
  for (const rp of rps) {
    const a = buscarAnimalTodos(rp);
    if (!a) continue;
    db.prepare("INSERT INTO costos (animal_id, fecha, concepto, detalle, monto, notas) VALUES (?,?,?,?,?,?)")
      .run(a.id, fecha || new Date().toISOString().slice(0,10), concepto, detalle||'', parseFloat(monto)||0, notas||'');
    ok++;
  }
  res.json({ mensaje: `✅ Costo registrado a ${ok} animales` });
});

app.delete("/api/costos/:id", (req, res) => {
  db.prepare("DELETE FROM costos WHERE id = ?").run(req.params.id);
  res.json({ mensaje: "✅ Costo eliminado" });
});

// ══════════════════════════════════════════════════════════════════════════════
// ── MÓDULO: STOCK GENÉTICA (embriones y pajuelas) ────────────────────────────
// ══════════════════════════════════════════════════════════════════════════════

app.get("/api/stock/genetica", (req, res) => {
  const rows = db.prepare("SELECT * FROM stock_genetica WHERE cantidad > 0 ORDER BY tipo, toro_nombre").all();
  res.json(rows);
});

app.post("/api/stock/genetica", (req, res) => {
  const { tipo, toro_nombre, toro_rp, toro_hba, donante_nombre, donante_rp, donante_hba, raza, cantidad, costo_unitario, proveedor, fecha_colecta, ubicacion, notas } = req.body;
  db.prepare(`INSERT INTO stock_genetica (tipo,toro_nombre,toro_rp,toro_hba,donante_nombre,donante_rp,donante_hba,raza,cantidad,costo_unitario,proveedor,fecha_colecta,ubicacion,notas) 
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
    .run(tipo, toro_nombre||'', toro_rp||'', toro_hba||'', donante_nombre||'', donante_rp||'', donante_hba||'', raza||'A. ANGUS', cantidad||0, costo_unitario||0, proveedor||'', fecha_colecta||'', ubicacion||'', notas||'');
  res.json({ mensaje: `✅ ${tipo} registrado: ${toro_nombre||''} x ${cantidad}` });
});

app.put("/api/stock/genetica/:id", (req, res) => {
  const { cantidad, uso } = req.body;
  if (uso) {
    // Descontar uso
    db.prepare("UPDATE stock_genetica SET cantidad = cantidad - ? WHERE id = ? AND cantidad >= ?").run(uso, req.params.id, uso);
  } else if (cantidad !== undefined) {
    db.prepare("UPDATE stock_genetica SET cantidad = ? WHERE id = ?").run(cantidad, req.params.id);
  }
  const item = db.prepare("SELECT * FROM stock_genetica WHERE id = ?").get(req.params.id);
  res.json({ mensaje: `✅ Stock actualizado: ${item?.cantidad || 0}`, item });
});

// ══════════════════════════════════════════════════════════════════════════════
// ── MÓDULO: DASHBOARD PRODUCTIVIDAD ──────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════════

// Diagnóstico: ver hijos de un padre
app.get("/api/diagnostico/padre/:rp", (req, res) => {
  const padre = req.params.rp;
  const hijos = db.prepare(`
    SELECT a.rp, a.fecha_nac, a.categoria, a.estado,
      (SELECT p.gdp FROM pesadas p WHERE p.animal_id = a.id AND p.gdp > 0 ORDER BY p.fecha DESC LIMIT 1) as gdp,
      (SELECT COUNT(*) FROM pesadas p WHERE p.animal_id = a.id) as n_pesadas
    FROM animales a WHERE a.padre_rp = ? ORDER BY a.rp
  `).all(padre);
  res.json({ padre, total_hijos: hijos.length, hijos });
});

app.get("/api/productividad", (req, res) => {
  // GDP por padre: promedia UN gdp por hijo (el último válido), no todas las pesadas
  const gdpPorPadre = db.prepare(`
    WITH gdp_por_hijo AS (
      SELECT a.id, a.padre_rp,
        (SELECT p.gdp FROM pesadas p WHERE p.animal_id = a.id AND p.gdp > 0 AND p.gdp <= 2.5 ORDER BY p.fecha DESC LIMIT 1) as gdp_hijo,
        (SELECT p.peso FROM pesadas p WHERE p.animal_id = a.id AND p.contexto='DESTETE' LIMIT 1) as peso_dest,
        (SELECT p.peso FROM pesadas p WHERE p.animal_id = a.id AND p.contexto='NACIMIENTO' LIMIT 1) as peso_nac
      FROM animales a
      WHERE a.padre_rp IS NOT NULL AND a.padre_rp != ''
    )
    SELECT padre_rp as padre, COUNT(*) as n_hijos,
      AVG(gdp_hijo) as gdp_prom,
      AVG(peso_dest) as peso_dest_prom,
      AVG(peso_nac) as peso_nac_prom
    FROM gdp_por_hijo
    GROUP BY padre_rp HAVING n_hijos >= 2 AND gdp_prom IS NOT NULL
    ORDER BY gdp_prom DESC
  `).all();
  
  // Eficiencia reproductiva por vaca
  const eficienciaVacas = db.prepare(`
    SELECT a.rp, a.fecha_nac, a.registro,
      COUNT(s.id) as total_servicios,
      SUM(CASE WHEN s.fecha_parto IS NOT NULL OR s.peso_nacimiento IS NOT NULL OR s.ternero_rp IS NOT NULL THEN 1 ELSE 0 END) as partos,
      SUM(CASE WHEN s.resultado = 'VACIA' THEN 1 ELSE 0 END) as vacias,
      AVG(s.cc_pre) as cc_promedio
    FROM animales a
    JOIN servicios s ON s.animal_id = a.id
    WHERE a.categoria = 'VACA' AND a.estado = 'ACTIVO'
    GROUP BY a.id ORDER BY partos DESC
  `).all();
  
  // Ranking vacas por PPD (peso promedio destete de crías)
  const rankingPPD = db.prepare(`
    SELECT a.rp, a.registro,
      COUNT(CASE WHEN p2.contexto='DESTETE' THEN 1 END) as n_crias_dest,
      AVG(CASE WHEN p2.contexto='DESTETE' THEN p2.peso END) as ppd,
      AVG(CASE WHEN p2.contexto='NACIMIENTO' THEN p2.peso END) as ppn
    FROM animales a
    JOIN animales hijo ON hijo.madre_rp = a.rp
    JOIN pesadas p2 ON p2.animal_id = hijo.id
    WHERE a.categoria = 'VACA'
    GROUP BY a.id HAVING n_crias_dest >= 1
    ORDER BY ppd DESC
  `).all();
  
  res.json({ gdpPorPadre, eficienciaVacas, rankingPPD });
});

// ══════════════════════════════════════════════════════════════════════════════
// ── MÓDULO: ALERTAS AUTOMÁTICAS ──────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════════

app.get("/api/alertas", (req, res) => {
  const hoy = new Date().toISOString().slice(0,10);
  const alertas = [];
  
  // 1. Terneros en edad de destete (>180 días sin pesada DESTETE)
  const terneros = db.prepare(`
    SELECT a.rp, a.fecha_nac, a.categoria, 
      CAST((julianday(?) - julianday(a.fecha_nac)) AS INTEGER) as dias,
      (SELECT COUNT(*) FROM pesadas WHERE animal_id = a.id AND contexto = 'DESTETE') as tiene_destete
    FROM animales a 
    WHERE a.estado = 'ACTIVO' AND a.fecha_nac IS NOT NULL
    AND CAST((julianday(?) - julianday(a.fecha_nac)) AS INTEGER) BETWEEN 160 AND 300
    AND (SELECT COUNT(*) FROM pesadas WHERE animal_id = a.id AND contexto = 'DESTETE') = 0
  `).all(hoy, hoy);
  terneros.forEach(t => alertas.push({ tipo: 'DESTETE', prioridad: 'ALTA', rp: t.rp, mensaje: `${t.rp} tiene ${t.dias} días, pendiente destete`, dias: t.dias }));
  
  // 2. Vacas sin servicio en la última temporada
  const anioActual = new Date().getFullYear().toString();
  const vacasSinServ = db.prepare(`
    SELECT a.rp FROM animales a 
    WHERE a.categoria = 'VACA' AND a.estado = 'ACTIVO'
    AND NOT EXISTS (SELECT 1 FROM servicios s WHERE s.animal_id = a.id AND s.temporada >= ?)
  `).all(String(parseInt(anioActual) - 1));
  vacasSinServ.forEach(v => alertas.push({ tipo: 'SERVICIO', prioridad: 'MEDIA', rp: v.rp, mensaje: `${v.rp} sin servicio registrado reciente` }));
  
  // 3. Tacto pendiente (preñadas sin resultado de esta temporada)
  const sinTacto = db.prepare(`
    SELECT a.rp, s.temporada FROM servicios s 
    JOIN animales a ON a.id = s.animal_id
    WHERE s.resultado IS NULL AND s.temporada >= ? AND a.estado = 'ACTIVO'
  `).all(String(parseInt(anioActual) - 1));
  sinTacto.forEach(s => alertas.push({ tipo: 'TACTO', prioridad: 'MEDIA', rp: s.rp, mensaje: `${s.rp} servicio ${s.temporada} sin resultado de tacto` }));
  
  // 4. Ración por acabarse. El stock real está en IMPROLUX; acá sabemos el consumo.
  try {
    const ds = db.prepare(`SELECT d.producto,
        SUM(CASE WHEN d.modo='POR_ANIMAL'
          THEN d.kg_dia * (SELECT COUNT(*) FROM lote_animales la WHERE la.lote_id = d.lote_id)
          ELSE d.kg_dia END) kg_dia,
        (SELECT stock_improlux FROM costeo_productos p WHERE UPPER(TRIM(p.producto)) = UPPER(TRIM(d.producto))) stock
      FROM costeo_dietas d WHERE d.activo = 1 GROUP BY UPPER(TRIM(d.producto))`).all();
    ds.forEach(x => {
      if (x.kg_dia > 0 && x.stock > 0) {
        const dias = Math.floor(x.stock / x.kg_dia);
        if (dias < 15) alertas.push({ tipo: 'ALIMENTO', prioridad: dias < 7 ? 'URGENTE' : 'ALTA',
          mensaje: `${x.producto}: quedan ${dias} días (${Math.round(x.stock)}kg en IMPROLUX, consumo ${Math.round(x.kg_dia)}kg/día)` });
      }
    });
  } catch (e) {}
  
  res.json(alertas);
});

// ══════════════════════════════════════════════════════════════════════════════
// ── MÓDULO: EVENTOS DE CAMPO ─────────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════════

app.get("/api/eventos", (req, res) => {
  const animal_id = req.query.animal_id;
  const limite = parseInt(req.query.limite) || 50;
  if (animal_id) {
    const rows = db.prepare("SELECT e.*, a.rp FROM eventos e JOIN animales a ON a.id = e.animal_id WHERE e.animal_id = ? ORDER BY e.fecha DESC LIMIT ?").all(animal_id, limite);
    res.json(rows);
  } else {
    const rows = db.prepare("SELECT e.*, a.rp FROM eventos e JOIN animales a ON a.id = e.animal_id ORDER BY e.fecha DESC, e.created_at DESC LIMIT ?").all(limite);
    res.json(rows);
  }
});

app.post("/api/eventos", (req, res) => {
  const { rp, animal_id, fecha, tipo, descripcion } = req.body;
  let aid = animal_id;
  if (!aid && rp) { const a = buscarAnimalTodos(rp); if (a) aid = a.id; }
  if (!aid) return res.status(400).json({ error: "Animal no encontrado" });
  db.prepare("INSERT INTO eventos (animal_id, fecha, tipo, descripcion) VALUES (?, ?, ?, ?)")
    .run(aid, fecha || new Date().toISOString().slice(0,10), tipo || 'OBSERVACION', descripcion || '');
  res.json({ mensaje: "✅ Evento registrado" });
});

app.delete("/api/eventos/:id", (req, res) => {
  db.prepare("DELETE FROM eventos WHERE id = ?").run(req.params.id);
  res.json({ mensaje: "✅ Evento eliminado" });
});

// ══════════════════════════════════════════════════════════════════════════════
// ── INFORMES BREEDPLAN — Excel templates ─────────────────────────────────────
// ══════════════════════════════════════════════════════════════════════════════

// Generar código Breedplan: prefijo + año(2d) + RP (con 0 hasta 99)
function codigoBreedplan(prefijo, rp, fecha_nac) {
  if (!rp || !fecha_nac) return '';
  const anio = fecha_nac.substring(2, 4); // "2024" → "24"
  let rpCode = rp;
  // Si RP es numérico y < 100, agregar 0 adelante
  if (/^\d+$/.test(rp) && parseInt(rp) < 100) {
    rpCode = rp.padStart(3, '0');
  }
  return `${prefijo}${anio}${rpCode}`;
}

// POST /api/informe-breedplan/pesos
// body: { rps: ["S219","S402",...], tipo: "200"|"400"|"600"|"ALL", manejo: "1", prefijo: "ADE" }
app.post("/api/informe-breedplan/pesos", async (req, res) => {
  if (!ExcelJS) return res.status(500).json({ error: "ExcelJS no disponible" });
  
  const { rps, tipo, manejo, prefijo } = req.body;
  const herdIdent = prefijo || 'ADE';
  const tipoFiltro = tipo || 'ALL'; // 200, 400, 600, ALL
  
  if (!Array.isArray(rps) || !rps.length) return res.status(400).json({ error: "Enviá al menos un RP" });
  
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('Data Input Form');
  
  // Headers Breedplan
  const headers = [
    'Herd Ident', 'kgs', 'Animal Ident', 'Disposal Code', 'Weigh Date',
    'Weight', 'Mgmt Grp', 'Analysis Indicator', 'Wet/Dry', 'Cow Cond',
    'Castrate Flag', 'Castrate Date', 'Scrotal Size', 'Hip Ht',
    'P8 Fat', 'Rib Fat', 'Eye Muscle', 'IMF% Avg', 'IMF% Num',
    'Accred No', 'Docility Score', 'Docility Grp',
    'Extra Code 1', 'Extra Value 1', 'Extra Code 2', 'Extra Value 2'
  ];
  
  const headerRow = ws.addRow(headers);
  headerRow.font = { bold: true, size: 10 };
  headerRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE0E0E0' } };
  
  let rowCount = 0;
  let noEncontrados = [];
  let sinPesadaRango = [];
  
  for (const rp of rps) {
    // Buscar por RP exacto, priorizando ACTIVO
    let animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?) AND estado = 'ACTIVO'").get(rp);
    if (!animal) animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?)").get(rp);
    if (!animal) { noEncontrados.push(rp); continue; }
    
    const bpCode = codigoBreedplan(herdIdent, animal.rp, animal.fecha_nac);
    if (!bpCode) { noEncontrados.push(rp + '(sin fecha nac)'); continue; }
    
    // Buscar pesadas del animal
    const pesadas = db.prepare("SELECT * FROM pesadas WHERE animal_id = ? ORDER BY fecha ASC").all(animal.id);
    
    // Buscar mediciones CE
    const medicionesCE = db.prepare("SELECT * FROM mediciones WHERE animal_id = ? AND tipo = 'CE' ORDER BY fecha ASC").all(animal.id);
    
    // Buscar mediciones altura
    const medicionesAltura = db.prepare("SELECT * FROM mediciones WHERE animal_id = ? AND tipo = 'ALTURA' ORDER BY fecha ASC").all(animal.id);
    
    // Buscar docilidad
    const medicionesDocilidad = db.prepare("SELECT * FROM mediciones WHERE animal_id = ? AND tipo = 'DOCILIDAD' ORDER BY fecha ASC").all(animal.id);
    
    // Buscar ecografías
    let ecografias = [];
    try { ecografias = db.prepare("SELECT * FROM ecografias WHERE animal_id = ? ORDER BY fecha_medicion ASC").all(animal.id); } catch(e) {}
    
    for (const p of pesadas) {
      if (!animal.fecha_nac || !p.fecha) continue;
      
      const diasVida = Math.floor((new Date(p.fecha) - new Date(animal.fecha_nac)) / (1000*60*60*24));
      
      // Determinar si esta pesada entra en 200/400/600
      let anlyInd = '';
      let incluir = false;
      
      if (diasVida >= 80 && diasVida <= 300) {
        anlyInd = ''; // 200 day = blank
        if (tipoFiltro === '200' || tipoFiltro === 'ALL') incluir = true;
      } else if (diasVida >= 301 && diasVida <= 500) {
        anlyInd = ''; // 400 day = blank
        if (tipoFiltro === '400' || tipoFiltro === 'ALL') incluir = true;
      } else if (diasVida >= 501 && diasVida <= 800) {
        anlyInd = ''; // 600 day = blank
        if (tipoFiltro === '600' || tipoFiltro === 'ALL') incluir = true;
      }
      
      // Mature cow weight
      if (animal.categoria === 'VACA' && diasVida > 870) {
        anlyInd = 'M';
        if (tipoFiltro === 'ALL') incluir = true;
      }
      
      if (!incluir) continue;
      
      // Buscar CE más cercano a la fecha de pesada (±30 días)
      let scrotal = '';
      if (animal.sexo === 'MACHO' && diasVida >= 300 && diasVida <= 700) {
        const ceCercano = medicionesCE.find(m => {
          const diffDias = Math.abs((new Date(m.fecha) - new Date(p.fecha)) / (1000*60*60*24));
          return diffDias <= 30;
        });
        if (ceCercano) scrotal = ceCercano.valor;
      }
      
      // Buscar altura más cercana
      let hipHt = '';
      const alturaCercana = medicionesAltura.find(m => {
        const diffDias = Math.abs((new Date(m.fecha) - new Date(p.fecha)) / (1000*60*60*24));
        return diffDias <= 30;
      });
      if (alturaCercana) hipHt = alturaCercana.valor;
      
      // Buscar docilidad cercana (para 200d)
      let docScore = '', docGrp = '';
      if (diasVida >= 80 && diasVida <= 300) {
        const docCercana = medicionesDocilidad.find(m => {
          const diffDias = Math.abs((new Date(m.fecha) - new Date(p.fecha)) / (1000*60*60*24));
          return diffDias <= 30;
        });
        if (docCercana) docScore = docCercana.valor;
      }
      
      // Buscar ecografía cercana (para 600d: 300-800 días)
      let p8fat = '', ribfat = '', eyemuscle = '', imfavg = '', imfnum = '', accredNo = '';
      if (diasVida >= 300 && diasVida <= 800) {
        const ecoCercana = ecografias.find(e => {
          const diffDias = Math.abs((new Date(e.fecha_medicion) - new Date(p.fecha)) / (1000*60*60*24));
          return diffDias <= 30;
        });
        if (ecoCercana) {
          p8fat = ecoCercana.gd || '';        // grasa dorsal → P8
          ribfat = ecoCercana.gc || '';        // grasa costillar → Rib Fat
          eyemuscle = ecoCercana.aob || '';    // área ojo bife → Eye Muscle
          imfavg = ecoCercana.pct_gi || '';    // % grasa intramuscular → IMF%
          if (imfavg) imfnum = '3';            // mínimo 3 lecturas
        }
      }
      
      // Formato fecha ddmmyyyy
      const fechaParts = p.fecha.split('-');
      const fechaBP = fechaParts[2] + fechaParts[1] + fechaParts[0];
      
      const row = ws.addRow([
        herdIdent,          // Herd Ident
        'K',                // kgs
        bpCode,             // Animal Ident
        '',                 // Disposal Code
        fechaBP,            // Weigh Date
        Math.round(p.peso), // Weight
        manejo || '',       // Mgmt Grp
        anlyInd,            // Analysis Indicator
        '',                 // Wet/Dry
        '',                 // Cow Cond
        '',                 // Castrate Flag
        '',                 // Castrate Date
        scrotal,            // Scrotal Size
        hipHt,              // Hip Ht
        p8fat,              // P8 Fat
        ribfat,             // Rib Fat
        eyemuscle,          // Eye Muscle
        imfavg,             // IMF% Avg
        imfnum,             // IMF% Num
        accredNo,           // Accred No
        docScore,           // Docility Score
        docGrp,             // Docility Grp
        '', '', '', ''      // Extra traits
      ]);
      rowCount++;
    }
    // Si el animal tiene pesadas pero ninguna quedó en rango
    if (pesadas.length > 0 && !pesadas.some(p => {
      if (!animal.fecha_nac || !p.fecha) return false;
      const d = Math.floor((new Date(p.fecha) - new Date(animal.fecha_nac)) / (1000*60*60*24));
      if (tipoFiltro === '200') return d >= 80 && d <= 300;
      if (tipoFiltro === '400') return d >= 301 && d <= 500;
      if (tipoFiltro === '600') return d >= 501 && d <= 800;
      return d >= 80 && d <= 800;
    })) {
      const edades = pesadas.map(p => Math.floor((new Date(p.fecha) - new Date(animal.fecha_nac)) / (1000*60*60*24)));
      sinPesadaRango.push(`${rp}(${edades.join(',')}d)`);
    }
  }
  
  console.log(`[BREEDPLAN] ${rps.length} solicitados, ${rowCount} exportados, no encontrados: [${noEncontrados.join(',')}], sin pesada en rango: [${sinPesadaRango.join(',')}]`);
  
  // Ajustar anchos
  ws.columns.forEach((col, i) => { col.width = i < 3 ? 15 : 12; });
  
  // Generar buffer y enviar
  const buffer = await wb.xlsx.writeBuffer();
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename=breedplan_pesos_${herdIdent}_${tipo||'ALL'}.xlsx`);
  res.send(buffer);
});

// POST /api/informe-breedplan/servicio
// body: { rps: [...], temporada: "2025", prefijo: "ADE" }
app.post("/api/informe-breedplan/servicio", async (req, res) => {
  if (!ExcelJS) return res.status(500).json({ error: "ExcelJS no disponible" });
  
  const { rps, temporada, prefijo } = req.body;
  const herdIdent = prefijo || 'ADE';
  
  if (!Array.isArray(rps) || !rps.length) return res.status(400).json({ error: "Enviá al menos un RP" });
  
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('Data Input Form');
  
  const headers = [
    'Herd Ident', 'Cow Ident',
    'Event 1 Type', 'Event 1 Sire Ident', 'Event 1 Date',
    'Event 2 Type', 'Event 2 Sire Ident', 'Event 2 Date',
    'Event 3 Type', 'Event 3 Sire Ident', 'Event 3 Date',
    'Preg Test Result', 'Preg Test Date'
  ];
  
  const headerRow = ws.addRow(headers);
  headerRow.font = { bold: true, size: 10 };
  headerRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE0E0E0' } };
  
  for (const rp of rps) {
    const animal = buscarAnimalTodos(rp);
    if (!animal) continue;
    
    const bpCode = codigoBreedplan(herdIdent, animal.rp, animal.fecha_nac);
    const servicios = db.prepare("SELECT * FROM servicios WHERE animal_id = ? AND temporada = ? ORDER BY created_at ASC").all(animal.id, temporada || '');
    
    if (!servicios.length) continue;
    
    for (const s of servicios) {
      // Event type: AI = artificial, N = natural
      const eventType = s.tipo_servicio === 'IATF' ? 'AI' : 'N';
      const sireIdent = s.semen_iatf || s.toro_natural || '';
      const eventDate = s.fecha_iatf || s.fecha_ingreso_toro || '';
      const eventDateBP = eventDate ? eventDate.split('-').reverse().join('') : '';
      
      // Preg test
      let pregResult = '';
      if (s.resultado === 'PREÑADA_IATF' || s.resultado === 'PREÑADA_TORO') pregResult = 'P';
      else if (s.resultado === 'VACIA') pregResult = 'E';
      
      ws.addRow([
        herdIdent, bpCode,
        eventType, sireIdent, eventDateBP,
        '', '', '', // Event 2
        '', '', '', // Event 3
        pregResult, ''
      ]);
    }
  }
  
  ws.columns.forEach((col, i) => { col.width = 15; });
  
  const buffer = await wb.xlsx.writeBuffer();
  res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition', `attachment; filename=breedplan_servicio_${herdIdent}_${temporada||''}.xlsx`);
  res.send(buffer);
});

// GET /api/informe-breedplan/resumen — qué datos tiene cada animal y qué le falta
app.get("/api/informe-breedplan/resumen", (req, res) => {
  const animales = db.prepare("SELECT * FROM animales WHERE estado = 'ACTIVO' AND registro IN ('PP','SA') ORDER BY rp").all();
  const hoy = new Date();
  const resultado = [];
  
  for (const a of animales) {
    if (!a.fecha_nac) continue;
    const diasVida = Math.floor((hoy - new Date(a.fecha_nac)) / (1000*60*60*24));
    
    const pesadas = db.prepare("SELECT * FROM pesadas WHERE animal_id = ? ORDER BY fecha ASC").all(a.id);
    const mediciones = db.prepare("SELECT * FROM mediciones WHERE animal_id = ? ORDER BY fecha ASC").all(a.id);
    let ecografias = [];
    try { ecografias = db.prepare("SELECT * FROM ecografias WHERE animal_id = ? ORDER BY fecha_medicion ASC").all(a.id); } catch(e) {}
    
    // Clasificar pesadas por rango
    const p200 = pesadas.filter(p => { const d = Math.floor((new Date(p.fecha) - new Date(a.fecha_nac)) / (1000*60*60*24)); return d >= 80 && d <= 300; });
    const p400 = pesadas.filter(p => { const d = Math.floor((new Date(p.fecha) - new Date(a.fecha_nac)) / (1000*60*60*24)); return d >= 301 && d <= 500; });
    const p600 = pesadas.filter(p => { const d = Math.floor((new Date(p.fecha) - new Date(a.fecha_nac)) / (1000*60*60*24)); return d >= 501 && d <= 800; });
    
    const tieneCE = mediciones.some(m => m.tipo === 'CE');
    const tieneAltura = mediciones.some(m => m.tipo === 'ALTURA');
    const tieneDocilidad = mediciones.some(m => m.tipo === 'DOCILIDAD');
    const tieneEco = ecografias.length > 0;
    const tieneNac = pesadas.some(p => p.contexto === 'NACIMIENTO');
    
    // Alertas de datos faltantes
    const faltantes = [];
    if (!tieneNac) faltantes.push('Peso nacimiento');
    if (diasVida >= 160 && !p200.length) faltantes.push('Peso 200d (destete)');
    if (diasVida >= 160 && !tieneDocilidad) faltantes.push('Docilidad destete');
    if (diasVida >= 350 && !p400.length) faltantes.push('Peso 400d');
    if (diasVida >= 550 && !p600.length) faltantes.push('Peso 600d (18m)');
    if (diasVida >= 350 && a.sexo === 'MACHO' && !tieneCE) faltantes.push('CE (circunferencia escrotal)');
    if (diasVida >= 550 && !tieneEco) faltantes.push('Ecografía 600d');
    if (diasVida >= 350 && !tieneAltura) faltantes.push('Altura cadera');
    
    resultado.push({
      rp: a.rp,
      registro: a.registro,
      sexo: a.sexo,
      categoria: a.categoria,
      edad_dias: diasVida,
      datos: {
        nac: tieneNac,
        p200: p200.length,
        p400: p400.length,
        p600: p600.length,
        docilidad: tieneDocilidad,
        ce: tieneCE,
        altura: tieneAltura,
        eco: tieneEco
      },
      faltantes,
      completo: faltantes.length === 0
    });
  }
  
  const stats = {
    total: resultado.length,
    completos: resultado.filter(r => r.completo).length,
    con_faltantes: resultado.filter(r => !r.completo).length,
    faltantes_resumen: {}
  };
  // Contar cada tipo de faltante
  resultado.forEach(r => r.faltantes.forEach(f => { stats.faltantes_resumen[f] = (stats.faltantes_resumen[f] || 0) + 1; }));
  
  res.json({ stats, animales: resultado });
});

// ── ACTUALIZAR HBA MASIVO ──
app.post("/api/animales/actualizar-hba", (req, res) => {
  const { animales } = req.body; // [{rp, hba}]
  if (!Array.isArray(animales)) return res.status(400).json({ error: "Formato inválido" });
  let ok = 0, noEnc = 0;
  for (const a of animales) {
    const animal = db.prepare("SELECT id FROM animales WHERE LOWER(rp) = LOWER(?) AND estado = 'ACTIVO'").get(String(a.rp));
    if (animal) {
      db.prepare("UPDATE animales SET hbu = ? WHERE id = ?").run(a.hba, animal.id);
      ok++;
    } else {
      noEnc++;
    }
  }
  res.json({ mensaje: `✅ ${ok} HBA actualizados, ${noEnc} no encontrados`, ok, noEnc });
});




// ── STOCK DEL SISTEMA FINANCIERO ─────────────────────────────────────────────
// Las pajuelas, la sanidad y los insumos viven en el stock del financiero de la
// empresa. El ganadero los lee y los descuenta por API: un solo lugar para el
// dato, un solo precio de compra.

async function listarStock(campoKey, rubro) {
  const { url, campo } = await resolverCampo(campoKey);
  const q = new URL(`${url}/api/stock/lista`);
  q.searchParams.set("campo", campo);
  if (rubro) q.searchParams.set("rubro", rubro);
  const r = await fetch(q.toString());
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return await r.json();
}

// Lo que en el stock son pajuelas: el rubro no está estandarizado, así que se
// reconoce por el nombre además de por la categoría.
const ES_SEMEN = /semen|pajuela|dosis|straw/i;

async function pajuelasDisponibles(campoKey) {
  const todo = await listarStock(campoKey);
  return todo
    .filter(p => ES_SEMEN.test(p.nombre) || ES_SEMEN.test(p.categoria || "") || ES_SEMEN.test(p.rubro || ""))
    .map(p => ({ id: p.id, nombre: p.nombre, cantidad: p.cantidad || 0, unidad: p.unidad || "u" }))
    .sort((a, b) => b.cantidad - a.cantidad);
}

async function consumirStock(campoKey, producto, cantidad, detalle, fecha) {
  const { url, campo } = await resolverCampo(campoKey);
  try {
    const r = await fetch(`${url}/api/stock/consumo`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ producto, cantidad, fecha, detalle, campo })
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || d.error) return { ok: false, error: d.error || `HTTP ${r.status}` };
    return { ok: true, ...d };
  } catch (e) {
    return { ok: false, error: `No pude conectar con el stock (${String(e.message).slice(0, 50)})` };
  }
}

// ── REPRODUCCIÓN: protocolo de servicio en etapas ────────────────────────────
// Módulo nuevo y aparte. Convive con `servicios` sin pisarlo.
function reproOk(res) {
  if (reproduccion) return true;
  res.status(503).json({ error: "Módulo de reproducción no disponible" });
  return false;
}

// La base del campo del request. `db` global lo setea el middleware, pero si
// el request no pasó por él quedaría apuntando al campo por defecto.
function dbRepro(req) {
  return getDB(req.query.campo || req.body?.campo || CAMPO_DEFAULT);
}

app.get("/api/repro/toros", (req, res) => {
  if (!reproOk(res)) return;
  res.json(dbRepro(req).prepare("SELECT * FROM repro_toros WHERE activo=1 ORDER BY tipo, nombre").all());
});

app.post("/api/repro/toros", (req, res) => {
  if (!reproOk(res)) return;
  try { res.json({ ok: true, id: reproduccion.guardarToro(dbRepro(req), req.body) }); }
  catch (e) { res.status(400).json({ error: e.message }); }
});


// Los toros de repaso son animales del plantel, no hay que tipearlos.
app.get("/api/repro/toros-plantel", (req, res) => {
  const base = dbRepro(req);
  try {
    // Cualquier macho activo sirve como repaso: filtrar por categoría dejaba
    // afuera a los que están cargados de otra forma. Los toros van primero.
    res.json(base.prepare(`
      SELECT id, rp, raza, hbu, registro, fecha_nac, categoria, destino
      FROM animales
      WHERE upper(COALESCE(sexo,'')) LIKE 'M%'
        AND upper(COALESCE(estado,'ACTIVO')) = 'ACTIVO'
      ORDER BY
        CASE WHEN upper(COALESCE(categoria,'')) LIKE '%TORO%' THEN 0
             WHEN upper(COALESCE(destino,'')) LIKE '%PLANTEL%' THEN 1
             ELSE 2 END,
        rp
    `).all());
  } catch (e) { res.status(500).json({ error: e.message }); }
});


// Vientres disponibles: hembras activas que no estén ya en un protocolo de esa
// temporada. Sin esto se cargaría la misma vaca en dos servicios a la vez.
app.get("/api/repro/vacas-disponibles", (req, res) => {
  const base = dbRepro(req);
  const temporada = req.query.temporada || "";
  const protocoloId = req.query.protocolo || 0;
  try {
    res.json(base.prepare(`
      SELECT a.id, a.rp, a.categoria, a.fecha_nac, a.raza,
             (SELECT COUNT(*) FROM repro_vacas rv2 WHERE rv2.animal_id = a.id) AS servicios
      FROM animales a
      WHERE upper(COALESCE(a.sexo,'')) LIKE 'H%'
        AND upper(COALESCE(a.estado,'ACTIVO')) = 'ACTIVO'
        AND a.id NOT IN (
          SELECT rv.animal_id FROM repro_vacas rv
          JOIN repro_protocolos rp ON rp.id = rv.protocolo_id
          WHERE (rp.temporada = ? OR rp.id = ?) AND rp.cerrado = 0
        )
      ORDER BY a.categoria, a.rp
    `).all(temporada, protocoloId));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/repro/protocolos", (req, res) => {
  if (!reproOk(res)) return;
  const lista = dbRepro(req).prepare("SELECT id FROM repro_protocolos ORDER BY temporada DESC, id DESC").all();
  res.json(lista.map(p => reproduccion.estado(dbRepro(req), p.id)));
});

app.get("/api/repro/protocolos/:id", (req, res) => {
  if (!reproOk(res)) return;
  const e = reproduccion.estado(dbRepro(req), req.params.id);
  if (!e) return res.status(404).json({ error: "No existe" });
  e.vacas = dbRepro(req).prepare(`
    SELECT rv.*, t.nombre AS repaso_nombre
    FROM repro_vacas rv LEFT JOIN repro_toros t ON t.id = rv.toro_repaso_id
    WHERE rv.protocolo_id=? ORDER BY rv.rp`).all(req.params.id);
  e.reparto = reproduccion.repartoRepaso(dbRepro(req), req.params.id);
  res.json(e);
});

app.post("/api/repro/protocolos", (req, res) => {
  if (!reproOk(res)) return;
  try { res.json({ ok: true, id: reproduccion.crearProtocolo(dbRepro(req), req.body) }); }
  catch (e) { res.status(400).json({ error: e.message }); }
});


app.delete("/api/repro/protocolos/:id", (req, res) => {
  if (!reproOk(res)) return;
  const r = reproduccion.borrarProtocolo(dbRepro(req), req.params.id, req.query.forzar === "1");
  res.status(r.ok ? 200 : (r.requiere_confirmacion ? 409 : 400)).json(r);
});

app.delete("/api/repro/protocolos/:id/vacas/:rp", (req, res) => {
  if (!reproOk(res)) return;
  const r = reproduccion.quitarVaca(dbRepro(req), req.params.id, req.params.rp);
  res.status(r.ok ? 200 : 400).json(r);
});

app.post("/api/repro/protocolos/:id/vacas", (req, res) => {
  if (!reproOk(res)) return;
  const rps = req.body.rps || [];
  const animales = rps.map(rp => dbRepro(req).prepare("SELECT id, rp FROM animales WHERE upper(rp)=upper(?)").get(rp)).filter(Boolean);
  res.json({ ok: true, agregadas: reproduccion.agregarVacas(dbRepro(req), req.params.id, animales), pedidas: rps.length });
});


// El repaso se asigna por vaca: sin eso no se puede confirmar la paternidad
// cuando el lote se reparte entre varios toros.
app.post("/api/repro/protocolos/:id/repaso", (req, res) => {
  if (!reproOk(res)) return;
  const r = reproduccion.asignarRepaso(dbRepro(req), req.params.id, req.body.rps || [], req.body.toro_id || null);
  res.status(r.ok ? 200 : 400).json(r);
});

app.get("/api/repro/protocolos/:id/reparto", (req, res) => {
  if (!reproOk(res)) return;
  res.json(reproduccion.repartoRepaso(dbRepro(req), req.params.id));
});

app.post("/api/repro/protocolos/:id/ejecucion", (req, res) => {
  if (!reproOk(res)) return;
  res.json(reproduccion.registrarEjecucion(dbRepro(req), req.params.id, req.body));
});

app.post("/api/repro/protocolos/:id/eco-precoz", (req, res) => {
  if (!reproOk(res)) return;
  res.json(reproduccion.registrarEcoPrecoz(dbRepro(req), req.params.id, req.body.resultados || [], req.body.fecha));
});

app.post("/api/repro/protocolos/:id/eco-final", (req, res) => {
  if (!reproOk(res)) return;
  res.json(reproduccion.registrarEcoFinal(dbRepro(req), req.params.id, req.body.resultados || [], req.body.fecha));
});


app.get("/api/repro/pajuelas", async (req, res) => {
  const campoKey = req.query.campo || CAMPO_DEFAULT;
  try { res.json(await pajuelasDisponibles(campoKey)); }
  catch (e) { res.status(502).json({ error: "No pude leer el stock: " + e.message }); }
});

// Ejecutar la IATF descuenta las pajuelas y carga el costo en cada vaca.
app.post("/api/repro/protocolos/:id/iatf", async (req, res) => {
  if (!reproOk(res)) return;
  const campoKey = req.body.campo || req.query.campo || CAMPO_DEFAULT;
  const r = await reproduccion.ejecutarIATF(dbRepro(req), req.params.id, req.body,
    (producto, cant, detalle, fecha) => consumirStock(campoKey, producto, cant, detalle, fecha));
  res.status(r.ok ? 200 : 400).json(r);
});


// Insumos y trabajo de cada etapa
app.get("/api/repro/protocolos/:id/items", (req, res) => {
  if (!reproOk(res)) return;
  const etapas = Object.keys(reproduccion.ETAPAS_COSTO);
  const out = {};
  etapas.forEach(e => { out[e] = reproduccion.itemsDeEtapa(dbRepro(req), req.params.id, e); });
  res.json({ items: out, etapas: reproduccion.ETAPAS_COSTO, costos: reproduccion.costosProtocolo(dbRepro(req), req.params.id) });
});

app.post("/api/repro/protocolos/:id/items", (req, res) => {
  if (!reproOk(res)) return;
  try { res.json({ ok: true, id: reproduccion.guardarItem(dbRepro(req), req.params.id, req.body) }); }
  catch (e) { res.status(400).json({ error: e.message }); }
});

app.delete("/api/repro/items/:id", (req, res) => {
  if (!reproOk(res)) return;
  res.json(reproduccion.borrarItem(dbRepro(req), req.params.id));
});

// Ejecutar una etapa: descuenta el stock e imputa el costo a cada vaca.
app.post("/api/repro/protocolos/:id/ejecutar/:etapa", async (req, res) => {
  if (!reproOk(res)) return;
  const campoKey = req.body.campo || req.query.campo || CAMPO_DEFAULT;
  const r = await reproduccion.ejecutarEtapa(dbRepro(req), req.params.id, req.params.etapa.toUpperCase(), req.body,
    (producto, cant, detalle, fecha) => consumirStock(campoKey, producto, cant, detalle, fecha));
  res.status(r.ok ? 200 : 400).json(r);
});

// Todo el stock, para los desplegables (no sólo pajuelas)
app.get("/api/repro/stock", async (req, res) => {
  try { res.json(await listarStock(req.query.campo || CAMPO_DEFAULT, req.query.rubro)); }
  catch (e) { res.status(502).json({ error: "No pude leer el stock: " + e.message }); }
});

app.get("/api/repro/partos", (req, res) => {
  if (!reproOk(res)) return;
  res.json(dbRepro(req).prepare(`SELECT p.*, t.nombre AS padre_nombre FROM repro_partos p
                       LEFT JOIN repro_toros t ON t.id = p.padre_id
                       ORDER BY p.fecha DESC LIMIT 300`).all());
});

app.post("/api/repro/partos", (req, res) => {
  if (!reproOk(res)) return;
  const campoK = req.campoKey || CAMPO_DEFAULT;
  const r = reproduccion.registrarParto(dbRepro(req), {
    ...req.body, verificarRpUnico: rp => rpEnOtroCampo(campoK, rp) });
  res.status(r.ok ? 200 : 400).json(r);
});

app.post("/api/repro/partos/:id/padre", (req, res) => {
  if (!reproOk(res)) return;
  res.json(reproduccion.corregirPadre(dbRepro(req), req.params.id, req.body.toro_id, req.body.quien, req.body.motivo));
});

// ── CONSISTENCIA DE DATOS ────────────────────────────────────────────────────
app.get("/api/inconsistencias", (req, res) => {
  if (!consistencia) return res.status(503).json({ error: "Módulo no disponible" });
  try {
    const h = consistencia.revisar(db);
    res.json({ total: h.length, hallazgos: h, titulos: consistencia.TITULOS });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post("/api/inconsistencias/corregir", (req, res) => {
  if (!consistencia) return res.status(503).json({ error: "Módulo no disponible" });
  try { res.json(consistencia.corregir(db, req.body.correccion)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// ── FIX RP REFERENCIAS (madre_rp, padre_rp, ternero_rp) ──
app.post("/api/fix-rp-ref", (req, res) => {
  const { rpViejo, rpNuevo } = req.body;
  if (!rpViejo || !rpNuevo) return res.status(400).json({ error: "Faltan datos" });
  
  let updates = 0;
  // Actualizar madre_rp
  const r1 = db.prepare("UPDATE animales SET madre_rp = ? WHERE madre_rp = ?").run(rpNuevo, rpViejo);
  updates += r1.changes;
  // Actualizar padre_rp
  const r2 = db.prepare("UPDATE animales SET padre_rp = ? WHERE padre_rp = ?").run(rpNuevo, rpViejo);
  updates += r2.changes;
  // Actualizar ternero_rp en servicios
  const r3 = db.prepare("UPDATE servicios SET ternero_rp = ? WHERE ternero_rp = ?").run(rpNuevo, rpViejo);
  updates += r3.changes;
  // Actualizar ternero2_rp en servicios
  const r4 = db.prepare("UPDATE servicios SET ternero2_rp = ? WHERE ternero2_rp = ?").run(rpNuevo, rpViejo);
  updates += r4.changes;
  // Actualizar mellizo_de
  const r5 = db.prepare("UPDATE animales SET mellizo_de = ? WHERE mellizo_de = ?").run(rpNuevo, rpViejo);
  updates += r5.changes;
  
  res.json({ ok: true, updates });
});

// ── HEALTH ────────────────────────────────────────────────────────────────────
// Test API Anthropic
app.get("/api/test-haiku", async (req, res) => {
  try {
    const result = await anthropic.messages.create({
      model: "claude-sonnet-4-6",
      max_tokens: 50,
      messages: [{ role: "user", content: "Decí solo OK" }],
    });
    res.json({ ok: true, respuesta: result.content[0].text });
  } catch(e) {
    res.json({ ok: false, error: e.message, type: e.constructor.name });
  }
});

// Stats anuales (para gráficos del dashboard)
app.get("/api/stats/anuales", (req, res) => {
  const campo = req.query.campo || CAMPO_DEFAULT;
  const d = getDB(campo);
  try {
    // Preñez por temporada (% del último servicio de cada vaca)
    const prenez = d.prepare(`
      SELECT temporada,
        COUNT(*) as total,
        SUM(CASE WHEN resultado IN ('PREÑADA_IATF','PREÑADA_TORO','PREÑADA') THEN 1 ELSE 0 END) as prenadas
      FROM servicios WHERE temporada IS NOT NULL AND resultado IS NOT NULL AND resultado != ''
      GROUP BY temporada ORDER BY temporada
    `).all().map(r => ({
      temporada: r.temporada,
      pct: r.total ? Math.round((r.prenadas / r.total) * 100) : 0
    }));

    // Peso promedio al destete por temporada de nacimiento (año de la pesada)
    const destete = d.prepare(`
      SELECT substr(fecha,1,4) as temporada, AVG(peso) as promedio, COUNT(*) as n
      FROM pesadas WHERE contexto = 'DESTETE' AND fecha IS NOT NULL AND peso > 0
      GROUP BY substr(fecha,1,4) ORDER BY temporada
    `).all().map(r => ({ temporada: r.temporada, promedio: r.promedio }));

    // GDP promedio por año (solo recrías, rango válido)
    const gdp = d.prepare(`
      SELECT substr(p.fecha,1,4) as anio, AVG(p.gdp) as promedio, COUNT(*) as n
      FROM pesadas p JOIN animales a ON a.id = p.animal_id
      WHERE p.gdp > 0 AND p.gdp <= 2.5 AND p.fecha IS NOT NULL
      GROUP BY substr(p.fecha,1,4) ORDER BY anio
    `).all().map(r => ({ anio: r.anio, promedio: r.promedio }));

    res.json({ prenez, destete, gdp });
  } catch(e) {
    console.error('stats/anuales:', e.message);
    res.json({ prenez: [], destete: [], gdp: [] });
  }
});

// Envios matriz (tracking INIA/Breedplan)
app.get("/api/envios/matriz", (req, res) => {
  const campo = req.query.campo || CAMPO_DEFAULT;
  const d = getDB(campo);
  try {
    // Check if table exists
    const exists = d.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='envios_evaluacion'").get();
    if (!exists) return res.json({ envios: [] });
    const envios = d.prepare("SELECT * FROM envios_evaluacion ORDER BY fecha DESC").all();
    res.json({ envios });
  } catch(e) { res.json({ envios: [] }); }
});

// Limpiar sesión de chat (si se corrompe)
app.post("/api/reset-sesion", (req, res) => {
  const campo = req.query.campo || CAMPO_DEFAULT;
  const d = getDB(campo);
  d.prepare("DELETE FROM sesiones").run();
  res.json({ mensaje: "✅ Sesiones limpiadas" });
});

// ── INTEGRACIÓN CON IMPROLUX ──────────────────────────────────────────────────
// Expone rodeo por categoría (para valuación por cabeza) e insumos.
// IMPROLUX lee este endpoint para valuar el stock ganadero y traer insumos.
app.get("/api/integracion/improlux", (req, res) => {
  // Token opcional de seguridad: si INTEGRACION_TOKEN está seteado, exigirlo
  const tokenEsperado = process.env.INTEGRACION_TOKEN;
  if (tokenEsperado) {
    const tokenRecibido = req.headers['x-integracion-token'] || req.query.token;
    if (tokenRecibido !== tokenEsperado) {
      return res.status(401).json({ error: "Token de integración inválido" });
    }
  }

  // El financiero pregunta por SU nombre de campo (?empresa=CAMPO VIDELA), no
  // por la clave del ganadero. Sin esto todos caían en el campo por defecto y
  // los tres sistemas veían el rodeo de Angus del Este.
  let campos;
  const pedido = req.query.campo || req.query.empresa || "";
  if (CAMPOS[pedido]) {
    campos = [pedido];                       // clave exacta del ganadero
  } else if (pedido) {
    // Nombre del financiero: pueden ser varios campos (Campo Videla → 3).
    const p = pedido.trim().toUpperCase();
    campos = Object.keys(CAMPOS).filter(k =>
      String(CAMPOS[k].campo_improlux || "").toUpperCase() === p);
    if (!campos.length) campos = [CAMPO_DEFAULT];
  } else {
    campos = [CAMPO_DEFAULT];
  }

  const campo = campos[0];
  const d = getDB(campo);
  const campoInfo = CAMPOS[campo] || CAMPOS[CAMPO_DEFAULT];

  try {
    // ── RODEO POR CATEGORÍA (para valuación por cabeza) ──
    const porCategoria = d.prepare(`
      SELECT categoria,
        COUNT(*) as cantidad,
        SUM(CASE WHEN sexo='MACHO' THEN 1 ELSE 0 END) as machos,
        SUM(CASE WHEN sexo='HEMBRA' THEN 1 ELSE 0 END) as hembras,
        SUM(CASE WHEN destino='VENTA' THEN 1 ELSE 0 END) as en_venta
      FROM animales WHERE estado='ACTIVO'
      GROUP BY categoria ORDER BY categoria
    `).all();
    const totalCabezas = porCategoria.reduce((s, c) => s + c.cantidad, 0);


    // ── GENÉTICA (pajuelas/embriones) ──
    let genetica = [];
    try {
      genetica = d.prepare(`
        SELECT tipo, toro_nombre, donante_nombre, cantidad, costo_unitario,
          ROUND(cantidad * COALESCE(costo_unitario,0), 2) as valor_total
        FROM stock_genetica WHERE cantidad > 0 ORDER BY tipo
      `).all();
    } catch(e) {}


    // Totales de valuación de insumos (lo que sí tiene costo cargado)
    const insumos = [], alimentos = [];
    const valorInsumos = 0;
    const valorGenetica = genetica.reduce((s,g) => s + (g.valor_total||0), 0);
    const valorAlimentos = 0;

    // Si el financiero abarca varios campos, se suman todos.
    let rodeoTotal = totalCabezas;
    let categorias = porCategoria;
    const detalle = [{ campo, nombre: campoInfo.nombre, cabezas: totalCabezas }];

    if (campos.length > 1) {
      const acum = {};
      porCategoria.forEach(c => { acum[c.categoria || "SIN CATEGORÍA"] = { ...c }; });

      for (const k of campos.slice(1)) {
        const otra = getDB(k);
        const cats = otra.prepare(`
          SELECT categoria, COUNT(*) as cantidad,
            SUM(CASE WHEN upper(COALESCE(sexo,'')) LIKE 'M%' THEN 1 ELSE 0 END) as machos,
            SUM(CASE WHEN upper(COALESCE(sexo,'')) LIKE 'H%' THEN 1 ELSE 0 END) as hembras
          FROM animales WHERE estado='ACTIVO' GROUP BY categoria`).all();
        const n = otra.prepare("SELECT COUNT(*) n FROM animales WHERE estado='ACTIVO'").get().n;
        rodeoTotal += n;
        detalle.push({ campo: k, nombre: CAMPOS[k].nombre, cabezas: n });
        cats.forEach(c => {
          const key = c.categoria || "SIN CATEGORÍA";
          if (!acum[key]) acum[key] = { ...c, categoria: key };
          else {
            acum[key].cantidad = (acum[key].cantidad || 0) + (c.cantidad || 0);
            acum[key].machos   = (acum[key].machos   || 0) + (c.machos   || 0);
            acum[key].hembras  = (acum[key].hembras  || 0) + (c.hembras  || 0);
          }
        });
      }
      categorias = Object.values(acum);
    }

    res.json({
      campo: campo,
      campo_nombre: campos.length > 1 ? (campoInfo.campo_improlux || campoInfo.nombre) : campoInfo.nombre,
      campos: campos,
      por_campo: detalle,
      fecha: new Date().toISOString().slice(0,10),
      rodeo: {
        total_cabezas: rodeoTotal,
        por_categoria: categorias
      },
      insumos: {
        veterinarios: insumos,
        genetica: genetica,
        alimentos: alimentos,
        valor_total_veterinarios: valorInsumos,
        valor_total_genetica: valorGenetica,
        valor_total_alimentos: valorAlimentos,
        valor_total_insumos: Math.round((valorInsumos + valorGenetica + valorAlimentos) * 100) / 100
      }
    });
  } catch(e) {
    console.error("Error integración IMPROLUX:", e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get("/", (req, res) => {
  const campos = Object.entries(CAMPOS).map(([k, c]) => ({
    key: k,
    nombre: c.nombre,
    pais: c.pais,
    animales: databases[k].prepare("SELECT COUNT(*) as n FROM animales").get().n
  }));
  res.json({ status: "GANADERÍA Bot activo 🐂", version: "5.21-pedigri-separado", costeo: costeo.VERSION, campos });
});

// ── INFORMES INIA / BREEDPLAN ─────────────────────────────────────────────────

// Helper: buscar dato de la madre para informe
function getInfoMadre(madre_rp) {
  if (!madre_rp) return { peso_adulta: null, cc: null, fecha_nac: null };
  const madre = buscarAnimal(madre_rp);
  if (!madre) return { peso_adulta: null, cc: null, fecha_nac: madre_rp ? null : null };
  
  // Peso adulta: última pesada ADULTA o última pesada no-NACIMIENTO/DESTETE
  let peso_adulta = null;
  const pAdulta = db.prepare("SELECT peso FROM pesadas WHERE animal_id = ? AND contexto = 'ADULTA' ORDER BY fecha DESC LIMIT 1").get(madre.id);
  if (pAdulta) peso_adulta = pAdulta.peso;
  if (!peso_adulta) {
    const pFallback = db.prepare("SELECT peso FROM pesadas WHERE animal_id = ? AND contexto NOT IN ('NACIMIENTO','DESTETE') ORDER BY fecha DESC LIMIT 1").get(madre.id);
    if (pFallback) peso_adulta = pFallback.peso;
  }
  
  // CC: última medición CC
  let cc = null;
  const ccRow = db.prepare("SELECT valor FROM mediciones WHERE animal_id = ? AND tipo = 'CC' ORDER BY fecha DESC LIMIT 1").get(madre.id);
  if (ccRow) cc = ccRow.valor;
  
  return { peso_adulta, cc, fecha_nac: madre.fecha_nac, rp: madre.rp };
}

// Helper: buscar peso destete de un ternero
function getPesoDestete(animal_id) {
  const p = db.prepare("SELECT peso FROM pesadas WHERE animal_id = ? AND contexto = 'DESTETE' ORDER BY fecha DESC LIMIT 1").get(animal_id);
  return p ? p.peso : null;
}

// Helper: buscar lote de un animal
function getLoteAnimal(animal_id) {
  const la = db.prepare(`SELECT l.nombre FROM lote_animales la JOIN lotes l ON l.id = la.lote_id WHERE la.animal_id = ?`).get(animal_id);
  return la ? la.nombre : null;
}

/*
  GET /api/informe-inia/destete?generacion=Primavera%20-%202025&manejo=1&fecha_pesada=2026-04-16
  
  Genera el Excel formato INIA "Formulario Pesada Destete" con todos los terneros
  nacidos en la generación indicada, completando los campos desde la DB:
  - Peso destete del ternero
  - Facilidad de parto (si está en notas/sanidad)
  - Manejo (parámetro, default 1 = Campo natural)
  - Lote (del sistema de lotes)
  - RP padre (del servicio de la madre)
  - Kg madre al destete (pesada adulta)
  - CC madre (medición CC)
*/
app.post("/api/informe-inia/destete", async (req, res) => {
  if (!ExcelJS) return res.status(500).json({ error: "exceljs no instalado" });
  
  try {
    const { rps, manejo: manejoParam, fecha_pesada } = req.body;
    if (!Array.isArray(rps) || !rps.length) return res.status(400).json({ error: "Enviá al menos un RP" });
    const manejo = parseInt(manejoParam) || 1;
    const fechaPesada = fecha_pesada || null;
    const cabana = "ANGUS DEL ESTE - 39100";
    const raza = "ABERDEEN ANGUS";
    
    // Buscar cada animal por RP
    const terneros = [];
    const noEncontrados = [];
    for (const rp of rps) {
      const animal = buscarAnimal(rp);
      if (!animal) { noEncontrados.push(rp); continue; }
      const peso_nac_row = db.prepare("SELECT peso FROM pesadas WHERE animal_id = ? AND contexto = 'NACIMIENTO' ORDER BY fecha DESC LIMIT 1").get(animal.id);
      const peso_dest_row = db.prepare("SELECT peso FROM pesadas WHERE animal_id = ? AND contexto = 'DESTETE' ORDER BY fecha DESC LIMIT 1").get(animal.id);
      animal.peso_nac = peso_nac_row ? peso_nac_row.peso : null;
      animal.peso_destete = peso_dest_row ? peso_dest_row.peso : null;
      terneros.push(animal);
    }
    
    if (!terneros.length) return res.status(404).json({ error: `No encontré ningún animal. No encontrados: ${noEncontrados.join(', ')}` });
    
    // Crear workbook
    const wb = new ExcelJS.Workbook();
    wb.creator = 'Angus del Este';
    const ws = wb.addWorksheet('RptAnimalesParaPesar_1');
    
    // ── ENCABEZADO ──
    // Fila 2: título
    ws.mergeCells('B2:D2');
    ws.getCell('B2').value = 'Formulario Pesada Destete';
    ws.getCell('B2').font = { bold: true, size: 14 };
    
    // Fila 4: Cabaña, Raza, Generación
    ws.getCell('C4').value = 'Cabaña';
    ws.getCell('F4').value = cabana;
    ws.getCell('F4').font = { bold: true };
    ws.getCell('O4').value = 'Raza';
    ws.getCell('R4').value = raza;
    ws.getCell('R4').font = { bold: true };
    ws.getCell('V4').value = 'Generación';
    ws.getCell('AA4').value = '';
    ws.getCell('AA4').font = { bold: true };
    
    // Fila 6: Fecha sugerida pesada / Fecha real pesada
    ws.getCell('C6').value = 'Fecha sugerida pesada';
    // Calcular fecha sugerida: promedio de fecha_nac + 200 días
    const fechasNac = terneros.filter(t => t.fecha_nac).map(t => new Date(t.fecha_nac));
    if (fechasNac.length) {
      const promNac = new Date(fechasNac.reduce((s, d) => s + d.getTime(), 0) / fechasNac.length);
      promNac.setDate(promNac.getDate() + 200);
      const meses = ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO','JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE'];
      ws.getCell('I6').value = `${promNac.getDate()} DE ${meses[promNac.getMonth()]} DE ${promNac.getFullYear()}`;
    }
    ws.getCell('T6').value = 'Fecha real de pesada';
    if (fechaPesada) ws.getCell('X6').value = fechaPesada;
    
    // Fila 8: Referencias
    ws.mergeCells('B8:AF8');
    ws.getCell('B8').value = 'Referencias:  Manejo: 1 - Campo natural, 2 - Campo natural + Pradera, 3 - Pradera, 4 - Campo natural + Ración, 5 - Pradera + Ración      Facilidad de parto: 1 - Sin asistencia, 2 - Asistencia menor, 3 - Asistencia mayor, 4 - Cesárea, 5 - Presentación anormal';
    ws.getCell('B8').font = { italic: true, size: 8 };
    
    // Fila 11: Group headers
    ws.getCell('B11').value = 'Identificación del Ternero';
    ws.getCell('B11').font = { bold: true, size: 10 };
    ws.getCell('P11').value = 'Inf. Parto';
    ws.getCell('P11').font = { bold: true };
    ws.getCell('U11').value = 'Inf. Destete';
    ws.getCell('U11').font = { bold: true };
    ws.getCell('AC11').value = 'Inf. Padre';
    ws.getCell('AC11').font = { bold: true };
    ws.getCell('AF11').value = 'Información de la Madre';
    ws.getCell('AF11').font = { bold: true };
    
    // Fila 12: Column headers
    const headers = {
      'B12': 'Reg.', 'D12': 'HBU', 'G12': 'Nacido/a', 'J12': 'TE',
      'K12': 'Sexo', 'L12': 'RP', 'P12': 'Facilidad', 'S12': 'Kg',
      'U12': 'Kg', 'X12': 'Manejo', 'Y12': 'Lote',
      'AC12': 'RP', 'AD12': 'HBU',
      'AF12': 'RP', 'AG12': 'Kg', 'AH12': 'CC', 'AI12': 'HBU', 'AL12': 'Nac'
    };
    for (const [cell, val] of Object.entries(headers)) {
      ws.getCell(cell).value = val;
      ws.getCell(cell).font = { bold: true, size: 9 };
      ws.getCell(cell).border = { bottom: { style: 'thin' } };
    }
    
    // ── DATOS DE TERNEROS ──
    let row = 13;
    for (const t of terneros) {
      const registro = t.registro === 'PP' ? 'PI' : (t.registro === 'SA' ? 'AR' : (t.registro || ''));
      
      // Info de la madre
      const infoMadre = getInfoMadre(t.madre_rp);
      
      // HBU del ternero: campo hbu, o fallback a notas
      let hbu = t.hbu || '';
      if (!hbu && t.notas) { const m = t.notas.match(/HBU:(\w+)/); if (m) hbu = m[1]; }
      
      // HBU de la madre: buscar campo hbu del animal madre
      let madreHbu = '';
      if (t.madre_rp) {
        const madreAnimal = buscarAnimal(t.madre_rp);
        if (madreAnimal) madreHbu = madreAnimal.hbu || madreAnimal.madre_hba || '';
        if (!madreHbu && madreAnimal && madreAnimal.notas) { const m = madreAnimal.notas.match(/HBU:(\w+)/); if (m) madreHbu = m[1]; }
      }
      
      // HBU del padre: buscar campo hbu del animal padre
      let padreHbu = '';
      if (t.padre_rp) {
        const padreAnimal = buscarAnimal(t.padre_rp);
        if (padreAnimal) padreHbu = padreAnimal.hbu || padreAnimal.padre_hba || '';
        if (!padreHbu && padreAnimal && padreAnimal.notas) { const m = padreAnimal.notas.match(/HBU:(\w+)/); if (m) padreHbu = m[1]; }
      }
      
      // TE (transferencia embrionaria) - buscar en notas
      let te = '';
      if (t.notas && /\bTE\b/i.test(t.notas)) te = 'Si';
      
      // Lote
      const lote = getLoteAnimal(t.id) || '';
      
      // Escribir fila
      ws.getCell(`B${row}`).value = registro;
      if (hbu) ws.getCell(`D${row}`).value = hbu;
      if (t.fecha_nac) ws.getCell(`G${row}`).value = new Date(t.fecha_nac);
      if (te) ws.getCell(`J${row}`).value = te;
      ws.getCell(`K${row}`).value = t.sexo === 'MACHO' ? 'M' : 'H';
      ws.getCell(`L${row}`).value = t.rp;
      
      // Inf. Parto — facilidad: por ahora vacío (se puede agregar campo al sistema)
      // ws.getCell(`P${row}`).value = 1;
      
      // Peso nacimiento
      if (t.peso_nac) ws.getCell(`S${row}`).value = t.peso_nac;
      
      // Peso destete
      if (t.peso_destete) ws.getCell(`U${row}`).value = t.peso_destete;
      
      // Manejo (igual para todos)
      ws.getCell(`X${row}`).value = manejo;
      
      // Lote
      if (lote) ws.getCell(`Y${row}`).value = lote;
      
      // Padre
      if (t.padre_rp) ws.getCell(`AC${row}`).value = t.padre_rp;
      if (padreHbu) ws.getCell(`AD${row}`).value = padreHbu;
      
      // Madre
      if (t.madre_rp) ws.getCell(`AF${row}`).value = t.madre_rp;
      if (infoMadre.peso_adulta) ws.getCell(`AG${row}`).value = infoMadre.peso_adulta;
      if (infoMadre.cc) ws.getCell(`AH${row}`).value = infoMadre.cc;
      if (madreHbu) ws.getCell(`AI${row}`).value = madreHbu;
      if (infoMadre.fecha_nac) ws.getCell(`AL${row}`).value = new Date(infoMadre.fecha_nac);
      
      // Formato fecha
      ws.getCell(`G${row}`).numFmt = 'yyyy-mm-dd';
      ws.getCell(`AL${row}`).numFmt = 'yyyy-mm-dd';
      
      row++;
    }
    
    // Anchos de columna
    ws.getColumn('B').width = 5;  // Reg
    ws.getColumn('D').width = 10; // HBU
    ws.getColumn('G').width = 12; // Nacido/a
    ws.getColumn('J').width = 4;  // TE
    ws.getColumn('K').width = 5;  // Sexo
    ws.getColumn('L').width = 8;  // RP
    ws.getColumn('P').width = 10; // Facilidad
    ws.getColumn('S').width = 6;  // Kg nac
    ws.getColumn('U').width = 6;  // Kg dest
    ws.getColumn('X').width = 7;  // Manejo
    ws.getColumn('Y').width = 8;  // Lote
    ws.getColumn('AC').width = 8; // Padre RP
    ws.getColumn('AD').width = 10;// Padre HBU
    ws.getColumn('AF').width = 8; // Madre RP
    ws.getColumn('AG').width = 6; // Madre Kg
    ws.getColumn('AH').width = 4; // Madre CC
    ws.getColumn('AI').width = 10;// Madre HBU
    ws.getColumn('AL').width = 12;// Madre Nac
    
    // Resumen al final
    row += 2;
    ws.getCell(`B${row}`).value = `Total: ${terneros.length} terneros`;
    ws.getCell(`B${row}`).font = { bold: true };
    const conPesoD = terneros.filter(t => t.peso_destete).length;
    const sinPesoD = terneros.filter(t => !t.peso_destete).length;
    ws.getCell(`B${row+1}`).value = `Con peso destete: ${conPesoD} | Sin peso destete: ${sinPesoD}`;
    
    // Enviar
    const filename = `INIA_Destete_ADE_${new Date().toISOString().slice(0,10)}.xlsx`;
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    await wb.xlsx.write(res);
    
  } catch(e) {
    console.error("Error informe INIA destete:", e);
    res.status(500).json({ error: e.message });
  }
});

/*
  GET /api/informe-inia/completar-destete
  
  Recibe el Excel 299 de INIA (pre-llenado) como upload y lo completa con datos del sistema.
  POST con multipart/form-data, campo "archivo" con el .xlsx
  Query params: manejo=1
*/
app.post("/api/informe-inia/completar", express.raw({ type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', limit: '10mb' }), async (req, res) => {
  if (!ExcelJS) return res.status(500).json({ error: "exceljs no instalado" });
  
  try {
    const manejo = parseInt(req.query.manejo) || 1;
    
    const wb = new ExcelJS.Workbook();
    await wb.xlsx.load(req.body);
    const ws = wb.getWorksheet(1);
    if (!ws) return res.status(400).json({ error: "No se encontró hoja de datos" });
    
    // ── AUTO-DETECTAR FORMATO ──
    // Destete: RP en col 12, hoja suele llamarse RptAnimalesParaPesar_1, título tiene "Destete"
    // 18 meses: RP en col 10, hoja suele llamarse Toros/Vaquillonas, título tiene "18 meses"
    const sheetName = (ws.name || '').toLowerCase();
    let formato = 'destete'; // default
    
    // Detectar por nombre de hoja
    if (sheetName.includes('toro') || sheetName.includes('vaquillona')) formato = '18meses';
    
    // Detectar por contenido: buscar "18 meses" o "Circ. Escrotal" en las primeras filas
    for (let r = 1; r <= 14; r++) {
      const row = ws.getRow(r);
      for (let c = 1; c <= 20; c++) {
        const v = String(row.getCell(c).value || '').toLowerCase();
        if (v.includes('18 meses') || v.includes('circ. escrotal') || v.includes('pesada 18')) formato = '18meses';
      }
    }
    
    console.log(`[completar-inia] Formato detectado: ${formato} (hoja: ${ws.name})`);
    
    let completados = 0, noEncontrados = [];
    
    if (formato === '18meses') {
      // ── FORMATO 18 MESES ──
      // RP en col 10, Peso→col 11, CE→col 14, Manejo→col 17, Lote→col 19, Padre→col 26
      ws.eachRow((row, rowNum) => {
        if (rowNum < 15) return;
        const rpVal = row.getCell(10).value;
        if (!rpVal) return;
        const rpStr = String(rpVal).trim();
        if (rpStr === 'RP' || rpStr === 'Registro') return;
        
        const animal = buscarAnimal(rpStr);
        if (!animal) { noEncontrados.push(rpStr); return; }
        
        console.log(`[completar-18m] ${rpStr} → id=${animal.id} nac=${animal.fecha_nac}`);
        
        if (animal.fecha_nac) {
          const p18 = db.prepare(`
            SELECT peso, fecha FROM pesadas WHERE animal_id = ? 
            AND CAST((julianday(fecha) - julianday(?)) AS INTEGER) BETWEEN 515 AND 605
            ORDER BY fecha DESC LIMIT 1
          `).get(animal.id, animal.fecha_nac);
          
          if (p18) {
            console.log(`[completar-18m]   peso=${p18.peso} fecha=${p18.fecha}`);
            row.getCell(11).value = p18.peso;
            const ce = db.prepare("SELECT valor FROM mediciones WHERE animal_id = ? AND tipo = 'CE' AND fecha = ?").get(animal.id, p18.fecha);
            if (ce) { row.getCell(14).value = ce.valor; console.log(`[completar-18m]   ce=${ce.valor}`); }
          } else {
            console.log(`[completar-18m]   NO pesada en rango 515-605`);
          }
        }
        
        row.getCell(17).value = manejo;
        const lote = getLoteAnimal(animal.id);
        if (lote) row.getCell(19).value = lote;
        if (!row.getCell(26).value && animal.padre_rp) row.getCell(26).value = animal.padre_rp;
        completados++;
      });
      
    } else {
      // ── FORMATO DESTETE ──
      // RP en col 12, PesoDest→col 21, Manejo→col 24, Lote→col 25, PadreRP→col 29, MadreRP→col 32, MadreKg→col 33, MadreCC→col 34
      ws.eachRow((row, rowNum) => {
        if (rowNum < 13) return;
        const rp = row.getCell(12).value;
        if (!rp) return;
        const rpStr = String(rp).trim();
        const animal = buscarAnimal(rpStr);
        if (!animal) { noEncontrados.push(rpStr); return; }
        
        console.log(`[completar-dest] ${rpStr} → id=${animal.id}`);
        
        const pesoDestete = getPesoDestete(animal.id);
        if (pesoDestete && !row.getCell(21).value) row.getCell(21).value = pesoDestete;
        if (!row.getCell(24).value) row.getCell(24).value = manejo;
        const lote = getLoteAnimal(animal.id);
        if (lote && !row.getCell(25).value) row.getCell(25).value = lote;
        if (!row.getCell(29).value && animal.padre_rp) row.getCell(29).value = animal.padre_rp;
        
        if (animal.madre_rp) {
          const infoMadre = getInfoMadre(animal.madre_rp);
          if (!row.getCell(32).value) row.getCell(32).value = animal.madre_rp;
          if (infoMadre.peso_adulta && !row.getCell(33).value) row.getCell(33).value = infoMadre.peso_adulta;
          if (infoMadre.cc && !row.getCell(34).value) row.getCell(34).value = infoMadre.cc;
        }
        completados++;
      });
    }
    
    console.log(`[completar-inia] ${completados} completados, ${noEncontrados.length} no encontrados`);
    
    const filename = `299_39100_${formato}_completo.xlsx`;
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    await wb.xlsx.write(res);
    
  } catch(e) {
    console.error("Error completar INIA:", e);
    res.status(500).json({ error: e.message });
  }
});

/*
app.post("/api/informe-inia/18meses", async (req, res) => {
  if (!ExcelJS) return res.status(500).json({ error: "exceljs no instalado" });
  
  try {
    const { rps, manejo: manejoParam } = req.body;
    if (!Array.isArray(rps) || !rps.length) return res.status(400).json({ error: "Enviá al menos un RP" });
    const manejo = parseInt(manejoParam) || 1;
    const cabana = "ANGUS DEL ESTE - 39100";
    
    const animales = [];
    const noEncontrados = [];
    for (const rp of rps) {
      const animal = buscarAnimal(rp);
      if (!animal) { noEncontrados.push(rp); continue; }
      
      // ── BUSCAR DATOS DE 18 MESES (misma fecha, 515-605 días de vida) ──
      animal.peso_18m = null;
      animal.ce = null;
      animal.altura = null;
      animal.fecha_18m = null;
      
      if (animal.fecha_nac) {
        // Buscar pesadas donde la edad esté en rango 515-605 días
        const pesadas18 = db.prepare(`
          SELECT p.* FROM pesadas p WHERE p.animal_id = ? 
          AND CAST((julianday(p.fecha) - julianday(?)) AS INTEGER) BETWEEN 515 AND 605
          ORDER BY p.fecha DESC
        `).all(animal.id, animal.fecha_nac);
        
        console.log(`[18m] ${animal.rp} nac=${animal.fecha_nac} pesadas_en_rango=${pesadas18.length}`);
        if (pesadas18.length) console.log(`[18m]   → peso=${pesadas18[0].peso} fecha=${pesadas18[0].fecha}`);
        
        if (pesadas18.length) {
          // Tomar la más reciente dentro del rango
          animal.peso_18m = pesadas18[0].peso;
          animal.fecha_18m = pesadas18[0].fecha;
          
          // Buscar CE y altura de LA MISMA FECHA
          const ce = db.prepare("SELECT valor FROM mediciones WHERE animal_id = ? AND tipo = 'CE' AND fecha = ?").get(animal.id, pesadas18[0].fecha);
          const alt = db.prepare("SELECT valor FROM mediciones WHERE animal_id = ? AND tipo = 'ALTURA' AND fecha = ?").get(animal.id, pesadas18[0].fecha);
          animal.ce = ce ? ce.valor : null;
          animal.altura = alt ? alt.valor : null;
        } else {
          // No hay pesada en rango — buscar si hay mediciones CE/altura en rango (puede haber medido sin pesar)
          const med18 = db.prepare(`
            SELECT m.fecha, m.tipo, m.valor FROM mediciones m WHERE m.animal_id = ?
            AND CAST((julianday(m.fecha) - julianday(?)) AS INTEGER) BETWEEN 515 AND 605
            ORDER BY m.fecha DESC
          `).all(animal.id, animal.fecha_nac);
          
          for (const m of med18) {
            if (m.tipo === 'CE' && !animal.ce) animal.ce = m.valor;
            if (m.tipo === 'ALTURA' && !animal.altura) animal.altura = m.valor;
            if (!animal.fecha_18m) animal.fecha_18m = m.fecha;
          }
        }
      }
      
      // Peso destete del animal
      const pdest = db.prepare("SELECT peso FROM pesadas WHERE animal_id = ? AND contexto = 'DESTETE' ORDER BY fecha DESC LIMIT 1").get(animal.id);
      animal.peso_destete = pdest ? pdest.peso : null;
      
      // Docilidad (última)
      const doc = db.prepare("SELECT valor FROM mediciones WHERE animal_id = ? AND tipo = 'DOCILIDAD' ORDER BY fecha DESC LIMIT 1").get(animal.id);
      animal.docilidad = doc ? doc.valor : null;
      
      // Lote
      animal.lote = getLoteAnimal(animal.id) || '';
      
      // HBU del animal
      if (!animal.hbu && animal.notas) { const m = animal.notas.match(/HBU:(\w+)/); if (m) animal.hbu = m[1]; }
      
      // Madre info
      animal.madre_hba_val = '';
      animal.madre_fecha_nac = '';
      animal.madre_peso_dest = null;
      if (animal.madre_rp) {
        const madre = buscarAnimal(animal.madre_rp);
        if (madre) {
          animal.madre_hba_val = madre.hbu || madre.madre_hba || '';
          if (!animal.madre_hba_val && madre.notas) { const m = madre.notas.match(/HBU:(\w+)/); if (m) animal.madre_hba_val = m[1]; }
          animal.madre_fecha_nac = madre.fecha_nac || '';
        }
      }
      
      // Padre HBU
      animal.padre_hba_val = '';
      if (animal.padre_rp) {
        const padre = buscarAnimal(animal.padre_rp);
        if (padre) {
          animal.padre_hba_val = padre.hbu || padre.padre_hba || '';
          if (!animal.padre_hba_val && padre.notas) { const m = padre.notas.match(/HBU:(\w+)/); if (m) animal.padre_hba_val = m[1]; }
        }
      }
      
      animales.push(animal);
    }
    
    if (!animales.length) return res.status(404).json({ error: `No encontré ningún animal. No encontrados: ${noEncontrados.join(', ')}` });
    
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet('18Meses');
    
    ws.mergeCells('A1:L1');
    ws.getCell('A1').value = `ANGUS DEL ESTE — Informe 18 Meses`;
    ws.getCell('A1').font = { bold: true, size: 14 };
    ws.getCell('A2').value = `Cabaña: ${cabana} | Raza: ABERDEEN ANGUS | ${animales.length} animales`;
    ws.getCell('A2').font = { size: 10, color: { argb: 'FF666666' } };
    
    // Headers fila 4
    const hdrs = ['Reg.','RP','HBU','Sexo','Fecha Nac','Fecha Med.','Peso 18m','CE','Altura','Docilidad','Manejo','Lote','Padre RP','Padre HBU','Madre RP','Madre HBU','Madre Nac','Peso Destete'];
    hdrs.forEach((h, i) => {
      const cell = ws.getCell(4, i + 1);
      cell.value = h;
      cell.font = { bold: true, size: 10 };
      cell.border = { bottom: { style: 'thin' } };
    });
    
    let row = 5;
    for (const a of animales) {
      const reg = a.registro === 'PP' ? 'PI' : (a.registro === 'SA' ? 'AR' : (a.registro || ''));
      
      ws.getCell(row, 1).value = reg;
      ws.getCell(row, 2).value = a.rp;
      if (a.hbu) ws.getCell(row, 3).value = a.hbu;
      ws.getCell(row, 4).value = a.sexo === 'MACHO' ? 'M' : 'H';
      if (a.fecha_nac) { ws.getCell(row, 5).value = new Date(a.fecha_nac); ws.getCell(row, 5).numFmt = 'yyyy-mm-dd'; }
      if (a.fecha_18m) { ws.getCell(row, 6).value = new Date(a.fecha_18m); ws.getCell(row, 6).numFmt = 'yyyy-mm-dd'; }
      if (a.peso_18m) ws.getCell(row, 7).value = a.peso_18m;
      if (a.ce) ws.getCell(row, 8).value = a.ce;
      if (a.altura) ws.getCell(row, 9).value = a.altura;
      if (a.docilidad) ws.getCell(row, 10).value = a.docilidad;
      ws.getCell(row, 11).value = manejo;
      if (a.lote) ws.getCell(row, 12).value = a.lote;
      if (a.padre_rp) ws.getCell(row, 13).value = a.padre_rp;
      if (a.padre_hba_val) ws.getCell(row, 14).value = a.padre_hba_val;
      if (a.madre_rp) ws.getCell(row, 15).value = a.madre_rp;
      if (a.madre_hba_val) ws.getCell(row, 16).value = a.madre_hba_val;
      if (a.madre_fecha_nac) { ws.getCell(row, 17).value = new Date(a.madre_fecha_nac); ws.getCell(row, 17).numFmt = 'yyyy-mm-dd'; }
      if (a.peso_destete) ws.getCell(row, 18).value = a.peso_destete;
      row++;
    }
    
    // Anchos
    [6,8,10,6,12,12,10,6,8,6,8,10,12,10,12,10,12,10].forEach((w, i) => ws.getColumn(i+1).width = w);
    
    row += 1;
    ws.getCell(row, 1).value = `Total: ${animales.length} animales`;
    ws.getCell(row, 1).font = { bold: true };
    const machos = animales.filter(a => a.sexo === 'MACHO').length;
    const hembras = animales.filter(a => a.sexo === 'HEMBRA').length;
    ws.getCell(row + 1, 1).value = `Machos: ${machos} | Hembras: ${hembras}`;
    
    const filename = `INIA_18Meses_ADE_${new Date().toISOString().slice(0,10)}.xlsx`;
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    await wb.xlsx.write(res);
    
  } catch(e) {
    console.error("Error informe INIA 18 meses:", e);
    res.status(500).json({ error: e.message });
  }
});

/*
  GET /api/informe-inia/servicio?temporada=2025&manejo=1
  
  Genera Excel formato INIA con datos de servicio de la temporada indicada.
*/
app.post("/api/informe-inia/servicio", async (req, res) => {
  if (!ExcelJS) return res.status(500).json({ error: "exceljs no instalado" });
  
  try {
    const { rps } = req.body;
    if (!Array.isArray(rps) || !rps.length) return res.status(400).json({ error: "Enviá al menos un RP" });
    const cabana = "ANGUS DEL ESTE - 39100";
    
    // Buscar servicios de los animales indicados (último servicio de cada uno)
    const servicios = [];
    const noEncontrados = [];
    for (const rp of rps) {
      const animal = buscarAnimal(rp);
      if (!animal) { noEncontrados.push(rp); continue; }
      const servs = db.prepare(`
        SELECT s.*, a.rp, a.categoria, a.registro, a.fecha_nac, a.madre_rp, a.padre_rp, a.notas as animal_notas
        FROM servicios s JOIN animales a ON a.id = s.animal_id
        WHERE s.animal_id = ? ORDER BY s.created_at DESC
      `).all(animal.id);
      if (servs.length) servicios.push(...servs);
      else servicios.push({ rp: animal.rp, categoria: animal.categoria, registro: animal.registro, notas: 'Sin servicio registrado' });
    }
    
    if (!servicios.length) return res.status(404).json({ error: `No encontré ningún animal. No encontrados: ${noEncontrados.join(', ')}` });
    
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet('Servicio');
    
    ws.mergeCells('A1:N1');
    ws.getCell('A1').value = `ANGUS DEL ESTE — Informe Servicio`;
    ws.getCell('A1').font = { bold: true, size: 14 };
    ws.getCell('A2').value = `Cabaña: ${cabana} | Raza: ABERDEEN ANGUS | ${rps.length} animales`;
    ws.getCell('A2').font = { size: 10, color: { argb: 'FF666666' } };
    
    const prenadas = servicios.filter(s => s.resultado && s.resultado.includes('PREÑADA')).length;
    const vacias = servicios.filter(s => s.resultado === 'VACIA').length;
    const pendientes = servicios.filter(s => !s.resultado).length;
    const pctPrenez = (prenadas + vacias) > 0 ? ((prenadas / (prenadas + vacias)) * 100).toFixed(1) : '0';
    ws.getCell('A3').value = `Preñadas: ${prenadas} | Vacías: ${vacias} | Pendientes: ${pendientes} | % Preñez: ${pctPrenez}%`;
    ws.getCell('A3').font = { bold: true, size: 10 };
    
    // Headers fila 5
    const hdrs = ['Reg.','RP','Cat.','Tipo Serv.','Semen IATF','F. IATF','Toro Repaso','F. Ingreso Toro','CC Pre','Resultado','F. Tacto','Cría RP','P. Nac','Notas'];
    hdrs.forEach((h, i) => {
      const cell = ws.getCell(5, i + 1);
      cell.value = h;
      cell.font = { bold: true, size: 10 };
      cell.border = { bottom: { style: 'thin' } };
    });
    
    let row = 6;
    for (const s of servicios) {
      const reg = s.registro === 'PP' ? 'PI' : (s.registro === 'SA' ? 'AR' : (s.registro || ''));
      
      ws.getCell(row, 1).value = reg;
      ws.getCell(row, 2).value = s.rp;
      ws.getCell(row, 3).value = s.categoria;
      ws.getCell(row, 4).value = s.tipo_servicio || '';
      ws.getCell(row, 5).value = s.semen_iatf || '';
      if (s.fecha_iatf) { ws.getCell(row, 6).value = new Date(s.fecha_iatf); ws.getCell(row, 6).numFmt = 'yyyy-mm-dd'; }
      ws.getCell(row, 7).value = s.toro_natural || '';
      if (s.fecha_ingreso_toro) { ws.getCell(row, 8).value = new Date(s.fecha_ingreso_toro); ws.getCell(row, 8).numFmt = 'yyyy-mm-dd'; }
      if (s.cc_pre) ws.getCell(row, 9).value = s.cc_pre;
      ws.getCell(row, 10).value = s.resultado || 'PENDIENTE';
      if (s.tacto_servicio) { ws.getCell(row, 11).value = new Date(s.tacto_servicio); ws.getCell(row, 11).numFmt = 'yyyy-mm-dd'; }
      ws.getCell(row, 12).value = s.ternero_rp || '';
      if (s.peso_nacimiento) ws.getCell(row, 13).value = s.peso_nacimiento;
      ws.getCell(row, 14).value = s.notas || '';
      row++;
    }
    
    // Anchos
    [6,8,10,10,14,12,14,12,6,14,12,8,8,20].forEach((w, i) => ws.getColumn(i+1).width = w);
    
    const filename = `INIA_Servicio_ADE_${temporada}.xlsx`;
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    await wb.xlsx.write(res);
    
  } catch(e) {
    console.error("Error informe INIA servicio:", e);
    res.status(500).json({ error: e.message });
  }
});

// ── MÓDULO COSTEO kgNE ────────────────────────────────────────────────────────
const costeo = require("./costeo");
const motoresCosteo = costeo.init(databases, app, { CAMPO_DEFAULT });

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`GANADERÍA Bot corriendo en puerto ${PORT}`);
  
  // Se ejecuta todos los días a las 8:00 AM Uruguay (UTC-3 = 11:00 UTC)

  // ── CRON: cierre diario del costeo ──
  // Devenga ración y piso, valoriza sanidad, incorpora servicios y pesadas nuevas
  // y descuenta el consumo en IMPROLUX. Todo sin intervención.
  setInterval(async () => {
    const ahora = new Date();
    if (ahora.getUTCHours() !== 12 || ahora.getUTCMinutes() >= 5) return;
    for (const [campoKey] of Object.entries(databases)) {
      try {
        const m = motoresCosteo[campoKey];
        if (!m || !m.precios()) continue;
        const r = m.sincronizar({});
        const partes = [];
        if (r.altas)    partes.push(`${r.altas} altas`);
        if (r.asientos) partes.push(`${r.asientos} movimientos`);
        if (r.dietas && r.dietas.aplicadas)
          partes.push(`ración ${Math.round(r.dietas.kg)} kg / US$ ${r.dietas.costo.toFixed(2)}`);
        if (r.potreros && r.potreros.aplicadas)
          partes.push(`piso ${r.potreros.aplicadas} potreros / US$ ${r.potreros.costo.toFixed(2)}`);
        if (r.sanidad && r.sanidad.cargados) partes.push(`${r.sanidad.cargados} sanidad`);

        // Empujar a IMPROLUX el consumo de ración que quedó en cola.
        // Si IMPROLUX no responde, queda pendiente y reintenta mañana.
        try {
          const env = await m.enviarConsumos(IMPROLUX_URL);
          if (env.enviados) partes.push(`${env.enviados} consumos descontados en IMPROLUX`);
          if (env.fallidos) partes.push(`⚠️ ${env.fallidos} consumos sin enviar`);
        } catch (e) { console.error(`[COSTEO] envío consumos ${campoKey}:`, e.message); }

        if (partes.length) console.log(`[COSTEO] ${campoKey}: ${partes.join(" · ")}`);
      } catch (e) { console.error(`[COSTEO] ${campoKey}:`, e.message); }
    }
  }, 5 * 60 * 1000);
});
