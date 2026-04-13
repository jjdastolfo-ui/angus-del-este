const express = require("express");
const Anthropic = require("@anthropic-ai/sdk");
const Database = require("better-sqlite3");
const path = require("path");
const fs = require("fs");
let PDFDocument;
try { PDFDocument = require("pdfkit"); } catch(e) { console.log("pdfkit no disponible, informes PDF deshabilitados"); }

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

// ── DB ────────────────────────────────────────────────────────────────────────
const DB_PATH = process.env.DB_PATH || "./amakaik.db";
const DB_DIR = path.dirname(DB_PATH);
if (!fs.existsSync(DB_DIR)) fs.mkdirSync(DB_DIR, { recursive: true });
const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");

db.exec(`
  -- Animal base: datos fijos del individuo
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
    created_at TEXT DEFAULT (datetime('now'))
  );
  CREATE INDEX IF NOT EXISTS idx_animales_rp ON animales(rp);
  CREATE INDEX IF NOT EXISTS idx_animales_chip ON animales(chip);
  CREATE INDEX IF NOT EXISTS idx_animales_categoria ON animales(categoria);

  -- Pesadas: cada evento de pesaje
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

  -- Mediciones corporales: CE, altura, frame, docilidad, CC
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

  -- Ecografías: datos CIIE (AOB, GD, GC, %GI)
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

  -- Servicios reproductivos: cada ciclo de una hembra
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

  -- Sanidad: tratamientos, vacunas, eventos sanitarios
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

  -- Toros plantel
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

  -- Lotes: grupos de animales por ubicación física
  CREATE TABLE IF NOT EXISTS lotes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT NOT NULL UNIQUE,
    descripcion TEXT,
    potrero TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  );

  -- Asignación de animales a lotes (un animal puede estar en UN solo lote)
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

  -- Sesiones chat
  CREATE TABLE IF NOT EXISTS sesiones (
    usuario TEXT PRIMARY KEY,
    historial TEXT DEFAULT '[]',
    updated_at TEXT DEFAULT (datetime('now'))
  );
`);

// Migration: add destino column if not exists
try { db.exec("ALTER TABLE animales ADD COLUMN destino TEXT DEFAULT 'PLANTEL'"); } catch(e) { /* already exists */ }

// ── ANTHROPIC ─────────────────────────────────────────────────────────────────
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

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
  const id = String(identificador).trim();
  // 1. Por RP exacto (activos)
  let animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?) AND estado = 'ACTIVO'").get(id);
  // 2. Con prefijo ADE: buscar sin ADE
  if (!animal && id.toUpperCase().startsWith('ADE')) {
    const sinADE = id.substring(3);
    animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?) AND estado = 'ACTIVO'").get(sinADE);
  }
  // 3. Sin prefijo ADE: buscar con ADE
  if (!animal) {
    animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?) AND estado = 'ACTIVO'").get('ADE' + id);
  }
  // 4. Por chip normalizado
  if (!animal) animal = buscarAnimalPorChip(id, true);
  // 5. Por ID numérico
  if (!animal && !isNaN(parseInt(id))) {
    animal = db.prepare("SELECT * FROM animales WHERE id = ? AND estado = 'ACTIVO'").get(parseInt(id));
  }
  return animal;
}

function buscarAnimalTodos(identificador) {
  if (!identificador) return null;
  const id = String(identificador).trim();
  let animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?)").get(id);
  if (!animal && id.toUpperCase().startsWith('ADE')) {
    animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?)").get(id.substring(3));
  }
  if (!animal) animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?)").get('ADE' + id);
  if (!animal) animal = buscarAnimalPorChip(id, false);
  if (!animal && !isNaN(parseInt(id))) {
    animal = db.prepare("SELECT * FROM animales WHERE id = ?").get(parseInt(id));
  }
  return animal;
}

// Determinar contexto de pesada por edad del animal
function determinarContextoPesada(fechaNac, fechaPesada) {
  if (!fechaNac) return 'DESARROLLO';
  const dias = Math.floor((new Date(fechaPesada) - new Date(fechaNac)) / (1000*60*60*24));
  if (dias <= 3) return 'NACIMIENTO';                     // mismo día o ±3 días
  if (dias >= 160 && dias <= 240) return 'DESTETE';       // 200 ± 40
  if (dias >= 355 && dias <= 445) return 'AÑO';           // 400 ± 45
  if (dias >= 555 && dias <= 645) return '18MESES';       // 600 ± 45
  return 'DESARROLLO';
}

// Calcular GDP entre primer y último peso
function calcularGDP(animalId) {
  const pesadas = db.prepare("SELECT * FROM pesadas WHERE animal_id = ? ORDER BY fecha ASC").all(animalId);
  if (pesadas.length < 2) return null;
  const primera = pesadas[0];
  const ultima = pesadas[pesadas.length - 1];
  const dias = Math.floor((new Date(ultima.fecha) - new Date(primera.fecha)) / (1000*60*60*24));
  if (dias <= 0) return null;
  return (ultima.peso - primera.peso) / dias;
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

  return `Sos el asistente ganadero de Angus del Este (Uruguay). Respondés conciso en español rioplatense.
HOY: ${new Date().toISOString().slice(0,10)}

REGLA DE FECHAS: SIEMPRE incluir "fecha" en el JSON. Si dice una fecha usarla. Si dice "peso nacimiento" la fecha es la fecha_nac del animal. Si no dice fecha usar HOY. Formato YYYY-MM-DD.

REGLA DE LOTES: Cuando lista VARIOS animales → UN SOLO JSON con array. NUNCA un JSON por animal.

ACCIONES DE REGISTRO (respondé SOLO JSON sin texto ni markdown):

{"accion":"registrar_animal","rp":"","chip":"","fecha_nac":"","sexo":"MACHO/HEMBRA","pelo":"NEGRO/COLORADO","categoria":"TERNERO/RECRIA/VAQUILLONA/VACA/TORO/NOVILLO","registro":"PP/SA/GENERAL","destino":"PLANTEL/VENTA","madre_rp":"","padre_rp":""}
{"accion":"registrar_pesada","rp":"","peso":0,"fecha":"YYYY-MM-DD","contexto":"NACIMIENTO/DESTETE/DESARROLLO/AÑO/18MESES"}
{"accion":"registrar_medicion","rp":"","tipo":"CE/ALTURA/CC/FRAME/DOCILIDAD","valor":0,"fecha":"YYYY-MM-DD"}
{"accion":"registrar_servicio","rp":"","temporada":"2025","tipo_servicio":"IATF/NATURAL","semen_iatf":"toro IATF","fecha_iatf":"YYYY-MM-DD","toro_natural":"toro repaso","fecha_ingreso_toro":"YYYY-MM-DD","cc_pre":0}
{"accion":"resultado_tacto","rp":"","resultado":"PREÑADA_IATF/PREÑADA_TORO/VACIA","fecha_tacto":"YYYY-MM-DD"}
{"accion":"registrar_parto","madre_rp":"","ternero_rp":"","peso_nac":0,"sexo":"MACHO/HEMBRA","pelo":"","fecha":"YYYY-MM-DD"}
{"accion":"registrar_sanidad","rp":"","tipo":"VACUNA/TRATAMIENTO/DESPARASITACION","producto":"","fecha":"YYYY-MM-DD"}
{"accion":"dar_baja","rp":"","motivo":"VENTA/MUERTE","fecha":"YYYY-MM-DD"}
{"accion":"cambiar_categoria","rp":"","nueva_categoria":""}
{"accion":"sanidad_lote","registros":[{"rp":"S219"},{"rp":"S211"}],"tipo":"VACUNA","producto":"Aftosa","fecha":"YYYY-MM-DD"}
{"accion":"baja_lote","rps":["S219","S211"],"motivo":"VENTA","fecha":"YYYY-MM-DD"}
{"accion":"borrar_pesada","id":0}
{"accion":"editar_pesada","id":0,"peso":0,"fecha":"","contexto":""}
{"accion":"borrar_sanidad","id":0}
{"accion":"borrar_servicio","id":0}
{"accion":"ficha_animal","rp":""}
{"accion":"ver_rodeo"}
{"accion":"ver_pesadas","rp":""}
{"accion":"ver_servicios","rp":""}
{"accion":"ver_ultimos"}

ACCIONES DE CONSULTA/INFORME — para preguntas analíticas usar:
{"accion":"consulta","tipo":"TIPO","temporada":"2025","filtros":{}}

TIPOS DE CONSULTA disponibles:
- servicio_resumen → resumen de servicio: cuántas se sirvieron, con qué toro, IATF vs natural
- servicio_detalle → listado de cada vaca servida con toro, fechas, CC
- servicio_por_toro → distribución: cuántas vacas por cada toro/semen
- tacto_resumen → % preñez total, % IATF, % toro, cantidad vacías
- tacto_detalle → listado con resultado por vaca, FPP de preñadas
- vacias → listado de vacías (para decidir re-servicio o descarte)
- fpp → fechas probables de parto ordenadas
- paricion_resumen → cuántas parieron, peso promedio nacimiento, distribución IATF vs repaso
- paricion_detalle → cada parto con madre, cría, peso, padre asignado
- evaluacion_toros → ranking de toros por: cantidad de crías, peso promedio nacimiento, peso destete
- destete_resumen → peso promedio destete, GDP promedio, distribución por sexo
- destete_ranking → ranking de terneros al destete por peso
- peso_por_padre → peso promedio de crías agrupado por padre
- recria_estado → estado de recría: pesos, CE, frame, quién va a plantel vs venta
- rodeo_composicion → composición detallada del rodeo activo
- lotes_estado → qué hay en cada lote, ubicación, cantidad
- sanidad_cobertura → qué animales tienen vacuna X, cuáles faltan
- sanidad_historial → historial sanitario por animal o período
- reproductivo_ciclo → resumen completo de un ciclo: servicio → tacto → parición → destete

Filtros opcionales en "filtros": temporada, toro, categoria, sexo, registro, lote, fecha_desde, fecha_hasta, contexto_pesada

{"accion":"texto","mensaje":"respuesta"}

REGLAS:
- ADE es prefijo del establecimiento: ADE2 = 2
- Peso nacimiento → fecha = fecha_nac del animal
- TACTO/ECOGRAFÍA REPRODUCTIVA: resultado siempre es PREÑADA_IATF, PREÑADA_TORO o VACIA. NUNCA solo "PREÑADA".
- Si dice "preñada de IATF/inseminación" → PREÑADA_IATF. Si dice "preñada de toro/cabeza/natural" → PREÑADA_TORO. Si solo dice "preñada" → preguntar si es de IATF o TORO.
- PARTO: el sistema calcula automáticamente el padre. ±15 días de fecha_iatf + 282 = padre IATF, sino repaso.
- NUNCA generar servicios inventados. Solo registrar lo que el usuario dice.
- Cuando el usuario hace una PREGUNTA sobre datos (no un registro) → usar accion "consulta" con el tipo apropiado.
- "¿Cuántas se sirvieron?" → consulta tipo servicio_resumen
- "¿% de preñez?" → consulta tipo tacto_resumen
- "¿Cuáles vacías?" → consulta tipo vacias
- "Ranking de toros" → consulta tipo evaluacion_toros
- "¿Cuánto pesaron al destete?" → consulta tipo destete_resumen
- "¿Qué hay en cada lote?" → consulta tipo lotes_estado
- Si no entendés → {"accion":"texto","mensaje":"¿Podés aclarar?"}

DATOS ACTUALES:
Rodeo: ${JSON.stringify(resumen)}
Servicios por temporada: ${JSON.stringify(serviciosStats)}
Lotes: ${JSON.stringify(lotesResumen)}
Pesadas recientes: ${JSON.stringify(ultimasPesadas.map(p => ({ rp: p.rp, peso: p.peso, fecha: p.fecha })))}`;
}

// ── EJECUTAR ACCIÓN ───────────────────────────────────────────────────────────
function ejecutarAccion(accion) {
  const hoy = new Date().toISOString().split("T")[0];

  // REGISTRAR ANIMAL
  if (accion.accion === "registrar_animal") {
    const { rp, chip, fecha_nac, sexo, pelo, categoria, registro, destino, madre_rp, padre_rp, notas } = accion;
    if (!rp || !sexo) return "❌ Faltan datos: necesito al menos RP y sexo.";
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
    let contexto = accion.contexto || determinarContextoPesada(animal.fecha_nac, fecha);

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

  // RESULTADO TACTO — con tipo de preñez y FPP
  if (accion.accion === "resultado_tacto") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    const serv = db.prepare("SELECT * FROM servicios WHERE animal_id = ? ORDER BY created_at DESC LIMIT 1").get(animal.id);
    if (!serv) return `❌ No hay servicio registrado para RP ${accion.rp}.`;
    
    const fechaTacto = accion.fecha_tacto || hoy;
    let resultado = (accion.resultado || "").toUpperCase();
    
    // Normalizar: aceptar PREÑADA sola como legado
    if (resultado === 'PREÑADA') resultado = 'PREÑADA_IATF'; // default, se puede corregir
    
    // Si es VACIA → registrar y listo
    if (resultado === 'VACIA') {
      db.prepare("UPDATE servicios SET resultado = 'VACIA', tacto_servicio = ? WHERE id = ?")
        .run(fechaTacto, serv.id);
      // Corregir si antes estaba como preñada (tacto mal)
      if (serv.resultado && serv.resultado.startsWith('PREÑADA')) {
        // Había un resultado previo de preñez → corregir
        return `⚪ Tacto corregido!\n🏷️ RP ${animal.rp} | ${serv.resultado} → VACIA\n📅 ${fechaTacto}\n⚠️ Se corrigió resultado previo de preñez`;
      }
      return `⚪ Tacto registrado!\n🏷️ RP ${animal.rp} | VACIA\n📅 ${fechaTacto}`;
    }
    
    // PREÑADA_IATF o PREÑADA_TORO
    let padre = null;
    let fpp = null; // fecha probable parto
    
    if (resultado === 'PREÑADA_IATF') {
      padre = serv.semen_iatf;
      if (serv.fecha_iatf) {
        const fechaIatf = new Date(serv.fecha_iatf);
        fechaIatf.setDate(fechaIatf.getDate() + 282);
        fpp = fechaIatf.toISOString().slice(0, 10);
      }
    } else if (resultado === 'PREÑADA_TORO') {
      padre = serv.toro_natural;
      if (serv.fecha_ingreso_toro) {
        // FPP estimada: fecha ingreso toro + 282 días (aproximado, puede variar)
        const fechaToro = new Date(serv.fecha_ingreso_toro);
        fechaToro.setDate(fechaToro.getDate() + 282);
        fpp = fechaToro.toISOString().slice(0, 10);
      }
    }
    
    // Corregir si antes tenía otro resultado (tacto/eco anterior estaba mal)
    let correccion = '';
    if (serv.resultado && serv.resultado !== resultado) {
      correccion = `\n⚠️ Corregido: ${serv.resultado} → ${resultado}`;
    }
    
    db.prepare(`UPDATE servicios SET resultado = ?, tacto_servicio = ?, 
      notas = COALESCE(notas,'') || ? WHERE id = ?`)
      .run(resultado, fechaTacto, 
           `${fpp ? ' | FPP: ' + fpp : ''}${padre ? ' | Padre: ' + padre : ''}`, serv.id);
    
    // Si es preñada → actualizar categoría a VACA si es vaquillona/recría
    if (animal.categoria === 'VAQUILLONA' || (animal.categoria === 'RECRIA' && animal.sexo === 'HEMBRA')) {
      db.prepare("UPDATE animales SET categoria = 'VACA' WHERE id = ?").run(animal.id);
    }
    
    let resp = `🤰 Tacto registrado!\n🏷️ RP ${animal.rp} | ${resultado}`;
    if (padre) resp += `\n🐂 Padre estimado: ${padre}`;
    if (fpp) resp += `\n📅 Fecha probable parto: ${fpp}`;
    resp += `\n📋 Tacto: ${fechaTacto}`;
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

  // SANIDAD
  if (accion.accion === "registrar_sanidad") {
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
        ficha += "\n";
      });
    }

    if (hijos.length) {
      ficha += `\n👶 *Crías (${hijos.length}):*\n`;
      hijos.forEach(h => ficha += `  RP ${h.rp} | ${h.sexo} | ${h.fecha_nac || ""}\n`);
    }

    if (sanidadRec.length) {
      ficha += `\n💉 *Últimos tratamientos:*\n`;
      sanidadRec.forEach(s => ficha += `  ${s.fecha}: ${s.producto || s.tipo}${s.dosis ? ` (${s.dosis})` : ""}\n`);
    }

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
      INSERT OR IGNORE INTO animales (chip, rp, fecha_nac, raza, registro, sexo, pelo, categoria, destino, madre_rp, madre_hba, padre_rp, padre_hba, estado, fecha_salida, motivo_salida, notas, fecha_ingreso)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    for (const a of accion.animales) {
      try {
        stmt.run(a.chip||null, a.rp, a.fecha_nac||null, a.raza||'A. ANGUS', a.registro||null,
                 a.sexo||'HEMBRA', a.pelo||null, a.categoria||'RECRIA', a.destino||'PLANTEL',
                 a.madre_rp||null, a.madre_hba||null, a.padre_rp||null, a.padre_hba||null,
                 a.estado||'ACTIVO', a.fecha_salida||null, a.motivo_salida||null, a.notas||null,
                 a.fecha_ingreso||new Date().toISOString().slice(0,10));
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
      let where = "1=1"; const params = [];
      if (temporada) { where += " AND s.temporada = ?"; params.push(temporada); }
      const total = db.prepare(`SELECT COUNT(*) as n FROM servicios s WHERE ${where} AND resultado IS NOT NULL`).get(...params);
      const porRes = db.prepare(`SELECT resultado, COUNT(*) as n FROM servicios s WHERE ${where} AND resultado IS NOT NULL GROUP BY resultado`).all(...params);
      const totalServ = db.prepare(`SELECT COUNT(*) as n FROM servicios s WHERE ${where}`).get(...params);
      const prenIatf = porRes.find(r => r.resultado === 'PREÑADA_IATF')?.n || 0;
      const prenToro = porRes.find(r => r.resultado === 'PREÑADA_TORO')?.n || 0;
      const vacias = porRes.find(r => r.resultado === 'VACIA')?.n || 0;
      const totalPren = prenIatf + prenToro;
      const totalDiag = total.n || 1;
      
      let resp = `📊 *Diagnóstico de Preñez${temporada ? ` — ${temporada}` : ''}*\n\n`;
      resp += `🐄 Total servidas: ${totalServ.n}\n`;
      resp += `🔍 Diagnosticadas: ${total.n}\n`;
      resp += `🤰 Preñadas: ${totalPren} (${((totalPren/totalDiag)*100).toFixed(1)}%)\n`;
      resp += `  🧬 IATF: ${prenIatf} (${((prenIatf/totalDiag)*100).toFixed(1)}%)\n`;
      resp += `  🐂 Toro: ${prenToro} (${((prenToro/totalDiag)*100).toFixed(1)}%)\n`;
      resp += `⚪ Vacías: ${vacias} (${((vacias/totalDiag)*100).toFixed(1)}%)\n`;
      resp += `⏳ Sin diagnosticar: ${totalServ.n - total.n}`;
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
        let fpp = '';
        if (s.resultado === 'PREÑADA_IATF' && s.fecha_iatf) {
          const d = new Date(s.fecha_iatf); d.setDate(d.getDate()+282);
          fpp = ` → FPP: ${d.toISOString().slice(0,10)}`;
        } else if (s.resultado === 'PREÑADA_TORO' && s.fecha_ingreso_toro) {
          const d = new Date(s.fecha_ingreso_toro); d.setDate(d.getDate()+282);
          fpp = ` → FPP: ~${d.toISOString().slice(0,10)}`;
        }
        const emoji = s.resultado === 'VACIA' ? '⚪' : '🤰';
        resp += `${emoji} ${s.rp} | ${s.resultado} | ${s.semen_iatf||s.toro_natural||'—'}${fpp}\n`;
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
      let where = "resultado IN ('PREÑADA_IATF','PREÑADA_TORO')"; const params = [];
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
        }
        if (fpp) fpps.push({ rp: s.rp, fpp, padre, tipo: s.resultado });
      });
      fpps.sort((a,b) => a.fpp.localeCompare(b.fpp));
      fpps.forEach(f => resp += `📅 ${f.fpp} | ${f.rp} | ${f.tipo === 'PREÑADA_IATF' ? '🧬 IATF' : '🐂 Toro'}: ${f.padre}\n`);
      const hoy = new Date().toISOString().slice(0,10);
      const vencidas = fpps.filter(f => f.fpp < hoy).length;
      if (vencidas) resp += `\n⚠️ ${vencidas} vacas ya pasaron su FPP y no registraron parto.`;
      return resp;
    }
    
    // ── PARICIÓN RESUMEN ──
    if (tipo === "paricion_resumen") {
      let where = "1=1"; const params = [];
      if (temporada) { where += " AND s.temporada = ?"; params.push(temporada); }
      const conParto = db.prepare(`SELECT COUNT(*) as n, AVG(s.peso_nacimiento) as peso_prom FROM servicios s WHERE ${where} AND s.fecha_parto IS NOT NULL`).get(...params);
      const porOrigen = db.prepare(`SELECT resultado, COUNT(*) as n FROM servicios s WHERE ${where} AND s.fecha_parto IS NOT NULL GROUP BY resultado`).all(...params);
      const porSexo = db.prepare(`SELECT sexo_cria, COUNT(*) as n FROM servicios s WHERE ${where} AND sexo_cria IS NOT NULL GROUP BY sexo_cria`).all(...params);
      let resp = `🐄 *Resumen Parición${temporada ? ` ${temporada}` : ''}*\n\n`;
      resp += `🐮 Total partos: ${conParto.n}\n`;
      if (conParto.peso_prom) resp += `⚖️ Peso promedio nacimiento: ${fmt(conParto.peso_prom)} kg\n`;
      porOrigen.forEach(o => resp += `  ${o.resultado === 'PREÑADA_IATF' ? '🧬 IATF' : '🐂 Toro'}: ${o.n}\n`);
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
      const rows = db.prepare(`SELECT s.*, a.rp as madre_rp FROM servicios s JOIN animales a ON a.id = s.animal_id WHERE ${where} ORDER BY s.fecha_parto DESC`).all(...params);
      if (!rows.length) return "📋 No hay partos registrados.";
      let resp = `📋 *Detalle Parición${temporada ? ` ${temporada}` : ''} — ${rows.length} partos*\n\n`;
      rows.forEach(s => {
        const padre = s.resultado === 'PREÑADA_IATF' ? `🧬 ${s.semen_iatf||'?'}` : `🐂 ${s.toro_natural||'?'}`;
        resp += `📅 ${s.fecha_parto} | Madre: ${s.madre_rp} → Cría: ${s.ternero_rp||'?'} ${s.sexo_cria||''} | ${fmt(s.peso_nacimiento||0)}kg | Padre: ${padre}\n`;
      });
      return resp;
    }
    
    // ── EVALUACIÓN DE TOROS ──
    if (tipo === "evaluacion_toros") {
      // Por padre_rp en la tabla animales → peso nacimiento y destete
      const toros = db.prepare(`
        SELECT a.padre_rp as toro, COUNT(*) as crias, 
          AVG(pn.peso) as peso_nac_prom, AVG(pd.peso) as peso_dest_prom
        FROM animales a 
        LEFT JOIN pesadas pn ON pn.animal_id = a.id AND pn.contexto = 'NACIMIENTO'
        LEFT JOIN pesadas pd ON pd.animal_id = a.id AND pd.contexto = 'DESTETE'
        WHERE a.padre_rp IS NOT NULL AND a.padre_rp != ''
        GROUP BY a.padre_rp ORDER BY crias DESC
      `).all();
      if (!toros.length) return "📋 No hay datos de crías con padre asignado.";
      let resp = `🐂 *Evaluación de Toros — Ranking por progenie*\n\n`;
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
      const stats = db.prepare(`
        SELECT a.sexo, COUNT(*) as n, AVG(p.peso) as prom, MIN(p.peso) as min, MAX(p.peso) as max, AVG(p.gdp) as gdp_prom
        FROM pesadas p JOIN animales a ON a.id = p.animal_id 
        WHERE p.contexto = 'DESTETE' GROUP BY a.sexo
      `).all();
      if (!stats.length) return "📋 No hay pesadas de destete registradas.";
      let resp = `⚖️ *Resumen Destete*\n\n`;
      stats.forEach(s => {
        resp += `${s.sexo}: ${s.n} terneros\n`;
        resp += `  Peso: ${fmt(s.prom)}kg (${fmt(s.min)}-${fmt(s.max)})\n`;
        if (s.gdp_prom) resp += `  GDP promedio: ${fmt(s.gdp_prom*1000)} g/día\n`;
      });
      return resp;
    }
    
    // ── DESTETE RANKING ──
    if (tipo === "destete_ranking") {
      const rows = db.prepare(`
        SELECT a.rp, a.sexo, a.pelo, a.padre_rp, p.peso, p.gdp, p.fecha
        FROM pesadas p JOIN animales a ON a.id = p.animal_id 
        WHERE p.contexto = 'DESTETE' ORDER BY p.peso DESC LIMIT 30
      `).all();
      if (!rows.length) return "📋 No hay pesadas de destete.";
      let resp = `🏆 *Ranking Destete — Top ${rows.length}*\n\n`;
      rows.forEach((r, i) => {
        resp += `${i+1}. ${r.rp} | ${r.sexo} ${r.pelo||''} | ${fmt(r.peso)}kg`;
        if (r.gdp) resp += ` | GDP: ${fmt(r.gdp*1000)}g/d`;
        if (r.padre_rp) resp += ` | Padre: ${r.padre_rp}`;
        resp += `\n`;
      });
      return resp;
    }
    
    // ── PESO POR PADRE ──
    if (tipo === "peso_por_padre") {
      const ctx = f.contexto_pesada || 'DESTETE';
      const rows = db.prepare(`
        SELECT a.padre_rp, COUNT(*) as n, AVG(p.peso) as prom, MIN(p.peso) as min, MAX(p.peso) as max
        FROM pesadas p JOIN animales a ON a.id = p.animal_id 
        WHERE p.contexto = ? AND a.padre_rp IS NOT NULL AND a.padre_rp != ''
        GROUP BY a.padre_rp ORDER BY prom DESC
      `).all(ctx);
      if (!rows.length) return `📋 No hay pesadas ${ctx} con padre asignado.`;
      let resp = `⚖️ *Peso promedio por Padre — ${ctx}*\n\n`;
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
      const vacias = porRes.find(r=>r.resultado==='VACIA')?.n||0;
      const totalDiag = prenIatf + prenToro + vacias || 1;
      
      let resp = `📊 *Ciclo Reproductivo Completo — Temporada ${temporada}*\n\n`;
      resp += `1️⃣ SERVICIO\n  Total servidas: ${totalServ.n}\n`;
      if (porToro.length) { resp += `  Toros IATF: ${porToro.map(t=>`${t.toro}(${t.n})`).join(', ')}\n`; }
      resp += `\n2️⃣ DIAGNÓSTICO\n`;
      resp += `  🤰 Preñadas: ${prenIatf+prenToro} (${((prenIatf+prenToro)/totalDiag*100).toFixed(0)}%)\n`;
      resp += `    IATF: ${prenIatf} | Toro: ${prenToro}\n`;
      resp += `  ⚪ Vacías: ${vacias} (${(vacias/totalDiag*100).toFixed(0)}%)\n`;
      resp += `\n3️⃣ PARICIÓN\n`;
      resp += `  Partos registrados: ${conParto.n}\n`;
      if (conParto.peso_prom) resp += `  Peso nac. promedio: ${fmt(conParto.peso_prom)}kg\n`;
      return resp;
    }
    
    return `❌ Tipo de consulta "${tipo}" no reconocido. Tipos disponibles: servicio_resumen, tacto_resumen, vacias, fpp, evaluacion_toros, destete_resumen, rodeo_composicion, lotes_estado, sanidad_cobertura, reproductivo_ciclo`;
  }

  if (accion.accion === "texto") return accion.mensaje;
  return "No entendí eso. Intentá de nuevo.";
}

// ── WEBHOOK INTERNO (bot web) ─────────────────────────────────────────────────
app.post("/webhook-interno", async (req, res) => {
  try {
    const body = (req.body.Body || "").trim();
    const usuario = "amakaik-web";
    if (!body) return res.json({ respuesta: "Escribí algo para comenzar." });

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
    const rpListMatch = body.match(/RP[:\s]+(.+?)$/i);
    const rpList = rpListMatch ? rpListMatch[1].split(/[,\s]+/).map(r => r.trim()).filter(r => r && /^[A-Za-z0-9]+$/.test(r)) : [];

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

      // Sanidad masiva (solo vacuné/apliqué/desparasité — requiere lista de RPs)
      if (/(?:vacun[eéa]|apliqu[eé]|aplicar|desparasit[eé])/i.test(body) && !/servicio|iatf|toro\s*repaso/i.test(body)) {
        const pm = body.match(/(?:vacun[eéa]\w*|apliqu[eé]|aplicar|desparasit[eé]\w*)\s+(?:con\s+)?(.+?)\s+(?:a\s+(?:los\s+)?|para\s+)/i);
        const producto = pm ? pm[1].trim() : (body.match(/(?:con|producto)\s+(.+?)(?:\s+a\s+|\s+para\s+|\s+RP)/i) || [,'Tratamiento'])[1].trim();
        let ok=0, errs=[];
        for (const rp of rpList) { const a=buscarAnimal(rp); if(!a){errs.push(rp);continue;} db.prepare("INSERT INTO sanidad (animal_id,fecha,tipo,producto,notas) VALUES(?,?,'TRATAMIENTO',?,'Lote')").run(a.id,fecha,producto); ok++; }
        let resp = `💉 Sanidad masiva: ${ok} registrados con ${producto} (${fecha})`;
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

    // ── Flujo normal con Haiku ──
    const historial = getHistorial(usuario);
    historial.push({ role: "user", content: body });

    const contexto = buildContexto();
    const result = await anthropic.messages.create({
      model: "claude-haiku-4-5-20251001",
      max_tokens: 1200,
      system: contexto,
      messages: historial,
    });

    const rawRespuesta = result.content[0].text.trim();
    console.log("HAIKU RAW:", rawRespuesta.substring(0, 500));
    historial.push({ role: "assistant", content: rawRespuesta });
    saveHistorial(usuario, historial);

    const limpio = rawRespuesta.replace(/```json\s*/gi, "").replace(/```\s*/g, "").trim();
    let respuesta = "";
    try {
      // Intentar array de acciones [{ },{ }]
      const arrMatch = limpio.match(/\[[\s\S]*\]/);
      if (arrMatch) {
        const arr = JSON.parse(arrMatch[0]);
        if (Array.isArray(arr)) {
          const resultados = [];
          for (const a of arr) { resultados.push(ejecutarAccion(a)); }
          respuesta = resultados.join("\n\n");
        }
      } else {
        // Extraer todos los JSON objects con "accion"
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
        
        // ── VALIDACIÓN PRE-EJECUCIÓN ──
        // Si el usuario pidió servicio, NO ejecutar acciones de sanidad que Haiku generó mal
        const userPidioServicio = /servicio|iatf|toro\s*repaso/i.test(body);
        const userPidioBaja = /baja.*?(venta|muerte)/i.test(body);
        const userPidioSanidad = /(?:vacun|apliqu|aplicar|desparasit)/i.test(body);
        
        let accionesFiltradas = acciones;
        if (userPidioServicio && !userPidioSanidad) {
          // Filtrar: solo permitir acciones de servicio, no sanidad
          accionesFiltradas = acciones.filter(a => a.accion !== 'registrar_sanidad' && a.accion !== 'sanidad_lote');
          if (accionesFiltradas.length === 0) {
            // Haiku generó solo sanidad cuando pedían servicio → forzar fallback
            respuesta = ""; // vaciar para que el fallback tome control
          }
        }
        
        if (accionesFiltradas.length > 1) {
          const resultados = accionesFiltradas.map(a => ejecutarAccion(a));
          respuesta = resultados.join("\n\n");
        } else if (accionesFiltradas.length === 1) {
          respuesta = ejecutarAccion(accionesFiltradas[0]);
        } else if (userPidioServicio && acciones.length > 0) {
          // Haiku generó acciones incorrectas (sanidad en vez de servicio) → forzar fallback
          respuesta = "";
        } else if (acciones.length === 0) {
          respuesta = limpio;
        } else {
          respuesta = limpio;
        }
      }
    } catch(parseErr) {
      console.error("Parse error:", parseErr.message);
      respuesta = limpio;
    }

    // ── FALLBACK: interpretar directamente si Haiku falló ──
    // Detectar si la respuesta es basura (muchos errores, sanidad cuando pidió servicio, etc)
    const pidioServicio = /servicio|iatf|toro\s*repaso/i.test(body);
    const pidioSanidad = /(?:vacun|apliqu|aplicar|desparasit)/i.test(body);
    const pidioBaja = /baja.*?(venta|muerte)/i.test(body);
    const respuestaErronea = (pidioServicio && respuesta.includes("Sanidad")) || (pidioServicio && respuesta.includes("No entendí"));
    const fallbackNeeded = !respuesta || respuesta.includes("No entendí") || respuesta.includes("Intentá de nuevo") || respuesta === limpio || respuestaErronea;
    if (fallbackNeeded) {
      const hoyFB = new Date().toISOString().split("T")[0];
      // Extraer fecha YYYY-MM-DD del mensaje
      const fM = body.match(/(\d{4})-(\d{2})-(\d{2})/g);
      const fechaFB = fM ? fM[0] : hoyFB;
      const bodyLow = body.toLowerCase();

      // ── SERVICIO (detectar primero — prioridad máxima) ──
      if (/servicio|iatf|toro\s*repaso/i.test(body)) {
        const rpM = body.match(/RP\s+([A-Za-z0-9]+)/i);
        if (rpM) {
          const animal = buscarAnimal(rpM[1]);
          if (animal) {
            const tempM = body.match(/temporada\s+(\d{4})/i);
            const iatfNombre = body.match(/IATF\s+([A-Z][A-Za-z0-9]+)/i);
            const fechasAll = body.match(/(\d{4}-\d{2}-\d{2})/g) || [];
            const repasoM = body.match(/repaso\s+([A-Za-z0-9]+)/i);
            const ccM = body.match(/CC\s+(\d+\.?\d*)/i);
            
            const temporada = tempM ? tempM[1] : new Date().getFullYear().toString();
            const fechaIatf = fechasAll[0] || null;
            const fechaToro = fechasAll[1] || null;
            
            db.prepare("INSERT INTO servicios (animal_id,temporada,tipo_servicio,semen_iatf,fecha_iatf,toro_natural,fecha_ingreso_toro,cc_pre,notas) VALUES (?,?,?,?,?,?,?,?,?)")
              .run(animal.id, temporada, iatfNombre?'IATF':'NATURAL', iatfNombre?iatfNombre[1]:null, fechaIatf, repasoM?repasoM[1]:null, fechaToro, ccM?parseFloat(ccM[1]):null, 'Manual');
            
            respuesta = "✅ Servicio registrado!\n🏷️ RP " + animal.rp + " | Temporada " + temporada;
            if (iatfNombre) respuesta += "\n🧬 IATF: " + iatfNombre[1] + (fechaIatf ? " (" + fechaIatf + ")" : "");
            if (repasoM) respuesta += "\n🐂 Repaso: " + repasoM[1] + (fechaToro ? " (desde " + fechaToro + ")" : "");
            if (ccM) respuesta += "\n📊 CC: " + ccM[1];
          } else {
            respuesta = "❌ No encontré animal RP " + rpM[1];
          }
        }
      }
      // ── BAJA MASIVA ──
      else if (/baja.*?(venta|muerte)/i.test(body)) {
        const rpLM = body.match(/RP[:\s]+(.+?)$/i);
        const rps = rpLM ? rpLM[1].split(/[,\s]+/).map(r=>r.trim()).filter(r=>r&&/^[A-Za-z0-9]+$/.test(r)) : [];
        const mot = (body.match(/(venta|muerte)/i)||['','VENTA'])[1].toUpperCase();
        const est = mot==='MUERTE'?'MUERTO':'VENDIDO';
        let ok=0,er=[];
        for (const rp of rps) { const a=buscarAnimal(rp); if(!a){er.push(rp);continue;} db.prepare("UPDATE animales SET estado=?,fecha_salida=?,motivo_salida=? WHERE id=?").run(est,fechaFB,mot,a.id); ok++; }
        respuesta = "📤 Baja: " + ok + " animales (" + mot + ") 📅 " + fechaFB;
        if (er.length) respuesta += "\n⚠️ No encontrados: " + er.join(", ");
      }
      // ── SANIDAD MASIVA (solo si tiene vacun/aplicar/desparasit + RP:) ──
      else if (/(?:vacun|apliqu|aplicar|desparasit)/i.test(body) && /RP[:\s]/i.test(body)) {
        const rpLM = body.match(/RP[:\s]+(.+?)$/i);
        const rps = rpLM ? rpLM[1].split(/[,\s]+/).map(r=>r.trim()).filter(r=>r&&/^[A-Za-z0-9]+$/.test(r)) : [];
        const pM = body.match(/(?:vacun[eé]|apliqu[eé]|aplicar|registrar)\s+(.+?)\s+(?:para|a)\s+/i);
        const prod = pM ? pM[1].trim() : 'Tratamiento';
        let ok=0,er=[];
        for (const rp of rps) { const a=buscarAnimal(rp); if(!a){er.push(rp);continue;} db.prepare("INSERT INTO sanidad (animal_id,fecha,tipo,producto,notas) VALUES (?,?,'TRATAMIENTO',?,'Masivo')").run(a.id,fechaFB,prod); ok++; }
        respuesta = "💉 Sanidad: " + ok + " registrados con " + prod + " (" + fechaFB + ")";
        if (er.length) respuesta += "\n⚠️ No encontrados: " + er.join(", ");
      }
    }

    res.json({ respuesta });
  } catch (err) {
    console.error("Error webhook-interno:", err);
    res.json({ respuesta: "❌ Error. Intentá de nuevo." });
  }
});

// ── API REST ──────────────────────────────────────────────────────────────────
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
  res.json({ ...animal, pesadas, mediciones, ecografias, servicios, sanidad, hijos, hijos_padre, lote });
});

app.put("/api/animales/:id", (req, res) => {
  const id = parseInt(req.params.id);
  const animal = db.prepare("SELECT * FROM animales WHERE id = ?").get(id);
  if (!animal) return res.status(404).json({ error: "No encontrado" });

  const { rp, chip, fecha_nac, sexo, pelo, registro, categoria, destino, madre_rp, padre_rp } = req.body;
  try {
    db.prepare(`
      UPDATE animales SET rp=?, chip=?, fecha_nac=?, sexo=?, pelo=?, registro=?, categoria=?, destino=?, madre_rp=?, padre_rp=? WHERE id=?
    `).run(
      rp || animal.rp, chip || animal.chip, fecha_nac || animal.fecha_nac,
      sexo || animal.sexo, pelo || animal.pelo, registro || animal.registro,
      categoria || animal.categoria, destino || animal.destino,
      madre_rp !== undefined ? madre_rp : animal.madre_rp,
      padre_rp !== undefined ? padre_rp : animal.padre_rp,
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
        INSERT INTO servicios (animal_id, temporada, tacto_pre, cc_pre, tipo_servicio, semen_iatf, fecha_iatf, toro_natural, fecha_ingreso_toro, tacto_servicio, cc_post, resultado, fecha_parto, ternero_rp, peso_nacimiento, peso_destete, sexo_cria, notas)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(animal.id, s.temporada||null, s.tacto_pre||null, s.cc_pre||null, s.tipo_servicio||null, s.semen_iatf||null, s.fecha_iatf||null, s.toro_natural||null, s.fecha_ingreso_toro||null, s.tacto_servicio||null, s.cc_post||null, s.resultado||null, s.fecha_parto||null, s.ternero_rp||null, s.peso_nacimiento||null, s.peso_destete||null, s.sexo_cria||null, s.notas||null);
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

  const resumen = { pesadas:0, animales_nuevos:0, mediciones:0, servicios:0, sanidad:0, updates:0, errores:0, no_encontrados:[] };
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
      
      // 2. Solo buscar por RP si NO tiene chip o si el chip matcheó
      if (!animal && rp && !esAnimalNuevoPorChip) {
        const matches = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?)").all(rp);
        if (matches.length === 1) {
          animal = matches[0];
        } else if (matches.length > 1) {
          resumen.no_encontrados.push(`${rp} (RP duplicado, usar chip)`);
          continue;
        } else {
          animal = buscarAnimalTodos(rp);
        }
      }

      // Si no existe → según modo: crear o solo reportar
      if (!animal && (rp || chip)) {
        if (req.body.auto_crear) {
          const nuevoRp = rp || `G${(chip || Date.now().toString()).slice(-4)}`;
          const pelo = r.color ? (r.color.toLowerCase()==='black'?'NEGRO':r.color.toLowerCase()==='red'?'COLORADO':null) : (r.pelo || null);
          const sexo = r.sexo ? r.sexo.toUpperCase() : 'HEMBRA';
          const fechaNac = r.fecha_nac || null;
          
          // Auto-detectar categoría por edad
          let cat = 'RECRIA';
          if (fechaNac) {
            const edadMeses = Math.floor((new Date() - new Date(fechaNac)) / (1000*60*60*24*30.44));
            if (edadMeses < 7) cat = 'TERNERO';
            else if (edadMeses < 15) cat = 'RECRIA';
            else if (edadMeses < 24 && sexo === 'HEMBRA') cat = 'VAQUILLONA';
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
              INSERT INTO animales (chip, rp, sexo, pelo, categoria, destino, estado, fecha_nac, fecha_ingreso, madre_rp, padre_rp, notas)
              VALUES (?, ?, ?, ?, ?, 'PLANTEL', 'ACTIVO', ?, ?, ?, ?, ?)
            `).run(chip||null, rpFinal, sexo, pelo, cat, fechaNac, fecha, r.madre||null, r.padre||null, `Creado Gallagher: ${nombre_sesion||''}`);
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
          const contexto = determinarContextoPesada(animal.fecha_nac, fecha);
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

    } catch(e) {
      resumen.errores++;
    }
  }

  let msg = `✅ Sesión Gallagher: ${nombre_sesion || ''}\n📊 ${registros.length} lecturas\n`;
  if (resumen.pesadas) msg += `⚖️ ${resumen.pesadas} pesadas\n`;
  if (resumen.mediciones) msg += `📐 ${resumen.mediciones} mediciones (CE/Altura/CC/GDP)\n`;
  if (resumen.animales_nuevos) msg += `🐄 ${resumen.animales_nuevos} animales nuevos\n`;
  if (resumen.servicios) msg += `🔄 ${resumen.servicios} servicios/tactos\n`;
  if (resumen.sanidad) msg += `💉 ${resumen.sanidad} vacunas/sanidad\n`;
  if (resumen.updates) msg += `🔄 ${resumen.updates} datos actualizados\n`;
  if (resumen.errores) msg += `⚠️ ${resumen.errores} errores\n`;
  if (resumen.no_encontrados.length) msg += `❓ No encontrados: ${resumen.no_encontrados.slice(0,10).join(', ')}`;

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

// ── LOTES CRUD ───────────────────────────────────────────────────────────────
app.get("/api/lotes", (req, res) => {
  const lotes = db.prepare("SELECT * FROM lotes ORDER BY nombre").all();
  const result = lotes.map(l => {
    const animales = db.prepare(`
      SELECT a.* FROM animales a JOIN lote_animales la ON la.animal_id = a.id
      WHERE la.lote_id = ? AND a.estado = 'ACTIVO' ORDER BY a.rp
    `).all(l.id);
    return { ...l, animales, cantidad: animales.length };
  });
  res.json(result);
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
app.post("/api/lotes/:id/sanidad", (req, res) => {
  const loteId = parseInt(req.params.id);
  const { tipo, producto, dosis, fecha } = req.body;
  const animales = db.prepare(`
    SELECT a.id, a.rp FROM animales a JOIN lote_animales la ON la.animal_id = a.id
    WHERE la.lote_id = ? AND a.estado = 'ACTIVO'
  `).all(loteId);
  if (!animales.length) return res.json({ mensaje: "📋 No hay animales en este lote." });
  const f = fecha || new Date().toISOString().split("T")[0];
  const stmt = db.prepare("INSERT INTO sanidad (animal_id, fecha, tipo, producto, dosis, notas) VALUES (?,?,?,?,?,?)");
  for (const a of animales) {
    stmt.run(a.id, f, tipo || 'TRATAMIENTO', producto || null, dosis || null, 'Lote');
  }
  res.json({ mensaje: `💉 Sanidad registrada: ${animales.length} animales | ${producto || tipo || 'Tratamiento'} | ${f}` });
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
    doc.moveDown(0.2);
    
    doc.font('Helvetica').fontSize(7);
    for (const a of catAnimales) {
      if (doc.y > 750) { doc.addPage(); doc.moveDown(0.5); }
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
      doc.moveDown(0.2);
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
  
  const doc = new PDFDocument({ size: 'A4', margin: 50, layout: 'landscape' });
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename=Pesadas_ADE_${new Date().toISOString().slice(0,10)}.pdf`);
  doc.pipe(res);
  
  doc.fontSize(18).font('Helvetica-Bold').text('ANGUS DEL ESTE — Informe de Pesadas', { align: 'center' });
  doc.fontSize(9).font('Helvetica').fillColor('#666').text(`Fecha: ${new Date().toISOString().slice(0,10)} | ${pesadas.length} registros`, { align: 'center' });
  doc.moveDown(0.5);
  doc.moveTo(50, doc.y).lineTo(742, doc.y).stroke('#ddd');
  doc.moveDown(0.3);
  
  doc.fontSize(8).font('Helvetica-Bold').fillColor('#000');
  let ty = doc.y;
  doc.text('RP', 50, ty, {width:60}); doc.text('Cat.', 110, ty, {width:60}); doc.text('Sexo', 170, ty, {width:50});
  doc.text('Peso (kg)', 220, ty, {width:60, align:'right'}); doc.text('Fecha', 290, ty, {width:70});
  doc.text('Contexto', 370, ty, {width:70}); doc.text('GDP (g/d)', 450, ty, {width:60, align:'right'});
  doc.moveDown(0.2); doc.moveTo(50, doc.y).lineTo(520, doc.y).stroke('#eee'); doc.moveDown(0.2);
  
  doc.font('Helvetica').fontSize(7);
  for (const p of pesadas) {
    if (doc.y > 520) { doc.addPage(); doc.moveDown(0.5); }
    ty = doc.y;
    doc.text(p.rp||'', 50, ty, {width:60}); doc.text(p.categoria||'', 110, ty, {width:60}); doc.text(p.sexo||'', 170, ty, {width:50});
    doc.text(fmt(p.peso), 220, ty, {width:60, align:'right'}); doc.text(p.fecha||'', 290, ty, {width:70});
    doc.text(p.contexto||'', 370, ty, {width:70}); doc.text(p.gdp ? fmt(p.gdp*1000) : '', 450, ty, {width:60, align:'right'});
    doc.moveDown(0.15);
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
  
  const doc = new PDFDocument({ size: 'A4', margin: 50, layout: 'landscape' });
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename=Servicios_ADE_${temporada||'todos'}_${new Date().toISOString().slice(0,10)}.pdf`);
  doc.pipe(res);
  
  doc.fontSize(18).font('Helvetica-Bold').text(`ANGUS DEL ESTE — Servicios${temporada ? ` Temporada ${temporada}` : ''}`, { align: 'center' });
  doc.fontSize(9).font('Helvetica').fillColor('#666').text(`${servicios.length} registros`, { align: 'center' });
  doc.moveDown(0.5);
  
  // Stats
  const prenadas = servicios.filter(s => s.resultado === 'PREÑADA').length;
  const vacias = servicios.filter(s => s.resultado === 'VACIA').length;
  const pendientes = servicios.filter(s => !s.resultado).length;
  doc.fontSize(10).font('Helvetica-Bold').fillColor('#000');
  doc.text(`Preñadas: ${prenadas} | Vacías: ${vacias} | Pendientes: ${pendientes} | % Preñez: ${servicios.length ? ((prenadas/(prenadas+vacias||1))*100).toFixed(1) : 0}%`);
  doc.moveDown(0.5);
  doc.moveTo(50, doc.y).lineTo(742, doc.y).stroke('#ddd');
  doc.moveDown(0.3);
  
  doc.fontSize(7).font('Helvetica-Bold');
  let ty = doc.y;
  doc.text('RP', 50, ty, {width:45}); doc.text('Temp.', 95, ty, {width:40}); doc.text('Tipo', 135, ty, {width:40});
  doc.text('Semen IATF', 175, ty, {width:70}); doc.text('F.IATF', 245, ty, {width:60}); 
  doc.text('Toro Repaso', 305, ty, {width:65}); doc.text('F.Toro', 370, ty, {width:60});
  doc.text('CC', 430, ty, {width:25}); doc.text('Resultado', 455, ty, {width:55});
  doc.text('Cría', 510, ty, {width:40}); doc.text('P.Nac', 550, ty, {width:35});
  doc.moveDown(0.2); doc.moveTo(50, doc.y).lineTo(590, doc.y).stroke('#eee'); doc.moveDown(0.2);
  
  doc.font('Helvetica').fontSize(6.5);
  for (const s of servicios) {
    if (doc.y > 520) { doc.addPage(); doc.moveDown(0.5); }
    ty = doc.y;
    doc.text(s.rp||'', 50, ty, {width:45}); doc.text(s.temporada||'', 95, ty, {width:40});
    doc.text(s.tipo_servicio||'', 135, ty, {width:40}); doc.text(s.semen_iatf||'', 175, ty, {width:70});
    doc.text(s.fecha_iatf||'', 245, ty, {width:60}); doc.text(s.toro_natural||'', 305, ty, {width:65});
    doc.text(s.fecha_ingreso_toro||'', 370, ty, {width:60}); doc.text(s.cc_pre ? String(s.cc_pre) : '', 430, ty, {width:25});
    doc.text(s.resultado||'—', 455, ty, {width:55}); doc.text(s.ternero_rp||'', 510, ty, {width:40});
    doc.text(s.peso_nacimiento ? String(s.peso_nacimiento) : '', 550, ty, {width:35});
    doc.moveDown(0.15);
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
  
  const doc = new PDFDocument({ size: 'A4', margin: 50 });
  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader('Content-Disposition', `attachment; filename=Sanidad_ADE_${new Date().toISOString().slice(0,10)}.pdf`);
  doc.pipe(res);
  
  doc.fontSize(18).font('Helvetica-Bold').text('ANGUS DEL ESTE — Informe Sanitario', { align: 'center' });
  doc.fontSize(9).font('Helvetica').fillColor('#666').text(`${sanidad.length} registros`, { align: 'center' });
  doc.moveDown(0.5); doc.moveTo(50, doc.y).lineTo(545, doc.y).stroke('#ddd'); doc.moveDown(0.3);
  
  doc.fontSize(8).font('Helvetica-Bold').fillColor('#000');
  let ty = doc.y;
  doc.text('Fecha', 50, ty, {width:65}); doc.text('RP', 115, ty, {width:50}); doc.text('Cat.', 165, ty, {width:60});
  doc.text('Tipo', 225, ty, {width:80}); doc.text('Producto', 305, ty, {width:120}); doc.text('Dosis', 425, ty, {width:70});
  doc.moveDown(0.2); doc.moveTo(50, doc.y).lineTo(500, doc.y).stroke('#eee'); doc.moveDown(0.2);
  
  doc.font('Helvetica').fontSize(7);
  for (const s of sanidad) {
    if (doc.y > 750) { doc.addPage(); doc.moveDown(0.5); }
    ty = doc.y;
    doc.text(s.fecha||'', 50, ty, {width:65}); doc.text(s.rp||'', 115, ty, {width:50}); doc.text(s.categoria||'', 165, ty, {width:60});
    doc.text(s.tipo||'', 225, ty, {width:80}); doc.text(s.producto||'', 305, ty, {width:120}); doc.text(s.dosis||'', 425, ty, {width:70});
    doc.moveDown(0.15);
  }
  doc.end();
});

// ── RECALCULAR CONTEXTOS, GDP Y CATEGORÍAS ───────────────────────────────────
app.post("/api/recalcular", (req, res) => {
  try {
    const pesadas = db.prepare(`
      SELECT p.id, p.animal_id, p.fecha, p.peso, p.contexto, a.fecha_nac 
      FROM pesadas p JOIN animales a ON a.id = p.animal_id
    `).all();
    
    let contextos = 0, gdps = 0, categorias = 0;
    
    for (const p of pesadas) {
      if (p.fecha_nac) {
        const nuevoCtx = determinarContextoPesada(p.fecha_nac, p.fecha);
        if (nuevoCtx !== p.contexto) {
          db.prepare("UPDATE pesadas SET contexto = ? WHERE id = ?").run(nuevoCtx, p.id);
          contextos++;
        }
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

// ── HEALTH ────────────────────────────────────────────────────────────────────
app.get("/", (req, res) => res.json({ status: "ANGUS DEL ESTE Bot activo 🐂", version: "4.0" }));

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`AMAKAIK Bot corriendo en puerto ${PORT}`));
