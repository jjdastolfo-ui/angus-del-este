const express = require("express");
const Anthropic = require("@anthropic-ai/sdk");
const Database = require("better-sqlite3");
const path = require("path");
const fs = require("fs");

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

  return `Sos el asistente ganadero de Angus del Este (Uruguay). Respondés conciso en español rioplatense.
HOY: ${new Date().toISOString().slice(0,10)}

REGLA DE FECHAS: SIEMPRE incluir "fecha" en el JSON. Si dice una fecha usarla. Si dice "peso nacimiento" la fecha es la fecha_nac del animal. Si no dice fecha usar HOY. Formato YYYY-MM-DD.

REGLA DE LOTES: Cuando lista VARIOS animales → UN SOLO JSON con array. NUNCA un JSON por animal.

ACCIONES (respondé SOLO JSON sin texto ni markdown):

{"accion":"registrar_animal","rp":"","chip":"","fecha_nac":"","sexo":"MACHO/HEMBRA","pelo":"NEGRO/COLORADO","categoria":"TERNERO/RECRIA/VAQUILLONA/VACA/TORO/NOVILLO","registro":"PP/SA/GENERAL","destino":"PLANTEL/VENTA","madre_rp":"","padre_rp":""}
{"accion":"registrar_pesada","rp":"","peso":0,"fecha":"YYYY-MM-DD","contexto":"NACIMIENTO/DESTETE/DESARROLLO/AÑO/18MESES"}
{"accion":"registrar_medicion","rp":"","tipo":"CE/ALTURA/CC/FRAME/DOCILIDAD","valor":0,"fecha":"YYYY-MM-DD"}
{"accion":"registrar_servicio","rp":"","temporada":"2025","tipo_servicio":"IATF/NATURAL","semen_iatf":"toro IATF","fecha_iatf":"YYYY-MM-DD","toro_natural":"toro repaso","fecha_ingreso_toro":"YYYY-MM-DD","cc_pre":0}
{"accion":"resultado_tacto","rp":"","resultado":"PREÑADA/VACIA"}
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
{"accion":"texto","mensaje":"respuesta"}

REGLAS:
- ADE es prefijo del establecimiento: ADE2 = 2
- Peso nacimiento → fecha = fecha_nac del animal
- Servicio se carga manual con IATF+toro repaso. Preñez se confirma por tacto
- Parto: padre automático (282±10 días IATF = padre IATF, sino repaso)
- NUNCA generar servicios inventados. Solo registrar lo que el usuario dice.
- Si no entendés → {"accion":"texto","mensaje":"¿Podés aclarar?"}

DATOS:
Rodeo: ${JSON.stringify(resumen)}
Pesadas: ${JSON.stringify(ultimasPesadas.map(p => ({ rp: p.rp, peso: p.peso, fecha: p.fecha })))}`;
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

  // RESULTADO TACTO
  if (accion.accion === "resultado_tacto") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    const serv = db.prepare("SELECT * FROM servicios WHERE animal_id = ? ORDER BY created_at DESC LIMIT 1").get(animal.id);
    if (!serv) return `❌ No hay servicio registrado para RP ${accion.rp}.`;
    db.prepare("UPDATE servicios SET resultado = ?, notas = COALESCE(notas,'') || ? WHERE id = ?")
      .run(accion.resultado, accion.fecha_parto_estimada ? ` | FPP: ${accion.fecha_parto_estimada}` : "", serv.id);
    const emoji = accion.resultado === "PREÑADA" ? "🤰" : "⚪";
    return `${emoji} Tacto registrado!\n🏷️ RP ${animal.rp} | ${accion.resultado}${accion.fecha_parto_estimada ? `\n📅 FPP: ${accion.fecha_parto_estimada}` : ""}`;
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
      // Gestación bovina típica: 282 días ± 10
      if (diasGestacion >= 272 && diasGestacion <= 292) {
        // Parto dentro del rango de IATF → padre = toro de inseminación
        padre_rp = serv.semen_iatf || serv.toro_natural;
        padre_origen = `IATF (${diasGestacion}d gestación)`;
      } else if (serv.toro_natural) {
        // Fuera de rango IATF → padre = toro de repaso
        padre_rp = serv.toro_natural;
        padre_origen = `REPASO (${diasGestacion}d desde IATF)`;
      } else {
        padre_rp = serv.semen_iatf;
        padre_origen = `Estimado (${diasGestacion}d)`;
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

  if (accion.accion === "texto") return accion.mensaje;
  return "No entendí eso. Intentá de nuevo.";
}

// ── WEBHOOK INTERNO (bot web) ─────────────────────────────────────────────────
app.post("/webhook-interno", async (req, res) => {
  try {
    const body = (req.body.Body || "").trim();
    const usuario = "amakaik-web";
    if (!body) return res.json({ respuesta: "Escribí algo para comenzar." });

    // ── INTERCEPT: operaciones masivas directo sin Haiku ──
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

      // Sanidad masiva
      if (/(?:registrar|vacun|apliqu|aplicar|desparasit)/i.test(body)) {
        const pm = body.match(/(?:registrar|vacun[eéa]\w*|apliqu[eé]|aplicar)\s+(.+?)\s+(?:para|a)\s+/i);
        const producto = pm ? pm[1].trim() : 'Tratamiento';
        let ok=0, errs=[];
        for (const rp of rpList) { const a=buscarAnimal(rp); if(!a){errs.push(rp);continue;} db.prepare("INSERT INTO sanidad (animal_id,fecha,tipo,producto,notas) VALUES(?,?,'TRATAMIENTO',?,'Lote')").run(a.id,fecha,producto); ok++; }
        let resp = `💉 Sanidad masiva: ${ok} registrados con ${producto} (${fecha})`;
        if (errs.length) resp += `\n⚠️ No encontrados: ${errs.join(', ')}`;
        return res.json({ respuesta: resp });
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
        
        if (acciones.length > 1) {
          const resultados = acciones.map(a => ejecutarAccion(a));
          respuesta = resultados.join("\n\n");
        } else if (acciones.length === 1) {
          respuesta = ejecutarAccion(acciones[0]);
        } else {
          respuesta = limpio;
        }
      }
    } catch(parseErr) {
      console.error("Parse error:", parseErr.message);
      respuesta = limpio;
    }

    // ── FALLBACK: si Haiku no pudo o todos fallaron, interpretar directamente ──
    const fallbackNeeded = !respuesta 
      || respuesta.includes("No entendí eso") 
      || respuesta.includes("Intentá de nuevo")
      || (respuesta.split("❌").length - 1) >= 2  // múltiples errores = Haiku generó JSON individual que falló
      || respuesta === limpio;
    if (fallbackNeeded) {
      
      // Extraer fecha si la mencionan (formatos: 15-12-25, 15/12/25, 2025-12-15, 15-12-2025)
      let fechaFallback = new Date().toISOString().split("T")[0];
      const fechaMatch = body.match(/(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
      if (fechaMatch) {
        let [_, d, m, y] = fechaMatch;
        if (y.length === 2) y = (parseInt(y) > 50 ? '19' : '20') + y;
        // Detectar si es DD-MM-YYYY o YYYY-MM-DD
        if (parseInt(d) > 31) { // Es YYYY-MM-DD
          fechaFallback = `${d}-${m.padStart(2,'0')}-${y}`;
        } else {
          fechaFallback = `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
        }
      }

      // Extraer lista de RPs: buscar después de "RP:" o "RP " o al final del mensaje
      const rpListMatch = body.match(/RP[:\s]+(.+?)$/i);
      const rpList = rpListMatch ? rpListMatch[1].split(/[,\s]+/).map(r => r.trim()).filter(r => r && r.match(/^[A-Za-z0-9]+$/)) : [];

      // Detectar sanidad masiva
      const esSanidad = /(?:registrar|vacun|apliqu|aplicar|desparasit)/i.test(body);
      if (esSanidad && rpList.length > 0) {
        // Extraer producto: lo que está entre el verbo y "para/a los RP"
        const prodMatch = body.match(/(?:registrar|vacun[eé]|apliqu[eé]|aplicar)\s+(.+?)\s+(?:para|a)\s+/i);
        const producto = prodMatch ? prodMatch[1].trim() : 'Tratamiento';
        let ok = 0, errs = [];
        for (const rp of rpList) {
          const animal = buscarAnimal(rp);
          if (!animal) { errs.push(rp); continue; }
          db.prepare("INSERT INTO sanidad (animal_id, fecha, tipo, producto, notas) VALUES (?, ?, 'TRATAMIENTO', ?, 'Carga masiva')")
            .run(animal.id, fechaFallback, producto);
          ok++;
        }
        respuesta = `💉 Sanidad masiva: ${ok} registrados con ${producto} (${fechaFallback})`;
        if (errs.length) respuesta += `\n⚠️ No encontrados: ${errs.join(', ')}`;
      }

      // Detectar baja masiva
      const esBaja = /baja\s+(?:por\s+)?(venta|muerte)/i.test(body);
      if (esBaja && rpList.length > 0 && !esSanidad) {
        const motivoMatch = body.match(/(venta|muerte)/i);
        const motivo = (motivoMatch ? motivoMatch[1] : 'VENTA').toUpperCase();
        const estado = motivo === 'MUERTE' ? 'MUERTO' : 'VENDIDO';
        let ok = 0, errs = [];
        for (const rp of rpList) {
          const animal = buscarAnimal(rp);
          if (!animal) { errs.push(rp); continue; }
          db.prepare("UPDATE animales SET estado = ?, fecha_salida = ?, motivo_salida = ? WHERE id = ?")
            .run(estado, fechaFallback, motivo, animal.id);
          ok++;
        }
        respuesta = `📤 Baja masiva: ${ok} animales (${motivo}) | 📅 ${fechaFallback}`;
        if (errs.length) respuesta += `\n⚠️ No encontrados: ${errs.join(', ')}`;
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
  res.json({ ...animal, pesadas, mediciones, ecografias, servicios, sanidad, hijos, hijos_padre });
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
    
    // ── ACTUALIZAR CATEGORÍAS POR EDAD ──
    // Solo animales activos que NO sean VACA ni TORO (esos no cambian por edad)
    const animales = db.prepare("SELECT * FROM animales WHERE estado = 'ACTIVO' AND fecha_nac IS NOT NULL").all();
    for (const a of animales) {
      const edadMeses = Math.floor((new Date() - new Date(a.fecha_nac)) / (1000*60*60*24*30.44));
      let nuevaCat = a.categoria;
      
      // VACA y TORO no se tocan — se asignan por reproducción, no por edad
      if (a.categoria === 'VACA' || a.categoria === 'TORO') continue;
      
      if (edadMeses < 7) {
        nuevaCat = 'TERNERO';
      } else if (edadMeses < 13) {
        nuevaCat = 'RECRIA';  // Recría 1 año (7-12 meses)
      } else if (edadMeses < 24) {
        // Recría 2 años (13-24 meses) — sigue como RECRIA, el dashboard filtra por edad
        nuevaCat = 'RECRIA';
      } else if (a.sexo === 'HEMBRA') {
        nuevaCat = 'VAQUILLONA';  // Hembra >24m sin servicio → vaquillona
      } else {
        nuevaCat = 'NOVILLO';  // Macho >24m → novillo (si fuera toro ya estaría asignado)
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
    db.exec("DELETE FROM animales");
    db.exec("DELETE FROM sesiones");
    res.json({ mensaje: "✅ Base de datos limpia." });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ── HEALTH ────────────────────────────────────────────────────────────────────
app.get("/", (req, res) => res.json({ status: "ANGUS DEL ESTE Bot activo 🐂", version: "2.0" }));

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`AMAKAIK Bot corriendo en puerto ${PORT}`));
