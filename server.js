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

function buscarAnimal(identificador) {
  // Buscar por RP (prioritario), chip, o id
  let animal = db.prepare("SELECT * FROM animales WHERE LOWER(rp) = LOWER(?) AND estado = 'ACTIVO'").get(identificador);
  if (!animal) animal = db.prepare("SELECT * FROM animales WHERE chip = ? AND estado = 'ACTIVO'").get(identificador);
  if (!animal) animal = db.prepare("SELECT * FROM animales WHERE id = ? AND estado = 'ACTIVO'").get(parseInt(identificador));
  return animal;
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

  return `Sos el asistente de ganadería de precisión de la Cabaña AMAKAIK, establecimiento Angus en Uruguay. Respondés en español rioplatense, conciso.

FECHA DE HOY: ${new Date().toISOString().slice(0,10)}

HERRAMIENTAS — respondé SOLO con JSON exacto sin texto extra cuando sea una acción:

REGISTRO DE ANIMALES:
{"accion":"registrar_animal","rp":"","chip":"","fecha_nac":"YYYY-MM-DD","sexo":"MACHO/HEMBRA","pelo":"NEGRO/COLORADO","categoria":"TERNERO/RECRIA/VAQUILLONA/VACA/TORO/NOVILLO","registro":"PP/SA/GRAL","madre_rp":"","padre_rp":"","notas":""}

PESADAS:
{"accion":"registrar_pesada","rp":"","peso":0,"contexto":"NACIMIENTO/DESTETE/DESARROLLO/AÑO/18MESES/ADULTA/VENTA","notas":""}
{"accion":"pesada_lote","pesadas":[{"rp":"","peso":0}],"contexto":"DESARROLLO","notas":""}

MEDICIONES:
{"accion":"registrar_medicion","rp":"","tipo":"CE/ALTURA/FRAME/DOCILIDAD/CC","valor":0,"notas":""}

ECOGRAFÍAS (datos CIIE):
{"accion":"registrar_ecografia","rp":"","fecha_medicion":"YYYY-MM-DD","dias_vida":0,"pct_gi":0,"aob":0,"gd":0,"gc":0,"estado":"Dentro de Protocolo/Fuera de Protocolo"}

SERVICIOS REPRODUCTIVOS:
{"accion":"registrar_servicio","rp":"","temporada":"2024/2025","tipo_servicio":"IATF/NATURAL/EMBRION","semen_iatf":"","toro_natural":"","fecha_iatf":"","tacto_pre":"","cc_pre":0}
{"accion":"resultado_tacto","rp":"","resultado":"PREÑADA/VACIA","fecha_parto_estimada":""}
{"accion":"registrar_parto","madre_rp":"","ternero_rp":"","peso_nac":0,"sexo":"MACHO/HEMBRA","pelo":"","fecha":""}

SANIDAD:
{"accion":"registrar_sanidad","rp":"","tipo":"TRATAMIENTO/VACUNA/DESPARASITACION/OBSERVACION","producto":"","dosis":"","notas":""}
{"accion":"sanidad_lote","registros":[{"rp":"","producto":"","dosis":""}],"tipo":"DESPARASITACION","notas":""}

MOVIMIENTOS:
{"accion":"dar_baja","rp":"","motivo":"VENTA/MUERTE/DESCARTE/TRANSFERENCIA","notas":""}
{"accion":"cambiar_categoria","rp":"","nueva_categoria":"VACA/VAQUILLONA/TORO/NOVILLO/RECRIA"}

CONSULTAS:
{"accion":"ficha_animal","rp":""}
{"accion":"ver_rodeo"}
{"accion":"ver_lote","categoria":"VACA/RECRIA/etc","sexo":"MACHO/HEMBRA"}
{"accion":"ver_pesadas","rp":""}
{"accion":"ver_servicios","rp":""}
{"accion":"ver_ecografias","rp":""}
{"accion":"ver_sanidad","rp":""}
{"accion":"buscar","termino":""}
{"accion":"ranking_peso","categoria":"","limite":10}
{"accion":"estadisticas_ecografia"}
{"accion":"resumen_servicios","temporada":"2024/2025"}
{"accion":"ver_ultimos"}
{"accion":"texto","mensaje":"respuesta"}

IMPORTACIÓN MASIVA:
{"accion":"importar_animales","animales":[{...}]}
{"accion":"importar_ecografias","ecografias":[{...}]}
{"accion":"importar_pesadas","pesadas":[{...}]}

REGLAS:
- RP es el identificador principal del animal (puede ser número o texto como "S401", "ADE1238")
- CHIP es el número electrónico (8 dígitos)
- Categorías: TERNERO (0-7m), RECRIA (7-15m), VAQUILLONA (15-24m hembra), NOVILLO (macho castrado), VACA (hembra adulta con cría), TORO (macho reproductor)
- CE = circunferencia escrotal (cm), CC = condición corporal (1-10), DOCILIDAD (1-5)
- Ecografías: %GI=grasa intramuscular, AOB=área ojo bife (cm²), GD=grasa dorsal (mm), GC=grasa cadera (mm)
- "Dentro de Protocolo" = medido entre 505-595 días de edad
- PPD = promedio peso destete, PPN = promedio peso nacimiento, PVA = peso vaca adulta
- Si no entendés → preguntar con accion texto

DATOS ACTUALES:
Rodeo: ${JSON.stringify(resumen)}
Últimas pesadas: ${JSON.stringify(ultimasPesadas.map(p => ({ rp: p.rp, peso: p.peso, fecha: p.fecha, contexto: p.contexto })))}
Última sanidad: ${JSON.stringify(ultimosSanidad.map(s => ({ rp: s.rp, tipo: s.tipo, producto: s.producto, fecha: s.fecha })))}`;
}

// ── EJECUTAR ACCIÓN ───────────────────────────────────────────────────────────
function ejecutarAccion(accion) {
  const hoy = new Date().toISOString().split("T")[0];

  // REGISTRAR ANIMAL
  if (accion.accion === "registrar_animal") {
    const { rp, chip, fecha_nac, sexo, pelo, categoria, registro, madre_rp, padre_rp, notas } = accion;
    if (!rp || !sexo) return "❌ Faltan datos: necesito al menos RP y sexo.";
    try {
      const r = db.prepare(`
        INSERT INTO animales (chip, rp, fecha_nac, sexo, pelo, categoria, registro, madre_rp, padre_rp, notas, fecha_ingreso)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(chip || null, rp, fecha_nac || null, sexo.toUpperCase(), pelo || null, categoria || "RECRIA", registro || null, madre_rp || null, padre_rp || null, notas || null, hoy);
      return `✅ Animal registrado!\n🏷️ RP: ${rp}${chip ? ` | CHIP: ${chip}` : ""}\n${sexo} ${pelo || ""} | ${categoria || "RECRIA"}\n${madre_rp ? `👩 Madre: ${madre_rp}` : ""}${padre_rp ? ` | 👨 Padre: ${padre_rp}` : ""}`;
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

    // Calcular GDP si hay pesada anterior
    const ultima = db.prepare("SELECT * FROM pesadas WHERE animal_id = ? ORDER BY fecha DESC LIMIT 1").get(animal.id);
    let gdp = null;
    if (ultima) {
      const dias = Math.floor((new Date() - new Date(ultima.fecha)) / (1000*60*60*24));
      if (dias > 0) gdp = ((peso - ultima.peso) / dias);
    }

    db.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto, gdp, notas) VALUES (?, ?, ?, ?, ?, ?)")
      .run(animal.id, hoy, peso, accion.contexto || "DESARROLLO", gdp, accion.notas || null);

    let resp = `✅ Pesada registrada!\n🏷️ RP ${animal.rp} | ${fmt(peso)} kg`;
    if (gdp !== null) resp += `\n📈 GDP: ${fmt(gdp * 1000)} g/día (desde última pesada)`;
    resp += `\n📋 ${accion.contexto || "DESARROLLO"}`;
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
    db.prepare("INSERT INTO mediciones (animal_id, fecha, tipo, valor, notas) VALUES (?, ?, ?, ?, ?)")
      .run(animal.id, hoy, accion.tipo, parseFloat(accion.valor), accion.notas || null);
    const unidades = { CE: "cm", ALTURA: "cm", FRAME: "", DOCILIDAD: "/5", CC: "/10" };
    return `✅ Medición registrada!\n🏷️ RP ${animal.rp} | ${accion.tipo}: ${accion.valor}${unidades[accion.tipo] || ""}`;
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
    // Registrar ternero como nuevo animal
    const terneroRp = accion.ternero_rp || `T${Date.now().toString().slice(-4)}`;
    try {
      db.prepare(`
        INSERT INTO animales (rp, fecha_nac, sexo, pelo, categoria, madre_rp, padre_rp, fecha_ingreso)
        VALUES (?, ?, ?, ?, 'TERNERO', ?, ?, ?)
      `).run(terneroRp, accion.fecha || hoy, accion.sexo || "MACHO", accion.pelo || null, accion.madre_rp, null, hoy);
    } catch(e) { /* ya existe */ }

    // Registrar peso nacimiento
    const ternero = buscarAnimal(terneroRp);
    if (ternero && accion.peso_nac) {
      db.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto) VALUES (?, ?, ?, 'NACIMIENTO')")
        .run(ternero.id, accion.fecha || hoy, parseFloat(accion.peso_nac));
    }

    // Actualizar servicio de la madre
    const serv = db.prepare("SELECT * FROM servicios WHERE animal_id = ? ORDER BY created_at DESC LIMIT 1").get(madre.id);
    if (serv) {
      db.prepare("UPDATE servicios SET ternero_rp = ?, peso_nacimiento = ?, sexo_cria = ?, fecha_parto = ? WHERE id = ?")
        .run(terneroRp, accion.peso_nac || null, accion.sexo || null, accion.fecha || hoy, serv.id);
    }

    return `🐄 Parto registrado!\n👩 Madre: RP ${accion.madre_rp}\n🐮 Ternero: RP ${terneroRp} | ${accion.sexo || ""} ${accion.pelo || ""}${accion.peso_nac ? `\n⚖️ Peso nac: ${accion.peso_nac} kg` : ""}`;
  }

  // SANIDAD
  if (accion.accion === "registrar_sanidad") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    db.prepare("INSERT INTO sanidad (animal_id, fecha, tipo, producto, dosis, notas) VALUES (?, ?, ?, ?, ?, ?)")
      .run(animal.id, hoy, accion.tipo, accion.producto || null, accion.dosis || null, accion.notas || null);
    return `💉 Sanidad registrada!\n🏷️ RP ${animal.rp} | ${accion.tipo}\n💊 ${accion.producto || ""}${accion.dosis ? ` | ${accion.dosis}` : ""}`;
  }

  // SANIDAD LOTE
  if (accion.accion === "sanidad_lote") {
    if (!Array.isArray(accion.registros)) return "❌ Formato inválido.";
    let ok = 0, errores = [];
    const stmt = db.prepare("INSERT INTO sanidad (animal_id, fecha, tipo, producto, dosis, notas) VALUES (?, ?, ?, ?, ?, ?)");
    for (const r of accion.registros) {
      const animal = buscarAnimal(r.rp);
      if (!animal) { errores.push(r.rp); continue; }
      stmt.run(animal.id, hoy, accion.tipo || "TRATAMIENTO", r.producto || accion.producto || null, r.dosis || null, accion.notas || null);
      ok++;
    }
    let resp = `💉 Sanidad lote: ${ok} registrados`;
    if (errores.length) resp += `\n⚠️ No encontrados: ${errores.join(", ")}`;
    return resp;
  }

  // DAR BAJA
  if (accion.accion === "dar_baja") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    db.prepare("UPDATE animales SET estado = 'BAJA', fecha_salida = ?, motivo_salida = ? WHERE id = ?")
      .run(hoy, accion.motivo || "VENTA", animal.id);
    return `📤 Baja registrada!\n🏷️ RP ${animal.rp} | Motivo: ${accion.motivo || "VENTA"}`;
  }

  // CAMBIAR CATEGORÍA
  if (accion.accion === "cambiar_categoria") {
    const animal = buscarAnimal(accion.rp);
    if (!animal) return `❌ No encontré animal con RP "${accion.rp}".`;
    db.prepare("UPDATE animales SET categoria = ? WHERE id = ?").run(accion.nueva_categoria, animal.id);
    return `✅ Categoría actualizada!\n🏷️ RP ${animal.rp} | ${animal.categoria} → ${accion.nueva_categoria}`;
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
      INSERT OR IGNORE INTO animales (chip, rp, fecha_nac, raza, registro, sexo, pelo, categoria, madre_rp, madre_hba, padre_rp, padre_hba, fecha_ingreso)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    for (const a of accion.animales) {
      try {
        stmt.run(a.chip||null, a.rp, a.fecha_nac||null, a.raza||'A. ANGUS', a.registro||null,
                 a.sexo||'HEMBRA', a.pelo||null, a.categoria||'RECRIA', a.madre_rp||null, a.madre_hba||null,
                 a.padre_rp||null, a.padre_hba||null, a.fecha_ingreso||new Date().toISOString().slice(0,10));
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
      const animal = buscarAnimal(e.rp);
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
      const animal = buscarAnimal(p.rp);
      if (!animal) { errores++; continue; }
      try {
        db.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto) VALUES (?, ?, ?, ?)")
          .run(animal.id, p.fecha||new Date().toISOString().slice(0,10), p.peso, p.contexto||"DESARROLLO");
        ok++;
      } catch(err) { errores++; }
    }
    return `✅ Pesadas importadas: ${ok} cargadas, ${errores} errores.`;
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
    historial.push({ role: "assistant", content: rawRespuesta });
    saveHistorial(usuario, historial);

    const limpio = rawRespuesta.replace(/```json\s*/gi, "").replace(/```\s*/g, "").trim();
    let respuesta = "";
    try {
      // Intentar array de acciones
      const arrMatch = limpio.match(/\[[\s\S]*\]/);
      if (arrMatch) {
        const arr = JSON.parse(arrMatch[0]);
        if (Array.isArray(arr)) {
          const resultados = arr.map(a => ejecutarAccion(a));
          respuesta = resultados.join("\n\n");
        }
      } else {
        const jsonMatch = limpio.match(/\{[\s\S]*"accion"[\s\S]*\}/);
        const jsonStr = jsonMatch ? jsonMatch[0] : limpio;
        const accion = JSON.parse(jsonStr);
        if (accion && accion.accion) {
          respuesta = ejecutarAccion(accion);
        } else {
          respuesta = limpio;
        }
      }
    } catch {
      respuesta = limpio;
    }

    res.json({ respuesta });
  } catch (err) {
    console.error("Error webhook-interno:", err);
    res.json({ respuesta: "❌ Error. Intentá de nuevo." });
  }
});

// ── API REST ──────────────────────────────────────────────────────────────────
app.get("/api/animales", (req, res) => {
  const { categoria, sexo, estado, limite } = req.query;
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
  const animal = buscarAnimal(req.params.rp);
  if (!animal) return res.status(404).json({ error: "No encontrado" });
  const pesadas = db.prepare("SELECT * FROM pesadas WHERE animal_id = ? ORDER BY fecha DESC").all(animal.id);
  const mediciones = db.prepare("SELECT * FROM mediciones WHERE animal_id = ? ORDER BY fecha DESC").all(animal.id);
  const ecografias = db.prepare("SELECT * FROM ecografias WHERE animal_id = ? ORDER BY fecha_medicion DESC").all(animal.id);
  const servicios = db.prepare("SELECT * FROM servicios WHERE animal_id = ? ORDER BY created_at DESC").all(animal.id);
  const sanidad = db.prepare("SELECT * FROM sanidad WHERE animal_id = ? ORDER BY fecha DESC").all(animal.id);
  const hijos = db.prepare("SELECT * FROM animales WHERE madre_rp = ? ORDER BY fecha_nac DESC").all(animal.rp);
  res.json({ ...animal, pesadas, mediciones, ecografias, servicios, sanidad, hijos });
});

app.get("/api/pesadas", (req, res) => {
  const rows = db.prepare(`
    SELECT p.*, a.rp, a.categoria, a.sexo FROM pesadas p
    JOIN animales a ON a.id = p.animal_id ORDER BY p.fecha DESC LIMIT ?
  `).all(parseInt(req.query.limite) || 200);
  res.json(rows);
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

// ── HEALTH ────────────────────────────────────────────────────────────────────
app.get("/", (req, res) => res.json({ status: "AMAKAIK Bot activo 🐄", version: "1.0" }));

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`AMAKAIK Bot corriendo en puerto ${PORT}`));
