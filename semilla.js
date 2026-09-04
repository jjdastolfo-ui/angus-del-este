// ─────────────────────────────────────────────────────────────────────────────
// SEMILLA — arma una base de prueba con un rodeo creíble.
//
//   node datos/semilla.js            → escribe ./data/principal.db
//   DB_DIR=/otra/carpeta node datos/semilla.js
//
// No se usa en producción: en Railway la base es la real. Sirve para correr
// el sistema en la compu y ver el tablero con números adentro.
// ─────────────────────────────────────────────────────────────────────────────
const Database = require("better-sqlite3");
const path = require("path");
const fs = require("fs");

const DB_DIR = process.env.DB_DIR || path.join(__dirname, "..", "data");
const CAMPO = process.env.CAMPO || "principal";
if (!fs.existsSync(DB_DIR)) fs.mkdirSync(DB_DIR, { recursive: true });
const archivo = path.join(DB_DIR, `${CAMPO}.db`);
if (fs.existsSync(archivo)) fs.unlinkSync(archivo);
const db = new Database(archivo);
db.pragma("journal_mode = WAL");

db.exec(`
  CREATE TABLE animales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rp TEXT NOT NULL, chip TEXT, sexo TEXT, categoria TEXT,
    estado TEXT DEFAULT 'ACTIVO', fecha_nac TEXT, pelo TEXT, raza TEXT,
    madre_rp TEXT, padre_rp TEXT, hbu TEXT, registro TEXT, lote TEXT,
    notas TEXT, created_at TEXT DEFAULT (datetime('now')), UNIQUE(rp));
  CREATE TABLE pesadas (id INTEGER PRIMARY KEY AUTOINCREMENT, animal_id INTEGER NOT NULL,
    fecha TEXT NOT NULL, peso REAL NOT NULL, contexto TEXT, gdp REAL, created_at TEXT DEFAULT (datetime('now')));
  CREATE TABLE servicios (id INTEGER PRIMARY KEY AUTOINCREMENT, animal_id INTEGER NOT NULL,
    temporada TEXT, tipo_servicio TEXT, semen_iatf TEXT, fecha_iatf TEXT, toro_natural TEXT,
    fecha_ingreso_toro TEXT, fecha_salida_toro TEXT, resultado TEXT, fecha_tacto TEXT, notas TEXT,
    created_at TEXT DEFAULT (datetime('now')));
  CREATE TABLE mediciones (id INTEGER PRIMARY KEY AUTOINCREMENT, animal_id INTEGER NOT NULL,
    fecha TEXT NOT NULL, tipo TEXT NOT NULL, valor REAL, created_at TEXT DEFAULT (datetime('now')));
  CREATE TABLE sanidad (id INTEGER PRIMARY KEY AUTOINCREMENT, animal_id INTEGER NOT NULL,
    fecha TEXT NOT NULL, producto TEXT, dosis TEXT, motivo TEXT, created_at TEXT DEFAULT (datetime('now')));
  CREATE TABLE lotes (id INTEGER PRIMARY KEY AUTOINCREMENT, nombre TEXT NOT NULL, potrero TEXT, descripcion TEXT);
  CREATE TABLE lote_animales (lote_id INTEGER NOT NULL, animal_id INTEGER NOT NULL, fecha_ingreso TEXT);
`);

// Un generador con semilla fija: la base sale igual cada vez que se corre.
let s = 20260903;
const rnd = () => { s = (s * 1103515245 + 12345) & 0x7fffffff; return s / 0x7fffffff; };
const entre = (a, b) => a + Math.floor(rnd() * (b - a + 1));
const elegir = arr => arr[Math.floor(rnd() * arr.length)];
const sumar = (f, n) => { const d = new Date(f); d.setDate(d.getDate() + n); return d.toISOString().slice(0, 10); };
const r1 = n => Math.round(n * 10) / 10;

const TOROS = [
  { rp: "B332", nombre: "HERCULES", hba: "876302" }, { rp: "9426", nombre: "PONCHO", hba: "904427" },
  { rp: "4178", nombre: "LUCUMA", hba: "834939" }, { rp: "833", nombre: "ITALO", hba: "888001" },
  { rp: "1039", nombre: "JAQUE MATE", hba: "877604" }, { rp: "IVAR4", nombre: "IVAR 4", hba: "864363" }
];
const SEMEN = ["ROBOVER FRANZ", "KARE 16", "ZEUS II", "MINIHUE", "HUINCA"];
const PELOS = ["NEGRO", "NEGRO", "NEGRO", "COLORADO", "COLORADO"];

db.exec("ALTER TABLE animales ADD COLUMN nombre TEXT");
const insA = db.prepare(`INSERT INTO animales (rp,chip,sexo,categoria,estado,fecha_nac,pelo,raza,madre_rp,padre_rp,hbu,registro,nombre)
  VALUES (@rp,@chip,@sexo,@categoria,@estado,@fecha_nac,@pelo,'A. ANGUS',@madre_rp,@padre_rp,@hbu,'PP',@nombre)`);
const insP = db.prepare("INSERT INTO pesadas (animal_id,fecha,peso,contexto) VALUES (?,?,?,?)");
const insS = db.prepare(`INSERT INTO servicios (animal_id,temporada,tipo_servicio,semen_iatf,fecha_iatf,toro_natural,
  fecha_ingreso_toro,fecha_salida_toro,resultado,fecha_tacto) VALUES (?,?,?,?,?,?,?,?,?,?)`);
const insSan = db.prepare("INSERT INTO sanidad (animal_id,fecha,producto,dosis,motivo) VALUES (?,?,?,?,?)");

// Toros
for (const t of TOROS) {
  const id = insA.run({ rp: t.rp, chip: null, sexo: "M", categoria: "TORO", estado: "ACTIVO",
    fecha_nac: `${entre(2017, 2022)}-0${entre(7, 9)}-${entre(10, 28)}`, pelo: elegir(PELOS),
    madre_rp: null, padre_rp: null, hbu: t.hba, nombre: t.nombre }).lastInsertRowid;
  insP.run(id, "2026-06-15", entre(780, 980), "ADULTO");
  db.prepare("INSERT INTO mediciones (animal_id,fecha,tipo,valor) VALUES (?,?,?,?)").run(id, "2026-06-15", "CE", entre(36, 44));
}

// Vientres: 90 vacas y vaquillonas nacidas entre 2015 y 2023.
// Cada campaña (2022/23 a 2025/26) tiene servicio, tacto y parto o falla.
const CAMPANAS = [
  { temp: "2022", iatf: "2022-11-20", ingreso: "2022-12-05", salida: "2023-02-10", tacto: "2023-04-12" },
  { temp: "2023", iatf: "2023-11-18", ingreso: "2023-12-03", salida: "2024-02-08", tacto: "2024-04-10" },
  { temp: "2024", iatf: "2024-11-16", ingreso: "2024-12-01", salida: "2025-02-06", tacto: "2025-04-09" },
  { temp: "2025", iatf: "2025-11-15", ingreso: "2025-11-30", salida: "2026-02-05", tacto: "2026-04-08" }
];
const HOY = "2026-09-03";
let ternero = 1;
const vientres = [];
for (let i = 0; i < 90; i++) {
  const anioNac = entre(2015, 2023);
  const rp = i < 60 ? String(11 + i * 2) : `${elegir(["B", "C", "D", "F"])}${entre(100, 899)}`;
  if (vientres.some(v => v.rp === rp)) { i--; continue; }
  const v = { rp, chip: rnd() < 0.7 ? `3201003626${entre(1000, 9999)}` : null, sexo: "H",
    categoria: anioNac >= 2023 ? "VAQUILLONA" : "VACA", estado: "ACTIVO",
    fecha_nac: `${anioNac}-0${entre(7, 9)}-${String(entre(1, 28)).padStart(2, "0")}`,
    pelo: elegir(PELOS), madre_rp: null, padre_rp: elegir(TOROS).nombre, hbu: String(entre(840000, 905000)), nombre: null };
  v.id = insA.run(v).lastInsertRowid;
  v.pesoAdulto = entre(400, 620);
  insP.run(v.id, v.fecha_nac, r1(24 + rnd() * 12), "NACIMIENTO");
  insP.run(v.id, "2026-03-20", v.pesoAdulto, "ADULTO");
  vientres.push(v);
}

// Campañas por vaca. Una vaca "mala" falla más seguido.
for (const v of vientres) {
  const nacio = +v.fecha_nac.slice(0, 4);
  const calidad = rnd();   // 0 mala … 1 buena
  for (const c of CAMPANAS) {
    const edadAlServicio = +c.temp - nacio;
    if (edadAlServicio < 2) continue;              // entra a servicio a los 2 años
    const iatf = rnd() < 0.8;
    const semen = iatf ? elegir(SEMEN) : null;
    const toro = elegir(TOROS);
    const suerte = rnd();
    // Qué pasó: IATF (45%), toro cabeza/cuerpo/cola, o vacía.
    let resultado, parto = null, origen = null;
    const pVacia = 0.06 + (1 - calidad) * 0.18;
    if (suerte < pVacia) resultado = "VACIA";
    else {
      const d = rnd();
      if (iatf && d < 0.5) { origen = "IATF"; parto = sumar(c.iatf, 283 + entre(-8, 8)); resultado = "PREÑADA_IATF"; }
      else { const tramo = d < 0.75 ? 1 : d < 0.92 ? 2 : 3; origen = "TORO";
        parto = sumar(c.iatf, 283 + 10 + entre((tramo - 1) * 20 + 1, tramo * 20)); resultado = "PREÑADA_TORO"; }
    }
    insS.run(v.id, c.temp, iatf ? "IATF+REPASO" : "NATURAL", semen, iatf ? c.iatf : null, toro.nombre,
      c.ingreso, c.salida, resultado, c.tacto);
    if (!parto) continue;

    // La parición 2026 está en curso: lo que viene después de hoy todavía no nació.
    if (parto > HOY) continue;
    // Una preñada de 2025 que "abortó": sin cría y ya vencida. 3% de los casos.
    if (c.temp === "2025" && rnd() < 0.03) continue;

    const sexo = rnd() < 0.5 ? "M" : "H";
    const anioP = parto.slice(0, 4);
    const rpT = `${anioP.slice(2)}${String(ternero++).padStart(3, "0")}`;
    const pn = r1(22 + rnd() * 16);
    const muerto = rnd() < 0.03;
    const tid = insA.run({ rp: rpT, chip: rnd() < 0.6 ? `3201003626${entre(1000, 9999)}` : null, sexo,
      categoria: sexo === "M" ? "TERNERO" : "TERNERA", estado: muerto ? "MUERTO" : "ACTIVO",
      fecha_nac: parto, pelo: elegir(PELOS), madre_rp: v.rp,
      padre_rp: origen === "IATF" ? semen : toro.nombre, hbu: null, nombre: null }).lastInsertRowid;
    insP.run(tid, parto, pn, "NACIMIENTO");
    if (muerto) continue;
    const edadDias = Math.round((new Date(HOY) - new Date(parto)) / 86400000);
    if (edadDias > 200) {
      const fDest = sumar(parto, 195 + entre(0, 20));
      const destete = Math.round(140 + calidad * 90 + rnd() * 50 + (sexo === "M" ? 12 : 0));
      if (rnd() > 0.05) insP.run(tid, fDest, destete, "DESTETE");
      // Recría: pesadas cada ~60 días hasta hoy.
      let f = sumar(fDest, 60), p = destete;
      while (f < HOY && edadDias < 800) { p += entre(35, 60); insP.run(tid, f, p, "RECRIA"); f = sumar(f, 60); }
      if (edadDias > 700 && sexo === "H") db.prepare("UPDATE animales SET categoria='VAQUILLONA' WHERE id=?").run(tid);
      if (edadDias > 700 && sexo === "M") db.prepare("UPDATE animales SET categoria='NOVILLO' WHERE id=?").run(tid);
    }
  }
}

// Terneros de esta semana: llegaron con caravana control y todavía no tienen RP.
db.exec("ALTER TABLE animales ADD COLUMN caravana_control TEXT; ALTER TABLE animales ADD COLUMN caravana_color TEXT; ALTER TABLE animales ADD COLUMN rp_provisorio INTEGER DEFAULT 0");
const madresLibres = vientres.filter(v => !db.prepare("SELECT 1 FROM animales WHERE madre_rp=? AND fecha_nac LIKE '2026%'").get(v.rp)).slice(0, 5);
madresLibres.forEach((m, i) => {
  const control = String(entre(100, 999)), color = elegir(["BLANCA", "VERDE", "AMARILLA"]);
  const fecha = sumar(HOY, -entre(0, 6)), sexo = i % 2 ? "H" : "M";
  const id = insA.run({ rp: "C" + control, chip: null, sexo, categoria: sexo === "M" ? "TERNERO" : "TERNERA", estado: "ACTIVO", fecha_nac: fecha, pelo: elegir(PELOS),
    madre_rp: m.rp, padre_rp: elegir(TOROS).nombre, hbu: null, nombre: null }).lastInsertRowid;
  db.prepare("UPDATE animales SET caravana_control=?, caravana_color=?, rp_provisorio=1 WHERE id=?").run(control, color, id);
  insP.run(id, fecha, r1(26 + rnd() * 10), "NACIMIENTO");
});

// Lotes: corral de terminación con novillos y toros que no calificaron.
const corral = db.prepare("INSERT INTO lotes (nombre,potrero,descripcion) VALUES (?,?,?)")
  .run("TERMINACION 2026", "CORRAL 2", "Novillos y toros a terminar").lastInsertRowid;
const recria = db.prepare("INSERT INTO lotes (nombre,potrero,descripcion) VALUES (?,?,?)")
  .run("RECRIA HEMBRAS", "POTRERO 7", "Terneras destetadas 2025").lastInsertRowid;
const insL = db.prepare("INSERT INTO lote_animales (lote_id,animal_id,fecha_ingreso) VALUES (?,?,?)");
const machos = db.prepare(`SELECT id FROM animales WHERE sexo='M' AND categoria IN ('NOVILLO','TERNERO')
  AND estado='ACTIVO' AND fecha_nac < '2025-12-01' ORDER BY rp LIMIT 18`).all();
for (const m of machos) {
  const ingreso = "2026-06-10";
  insL.run(corral, m.id, ingreso);
  const ult = db.prepare("SELECT peso FROM pesadas WHERE animal_id=? ORDER BY fecha DESC LIMIT 1").get(m.id);
  const base = ult ? ult.peso : 300;
  insP.run(m.id, ingreso, base, "ENTRADA CORRAL");
  insP.run(m.id, "2026-07-15", r1(base + 35 * (0.9 + rnd() * 0.6)), "CORRAL");
  if (rnd() > 0.2) insP.run(m.id, "2026-08-20", r1(base + 75 * (0.9 + rnd() * 0.6)), "CORRAL");
}
const hembras = db.prepare(`SELECT id FROM animales WHERE sexo='H' AND categoria='TERNERA'
  AND estado='ACTIVO' AND fecha_nac BETWEEN '2025-07-01' AND '2025-12-31' ORDER BY rp`).all();
for (const h of hembras) insL.run(recria, h.id, "2026-04-20");

// Sanidad: una vacunación general.
for (const a of db.prepare("SELECT id FROM animales WHERE estado='ACTIVO'").all())
  insSan.run(a.id, "2026-05-12", "AFTOSA", "2 ml", "Campaña obligatoria");

// Notas de campo sobre algunas vacas.
const plantelMod = require("../plantel.js");
plantelMod.init(db);
const algunas = vientres.slice(0, 6);
plantelMod.guardarNota(db, algunas[0].rp, "Muy mansa, buena madre");
plantelMod.guardarNota(db, algunas[1].rp, "Renga de la pata izquierda, revisar casco");
plantelMod.guardarNota(db, algunas[2].rp, "Brava en la manga");
plantelMod.guardarNota(db, algunas[3].rp, "Ubre con un cuarto perdido");

const n = t => db.prepare(`SELECT COUNT(*) n FROM ${t}`).get().n;
console.log(`Base de prueba en ${archivo}`);
console.log(`  animales ${n("animales")} · pesadas ${n("pesadas")} · servicios ${n("servicios")} · lotes ${n("lotes")} · notas ${n("notas_campo")}`);
db.close();
