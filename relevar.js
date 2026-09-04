// ─────────────────────────────────────────────────────────────────────────────
// RELEVAR — cargar lo que se junta en el campo, rápido y sin equivocarse.
//
// En la manga se anota "148 312", "N022 298", y así veinte veces. Acá eso se
// pega tal cual, el sistema entiende cada línea, dice qué RP no reconoce y
// qué peso no cierra con la historia del animal, y recién cuando se confirma
// escribe en la base. Lo mismo para sanidad, nacimientos, mediciones y notas,
// y para planillas CSV que vienen de otro sistema o de una balanza.
//
// Todo tiene dos pasos: `simular: true` muestra qué haría; sin eso, lo hace.
// ─────────────────────────────────────────────────────────────────────────────
const animalesMod = require("./animales.js");
const xlsx = require("./xlsx.js");

const hoyIso = () => new Date().toISOString().slice(0, 10);
const dias = (a, b) => (a && b) ? Math.round((new Date(b) - new Date(a)) / 86400000) : null;
const r1 = n => Math.round(n * 10) / 10;

// ── ENTENDER LO QUE VIENE ────────────────────────────────────────────────────

// "12/08/2026", "12-8-26", "2026-08-12" → "2026-08-12". Lo que no se entiende, null.
function fechaIso(v) {
  if (v == null || v === "") return null;
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  const t = String(v).trim();
  let m = t.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if (m) return `${m[1]}-${m[2].padStart(2, "0")}-${m[3].padStart(2, "0")}`;
  m = t.match(/^(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{2,4})$/);
  if (m) {
    const a = m[3].length === 2 ? "20" + m[3] : m[3];
    const iso = `${a}-${m[2].padStart(2, "0")}-${m[1].padStart(2, "0")}`;
    return isNaN(new Date(iso)) ? null : iso;
  }
  // Serial de Excel
  if (/^\d{5}$/.test(t)) return new Date(Date.UTC(1899, 11, 30) + Number(t) * 86400000).toISOString().slice(0, 10);
  return null;
}
// "312", "312,5", "312.5 kg" → 312.5
function numero(v) {
  if (v == null || v === "") return null;
  if (typeof v === "number") return v;
  const t = String(v).replace(/kg|kgs|k/gi, "").trim().replace(/\.(?=\d{3}(\D|$))/g, "").replace(",", ".");
  const n = Number(t);
  return isFinite(n) ? n : null;
}
const sexoNorm = s => { const t = String(s || "").trim().toUpperCase(); return !t ? null : t.startsWith("M") ? "M" : t.startsWith("H") || t.startsWith("F") ? "H" : null; };
const peloNorm = p => { const t = String(p || "").trim().toUpperCase().normalize("NFD").replace(/[̀-ͯ]/g, ""); return !t ? null : /COL|ROJ|RED/.test(t) ? "COLORADO" : /NEG|BLACK/.test(t) ? "NEGRO" : t; };

/**
 * Texto pegado de la libreta: una línea por animal, "RP peso" o "RP, peso"
 * o "RP\tpeso". También acepta "RP peso fecha".
 */
function parsearLineas(texto) {
  return String(texto || "").split(/\r?\n/).map(l => l.trim()).filter(Boolean).map((l, i) => {
    const partes = l.split(/[\s,;\t]+/).filter(Boolean);
    return { linea: i + 1, rp: partes[0], valor: partes[1] != null ? partes[1] : null, extra: partes.slice(2), texto: l };
  });
}

// ── PESADAS ──────────────────────────────────────────────────────────────────

/**
 * filas: [{rp, peso, fecha?, contexto?}]. Devuelve qué haría (o hizo) fila por fila.
 * Avisa cuando un peso no cierra: baja más de 12% o sube más de 3 kg/día
 * respecto de la pesada anterior. No frena por eso, avisa.
 */
function pesadas(db, { filas, fecha, contexto, simular, usuario }) {
  const f0 = fechaIso(fecha) || hoyIso();
  const ctx = String(contexto || "PESADA").toUpperCase().trim();
  const ins = db.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto) VALUES (?,?,?,?)");
  const ultima = db.prepare("SELECT fecha, peso FROM pesadas WHERE animal_id=? AND fecha<=? ORDER BY fecha DESC, id DESC LIMIT 1");
  const repetida = db.prepare("SELECT id, peso FROM pesadas WHERE animal_id=? AND fecha=? AND peso=?");
  const out = [];
  const vistos = new Map();

  const hacer = () => {
    for (const [i, f] of (filas || []).entries()) {
      const rp = String(f.rp || "").trim();
      const peso = numero(f.peso != null ? f.peso : f.valor);
      const fe = fechaIso(f.fecha) || f0;
      const c = String(f.contexto || ctx).toUpperCase();
      const r = { fila: i + 1, rp, peso, fecha: fe, contexto: c, ok: false, avisos: [] };
      out.push(r);
      if (!rp) { r.error = "Sin RP"; continue; }
      const a = animalesMod.porRp(db, rp);
      if (!a) { r.error = `No existe el animal ${rp}`; continue; }
      r.rp = a.rp; r.categoria = a.categoria;
      if (peso == null) { r.error = "Sin peso"; continue; }
      if (peso < 10 || peso > 1500) { r.error = `Peso ${peso} fuera de rango`; continue; }
      if (String(a.estado || "ACTIVO").toUpperCase() !== "ACTIVO") r.avisos.push(`el animal figura ${a.estado}`);
      if (vistos.has(a.rp)) r.avisos.push(`repetido en la fila ${vistos.get(a.rp)}`);
      vistos.set(a.rp, i + 1);
      const u = ultima.get(a.id, fe);
      if (u) {
        r.anterior = u.peso; r.fecha_anterior = u.fecha;
        const d = dias(u.fecha, fe);
        r.diferencia = r1(peso - u.peso);
        if (d > 0) r.gdp = Math.round((peso - u.peso) / d * 1000) / 1000;
        if (peso < u.peso * 0.88) r.avisos.push(`bajó ${r1(u.peso - peso)} kg desde ${u.fecha}`);
        else if (d > 0 && (peso - u.peso) / d > 3) r.avisos.push(`subió ${r1(peso - u.peso)} kg en ${d} días, más de 3 kg/día`);
        else if (d === 0 && peso !== u.peso) r.avisos.push(`ya tenía una pesada ese día: ${u.peso} kg`);
      }
      if (repetida.get(a.id, fe, peso)) { r.error = "Ya estaba cargada igual"; continue; }
      r.ok = true;
      if (!simular) { ins.run(a.id, fe, peso, c); r.hecho = true; }
    }
  };
  if (simular) hacer(); else db.transaction(hacer)();
  return resumen(out, simular, "pesada");
}

// ── SANIDAD ──────────────────────────────────────────────────────────────────
// Un producto a muchos animales de una vez: una lista de RP, un lote, o todos.

function sanidad(db, { rps, lote_id, todos, fecha, producto, dosis, motivo, simular }) {
  if (!producto) throw new Error("Falta el producto");
  const fe = fechaIso(fecha) || hoyIso();
  let ids = [];
  const out = [];
  if (todos) ids = db.prepare("SELECT id, rp FROM animales WHERE upper(COALESCE(estado,'ACTIVO'))='ACTIVO' ORDER BY rp").all();
  else if (lote_id) ids = db.prepare("SELECT a.id, a.rp FROM lote_animales la JOIN animales a ON a.id=la.animal_id WHERE la.lote_id=? ORDER BY a.rp").all(lote_id);
  else for (const rp of (rps || [])) {
    const a = animalesMod.porRp(db, rp);
    if (a) ids.push({ id: a.id, rp: a.rp }); else out.push({ rp, ok: false, error: `No existe el animal ${rp}` });
  }
  const ins = db.prepare("INSERT INTO sanidad (animal_id, fecha, producto, dosis, motivo) VALUES (?,?,?,?,?)");
  const ya = db.prepare("SELECT 1 FROM sanidad WHERE animal_id=? AND fecha=? AND upper(producto)=upper(?)");
  const hacer = () => {
    for (const a of ids) {
      const r = { rp: a.rp, ok: true, avisos: [] };
      out.push(r);
      if (ya.get(a.id, fe, producto)) { r.ok = false; r.error = "Ya tenía ese producto ese día"; continue; }
      if (!simular) { ins.run(a.id, fe, String(producto).trim(), dosis || null, motivo || null); r.hecho = true; }
    }
  };
  if (simular) hacer(); else db.transaction(hacer)();
  return { ...resumen(out, simular, "aplicación"), fecha: fe, producto, dosis, motivo };
}

// ── NACIMIENTOS ──────────────────────────────────────────────────────────────
// Cada ternero nuevo: el animal, su pesada de nacimiento y, si hay, la nota.

// El RP provisorio de un ternero con caravana control: "C" + número. Si ya
// hay uno igual (un número al azar se puede repetir), se le agrega el color o un sufijo.
function rpProvisorio(db, control, color) {
  const base = "C" + String(control).trim().toUpperCase().replace(/^C/, "");
  const existe = rp => !!db.prepare("SELECT 1 FROM animales WHERE upper(rp)=upper(?)").get(rp);
  if (!existe(base)) return base;
  const conColor = color ? `${base}-${String(color).trim().toUpperCase().slice(0, 3)}` : null;
  if (conColor && !existe(conColor)) return conColor;
  for (let i = 2; i < 100; i++) if (!existe(`${base}.${i}`)) return `${base}.${i}`;
  throw new Error(`No pude armar un RP provisorio para la control ${control}`);
}

function nacimientos(db, { filas, simular, usuario }) {
  const out = [];
  const insA = db.prepare(`INSERT INTO animales (rp, chip, sexo, categoria, estado, fecha_nac, pelo, raza, madre_rp, padre_rp, notas, caravana_control, caravana_color, rp_provisorio)
    VALUES (?,?,?,?, 'ACTIVO', ?,?,?,?,?,?,?,?,?)`);
  const insP = db.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto) VALUES (?,?,?,'NACIMIENTO')");
  const hacer = () => {
    for (const [i, f] of (filas || []).entries()) {
      const control = String(f.caravana_control || f.control || "").trim() || null;
      const color = String(f.caravana_color || f.color_caravana || "").trim() || null;
      const r = { fila: i + 1, rp: String(f.rp || "").trim(), madre: String(f.madre_rp || f.madre || "").trim(), control, color, ok: false, avisos: [] };
      out.push(r);
      const fe = fechaIso(f.fecha_nac || f.fecha);
      const sexo = sexoNorm(f.sexo);
      const pn = numero(f.peso_nac != null ? f.peso_nac : f.peso);
      // Sin RP definitivo: queda con la caravana control y un RP provisorio.
      let provisorio = false;
      if (!r.rp && control) {
        try { r.rp = rpProvisorio(db, control, color); provisorio = true; } catch (e) { r.error = e.message; continue; }
        const otra = db.prepare("SELECT rp, caravana_color FROM animales WHERE rp_provisorio=1 AND caravana_control=? AND upper(COALESCE(estado,'ACTIVO'))='ACTIVO'").all(control);
        if (otra.length) r.avisos.push(`ya hay ${otra.length} ternero(s) con control ${control} sin RP (${otra.map(o => o.rp + (o.caravana_color ? " " + o.caravana_color : "")).join(", ")}): al identificar, decí el color`);
      }
      if (!r.rp) { r.error = "Sin RP ni caravana control para el ternero"; continue; }
      if (!fe) { r.error = "Sin fecha de nacimiento (o no la entiendo)"; continue; }
      if (fe > hoyIso()) { r.error = `La fecha ${fe} es futura`; continue; }
      if (!sexo) { r.error = "Sin sexo (M/H)"; continue; }
      if (db.prepare("SELECT 1 FROM animales WHERE upper(rp)=upper(?)").get(r.rp)) { r.error = `Ya existe un animal con RP ${r.rp}`; continue; }
      let madre = null;
      if (r.madre) {
        madre = animalesMod.porRp(db, r.madre);
        if (!madre) r.avisos.push(`no encuentro a la madre ${r.madre}: queda anotada igual`);
        else {
          r.madre = madre.rp;
          if (!String(madre.sexo || "").toUpperCase().startsWith("H")) r.avisos.push(`${madre.rp} figura como macho`);
          const otra = db.prepare("SELECT rp, fecha_nac FROM animales WHERE upper(madre_rp)=upper(?) AND substr(fecha_nac,1,4)=?").get(madre.rp, fe.slice(0, 4));
          if (otra && Math.abs(dias(otra.fecha_nac, fe)) > 3) r.avisos.push(`${madre.rp} ya tiene cría este año: ${otra.rp} (${otra.fecha_nac})`);
          else if (otra) r.avisos.push(`${madre.rp} ya tiene ${otra.rp} nacido el mismo día: ¿mellizos?`);
        }
      } else r.avisos.push("sin madre");
      if (pn != null && (pn < 12 || pn > 70)) r.avisos.push(`peso al nacer raro: ${pn}`);
      if (pn == null) r.avisos.push("sin peso al nacer");
      const padre = String(f.padre_rp || f.padre || "").trim() || null;
      r.ok = true; r.fecha_nac = fe; r.sexo = sexo; r.peso_nac = pn; r.padre = padre; r.provisorio = provisorio;
      if (!simular) {
        const id = insA.run(r.rp, f.chip || null, sexo, sexo === "M" ? "TERNERO" : "TERNERA", fe, peloNorm(f.pelo || f.pelaje),
          f.raza || "A. ANGUS", madre ? madre.rp : (r.madre || null), padre, f.observaciones || f.obs || f.notas || null,
          control, color ? String(color).toUpperCase() : null, provisorio ? 1 : 0).lastInsertRowid;
        if (pn != null) insP.run(id, fe, pn);
        r.hecho = true;
      }
    }
  };
  if (simular) hacer(); else db.transaction(hacer)();
  return resumen(out, simular, "nacimiento");
}

// ── IDENTIFICAR ──────────────────────────────────────────────────────────────
// El paso del medio: al ternero que nació con caravana control se le asigna
// el RP definitivo y, cuando se lo pone, el chip. Todo lo que ya tenía
// (pesadas, notas, madre) sigue con él.
//   filas: [{ control?, color?, rp_actual?, rp?, chip? }]
//   · control (+ color si hay dos iguales) o rp_actual dicen de quién se trata.
//   · rp es el definitivo; chip la caravana electrónica. Puede venir uno solo.

function encontrarParaIdentificar(db, f) {
  const control = String(f.control || f.caravana_control || "").trim();
  const color = String(f.color || f.caravana_color || "").trim().toUpperCase();
  const actual = String(f.rp_actual || f.actual || "").trim();
  if (actual) {
    const a = animalesMod.porRp(db, actual);
    return a ? { animal: a } : { error: `No existe el animal ${actual}` };
  }
  if (!control) return { error: "Falta la caravana control (o el RP actual) para saber de quién se trata" };
  let cands = db.prepare(`SELECT * FROM animales WHERE caravana_control=? AND upper(COALESCE(estado,'ACTIVO'))='ACTIVO' ORDER BY rp_provisorio DESC, id DESC`).all(control.replace(/^C/i, ""));
  if (!cands.length) {
    // Quizá escribieron el RP provisorio ("C150") o un control con la C adelante.
    const a = animalesMod.porRp(db, /^C/i.test(control) ? control : "C" + control);
    if (a) cands = [a];
  }
  if (!cands.length) return { error: `No hay ningún ternero con caravana control ${control}` };
  if (cands.length > 1) {
    const porColor = color ? cands.filter(c => String(c.caravana_color || "").toUpperCase().startsWith(color.slice(0, 3))) : [];
    if (porColor.length === 1) return { animal: porColor[0] };
    return { error: `Hay ${cands.length} con control ${control}: ${cands.map(c => `${c.rp}${c.caravana_color ? " (" + c.caravana_color + ")" : ""}, nacido ${c.fecha_nac}`).join("; ")}. Decime el color o el RP provisorio.` };
  }
  return { animal: cands[0] };
}

function identificar(db, { filas, simular, usuario }) {
  const out = [];
  const existeRp = rp => db.prepare("SELECT rp FROM animales WHERE upper(rp)=upper(?)").get(rp);
  const hacer = () => {
    for (const [i, f] of (filas || []).entries()) {
      const r = { fila: i + 1, control: f.control || f.caravana_control || null, rp_nuevo: String(f.rp || f.rp_nuevo || "").trim() || null,
        chip: String(f.chip || "").trim() || null, ok: false, avisos: [] };
      out.push(r);
      if (!r.rp_nuevo && !r.chip) { r.error = "No hay nada para asignar: falta el RP definitivo o el chip"; continue; }
      const h = encontrarParaIdentificar(db, f);
      if (h.error) { r.error = h.error; continue; }
      const a = h.animal;
      r.rp_actual = a.rp; r.era_provisorio = !!a.rp_provisorio; r.categoria = a.categoria;
      if (r.rp_nuevo) {
        if (r.rp_nuevo.toUpperCase() === String(a.rp).toUpperCase()) { r.avisos.push("ya tenía ese RP"); r.rp_nuevo = null; }
        else {
          const otro = existeRp(r.rp_nuevo);
          if (otro) { r.error = `El RP ${r.rp_nuevo} ya lo tiene otro animal`; continue; }
          if (!a.rp_provisorio) r.avisos.push(`${a.rp} ya tenía RP definitivo: se cambia a ${r.rp_nuevo}`);
        }
      }
      if (r.chip) {
        const otro = db.prepare("SELECT rp FROM animales WHERE chip=? AND id<>?").get(r.chip, a.id);
        if (otro) { r.error = `El chip ${r.chip} ya está en el animal ${otro.rp}`; continue; }
        if (a.chip && a.chip !== r.chip) r.avisos.push(`tenía el chip ${a.chip}: se reemplaza`);
        if (!/^\d{12,16}$/.test(r.chip)) r.avisos.push("el chip no tiene el largo habitual (15 dígitos)");
      }
      r.ok = true;
      if (!simular) {
        const viejo = a.rp;
        if (r.rp_nuevo) {
          db.prepare("UPDATE animales SET rp=?, rp_provisorio=0 WHERE id=?").run(r.rp_nuevo, a.id);
          // Lo que lo nombraba por el RP viejo sigue con él.
          for (const [tabla, col] of [["notas_campo", "animal_rp"], ["destinos", "animal_rp"], ["animales", "madre_rp"], ["animales", "padre_rp"]]) {
            try { db.prepare(`UPDATE ${tabla} SET ${col}=? WHERE upper(${col})=upper(?)`).run(r.rp_nuevo, viejo); } catch (e) {}
          }
        }
        if (r.chip) db.prepare("UPDATE animales SET chip=? WHERE id=?").run(r.chip, a.id);
        r.hecho = true;
      }
    }
  };
  if (simular) hacer(); else db.transaction(hacer)();
  const res = resumen(out, simular, "identificación");
  res.mensaje = res.mensaje.replace("identificaciónes", "identificaciones");
  return res;
}

// ── MEDICIONES ───────────────────────────────────────────────────────────────
// Condición corporal, circunferencia escrotal, altura, frame… lo que se mida.

function mediciones(db, { filas, tipo, fecha, simular }) {
  const fe0 = fechaIso(fecha) || hoyIso();
  const t0 = String(tipo || "").toUpperCase().trim();
  const ins = db.prepare("INSERT INTO mediciones (animal_id, fecha, tipo, valor) VALUES (?,?,?,?)");
  const out = [];
  const hacer = () => {
    for (const [i, f] of (filas || []).entries()) {
      const r = { fila: i + 1, rp: String(f.rp || "").trim(), ok: false, avisos: [] };
      out.push(r);
      const t = String(f.tipo || t0).toUpperCase().trim();
      const v = numero(f.valor != null ? f.valor : f.peso);
      const fe = fechaIso(f.fecha) || fe0;
      if (!t) { r.error = "Sin tipo de medición"; continue; }
      const a = animalesMod.porRp(db, r.rp);
      if (!a) { r.error = `No existe el animal ${r.rp}`; continue; }
      if (v == null) { r.error = "Sin valor"; continue; }
      if (t === "CC" && (v < 1 || v > 9)) r.avisos.push("condición corporal fuera de 1-9");
      r.rp = a.rp; r.tipo = t; r.valor = v; r.fecha = fe; r.ok = true;
      if (!simular) { ins.run(a.id, fe, t, v); r.hecho = true; }
    }
  };
  if (simular) hacer(); else db.transaction(hacer)();
  return resumen(out, simular, "medición");
}

// ── NOTAS EN LOTE ────────────────────────────────────────────────────────────

function notas(db, plantelMod, { filas, fecha, simular, usuario }) {
  const out = [];
  const hacer = () => {
    for (const [i, f] of (filas || []).entries()) {
      const r = { fila: i + 1, rp: String(f.rp || "").trim(), texto: String(f.texto || f.nota || "").trim(), ok: false, avisos: [] };
      out.push(r);
      const a = animalesMod.porRp(db, r.rp);
      if (!a) { r.error = `No existe el animal ${r.rp}`; continue; }
      if (!r.texto) { r.error = "Sin texto"; continue; }
      r.rp = a.rp; r.ok = true;
      const señales = plantelMod.SENALES.filter(s => s.re.test(r.texto));
      if (señales.some(s => s.grave)) r.avisos.push(`se entiende como ${plantelMod.textoCausa(señales.find(s => s.grave).causa)}: la vaca no desteta esta temporada`);
      if (!simular) { plantelMod.guardarNota(db, a.rp, r.texto, { fecha: fechaIso(f.fecha || fecha) || hoyIso(), usuario }); r.hecho = true; }
    }
  };
  if (simular) hacer(); else db.transaction(hacer)();
  return resumen(out, simular, "nota");
}

// ── CSV DE AFUERA ────────────────────────────────────────────────────────────
// Una planilla exportada de otro sistema, de una balanza o de la propia
// planilla de relevamiento que emite RODEO. Se detecta el separador, se
// reconocen los encabezados por sinónimos, y el resto lo hacen las funciones
// de arriba.

function parsearCsv(texto) {
  let t = String(texto || "").replace(/^﻿/, "");
  const primera = t.split(/\r?\n/)[0] || "";
  const sep = [";", ",", "\t", "|"].map(s => [s, (primera.match(new RegExp(s === "|" ? "\\|" : s, "g")) || []).length]).sort((a, b) => b[1] - a[1])[0][0];
  const filas = [];
  let fila = [], campo = "", enComillas = false;
  for (let i = 0; i < t.length; i++) {
    const ch = t[i];
    if (enComillas) {
      if (ch === '"' && t[i + 1] === '"') { campo += '"'; i++; }
      else if (ch === '"') enComillas = false;
      else campo += ch;
    } else if (ch === '"') enComillas = true;
    else if (ch === sep) { fila.push(campo); campo = ""; }
    else if (ch === "\n" || ch === "\r") {
      if (ch === "\r" && t[i + 1] === "\n") i++;
      fila.push(campo); campo = "";
      if (fila.some(c => c.trim() !== "")) filas.push(fila);
      fila = [];
    } else campo += ch;
  }
  fila.push(campo);
  if (fila.some(c => c.trim() !== "")) filas.push(fila);
  if (!filas.length) return { sep, encabezados: [], filas: [] };
  const encabezados = filas[0].map(h => h.trim());
  return { sep, encabezados, filas: filas.slice(1).map(r => Object.fromEntries(encabezados.map((h, i) => [h, (r[i] || "").trim()]))) };
}

// Cómo puede venir escrito cada campo.
const SINONIMOS = {
  rp: ["rp", "numero", "número", "nro", "id", "animal", "identificacion", "identificación", "tag", "rp ternero", "ternero", "rp definitivo", "rp nuevo"],
  caravana_control: ["caravana_control", "caravana control", "control", "caravana_numero", "caravana numero", "caravana", "nro caravana", "numero caravana", "número caravana"],
  caravana_color: ["caravana_color", "caravana color", "color caravana", "color de caravana", "color"],
  rp_actual: ["rp_actual", "rp actual", "rp provisorio", "provisorio", "rp viejo"],
  peso: ["peso", "kg", "kilos", "peso kg", "peso_kg", "pesada", "peso actual", "ultimo peso", "último peso"],
  fecha: ["fecha", "date", "fecha pesada", "fecha_pesada", "dia", "día"],
  contexto: ["contexto", "tipo", "evento", "momento"],
  madre_rp: ["madre", "madre_rp", "madre rp", "rp madre", "vaca", "mother"],
  padre_rp: ["padre", "padre_rp", "padre rp", "rp padre", "toro", "semen", "sire"],
  fecha_nac: ["fecha_nacimiento", "fecha nacimiento", "fecha nac", "fecha_nac", "nacimiento", "nacio", "nació", "fecha de nacimiento"],
  sexo: ["sexo", "sex", "s"],
  pelo: ["pelo", "pelaje", "color", "pelaje ternero"],
  peso_nac: ["peso_nac", "peso nac", "peso nacimiento", "peso_nac_kg", "peso al nacer", "pn", "peso nacer"],
  chip: ["chip", "electronica", "electrónica", "caravana electronica", "caravana electrónica", "eid", "rfid", "bolo"],
  observaciones: ["observaciones", "obs", "nota", "notas", "comentario", "comentarios", "texto"],
  producto: ["producto", "vacuna", "medicamento", "droga"],
  dosis: ["dosis", "ml", "cantidad"],
  motivo: ["motivo", "razon", "razón", "campaña", "campana"],
  tipo: ["tipo", "medicion", "medición", "medida"],
  valor: ["valor", "medida", "cc", "ce", "altura", "frame"]
};
const normEnc = h => String(h || "").toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "").replace(/[^a-z0-9]+/g, " ").trim();

function mapearEncabezados(encabezados, campos) {
  const mapa = {};
  const usados = new Set();
  for (const campo of campos) {
    const opciones = (SINONIMOS[campo] || [campo]).map(normEnc);
    // Primero el que coincide exacto; si no, el que empieza igual.
    let hit = encabezados.find(h => !usados.has(h) && opciones.includes(normEnc(h)))
      || encabezados.find(h => !usados.has(h) && opciones.some(o => o.length >= 3 && normEnc(h).startsWith(o)));
    if (hit) { mapa[campo] = hit; usados.add(hit); }
  }
  return mapa;
}

const CAMPOS_POR_TIPO = {
  pesadas: ["rp", "peso", "fecha", "contexto"],
  nacimientos: ["rp", "caravana_control", "caravana_color", "madre_rp", "fecha_nac", "sexo", "pelo", "peso_nac", "padre_rp", "chip", "observaciones"],
  identificar: ["caravana_control", "caravana_color", "rp_actual", "rp", "chip"],
  sanidad: ["rp", "fecha", "producto", "dosis", "motivo"],
  mediciones: ["rp", "fecha", "tipo", "valor"],
  notas: ["rp", "fecha", "observaciones"]
};

/**
 * Importa un CSV. `tipo` dice qué es; si no viene, se adivina por los encabezados.
 * Con `mapa` se puede forzar qué columna es cada campo: { rp: "Caravana", peso: "Kg" }.
 */
function importarCsv(db, plantelMod, { texto, tipo, mapa, fecha, contexto, producto, dosis, motivo, simular, usuario }) {
  const csv = parsearCsv(texto);
  if (!csv.filas.length) throw new Error("El archivo no tiene filas de datos");
  const enc = csv.encabezados;
  let t = tipo;
  if (!t) {
    const m = mapearEncabezados(enc, ["rp", "peso", "madre_rp", "fecha_nac", "sexo", "producto", "valor", "observaciones"]);
    t = (m.madre_rp || m.fecha_nac) ? "nacimientos" : m.producto ? "sanidad" : m.peso ? "pesadas" : m.valor ? "mediciones" : m.observaciones ? "notas" : "pesadas";
  }
  if (!CAMPOS_POR_TIPO[t]) throw new Error(`Tipo "${t}" no. Puede ser: ${Object.keys(CAMPOS_POR_TIPO).join(", ")}`);
  const m = { ...mapearEncabezados(enc, CAMPOS_POR_TIPO[t]), ...(mapa || {}) };
  if (!m.rp && !(t === "nacimientos" && m.caravana_control) && !(t === "identificar" && (m.caravana_control || m.rp_actual)))
    throw new Error(`No encuentro la columna del RP. Encabezados: ${enc.join(", ")}`);
  const filas = csv.filas.map(f => Object.fromEntries(Object.entries(m).map(([campo, col]) => [campo, f[col]])));
  const base = { encabezados: enc, separador: csv.sep, tipo: t, mapa: m, leidas: csv.filas.length };

  let r;
  if (t === "pesadas") r = pesadas(db, { filas, fecha, contexto, simular, usuario });
  else if (t === "nacimientos") r = nacimientos(db, { filas, simular, usuario });
  else if (t === "sanidad") r = sanidad(db, { rps: filas.map(f => f.rp), fecha: fecha || filas[0].fecha, producto: producto || filas[0].producto, dosis: dosis || filas[0].dosis, motivo: motivo || filas[0].motivo, simular });
  else if (t === "identificar") r = identificar(db, { filas: filas.map(f => ({ control: f.caravana_control, color: f.caravana_color, rp_actual: f.rp_actual, rp: f.rp, chip: f.chip })), simular, usuario });
  else if (t === "mediciones") r = mediciones(db, { filas, fecha, simular });
  else r = notas(db, plantelMod, { filas: filas.map(f => ({ rp: f.rp, texto: f.observaciones, fecha: f.fecha })), fecha, simular, usuario });
  return { ...base, ...r };
}

// ── PLANILLA PARA LLEVAR AL CAMPO ────────────────────────────────────────────
// Los animales que se van a revisar, con columnas vacías para anotar. Sale en
// Excel (para la tablet) o imprimible (para la libreta). Lo que se llena
// vuelve por importarCsv.

function planilla(db, { rps, lote_id, conjunto, columnas, titulo, campoNombre, formato = "xlsx" }, exportarMod, mods) {
  let animales;
  if (lote_id) animales = db.prepare(`SELECT a.* FROM lote_animales la JOIN animales a ON a.id=la.animal_id WHERE la.lote_id=? ORDER BY a.rp`).all(lote_id);
  else if (Array.isArray(rps) && rps.length) animales = rps.map(rp => animalesMod.porRp(db, rp)).filter(Boolean);
  else if (conjunto) animales = exportarMod.conjunto(db, mods, conjunto).filas.map(f => animalesMod.porRp(db, f.rp || f.animal_rp)).filter(Boolean);
  else animales = db.prepare("SELECT * FROM animales WHERE upper(COALESCE(estado,'ACTIVO'))='ACTIVO' ORDER BY rp").all();

  const ult = db.prepare("SELECT peso, fecha FROM pesadas WHERE animal_id=? ORDER BY fecha DESC, id DESC LIMIT 1");
  const extras = (columnas && columnas.length ? columnas : ["Peso", "CC", "Observaciones"]).map(c => ({ k: "_" + normEnc(c).replace(/ /g, "_"), t: c, ancho: c.length > 8 ? 26 : 12 }));
  const cols = [
    { k: "rp", t: "RP", ancho: 10 }, { k: "chip", t: "Caravana elec.", ancho: 16 }, { k: "categoria", t: "Categoría" },
    { k: "sexo", t: "Sexo", ancho: 6 }, { k: "pelo", t: "Pelaje" }, { k: "ultimo_peso", t: "Último peso", tipo: "decimal" },
    { k: "fecha_ultimo", t: "Fecha", tipo: "fecha" }, { k: "fecha", t: "Fecha relev.", ancho: 12 }, ...extras
  ];
  const filas = animales.map(a => { const u = ult.get(a.id) || {}; return { rp: a.rp, chip: a.chip, categoria: a.categoria, sexo: a.sexo, pelo: a.pelo, ultimo_peso: u.peso, fecha_ultimo: u.fecha, fecha: null }; });
  const tit = titulo || "Planilla de relevamiento";
  const sub = `${filas.length} animales · ${campoNombre || ""} · emitida el ${hoyIso().split("-").reverse().join("/")}`;
  const nombre = exportarMod.nombreArchivo(tit, formato === "html" ? "html" : "xlsx");
  if (formato === "html") return { buffer: Buffer.from(exportarMod.htmlImprimible({ titulo: tit, subtitulo: sub, columnas: cols, filas, campoNombre }), "utf8"), mime: "text/html; charset=utf-8", nombre, inline: true };
  return { buffer: xlsx.armar([{ nombre: "Relevamiento", titulo: tit, subtitulo: sub, columnas: cols, filas }]), mime: xlsx.MIME, nombre };
}

// ── RESUMEN ──────────────────────────────────────────────────────────────────

function resumen(out, simular, que) {
  const ok = out.filter(r => r.ok), mal = out.filter(r => !r.ok), conAviso = out.filter(r => r.ok && r.avisos && r.avisos.length);
  const plural = n => n === 1 ? que : que.endsWith("n") ? que.slice(0, -1) + "nes" : que + "s";
  return {
    ok: ok.length > 0, simulado: !!simular, total: out.length, bien: ok.length, mal: mal.length, con_avisos: conAviso.length,
    filas: out,
    mensaje: simular
      ? `${ok.length} ${plural(ok.length)} para cargar${mal.length ? `, ${mal.length} con error` : ""}${conAviso.length ? `, ${conAviso.length} con avisos` : ""}. Revisá y confirmá.`
      : `${ok.length} ${plural(ok.length)} cargada${ok.length === 1 ? "" : "s"}${mal.length ? `. ${mal.length} no: ${mal.slice(0, 5).map(r => `${r.rp || "fila " + r.fila} (${r.error})`).join(", ")}${mal.length > 5 ? "…" : ""}` : ""}.`
  };
}

module.exports = { pesadas, sanidad, nacimientos, identificar, mediciones, notas, importarCsv, parsearCsv, parsearLineas, planilla, rpProvisorio, fechaIso, numero, SINONIMOS, CAMPOS_POR_TIPO };
