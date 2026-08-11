// ─────────────────────────────────────────────────────────────────────────────
// REPRODUCCIÓN — protocolo de servicio en etapas
//
// Módulo nuevo, con tablas propias. No toca `servicios` ni nada de lo anterior:
// los datos viejos siguen donde están y este esquema arranca de cero.
//
// Las cuatro etapas, en orden:
//   1. PROGRAMACIÓN   toros de IATF y de repaso, con RP, nombre y registro
//   2. EJECUCIÓN      sincronización, IATF, entrada y salida del toro
//   3. ECO PRECOZ     confirma la preñez de IATF
//   4. ECO FINAL      cierra el servicio y adjudica el toro
//
// Al nacer, el padre se asigna solo: si el ternero llega hasta 15 días después
// de la fecha probable de la IATF, es del toro de IATF; si llega más tarde, es
// del repaso. Siempre se puede corregir a mano, y queda registrado quién lo
// cambió y por qué.
// ─────────────────────────────────────────────────────────────────────────────

const GESTACION = 283;
const MARGEN_IATF = 15;      // días después de la FPP que siguen siendo de IATF
const PREMATURO = 20;        // días antes de la FPP que aún se aceptan

function sumarDias(fecha, dias) {
  const d = new Date(fecha);
  d.setDate(d.getDate() + dias);
  return d.toISOString().slice(0, 10);
}

function diasEntre(a, b) {
  return Math.round((new Date(b) - new Date(a)) / 86400000);
}

// ── ESQUEMA ──────────────────────────────────────────────────────────────────

function init(db) {
  db.exec(`
    -- Padres disponibles: semen de IATF o toros de repaso.
    CREATE TABLE IF NOT EXISTS repro_toros (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      rp TEXT NOT NULL,
      nombre TEXT NOT NULL,
      hbu TEXT,
      hba TEXT,
      raza TEXT,
      tipo TEXT NOT NULL DEFAULT 'REPASO',   -- IATF | REPASO | AMBOS
      activo INTEGER DEFAULT 1,
      notas TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      UNIQUE(rp, nombre)
    );

    -- Un protocolo por temporada y grupo de vacas.
    CREATE TABLE IF NOT EXISTS repro_protocolos (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      nombre TEXT NOT NULL,
      temporada TEXT NOT NULL,
      etapa TEXT NOT NULL DEFAULT 'PROGRAMACION',
      -- 1. Programación
      toro_iatf_id INTEGER REFERENCES repro_toros(id),
      toro_repaso_id INTEGER REFERENCES repro_toros(id),
      -- 2. Ejecución
      fecha_sincronizacion TEXT,
      fecha_iatf TEXT,
      fecha_ingreso_toro TEXT,
      fecha_salida_toro TEXT,
      -- 3 y 4. Ecografías
      fecha_eco_precoz TEXT,
      fecha_eco_final TEXT,
      cerrado INTEGER DEFAULT 0,
      notas TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    -- Cada vaca dentro de un protocolo.
    CREATE TABLE IF NOT EXISTS repro_vacas (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      protocolo_id INTEGER NOT NULL REFERENCES repro_protocolos(id) ON DELETE CASCADE,
      animal_id INTEGER NOT NULL,
      rp TEXT NOT NULL,
      cc_pre REAL,
      -- eco precoz: confirma la IATF
      eco_precoz TEXT,           -- PREÑADA | VACIA
      -- eco final: cierra el servicio
      eco_final TEXT,            -- PREÑADA | VACIA
      origen_prenez TEXT,        -- IATF | REPASO | VACIA
      fpp TEXT,                  -- fecha probable de parto que corresponda
      parto_id INTEGER,
      notas TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      UNIQUE(protocolo_id, animal_id)
    );

    -- El parto. Peso, pelo, RP y madre son obligatorios.
    CREATE TABLE IF NOT EXISTS repro_partos (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      repro_vaca_id INTEGER REFERENCES repro_vacas(id),
      madre_rp TEXT NOT NULL,
      rp_cria TEXT NOT NULL,
      fecha TEXT NOT NULL,
      peso_nac REAL NOT NULL,
      pelo TEXT NOT NULL,
      sexo TEXT NOT NULL,
      padre_id INTEGER REFERENCES repro_toros(id),
      padre_origen TEXT,          -- IATF | REPASO
      padre_automatico INTEGER DEFAULT 1,
      padre_motivo TEXT,          -- por qué se asignó ese padre
      corregido_por TEXT,
      corregido_motivo TEXT,
      notas TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_repro_vacas_prot ON repro_vacas(protocolo_id);
    CREATE INDEX IF NOT EXISTS idx_repro_partos_madre ON repro_partos(madre_rp);
  `);
}

// ── 1. PROGRAMACIÓN ──────────────────────────────────────────────────────────

function guardarToro(db, t) {
  if (!t.nombre) throw new Error("El toro necesita nombre");
  const info = db.prepare(`INSERT INTO repro_toros (rp,nombre,hbu,hba,raza,tipo,notas)
    VALUES (?,?,?,?,?,?,?)
    ON CONFLICT(rp,nombre) DO UPDATE SET
      hbu=excluded.hbu, hba=excluded.hba, raza=excluded.raza, tipo=excluded.tipo`)
    .run(t.rp || "", t.nombre, t.hbu || null, t.hba || null, t.raza || null,
         ["IATF", "REPASO", "AMBOS"].includes(t.tipo) ? t.tipo : "REPASO", t.notas || null);
  return info.lastInsertRowid ||
    db.prepare("SELECT id FROM repro_toros WHERE rp=? AND nombre=?").get(t.rp || "", t.nombre).id;
}

function crearProtocolo(db, p) {
  if (!p.nombre || !p.temporada) throw new Error("Faltan nombre y temporada");
  const info = db.prepare(`INSERT INTO repro_protocolos (nombre,temporada,toro_iatf_id,toro_repaso_id,notas)
    VALUES (?,?,?,?,?)`).run(p.nombre, p.temporada, p.toro_iatf_id || null, p.toro_repaso_id || null, p.notas || null);
  return info.lastInsertRowid;
}

// Etapa 1 completa cuando hay al menos un toro definido con sus datos.
function validarProgramacion(db, protocoloId) {
  const p = db.prepare("SELECT * FROM repro_protocolos WHERE id=?").get(protocoloId);
  if (!p) throw new Error("Protocolo inexistente");
  const faltan = [];
  if (!p.toro_iatf_id && !p.toro_repaso_id) faltan.push("al menos un toro (IATF o repaso)");
  [["toro_iatf_id", "de IATF"], ["toro_repaso_id", "de repaso"]].forEach(([campo, quien]) => {
    if (!p[campo]) return;
    const t = db.prepare("SELECT * FROM repro_toros WHERE id=?").get(p[campo]);
    if (!t) { faltan.push(`el toro ${quien} no existe`); return; }
    if (!t.rp) faltan.push(`RP del toro ${quien}`);
    if (!t.hbu && !t.hba) faltan.push(`registro (HBU o HBA) del toro ${quien}`);
  });
  return { ok: !faltan.length, faltan };
}

function agregarVacas(db, protocoloId, animales) {
  const ins = db.prepare(`INSERT OR IGNORE INTO repro_vacas (protocolo_id,animal_id,rp,cc_pre) VALUES (?,?,?,?)`);
  let n = 0;
  const correr = db.transaction(() => {
    for (const a of animales) { if (ins.run(protocoloId, a.id, a.rp, a.cc_pre || null).changes) n++; }
  });
  correr();
  return n;
}

// ── 2. EJECUCIÓN ─────────────────────────────────────────────────────────────

function registrarEjecucion(db, protocoloId, datos) {
  const v = validarProgramacion(db, protocoloId);
  if (!v.ok) return { ok: false, error: `Antes de ejecutar falta cargar: ${v.faltan.join(", ")}` };

  const campos = ["fecha_sincronizacion", "fecha_iatf", "fecha_ingreso_toro", "fecha_salida_toro"];
  const sets = [], vals = [];
  campos.forEach(c => { if (datos[c]) { sets.push(`${c}=?`); vals.push(datos[c]); } });
  if (!sets.length) return { ok: false, error: "No mandaste ninguna fecha" };

  const p = db.prepare("SELECT * FROM repro_protocolos WHERE id=?").get(protocoloId);
  const fIatf = datos.fecha_iatf || p.fecha_iatf;
  const fIng = datos.fecha_ingreso_toro || p.fecha_ingreso_toro;
  if (fIatf && fIng && diasEntre(fIatf, fIng) < 0) {
    return { ok: false, error: "El toro de repaso no puede entrar antes de la IATF" };
  }
  const fSal = datos.fecha_salida_toro || p.fecha_salida_toro;
  if (fIng && fSal && diasEntre(fIng, fSal) < 0) {
    return { ok: false, error: "El toro no puede salir antes de entrar" };
  }

  sets.push("etapa=?"); vals.push("EJECUCION");
  vals.push(protocoloId);
  db.prepare(`UPDATE repro_protocolos SET ${sets.join(",")} WHERE id=?`).run(...vals);
  return { ok: true, mensaje: "Ejecución registrada." };
}

// ── 3. ECO PRECOZ ────────────────────────────────────────────────────────────
// Confirma qué vacas quedaron preñadas de la IATF.

function registrarEcoPrecoz(db, protocoloId, resultados, fecha) {
  const p = db.prepare("SELECT * FROM repro_protocolos WHERE id=?").get(protocoloId);
  if (!p) return { ok: false, error: "Protocolo inexistente" };
  if (!p.fecha_iatf) return { ok: false, error: "Todavía no hay fecha de IATF cargada" };

  const fppIatf = sumarDias(p.fecha_iatf, GESTACION);
  const upd = db.prepare(`UPDATE repro_vacas SET eco_precoz=?, origen_prenez=?, fpp=? WHERE protocolo_id=? AND upper(rp)=upper(?)`);
  let n = 0;
  const correr = db.transaction(() => {
    for (const r of resultados) {
      const prenada = /PRE/i.test(r.resultado);
      if (upd.run(prenada ? "PREÑADA" : "VACIA", prenada ? "IATF" : null,
                  prenada ? fppIatf : null, protocoloId, r.rp).changes) n++;
    }
  });
  correr();
  db.prepare("UPDATE repro_protocolos SET etapa='ECO_PRECOZ', fecha_eco_precoz=? WHERE id=?")
    .run(fecha || new Date().toISOString().slice(0, 10), protocoloId);
  return { ok: true, actualizadas: n, fpp_iatf: fppIatf };
}

// ── 4. ECO FINAL ─────────────────────────────────────────────────────────────
// Cierra el servicio: lo que no era de IATF y quedó preñado, es del repaso.

function registrarEcoFinal(db, protocoloId, resultados, fecha) {
  const p = db.prepare("SELECT * FROM repro_protocolos WHERE id=?").get(protocoloId);
  if (!p) return { ok: false, error: "Protocolo inexistente" };

  const fppIatf = p.fecha_iatf ? sumarDias(p.fecha_iatf, GESTACION) : null;
  // Sin fecha de salida, se estima el parto del repaso desde la mitad del período.
  const baseRepaso = p.fecha_ingreso_toro && p.fecha_salida_toro
    ? sumarDias(p.fecha_ingreso_toro, Math.round(diasEntre(p.fecha_ingreso_toro, p.fecha_salida_toro) / 2))
    : p.fecha_ingreso_toro;
  const fppRepaso = baseRepaso ? sumarDias(baseRepaso, GESTACION) : null;

  let iatf = 0, repaso = 0, vacias = 0;
  const correr = db.transaction(() => {
    for (const r of resultados) {
      const v = db.prepare("SELECT * FROM repro_vacas WHERE protocolo_id=? AND upper(rp)=upper(?)")
        .get(protocoloId, r.rp);
      if (!v) continue;
      const prenada = /PRE/i.test(r.resultado);
      if (!prenada) {
        db.prepare("UPDATE repro_vacas SET eco_final='VACIA', origen_prenez='VACIA', fpp=NULL WHERE id=?").run(v.id);
        vacias++;
        continue;
      }
      // Si la precoz ya la había confirmado, la preñez es de la IATF.
      const esIatf = v.eco_precoz === "PREÑADA";
      db.prepare("UPDATE repro_vacas SET eco_final='PREÑADA', origen_prenez=?, fpp=? WHERE id=?")
        .run(esIatf ? "IATF" : "REPASO", esIatf ? fppIatf : fppRepaso, v.id);
      esIatf ? iatf++ : repaso++;
    }
  });
  correr();

  db.prepare("UPDATE repro_protocolos SET etapa='CERRADO', fecha_eco_final=?, cerrado=1 WHERE id=?")
    .run(fecha || new Date().toISOString().slice(0, 10), protocoloId);

  return { ok: true, iatf, repaso, vacias, fpp_iatf: fppIatf, fpp_repaso: fppRepaso };
}

// ── PADRE AUTOMÁTICO ─────────────────────────────────────────────────────────
// Hasta 15 días después de la fecha probable de la IATF, la cría es del toro de
// IATF. Más tarde, del repaso. Se devuelve siempre el motivo, para que quede
// claro por qué y se pueda discutir.

function decidirPadre(db, protocolo, fechaParto) {
  const r = { padre_id: null, origen: null, motivo: "", revisar: false };
  if (!protocolo) { r.motivo = "Sin protocolo asociado"; r.revisar = true; return r; }

  if (protocolo.fecha_iatf) {
    const fppIatf = sumarDias(protocolo.fecha_iatf, GESTACION);
    const dif = diasEntre(fppIatf, fechaParto);
    if (dif >= -PREMATURO && dif <= MARGEN_IATF) {
      r.padre_id = protocolo.toro_iatf_id;
      r.origen = "IATF";
      r.motivo = dif === 0
        ? `Nació justo en la fecha probable de la IATF (${fppIatf}).`
        : `Nació ${Math.abs(dif)} días ${dif > 0 ? "después" : "antes"} de la fecha probable de la IATF (${fppIatf}), dentro del margen de ${MARGEN_IATF} días.`;
      if (!r.padre_id) { r.motivo += " Ojo: el protocolo no tiene toro de IATF cargado."; r.revisar = true; }
      return r;
    }
    if (dif < -PREMATURO) {
      r.origen = null;
      r.motivo = `Nació ${Math.abs(dif)} días antes de la fecha probable de la IATF (${fppIatf}): demasiado prematuro para ser de ese servicio.`;
      r.revisar = true;
      return r;
    }
  }

  if (protocolo.toro_repaso_id) {
    r.padre_id = protocolo.toro_repaso_id;
    r.origen = "REPASO";
    const fppIatf = protocolo.fecha_iatf ? sumarDias(protocolo.fecha_iatf, GESTACION) : null;
    r.motivo = fppIatf
      ? `Nació ${diasEntre(fppIatf, fechaParto)} días después de la fecha probable de la IATF (${fppIatf}), más allá del margen de ${MARGEN_IATF}: corresponde al toro de repaso.`
      : `El protocolo no tuvo IATF: corresponde al toro de repaso.`;

    if (protocolo.fecha_ingreso_toro) {
      const dias = diasEntre(protocolo.fecha_ingreso_toro, fechaParto);
      if (dias < GESTACION - 30) {
        r.motivo += ` Pero pasaron sólo ${dias} días desde que entró el toro: revisalo.`;
        r.revisar = true;
      }
    }
    return r;
  }

  r.motivo = "No hay toro de repaso cargado en el protocolo.";
  r.revisar = true;
  return r;
}

// ── REGISTRAR PARTO ──────────────────────────────────────────────────────────
// Peso, pelo, RP y madre son obligatorios: sin eso no se guarda.

function registrarParto(db, datos) {
  const faltan = [];
  if (!datos.madre_rp) faltan.push("RP de la madre");
  if (!datos.rp_cria) faltan.push("RP de la cría");
  if (!datos.fecha) faltan.push("fecha de parto");
  if (datos.peso_nac === undefined || datos.peso_nac === null || datos.peso_nac === "") faltan.push("peso al nacer");
  if (!datos.pelo) faltan.push("pelo");
  if (!datos.sexo) faltan.push("sexo");
  if (faltan.length) return { ok: false, error: `Falta ${faltan.join(", ")}.`, faltan };

  const peso = parseFloat(datos.peso_nac);
  if (isNaN(peso) || peso <= 0 || peso > 90) {
    return { ok: false, error: `El peso al nacer (${datos.peso_nac}) no parece correcto.` };
  }

  const madre = db.prepare("SELECT * FROM animales WHERE upper(rp)=upper(?)").get(datos.madre_rp);
  if (!madre) return { ok: false, error: `No encuentro la madre ${datos.madre_rp} en el inventario.` };

  const yaExiste = db.prepare("SELECT id FROM animales WHERE upper(rp)=upper(?)").get(datos.rp_cria);
  if (yaExiste && !datos.permitir_existente) {
    return { ok: false, error: `Ya existe un animal con RP ${datos.rp_cria}.` };
  }

  // Protocolo abierto de esa vaca, para decidir el padre.
  const vaca = db.prepare(`
    SELECT rv.*, rp.* , rv.id AS repro_vaca_id
    FROM repro_vacas rv JOIN repro_protocolos rp ON rp.id = rv.protocolo_id
    WHERE rv.animal_id = ? AND rv.parto_id IS NULL
    ORDER BY rp.temporada DESC, rp.id DESC LIMIT 1
  `).get(madre.id);

  const protocolo = vaca ? db.prepare("SELECT * FROM repro_protocolos WHERE id=?").get(vaca.protocolo_id) : null;
  const decision = decidirPadre(db, protocolo, datos.fecha);
  const toro = decision.padre_id ? db.prepare("SELECT * FROM repro_toros WHERE id=?").get(decision.padre_id) : null;

  let partoId;
  const correr = db.transaction(() => {
    const info = db.prepare(`INSERT INTO repro_partos
      (repro_vaca_id, madre_rp, rp_cria, fecha, peso_nac, pelo, sexo, padre_id, padre_origen, padre_automatico, padre_motivo, notas)
      VALUES (?,?,?,?,?,?,?,?,?,1,?,?)`)
      .run(vaca ? vaca.repro_vaca_id : null, madre.rp, datos.rp_cria, datos.fecha, peso,
           datos.pelo, datos.sexo, decision.padre_id, decision.origen, decision.motivo, datos.notas || null);
    partoId = info.lastInsertRowid;

    if (vaca) db.prepare("UPDATE repro_vacas SET parto_id=? WHERE id=?").run(partoId, vaca.repro_vaca_id);

    if (!yaExiste) {
      db.prepare(`INSERT INTO animales (rp, fecha_nac, sexo, pelo, categoria, madre_rp, padre_rp, destino, fecha_ingreso, notas)
                  VALUES (?,?,?,?,'TERNERO',?,?,'PLANTEL',?,?)`)
        .run(datos.rp_cria, datos.fecha, datos.sexo, datos.pelo, madre.rp,
             toro ? (toro.rp || toro.nombre) : null,
             new Date().toISOString().slice(0, 10),
             decision.origen ? `Padre por ${decision.origen}` : null);
    }
    const cria = db.prepare("SELECT id FROM animales WHERE upper(rp)=upper(?)").get(datos.rp_cria);
    if (cria) {
      db.prepare("INSERT INTO pesadas (animal_id, fecha, peso, contexto) VALUES (?,?,?,'NACIMIENTO')")
        .run(cria.id, datos.fecha, peso);
    }
  });
  correr();

  return {
    ok: true, parto_id: partoId,
    padre: toro ? `${toro.nombre}${toro.rp ? ` (${toro.rp})` : ""}` : null,
    origen: decision.origen,
    motivo: decision.motivo,
    revisar: decision.revisar,
    mensaje: `✅ Parto de ${madre.rp}: ${datos.rp_cria}, ${datos.sexo}, ${datos.pelo}, ${peso} kg.` +
      (toro ? `\n👨 Padre: ${toro.nombre} (${decision.origen}).\n   ${decision.motivo}` : `\n⚠️ Sin padre asignado. ${decision.motivo}`) +
      (decision.revisar ? `\n\n🔍 Conviene revisarlo a mano.` : "")
  };
}

// ── CORRECCIÓN MANUAL DEL PADRE ──────────────────────────────────────────────

function corregirPadre(db, partoId, toroId, quien, motivo) {
  const parto = db.prepare("SELECT * FROM repro_partos WHERE id=?").get(partoId);
  if (!parto) return { ok: false, error: "No encuentro ese parto" };
  const toro = db.prepare("SELECT * FROM repro_toros WHERE id=?").get(toroId);
  if (!toro) return { ok: false, error: "No encuentro ese toro" };

  const anterior = parto.padre_id
    ? db.prepare("SELECT nombre FROM repro_toros WHERE id=?").get(parto.padre_id)?.nombre
    : "sin asignar";

  db.prepare(`UPDATE repro_partos SET padre_id=?, padre_origen=?, padre_automatico=0,
              corregido_por=?, corregido_motivo=? WHERE id=?`)
    .run(toroId, toro.tipo === "IATF" ? "IATF" : "REPASO", quien || "manual", motivo || null, partoId);

  db.prepare("UPDATE animales SET padre_rp=? WHERE upper(rp)=upper(?)")
    .run(toro.rp || toro.nombre, parto.rp_cria);

  return { ok: true, mensaje: `Padre de ${parto.rp_cria} cambiado de ${anterior} a ${toro.nombre}.` };
}

// ── ESTADO DEL PROTOCOLO ─────────────────────────────────────────────────────

const ETAPAS = ["PROGRAMACION", "EJECUCION", "ECO_PRECOZ", "CERRADO"];

function estado(db, protocoloId) {
  const p = db.prepare("SELECT * FROM repro_protocolos WHERE id=?").get(protocoloId);
  if (!p) return null;
  const toro = id => id ? db.prepare("SELECT * FROM repro_toros WHERE id=?").get(id) : null;
  const vacas = db.prepare("SELECT * FROM repro_vacas WHERE protocolo_id=?").all(protocoloId);
  const cont = t => vacas.filter(v => v.origen_prenez === t).length;

  const pendientes = [];
  if (!p.fecha_sincronizacion && !p.fecha_iatf) pendientes.push("cargar las fechas de sincronización e IATF");
  else if (!p.fecha_eco_precoz && p.fecha_iatf) pendientes.push("hacer la ecografía precoz");
  else if (!p.fecha_salida_toro && p.fecha_ingreso_toro) pendientes.push("registrar la salida del toro");
  else if (!p.fecha_eco_final) pendientes.push("hacer la ecografía final y cerrar el servicio");

  return {
    ...p,
    toro_iatf: toro(p.toro_iatf_id),
    toro_repaso: toro(p.toro_repaso_id),
    total_vacas: vacas.length,
    prenadas_iatf: cont("IATF"),
    prenadas_repaso: cont("REPASO"),
    vacias: cont("VACIA"),
    fpp_iatf: p.fecha_iatf ? sumarDias(p.fecha_iatf, GESTACION) : null,
    paso: ETAPAS.indexOf(p.etapa) + 1,
    de: ETAPAS.length,
    pendientes
  };
}

module.exports = {
  init, guardarToro, crearProtocolo, validarProgramacion, agregarVacas,
  registrarEjecucion, registrarEcoPrecoz, registrarEcoFinal,
  decidirPadre, registrarParto, corregirPadre, estado,
  GESTACION, MARGEN_IATF, sumarDias, diasEntre
};
