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
      producto_semen TEXT,            -- nombre en el stock del sistema financiero
      costo_sincro_vaca REAL,         -- protocolo de sincronización, por vaca
      costo_veterinario_vaca REAL,    -- honorarios, por vaca
      costo_cargado INTEGER DEFAULT 0,
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

    -- Insumos y trabajo de cada etapa. Los productos salen del stock del
    -- sistema financiero; el trabajo se carga con su precio. Al ejecutar la
    -- etapa, todo esto se descuenta y se imputa a cada vaca del protocolo.
    CREATE TABLE IF NOT EXISTS repro_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      protocolo_id INTEGER NOT NULL REFERENCES repro_protocolos(id) ON DELETE CASCADE,
      etapa TEXT NOT NULL,              -- SINCRO | IATF | TACTO1 | TACTO2
      tipo TEXT NOT NULL,               -- PRODUCTO | TRABAJO
      nombre TEXT NOT NULL,             -- nombre en el stock, o descripción del trabajo
      cantidad_vaca REAL DEFAULT 1,     -- por vaca
      precio_unitario REAL,             -- si es TRABAJO, o para pisar el del stock
      concepto TEXT,                    -- para la ficha de costos del animal
      ejecutado INTEGER DEFAULT 0,
      costo_real REAL,                  -- lo que efectivamente costó al ejecutar
      created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_repro_items_prot ON repro_items(protocolo_id, etapa);

    CREATE TABLE IF NOT EXISTS repro_etapas_hechas (
      protocolo_id INTEGER NOT NULL,
      etapa TEXT NOT NULL,
      fecha TEXT,
      vacas INTEGER,
      costo_vaca REAL,
      costo_total REAL,
      created_at TEXT DEFAULT (datetime('now')),
      PRIMARY KEY (protocolo_id, etapa)
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


// ── COSTOS ───────────────────────────────────────────────────────────────────
// El costo se carga en la vaca en el momento en que pasa la cosa, no repartido
// después: la que necesitó dos servicios cuesta más que la que preñó de una.
// El precio de la pajuela sale del stock del sistema financiero, que es donde
// están las compras reales — acá no se duplica ningún precio.

function cargarCostoVacas(db, protocoloId, concepto, detalle, montoPorVaca, fecha) {
  if (!(montoPorVaca > 0)) return 0;
  const vacas = db.prepare("SELECT animal_id FROM repro_vacas WHERE protocolo_id=?").all(protocoloId);
  const ins = db.prepare("INSERT INTO costos (animal_id,fecha,concepto,detalle,monto) VALUES (?,?,?,?,?)");
  const correr = db.transaction(() => {
    for (const v of vacas) ins.run(v.animal_id, fecha, concepto, detalle, montoPorVaca);
  });
  correr();
  return vacas.length;
}

// Ejecuta la IATF: descuenta las pajuelas del stock y carga el costo en cada
// vaca. `consumirStock` la inyecta el server, que es quien sabe hablar con el
// sistema financiero de cada empresa.
async function ejecutarIATF(db, protocoloId, datos, consumirStock) {
  const p = db.prepare("SELECT * FROM repro_protocolos WHERE id=?").get(protocoloId);
  if (!p) return { ok: false, error: "Protocolo inexistente" };
  const v = validarProgramacion(db, protocoloId);
  if (!v.ok) return { ok: false, error: `Falta cargar: ${v.faltan.join(", ")}` };

  const vacas = db.prepare("SELECT COUNT(*) n FROM repro_vacas WHERE protocolo_id=?").get(protocoloId).n;
  if (!vacas) return { ok: false, error: "El protocolo no tiene vacas cargadas" };
  if (p.costo_cargado) return { ok: false, error: "La IATF de este protocolo ya fue ejecutada" };

  const fecha = datos.fecha_iatf || new Date().toISOString().slice(0, 10);
  const producto = datos.producto_semen || p.producto_semen;
  const detalle = [];
  let costoPajuela = 0, avisoStock = null;

  if (producto && consumirStock) {
    const r = await consumirStock(producto, vacas, `IATF ${p.nombre} ${p.temporada}`, fecha);
    if (!r.ok) return { ok: false, error: `Stock: ${r.error}` };
    costoPajuela = r.costo_unitario || 0;
    avisoStock = r.aviso || null;
    detalle.push(`${vacas} pajuelas de ${r.producto} · quedan ${r.restante}`);
  }

  const sincro = parseFloat(datos.costo_sincro_vaca ?? p.costo_sincro_vaca) || 0;
  const vet = parseFloat(datos.costo_veterinario_vaca ?? p.costo_veterinario_vaca) || 0;

  db.prepare(`UPDATE repro_protocolos SET fecha_iatf=?, etapa='EJECUCION', producto_semen=?,
              costo_sincro_vaca=?, costo_veterinario_vaca=?, costo_cargado=1 WHERE id=?`)
    .run(fecha, producto || null, sincro || null, vet || null, protocoloId);

  const cargados = [];
  if (costoPajuela > 0) {
    cargarCostoVacas(db, protocoloId, "SEMEN", `IATF ${p.temporada} · ${producto}`, costoPajuela, fecha);
    cargados.push(`semen ${costoPajuela.toFixed(2)}`);
  }
  if (sincro > 0) {
    cargarCostoVacas(db, protocoloId, "SANIDAD", `Sincronización IATF ${p.temporada}`, sincro, fecha);
    cargados.push(`sincronización ${sincro.toFixed(2)}`);
  }
  if (vet > 0) {
    cargarCostoVacas(db, protocoloId, "VETERINARIO", `IATF ${p.temporada}`, vet, fecha);
    cargados.push(`veterinario ${vet.toFixed(2)}`);
  }

  const porVaca = costoPajuela + sincro + vet;
  return {
    ok: true, vacas, costo_por_vaca: porVaca, costo_total: porVaca * vacas,
    aviso: avisoStock,
    mensaje: `✅ IATF del ${fecha} en ${vacas} vacas.` +
      (detalle.length ? `\n📦 ${detalle.join(" · ")}` : "") +
      (cargados.length ? `\n💰 US$ ${porVaca.toFixed(2)} por vaca (${cargados.join(", ")}) — total US$ ${(porVaca * vacas).toFixed(2)}` : "") +
      (avisoStock ? `\n${avisoStock}` : "")
  };
}


// ── ETAPAS CON COSTO ─────────────────────────────────────────────────────────
// Cada etapa tiene sus productos (que salen del stock del financiero) y su
// trabajo. Al ejecutarla: se descuenta el stock, se suma el trabajo, y el total
// se imputa a cada vaca del protocolo. El costo queda en el animal en el
// momento en que pasa la cosa, no repartido después.

const ETAPAS_COSTO = {
  SINCRO: { nombre: "Protocolo de sincronización", concepto: "SANIDAD" },
  IATF:   { nombre: "Inseminación",                concepto: "SEMEN" },
  TACTO1: { nombre: "Ecografía precoz",            concepto: "VETERINARIO" },
  TACTO2: { nombre: "Ecografía final",             concepto: "VETERINARIO" }
};

function guardarItem(db, protocoloId, item) {
  if (!item.nombre) throw new Error("El insumo necesita nombre");
  if (!ETAPAS_COSTO[item.etapa]) throw new Error("Etapa inválida");
  if (item.id) {
    db.prepare(`UPDATE repro_items SET etapa=?,tipo=?,nombre=?,cantidad_vaca=?,precio_unitario=?,concepto=?
                WHERE id=? AND ejecutado=0`)
      .run(item.etapa, item.tipo || "PRODUCTO", item.nombre, parseFloat(item.cantidad_vaca) || 1,
           item.precio_unitario != null ? parseFloat(item.precio_unitario) : null,
           item.concepto || null, item.id);
    return item.id;
  }
  return db.prepare(`INSERT INTO repro_items (protocolo_id,etapa,tipo,nombre,cantidad_vaca,precio_unitario,concepto)
                     VALUES (?,?,?,?,?,?,?)`)
    .run(protocoloId, item.etapa, item.tipo || "PRODUCTO", item.nombre,
         parseFloat(item.cantidad_vaca) || 1,
         item.precio_unitario != null ? parseFloat(item.precio_unitario) : null,
         item.concepto || null).lastInsertRowid;
}

function borrarItem(db, id) {
  const r = db.prepare("DELETE FROM repro_items WHERE id=? AND ejecutado=0").run(id);
  return { ok: !!r.changes, error: r.changes ? null : "No se puede borrar un insumo ya ejecutado" };
}

function itemsDeEtapa(db, protocoloId, etapa) {
  return db.prepare("SELECT * FROM repro_items WHERE protocolo_id=? AND etapa=? ORDER BY tipo DESC, id").all(protocoloId, etapa);
}

// Ejecuta una etapa: descuenta el stock, imputa el costo a cada vaca y la marca
// como hecha. `consumirStock` la inyecta el server, que sabe hablar con el
// financiero de cada empresa.
async function ejecutarEtapa(db, protocoloId, etapa, datos, consumirStock) {
  const def = ETAPAS_COSTO[etapa];
  if (!def) return { ok: false, error: "Etapa desconocida" };

  const p = db.prepare("SELECT * FROM repro_protocolos WHERE id=?").get(protocoloId);
  if (!p) return { ok: false, error: "Protocolo inexistente" };

  const yaHecha = db.prepare("SELECT * FROM repro_etapas_hechas WHERE protocolo_id=? AND etapa=?").get(protocoloId, etapa);
  if (yaHecha) return { ok: false, error: `${def.nombre} ya fue ejecutada el ${yaHecha.fecha}` };

  const v = validarProgramacion(db, protocoloId);
  if (!v.ok) return { ok: false, error: `Falta cargar: ${v.faltan.join(", ")}` };

  const vacas = db.prepare("SELECT COUNT(*) n FROM repro_vacas WHERE protocolo_id=?").get(protocoloId).n;
  if (!vacas) return { ok: false, error: "El protocolo no tiene vacas cargadas" };

  const fecha = datos.fecha || new Date().toISOString().slice(0, 10);
  const items = itemsDeEtapa(db, protocoloId, etapa);
  const lineas = [], avisos = [];
  let costoVaca = 0;

  for (const it of items) {
    const cant = (it.cantidad_vaca || 1) * vacas;
    let unitario = it.precio_unitario || 0;

    if (it.tipo === "PRODUCTO" && consumirStock) {
      const r = await consumirStock(it.nombre, cant, `${def.nombre} · ${p.nombre} ${p.temporada}`, fecha);
      if (!r.ok) return { ok: false, error: `${it.nombre}: ${r.error}` };
      // El precio real es el del stock, salvo que se haya fijado uno a mano.
      if (!it.precio_unitario) unitario = r.costo_unitario || 0;
      lineas.push(`${cant} ${r.unidad || ""} de ${r.producto} · quedan ${r.restante}`);
      if (r.aviso) avisos.push(r.aviso);
    } else {
      lineas.push(`${it.nombre}${cant !== vacas ? ` (${cant})` : ""}`);
    }

    const porVaca = unitario * (it.cantidad_vaca || 1);
    costoVaca += porVaca;
    db.prepare("UPDATE repro_items SET ejecutado=1, costo_real=? WHERE id=?").run(unitario, it.id);

    if (porVaca > 0) {
      cargarCostoVacas(db, protocoloId, it.concepto || def.concepto,
        `${def.nombre} ${p.temporada} · ${it.nombre}`, porVaca, fecha);
    }
  }

  // La fecha de la etapa también actualiza el protocolo.
  const campoFecha = { SINCRO: "fecha_sincronizacion", IATF: "fecha_iatf",
                       TACTO1: "fecha_eco_precoz", TACTO2: "fecha_eco_final" }[etapa];
  if (campoFecha) db.prepare(`UPDATE repro_protocolos SET ${campoFecha}=? WHERE id=?`).run(fecha, protocoloId);
  if (etapa === "IATF") db.prepare("UPDATE repro_protocolos SET etapa='EJECUCION' WHERE id=?").run(protocoloId);

  db.prepare(`INSERT INTO repro_etapas_hechas (protocolo_id,etapa,fecha,vacas,costo_vaca,costo_total)
              VALUES (?,?,?,?,?,?)`).run(protocoloId, etapa, fecha, vacas, costoVaca, costoVaca * vacas);

  return {
    ok: true, etapa, vacas, costo_vaca: costoVaca, costo_total: costoVaca * vacas, avisos,
    mensaje: `✅ ${def.nombre} del ${fecha} en ${vacas} vacas.` +
      (lineas.length ? `\n📦 ${lineas.join(" · ")}` : "") +
      (costoVaca > 0 ? `\n💰 US$ ${costoVaca.toFixed(2)} por vaca — total US$ ${(costoVaca * vacas).toFixed(2)}` : "") +
      (avisos.length ? `\n${avisos.join("\n")}` : "")
  };
}

// Resumen de costos del protocolo, etapa por etapa.
function costosProtocolo(db, protocoloId) {
  const hechas = db.prepare("SELECT * FROM repro_etapas_hechas WHERE protocolo_id=?").all(protocoloId);
  const total = hechas.reduce((a, e) => a + (e.costo_total || 0), 0);
  const porVaca = hechas.reduce((a, e) => a + (e.costo_vaca || 0), 0);
  return {
    etapas: hechas.map(e => ({ ...e, nombre: (ETAPAS_COSTO[e.etapa] || {}).nombre || e.etapa })),
    costo_por_vaca: Math.round(porVaca * 100) / 100,
    costo_total: Math.round(total * 100) / 100
  };
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
  registrarEjecucion, ejecutarIATF, ejecutarEtapa, guardarItem, borrarItem, itemsDeEtapa, costosProtocolo, cargarCostoVacas, ETAPAS_COSTO, registrarEcoPrecoz, registrarEcoFinal,
  decidirPadre, registrarParto, corregirPadre, estado,
  GESTACION, MARGEN_IATF, sumarDias, diasEntre
};
