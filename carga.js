// ─────────────────────────────────────────────────────────────────────────────
// CARGA RAZONADA
//
// El ciclo que hace que el sistema entienda en vez de tragar datos:
//
//   1. Cargás algo — un Excel, una foto, un audio, un texto suelto
//   2. El bot lo lee y lo CONTRASTA contra lo que ya hay en la base
//   3. Te informa: qué cargó, qué resumen da, y qué quedó raro
//   4. Le mandás la corrección hablando
//   5. Acomoda y vuelve a informar
//
// Lo que lo diferencia de un importador común: no valida contra reglas fijas.
// Compara lo nuevo con lo viejo y razona si tiene sentido. Una vaca repetida en
// nacimientos, un RP que no existe, una fecha imposible, un peso que no cierra
// con la historia del animal — todo eso se detecta mirando el conjunto, no
// cada fila por separado.
// ─────────────────────────────────────────────────────────────────────────────

const dias = (a, b) => Math.round((new Date(b) - new Date(a)) / 86400000);
const GESTACION = 283;

function init(db) {
  db.exec(`
    -- Cada carga queda registrada con lo que se hizo y lo que quedó pendiente,
    -- para poder volver sobre ella y corregir.
    CREATE TABLE IF NOT EXISTS cargas (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fecha TEXT DEFAULT (datetime('now')),
      origen TEXT,              -- excel | foto | audio | texto | csv
      nombre TEXT,              -- el archivo, si lo hubo
      tipo TEXT,                -- animales | pesadas | servicios | partos…
      filas INTEGER,
      aplicadas INTEGER,
      observaciones TEXT,       -- JSON con lo que quedó raro
      estado TEXT DEFAULT 'PENDIENTE',   -- PENDIENTE | CONFIRMADA | CORREGIDA
      usuario TEXT,
      datos TEXT                -- JSON con lo cargado, por si hay que rehacer
    );
    CREATE INDEX IF NOT EXISTS idx_cargas_fecha ON cargas(fecha DESC);
  `);
}

// ── LO QUE SE MIRA ANTES DE ESCRIBIR ─────────────────────────────────────────
//
// No son validaciones de formato: son contrastes contra lo que ya existe.
// Cada uno responde una pregunta que un ganadero se haría mirando la planilla.

function revisar(db, tipo, filas, opciones = {}) {
  const hoy = opciones.hoy || new Date().toISOString().slice(0, 10);
  const obs = [];
  const marcar = (fila, nivel, que, sugerencia) =>
    obs.push({ fila: fila.rp || fila.animal || "?", nivel, que, sugerencia });

  const existe = rp => {
    if (!rp) return null;
    try { return db.prepare("SELECT * FROM animales WHERE upper(rp)=upper(?)").get(String(rp).trim()); }
    catch (e) { return null; }
  };

  // ¿Viene el mismo RP dos veces en lo que se está cargando?
  // Los repetidos se cuentan primero y se avisan una sola vez, con el total.
  const cuenta = {};
  for (const f of filas) {
    const rp = String(f.rp || f.animal || "").trim().toUpperCase();
    if (rp) cuenta[rp] = (cuenta[rp] || 0) + 1;
  }
  Object.entries(cuenta).filter(([, n]) => n > 1).forEach(([rp, n]) => {
    // En pesadas puede ser legítimo: dos contextos distintos el mismo día.
    const mismas = filas.filter(f => String(f.rp || f.animal || "").trim().toUpperCase() === rp);
    const contextos = new Set(mismas.map(f => `${f.fecha || ""}|${f.contexto || ""}`));
    if (tipo === "pesadas" && contextos.size === mismas.length) return;
    obs.push({ fila: rp, nivel: "alta", que: `aparece ${n} veces en lo que cargaste`,
      sugerencia: "Decime cuál vale, o si son animales distintos con el mismo RP" });
  });

  for (const f of filas) {
    const rp = String(f.rp || f.animal || "").trim();
    const a = existe(rp);

    if (tipo === "animales") {
      // ¿Ya existe? Puede ser un alta repetida o un mellizo mal cargado.
      if (a) marcar(f, "alta", `${rp} ya existe en el sistema` +
        (a.fecha_nac ? `, nacido el ${a.fecha_nac}` : ""),
        "Si es el mismo animal, no hace falta cargarlo. Si es otro, necesita un RP distinto");

      // ¿La madre existe?
      if (f.madre_rp && !existe(f.madre_rp))
        marcar(f, "alta", `la madre ${f.madre_rp} no existe en el sistema`,
          `Cargá primero a ${f.madre_rp}, o corregí el RP si es un tipeo`);

      // ¿La madre pudo parirlo?
      if (f.madre_rp && f.fecha_nac) {
        const m = existe(f.madre_rp);
        if (m && m.fecha_nac) {
          const edadMadre = dias(m.fecha_nac, f.fecha_nac) / 365.25;
          if (edadMadre < 1.8)
            marcar(f, "alta", `${f.madre_rp} tenía ${edadMadre.toFixed(1)} años cuando nació este ternero`,
              "Una vaca no puede parir antes de los dos años: revisá las fechas");
        }
        // ¿Esa madre ya tiene otra cría este año? Puede ser mellizo o error.
        try {
          const otra = db.prepare(`SELECT rp, fecha_nac FROM animales
            WHERE upper(COALESCE(madre_rp,''))=upper(?) AND substr(fecha_nac,1,4)=?`)
            .get(f.madre_rp, String(f.fecha_nac).slice(0, 4));
          if (otra && String(otra.rp).toUpperCase() !== rp.toUpperCase()) {
            const d = Math.abs(dias(otra.fecha_nac, f.fecha_nac));
            marcar(f, d < 5 ? "media" : "alta",
              `${f.madre_rp} ya tiene otra cría este año: ${otra.rp} del ${otra.fecha_nac}`,
              d < 5 ? "Podrían ser mellizos" : "Una vaca no pare dos veces al año: revisá cuál corresponde");
          }
        } catch (e) {}
      }

      if (f.fecha_nac && f.fecha_nac > hoy) {
        const d = dias(hoy, f.fecha_nac);
        // Unos días adelante puede ser el reloj o un parte cargado antes de
        // tiempo. Un mes ya es un error.
        marcar(f, d > 15 ? "alta" : "media",
          `nació el ${f.fecha_nac}, que es ${d} día${d > 1 ? "s" : ""} adelante de hoy`,
          d > 15 ? "Revisá la fecha" : "Confirmame si está bien");
      }
    }

    if (tipo === "pesadas") {
      if (!a) { marcar(f, "alta", `${rp} no existe en el sistema`,
        "Cargá el animal primero, o corregí el RP"); continue; }

      // ¿El peso tiene sentido con lo que ya pesaba?
      try {
        const ult = db.prepare(`SELECT fecha, peso FROM pesadas WHERE animal_id=?
                                ORDER BY fecha DESC LIMIT 1`).get(a.id);
        if (ult && f.peso != null && f.fecha) {
          const d = dias(ult.fecha, f.fecha);
          const dif = f.peso - ult.peso;
          if (d > 0) {
            const gdp = dif / d;
            if (gdp < -0.3)
              marcar(f, "media", `bajó de ${ult.peso} a ${f.peso} kg en ${d} días`,
                "Puede ser real si estuvo enferma o parió, pero conviene confirmarlo");
            else if (gdp > 2.2)
              marcar(f, "alta", `pasó de ${ult.peso} a ${f.peso} kg en ${d} días — ${gdp.toFixed(2)} kg por día`,
                "Ningún bovino gana eso: revisá el peso o la fecha");
          } else if (d === 0 && Math.abs(dif) > 5) {
            marcar(f, "alta", `dos pesos distintos el mismo día: ${ult.peso} y ${f.peso}`,
              "Decime cuál vale");
          }
        }
        // ¿Ya hay una pesada de ese contexto en esa fecha?
        if (f.contexto) {
          const rep = db.prepare(`SELECT peso FROM pesadas WHERE animal_id=? AND fecha=? AND contexto=?`)
            .get(a.id, f.fecha, f.contexto);
          if (rep) marcar(f, "media", `ya tenía una pesada de ${f.contexto} el ${f.fecha}: ${rep.peso} kg`,
            "Si es una corrección, decímelo y la reemplazo");
        }
      } catch (e) {}
    }

    if (tipo === "servicios" || tipo === "partos") {
      if (!a) marcar(f, "alta", `${rp} no existe en el sistema`, "Cargá el animal primero");
      else if (String(a.sexo || "").toUpperCase().startsWith("M"))
        marcar(f, "alta", `${rp} figura como macho`, "Un servicio o parto va sobre una hembra: revisá el RP");
    }
  }

  return obs;
}

// ── EL INFORME ───────────────────────────────────────────────────────────────
// Lo que se le devuelve al usuario después de cargar: qué entró, qué resumen
// da, y qué quedó raro. Escrito como se lo diría una persona.

function informe(tipo, filas, obs, resumen) {
  const graves = obs.filter(o => o.nivel === "alta");
  const medias = obs.filter(o => o.nivel === "media");

  let t = `📥 Cargué *${filas.length}* ${etiqueta(tipo, filas.length)}.\n`;
  if (resumen) t += `\n${resumen}\n`;

  if (graves.length) {
    t += `\n⚠️ *${graves.length} cosa${graves.length > 1 ? "s" : ""} que no cierra${graves.length > 1 ? "n" : ""}:*\n`;
    graves.slice(0, 8).forEach(o => { t += `\n· *${o.fila}*: ${o.que}\n  ${o.sugerencia}\n`; });
    if (graves.length > 8) t += `\n…y ${graves.length - 8} más.\n`;
  }
  if (medias.length) {
    t += `\n🟡 *${medias.length} para confirmar:*\n`;
    medias.slice(0, 5).forEach(o => { t += `· *${o.fila}*: ${o.que}\n`; });
  }
  if (!obs.length) t += `\nTodo cierra con lo que ya había.`;
  else t += `\nDecime qué corrijo y lo acomodo.`;
  return t;
}

function etiqueta(tipo, n) {
  const e = { animales: ["animal", "animales"], pesadas: ["pesada", "pesadas"],
              servicios: ["servicio", "servicios"], partos: ["parto", "partos"],
              mediciones: ["medición", "mediciones"], sanidad: ["registro de sanidad", "registros de sanidad"] };
  const p = e[tipo] || ["registro", "registros"];
  return n === 1 ? p[0] : p[1];
}

// ── GUARDAR Y CORREGIR ───────────────────────────────────────────────────────

function registrar(db, d) {
  const info = db.prepare(`INSERT INTO cargas (origen,nombre,tipo,filas,aplicadas,observaciones,usuario,datos)
                           VALUES (?,?,?,?,?,?,?,?)`)
    .run(d.origen || "texto", d.nombre || null, d.tipo, d.filas || 0, d.aplicadas || 0,
         JSON.stringify(d.observaciones || []), d.usuario || null,
         d.datos ? JSON.stringify(d.datos).slice(0, 200000) : null);
  return info.lastInsertRowid;
}

function ultima(db) {
  try {
    const c = db.prepare("SELECT * FROM cargas ORDER BY id DESC LIMIT 1").get();
    if (!c) return null;
    return { ...c, observaciones: JSON.parse(c.observaciones || "[]"),
             datos: c.datos ? JSON.parse(c.datos) : null };
  } catch (e) { return null; }
}

function historial(db, n = 20) {
  try {
    return db.prepare("SELECT id,fecha,origen,nombre,tipo,filas,aplicadas,estado FROM cargas ORDER BY id DESC LIMIT ?").all(n);
  } catch (e) { return []; }
}

// Aplica una corrección sobre lo ya cargado. Cada tipo sabe qué tocar.
function corregir(db, accion) {
  const { que, rp } = accion;
  const a = db.prepare("SELECT * FROM animales WHERE upper(rp)=upper(?)").get(String(rp || "").trim());

  if (que === "borrar_animal") {
    if (!a) return { ok: false, error: `No encuentro ${rp}` };
    const hijos = db.prepare("SELECT COUNT(*) n FROM animales WHERE upper(COALESCE(madre_rp,''))=upper(?)").get(a.rp).n;
    if (hijos && !accion.forzar)
      return { ok: false, error: `${a.rp} tiene ${hijos} cría${hijos > 1 ? "s" : ""} registrada${hijos > 1 ? "s" : ""}. Si igual querés borrarla, decímelo de nuevo.` };
    ["pesadas", "mediciones", "servicios", "costos"].forEach(t => {
      try { db.prepare(`DELETE FROM ${t} WHERE animal_id=?`).run(a.id); } catch (e) {}
    });
    db.prepare("DELETE FROM animales WHERE id=?").run(a.id);
    return { ok: true, mensaje: `Borré ${a.rp} y todo su historial.` };
  }

  if (que === "cambiar_rp") {
    if (!a) return { ok: false, error: `No encuentro ${rp}` };
    const nuevo = String(accion.nuevo || "").trim();
    if (!nuevo) return { ok: false, error: "Decime cuál es el RP correcto" };
    if (db.prepare("SELECT id FROM animales WHERE upper(rp)=upper(?)").get(nuevo))
      return { ok: false, error: `Ya existe un animal con RP ${nuevo}` };
    db.prepare("UPDATE animales SET rp=? WHERE id=?").run(nuevo, a.id);
    // Las referencias también se corrigen: si no, quedan crías huérfanas.
    db.prepare("UPDATE animales SET madre_rp=? WHERE upper(COALESCE(madre_rp,''))=upper(?)").run(nuevo, a.rp);
    db.prepare("UPDATE animales SET padre_rp=? WHERE upper(COALESCE(padre_rp,''))=upper(?)").run(nuevo, a.rp);
    return { ok: true, mensaje: `${a.rp} pasó a ser ${nuevo}, y actualicé las referencias de sus crías.` };
  }

  if (que === "asignar_madre") {
    if (!a) return { ok: false, error: `No encuentro ${rp}` };
    const m = db.prepare("SELECT rp FROM animales WHERE upper(rp)=upper(?)").get(String(accion.madre || "").trim());
    if (!m) return { ok: false, error: `No encuentro la madre ${accion.madre}` };
    db.prepare("UPDATE animales SET madre_rp=? WHERE id=?").run(m.rp, a.id);
    return { ok: true, mensaje: `${a.rp} ahora figura como hijo de ${m.rp}.` };
  }

  if (que === "corregir_peso") {
    if (!a) return { ok: false, error: `No encuentro ${rp}` };
    const r = db.prepare(`UPDATE pesadas SET peso=? WHERE animal_id=? AND fecha=?`)
      .run(accion.peso, a.id, accion.fecha);
    return r.changes
      ? { ok: true, mensaje: `Corregí el peso de ${a.rp} del ${accion.fecha} a ${accion.peso} kg.` }
      : { ok: false, error: `No encontré una pesada de ${a.rp} el ${accion.fecha}` };
  }

  if (que === "borrar_pesada") {
    if (!a) return { ok: false, error: `No encuentro ${rp}` };
    const r = db.prepare("DELETE FROM pesadas WHERE animal_id=? AND fecha=?").run(a.id, accion.fecha);
    return r.changes
      ? { ok: true, mensaje: `Borré la pesada de ${a.rp} del ${accion.fecha}.` }
      : { ok: false, error: `No encontré esa pesada` };
  }

  return { ok: false, error: `No sé cómo hacer "${que}"` };
}

// ── LO QUE SE LE DICE AL BOT ─────────────────────────────────────────────────

const INSTRUCCIONES = `CARGA RAZONADA: cuando alguien carga datos, no los tragues. Contrastalos con lo que ya hay y contá qué encontraste.

Después de cargar, informá tres cosas:
1. Qué cargaste — cuántos registros y de qué tipo.
2. El resumen que sale de eso — promedios, totales, lo que sirva para saber si está bien.
3. Qué NO cierra — con el RP, qué encontraste, y qué haría falta para resolverlo.

Lo que hay que mirar, contrastando contra la base:
· El mismo RP dos veces en la misma carga.
· Un animal que ya existe y se está cargando de nuevo.
· Una madre que no está en el sistema.
· Una vaca con dos crías el mismo año — pueden ser mellizos si nacieron con días de diferencia, o un error si no.
· Una madre que era demasiado joven cuando nació esa cría.
· Un peso que no cierra con la historia del animal: bajó mucho, o ganó más de lo posible.
· Fechas futuras o imposibles.

CUANDO TE MANDEN UNA CORRECCIÓN, aplicala y confirmá qué hiciste. Las correcciones se piden hablando: "la 2077 no existe, borrala", "el RP correcto es S603 no rp603", "la madre de S610 es la 2655", "el peso de la 105 del 18/3 era 190 no 290".

Respondé con {"accion":"corregir_carga","que":"...","rp":"...", …} usando: borrar_animal, cambiar_rp (con "nuevo"), asignar_madre (con "madre"), corregir_peso (con "fecha" y "peso"), borrar_pesada (con "fecha").

No apliques nada sin estar seguro de qué te piden. Si la corrección es ambigua, preguntá.`;

module.exports = { init, revisar, informe, registrar, ultima, historial, corregir, INSTRUCCIONES };
