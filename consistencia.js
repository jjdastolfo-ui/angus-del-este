// ─────────────────────────────────────────────────────────────────────────────
// CONTROL DE CONSISTENCIA
//
// La tabla `servicios` guarda `ternero_rp` como texto libre, sin relación con
// `animales`. Así se cuelan crías que no existen, vacas que ya parieron pero
// figuran preñadas, y terneros cuya madre no es la del servicio.
//
// Este módulo NO corrige por su cuenta: detecta, explica y propone. Cada
// hallazgo dice qué encontró, por qué está mal y qué haría para arreglarlo.
// La corrección se aplica sólo cuando alguien la confirma.
// ─────────────────────────────────────────────────────────────────────────────

const DIAS_GESTACION = 283;
const TOLERANCIA_PARTO = 25;   // días de margen contra la fecha probable

function diasEntre(a, b) {
  return Math.round((new Date(b) - new Date(a)) / 86400000);
}

function fppDeServicio(s) {
  const base = s.fecha_iatf || s.fecha_ingreso_toro;
  if (!base) return null;
  const d = new Date(base);
  d.setDate(d.getDate() + DIAS_GESTACION);
  return d.toISOString().slice(0, 10);
}

// ── DETECCIÓN ────────────────────────────────────────────────────────────────

function revisar(db, opciones = {}) {
  const hoy = opciones.hoy || new Date().toISOString().slice(0, 10);
  const hallazgos = [];
  const agregar = h => hallazgos.push(h);

  const animales = db.prepare("SELECT id, rp, sexo, fecha_nac, madre_rp, padre_rp, estado FROM animales").all();
  const porRp = new Map(animales.map(a => [String(a.rp).toUpperCase(), a]));

  const servicios = db.prepare(`
    SELECT s.*, a.rp AS madre_rp_real
    FROM servicios s JOIN animales a ON a.id = s.animal_id
  `).all();

  // 1. La cría anotada en el servicio no existe en el inventario.
  //    Suele ser un tipeo: "rp603" cuando el animal es "S603".
  for (const s of servicios) {
    if (!s.ternero_rp) continue;
    const buscado = String(s.ternero_rp).toUpperCase();
    if (porRp.has(buscado)) continue;

    // ¿Hay un hijo real de esta madre que se le parezca?
    const hijos = animales.filter(a =>
      String(a.madre_rp || "").toUpperCase() === String(s.madre_rp_real).toUpperCase());

    const soloDigitos = buscado.replace(/\D/g, "");
    let candidato = hijos.find(h => String(h.rp).replace(/\D/g, "") === soloDigitos && soloDigitos.length >= 2);
    if (!candidato && hijos.length === 1) candidato = hijos[0];

    agregar({
      tipo: "CRIA_INEXISTENTE",
      severidad: "alta",
      madre: s.madre_rp_real,
      servicio_id: s.id,
      temporada: s.temporada,
      descripcion: `La cría "${s.ternero_rp}" anotada en el servicio ${s.temporada || ""} de ${s.madre_rp_real} no existe en el inventario.`,
      sugerencia: candidato
        ? `Parece ser ${candidato.rp}, que sí figura como hijo de ${s.madre_rp_real}${candidato.fecha_nac ? ` (nacido ${candidato.fecha_nac})` : ""}.`
        : `No encontré ningún hijo de ${s.madre_rp_real} que se le parezca. Habría que dar de alta el ternero o borrar el dato.`,
      correccion: candidato ? { accion: "reasignar_cria", servicio_id: s.id, rp_nuevo: candidato.rp } : null
    });
  }

  // 2. La cría existe, pero su madre registrada es otra.
  for (const s of servicios) {
    if (!s.ternero_rp) continue;
    const cria = porRp.get(String(s.ternero_rp).toUpperCase());
    if (!cria) continue;
    const madreDeLaCria = String(cria.madre_rp || "").toUpperCase();
    if (!madreDeLaCria) {
      agregar({
        tipo: "CRIA_SIN_MADRE",
        severidad: "media",
        madre: s.madre_rp_real,
        servicio_id: s.id,
        descripcion: `${cria.rp} figura como cría del servicio de ${s.madre_rp_real}, pero en su ficha no tiene madre asignada.`,
        sugerencia: `Poner ${s.madre_rp_real} como madre de ${cria.rp}.`,
        correccion: { accion: "asignar_madre", rp_cria: cria.rp, madre_rp: s.madre_rp_real }
      });
    } else if (madreDeLaCria !== String(s.madre_rp_real).toUpperCase()) {
      agregar({
        tipo: "MADRE_CONTRADICTORIA",
        severidad: "alta",
        madre: s.madre_rp_real,
        servicio_id: s.id,
        descripcion: `${cria.rp} está anotado como cría de ${s.madre_rp_real} en el servicio, pero en su ficha la madre es ${cria.madre_rp}.`,
        sugerencia: `Definir cuál de las dos es correcta antes de tocar nada: hay dos registros que se contradicen.`,
        correccion: null
      });
    }
  }

  // 3. Parió pero el servicio no lo sabe: hay un hijo con fecha compatible y
  //    el servicio sigue sin parto anotado. Es el caso de la vaca que muestra
  //    fecha probable de parto cuando el ternero ya está en el campo.
  for (const s of servicios) {
    if (s.fecha_parto || s.ternero_rp) continue;
    if (!/PRE/i.test(String(s.resultado || ""))) continue;
    const fpp = fppDeServicio(s);
    if (!fpp) continue;

    const hijos = animales.filter(a =>
      String(a.madre_rp || "").toUpperCase() === String(s.madre_rp_real).toUpperCase() &&
      a.fecha_nac && Math.abs(diasEntre(fpp, a.fecha_nac)) <= 60);

    if (hijos.length === 1) {
      const h = hijos[0];
      agregar({
        tipo: "PARTO_NO_REGISTRADO",
        severidad: "alta",
        madre: s.madre_rp_real,
        servicio_id: s.id,
        descripcion: `${s.madre_rp_real} figura preñada con parto previsto para el ${fpp}, pero ${h.rp} ya está cargado como hijo suyo (nacido ${h.fecha_nac}).`,
        sugerencia: `Cerrar el servicio con el parto del ${h.fecha_nac} y la cría ${h.rp}.`,
        correccion: { accion: "cerrar_parto", servicio_id: s.id, rp_cria: h.rp, fecha_parto: h.fecha_nac }
      });
    } else if (diasEntre(fpp, hoy) > TOLERANCIA_PARTO) {
      agregar({
        tipo: "FPP_VENCIDA",
        severidad: hijos.length > 1 ? "media" : "alta",
        madre: s.madre_rp_real,
        servicio_id: s.id,
        descripcion: hijos.length > 1
          ? `${s.madre_rp_real} tenía parto previsto para el ${fpp} y hay ${hijos.length} crías suyas con fecha compatible: no sé cuál corresponde.`
          : `${s.madre_rp_real} tenía parto previsto para el ${fpp}, hace ${diasEntre(fpp, hoy)} días, y no hay parto ni cría registrados.`,
        sugerencia: hijos.length > 1
          ? `Elegir cuál de estas es la cría: ${hijos.map(h => `${h.rp} (${h.fecha_nac})`).join(", ")}.`
          : `Registrar el parto, o marcar el servicio como perdido si la vaca falló.`,
        correccion: null
      });
    }
  }

  // 4. El padre de la cría no coincide con el semen o el toro del servicio.
  for (const s of servicios) {
    if (!s.ternero_rp) continue;
    const cria = porRp.get(String(s.ternero_rp).toUpperCase());
    if (!cria || !cria.padre_rp) continue;
    // Con IATF más repaso, el padre puede ser cualquiera de los dos: sólo se
    // avisa cuando no coincide con ninguno.
    const posibles = [s.semen_iatf, s.toro_natural]
      .filter(Boolean).map(x => String(x).toUpperCase().trim());
    if (!posibles.length) continue;
    const real = String(cria.padre_rp).toUpperCase().trim();
    const coincide = posibles.some(p => p === real || real.includes(p) || p.includes(real));
    if (coincide) continue;
    agregar({
      tipo: "PADRE_CONTRADICTORIO",
      severidad: "media",
      madre: s.madre_rp_real,
      servicio_id: s.id,
      descripcion: `El servicio de ${s.madre_rp_real} fue con ${posibles.join(" o ")}, pero la cría ${cria.rp} tiene como padre a ${cria.padre_rp}.`,
      sugerencia: `Verificar cuál corresponde: el padre de la cría no figura en ese servicio.`,
      correccion: null
    });
  }

  // 5. Crías nacidas antes de que la madre pudiera parirlas.
  for (const a of animales) {
    if (!a.madre_rp || !a.fecha_nac) continue;
    const madre = porRp.get(String(a.madre_rp).toUpperCase());
    if (!madre) {
      agregar({
        tipo: "MADRE_INEXISTENTE",
        severidad: "media",
        madre: a.madre_rp,
        descripcion: `${a.rp} tiene como madre a ${a.madre_rp}, que no existe en el inventario.`,
        sugerencia: `Revisar el RP de la madre: puede ser un tipeo.`,
        correccion: null
      });
      continue;
    }
    if (madre.fecha_nac && diasEntre(madre.fecha_nac, a.fecha_nac) < 600) {
      agregar({
        tipo: "MADRE_DEMASIADO_JOVEN",
        severidad: "alta",
        madre: madre.rp,
        descripcion: `${a.rp} nació el ${a.fecha_nac}, cuando su madre ${madre.rp} tenía ${Math.floor(diasEntre(madre.fecha_nac, a.fecha_nac) / 30)} meses.`,
        sugerencia: `Alguna de las dos fechas de nacimiento está mal, o la madre no es esa.`,
        correccion: null
      });
    }
  }

  const orden = { alta: 0, media: 1, baja: 2 };
  hallazgos.sort((x, y) => orden[x.severidad] - orden[y.severidad]);
  return hallazgos;
}

// ── CORRECCIÓN ───────────────────────────────────────────────────────────────
// Sólo se aplican las correcciones que el módulo propuso, y sólo de a una.

function corregir(db, correccion) {
  if (!correccion || !correccion.accion) return { ok: false, error: "Sin corrección para aplicar" };

  if (correccion.accion === "reasignar_cria") {
    const r = db.prepare("UPDATE servicios SET ternero_rp = ? WHERE id = ?")
      .run(correccion.rp_nuevo, correccion.servicio_id);
    return { ok: !!r.changes, mensaje: `Cría del servicio corregida a ${correccion.rp_nuevo}.` };
  }

  if (correccion.accion === "asignar_madre") {
    const r = db.prepare("UPDATE animales SET madre_rp = ? WHERE upper(rp) = upper(?)")
      .run(correccion.madre_rp, correccion.rp_cria);
    return { ok: !!r.changes, mensaje: `${correccion.rp_cria} ahora figura como hijo de ${correccion.madre_rp}.` };
  }

  if (correccion.accion === "cerrar_parto") {
    const cria = db.prepare("SELECT * FROM animales WHERE upper(rp) = upper(?)").get(correccion.rp_cria);
    if (!cria) return { ok: false, error: `No encuentro ${correccion.rp_cria}` };
    const r = db.prepare(`UPDATE servicios SET fecha_parto = ?, ternero_rp = ?, sexo_cria = COALESCE(sexo_cria, ?)
                          WHERE id = ?`)
      .run(correccion.fecha_parto, cria.rp, cria.sexo, correccion.servicio_id);
    return { ok: !!r.changes, mensaje: `Parto cerrado: ${cria.rp} el ${correccion.fecha_parto}.` };
  }

  return { ok: false, error: "Corrección desconocida" };
}

// ── RESUMEN PARA WHATSAPP ────────────────────────────────────────────────────

const TITULOS = {
  CRIA_INEXISTENTE: "Cría que no existe",
  CRIA_SIN_MADRE: "Cría sin madre asignada",
  MADRE_CONTRADICTORIA: "Madre contradictoria",
  PARTO_NO_REGISTRADO: "Parió y no está registrado",
  FPP_VENCIDA: "Parto vencido sin registrar",
  PADRE_CONTRADICTORIO: "Padre que no coincide",
  MADRE_INEXISTENTE: "Madre que no existe",
  MADRE_DEMASIADO_JOVEN: "Fechas imposibles"
};

function resumir(hallazgos, limite = 10) {
  if (!hallazgos.length) return "✅ Revisé los servicios y las crías: no encontré inconsistencias.";

  const porTipo = {};
  hallazgos.forEach(h => { porTipo[h.tipo] = (porTipo[h.tipo] || 0) + 1; });

  let msg = `🔍 *Encontré ${hallazgos.length} inconsistencia${hallazgos.length > 1 ? "s" : ""}*\n\n`;
  Object.entries(porTipo).forEach(([t, n]) => { msg += `· ${TITULOS[t] || t}: ${n}\n`; });
  msg += "\n";

  hallazgos.slice(0, limite).forEach((h, i) => {
    msg += `${i + 1}. ${h.descripcion}\n   → ${h.sugerencia}\n\n`;
  });
  if (hallazgos.length > limite) msg += `…y ${hallazgos.length - limite} más.\n\n`;

  const corregibles = hallazgos.filter(h => h.correccion).length;
  if (corregibles) msg += `${corregibles} las puedo corregir solo. Respondé *corregir* y te digo una por una qué haría.`;
  return msg;
}

module.exports = { revisar, corregir, resumir, fppDeServicio, TITULOS };
