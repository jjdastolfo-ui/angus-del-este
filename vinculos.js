// ─────────────────────────────────────────────────────────────────────────────
// VÍNCULOS — cruzar información entre los campos de una misma empresa.
//
// Cada campo tiene su base, pero los animales no respetan esa frontera: una
// vaca queda en un campo y su ternero se cría en otro, un toro sirve en los
// tres. Cuando eso pasa, el hijo apunta a una madre que no está en su base y
// el sistema decía "no encuentro a la madre" sin más.
//
// Acá está lo que resuelve eso:
//   · buscar un RP, un nombre, una caravana o "46 VERDE" en todos los campos
//   · revisar: qué animales tienen madre o padre que no está en su campo, y
//     dónde está cada uno realmente
//   · aplicar: dejar el vínculo anotado (madre_campo / padre_campo) y corregir
//     el RP cuando estaba escrito distinto ("011" por "11", el nombre del toro)
//   · los padres que nunca fueron del campo (semen de IATF, toros prestados):
//     se anotan aparte para que no figuren como error
//   · los hijos que un animal tiene en los otros campos
//
// Nada se borra ni se mueve: el animal sigue en su campo, sólo queda dicho
// dónde vive su madre o su padre.
// ─────────────────────────────────────────────────────────────────────────────

const { norm, compacto } = require("./animales.js");

// En el campo a muchos animales se los nombra por caravana y color: "46 VERDE",
// "157 BLANCA". Eso no es un nombre de semen: es un animal, y hay que buscarlo
// por su número de caravana.
const COLORES = "VERDE|BLANCA|BLANCO|AMARILLA|AMARILLO|ROJA|ROJO|NEGRA|NEGRO|AZUL|NARANJA|CELESTE|ROSA|VIOLETA|LILA|GRIS";
const RE_CARAVANA_COLOR = new RegExp("^\\s*([A-Za-z]?\\d+)\\s+(" + COLORES + ")\\s*$", "i");
const caravanaColor = t => {
  const m = RE_CARAVANA_COLOR.exec(String(t || ""));
  return m ? { numero: m[1], color: m[2].toUpperCase() } : null;
};

function init(db) {
  const cols = db.prepare("PRAGMA table_info(animales)").all().map(c => c.name);
  // En qué campo vive la madre / el padre, cuando no es este mismo.
  if (!cols.includes("madre_campo")) db.exec("ALTER TABLE animales ADD COLUMN madre_campo TEXT");
  if (!cols.includes("padre_campo")) db.exec("ALTER TABLE animales ADD COLUMN padre_campo TEXT");
  // Los padres que nunca fueron animales del campo: semen de IATF, toros
  // prestados, toros de otra cabaña. No son un error: se anotan y dejan de
  // aparecer como vínculo roto.
  db.exec(`
    CREATE TABLE IF NOT EXISTS padres_externos (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      valor TEXT NOT NULL UNIQUE, tipo TEXT DEFAULT ('SEMEN'), nota TEXT,
      created_at TEXT DEFAULT (datetime('now')));
  `);
}

function crear({ CAMPOS, getDB, empresasDe }) {
  const nombreCampo = k => (CAMPOS[k] || {}).nombre || k;

  // Todo lo que sirve para reconocer a un animal, en un solo mapa.
  // La misma clave puede llevar a más de un animal: eso es una ambigüedad real.
  function indice(campoKey) {
    const db = getDB(campoKey);
    const filas = db.prepare(`SELECT rp, nombre, chip, hbu, registro, sexo, categoria, estado, fecha_nac, rp_provisorio,
      caravana_control, caravana_color FROM animales`).all();
    const mapa = new Map();
    const meter = (clave, fila, como) => {
      if (!clave) return;
      if (!mapa.has(clave)) mapa.set(clave, []);
      const lista = mapa.get(clave);
      if (!lista.some(x => x.rp === fila.rp)) lista.push({ ...fila, campo: campoKey, campo_nombre: nombreCampo(campoKey), coincide: como });
    };
    for (const f of filas) {
      meter(compacto(f.rp), f, "RP");
      meter(compacto(f.nombre), f, "nombre");
      meter(norm(f.chip), f, "caravana");
      meter(compacto(f.hbu), f, "HBA");
      if (f.caravana_control) {
        meter(compacto(f.caravana_control), f, "caravana control");
        if (f.caravana_color) meter(compacto(f.caravana_control + f.caravana_color), f, "caravana y color");
      }
    }
    return mapa;
  }

  const cache = new Map();   // campo → { mapa, cuando }
  function indiceVigente(campoKey, frescoMs = 20000) {
    const c = cache.get(campoKey);
    if (c && Date.now() - c.cuando < frescoMs) return c.mapa;
    const mapa = indice(campoKey);
    cache.set(campoKey, { mapa, cuando: Date.now() });
    return mapa;
  }
  const olvidar = campoKey => { if (campoKey) cache.delete(campoKey); else cache.clear(); };

  // Las claves con las que se puede buscar un texto: tal cual, y si es
  // "46 VERDE", también por número y color.
  function clavesDe(texto) {
    const cc = caravanaColor(texto);
    const claves = [compacto(texto), norm(texto)];
    if (cc) claves.push(compacto(cc.numero + cc.color), compacto(cc.numero));
    return [...new Set(claves.filter(Boolean))];
  }

  /**
   * Busca un RP, nombre, caravana o HBA en todos los campos de la empresa.
   * `excluir` deja afuera un campo (normalmente, el propio).
   */
  function buscarEnEmpresa(campoKey, texto, opciones = {}) {
    const t = String(texto || "").trim();
    if (!t) return [];
    const claves = clavesDe(t);
    const empresa = empresasDe().empresaDe(campoKey);
    const out = [];
    for (const c of empresa.campos) {
      if (opciones.excluir && c.key === opciones.excluir) continue;
      if (!CAMPOS[c.key]) continue;
      let mapa;
      try { mapa = indiceVigente(c.key); } catch (e) { continue; }
      for (const encontrado of claves.flatMap(k => mapa.get(k) || [])) {
        if (!out.some(x => x.campo === encontrado.campo && x.rp === encontrado.rp)) out.push(encontrado);
      }
    }
    // Primero los activos.
    const activo = x => String(x.estado || "ACTIVO").toUpperCase() === "ACTIVO" ? 0 : 1;
    return out.sort((a, b) => activo(a) - activo(b));
  }

  // Una madre tiene que ser hembra y mayor que su cría; un padre, macho.
  function coherente(relacion, candidato, hijo) {
    const sexo = String(candidato.sexo || "").toUpperCase();
    if (relacion === "madre" && sexo && !sexo.startsWith("H") && !sexo.startsWith("F")) return "figura macho";
    if (relacion === "padre" && sexo && !sexo.startsWith("M")) return "figura hembra";
    if (candidato.fecha_nac && hijo.fecha_nac && candidato.fecha_nac >= hijo.fecha_nac) return "nació después que la cría";
    return null;
  }

  /**
   * Qué animales de este campo apuntan a una madre o un padre que no está acá.
   * Devuelve, para cada uno, dónde está realmente y qué se puede hacer.
   *   estado: "corregir_rp"    el RP existe en este campo, escrito distinto
   *           "en_otro_campo"  está en otro campo de la empresa (uno solo)
   *           "ambiguo"        aparece en más de un lugar: hay que elegir
   *           "no_existe"      no está en ningún campo · subtipo dice si es
   *                            probable_externo (semen), madre_no_cargada o
   *                            padre_no_cargado
   */
  function revisar(campoKey, opciones = {}) {
    const db = getDB(campoKey);
    const empresa = empresasDe().empresaDe(campoKey);
    const propio = indiceVigente(campoKey, opciones.fresco ? 0 : 20000);
    const estado = String(opciones.estado || "ACTIVO").toUpperCase();
    const filas = db.prepare(`SELECT id, rp, nombre, sexo, categoria, estado, fecha_nac, madre_rp, padre_rp, madre_campo, padre_campo
      FROM animales ${estado === "TODOS" ? "" : "WHERE upper(COALESCE(estado,'ACTIVO'))='ACTIVO'"} ORDER BY rp`).all();

    // Los que ya se dieron por externos no vuelven a figurar.
    let externos = new Set();
    try { externos = new Set(db.prepare("SELECT valor FROM padres_externos").all().map(x => compacto(x.valor))); } catch (e) {}
    // Un padre que no parece un RP ni una caravana, y se repite en varios
    // animales, es casi seguro semen o un toro de afuera.
    const vecesPadre = new Map();
    for (const a of filas) if (a.padre_rp) vecesPadre.set(compacto(a.padre_rp), (vecesPadre.get(compacto(a.padre_rp)) || 0) + 1);
    function pareceExterno(relacion, valor) {
      if (relacion !== "padre") return false;
      const t = String(valor).trim();
      if (/^[A-Za-z]{0,2}[\d\s.\-\/]+$/.test(t)) return false;   // "B332", "4178", "LR / 629": parecen RP
      if (caravanaColor(t)) return false;                        // "46 VERDE": es una caravana
      return (vecesPadre.get(compacto(t)) || 0) >= 2 || /\s/.test(t);
    }

    const out = [];
    for (const a of filas) {
      for (const relacion of ["madre", "padre"]) {
        const valor = relacion === "madre" ? a.madre_rp : a.padre_rp;
        const campoYa = relacion === "madre" ? a.madre_campo : a.padre_campo;
        if (!valor || !String(valor).trim()) continue;
        const claves = clavesDe(valor);
        const aquí = claves.map(k => propio.get(k)).find(x => x && x.length) || [];
        // Ya resuelto en este campo y con el RP tal cual: nada que hacer.
        if (aquí.length === 1 && aquí[0].rp === String(valor).trim()) continue;
        if (aquí.length === 1) {
          out.push({ rp: a.rp, categoria: a.categoria, fecha_nac: a.fecha_nac, relacion, valor,
            estado: "corregir_rp", subtipo: aquí[0].coincide === "nombre" ? "nombre_a_rp" : "rp_distinto",
            propuesta: { campo: campoKey, campo_nombre: nombreCampo(campoKey), rp: aquí[0].rp },
            porque: aquí[0].coincide === "nombre" ? `"${valor}" es el nombre de ${aquí[0].rp}, que está en este campo`
              : `en este campo el RP se escribe "${aquí[0].rp}"`, encontrado: aquí });
          continue;
        }
        if (aquí.length > 1) {
          out.push({ rp: a.rp, categoria: a.categoria, fecha_nac: a.fecha_nac, relacion, valor,
            estado: "ambiguo", porque: `hay ${aquí.length} animales con ese número en este campo`, encontrado: aquí });
          continue;
        }
        // No está acá: buscar en los otros campos de la empresa.
        const fuera = buscarEnEmpresa(campoKey, valor, { excluir: campoKey });
        const validos = fuera.map(f => ({ ...f, problema: coherente(relacion, f, a) }));
        const buenos = validos.filter(f => !f.problema);
        if (!fuera.length) {
          if (externos.has(compacto(valor))) continue;   // ya se sabe que es de afuera
          const ext = pareceExterno(relacion, valor);
          out.push({ rp: a.rp, categoria: a.categoria, fecha_nac: a.fecha_nac, relacion, valor, estado: "no_existe",
            subtipo: ext ? "probable_externo" : relacion === "madre" ? "madre_no_cargada" : "padre_no_cargado",
            porque: ext ? `"${valor}" parece semen o un toro de afuera, no un animal del campo`
              : `no hay ningún animal "${valor}" en ${empresa.nombre}`, encontrado: [] });
        } else if (buenos.length === 1) {
          const b = buenos[0];
          const yaEsta = campoYa === b.campo && String(valor).trim() === b.rp;
          if (!yaEsta) out.push({ rp: a.rp, categoria: a.categoria, fecha_nac: a.fecha_nac, relacion, valor,
            estado: "en_otro_campo", propuesta: { campo: b.campo, campo_nombre: b.campo_nombre, rp: b.rp },
            porque: `está en ${b.campo_nombre}${b.coincide !== "RP" ? ` (coincide por ${b.coincide})` : ""}`, encontrado: validos });
        } else {
          out.push({ rp: a.rp, categoria: a.categoria, fecha_nac: a.fecha_nac, relacion, valor,
            estado: buenos.length ? "ambiguo" : "no_existe",
            subtipo: buenos.length ? undefined : "sin_candidato_valido",
            porque: buenos.length ? `aparece en ${buenos.length} campos: ${buenos.map(b => b.campo_nombre).join(", ")}`
              : `los candidatos no sirven: ${validos.map(v => `${v.rp} en ${v.campo_nombre} (${v.problema})`).join("; ")}`,
            encontrado: validos });
        }
      }
    }
    const cuenta = e => out.filter(f => f.estado === e).length;
    const sub = s => out.filter(f => f.subtipo === s).length;
    return { campo: campoKey, campo_nombre: nombreCampo(campoKey), empresa: empresa.nombre,
      campos_empresa: empresa.campos.map(c => ({ key: c.key, nombre: c.nombre })),
      filas: out.slice(0, opciones.limite || 500),
      resumen: { total: out.length, corregir_rp: cuenta("corregir_rp"), en_otro_campo: cuenta("en_otro_campo"),
        ambiguo: cuenta("ambiguo"), no_existe: cuenta("no_existe"),
        probables_externos: sub("probable_externo"), madres_no_cargadas: sub("madre_no_cargada"),
        nombre_a_rp: sub("nombre_a_rp"), rp_distinto: sub("rp_distinto"),
        arreglables: cuenta("corregir_rp") + cuenta("en_otro_campo") } };
  }

  /** Lo mismo para todos los campos de la empresa. */
  function revisarEmpresa(campoKey, opciones = {}) {
    const empresa = empresasDe().empresaDe(campoKey);
    const campos = empresa.campos.map(c => {
      try { return revisar(c.key, opciones); }
      catch (e) { return { campo: c.key, campo_nombre: c.nombre, error: e.message, filas: [], resumen: {} }; }
    });
    const suma = k => campos.reduce((s, c) => s + ((c.resumen || {})[k] || 0), 0);
    return { empresa: empresa.nombre, campos,
      resumen: { total: suma("total"), corregir_rp: suma("corregir_rp"), en_otro_campo: suma("en_otro_campo"),
        ambiguo: suma("ambiguo"), no_existe: suma("no_existe"), probables_externos: suma("probables_externos"),
        madres_no_cargadas: suma("madres_no_cargadas"), nombre_a_rp: suma("nombre_a_rp"), rp_distinto: suma("rp_distinto"),
        arreglables: suma("arreglables") } };
  }

  /**
   * Deja los vínculos anotados. Sin `filas`, arregla todo lo inequívoco del campo
   * (los "corregir_rp" y los "en_otro_campo"); nunca toca lo ambiguo ni lo que no
   * existe. Con `simular: true` sólo dice qué haría.
   */
  function aplicar(campoKey, opciones = {}) {
    const db = getDB(campoKey);
    const pedidas = Array.isArray(opciones.filas) && opciones.filas.length ? opciones.filas : null;
    const revision = revisar(campoKey, { fresco: true, limite: 5000 });
    let objetivo = revision.filas.filter(f => ["corregir_rp", "en_otro_campo"].includes(f.estado));
    if (pedidas) {
      objetivo = pedidas.map(p => {
        const base = revision.filas.find(f => compacto(f.rp) === compacto(p.rp) && f.relacion === (p.relacion || "madre"));
        const rpPedido = p.rp_madre || p.rp_padre || p.valor;
        const campoPedido = p.campo || (base && base.propuesta && base.propuesta.campo) || campoKey;
        const propuesta = (rpPedido || p.campo)
          ? { campo: campoPedido, campo_nombre: nombreCampo(campoPedido), rp: rpPedido || (base && base.propuesta && base.propuesta.rp) }
          : base && base.propuesta;
        return base ? { ...base, propuesta } : { rp: p.rp, relacion: p.relacion || "madre", estado: "manual", propuesta, valor: p.valor };
      }).filter(f => f.propuesta && f.propuesta.rp);
    }

    const out = [];
    const hacer = () => {
      for (const f of objetivo) {
        const r = { rp: f.rp, relacion: f.relacion, valor: f.valor, ok: false, avisos: [] };
        out.push(r);
        const a = db.prepare("SELECT id, rp FROM animales WHERE upper(rp)=upper(?)").get(String(f.rp).trim());
        if (!a) { r.error = `No existe ${f.rp} en ${revision.campo_nombre}`; continue; }
        const p = f.propuesta;
        if (!p || !p.rp) { r.error = "Sin propuesta: hay que decir a qué animal apunta"; continue; }
        const mismoCampo = p.campo === campoKey;
        r.queda = { rp: p.rp, campo: mismoCampo ? null : p.campo, campo_nombre: mismoCampo ? revision.campo_nombre : p.campo_nombre };
        if (!mismoCampo && !CAMPOS[p.campo]) { r.error = `No existe el campo "${p.campo}"`; continue; }
        if (!mismoCampo && empresasDe().empresaDe(p.campo).key !== empresasDe().empresaDe(campoKey).key) { r.error = "Ese campo es de otra empresa"; continue; }
        r.ok = true;
        if (!opciones.simular) {
          const col = f.relacion === "madre" ? "madre_rp" : "padre_rp";
          const colCampo = f.relacion === "madre" ? "madre_campo" : "padre_campo";
          db.prepare(`UPDATE animales SET ${col}=?, ${colCampo}=? WHERE id=?`).run(p.rp, mismoCampo ? null : p.campo, a.id);
          r.hecho = true;
        }
      }
    };
    if (opciones.simular) hacer(); else db.transaction(hacer)();
    olvidar(campoKey);
    const bien = out.filter(r => r.ok).length, mal = out.filter(r => !r.ok).length;
    return { ok: bien > 0, simulado: !!opciones.simular, campo: campoKey, total: out.length, bien, mal, filas: out,
      quedan: { ambiguo: revision.resumen.ambiguo, no_existe: revision.resumen.no_existe },
      mensaje: (opciones.simular ? `${bien} vínculo${bien === 1 ? "" : "s"} para arreglar` : `${bien} vínculo${bien === 1 ? "" : "s"} arreglado${bien === 1 ? "" : "s"}`)
        + (mal ? `, ${mal} con problema` : "")
        + (revision.resumen.ambiguo ? `. Quedan ${revision.resumen.ambiguo} ambiguos (hay que elegir a mano)` : "")
        + (revision.resumen.no_existe ? ` y ${revision.resumen.no_existe} que no están en ningún campo` : "") + "." };
  }

  /**
   * Anota padres que no son animales del campo (semen, toros prestados) para que
   * dejen de figurar como vínculos rotos. Sin `valores`, toma los que el propio
   * revisar marcó como probables externos.
   */
  function marcarExternos(campoKey, opciones = {}) {
    const db = getDB(campoKey);
    let valores = opciones.valores;
    if (!valores || !valores.length) {
      const rev = revisar(campoKey, { fresco: true, limite: 5000 });
      valores = [...new Set(rev.filas.filter(f => f.subtipo === "probable_externo").map(f => f.valor))];
    }
    const ins = db.prepare("INSERT OR IGNORE INTO padres_externos (valor, tipo, nota) VALUES (?,?,?)");
    if (!opciones.simular) db.transaction(() => { for (const v of valores) ins.run(String(v).trim(), opciones.tipo || "SEMEN", opciones.nota || null); })();
    olvidar(campoKey);
    return { ok: true, simulado: !!opciones.simular, cuantos: valores.length, valores,
      mensaje: `${valores.length} padre${valores.length === 1 ? "" : "s"} ${opciones.simular ? "se marcarían" : "marcados"} como de afuera (semen o toro prestado)` +
        (valores.length ? `: ${valores.slice(0, 8).join(", ")}${valores.length > 8 ? "…" : ""}` : "") };
  }
  const externos = campoKey => { try { return getDB(campoKey).prepare("SELECT valor, tipo, nota, created_at FROM padres_externos ORDER BY valor").all(); } catch (e) { return []; } };
  function olvidarExterno(campoKey, valor) {
    const r = getDB(campoKey).prepare("DELETE FROM padres_externos WHERE upper(valor)=upper(?)").run(String(valor).trim());
    olvidar(campoKey);
    return { ok: !!r.changes };
  }

  /** Los hijos que este animal tiene en los otros campos de la empresa. */
  function hijosFuera(campoKey, rp, nombre) {
    const empresa = empresasDe().empresaDe(campoKey);
    const claves = new Set([compacto(rp), compacto(nombre)].filter(Boolean));
    const out = [];
    for (const c of empresa.campos) {
      if (c.key === campoKey || !CAMPOS[c.key]) continue;
      let filas = [];
      try {
        filas = getDB(c.key).prepare(`SELECT h.rp, h.fecha_nac, h.sexo, h.pelo, h.estado, h.madre_rp, h.padre_rp,
          (SELECT peso FROM pesadas p WHERE p.animal_id=h.id AND upper(COALESCE(p.contexto,''))='NACIMIENTO' ORDER BY p.fecha LIMIT 1) peso_nac,
          (SELECT peso FROM pesadas p WHERE p.animal_id=h.id AND upper(COALESCE(p.contexto,''))='DESTETE' ORDER BY p.fecha DESC LIMIT 1) destete
          FROM animales h WHERE COALESCE(h.madre_rp,'') <> '' OR COALESCE(h.padre_rp,'') <> ''`).all();
      } catch (e) { continue; }
      for (const h of filas) {
        if (claves.has(compacto(h.madre_rp)) || claves.has(compacto(h.padre_rp)))
          out.push({ ...h, campo: c.key, campo_nombre: c.nombre });
      }
    }
    return out.sort((a, b) => String(a.fecha_nac || "").localeCompare(String(b.fecha_nac || "")));
  }

  /** Los datos de la madre o el padre que vive en otro campo. */
  function familiaFuera(campoKey, animal) {
    const out = {};
    for (const rel of ["madre", "padre"]) {
      const campo = animal[rel + "_campo"], valor = animal[rel === "madre" ? "madre_rp" : "padre_rp"];
      if (!campo || !valor || !CAMPOS[campo]) continue;
      try {
        const a = getDB(campo).prepare(`SELECT rp, nombre, categoria, sexo, estado, fecha_nac, pelo FROM animales WHERE upper(rp)=upper(?)`).get(String(valor).trim());
        if (a) out[rel] = { ...a, campo, campo_nombre: nombreCampo(campo) };
      } catch (e) {}
    }
    return out;
  }

  return { init, indice, buscarEnEmpresa, revisar, revisarEmpresa, aplicar, marcarExternos, externos,
    olvidarExterno, hijosFuera, familiaFuera, olvidar, caravanaColor };
}

module.exports = { init, crear, caravanaColor };
