// ─────────────────────────────────────────────────────────────────────────────
// PREGUNTAS — el banco para medir al bot.
//
// Cada pregunta trae cómo verificar la respuesta contra la base de prueba
// (datos/semilla.js), que es determinística: la respuesta correcta se calcula,
// no se adivina. Así, cuando cambia el modelo, el esfuerzo o el prompt, se
// corre `npm run evaluar` y se ve si mejoró o empeoró, con números.
//
// `verificar(r, db, mods)` recibe la respuesta del bot ({respuesta, pasos}) y
// devuelve { ok, esperado, motivo }.
// ─────────────────────────────────────────────────────────────────────────────

const tiene = (texto, ...cosas) => cosas.every(c => String(texto).toLowerCase().includes(String(c).toLowerCase()));
const numeros = texto => (String(texto).match(/-?\d+(?:[.,]\d+)?/g) || []).map(n => Number(n.replace(",", ".")));
const menciona = (texto, n, tol = 0) => numeros(texto).some(x => Math.abs(x - n) <= tol);
// Un RP aparece como palabra entera, no adentro de otro número ("11" no cuenta en "2011").
const mencionaRp = (texto, rp) => new RegExp(`(^|[^A-Za-z0-9])${String(rp).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?![A-Za-z0-9])`, "i").test(String(texto));
const usoHerramienta = (r, nombre) => (r.pasos || []).some(p => p.herramienta === nombre);

module.exports = [
  {
    id: "vientres",
    pregunta: "¿Cuántos vientres tiene el campo?",
    verificar: (r, db, { plantelMod }) => {
      const n = plantelMod.plantel(db).filas.length;
      return { ok: menciona(r.respuesta, n), esperado: `${n} vientres` };
    }
  },
  {
    id: "paricion",
    pregunta: "¿Cómo viene la parición?",
    verificar: (r, db, { plantelMod }) => {
      const R = plantelMod.plantel(db).resumen;
      const ok = menciona(r.respuesta, R.prenadas) && menciona(r.respuesta, R.criando) && menciona(r.respuesta, R.fallaron);
      return { ok, esperado: `${R.prenadas} preñadas, ${R.criando} criando, ${R.fallaron} fallaron` };
    }
  },
  {
    id: "vacias",
    pregunta: "¿Qué vacas quedaron vacías esta temporada? Dame los RP.",
    verificar: (r, db, { plantelMod }) => {
      const rps = plantelMod.plantel(db).filas.filter(f => f.causa === "VACIA").map(f => f.rp);
      const halladas = rps.filter(rp => mencionaRp(r.respuesta, rp));
      return { ok: halladas.length >= Math.ceil(rps.length * 0.9), esperado: `${rps.length} vacías: ${rps.join(", ")}`, motivo: `nombró ${halladas.length}` };
    }
  },
  {
    id: "mas_eficiente",
    pregunta: "¿Cuál es la vaca más eficiente del plantel y cuánto rinde?",
    verificar: (r, db, { plantelMod }) => {
      const top = plantelMod.plantel(db).filas.filter(f => f.eficiencia).sort((a, b) => b.eficiencia - a.eficiencia);
      const mejor = top[0];
      // Vale cualquiera de las empatadas en el primer lugar.
      const empatadas = top.filter(f => f.eficiencia === mejor.eficiencia);
      const ok = empatadas.some(f => mencionaRp(r.respuesta, f.rp)) && menciona(r.respuesta, mejor.eficiencia, 1);
      return { ok, esperado: `${empatadas.map(f => f.rp).join(" o ")} con ${mejor.eficiencia}%` };
    }
  },
  {
    id: "no_abortó",
    pregunta: "La vaca 15 está preñada y no tiene ternero cargado. ¿Abortó?",
    verificar: (r, db, { plantelMod }) => {
      const v = plantelMod.plantel(db).filas.find(f => f.rp === "15");
      // En la semilla la 15 está PREÑADA con FPP futura: no abortó, todavía no parió.
      const esperaba = v.estado === "PREÑADA";
      const dice = /\bno\b.*abort|todav[ií]a no pari|por parir|no (ha|había) parido|falta.*par/i.test(r.respuesta) && !/s[ií],? abort|abortó\.?$/i.test(r.respuesta);
      return { ok: esperaba ? dice : true, esperado: esperaba ? `no abortó: FPP ${v.fpp}, todavía no parió` : `estado ${v.estado}` };
    }
  },
  {
    id: "peso_ternero",
    pregunta: "¿Cuánto pesó por última vez el 24011 y cuánto viene ganando por día?",
    verificar: (r, db, { animalesMod }) => {
      const f = animalesMod.ficha(db, "24011");
      const ok = menciona(r.respuesta, f.peso_actual, 0.5) && (menciona(r.respuesta, f.gdp_ultima, 0.05) || menciona(r.respuesta, f.gdp_total, 0.05) || menciona(r.respuesta, Math.round(f.gdp_ultima * 1000), 50));
      return { ok, esperado: `${f.peso_actual} kg, ${f.gdp_ultima} kg/día (última) o ${f.gdp_total} (de vida)` };
    }
  },
  {
    id: "rp_con_cero",
    pregunta: "Contame de la 011: edad, peso adulto y cómo viene este año.",
    verificar: (r, db, { plantelMod }) => {
      const v = plantelMod.plantel(db).filas.find(f => f.rp === "11");
      const ok = menciona(r.respuesta, v.peso_adulto, 0.5) && tiene(r.respuesta, v.estado.toLowerCase().slice(0, 4));
      return { ok, esperado: `${v.edad_meses} meses, ${v.peso_adulto} kg, ${v.estado}` };
    }
  },
  {
    id: "hijos_toro",
    pregunta: "¿Cuántos hijos tiene Hércules registrados y cuántos son machos?",
    verificar: (r, db) => {
      const hijos = db.prepare("SELECT sexo FROM animales WHERE upper(padre_rp) LIKE '%HERCULES%'").all();
      const machos = hijos.filter(h => String(h.sexo).toUpperCase().startsWith("M")).length;
      return { ok: menciona(r.respuesta, hijos.length) && menciona(r.respuesta, machos), esperado: `${hijos.length} hijos, ${machos} machos` };
    }
  },
  {
    id: "nacimientos_anio",
    pregunta: "¿Cuántos terneros nacieron en 2026 y cuál fue el peso promedio al nacer?",
    verificar: (r, db) => {
      const filas = db.prepare(`SELECT p.peso FROM animales a JOIN pesadas p ON p.animal_id=a.id AND upper(p.contexto)='NACIMIENTO'
        WHERE a.fecha_nac LIKE '2026%' AND COALESCE(a.madre_rp,'')<>''`).all();
      const n = db.prepare("SELECT COUNT(*) n FROM animales WHERE fecha_nac LIKE '2026%' AND COALESCE(madre_rp,'')<>''").get().n;
      const prom = Math.round(filas.reduce((a, b) => a + b.peso, 0) / filas.length * 10) / 10;
      return { ok: menciona(r.respuesta, n) && menciona(r.respuesta, prom, 0.6), esperado: `${n} terneros, ${prom} kg promedio` };
    }
  },
  {
    id: "corral",
    pregunta: "¿Cómo viene el corral de terminación? ¿Alguno anda mal?",
    verificar: (r, db, { animalesMod }) => {
      const t = animalesMod.terminacion(db);
      const peores = t.filas.filter(f => f.gdp != null).sort((a, b) => a.gdp - b.gdp).slice(0, 3);
      const ok = menciona(r.respuesta, t.resumen.total) && peores.some(f => mencionaRp(r.respuesta, f.rp));
      return { ok, esperado: `${t.resumen.total} en corral, GDP prom ${t.resumen.gdp_prom}; los peores: ${peores.map(f => `${f.rp} (${f.gdp})`).join(", ")}` };
    }
  },
  {
    id: "mal_cargado",
    pregunta: "¿Hay algo mal cargado en la base?",
    verificar: (r, db) => {
      // La semilla no tiene errores groseros: lo correcto es no inventar ninguno.
      const inventa = /madre m[aá]s joven|dos cr[ií]as el mismo|1970|rp repetido|repetidos?\b.*rp/i.test(r.respuesta) && !/\bno\b/i.test(r.respuesta);
      return { ok: !inventa, esperado: "no inventar errores; puede señalar faltantes (pesos de destete, notas)" };
    }
  },
  {
    id: "pesada_nueva",
    pregunta: "Cargá una pesada: la 13 pesó 480 hoy en el control.",
    verificar: (r, db) => {
      const a = db.prepare("SELECT id FROM animales WHERE rp='13'").get();
      const p = db.prepare("SELECT peso, fecha FROM pesadas WHERE animal_id=? AND peso=480 ORDER BY id DESC LIMIT 1").get(a.id);
      return { ok: !!p && p.fecha === new Date().toISOString().slice(0, 10), esperado: "una pesada de 480 kg con fecha de hoy para la 13", motivo: p ? "cargada" : "no se cargó" };
    }
  },
  {
    id: "pesadas_varias",
    pregunta: "Anoté en la libreta: 011 435, 13 482, ZZZ 300. Cargalas como control de hoy.",
    verificar: (r, db) => {
      const p11 = db.prepare("SELECT 1 FROM pesadas p JOIN animales a ON a.id=p.animal_id WHERE a.rp='11' AND p.peso=435").get();
      const p13 = db.prepare("SELECT 1 FROM pesadas p JOIN animales a ON a.id=p.animal_id WHERE a.rp='13' AND p.peso=482").get();
      const avisa = /zzz/i.test(r.respuesta);
      return { ok: !!p11 && !!p13 && avisa, esperado: "cargar 11 y 13, avisar que ZZZ no existe", motivo: `11:${!!p11} 13:${!!p13} avisa ZZZ:${avisa}` };
    }
  },
  {
    id: "excel",
    pregunta: "Dame un Excel con las vacas que no destetaron.",
    verificar: (r, db) => {
      const paso = (r.pasos || []).find(p => p.tipo === "archivo");
      const link = /\/archivos\/\d+\//.test(r.respuesta);
      return { ok: !!paso && link, esperado: "un archivo guardado y su link en la respuesta", motivo: paso ? `archivo ${paso.url}` : "no armó archivo" };
    }
  },
  {
    id: "tablero",
    pregunta: "Armame un tablero con los toros y cuántos hijos tiene cada uno.",
    verificar: (r, db) => {
      const paso = (r.pasos || []).find(p => p.tipo === "tablero");
      const t = paso && db.prepare("SELECT html FROM tableros WHERE slug=?").get(paso.slug);
      const toros = db.prepare("SELECT rp FROM animales WHERE categoria='TORO'").all().map(x => x.rp);
      const ok = !!t && toros.filter(rp => t.html.includes(rp)).length >= 4;
      return { ok, esperado: "un tablero con los 6 toros", motivo: paso ? paso.url : "no armó tablero" };
    }
  },
  {
    id: "memoria",
    pregunta: "Acordate que al potrero 7 le decimos La Loma, y que las vaquillonas se sirven a los 15 meses.",
    verificar: (r, db) => {
      const m = db.prepare("SELECT texto FROM memoria WHERE activo=1").all().map(x => x.texto.toLowerCase());
      const loma = m.some(t => t.includes("loma")), meses = m.some(t => t.includes("15"));
      return { ok: loma && meses, esperado: "dos memorias guardadas (La Loma; 15 meses)", motivo: `loma:${loma} meses:${meses}` };
    }
  },
  {
    id: "usa_memoria",
    pregunta: "¿Qué animales hay en La Loma?",
    depende: "memoria",
    verificar: (r, db) => {
      const n = db.prepare("SELECT COUNT(*) n FROM lote_animales la JOIN lotes l ON l.id=la.lote_id WHERE l.potrero='POTRERO 7'").get().n;
      return { ok: menciona(r.respuesta, n) || tiene(r.respuesta, "potrero 7"), esperado: `entender que La Loma es el potrero 7 (${n} animales)` };
    }
  },
  {
    id: "ambiguo",
    pregunta: "¿Cuánto pesa la vaca esa que te dije?",
    depende: null,
    verificar: r => ({ ok: /\?/.test(r.respuesta) || /cu[aá]l|qu[eé] vaca|no s[eé] a cu[aá]l|rp/i.test(r.respuesta), esperado: "preguntar cuál, no inventar" })
  },
  {
    id: "sin_datos",
    pregunta: "¿Cuál fue el precio de venta de la última vaca que vendimos?",
    verificar: r => ({ ok: /no hay|no tengo|no figura|sin (datos|registro)|no (se|hay).*(cargad|registrad)|ninguna venta|no encuentro/i.test(r.respuesta) && !/\$\s?\d/.test(r.respuesta), esperado: "decir que no hay ventas registradas, no inventar un precio" })
  },
  {
    id: "toros_hijos_pesos",
    pregunta: "¿Qué toro tiene los hijos con mejor peso al destete?",
    verificar: (r, db) => {
      const filas = db.prepare(`SELECT a.padre_rp padre, AVG(p.peso) prom, COUNT(*) n FROM animales a JOIN pesadas p ON p.animal_id=a.id AND upper(p.contexto)='DESTETE'
        WHERE COALESCE(a.padre_rp,'')<>'' GROUP BY a.padre_rp HAVING n>=3 ORDER BY prom DESC`).all();
      const mejor = filas[0];
      return { ok: !!mejor && tiene(r.respuesta, mejor.padre.split(" ")[0]), esperado: `${mejor && mejor.padre} (${mejor && Math.round(mejor.prom)} kg, ${mejor && mejor.n} hijos)` };
    }
  }
];
