// ─────────────────────────────────────────────────────────────────────────────
// IMPORTADOR INTELIGENTE
//
// El parser viejo buscaba columnas que se llamaran "peso" o "rp". Si la planilla
// venía distinta, fallaba. Acá la IA lee una muestra del archivo y decide qué es
// y cómo mapear cada columna; después el mapeo se aplica en código a TODAS las
// filas. Esa división importa: la IA interpreta una vez (barato y flexible), el
// código aplica mil veces (rápido, determinístico y auditable). Mandarle las mil
// filas a la IA sería caro, lento, y podría inventar datos.
//
// Nada se escribe en la base desde acá: se devuelve una propuesta para confirmar.
// ─────────────────────────────────────────────────────────────────────────────

let ExcelJS, pdfjsLib;
try { ExcelJS = require("exceljs"); } catch (e) {}
try { pdfjsLib = require("pdfjs-dist/legacy/build/pdf.js"); } catch (e) {}

const MODELO = "claude-sonnet-4-6";
const MUESTRA = 12;          // filas que ve la IA para decidir el mapeo
const MAX_FILAS = 3000;      // tope de seguridad por archivo

// ── EXTRACCIÓN ───────────────────────────────────────────────────────────────
// Saca el contenido crudo sin interpretarlo. Cada formato da lo mismo: filas.

async function extraerExcel(buf) {
  if (!ExcelJS) throw new Error("ExcelJS no disponible");
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buf);
  const ws = wb.getWorksheet(1);
  if (!ws) throw new Error("La planilla no tiene hojas");

  // Las filas de arriba pueden ser título, logo o metadatos de la balanza. La
  // fila de encabezados es la primera con varias celdas de texto no vacías.
  let filaEnc = 1, mejor = 0;
  for (let r = 1; r <= Math.min(ws.rowCount, 12); r++) {
    let llenas = 0;
    ws.getRow(r).eachCell(c => { if (String(c.value ?? "").trim()) llenas++; });
    if (llenas > mejor) { mejor = llenas; filaEnc = r; }
  }

  const cols = [];
  ws.getRow(filaEnc).eachCell((c, n) => { cols[n] = String(c.value ?? "").trim(); });

  const filas = [];
  for (let r = filaEnc + 1; r <= ws.rowCount && filas.length < MAX_FILAS; r++) {
    const fila = {};
    let tiene = false;
    ws.getRow(r).eachCell((c, n) => {
      const col = cols[n];
      if (!col) return;
      let v = c.value;
      if (v && typeof v === "object") v = v.result ?? v.text ?? v.hyperlink ?? String(v);
      if (v !== null && v !== undefined && String(v).trim() !== "") { fila[col] = v; tiene = true; }
    });
    if (tiene) filas.push(fila);
  }
  return { columnas: cols.filter(Boolean), filas };
}

function extraerCSV(buf) {
  const texto = buf.toString("utf-8").replace(/^\uFEFF/, "");
  const lineas = texto.split(/\r?\n/).filter(l => l.trim());
  if (lineas.length < 2) throw new Error("El archivo está vacío");

  // Separador: el que más aparece en la primera línea.
  const sep = [";", ",", "\t", "|"]
    .map(s => ({ s, n: lineas[0].split(s).length }))
    .sort((a, b) => b.n - a.n)[0].s;

  const partir = l => {
    const out = []; let act = "", comillas = false;
    for (const ch of l) {
      if (ch === '"') comillas = !comillas;
      else if (ch === sep && !comillas) { out.push(act.trim()); act = ""; }
      else act += ch;
    }
    out.push(act.trim());
    return out;
  };

  const cols = partir(lineas[0]);
  const filas = [];
  for (let i = 1; i < lineas.length && filas.length < MAX_FILAS; i++) {
    const vals = partir(lineas[i]);
    const fila = {};
    let tiene = false;
    cols.forEach((c, j) => { if (c && vals[j]) { fila[c] = vals[j]; tiene = true; } });
    if (tiene) filas.push(fila);
  }
  return { columnas: cols.filter(Boolean), filas };
}

async function extraerPDF(buf) {
  if (!pdfjsLib) throw new Error("pdfjs-dist no disponible");
  const doc = await pdfjsLib.getDocument({ data: new Uint8Array(buf) }).promise;
  let texto = "";
  for (let i = 1; i <= doc.numPages; i++) {
    const c = await (await doc.getPage(i)).getTextContent();
    texto += c.items.map(x => x.str).join(" ") + "\n";
  }
  return { texto: texto.trim() };
}

// ── INTERPRETACIÓN ───────────────────────────────────────────────────────────

const ESQUEMA = `
TIPOS DE IMPORTACIÓN POSIBLES:

"pesadas" — pesajes de animales
  rp        (texto, obligatorio) identificación del animal: RP, caravana, arete, tag
  peso      (número, obligatorio) en kilos
  fecha     (AAAA-MM-DD)
  contexto  uno de: NACIMIENTO, DESTETE, AÑO, 18MESES, ADULTA
  notas     (texto)

"animales" — alta de animales al inventario
  rp        (texto, obligatorio)
  chip      (texto)
  sexo      (obligatorio) M o H
  fecha_nac (AAAA-MM-DD)
  raza, registro, categoria, madre_rp, padre_rp, hbu, notas

"mediciones" — circunferencia escrotal, altura, condición corporal
  rp     (obligatorio)
  tipo   (obligatorio) CE, ALTURA, CC, DOCILIDAD
  valor  (número, obligatorio)
  fecha  (AAAA-MM-DD)

"sanidad" — vacunas y tratamientos
  rp       (obligatorio)
  tipo     VACUNA o TRATAMIENTO
  producto (texto)
  dosis, fecha, notas
`.trim();

function muestraLegible(datos) {
  if (datos.texto) return datos.texto.slice(0, 4000);
  const filas = datos.filas.slice(0, MUESTRA);
  return `Columnas: ${datos.columnas.join(" | ")}\n\nPrimeras filas:\n` +
    filas.map((f, i) => `${i + 1}. ${JSON.stringify(f)}`).join("\n") +
    `\n\n(total de filas en el archivo: ${datos.filas.length})`;
}

async function interpretar(anthropic, datos, nombre, instruccion) {
  const prompt = `Sos un asistente de un sistema de gestión ganadera. Te paso una muestra
de un archivo que subió un productor. Tenés que decidir qué contiene y cómo mapear
sus columnas a nuestro esquema.

ARCHIVO: ${nombre}
${instruccion ? `LO QUE DIJO EL USUARIO: "${instruccion}"` : "El usuario no aclaró nada."}

${ESQUEMA}

MUESTRA DEL ARCHIVO:
${muestraLegible(datos)}

Respondé SOLO con un objeto JSON, sin texto alrededor y sin backticks:

{
  "tipo": "pesadas" | "animales" | "mediciones" | "sanidad" | "desconocido",
  "confianza": "alta" | "media" | "baja",
  "mapeo": { "campo_nuestro": "Nombre exacto de la columna del archivo" },
  "constantes": { "campo_nuestro": "valor fijo para todas las filas" },
  "formato_fecha": "DMY" | "MDY" | "YMD" | "excel" | null,
  "observacion": "una línea explicando qué encontraste, en castellano rioplatense"
}

REGLAS:
- En "mapeo" usá el nombre EXACTO de la columna como figura en el archivo.
- Lo que sea igual para todas las filas (por ejemplo el contexto DESTETE si el
  usuario lo dijo, o la fecha si está en el encabezado y no en cada fila) va en
  "constantes", no en "mapeo".
- Si el usuario dijo el contexto de la pesada, respetalo por encima de lo que
  parezca el archivo.
- Si no podés identificar el contenido con seguridad, poné tipo "desconocido" y
  explicá en "observacion" qué te falta. Es preferible eso a adivinar.`;

  const r = await anthropic.messages.create({
    model: MODELO,
    max_tokens: 1200,
    messages: [{ role: "user", content: prompt }]
  });

  const txt = r.content.map(c => c.text || "").join("").trim();
  const limpio = txt.replace(/^```(?:json)?/i, "").replace(/```$/, "").trim();
  try {
    return JSON.parse(limpio);
  } catch (e) {
    const m = limpio.match(/\{[\s\S]*\}/);
    if (m) return JSON.parse(m[0]);
    throw new Error("La IA no devolvió un mapeo válido");
  }
}

// ── NORMALIZACIÓN ────────────────────────────────────────────────────────────

function aFecha(v, formato) {
  if (v === null || v === undefined || v === "") return null;
  if (v instanceof Date) return v.toISOString().slice(0, 10);

  // Excel guarda fechas como días desde 1900.
  if (typeof v === "number" && v > 20000 && v < 60000) {
    return new Date(Math.round((v - 25569) * 86400000)).toISOString().slice(0, 10);
  }

  const s = String(v).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);

  const m = s.match(/^(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{2,4})/);
  if (m) {
    let [, a, b, y] = m;
    if (y.length === 2) y = (parseInt(y) > 50 ? "19" : "20") + y;
    // Con formato MDY el primero es el mes; si no, día. Un valor > 12 en la
    // primera posición sólo puede ser día, sea cual sea el formato declarado.
    let dia = a, mes = b;
    if (formato === "MDY" && parseInt(a) <= 12) { mes = a; dia = b; }
    return `${y}-${String(mes).padStart(2, "0")}-${String(dia).padStart(2, "0")}`;
  }

  const d = new Date(s);
  return isNaN(d) ? null : d.toISOString().slice(0, 10);
}

function aNumero(v) {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number") return v;
  // "1.234,5" (europeo) vs "1,234.5" (inglés): manda el último separador.
  let s = String(v).replace(/[^\d,.\-]/g, "");
  const ic = s.lastIndexOf(","), ip = s.lastIndexOf(".");
  if (ic > ip) s = s.replace(/\./g, "").replace(",", ".");
  else s = s.replace(/,/g, "");
  const n = parseFloat(s);
  return isNaN(n) ? null : n;
}

const NUMERICOS = new Set(["peso", "valor"]);
const FECHAS = new Set(["fecha", "fecha_nac"]);

function normalizar(datos, plan) {
  const { mapeo = {}, constantes = {}, formato_fecha } = plan;
  const filas = [];

  for (const cruda of datos.filas) {
    const fila = { ...constantes };
    for (const [campo, columna] of Object.entries(mapeo)) {
      let v = cruda[columna];
      if (v === undefined) {
        // Tolerancia a diferencias de mayúsculas y espacios en el encabezado.
        const k = Object.keys(cruda).find(
          x => x.toLowerCase().trim() === String(columna).toLowerCase().trim()
        );
        if (k) v = cruda[k];
      }
      if (v === undefined || v === null || String(v).trim() === "") continue;

      if (FECHAS.has(campo)) fila[campo] = aFecha(v, formato_fecha);
      else if (NUMERICOS.has(campo)) fila[campo] = aNumero(v);
      else fila[campo] = String(v).trim();
    }
    if (Object.keys(fila).length) filas.push(fila);
  }
  return filas;
}

// ── VALIDACIÓN ───────────────────────────────────────────────────────────────

const OBLIGATORIOS = {
  pesadas:    ["rp", "peso"],
  animales:   ["rp", "sexo"],
  mediciones: ["rp", "tipo", "valor"],
  sanidad:    ["rp"]
};

const RANGOS = {
  peso:  [1, 1500],
  valor: [0, 500]
};

function validar(tipo, filas) {
  const req = OBLIGATORIOS[tipo] || [];
  const buenas = [], problemas = [];

  filas.forEach((f, i) => {
    const linea = i + 1;
    const faltan = req.filter(c => f[c] === undefined || f[c] === null || f[c] === "");
    if (faltan.length) { problemas.push({ linea, motivo: `falta ${faltan.join(" y ")}`, fila: f }); return; }

    let fuera = null;
    for (const [campo, [min, max]] of Object.entries(RANGOS)) {
      if (f[campo] !== undefined && (f[campo] < min || f[campo] > max)) {
        fuera = `${campo} fuera de rango (${f[campo]})`;
      }
    }
    if (fuera) { problemas.push({ linea, motivo: fuera, fila: f }); return; }

    buenas.push(f);
  });

  return { buenas, problemas };
}

// ── RESUMEN ──────────────────────────────────────────────────────────────────

const ETIQUETA = {
  pesadas: "pesadas", animales: "animales", mediciones: "mediciones", sanidad: "registros de sanidad"
};

function resumir(tipo, buenas, problemas, plan, existe) {
  const l = [];
  l.push(`📄 ${plan.observacion || "Archivo leído"}`);
  l.push("");
  l.push(`Encontré *${buenas.length} ${ETIQUETA[tipo] || "registros"}* para cargar.`);

  if (tipo === "pesadas" && buenas.length) {
    const pesos = buenas.map(b => b.peso).filter(Boolean);
    const prom = pesos.reduce((a, b) => a + b, 0) / (pesos.length || 1);
    l.push(`Peso promedio ${prom.toFixed(1)} kg — del más liviano ${Math.min(...pesos)} al más pesado ${Math.max(...pesos)}.`);
    const ctx = buenas[0].contexto;
    if (ctx) l.push(`Contexto: ${ctx}.`);
    const f = buenas.find(b => b.fecha);
    if (f) l.push(`Fecha: ${f.fecha}.`);
  }

  if (existe && existe.faltantes.length) {
    const m = existe.faltantes.slice(0, 8).join(", ");
    l.push("");
    l.push(`⚠️ ${existe.faltantes.length} no están en el sistema: ${m}${existe.faltantes.length > 8 ? "…" : ""}`);
  }

  if (problemas.length) {
    l.push("");
    l.push(`⚠️ Salteo ${problemas.length} fila${problemas.length > 1 ? "s" : ""}:`);
    problemas.slice(0, 4).forEach(p => l.push(`   línea ${p.linea}: ${p.motivo}`));
    if (problemas.length > 4) l.push(`   …y ${problemas.length - 4} más`);
  }

  if (plan.confianza === "baja") {
    l.push("");
    l.push("No estoy del todo seguro de haber leído bien el archivo. Revisá antes de confirmar.");
  }

  l.push("");
  l.push(buenas.length ? "¿Los cargo? Respondé *sí* o *no*." : "No hay nada para cargar.");
  return l.join("\n");
}

// ── ENTRADA PRINCIPAL ────────────────────────────────────────────────────────

async function analizar({ buffer, nombre, instruccion, anthropic, rpsExistentes }) {
  const ext = String(nombre || "").toLowerCase().split(".").pop();

  let datos;
  if (["xlsx", "xls"].includes(ext)) datos = await extraerExcel(buffer);
  else if (ext === "csv" || ext === "txt") datos = extraerCSV(buffer);
  else if (ext === "pdf") datos = await extraerPDF(buffer);
  else throw new Error(`No sé leer archivos .${ext}`);

  if (datos.filas && !datos.filas.length) throw new Error("El archivo no tiene filas con datos");

  const plan = await interpretar(anthropic, datos, nombre, instruccion);
  if (plan.tipo === "desconocido") {
    return { ok: false, mensaje: `No pude identificar qué contiene el archivo.\n${plan.observacion || ""}\n\nDecime qué son los datos y lo vuelvo a intentar.` };
  }

  // El PDF viene como texto suelto: la IA ya devolvió el mapeo pero no hay
  // filas tabuladas que normalizar. Por ahora sólo tablas.
  if (!datos.filas) {
    return { ok: false, mensaje: "Leí el PDF pero no encontré una tabla clara. Probá con la planilla en Excel o CSV." };
  }

  const filas = normalizar(datos, plan);
  const { buenas, problemas } = validar(plan.tipo, filas);

  let existe = null;
  if (rpsExistentes && buenas.length) {
    const set = new Set([...rpsExistentes].map(r => String(r).toUpperCase()));
    const faltantes = [...new Set(buenas.map(b => String(b.rp).toUpperCase()).filter(rp => !set.has(rp)))];
    existe = { faltantes };
  }

  return {
    ok: true,
    tipo: plan.tipo,
    plan,
    filas: buenas,
    problemas,
    mensaje: resumir(plan.tipo, buenas, problemas, plan, existe)
  };
}

module.exports = { analizar, aFecha, aNumero, normalizar, validar };
