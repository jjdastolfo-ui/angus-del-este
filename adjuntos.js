// ─────────────────────────────────────────────────────────────────────────────
// ADJUNTOS — lo que le mandan al bot: fotos, PDF, Excel, CSV, Word, texto.
//
// Cada archivo se guarda en la base (para poder volver a él) y se convierte en
// algo que el modelo puede leer:
//   · fotos y PDF van tal cual: el modelo los ve (visión y documentos).
//   · Excel, CSV y TSV se leen como tablas: se le muestra un resumen (hojas,
//     columnas, primeras filas) y con las herramientas leer_adjunto e
//     importar_adjunto puede ver el resto o cargarlo con validación.
//   · Word y texto se pasan como texto.
//   · Audio no: la API no transcribe. Se avisa.
// ─────────────────────────────────────────────────────────────────────────────
const zlib = require("zlib");
const XLSX = require("xlsx");
const relevarMod = require("./relevar.js");

const MAX_IMAGEN = 5 * 1024 * 1024;        // límite de la API por imagen
const MAX_PDF = 32 * 1024 * 1024;
const MAX_TEXTO_PROMPT = 40000;            // caracteres de texto que van directo al prompt
const FILAS_VISTA = 40;                    // filas de una tabla que se muestran de entrada

function init(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS adjuntos (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      nombre TEXT NOT NULL,
      mime TEXT,
      tipo TEXT,
      bytes BLOB,
      tamano INTEGER,
      texto TEXT,
      hojas TEXT,
      canal TEXT, usuario TEXT,
      created_at TEXT DEFAULT (datetime('now')));
  `);
}

// ── QUÉ ES ───────────────────────────────────────────────────────────────────

function tipoDe(nombre, mime) {
  const ext = String(nombre || "").toLowerCase().split(".").pop();
  const m = String(mime || "").toLowerCase().split(";")[0];
  if (/^image\/(jpeg|png|gif|webp)$/.test(m) || ["jpg", "jpeg", "png", "gif", "webp"].includes(ext)) return "imagen";
  if (m === "image/heic" || m === "image/heif" || ["heic", "heif"].includes(ext)) return "heic";
  if (m === "application/pdf" || ext === "pdf") return "pdf";
  if (/spreadsheet|ms-excel|\/csv|tab-separated/.test(m) || ["xlsx", "xlsm", "xls", "ods", "csv", "tsv", "numbers"].includes(ext)) return ext === "numbers" ? "numbers" : "tabla";
  if (/wordprocessingml|msword/.test(m) || ["docx", "doc"].includes(ext)) return ext === "doc" ? "doc" : "documento";
  if (/^audio\//.test(m) || ["ogg", "opus", "mp3", "m4a", "wav", "aac"].includes(ext)) return "audio";
  if (/^video\//.test(m) || ["mp4", "mov"].includes(ext)) return "video";
  if (/^text\/|json|xml/.test(m) || ["txt", "md", "json", "xml", "log"].includes(ext)) return "texto";
  return "desconocido";
}

// ── ZIP MÍNIMO (para Word) ───────────────────────────────────────────────────
// Un .docx es un zip; el texto está en word/document.xml. Se lee el directorio
// central y se descomprime sólo lo que hace falta.
function leerZip(buf) {
  const fin = buf.lastIndexOf(Buffer.from([0x50, 0x4b, 0x05, 0x06]));
  if (fin < 0) throw new Error("No es un zip");
  const n = buf.readUInt16LE(fin + 10), cdOffset = buf.readUInt32LE(fin + 16);
  const archivos = {};
  let p = cdOffset;
  for (let i = 0; i < n; i++) {
    if (buf.readUInt32LE(p) !== 0x02014b50) break;
    const metodo = buf.readUInt16LE(p + 10), comp = buf.readUInt32LE(p + 20), largo = buf.readUInt32LE(p + 24);
    const nLen = buf.readUInt16LE(p + 28), eLen = buf.readUInt16LE(p + 30), cLen = buf.readUInt16LE(p + 32);
    const offset = buf.readUInt32LE(p + 42);
    const nombre = buf.slice(p + 46, p + 46 + nLen).toString("utf8");
    archivos[nombre] = () => {
      const lnLen = buf.readUInt16LE(offset + 26), leLen = buf.readUInt16LE(offset + 28);
      const datos = buf.slice(offset + 30 + lnLen + leLen, offset + 30 + lnLen + leLen + comp);
      return metodo === 8 ? zlib.inflateRawSync(datos) : metodo === 0 ? datos : (() => { throw new Error("Compresión no soportada"); })();
    };
    p += 46 + nLen + eLen + cLen;
  }
  return archivos;
}

function textoDeDocx(buf) {
  const z = leerZip(buf);
  if (!z["word/document.xml"]) throw new Error("No parece un archivo de Word");
  const xml = z["word/document.xml"]().toString("utf8");
  return xml.replace(/<w:tab\/>/g, "\t").replace(/<\/w:p>/g, "\n").replace(/<w:br\/>/g, "\n")
    .replace(/<[^>]+>/g, "").replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&apos;/g, "'")
    .replace(/\n{3,}/g, "\n\n").trim();
}

// ── TABLAS ───────────────────────────────────────────────────────────────────
// Excel, CSV, TSV → una lista de hojas, cada una como CSV con ; (el que
// entiende el importador) y como matriz para mostrar.

function hojasDe(nombre, buf) {
  const ext = String(nombre || "").toLowerCase().split(".").pop();
  if (["csv", "tsv", "txt"].includes(ext)) {
    const texto = buf.toString("utf8");
    const c = relevarMod.parsearCsv(texto);
    return [{ nombre: "Hoja 1", csv: texto, encabezados: c.encabezados, filas: c.filas.length }];
  }
  const wb = XLSX.read(buf, { type: "buffer", cellDates: true });
  return wb.SheetNames.map(n => {
    const ws = wb.Sheets[n];
    // Fechas como AAAA-MM-DD y números con punto: es lo que espera relevar.
    const matriz = XLSX.utils.sheet_to_json(ws, { header: 1, raw: true, defval: "" })
      .map(f => f.map(v => v instanceof Date ? v.toISOString().slice(0, 10) : v));
    // Saltar filas de título: la primera fila con 2 o más celdas llenas es el encabezado.
    let enc = 0;
    while (enc < matriz.length - 1 && matriz[enc].filter(v => v !== "" && v != null).length < 2) enc++;
    const util = matriz.slice(enc).filter(f => f.some(v => v !== "" && v != null));
    const csv = util.map(f => f.map(v => { const s = String(v == null ? "" : v); return /[";\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; }).join(";")).join("\r\n");
    return { nombre: n, csv, encabezados: (util[0] || []).map(String), filas: Math.max(util.length - 1, 0) };
  }).filter(h => h.filas > 0 || h.encabezados.length);
}

function vistaDeHoja(h, desde = 0, cuantas = FILAS_VISTA) {
  const c = relevarMod.parsearCsv(h.csv);
  const filas = c.filas.slice(desde, desde + cuantas);
  const lineas = [c.encabezados.join(" | "), ...filas.map(f => c.encabezados.map(e => f[e]).join(" | "))];
  return { texto: lineas.join("\n"), mostradas: filas.length, total: c.filas.length, desde };
}

// ── EXTRAER ──────────────────────────────────────────────────────────────────

function extraer({ nombre, mime, buffer }) {
  const tipo = tipoDe(nombre, mime);
  const out = { tipo, nombre, mime, tamano: buffer.length };
  try {
    if (tipo === "imagen") {
      if (buffer.length > MAX_IMAGEN) throw new Error(`La foto pesa ${Math.round(buffer.length / 1048576)} MB y el máximo son 5 MB. Mandala más chica.`);
      out.mime = mime && /^image\//.test(mime) ? mime.split(";")[0] : `image/${String(nombre).toLowerCase().endsWith(".png") ? "png" : "jpeg"}`;
    } else if (tipo === "pdf") {
      if (buffer.length > MAX_PDF) throw new Error("El PDF pesa más de 32 MB.");
      out.mime = "application/pdf";
    } else if (tipo === "tabla") {
      out.hojas = hojasDe(nombre, buffer);
      out.texto = out.hojas.length ? out.hojas[0].csv : "";
    } else if (tipo === "documento") {
      out.texto = textoDeDocx(buffer);
    } else if (tipo === "texto") {
      out.texto = buffer.toString("utf8");
      // Un .txt con columnas también es una tabla.
      if (/[;,\t]/.test(out.texto.split("\n")[0] || "") && out.texto.split("\n").length > 2) { out.hojas = hojasDe("x.csv", buffer); out.tipo = "tabla"; }
    } else if (tipo === "heic") throw new Error("Las fotos HEIC no las puedo abrir: mandala como JPG (en el iPhone: Ajustes → Cámara → Formatos → Más compatible), o desde WhatsApp que ya la convierte.");
    else if (tipo === "audio") throw new Error("Los audios todavía no los escucho. Escribime lo que dice.");
    else if (tipo === "video") throw new Error("Los videos no los puedo ver. Una foto sí.");
    else if (tipo === "doc") throw new Error("El .doc viejo no lo abro: guardalo como .docx o PDF.");
    else if (tipo === "numbers") throw new Error("Numbers no lo abro: exportalo como Excel o CSV.");
    else throw new Error(`No sé abrir "${nombre}" (${mime || "tipo desconocido"}).`);
  } catch (e) { out.error = e.message; }
  return out;
}

// ── PREPARAR EL MENSAJE ──────────────────────────────────────────────────────
// Guarda cada adjunto y arma los bloques de contenido para el modelo.

function preparar(db, { texto, adjuntos, canal, usuario }) {
  const bloques = [], guardados = [];
  const lista = Array.isArray(adjuntos) ? adjuntos : [];
  lista.forEach((a, i) => {
    const buffer = Buffer.isBuffer(a.buffer) ? a.buffer : Buffer.from(String(a.base64 || ""), "base64");
    const x = extraer({ nombre: a.nombre || `adjunto${i + 1}`, mime: a.mime, buffer });
    const r = db.prepare(`INSERT INTO adjuntos (nombre, mime, tipo, bytes, tamano, texto, hojas, canal, usuario) VALUES (?,?,?,?,?,?,?,?,?)`)
      .run(x.nombre, x.mime || null, x.tipo, buffer, buffer.length, x.texto || null, x.hojas ? JSON.stringify(x.hojas) : null, canal || null, usuario || null);
    const id = Number(r.lastInsertRowid);
    guardados.push({ id, nombre: x.nombre, tipo: x.tipo, error: x.error || null, hojas: x.hojas ? x.hojas.map(h => ({ nombre: h.nombre, filas: h.filas })) : undefined });
    const kb = Math.round(buffer.length / 1024);

    if (x.error) { bloques.push({ type: "text", text: `[Adjunto ${i + 1} "${x.nombre}" (id ${id}): no se pudo leer: ${x.error}]` }); return; }
    if (x.tipo === "imagen") {
      bloques.push({ type: "text", text: `[Adjunto ${i + 1}: foto "${x.nombre}" (id ${id}, ${kb} KB)]` });
      bloques.push({ type: "image", source: { type: "base64", media_type: x.mime, data: buffer.toString("base64") } });
    } else if (x.tipo === "pdf") {
      bloques.push({ type: "text", text: `[Adjunto ${i + 1}: PDF "${x.nombre}" (id ${id}, ${kb} KB)]` });
      bloques.push({ type: "document", source: { type: "base64", media_type: "application/pdf", data: buffer.toString("base64") }, title: x.nombre });
    } else if (x.tipo === "tabla") {
      const hojas = x.hojas || [];
      let t = `[Adjunto ${i + 1}: planilla "${x.nombre}" (id ${id}, ${kb} KB) con ${hojas.length} hoja${hojas.length === 1 ? "" : "s"}: ${hojas.map(h => `"${h.nombre}" (${h.filas} filas: ${h.encabezados.slice(0, 12).join(", ")}${h.encabezados.length > 12 ? "…" : ""})`).join("; ")}]`;
      if (hojas[0]) { const v = vistaDeHoja(hojas[0]); t += `\nHoja "${hojas[0].nombre}", filas 1 a ${v.mostradas} de ${v.total}:\n${v.texto}` + (v.total > v.mostradas ? `\n(hay ${v.total - v.mostradas} filas más: leer_adjunto con id ${id} y desde_fila, o importar_adjunto para cargarla entera)` : ""); }
      bloques.push({ type: "text", text: t });
    } else {
      const cuerpo = String(x.texto || "");
      bloques.push({ type: "text", text: `[Adjunto ${i + 1}: ${x.tipo === "documento" ? "Word" : "texto"} "${x.nombre}" (id ${id}, ${cuerpo.length} caracteres)]\n${cuerpo.slice(0, MAX_TEXTO_PROMPT)}${cuerpo.length > MAX_TEXTO_PROMPT ? `\n(sigue: leer_adjunto con id ${id})` : ""}` });
    }
  });
  const t = String(texto || "").trim();
  if (t) bloques.push({ type: "text", text: t });
  else if (guardados.length) bloques.push({ type: "text", text: "Te mandé esto. Decime qué es y qué contiene, y proponeme qué hacer con eso (importar, cargar, comparar con la base). Si es una planilla o una foto de la libreta con datos, mostrame qué entendiste con relevar o importar_adjunto en modo simular antes de cargar nada." });
  return { content: bloques.length === 1 && bloques[0].type === "text" && !guardados.length ? bloques[0].text : bloques, guardados };
}

// ── HERRAMIENTAS DEL BOT ─────────────────────────────────────────────────────

const traer = (db, id) => db.prepare("SELECT id, nombre, mime, tipo, texto, hojas, tamano, created_at FROM adjuntos WHERE id=?").get(Number(id));

function leer(db, { id, hoja, desde_fila = 0, cuantas = 100 }) {
  const a = traer(db, id);
  if (!a) throw new Error(`No hay un adjunto con id ${id}`);
  if (a.tipo === "tabla") {
    const hojas = JSON.parse(a.hojas || "[]");
    const h = hojas.find(x => x.nombre === hoja) || hojas[Number(hoja)] || hojas[0];
    if (!h) throw new Error("La planilla no tiene hojas con datos");
    const v = vistaDeHoja(h, Number(desde_fila) || 0, Math.min(Number(cuantas) || 100, 300));
    return { id: a.id, nombre: a.nombre, hoja: h.nombre, hojas: hojas.map(x => x.nombre), ...v };
  }
  if (a.texto) return { id: a.id, nombre: a.nombre, tipo: a.tipo, desde: desde_fila, texto: String(a.texto).slice(Number(desde_fila) || 0, (Number(desde_fila) || 0) + 30000), total_caracteres: a.texto.length };
  return { id: a.id, nombre: a.nombre, tipo: a.tipo, aviso: "Este adjunto no tiene texto (es una foto o un PDF): ya lo tenés en el mensaje." };
}

function importar(db, plantelMod, { id, hoja, tipo, mapa, fecha, contexto, producto, dosis, motivo, simular, usuario }) {
  const a = traer(db, id);
  if (!a) throw new Error(`No hay un adjunto con id ${id}`);
  let texto = a.texto;
  if (a.tipo === "tabla") {
    const hojas = JSON.parse(a.hojas || "[]");
    const h = hojas.find(x => x.nombre === hoja) || hojas[Number(hoja)] || hojas[0];
    if (!h) throw new Error("La planilla no tiene hojas con datos");
    texto = h.csv;
  }
  if (!texto) throw new Error("Este adjunto no es una planilla ni un texto con columnas");
  return { adjunto: a.nombre, ...relevarMod.importarCsv(db, plantelMod, { texto, tipo, mapa, fecha, contexto, producto, dosis, motivo, simular, usuario }) };
}

const listar = db => db.prepare("SELECT id, nombre, mime, tipo, tamano, canal, usuario, created_at FROM adjuntos ORDER BY id DESC LIMIT 200").all();
const leerBytes = (db, id) => db.prepare("SELECT nombre, mime, bytes FROM adjuntos WHERE id=?").get(Number(id));

module.exports = { init, tipoDe, extraer, preparar, leer, importar, listar, leerBytes, leerZip, textoDeDocx, hojasDe };
