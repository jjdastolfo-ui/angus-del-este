// ─────────────────────────────────────────────────────────────────────────────
// XLSX — escribe planillas de Excel sin depender de nada.
//
// Un .xlsx es un zip con unos XML adentro. Acá se arma el zip a mano (Node ya
// trae deflate) y se escriben los XML justos: libro, hojas y estilos. Con eso
// alcanza para que Excel, Numbers y Google Sheets lo abran bien, con el
// encabezado pintado, las columnas con ancho, el filtro puesto y la primera
// fila congelada. Los números salen como números y las fechas como fechas,
// así se pueden sumar y ordenar sin pelearse con el archivo.
//
//   const buf = armar([{ nombre: "Plantel", columnas: [{k:"rp",t:"RP"}, ...], filas: [...] }]);
// ─────────────────────────────────────────────────────────────────────────────
const zlib = require("zlib");

// ── ZIP ──────────────────────────────────────────────────────────────────────

const CRC = (() => {
  const t = new Int32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xEDB88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c;
  }
  return t;
})();
function crc32(buf) {
  let c = -1;
  for (let i = 0; i < buf.length; i++) c = CRC[(c ^ buf[i]) & 0xFF] ^ (c >>> 8);
  return (c ^ -1) >>> 0;
}

function zip(archivos) {
  const partes = [], central = [];
  let offset = 0;
  const ahora = new Date();
  const hora = (ahora.getHours() << 11) | (ahora.getMinutes() << 5) | (ahora.getSeconds() >> 1);
  const fecha = ((ahora.getFullYear() - 1980) << 9) | ((ahora.getMonth() + 1) << 5) | ahora.getDate();

  for (const [nombre, contenido] of archivos) {
    const nom = Buffer.from(nombre, "utf8");
    const datos = Buffer.isBuffer(contenido) ? contenido : Buffer.from(contenido, "utf8");
    const comp = zlib.deflateRawSync(datos);
    const crc = crc32(datos);

    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0); local.writeUInt16LE(20, 4); local.writeUInt16LE(0x0800, 6);
    local.writeUInt16LE(8, 8); local.writeUInt16LE(hora, 10); local.writeUInt16LE(fecha, 12);
    local.writeUInt32LE(crc, 14); local.writeUInt32LE(comp.length, 18); local.writeUInt32LE(datos.length, 22);
    local.writeUInt16LE(nom.length, 26); local.writeUInt16LE(0, 28);
    partes.push(local, nom, comp);

    const c = Buffer.alloc(46);
    c.writeUInt32LE(0x02014b50, 0); c.writeUInt16LE(20, 4); c.writeUInt16LE(20, 6); c.writeUInt16LE(0x0800, 8);
    c.writeUInt16LE(8, 10); c.writeUInt16LE(hora, 12); c.writeUInt16LE(fecha, 14);
    c.writeUInt32LE(crc, 16); c.writeUInt32LE(comp.length, 20); c.writeUInt32LE(datos.length, 24);
    c.writeUInt16LE(nom.length, 28); c.writeUInt16LE(0, 30); c.writeUInt16LE(0, 32);
    c.writeUInt16LE(0, 34); c.writeUInt16LE(0, 36); c.writeUInt32LE(0, 38); c.writeUInt32LE(offset, 42);
    central.push(c, nom);
    offset += local.length + nom.length + comp.length;
  }
  const cd = Buffer.concat(central);
  const fin = Buffer.alloc(22);
  fin.writeUInt32LE(0x06054b50, 0); fin.writeUInt16LE(0, 4); fin.writeUInt16LE(0, 6);
  fin.writeUInt16LE(archivos.length, 8); fin.writeUInt16LE(archivos.length, 10);
  fin.writeUInt32LE(cd.length, 12); fin.writeUInt32LE(offset, 16); fin.writeUInt16LE(0, 20);
  return Buffer.concat([...partes, cd, fin]);
}

// ── XML ──────────────────────────────────────────────────────────────────────

const esc = s => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
  .replace(/"/g, "&quot;").replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, "");

function letra(n) {   // 0 → A, 25 → Z, 26 → AA
  let s = "";
  n++;
  while (n > 0) { const r = (n - 1) % 26; s = String.fromCharCode(65 + r) + s; n = Math.floor((n - 1) / 26); }
  return s;
}

const ES_FECHA = /^\d{4}-\d{2}-\d{2}(T.*)?$/;
function serialFecha(iso) {
  const d = new Date(String(iso).slice(0, 10) + "T00:00:00Z");
  return Math.round((d - Date.UTC(1899, 11, 30)) / 86400000);
}

// Estilos: el índice es el que se pone en cada celda como s="n".
//  0 normal · 1 encabezado · 2 texto · 3 entero · 4 decimal · 5 fecha · 6 título · 7 subtítulo · 8 porcentaje
const ESTILOS = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<numFmts count="3">
  <numFmt numFmtId="164" formatCode="dd/mm/yyyy"/>
  <numFmt numFmtId="165" formatCode="#,##0.0"/>
  <numFmt numFmtId="166" formatCode="0%"/>
</numFmts>
<fonts count="4">
  <font><sz val="10"/><name val="Calibri"/></font>
  <font><b/><sz val="10"/><color rgb="FFFFFFFF"/><name val="Calibri"/></font>
  <font><b/><sz val="14"/><color rgb="FF0B3D7C"/><name val="Calibri"/></font>
  <font><sz val="10"/><color rgb="FF8A827A"/><name val="Calibri"/></font>
</fonts>
<fills count="3">
  <fill><patternFill patternType="none"/></fill>
  <fill><patternFill patternType="gray125"/></fill>
  <fill><patternFill patternType="solid"><fgColor rgb="FF0B3D7C"/><bgColor indexed="64"/></patternFill></fill>
</fills>
<borders count="2">
  <border><left/><right/><top/><bottom/><diagonal/></border>
  <border><left style="thin"><color rgb="FFE2D9CB"/></left><right style="thin"><color rgb="FFE2D9CB"/></right>
    <top style="thin"><color rgb="FFE2D9CB"/></top><bottom style="thin"><color rgb="FFE2D9CB"/></bottom><diagonal/></border>
</borders>
<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
<cellXfs count="9">
  <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
  <xf numFmtId="0" fontId="1" fillId="2" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1">
    <alignment horizontal="center" vertical="center" wrapText="1"/></xf>
  <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1"/>
  <xf numFmtId="1" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1"/>
  <xf numFmtId="165" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1"/>
  <xf numFmtId="164" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1" applyAlignment="1">
    <alignment horizontal="center"/></xf>
  <xf numFmtId="0" fontId="2" fillId="0" borderId="0" xfId="0" applyFont="1"/>
  <xf numFmtId="0" fontId="3" fillId="0" borderId="0" xfId="0" applyFont="1"/>
  <xf numFmtId="166" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1"/>
</cellXfs>
<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
</styleSheet>`;

/**
 * Una hoja. `columnas` es [{k, t, ancho?, tipo?}]: k la clave en cada fila,
 * t el encabezado, tipo "texto" | "entero" | "decimal" | "fecha" | "porcentaje"
 * (si no viene, se deduce mirando los datos).
 */
function hoja({ columnas, filas, titulo, subtitulo }) {
  const cols = columnas.map(c => typeof c === "string" ? { k: c, t: c } : c);
  const tipos = cols.map(c => c.tipo || deducirTipo(filas.map(f => valor(f, c.k))));

  const xml = [];
  let r = 1;
  const filasCabecera = [];
  if (titulo) {
    filasCabecera.push(`<row r="${r}"><c r="A${r}" s="6" t="inlineStr"><is><t>${esc(titulo)}</t></is></c></row>`); r++;
    if (subtitulo) { filasCabecera.push(`<row r="${r}"><c r="A${r}" s="7" t="inlineStr"><is><t>${esc(subtitulo)}</t></is></c></row>`); r++; }
    r++;   // una fila en blanco
  }
  const filaEncabezado = r;
  xml.push(`<row r="${r}" ht="26" customHeight="1">` +
    cols.map((c, i) => `<c r="${letra(i)}${r}" s="1" t="inlineStr"><is><t>${esc(c.t)}</t></is></c>`).join("") + `</row>`);
  r++;
  for (const f of filas) {
    const celdas = cols.map((c, i) => celda(letra(i) + r, valor(f, c.k), tipos[i]));
    xml.push(`<row r="${r}">${celdas.join("")}</row>`);
    r++;
  }
  const ultimaFila = Math.max(r - 1, filaEncabezado);

  // Ancho: el que pida la columna, o lo que ocupe el dato más largo (con tope).
  const anchos = cols.map((c, i) => {
    if (c.ancho) return c.ancho;
    const largo = Math.max(String(c.t).length, ...filas.slice(0, 500).map(f => String(valor(f, c.k) ?? "").length));
    return Math.min(Math.max(largo + 2, 6), 48);
  });
  const ref = `A${filaEncabezado}:${letra(cols.length - 1)}${ultimaFila}`;

  return {
    xml: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<sheetViews><sheetView workbookViewId="0"${filas.length ? "" : ""}>
  <pane ySplit="${filaEncabezado}" topLeftCell="A${filaEncabezado + 1}" activePane="bottomLeft" state="frozen"/>
</sheetView></sheetViews>
<sheetFormatPr defaultRowHeight="15"/>
<cols>${anchos.map((a, i) => `<col min="${i + 1}" max="${i + 1}" width="${a}" customWidth="1"/>`).join("")}</cols>
<sheetData>${filasCabecera.join("")}${xml.join("")}</sheetData>
<autoFilter ref="${ref}"/>
<pageSetup orientation="landscape" fitToWidth="1" fitToHeight="0"/>
</worksheet>`,
    ref, filaEncabezado, ultimaFila, ncols: cols.length
  };
}

const valor = (f, k) => (k in Object(f)) ? f[k] : undefined;

function deducirTipo(valores) {
  const v = valores.filter(x => x !== null && x !== undefined && x !== "");
  if (!v.length) return "texto";
  if (v.every(x => typeof x === "number" && isFinite(x)))
    return v.every(x => Number.isInteger(x)) ? "entero" : "decimal";
  if (v.every(x => typeof x === "string" && ES_FECHA.test(x))) return "fecha";
  return "texto";
}

function celda(ref, v, tipo) {
  if (v === null || v === undefined || v === "") return `<c r="${ref}" s="2"/>`;
  if (tipo === "entero" || tipo === "decimal") {
    const n = typeof v === "number" ? v : Number(v);
    if (!isFinite(n)) return `<c r="${ref}" s="2" t="inlineStr"><is><t>${esc(v)}</t></is></c>`;
    return `<c r="${ref}" s="${tipo === "entero" ? 3 : 4}"><v>${n}</v></c>`;
  }
  if (tipo === "porcentaje") {
    const n = Number(v);
    return isFinite(n) ? `<c r="${ref}" s="8"><v>${n / 100}</v></c>` : `<c r="${ref}" s="2"/>`;
  }
  if (tipo === "fecha" && ES_FECHA.test(String(v))) return `<c r="${ref}" s="5"><v>${serialFecha(v)}</v></c>`;
  if (typeof v === "boolean") v = v ? "sí" : "no";
  if (typeof v === "object") v = JSON.stringify(v);
  return `<c r="${ref}" s="2" t="inlineStr"><is><t xml:space="preserve">${esc(v)}</t></is></c>`;
}

// Excel no acepta más de 31 caracteres ni estos símbolos en el nombre de una hoja.
function nombreHoja(n, i, usados) {
  let s = String(n || `Hoja${i + 1}`).replace(/[\[\]\*\?\/\\:]/g, " ").trim().slice(0, 31) || `Hoja${i + 1}`;
  let base = s, k = 2;
  while (usados.has(s.toLowerCase())) s = `${base.slice(0, 28)} ${k++}`;
  usados.add(s.toLowerCase());
  return s;
}

/**
 * Arma el archivo. `hojas` es [{nombre, columnas, filas, titulo?, subtitulo?}].
 * Devuelve un Buffer listo para mandar con content-type de xlsx.
 */
function armar(hojas) {
  if (!hojas.length) hojas = [{ nombre: "Vacío", columnas: [{ k: "x", t: "Sin datos" }], filas: [] }];
  const usados = new Set();
  const hs = hojas.map((h, i) => ({ nombre: nombreHoja(h.nombre, i, usados), ...hoja(h) }));

  const archivos = [
    ["[Content_Types].xml", `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
${hs.map((h, i) => `<Override PartName="/xl/worksheets/sheet${i + 1}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`).join("\n")}
</Types>`],
    ["_rels/.rels", `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>`],
    ["xl/workbook.xml", `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
<bookViews><workbookView xWindow="0" yWindow="0" windowWidth="28800" windowHeight="16000"/></bookViews>
<sheets>${hs.map((h, i) => `<sheet name="${esc(h.nombre)}" sheetId="${i + 1}" r:id="rId${i + 1}"/>`).join("")}</sheets>
<definedNames>${hs.map((h, i) => `<definedName name="_xlnm._FilterDatabase" localSheetId="${i}" hidden="1">'${esc(h.nombre).replace(/'/g, "''")}'!$A$${h.filaEncabezado}:$${letra(h.ncols - 1)}$${h.ultimaFila}</definedName>`).join("")}</definedNames>
</workbook>`],
    ["xl/_rels/workbook.xml.rels", `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
${hs.map((h, i) => `<Relationship Id="rId${i + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet${i + 1}.xml"/>`).join("\n")}
<Relationship Id="rId${hs.length + 1}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>`],
    ["xl/styles.xml", ESTILOS],
    ...hs.map((h, i) => [`xl/worksheets/sheet${i + 1}.xml`, h.xml])
  ];
  return zip(archivos);
}

const MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";

module.exports = { armar, zip, MIME, deducirTipo };
