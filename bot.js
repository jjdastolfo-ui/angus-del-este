// ─────────────────────────────────────────────────────────────────────────────
// EL BOT
//
// Es Claude con la base en la mano. No hay respuestas armadas ni un menú de
// intenciones: recibe la pregunta, razona, consulta lo que necesita, y contesta.
//
// Lo que lo hace pensar de verdad:
//   · Corre con razonamiento extendido (adaptive thinking) y esfuerzo alto.
//   · Ve lo mismo que el tablero: las herramientas `plantel` y `ficha` devuelven
//     lo que calcula el sistema (estado, eficiencia, bloques), así no lo
//     recalcula por su cuenta ni contradice la pantalla. El SQL queda para
//     todo lo demás.
//   · Tiene memoria: guarda lo que el usuario le enseña del campo (con
//     `recordar`) y las conversaciones quedan en la base, también por WhatsApp.
//   · Habla mientras trabaja: cada consulta y cada pedazo de texto se emite
//     por `onEvento`, para que el tablero lo muestre en vivo.
//
// El prompt está partido en dos: una parte estable (reglas, esquema, memoria)
// que se cachea, y una cola volátil (la fecha, los conteos, el calendario).
// ─────────────────────────────────────────────────────────────────────────────
const Anthropic = require("@anthropic-ai/sdk");

const hoyIso = () => new Date().toISOString().slice(0, 10);

// Precio por millón de tokens: entrada, salida, lectura de caché, escritura de caché.
// Si el modelo no está en la lista, se usa el de Opus 5.
const PRECIOS = {
  "claude-opus-5": [5, 25, 0.5, 6.25], "claude-opus-4-8": [5, 25, 0.5, 6.25], "claude-opus-4-7": [5, 25, 0.5, 6.25],
  "claude-sonnet-5": [2, 10, 0.2, 2.5], "claude-sonnet-4-6": [3, 15, 0.3, 3.75],
  "claude-fable-5-1": [10, 50, 1, 12.5], "claude-haiku-4-5": [1, 5, 0.1, 1.25]
};
function costoUsd(uso, modelo) {
  const p = PRECIOS[modelo] || PRECIOS["claude-opus-5"];
  return Math.round(((uso.input || 0) * p[0] + (uso.output || 0) * p[1] + (uso.cache_read || 0) * p[2] + (uso.cache_creation || 0) * p[3]) / 1e6 * 1e6) / 1e6;
}

function init(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS memoria (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      texto TEXT NOT NULL,
      categoria TEXT,
      usuario TEXT,
      activo INTEGER DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now')));
    CREATE TABLE IF NOT EXISTS conversaciones (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      canal TEXT NOT NULL,
      usuario TEXT NOT NULL,
      role TEXT NOT NULL,
      texto TEXT NOT NULL,
      created_at TEXT DEFAULT (datetime('now')));
    CREATE INDEX IF NOT EXISTS idx_conv ON conversaciones(canal, usuario, id);
    -- Lo que consume cada respuesta del bot, para saber cuánto sale tenerlo andando.
    CREATE TABLE IF NOT EXISTS uso_bot (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fecha TEXT NOT NULL, modelo TEXT, canal TEXT, usuario TEXT,
      entrada INTEGER DEFAULT 0, salida INTEGER DEFAULT 0,
      cache_lectura INTEGER DEFAULT 0, cache_escritura INTEGER DEFAULT 0,
      vueltas INTEGER DEFAULT 0, segundos REAL, usd REAL,
      created_at TEXT DEFAULT (datetime('now')));
    CREATE INDEX IF NOT EXISTS idx_uso_fecha ON uso_bot(fecha);
  `);
}

// ── HERRAMIENTAS ─────────────────────────────────────────────────────────────
// El orden es fijo: cambiarlo rompe el caché del prompt.

const HERRAMIENTAS = [
  {
    name: "plantel",
    description: "Los vientres del campo con lo que el sistema calcula de cada uno: estado de la parición en " +
      "curso (PREÑADA, CRIANDO, DESTETÓ, FALLÓ con su causa, SIN SERVICIO), peso adulto, partos, destete " +
      "promedio, eficiencia (destete sobre peso propio), intervalo entre partos, bloque de parición, tacto, " +
      "fecha probable de parto, ternero del año y notas. Es lo mismo que ve el tablero: usalo como verdad " +
      "para estados y eficiencias en vez de recalcularlos con SQL. Devuelve también el resumen del rodeo.",
    input_schema: {
      type: "object",
      properties: {
        estado: { type: "string", description: "Sólo las de este estado: PREÑADA, CRIANDO, DESTETÓ, FALLÓ, SIN SERVICIO." },
        causa: { type: "string", description: "Con FALLÓ: VACIA, ABORTO, TERNERO_MUERTO, NO_CRIO, MUERTA." },
        rps: { type: "array", items: { type: "string" }, description: "Sólo estos RP." },
        anio: { type: "string", description: "Año de parición a mirar. Por defecto el que está en curso." },
        orden: { type: "string", description: "Campo para ordenar: eficiencia, destete_prom, ipp, edad_meses, peso_adulto, partos…" },
        desc: { type: "boolean", description: "De mayor a menor." },
        limite: { type: "integer", description: "Máximo de filas (por defecto 120)." },
        solo_resumen: { type: "boolean", description: "true: sólo el resumen, sin filas." },
        incluir_destinados: { type: "boolean", description: "true: incluir también a las que tienen destino de salida marcado (por defecto no cuentan como plantel)." }
      }
    }
  },
  {
    name: "ficha",
    description: "Todo lo registrado de UN animal, sea vaca, toro o ternero: datos, origen (madre y padre), " +
      "pesadas con ganancia diaria, destete, sanidad, mediciones, lotes, hijos, notas de campo, destino, y " +
      "si es vientre, su historial campaña por campaña (servicio, tacto, parto, bloque, ternero) con los " +
      "tactos corregidos por la fecha real de nacimiento. Acepta el RP como se escribe en la manga: " +
      "'011', '11', 'b 332' y también la caravana electrónica.",
    input_schema: { type: "object", properties: { rp: { type: "string" } }, required: ["rp"] }
  },
  {
    name: "toros",
    description: "Los toros del campo (reproductores) con lo que dicen de ellos sus hijos: cuántos, cuántos este " +
      "año, peso al nacer y al destete promedio de los hijos, temporadas en que trabajaron, preñez lograda, " +
      "circunferencia escrotal, lote y destino. Es la pestaña Toros del tablero. Un hijo es del toro si su " +
      "padre_rp coincide con el RP o con el nombre del toro.",
    input_schema: { type: "object", properties: {
      estado: { type: "string", description: "ACTIVO (default) o TODOS." },
      anio: { type: "string", description: "Año para contar los hijos del año. Por defecto el actual." },
      incluir_destinados: { type: "boolean", description: "true: también los toros con destino de salida marcado." } } }
  },
  {
    name: "buscar",
    description: "Encuentra animales por RP (tolerando ceros y espacios), caravana, HBA, madre, padre o palabras " +
      "de sus notas. Usalo cuando un RP no aparece con SQL exacto, cuando puede haber más de un animal " +
      "con el mismo número, o para 'los hijos de Hércules'.",
    input_schema: { type: "object", properties: { q: { type: "string" } }, required: ["q"] }
  },
  {
    name: "consultar",
    description: "Ejecuta un SELECT sobre la base del campo y devuelve las filas. Para lo que plantel y ficha " +
      "no cubren: pesadas de un lote, servicios de una temporada, sanidad, cruces raros. Podés llamarlo " +
      "varias veces. Si hay más de 300 filas se recortan y se avisa el total: agrupá o filtrá.",
    input_schema: {
      type: "object",
      properties: {
        sql: { type: "string", description: "Un SELECT. Sólo lectura." },
        porque: { type: "string", description: "Qué estás tratando de averiguar con esto." }
      },
      required: ["sql"]
    }
  },
  {
    name: "escribir",
    description: "Ejecuta un INSERT, UPDATE o DELETE. Sólo cuando te piden cargar o corregir algo que no " +
      "cubre `relevar` (pesadas, sanidad, nacimientos, mediciones y notas van por relevar), y después de " +
      "verificar con consultar o ficha que tiene sentido. Contá siempre qué escribiste.",
    input_schema: {
      type: "object",
      properties: {
        sql: { type: "string", description: "INSERT, UPDATE o DELETE." },
        params: { type: "array", items: {}, description: "Valores para los ? del SQL." },
        que: { type: "string", description: "Qué estás cambiando, en una línea." }
      },
      required: ["sql", "que"]
    }
  },
  {
    name: "relevar",
    description: "Carga datos de campo con validación: pesadas, sanidad, nacimientos, mediciones o notas, " +
      "de a muchos. Verifica que el RP exista (tolera ceros y espacios), avisa si un peso no cierra con la " +
      "historia, evita duplicados y carga todo junto. Con simular=true sólo muestra qué haría: usalo " +
      "primero si hay avisos que el usuario debería ver antes de confirmar.",
    input_schema: {
      type: "object",
      properties: {
        tipo: { type: "string", enum: ["pesadas", "sanidad", "nacimientos", "identificar", "mediciones", "notas"] },
        filas: { type: "array", items: { type: "object" }, description:
          "pesadas: [{rp, peso, fecha?, contexto?}] · nacimientos: [{rp?, caravana_control?, caravana_color?, madre_rp, fecha_nac, sexo, pelo?, peso_nac?, padre_rp?, chip?, observaciones?}] " +
          "(sin rp queda con RP provisorio C+control) · identificar: [{control? | rp_actual?, color?, rp?, chip?}] (asigna el RP definitivo y/o el chip) · " +
          "mediciones: [{rp, valor, tipo?, fecha?}] · notas: [{rp, texto, fecha?}] · sanidad: no usa filas, usa rps/lote_id/todos." },
        rps: { type: "array", items: { type: "string" }, description: "sanidad: a quiénes." },
        lote_id: { type: "integer", description: "sanidad: a todo un lote." },
        todos: { type: "boolean", description: "sanidad: a todos los activos." },
        fecha: { type: "string", description: "Fecha por defecto (AAAA-MM-DD o DD/MM/AAAA). Hoy si no viene." },
        contexto: { type: "string", description: "pesadas: NACIMIENTO, DESTETE, ADULTO, CONTROL, RECRIA, CORRAL…" },
        producto: { type: "string" }, dosis: { type: "string" }, motivo: { type: "string" },
        tipo_medicion: { type: "string", description: "mediciones: CC, CE, ALTURA, FRAME…" },
        simular: { type: "boolean", description: "true: sólo mostrar qué haría." }
      },
      required: ["tipo"]
    }
  },
  {
    name: "crear_tablero",
    description: "Arma una página web propia y la publica en una URL del sistema. Usalo cuando te pidan un " +
      "tablero, un informe visual, o una tabla para mirar en pantalla. Antes de armarlo consultá los datos " +
      "que va a mostrar, así el HTML sale con los números reales adentro.",
    input_schema: {
      type: "object",
      properties: {
        slug: { type: "string", description: "Nombre corto para la URL, sin espacios ni acentos. Ej: 'toros-2026'." },
        titulo: { type: "string", description: "Título que se ve arriba." },
        contenido: { type: "string", description:
          "Sólo el contenido: tablas, párrafos, tarjetas de números. NO pongas <html>, <head>, <style> ni <body>: " +
          "de eso se encarga el sistema. Usá <table> con <thead>/<tbody>, <h2> para secciones, class=\"n\" en " +
          "celdas numéricas, class=\"al\" rojo y class=\"bi\" verde. Para los números grandes de arriba: " +
          "<div class='kpis'><div class='kpi'><b>42</b><span>VIENTRES</span></div></div>. Datos ya calculados adentro, sin scripts." },
        subtitulo: { type: "string", description: "Una línea que aclare qué muestra. Opcional." }
      },
      required: ["slug", "titulo", "contenido"]
    }
  },
  {
    name: "exportar_archivo",
    description: "Arma un archivo para bajar (Excel, CSV o página imprimible) y devuelve el link. Usalo cuando " +
      "pidan 'un excel', 'una planilla', 'un listado para imprimir', 'pasame en csv'. Para los conjuntos que " +
      "ya existen no hace falta SQL: pasá el nombre en conjunto (y rps si es un subconjunto). Si es una " +
      "consulta propia, poné nombres de columna legibles con AS: SELECT rp AS \"RP\", peso AS \"Peso kg\".",
    input_schema: {
      type: "object",
      properties: {
        titulo: { type: "string", description: "Cómo se llama el archivo. Ej: 'Vacías 2026'." },
        sql: { type: "string", description: "Un SELECT con las filas que van al archivo. Opcional si usás conjunto." },
        conjunto: { type: "string", description: "plantel | animales | nacimientos | recria | terminacion | destinos | fallos | pesadas | servicios | sanidad | mediciones | notas | lotes | rodeo (todo, una hoja por conjunto)." },
        rps: { type: "array", items: { type: "string" }, description: "Con conjunto: sólo estos RP." },
        formato: { type: "string", enum: ["xlsx", "csv", "html"], description: "xlsx por defecto. html es una página para imprimir." },
        descripcion: { type: "string", description: "Una línea que diga qué tiene." }
      },
      required: ["titulo"]
    }
  },
  {
    name: "destinar",
    description: "Marca a dónde va un animal (o varios) cuando sale del plantel, o registra que ya salió. " +
      "Destinos: TERMINACION (engorde, corral, gordas), VENTA PREÑADA, VENTA DIRECTA, TORO REPRODUCTOR, " +
      "TORO TERMINACION, NOVILLO TERMINACION, QUEDA. Acepta sinónimos ('engorde' → terminación según el sexo). " +
      "Es lo que muestra la pestaña Destinos del tablero: usalo en vez de escribir SQL en la tabla destinos.",
    input_schema: {
      type: "object",
      properties: {
        rps: { type: "array", items: { type: "string" }, description: "Los RP a marcar." },
        destino: { type: "string", description: "A dónde van. Ej: engorde, terminacion, venta preñada, reproductor, queda." },
        motivo: { type: "string", description: "NO_DESTETO, VACIA, EDAD, PRODUCTIVIDAD, CARACTER, APLOMOS, UBRE, SANIDAD, SELECCION, COMERCIAL." },
        nota: { type: "string" },
        temporada: { type: "string", description: "Año. Por defecto el actual." },
        accion: { type: "string", enum: ["marcar", "sacar", "salida"], description: "marcar (default) · sacar: le quita el destino y vuelve al plantel · salida: ya se fue del campo. Con precio, la venta se manda al sistema financiero." },
        fecha: { type: "string" }, precio: { type: "number", description: "salida: precio por cabeza." },
        precio_total: { type: "number", description: "salida: precio total de todos juntos (alternativa a precio)." },
        comprador: { type: "string", description: "salida: a quién se vendió." }, kg: { type: "number", description: "salida: kilos vendidos en total, si se pesaron." }
      },
      required: ["rps"]
    }
  },
  {
    name: "leer_adjunto",
    description: "Ver más de un archivo que mandó el usuario: las filas siguientes de una planilla (Excel/CSV), " +
      "otra hoja, o el resto de un texto largo. El id viene en el mensaje: [Adjunto 1: planilla \"x.xlsx\" (id 12…)].",
    input_schema: { type: "object", properties: {
      id: { type: "integer" }, hoja: { type: "string", description: "Nombre o número de hoja. Por defecto la primera." },
      desde_fila: { type: "integer", description: "Desde qué fila (0 = la primera)." }, cuantas: { type: "integer", description: "Cuántas (máximo 300)." } },
      required: ["id"] }
  },
  {
    name: "importar_adjunto",
    description: "Carga en la base una planilla que mandó el usuario (Excel, CSV o texto con columnas), con la " +
      "misma validación que relevar: detecta las columnas por sinónimos (RP, peso, fecha, madre, sexo…), avisa " +
      "qué RP no existen y qué no cierra. Primero con simular=true y mostrale al usuario qué entendió; " +
      "después sin simular cuando confirme. Si las columnas no se detectan bien, pasá mapa: {rp: \"Caravana\", peso: \"Kg\"}.",
    input_schema: { type: "object", properties: {
      id: { type: "integer" }, hoja: { type: "string" },
      tipo: { type: "string", enum: ["pesadas", "nacimientos", "sanidad", "mediciones", "notas"], description: "Qué es. Si no viene, se adivina por las columnas." },
      mapa: { type: "object", description: "campo → nombre de columna en la planilla." },
      fecha: { type: "string", description: "Fecha por defecto si la planilla no trae." }, contexto: { type: "string", description: "pesadas: CONTROL, DESTETE…" },
      producto: { type: "string" }, dosis: { type: "string" }, motivo: { type: "string" },
      simular: { type: "boolean" } },
      required: ["id"] }
  },
  {
    name: "campos",
    description: "La empresa entera: sus campos con cabezas, vientres, parición (preñadas, criando, fallaron), " +
      "toros y terminación de cada uno, los totales, y el stock consolidado por categoría. Para 'cuántas " +
      "vacas tenemos en total', 'cómo viene la parición en los tres campos', 'qué campo tiene más terneros'. " +
      "Vos estás parado en un campo; con esto ves los otros.",
    input_schema: { type: "object", properties: { stock: { type: "boolean", description: "true: también el stock consolidado por categoría con kilos." } } }
  },
  {
    name: "trasladar",
    description: "Mueve animales de este campo a otro campo de la misma empresa, con todo su historial (pesadas, " +
      "servicios, sanidad, notas). En el origen quedan como TRASLADADO. Primero con simular=true para ver " +
      "avisos (por ejemplo una vaca con ternero al pie que no viaja) y confirmar.",
    input_schema: { type: "object", properties: {
      rps: { type: "array", items: { type: "string" } },
      hasta: { type: "string", description: "Clave del campo de destino (ver campos)." },
      desde: { type: "string", description: "Clave del campo de origen. Por defecto, este." },
      fecha: { type: "string" }, motivo: { type: "string" }, simular: { type: "boolean" } }, required: ["rps", "hasta"] }
  },
  {
    name: "vinculos",
    description: "Cruza los campos de la empresa: encuentra a las madres y los padres que no están en este campo " +
      "sino en otro, y arregla el vínculo. Un ternero puede estar acá y su madre en otro campo; el sistema " +
      "no la encontraba y quedaba huérfano. Con accion=revisar te dice cuántos hay y cuáles: los que tienen el " +
      "RP escrito distinto, los que están en otro campo, los ambiguos (más de un candidato) y los que no existen " +
      "en ninguna parte. Con accion=arreglar deja anotado el vínculo de todos los casos claros; los ambiguos no " +
      "los toca (esos se resuelven de a uno, diciendo campo y RP). Con accion=buscar encontrás un RP en todos los campos. " +
      "Ojo: muchos padres que \"no existen\" son semen de IATF o toros prestados (KARE 16, IVAR 4): eso no es un error. " +
      "El revisar los marca como probable_externo; con accion=externos se anotan y dejan de figurar.",
    input_schema: { type: "object", properties: {
      accion: { type: "string", enum: ["revisar", "arreglar", "buscar", "externos", "duplicados", "unificar"], description:
        "revisar (default) · arreglar · buscar · externos (marcar padres que son semen o toros de afuera) · " +
        "duplicados (el mismo ternero cargado en dos campos, típico de cargas viejas: uno con un RP armado como \"HB557-21\" y otro con su RP real) · unificar (marcar el repetido como duplicado)" },
      valores: { type: "array", items: { type: "string" }, description: "externos: los nombres a marcar. Sin esto, todos los que el sistema detectó como probables." },
      q: { type: "string", description: "buscar: el RP, nombre o caravana a encontrar en los otros campos." },
      todos_los_campos: { type: "boolean", description: "revisar: mirar todos los campos de la empresa, no sólo éste." },
      filas: { type: "array", items: { type: "object" }, description:
        "arreglar, caso por caso: [{rp: \"el hijo\", relacion: \"madre\"|\"padre\", campo: \"clave del campo donde está\", rp_madre: \"RP real\"}]. " +
        "Sin filas, arregla todo lo inequívoco." },
      simular: { type: "boolean", description: "true: mostrar qué haría, sin escribir." } } }
  },
  {
    name: "finanzas",
    description: "Lee el sistema financiero (IMPROLUX/VIDELA) si está enlazado: el resumen del mes, las " +
      "transacciones (gastos e ingresos por concepto: SANIDAD, ALIMENTO, VENTA HACIENDA…), el stock valuado " +
      "que tiene cargado, cuentas y cheques. Para 'cuánto gastamos en sanidad este ciclo', 'qué se vendió en " +
      "agosto', 'cuánto vale el rodeo según el financiero'. Si no está enlazado, avisá.",
    input_schema: { type: "object", properties: {
      consulta: { type: "string", enum: ["resumen", "transacciones", "ganado", "cuentas", "cheques"] },
      desde: { type: "string", description: "AAAA-MM-DD, para transacciones." }, hasta: { type: "string" },
      concepto: { type: "string", description: "Filtrar por concepto (contiene)." }, texto: { type: "string", description: "Buscar en detalle o proveedor." },
      limite: { type: "integer", description: "Cuántas transacciones devolver (default 200). Los totales siempre son de todas." } } }
  },
  {
    name: "finanzas_registrar",
    description: "Registra un movimiento en el sistema financiero de la empresa: un gasto del campo (sanidad, " +
      "alimento, combustible, personal, flete, servicios) o un ingreso que no sea venta de hacienda (esas van " +
      "por destinar salida). Ej: \"compré 10 frascos de ivermectina, 300 dólares, a Diego Pioli\". Antes de " +
      "escribir, mirá con finanzas qué conceptos se usan en esa empresa y elegí uno de ésos. Mostrale al " +
      "usuario qué vas a registrar (simular=true) y mandalo cuando confirme.",
    input_schema: { type: "object", properties: {
      concepto: { type: "string", description: "El concepto tal como se usa en el financiero: SANIDAD, ALIMENTO, COMBUSTIBLE, PERSONAL, FLETE…" },
      egreso: { type: "number", description: "Monto del gasto, en la moneda del financiero." },
      ingreso: { type: "number", description: "Monto del ingreso, si es una entrada de plata." },
      detalle: { type: "string", description: "Qué fue, en una línea." },
      proveedor: { type: "string" }, fecha: { type: "string", description: "AAAA-MM-DD. Hoy si no viene." },
      es_cc: { type: "boolean", description: "true si queda en cuenta corriente (no se pagó todavía)." },
      simular: { type: "boolean", description: "true: mostrar qué se registraría, sin escribir." } },
      required: ["concepto"] }
  },
  {
    name: "recordar",
    description: "Guarda algo que el usuario te enseña del campo y que va a servir para siempre: cómo llaman " +
      "a un potrero, un criterio de descarte, que tal toro ya no se usa, quién es quién, una corrección " +
      "sobre cómo interpretar un dato. NO guardes datos de animales (eso va a la base) ni cosas de esta " +
      "conversación nada más. Con olvidar_id borrás una memoria que quedó vieja.",
    input_schema: {
      type: "object",
      properties: {
        texto: { type: "string", description: "La memoria, en una o dos frases, tal como la vas a querer leer después." },
        categoria: { type: "string", enum: ["campo", "criterio", "persona", "interpretacion", "otro"] },
        olvidar_id: { type: "integer", description: "Id de una memoria a borrar." }
      }
    }
  }
];

// ── EJECUCIÓN ────────────────────────────────────────────────────────────────

function correrConsulta(db, sql) {
  const limpio = String(sql).trim().replace(/;+\s*$/, "");
  if (!/^select\b/i.test(limpio)) throw new Error("Sólo SELECT en consultar");
  if (/;/.test(limpio)) throw new Error("Una sola consulta por vez");
  const filas = db.prepare(limpio).all();
  if (filas.length > 300) return { filas: filas.slice(0, 300), total: filas.length, recortado: true,
    aviso: `Hay ${filas.length} filas y se muestran 300. Agrupá (GROUP BY) o filtrá para ver el resto.` };
  return { filas, total: filas.length };
}

function correrEscritura(db, sql, params) {
  const limpio = String(sql).trim().replace(/;+\s*$/, "");
  if (!/^(insert|update|delete)\b/i.test(limpio)) throw new Error("Sólo INSERT, UPDATE o DELETE en escribir");
  if (/;/.test(limpio)) throw new Error("Una sola sentencia por vez");
  if (/\bdrop\b|\balter\b|\btruncate\b/i.test(limpio)) throw new Error("No puedo hacer eso");
  const r = db.prepare(limpio).run(...(params || []));
  return { cambios: r.changes, id: r.lastInsertRowid };
}

const CAMPOS_PLANTEL = ["rp", "categoria", "pelo", "edad_meses", "peso_adulto", "partos", "pn_prom", "destete_prom",
  "eficiencia", "ipp", "estado", "causa", "causa_texto", "servicio", "padre_servicio", "tacto", "fpp", "parto",
  "ternero", "peso_nac", "destete", "bloque", "se_atrasa", "padre", "madre"];

// Los errores de la API, dichos como para quien está en el campo.
function errorClaro(e) {
  if (e instanceof Anthropic.AuthenticationError) return "Falta la clave de la API o no es válida (ANTHROPIC_API_KEY).";
  if (e instanceof Anthropic.RateLimitError) return "Demasiadas consultas seguidas: esperá un minuto y probá de nuevo.";
  if (e instanceof Anthropic.BadRequestError) return `La API rechazó el pedido: ${(e.error && e.error.error && e.error.error.message) || e.message}`;
  if (e instanceof Anthropic.APIConnectionError) return "No pude conectar con la API. ¿Hay internet?";
  if (e instanceof Anthropic.InternalServerError) return "La API está con problemas en este momento. Probá en un rato.";
  if (e instanceof Anthropic.APIError) return `La API respondió ${e.status}: ${e.message}`;
  return e.message || String(e);
}

function crear(deps) {
  const { plantelMod, animalesMod, destinosMod, exportarMod, relevarMod, guardarTablero } = deps;
  const adjuntosMod = deps.adjuntosMod || require("./adjuntos.js");
  const finanzasMod = deps.finanzasMod || require("./finanzas.js");
  const empresasDe = typeof deps.empresas === "function" ? deps.empresas : () => deps.empresas || null;
  const vinculosDe = typeof deps.vinculos === "function" ? deps.vinculos : () => deps.vinculos || null;
  const CAMPOS = deps.CAMPOS || {};
  const modelo = deps.modelo || process.env.MODELO || "claude-opus-5";
  // "medium" alcanza para casi todo lo del campo y sale bastante menos que "high".
  // Se sube con la variable ESFUERZO cuando hace falta exprimirlo (xhigh, max).
  const esfuerzo = deps.esfuerzo || process.env.ESFUERZO || "medium";
  const cliente = deps.cliente || new Anthropic();
  const MAX_VUELTAS = 16;

  // ── memoria ────────────────────────────────────────────────────────────
  const memorias = db => { try { return db.prepare("SELECT id, texto, categoria, created_at FROM memoria WHERE activo=1 ORDER BY id").all(); } catch (e) { return []; } };
  function recordar(db, input, usuario) {
    if (input.olvidar_id) {
      const r = db.prepare("UPDATE memoria SET activo=0 WHERE id=?").run(input.olvidar_id);
      return { ok: !!r.changes, mensaje: r.changes ? `Olvidé la memoria ${input.olvidar_id}` : "No había una memoria con ese id" };
    }
    const texto = String(input.texto || "").trim();
    if (!texto) throw new Error("Falta el texto");
    const r = db.prepare("INSERT INTO memoria (texto, categoria, usuario) VALUES (?,?,?)").run(texto, input.categoria || "otro", usuario || null);
    return { ok: true, id: Number(r.lastInsertRowid), mensaje: `Anotado: "${texto}"` };
  }

  // ── conversaciones ─────────────────────────────────────────────────────
  function historial(db, canal, usuario, opciones = {}) {
    if (!canal || !usuario) return [];
    const limite = opciones.limite || 14;
    const desde = opciones.horas ? new Date(Date.now() - opciones.horas * 3600000).toISOString().slice(0, 19).replace("T", " ") : "0000";
    const filas = db.prepare(`SELECT role, texto FROM conversaciones WHERE canal=? AND usuario=? AND created_at>=?
      ORDER BY id DESC LIMIT ?`).all(canal, usuario, desde, limite).reverse();
    // Tiene que empezar con el usuario y alternar; si no, la API se queja.
    const out = [];
    for (const f of filas) {
      if (!out.length && f.role !== "user") continue;
      if (out.length && out[out.length - 1].role === f.role) { out[out.length - 1].content += "\n" + f.texto; continue; }
      out.push({ role: f.role, content: f.texto });
    }
    if (out.length && out[out.length - 1].role === "user") out.pop();
    return out;
  }
  function guardarTurno(db, canal, usuario, role, texto) {
    if (!canal || !usuario || !texto) return;
    try { db.prepare("INSERT INTO conversaciones (canal, usuario, role, texto) VALUES (?,?,?,?)").run(canal, usuario, role, String(texto)); } catch (e) {}
  }
  function conversacion(db, canal, usuario, limite = 40) {
    try { return db.prepare("SELECT id, role, texto, created_at FROM conversaciones WHERE canal=? AND usuario=? ORDER BY id DESC LIMIT ?").all(canal, usuario, limite).reverse(); }
    catch (e) { return []; }
  }

  // ── el prompt ──────────────────────────────────────────────────────────
  // Sin conteos: los conteos cambian con cada carga y romperían el caché.
  function esquema(db) {
    const tablas = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`).all().map(t => t.name);
    return tablas.map(t => `${t} (${db.prepare(`PRAGMA table_info(${t})`).all().map(c => `${c.name} ${c.type}`).join(", ")})`).join("\n");
  }
  function conteos(db) {
    const tablas = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`).all().map(t => t.name);
    return tablas.map(t => `${t}: ${db.prepare(`SELECT COUNT(*) n FROM ${t}`).get().n}`).join(" · ");
  }

  function parteEstable(db, campoNombre) {
    const mem = memorias(db);
    return `Sos el asistente de ${campoNombre}, una cabaña de Angus. Trabajás para la gente del campo: el dueño, el encargado, el veterinario.

Tenés acceso directo a la base del campo. Consultá lo que necesites antes de responder: no adivines ni respondas de memoria.

ESTRUCTURA DE LA BASE (columnas):
${esquema(db)}

QUÉ HERRAMIENTA PARA QUÉ:
· plantel: estados, eficiencias, bloques, quién falló y por qué. Es lo que ve el tablero: la verdad para esas cosas. No las recalcules con SQL.
· toros: los reproductores y el desempeño de sus hijos. La pestaña Toros del tablero.
· La pestaña Terminación del tablero muestra a los que están en un lote de corral y a los marcados con destino terminación (destinar). La recría son los de 6 a 20 meses.
· ficha: todo de un animal, con su historial campaña por campaña.
· buscar: cuando un RP no aparece exacto o puede haber dos con el mismo número.
· consultar: SQL para lo que plantel y ficha no cubren. Un SELECT por vez.
· relevar: cargar pesadas, sanidad, nacimientos, mediciones y notas. Antes que escribir.
· escribir: correcciones puntuales que relevar no cubre, después de verificar.
· destinar: marcar a dónde va un animal (engorde/terminación, venta, reproductor, queda) o que ya salió. Nunca por SQL.
· crear_tablero: algo para mirar en pantalla. exportar_archivo: algo para bajar (Excel, CSV, imprimir).
· recordar: lo que el usuario te enseña del campo y va a servir siempre.
· finanzas: el sistema financiero de la empresa, si está enlazado: gastos por concepto, ventas, stock valuado, cuentas, cheques.
· finanzas_registrar: cargar un gasto o un ingreso ahí. Las ventas de hacienda NO: ésas van por destinar salida con el precio, y se mandan solas.

ATENDÉS LAS DOS COSAS: lo del campo (animales, pesadas, sanidad aplicada, nacimientos) va a la base ganadera; lo de plata (cuánto se gastó, qué se compró, cuánto entró) va al financiero de esta empresa. Un mismo hecho puede ser las dos: "vacuné 80 vacas con ivermectina que compré a 300 dólares" es una aplicación de sanidad (relevar) y un gasto (finanzas_registrar). Hacé las dos y contá las dos. Si el financiero no está enlazado, decilo y cargá igual lo del campo.
· campos: la empresa entera, campo por campo, cuando preguntan por el total o comparan campos. Vos estás parado en un campo: plantel, ficha y consultar miran sólo éste.
· trasladar: mover animales a otro campo de la empresa. Simulá primero.
· vinculos: cuando una madre o un padre no aparecen, antes de decir que no existen fijate si están en otro campo de la empresa. Con accion=buscar encontrás un RP en todos los campos; con revisar ves todos los huérfanos del campo; con arreglar los dejás vinculados.

MADRES Y PADRES DE OTRO CAMPO: los campos de una empresa comparten animales. Un ternero puede estar acá y su madre en otro campo. Si ficha te dice que la madre no existe, NO concluyas que está mal cargada: usá vinculos (buscar) antes. Cuando el vínculo queda anotado, la ficha muestra a la madre con su campo, y en la ficha de la madre aparecen los hijos que tiene en los otros campos.
· leer_adjunto / importar_adjunto: cuando mandan un archivo. Fotos y PDF los ves directo; planillas y textos llegan resumidos con un id.

ARCHIVOS QUE TE MANDAN: primero decí en una línea qué es y qué tiene (una planilla de pesadas con 40 filas, la foto de una libreta con RP y pesos, un informe del veterinario). Después hacé lo que corresponda:
· Planilla o foto con datos para cargar → relevar o importar_adjunto con simular=true, mostrá qué entendiste y los avisos, y cargá cuando confirmen. En una foto, leé cada renglón con cuidado: un 3 y un 8 se confunden; si dudás de un número, decilo.
· Un PDF o informe → contá lo que importa para el campo y cruzalo con la base si sirve (un análisis de un toro, una liquidación de venta).
· Si el archivo no se pudo leer, decí por qué y qué formato mandar.

LO QUE SABÉS DE GANADERÍA y no hace falta que nadie te cargue:
· La gestación de un bovino son 283 días.
· Una vaca desteta un ternero por año. El destete es a los 6-8 meses del parto.
· Un tacto "PREÑADA" dice que estaba preñada ESE DÍA. Va a parir unos nueve meses y medio después del SERVICIO, no del tacto.
· Cabeza, cuerpo y cola son tramos de la parición. Cuanto antes pare, más pesado llega el ternero al destete.
· Lo que mide de verdad a una vaca es cuánto desteta EN RELACIÓN A SU PROPIO PESO: una de 430 kg que desteta 255 rinde 59%, mejor que una de 600 que desteta 250 (42%), porque come menos todo el año.
· De dónde vino una preñez se confirma con la fecha de nacimiento: ±10 días de la fecha probable de la IATF es IATF; después, tramos de 20 días son toro cabeza, cuerpo y cola.

EL ERROR QUE NO PODÉS COMETER: si una vaca figura preñada y no tiene cría registrada, NO concluyas que abortó sin mirar CUÁNDO fue el servicio. Si fue hace menos de nueve meses, esa vaca simplemente todavía no parió. plantel ya distingue PREÑADA (esperando) de FALLÓ por ABORTO (vencida): confiá en eso.

CÓMO TRABAJAR:
· Consultá primero, respondé después. Cruzá datos si hace falta. Verificá lo que no te cierra desde otro ángulo.
· Decí lo que concluís y en qué te basás, con las fechas y los números en la mano.
· Si los datos no alcanzan para responder, decilo. No inventes. Si una estimación es una estimación, decilo.
· Si un pedido es ambiguo (dos animales con el mismo número, "la vaca esa", una fecha que puede ser de dos años), preguntá antes de actuar. Para consultar no hace falta preguntar: mirá y contá lo que hay.
· Si encontrás algo mal cargado, avisalo aunque no te lo hayan preguntado: una madre más joven que su cría, una vaca con dos crías el mismo año, un 1970 en una fecha, un peso que no cierra, un RP repetido.
· Cuando te pidan cargar o corregir, verificá que exista y tenga sentido, escribí, y contá qué hiciste. Si relevar devuelve avisos importantes (un peso que bajó mucho, una madre que ya tiene cría este año), mostráselos y preguntá antes de cargar: primero simular=true, después sin simular.
· Cuando te corrigen ("la 23 no tiene ternero", "el RP correcto es otro") no es una pregunta: es una corrección. Verificá qué hay cargado, mostrale lo que encontraste, y proponé el cambio concreto antes de hacerlo.
· Cuando el usuario te cuenta algo del campo que vale para siempre ("al potrero 7 le decimos La Loma", "Hércules ya no se usa", "las vaquillonas se sirven a los 15 meses"), guardalo con recordar y decile que lo anotaste.
· El RP se escribe de cualquier forma: "011", "11", "b 332", "B332" son el mismo animal. Si un SELECT exacto no lo encuentra, usá buscar o ficha.
· CÓMO SE IDENTIFICA UN ANIMAL EN ESTE CAMPO: al nacer, el ternero recibe una caravana control (un número al azar y un color). Se carga el nacimiento con esa control (relevar nacimientos con caravana_control y caravana_color, sin rp) y queda con un RP provisorio "C"+número, marcado como sin RP. Más adelante se le asigna el RP definitivo y se le pone el chip: relevar identificar con {control, rp, chip}. "La control 150 es la 2077" o "al C150 ponele el chip 3201…" van por ahí. Si hay dos con el mismo número de control, preguntá el color. Todo lo que tenía (madre, pesadas, notas) sigue con el animal.
· Podés llamar varias herramientas a la vez cuando son independientes.

ARMAR TABLEROS: consultá los datos primero y ponelos ya calculados adentro. Mandá sólo el contenido; el sistema pone estilos y encabezado. Después decile en qué URL quedó y qué muestra.

ARCHIVOS: devolvé el link tal cual te lo da la herramienta (empieza con /archivos/). Para subconjuntos, consultá primero y mandá los RP en rps, o directamente el SELECT con columnas legibles.

${destinosMod ? destinosMod.INSTRUCCIONES + "\n" : ""}
CÓMO HABLAR: como un asesor que conoce el campo. Frases cortas, sin listas de más, sin repetir la pregunta. Números concretos. Español rioplatense. Si la respuesta es larga, lo importante primero.

LO QUE TE ENSEÑARON DE ESTE CAMPO (memoria, con su id por si hay que corregirla):
${mem.length ? mem.map(m => `[${m.id}] (${m.categoria}) ${m.texto}`).join("\n") : "(todavía nada)"}`;
  }

  function parteVolatil(db) {
    const cal = plantelMod.calendario(db);
    return `HOY ES ${hoyIso()}.

REGISTROS EN LA BASE: ${conteos(db)}

EL CALENDARIO DE ESTE CAMPO, sacado de sus propios registros:
Servicios: ${cal.servicios.map(s => `${s.temporada} (${s.desde} a ${s.hasta}, ${s.n} vientres)`).join(" · ") || "sin datos"}
Pariciones: ${cal.pariciones.map(p => `${p.anio} (${p.primero} a ${p.ultimo}, ${p.n} terneros)`).join(" · ") || "sin datos"}
${cal.cortes ? `Bloques de la parición en curso: cabeza hasta ${cal.cortes.CABEZA}, cuerpo hasta ${cal.cortes.CUERPO}, cola hasta ${cal.cortes.COLA} (referencia: ${cal.cortes.origen}).` : ""}`;
  }

  const instrucciones = (db, campoNombre) => parteEstable(db, campoNombre) + "\n\n" + parteVolatil(db);

  // ── herramientas: qué hace cada una ────────────────────────────────────
  async function ejecutar(db, nombre, input, ctx) {
    const usuario = ctx.usuario || null;
    switch (nombre) {
      case "plantel": {
        const p = plantelMod.plantel(db, { anio: input.anio, incluirDestinados: !!input.incluir_destinados, criasFuera: deps.criasFuera ? deps.criasFuera(ctx.campoKey) : undefined });
        let filas = p.filas;
        if (input.estado) filas = filas.filter(f => f.estado === String(input.estado).toUpperCase());
        if (input.causa) filas = filas.filter(f => f.causa === String(input.causa).toUpperCase());
        if (Array.isArray(input.rps) && input.rps.length) { const set = new Set(input.rps.map(animalesMod.compacto)); filas = filas.filter(f => set.has(animalesMod.compacto(f.rp))); }
        if (input.orden) { const c = input.orden, d = input.desc ? -1 : 1; filas = [...filas].sort((a, b) => (a[c] == null) - (b[c] == null) || (typeof a[c] === "number" ? (a[c] - b[c]) * d : String(a[c]).localeCompare(String(b[c]), "es", { numeric: true }) * d)); }
        const limite = input.limite || 120;
        const compactas = filas.slice(0, limite).map(f => { const o = {}; for (const k of CAMPOS_PLANTEL) if (f[k] != null && f[k] !== false) o[k] = f[k]; if (f.notas && f.notas.length) o.notas = f.notas.slice(0, 3).map(n => `${n.fecha} ${n.texto}`); return o; });
        return { anio_paricion: p.anio_paricion, resumen: p.resumen, total: filas.length,
          ...(input.solo_resumen ? {} : { filas: compactas, recortado: filas.length > limite ? `se muestran ${limite} de ${filas.length}` : undefined }) };
      }
      case "ficha": {
        const g = animalesMod.ficha(db, input.rp);
        if (!g.ok) return g;
        const v = g.es_vientre ? plantelMod.ficha(db, g.rp, { criasFuera: deps.criasFuera ? deps.criasFuera(ctx.campoKey) : undefined }) : { ok: false };
        const f = v.ok ? { ...g, ...v } : g;
        // Recortes para no inflar el contexto: lo reciente y lo relevante.
        return { ...f, pesadas: (f.pesadas || []).slice(-24), sanidad: (f.sanidad || []).slice(0, 12), mediciones: (f.mediciones || []).slice(0, 12), _id: undefined, _crias: undefined, _servicios: undefined };
      }
      case "toros": return animalesMod.toros(db, { estado: input.estado, anio: input.anio, incluirDestinados: !!input.incluir_destinados,
        hijosFuera: deps.hijosFuera ? deps.hijosFuera(ctx.campoKey) : undefined });
      case "buscar": return { resultados: animalesMod.buscar(db, input.q, { limite: 20 }) };
      case "consultar": return correrConsulta(db, input.sql);
      case "escribir":
        if (ctx.soloLectura) throw new Error("Esta sesión es de sólo lectura");
        return correrEscritura(db, input.sql, input.params);
      case "relevar":
        if (ctx.soloLectura) throw new Error("Esta sesión es de sólo lectura");
        return relevarDesdeBot(db, input);
      case "crear_tablero":
        if (ctx.soloLectura) throw new Error("Esta sesión es de sólo lectura");
        return guardarTablero(db, input, ctx.campoKey);
      case "exportar_archivo": return exportarDesdeBot(db, input, ctx);
      case "destinar": {
        if (ctx.soloLectura) throw new Error("Esta sesión es de sólo lectura");
        if (!destinosMod) throw new Error("El módulo de destinos no está disponible");
        const rps = Array.isArray(input.rps) ? input.rps : [input.rps].filter(Boolean);
        if (!rps.length) throw new Error("Falta al menos un RP");
        const op = { motivo: input.motivo, nota: input.nota, temporada: input.temporada, usuario: usuario || "bot", fecha: input.fecha, precio: input.precio };
        if (input.accion === "sacar") return { resultados: rps.map(rp => destinosMod.sacar(db, rp, input.temporada)) };
        if (input.accion === "salida") return deps.registrarSalida
          ? deps.registrarSalida(db, { rps, fecha: input.fecha, precio: input.precio, precio_total: input.precio_total, comprador: input.comprador, kg: input.kg, temporada: input.temporada, campo: ctx.campoKey })
          : { resultados: rps.map(rp => destinosMod.concretar(db, rp, op)) };
        if (!input.destino) throw new Error("Falta el destino");
        return destinosMod.marcarVarios(db, rps, input.destino, op);
      }
      case "vinculos": {
        const v = vinculosDe(); if (!v) throw new Error("Los vínculos entre campos no están disponibles");
        const k = ctx.campoKey;
        if (input.accion === "buscar") return { resultados: v.buscarEnEmpresa(k, input.q) };
        if (input.accion === "arreglar") {
          if (ctx.soloLectura && !input.simular) throw new Error("Esta sesión es de sólo lectura");
          return v.aplicar(k, { filas: input.filas, simular: input.simular, usuario });
        }
        if (input.accion === "duplicados") return v.duplicados(k, { fresco: true });
        if (input.accion === "unificar") {
          if (ctx.soloLectura && !input.simular) throw new Error("Esta sesión es de sólo lectura");
          return v.unificar(k, { pares: input.filas, simular: input.simular, usuario });
        }
        if (input.accion === "externos") {
          if (ctx.soloLectura && !input.simular) throw new Error("Esta sesión es de sólo lectura");
          return v.marcarExternos(k, { valores: input.valores, simular: input.simular });
        }
        return input.todos_los_campos ? v.revisarEmpresa(k) : v.revisar(k, { fresco: true });
      }
      case "finanzas": { const em = empresasDe(); return finanzasMod.consultar(db, input, em && ctx.campoKey ? em.finanzasDe(ctx.campoKey) : undefined); }
      case "finanzas_registrar": {
        if (ctx.soloLectura && !input.simular) throw new Error("Esta sesión es de sólo lectura");
        const em = empresasDe();
        return finanzasMod.registrarMovimiento(db, input, em && ctx.campoKey ? em.finanzasDe(ctx.campoKey) : undefined);
      }
      case "campos": {
        const em = empresasDe(); if (!em) throw new Error("No hay empresas configuradas");
        const e = em.empresaDe(ctx.campoKey);
        const r = em.resumen(e.key);
        return input.stock ? { ...r, stock: em.rodeoResumen(e.key) } : r;
      }
      case "trasladar": {
        if (ctx.soloLectura && !input.simular) throw new Error("Esta sesión es de sólo lectura");
        const em = empresasDe(); if (!em) throw new Error("No hay empresas configuradas");
        return em.trasladar({ rps: input.rps, desde: input.desde || ctx.campoKey, hasta: input.hasta, fecha: input.fecha, motivo: input.motivo, simular: input.simular, usuario: usuario || "bot" });
      }
      case "leer_adjunto": return adjuntosMod.leer(db, input);
      case "importar_adjunto":
        if (ctx.soloLectura && !input.simular) throw new Error("Esta sesión es de sólo lectura");
        return adjuntosMod.importar(db, plantelMod, { ...input, usuario: usuario || "bot" });
      case "recordar":
        if (ctx.soloLectura) throw new Error("Esta sesión es de sólo lectura");
        return recordar(db, input, usuario);
      default: return { error: "herramienta desconocida" };
    }
  }

  function exportarDesdeBot(db, input, ctx = {}) {
    const campoNombre = (CAMPOS[ctx.campoKey] || {}).nombre || ctx.campoNombre || "";
    const formato = ["xlsx", "csv", "html"].includes(input.formato) ? input.formato : "xlsx";
    if (input.conjunto && !input.sql) {
      const a = exportarMod.armar(db, { plantelMod, animalesMod, destinosMod }, String(input.conjunto).toLowerCase(), formato,
        { rps: input.rps, campoNombre, filtro: input.descripcion });
      const g = exportarMod.guardarArchivo(db, { nombre: exportarMod.nombreArchivo(input.titulo || a.nombre, formato), mime: a.mime,
        buffer: a.buffer, descripcion: input.descripcion || input.titulo, creado_por: "bot" });
      return { ...g, filas: input.rps ? input.rps.length : null, mensaje: `Listo: ${g.url}` };
    }
    if (!input.sql) throw new Error("Falta el SELECT o el conjunto");
    const r = exportarMod.desdeConsulta(db, { sql: input.sql, titulo: input.titulo, formato, descripcion: input.descripcion, creado_por: "bot", campoNombre });
    return { ...r, mensaje: `Listo: ${r.url} (${r.filas} filas)` };
  }

  function relevarDesdeBot(db, input) {
    const t = String(input.tipo || "").toLowerCase();
    const filas = Array.isArray(input.filas) ? input.filas : [];
    if (t === "pesadas") return relevarMod.pesadas(db, { filas, fecha: input.fecha, contexto: input.contexto, simular: input.simular, usuario: "bot" });
    if (t === "nacimientos") return relevarMod.nacimientos(db, { filas, simular: input.simular, usuario: "bot" });
    if (t === "identificar") return relevarMod.identificar(db, { filas, simular: input.simular, usuario: "bot" });
    if (t === "mediciones") return relevarMod.mediciones(db, { filas, tipo: input.tipo_medicion, fecha: input.fecha, simular: input.simular });
    if (t === "notas") return relevarMod.notas(db, plantelMod, { filas, fecha: input.fecha, simular: input.simular, usuario: "bot" });
    if (t === "sanidad") return relevarMod.sanidad(db, { rps: input.rps, lote_id: input.lote_id, todos: input.todos, fecha: input.fecha,
      producto: input.producto, dosis: input.dosis, motivo: input.motivo, simular: input.simular });
    throw new Error(`Tipo "${input.tipo}" no. Puede ser pesadas, sanidad, nacimientos, identificar, mediciones o notas`);
  }

  // Cómo se cuenta cada paso en el tablero.
  function pasoDe(nombre, input, out) {
    if (nombre === "consultar") return { tipo: "consulta", herramienta: nombre, sql: input.sql, porque: input.porque, filas: out.total };
    if (nombre === "plantel") return { tipo: "consulta", herramienta: nombre, porque: `plantel${input.estado ? " " + input.estado : ""}`, filas: out.total };
    if (nombre === "ficha") return { tipo: "consulta", herramienta: nombre, porque: `ficha de ${out.rp || input.rp}` };
    if (nombre === "buscar") return { tipo: "consulta", herramienta: nombre, porque: `buscar "${input.q}"`, filas: (out.resultados || []).length };
    if (nombre === "toros") return { tipo: "consulta", herramienta: nombre, porque: "toros", filas: (out.filas || []).length };
    if (nombre === "escribir") return { tipo: "escritura", herramienta: nombre, que: input.que, cambios: out.cambios };
    if (nombre === "relevar") return { tipo: input.simular ? "consulta" : "escritura", herramienta: nombre, que: `relevar ${input.tipo}`, cambios: out.bien, porque: out.mensaje };
    if (nombre === "crear_tablero") return { tipo: "tablero", herramienta: nombre, slug: out.slug, url: out.url };
    if (nombre === "exportar_archivo") return { tipo: "archivo", herramienta: nombre, nombre: out.nombre, url: out.url, filas: out.filas };
    if (nombre === "destinar") return { tipo: "escritura", herramienta: nombre, que: out.mensaje || `destinar ${input.accion || "marcar"}`, cambios: (out.hechos || out.resultados || []).length };
    if (nombre === "vinculos") return { tipo: (input.accion === "arreglar" && !input.simular) ? "escritura" : "consulta", herramienta: nombre,
      que: out.mensaje, porque: `vínculos: ${input.accion || "revisar"}`, cambios: out.bien, filas: out.resumen ? out.resumen.total : (out.resultados || []).length };
    if (nombre === "finanzas") return { tipo: "consulta", herramienta: nombre, porque: `finanzas: ${input.consulta || "resumen"}`, filas: out && out.total };
    if (nombre === "finanzas_registrar") return { tipo: input.simular ? "consulta" : "escritura", herramienta: nombre, que: out.mensaje || out.motivo, cambios: out.ok ? 1 : 0 };
    if (nombre === "campos") return { tipo: "consulta", herramienta: nombre, porque: "la empresa entera", filas: out && out.campos && out.campos.length };
    if (nombre === "trasladar") return { tipo: input.simular ? "consulta" : "escritura", herramienta: nombre, que: out.mensaje, cambios: out.bien };
    if (nombre === "leer_adjunto") return { tipo: "consulta", herramienta: nombre, porque: `leer adjunto ${input.id}`, filas: out.mostradas };
    if (nombre === "importar_adjunto") return { tipo: input.simular ? "consulta" : "escritura", herramienta: nombre, que: `importar ${out.adjunto || input.id}`, cambios: out.bien, porque: out.mensaje };
    if (nombre === "recordar") return { tipo: "memoria", herramienta: nombre, que: out.mensaje };
    return { tipo: "consulta", herramienta: nombre };
  }
  const describir = (nombre, input) => {
    if (nombre === "consultar") return input.porque || "consulto la base";
    if (nombre === "plantel") return `miro el plantel${input.estado ? " (" + input.estado + ")" : ""}`;
    if (nombre === "ficha") return `abro la ficha de ${input.rp}`;
    if (nombre === "buscar") return `busco "${input.q}"`;
    if (nombre === "toros") return "miro los toros";
    if (nombre === "escribir") return input.que || "escribo en la base";
    if (nombre === "relevar") return `${input.simular ? "reviso" : "cargo"} ${input.tipo}`;
    if (nombre === "crear_tablero") return `armo el tablero "${input.titulo}"`;
    if (nombre === "exportar_archivo") return `armo el archivo "${input.titulo}"`;
    if (nombre === "destinar") return input.accion === "sacar" ? `le saco el destino a ${(input.rps || []).length} animal(es)` : input.accion === "salida" ? `registro la salida de ${(input.rps || []).length} animal(es)` : `marco ${(input.rps || []).length} animal(es) → ${input.destino}`;
    if (nombre === "vinculos") return input.accion === "arreglar" ? "arreglo los vínculos entre campos"
      : input.accion === "buscar" ? `busco "${input.q}" en todos los campos`
      : input.accion === "duplicados" ? "busco terneros cargados dos veces"
      : input.accion === "unificar" ? "unifico los terneros repetidos"
      : "reviso las madres y padres de otros campos";
    if (nombre === "finanzas") return `consulto el financiero (${input.consulta || "resumen"})`;
    if (nombre === "finanzas_registrar") return input.simular ? "preparo el movimiento" : `registro ${input.concepto} en el financiero`;
    if (nombre === "campos") return "miro todos los campos de la empresa";
    if (nombre === "trasladar") return `${input.simular ? "reviso el traslado de" : "traslado"} ${(input.rps || []).length} animal(es) a ${input.hasta}`;
    if (nombre === "leer_adjunto") return "leo más del archivo";
    if (nombre === "importar_adjunto") return input.simular ? "reviso la planilla" : "cargo la planilla";
    if (nombre === "recordar") return input.olvidar_id ? "borro una memoria" : "anoto en la memoria";
    return nombre;
  };

  /**
   * La conversación. `mensajes` son los turnos previos más el actual (formato
   * de la API). Si viene canal+usuario y no viene historia, la historia sale
   * de la base. `onEvento` recibe {tipo: "vuelta"|"texto"|"pensando"|"paso"|"fin", ...}.
   */
  async function conversar(db, campoNombre, mensajes, opciones = {}) {
    const ctx = { campoKey: opciones.campoKey, campoNombre, soloLectura: opciones.soloLectura, usuario: opciones.usuario };
    const emitir = e => { try { opciones.onEvento && opciones.onEvento(e); } catch (x) {} };
    const t0 = Date.now();
    const pasos = [];
    const uso = { input: 0, output: 0, cache_read: 0, cache_creation: 0 };
    const historia = [...mensajes];
    const ultimoUsuario = [...historia].reverse().find(m => m.role === "user");
    const textoUsuario = !ultimoUsuario ? "" : typeof ultimoUsuario.content === "string" ? ultimoUsuario.content
      : ultimoUsuario.content.map(b => b.type === "text" ? (b.text.startsWith("[Adjunto") ? b.text.split("\n")[0] : b.text) : b.type === "image" ? "[foto]" : b.type === "document" ? "[pdf]" : "").filter(Boolean).join(" ");

    const system = [
      { type: "text", text: parteEstable(db, campoNombre), cache_control: { type: "ephemeral" } },
      { type: "text", text: parteVolatil(db) }
    ];
    let respuesta = "", motivo = null;

    for (let vuelta = 0; vuelta < MAX_VUELTAS; vuelta++) {
      const ultima = vuelta === MAX_VUELTAS - 1;
      emitir({ tipo: "vuelta", n: vuelta + 1 });
      const stream = cliente.messages.stream({
        model: modelo,
        max_tokens: 16000,
        thinking: { type: "adaptive", display: "summarized" },
        output_config: { effort: esfuerzo },
        system: ultima
          ? [...system, { type: "text", text: "SE TE ACABÓ EL TIEMPO DE CONSULTAR. Respondé ahora con lo que averiguaste. Si te falta algo, decí qué encontraste y qué te falta." }]
          : system,
        ...(ultima ? {} : { tools: HERRAMIENTAS }),
        messages: historia
      });
      let textoVuelta = "";
      stream.on("text", delta => { textoVuelta += delta; emitir({ tipo: "texto", delta }); });
      stream.on("streamEvent", ev => {
        if (ev.type === "content_block_delta" && ev.delta && ev.delta.type === "thinking_delta" && ev.delta.thinking) emitir({ tipo: "pensando", delta: ev.delta.thinking });
      });
      let msg;
      try { msg = await stream.finalMessage(); }
      catch (e) { throw new Error(errorClaro(e)); }
      if (msg.usage) {
        uso.input += msg.usage.input_tokens || 0; uso.output += msg.usage.output_tokens || 0;
        uso.cache_read += msg.usage.cache_read_input_tokens || 0; uso.cache_creation += msg.usage.cache_creation_input_tokens || 0;
      }
      const usos = (msg.content || []).filter(c => c.type === "tool_use");

      if (msg.stop_reason === "refusal") {
        respuesta = textoVuelta.trim() || "No puedo ayudarte con eso."; motivo = "refusal"; break;
      }
      if (msg.stop_reason === "pause_turn") { historia.push({ role: "assistant", content: msg.content }); continue; }
      if (!usos.length) {
        respuesta = textoVuelta.trim(); motivo = msg.stop_reason;
        if (msg.stop_reason === "max_tokens") respuesta += "\n\n(me quedé sin espacio: pedime que siga)";
        break;
      }

      // Lo que dijo antes de usar herramientas queda como paso, no como respuesta.
      if (textoVuelta.trim()) pasos.push({ tipo: "nota", texto: textoVuelta.trim() });
      historia.push({ role: "assistant", content: msg.content });
      const resultados = [];
      for (const u of usos) {
        emitir({ tipo: "paso", herramienta: u.name, texto: describir(u.name, u.input || {}) });
        let out;
        try {
          out = await ejecutar(db, u.name, u.input || {}, ctx);
          pasos.push(pasoDe(u.name, u.input || {}, out || {}));
        } catch (e) {
          out = { error: e.message };
          pasos.push({ tipo: "error", herramienta: u.name, detalle: e.message });
          emitir({ tipo: "paso", herramienta: u.name, texto: `no pude: ${e.message}`, error: true });
        }
        resultados.push({ type: "tool_result", tool_use_id: u.id, content: JSON.stringify(out), ...(out && out.error ? { is_error: true } : {}) });
      }
      historia.push({ role: "user", content: resultados });
    }

    if (!respuesta) {
      const consultas = pasos.filter(p => p.tipo === "consulta");
      respuesta = `Revisé la base ${consultas.length} veces pero no llegué a una conclusión. ` +
        `Estuve mirando: ${consultas.slice(0, 4).map(c => c.porque || "datos").join("; ")}. Probá siendo más específico.`;
    }
    if (opciones.canal && opciones.usuario) {
      guardarTurno(db, opciones.canal, opciones.usuario, "user", textoUsuario);
      guardarTurno(db, opciones.canal, opciones.usuario, "assistant", respuesta);
    }
    const usd = costoUsd(uso, modelo);
    try {
      db.prepare(`INSERT INTO uso_bot (fecha, modelo, canal, usuario, entrada, salida, cache_lectura, cache_escritura, vueltas, segundos, usd)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)`).run(hoyIso(), modelo, opciones.canal || "web", opciones.usuario || null,
        uso.input, uso.output, uso.cache_read, uso.cache_creation, pasos.length, Math.round((Date.now() - t0) / 100) / 10, usd);
    } catch (e) {}
    uso.usd = usd;
    emitir({ tipo: "fin", respuesta, pasos, uso });
    return { respuesta, pasos, uso, modelo, motivo };
  }

  /** Un mensaje nuevo con la historia sacada de la base. */
  async function responder(db, campoNombre, texto, opciones = {}) {
    const previos = Array.isArray(opciones.historia) ? opciones.historia
      : historial(db, opciones.canal, opciones.usuario, { horas: opciones.canal === "whatsapp" ? 48 : null });
    return conversar(db, campoNombre, [...previos, { role: "user", content: texto }], opciones);
  }

  /** Cuánto se gastó: hoy, este mes, y el detalle de los últimos días. */
  function uso(db, opciones = {}) {
    const q = (sql, ...p) => { try { return db.prepare(sql).all(...p); } catch (x) { return []; } };
    const desde = opciones.desde || new Date(Date.now() - 30 * 86400000).toISOString().slice(0, 10);
    const hasta = opciones.hasta || hoyIso();
    const filas = q(`SELECT fecha, COUNT(*) consultas, SUM(entrada) entrada, SUM(salida) salida, SUM(cache_lectura) cache_lectura,
      SUM(cache_escritura) cache_escritura, SUM(usd) usd FROM uso_bot WHERE fecha BETWEEN ? AND ? GROUP BY fecha ORDER BY fecha DESC`, desde, hasta);
    const porCanal = q(`SELECT canal, COUNT(*) consultas, SUM(usd) usd FROM uso_bot WHERE fecha BETWEEN ? AND ? GROUP BY canal ORDER BY usd DESC`, desde, hasta);
    const suma = k => filas.reduce((s, f) => s + (Number(f[k]) || 0), 0);
    const hoy = filas.find(f => f.fecha === hoyIso()) || {};
    const mes = filas.filter(f => f.fecha.startsWith(hoyIso().slice(0, 7)));
    const r2 = n => Math.round((n || 0) * 100) / 100;
    return { modelo, desde, hasta,
      hoy: { consultas: hoy.consultas || 0, usd: r2(hoy.usd) },
      mes: { consultas: mes.reduce((s, f) => s + f.consultas, 0), usd: r2(mes.reduce((s, f) => s + (f.usd || 0), 0)) },
      periodo: { consultas: suma("consultas"), usd: r2(suma("usd")), entrada: suma("entrada"), salida: suma("salida"),
        cache_lectura: suma("cache_lectura"), cache_escritura: suma("cache_escritura"),
        promedio_por_consulta: suma("consultas") ? Math.round(suma("usd") / suma("consultas") * 10000) / 10000 : 0,
        ahorro_cache: r2(suma("cache_lectura") / 1e6 * ((PRECIOS[modelo] || PRECIOS["claude-opus-5"])[0] - (PRECIOS[modelo] || PRECIOS["claude-opus-5"])[2])) },
      por_dia: filas.map(f => ({ ...f, usd: r2(f.usd) })), por_canal: porCanal.map(f => ({ ...f, usd: r2(f.usd) })) };
  }

  return { HERRAMIENTAS, instrucciones, parteEstable, parteVolatil, conversar, responder, ejecutar, uso, costoUsd,
    exportarDesdeBot, relevarDesdeBot, recordar, memorias, historial, conversacion, guardarTurno, modelo, esfuerzo };
}

module.exports = { init, crear, HERRAMIENTAS, correrConsulta, correrEscritura };
