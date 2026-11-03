'use strict';

const { parse } = require('csv-parse/sync');
const { createHash, randomUUID } = require('node:crypto');
const fs = require('node:fs/promises');
const sharp = require('sharp');

const LIMITS = Object.freeze({ maxRows: 100, maxImageBytes: 5 * 1024 * 1024,
  maxTotalImageBytes: 100 * 1024 * 1024, maxCsvBytes: 2 * 1024 * 1024 });
const column = (key, required, descriptionEn, descriptionEs) => ({ key, required, descriptionEn, descriptionEs });
const COLUMNS = [
  column('titleEnglish', true, 'English title, up to 500 characters; must be unique.', 'Título en inglés, hasta 500 caracteres; debe ser único.'),
  column('titleSpanish', true, 'Spanish title, up to 500 characters; must be unique.', 'Título en español, hasta 500 caracteres; debe ser único.'),
  column('contentEnglish', true, 'English body, plain text or HTML. No embedded data images.', 'Contenido en inglés, texto o HTML. Sin imágenes incrustadas como datos.'),
  column('contentSpanish', true, 'Spanish body, plain text or HTML. No embedded data images.', 'Contenido en español, texto o HTML. Sin imágenes incrustadas como datos.'),
  column('author', true, 'Author name, up to 255 characters.', 'Nombre del autor, hasta 255 caracteres.'),
  column('date', true, 'Publication date with timezone, e.g. 2026-11-03T12:00:00-03:00 or 2026-11-03T15:00:00Z.', 'Fecha de publicación con zona horaria, por ejemplo 2026-11-03T12:00:00-03:00 o 2026-11-03T15:00:00Z.'),
  column('categoryIds', true, 'Category IDs separated by |, e.g. 2|3. See the catalog below.', 'IDs de categorías separados por |, por ejemplo 2|3. Ver el catálogo debajo.'),
  column('article_status_id', true, 'draft / borrador, published / publicado or archived / archivado; a status ID is also accepted. Published rows become visible on their publication date.', 'draft / borrador, published / publicado o archived / archivado; también se acepta un ID de estado. Las filas publicadas serán visibles desde su fecha de publicación.'),
  column('subtitleEnglish', false, 'English subtitle, up to 1,000 characters.', 'Subtítulo en inglés, hasta 1.000 caracteres.'),
  column('subtitleSpanish', false, 'Spanish subtitle, up to 1,000 characters.', 'Subtítulo en español, hasta 1.000 caracteres.'),
  column('author_gender', false, 'Author avatar: girl or boy. Leave blank for no avatar.', 'Avatar del autor: girl o boy. Dejar vacío para no usar avatar.'),
  column('filter', false, 'Filter IDs separated by |.', 'IDs de filtros separados por |.'),
  column('priority_category_id', false, 'One of the categoryIds, to use as the primary category.', 'Uno de los categoryIds, para usar como categoría principal.'),
  column('priority_filter_id', false, 'One of the filter IDs, to use as the primary filter.', 'Uno de los IDs de filter, para usar como filtro principal.'),
  column('priority', false, 'An unused priority from 1 to 8. Leave blank for no featured priority.', 'Una prioridad libre del 1 al 8. Dejar vacío para no destacar por prioridad.'),
  column('tags', false, 'Existing tag IDs separated by |. See the catalog below.', 'IDs de etiquetas existentes separados por |. Ver el catálogo debajo.'),
  column('genderIds', false, 'Audience gender IDs separated by |. Blank means unrestricted.', 'IDs de géneros del público separados por |. Vacío significa sin restricción.'),
  column('ethnicityIds', false, 'Audience ethnicity IDs separated by |. Blank means unrestricted.', 'IDs de etnias del público separados por |. Vacío significa sin restricción.'),
  column('locationIds', false, 'Audience location IDs separated by |. Blank means unrestricted.', 'IDs de locaciones del público separados por |. Vacío significa sin restricción.'),
  column('ageRanges', false, 'Audience age ranges separated by |, e.g. 18-35|65-100. Blank means unrestricted.', 'Rangos de edad del público separados por |, por ejemplo 18-35|65-100. Vacío significa sin restricción.'),
  column('imageCaptionEnglish', false, 'English image caption.', 'Epígrafe de la imagen en inglés.'),
  column('imageCaptionSpanish', false, 'Spanish image caption.', 'Epígrafe de la imagen en español.'),
  column('showImageEnglish', false, 'true or false: show the image inside the English article. Defaults to true.', 'true o false: mostrar la imagen dentro del artículo en inglés. Por defecto true.'),
  column('showImageSpanish', false, 'true or false: show the image inside the Spanish article. Defaults to true.', 'true o false: mostrar la imagen dentro del artículo en español. Por defecto true.')
];
const IMAGE_TYPES = { jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png', webp: 'image/webp' };
const normalizeName = value => String(value || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/\s+/g, ' ').trim();
// Match the existing editor's URLs so bulk imports cannot collide with its slugs.
const slug = value => String(value).toLowerCase().replace(/[^a-z0-9\s-]/g, '').replace(/\s+/g, '-').replace(/-+/g, '-').trim().substring(0, 100);
const issue = (code, messageEn, messageEs, row, column) => ({ code, messageEn, messageEs, ...(row ? { row } : {}), ...(column ? { column } : {}) });
class BulkImportError extends Error {
  constructor(errors, status = 400) { super('Article import validation failed'); this.errors = errors; this.status = status; }
}

async function loadCatalogs(db) {
  const queries = {
    categories: "SELECT id, name_en AS nameEnglish, name_es AS nameSpanish FROM category WHERE name_en <> 'All categories' ORDER BY name_en",
    filters: 'SELECT id, name_en AS nameEnglish, name_es AS nameSpanish FROM filter ORDER BY name_en',
    statuses: 'SELECT id, name_en AS nameEnglish, name_es AS nameSpanish FROM article_status ORDER BY id',
    tags: 'SELECT id, name_en AS nameEnglish, name_es AS nameSpanish FROM tags ORDER BY name_en',
    genders: 'SELECT id, name AS nameEnglish, name_es AS nameSpanish FROM gender ORDER BY id',
    ethnicities: 'SELECT id, name AS nameEnglish, name_es AS nameSpanish FROM ethnicity ORDER BY id',
    locations: "SELECT id, CONCAT_WS(' — ', organization, community_city, address) AS nameEnglish, CONCAT_WS(' — ', organization, community_city, address) AS nameSpanish FROM location ORDER BY community_city, organization"
  };
  return Object.fromEntries(await Promise.all(Object.entries(queries).map(async ([key, sql]) => [key, (await db.query(sql))[0]])));
}

function parseCsv(csv) {
  const errors = [];
  if (typeof csv !== 'string' || !csv.trim()) return { rows: [], errors: [issue('csv_required', 'Select a nonempty UTF-8 CSV file.', 'Selecciona un archivo CSV UTF-8 con contenido.')] };
  if (Buffer.byteLength(csv, 'utf8') > LIMITS.maxCsvBytes) return { rows: [], errors: [issue('csv_too_large', 'CSV files must be at most 2 MiB.', 'El CSV debe pesar como máximo 2 MiB.')] };
  if (csv.includes('\uFFFD') || csv.includes('\u0000')) return { rows: [], errors: [issue('csv_encoding', 'Save the CSV as UTF-8.', 'Guarda el CSV como UTF-8.')] };
  let records;
  try {
    records = parse(csv, { bom: true, skip_empty_lines: true, trim: true, max_record_size: LIMITS.maxCsvBytes,
      delimiter: csv.replace(/^\uFEFF/, '').split(/\r?\n/, 1)[0].includes(';') ? ';' : ',' });
  } catch (error) {
    return { rows: [], errors: [issue('csv_format', `Invalid CSV near line ${error.lines || 1}: check quotes and column counts.`, `CSV inválido cerca de la línea ${error.lines || 1}: revisa comillas y cantidad de columnas.`, Math.max(1, Number(error.records) || 1))] };
  }
  if (!records.length) return { rows: [], errors: [issue('csv_empty', 'The CSV has no header.', 'El CSV no tiene encabezado.')] };
  const [headers, ...data] = records;
  const supported = new Set(COLUMNS.map(item => item.key));
  for (const header of headers) {
    if (!supported.has(header)) errors.push(issue('unknown_column', `Unknown column: ${header || '(empty)'}.`, `Columna desconocida: ${header || '(vacía)'}.`, null, header));
    if (headers.indexOf(header) !== headers.lastIndexOf(header)) errors.push(issue('duplicate_column', `Repeated column: ${header}.`, `Columna repetida: ${header}.`, null, header));
  }
  for (const col of COLUMNS.filter(item => item.required)) if (!headers.includes(col.key)) errors.push(issue('missing_column', `Required column missing: ${col.key}.`, `Falta la columna obligatoria ${col.key}.`, null, col.key));
  if (!data.length) errors.push(issue('no_articles', 'Add at least one article below the header.', 'Agrega al menos un artículo debajo del encabezado.'));
  if (data.length > LIMITS.maxRows) errors.push(issue('too_many_rows', 'Import at most 100 articles at a time.', 'Importa como máximo 100 artículos por vez.'));
  return { errors, rows: data.slice(0, LIMITS.maxRows).map((values, index) => ({ row: index + 1, ...Object.fromEntries(headers.map((key, i) => [key, values[i] || ''])) })) };
}

function validateRows(rows, catalogs) {
  const errors = []; const normalized = []; const names = new Map(); const priorities = new Set();
  const catalogSets = Object.fromEntries(Object.entries(catalogs).map(([key, values]) => [key, new Set(values.map(item => Number(item.id)))]));
  for (const raw of rows) {
    const row = { ...raw };
    const add = (code, en, es, key) => errors.push(issue(code, en, es, raw.row, key));
    for (const col of COLUMNS.filter(item => item.required)) if (!String(raw[col.key] || '').trim()) add('required', 'This field is required.', 'Este campo es obligatorio.', col.key);
    for (const [key, max] of Object.entries({ titleEnglish: 500, titleSpanish: 500, subtitleEnglish: 1000, subtitleSpanish: 1000, author: 255, contentEnglish: 100000, contentSpanish: 100000, imageCaptionEnglish: 10000, imageCaptionSpanish: 10000 })) {
      row[key] = String(raw[key] || '').trim();
      if (row[key].length > max) add('too_long', `Maximum ${max} characters.`, `Máximo ${max} caracteres.`, key);
    }
    for (const key of ['titleEnglish', 'titleSpanish']) {
      row[key === 'titleEnglish' ? 'slugEnglish' : 'slugSpanish'] = slug(row[key]);
      if (!slug(row[key]) && row[key]) add('invalid_title', 'The title must include letters or numbers usable in its URL.', 'El título debe incluir letras o números utilizables en su URL.', key);
      for (const value of [normalizeName(row[key]), slug(row[key])]) {
        const nameKey = `${key}:${value}`;
        if (value && names.has(nameKey) && names.get(nameKey) !== row.row) add('duplicate_title', `This title or URL repeats article ${names.get(nameKey)}.`, `Este título o URL repite el artículo ${names.get(nameKey)}.`, key);
        if (value) names.set(nameKey, row.row);
      }
    }
    for (const key of ['contentEnglish', 'contentSpanish']) {
      if (!row[key].replace(/<[^>]*>/g, '').replace(/&nbsp;|&#160;/g, ' ').trim()) add('required', 'Article body cannot be empty.', 'El contenido del artículo no puede estar vacío.', key);
      if (/\b(?:data|blob):/i.test(row[key])) add('embedded_image', 'Use the numbered image files; embedded data/blob images are not supported.', 'Usa los archivos de imagen numerados; no se admiten imágenes incrustadas data/blob.', key);
    }
    const dateMatch = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?(Z|[+-]\d{2}:\d{2})$/.exec(raw.date || '');
    const parsedDate = dateMatch ? new Date(raw.date) : null;
    const calendarValid = dateMatch && Number(dateMatch[1]) >= 1000 && Number(dateMatch[2]) >= 1 && Number(dateMatch[2]) <= 12 && Number(dateMatch[3]) >= 1 && Number(dateMatch[3]) <= new Date(Date.UTC(Number(dateMatch[1]), Number(dateMatch[2]), 0)).getUTCDate() && Number(dateMatch[4]) < 24 && Number(dateMatch[5]) < 60 && Number(dateMatch[6] || 0) < 60;
    if (!calendarValid || !parsedDate || !Number.isFinite(parsedDate.getTime())) add('invalid_date', 'Use a valid ISO date with timezone, e.g. 2026-11-03T12:00:00-03:00.', 'Usa una fecha ISO válida con zona horaria, por ejemplo 2026-11-03T12:00:00-03:00.', 'date');
    else row.date = parsedDate.toISOString().slice(0, 19).replace('T', ' ');
    for (const [key, catalog] of Object.entries({ categoryIds: 'categories', filter: 'filters', tags: 'tags', genderIds: 'genders', ethnicityIds: 'ethnicities', locationIds: 'locations' })) {
      const value = String(raw[key] || '').trim();
      row[key] = value ? value.split('|').map(part => Number(part.trim())) : [];
      if (value && (!/^\d+(\s*\|\s*\d+)*$/.test(value) || row[key].some(id => !Number.isSafeInteger(id) || id <= 0))) add('invalid_ids', 'Use positive IDs separated by |.', 'Usa IDs positivos separados por |.', key);
      else {
        if (new Set(row[key]).size !== row[key].length) add('duplicate_ids', 'An ID is repeated in this field.', 'Hay un ID repetido en este campo.', key);
        const missing = row[key].filter(id => !catalogSets[catalog]?.has(id));
        if (missing.length) add('unknown_ids', `Unknown IDs: ${missing.join(', ')}. See the catalog.`, `IDs inexistentes: ${missing.join(', ')}. Consulta el catálogo.`, key);
      }
    }
    const statusText = normalizeName(raw.article_status_id);
    row.article_status_id = ({ draft: 1, borrador: 1, published: 2, publicado: 2, archived: 3, archivado: 3 })[statusText] || (/^\d+$/.test(statusText) ? Number(statusText) : NaN);
    if (!catalogSets.statuses?.has(row.article_status_id)) add('invalid_status', 'Choose draft, published, archived or an existing status ID.', 'Elige draft, published, archived o un ID de estado existente.', 'article_status_id');
    row.author_gender = raw.author_gender || null;
    if (row.author_gender && !['girl', 'boy'].includes(row.author_gender)) add('invalid_avatar', 'Use girl, boy or leave blank.', 'Usa girl, boy o deja vacío.', 'author_gender');
    for (const [key, list] of [['priority_category_id', 'categoryIds'], ['priority_filter_id', 'filter']]) {
      row[key] = raw[key] ? Number(raw[key]) : null;
      if (raw[key] && (!/^\d+$/.test(raw[key]) || !row[list].includes(row[key]))) add('invalid_primary', `Must be one of the IDs in ${list}.`, `Debe ser uno de los IDs de ${list}.`, key);
    }
    row.priority = raw.priority ? Number(raw.priority) : null;
    if (raw.priority && (!/^[1-8]$/.test(raw.priority) || priorities.has(row.priority))) add('invalid_priority', 'Use an unrepeated priority from 1 to 8, or leave blank.', 'Usa una prioridad no repetida del 1 al 8, o deja vacío.', 'priority');
    if (row.priority) priorities.add(row.priority);
    for (const key of ['showImageEnglish', 'showImageSpanish']) {
      const value = normalizeName(raw[key]); row[key] = value !== 'false';
      if (value && !['true', 'false'].includes(value)) add('invalid_boolean', 'Use true, false or leave blank (true).', 'Usa true, false o deja vacío (true).', key);
    }
    row.ageRanges = [];
    for (const value of String(raw.ageRanges || '').trim().split('|').filter(Boolean)) {
      const match = /^(\d{1,3})\s*-\s*(\d{1,3})$/.exec(value.trim());
      if (!match || Number(match[1]) > Number(match[2]) || Number(match[2]) > 120) add('invalid_age_range', 'Use age ranges from 0 to 120, e.g. 18-35|65-100.', 'Usa rangos entre 0 y 120 años, por ejemplo 18-35|65-100.', 'ageRanges');
      else {
        const range = { min: Number(match[1]), max: Number(match[2]) };
        if (row.ageRanges.some(old => range.min <= old.max && range.max >= old.min)) add('overlapping_age_ranges', 'Age ranges must not overlap or repeat.', 'Los rangos de edad no deben superponerse ni repetirse.', 'ageRanges');
        row.ageRanges.push(range);
      }
    }
    normalized.push(row);
  }
  return { rows: normalized, errors };
}

function validateImageManifest(images, rowCount) {
  const errors = []; const names = new Set(); const rows = new Set(); const hashes = new Map(); let bytes = 0;
  if (!Array.isArray(images)) return [issue('images_required', 'Select the numbered article images.', 'Selecciona las imágenes numeradas de los artículos.', null, 'images')];
  if (images.length !== rowCount) errors.push(issue('image_count', `${rowCount} article rows require exactly ${rowCount} images; ${images.length} were selected.`, `${rowCount} filas de artículos requieren exactamente ${rowCount} imágenes; se seleccionaron ${images.length}.`, null, 'images'));
  for (const file of images.slice(0, LIMITS.maxRows + 1)) {
    if (!file || typeof file !== 'object') { errors.push(issue('invalid_image_manifest', 'Invalid image information.', 'La información de una imagen es inválida.', null, 'images')); continue; }
    const name = String(file?.name || ''); const match = /^([1-9]\d*)\.(jpg|jpeg|png|webp)$/i.exec(name); const row = match ? Number(match[1]) : undefined;
    if (!match || row > rowCount) errors.push(issue('image_name', `Invalid image name: ${name}. Use 1.jpg, 2.png, etc., matching CSV article numbers.`, `Nombre de imagen inválido: ${name}. Usa 1.jpg, 2.png, etc., según los números de artículo del CSV.`, row, 'images'));
    if (names.has(name.toLowerCase()) || rows.has(row)) errors.push(issue('duplicate_image_name', `More than one image matches ${name}.`, `Hay más de una imagen correspondiente a ${name}.`, row, 'images'));
    names.add(name.toLowerCase()); if (row) rows.add(row);
    if (!Number.isSafeInteger(file?.size) || file.size <= 0 || file.size > LIMITS.maxImageBytes) errors.push(issue('image_size', `${name}: images must be nonempty and at most 5 MiB.`, `${name}: las imágenes deben tener contenido y pesar como máximo 5 MiB.`, row, 'images'));
    bytes += Number(file?.size) || 0;
    if (match && file.type && file.type !== IMAGE_TYPES[match[2].toLowerCase()]) errors.push(issue('image_type', `${name}: file type does not match the extension.`, `${name}: el tipo de archivo no coincide con su extensión.`, row, 'images'));
    if (file.hash && !/^[a-f0-9]{64}$/i.test(file.hash)) errors.push(issue('image_hash', `${name}: invalid image checksum.`, `${name}: la verificación de la imagen es inválida.`, row, 'images'));
    if (file.hash && hashes.has(file.hash.toLowerCase())) errors.push(issue('duplicate_image_content', `${name} repeats the content of ${hashes.get(file.hash.toLowerCase())}.`, `${name} repite el contenido de ${hashes.get(file.hash.toLowerCase())}.`, row, 'images'));
    if (file.hash) hashes.set(file.hash.toLowerCase(), name);
  }
  for (let row = 1; row <= rowCount; row++) if (!rows.has(row)) errors.push(issue('missing_image', `Missing image for article ${row}.`, `Falta la imagen del artículo ${row}.`, row, 'images'));
  if (bytes > LIMITS.maxTotalImageBytes) errors.push(issue('images_too_large', 'Images may total at most 100 MiB.', 'Las imágenes pueden pesar como máximo 100 MiB en total.', null, 'images'));
  return errors;
}

async function findDatabaseConflicts(db, rows) {
  if (!rows.length) return [];
  const [existing] = await db.query('SELECT id, title_en, title_es, slug_en, slug_es, priority FROM article WHERE title_en IN (?) OR title_es IN (?) OR slug_en IN (?) OR slug_es IN (?) OR priority IN (?)',
    [rows.map(row => row.titleEnglish), rows.map(row => row.titleSpanish), rows.map(row => row.slugEnglish), rows.map(row => row.slugSpanish), rows.map(row => row.priority).filter(Boolean).concat([0])]);
  const errors = [];
  for (const row of rows) {
    for (const [key, title, slugKey, value] of [['titleEnglish', 'title_en', 'slug_en', row.slugEnglish], ['titleSpanish', 'title_es', 'slug_es', row.slugSpanish]]) {
      if (existing.some(old => normalizeName(old[title]) === normalizeName(row[key]) || old[slugKey] === value)) errors.push(issue('existing_title', 'This title or URL already belongs to an article.', 'Este título o URL ya pertenece a un artículo.', row.row, key));
    }
    if (row.priority && existing.some(old => Number(old.priority) === row.priority)) errors.push(issue('occupied_priority', 'This priority is already in use; leave blank or choose an available number.', 'Esta prioridad ya está en uso; deja vacío o elige un número libre.', row.row, 'priority'));
  }
  return errors;
}

async function validateImport(db, csv, images) {
  const parsed = parseCsv(csv); const errors = [...parsed.errors]; let rows = parsed.rows;
  if (!errors.length) {
    const validated = validateRows(rows, await loadCatalogs(db)); rows = validated.rows; errors.push(...validated.errors);
    if (!errors.length) errors.push(...await findDatabaseConflicts(db, rows));
  }
  errors.push(...validateImageManifest(images, rows.length));
  return { valid: !errors.length, errors, rowCount: rows.length, rows,
    summary: { draft: rows.filter(row => row.article_status_id === 1).length, published: rows.filter(row => row.article_status_id === 2).length, archived: rows.filter(row => row.article_status_id === 3).length } };
}

async function inspectImages(files) {
  const images = []; const errors = [];
  for (const file of files) {
    const buffer = await fs.readFile(file.path); const row = Number.parseInt(file.originalname, 10);
    const image = { name: file.originalname, size: buffer.length, type: file.mimetype, hash: createHash('sha256').update(buffer).digest('hex'), path: file.path, row };
    try {
      const metadata = await sharp(buffer, { limitInputPixels: 40000000, failOn: 'warning' }).metadata();
      const expected = { jpg: 'jpeg', jpeg: 'jpeg', png: 'png', webp: 'webp' }[file.originalname.split('.').pop().toLowerCase()];
      if (!metadata.width || !metadata.height || metadata.format !== expected || (metadata.pages || 1) > 1) throw new Error('Invalid image');
      // Force decoding; a plausible header alone must not pass validation.
      await sharp(buffer, { limitInputPixels: 40000000, failOn: 'warning' }).resize(1, 1).toBuffer();
      image.metadata = metadata; image.type = IMAGE_TYPES[file.originalname.split('.').pop().toLowerCase()];
    } catch {
      errors.push(issue('invalid_image', `${file.originalname}: use a valid, static JPG, PNG or WebP image (up to 40 megapixels).`, `${file.originalname}: usa una imagen JPG, PNG o WebP válida, estática (hasta 40 megapíxeles).`, row, 'images'));
    }
    images.push(image);
  }
  return { images, errors };
}

async function saveImport({ pool, rows, images, uploadImageWithVariants, deleteS3Objects, bucketName, logger, aborted = () => false }) {
  const uploadId = randomUUID(); const keys = []; const uploaded = new Map(); const articleIds = [];
  let connection; let begun = false; let committed = false; let commitAttempted = false; let locked = false; let safeToClean = true;
  const cleanup = async () => { if (keys.length) try { await deleteS3Objects(keys); } catch (error) { logger.error('Could not clean up failed bulk article images:', error); } };
  try {
    // All CSV and binary images have passed validation before the first S3 write.
    for (const image of images) {
      if (aborted()) throw new BulkImportError([issue('cancelled', 'The upload was interrupted; no articles were created.', 'La carga se interrumpió; no se crearon artículos.')], 400);
      const buffer = await fs.readFile(image.path); const pair = {};
      for (const language of ['en', 'es']) {
        const originalKey = `articles/bulk/${uploadId}/${image.row}/${language}`;
        // Include anticipated variants so partial S3 failures are cleaned up too.
        keys.push(originalKey, `${originalKey}__small`, `${originalKey}__medium`);
        pair[language] = await uploadImageWithVariants({ originalKey, buffer, contentType: image.type, presetName: 'article' });
      }
      uploaded.set(image.row, pair);
    }
    if (aborted()) throw new BulkImportError([issue('cancelled', 'The upload was interrupted; no articles were created.', 'La carga se interrumpió; no se crearon artículos.')], 400);
    connection = await pool.getConnection();
    const [lockRows] = await connection.query("SELECT GET_LOCK('article_bulk_import', 10) AS acquired");
    if (Number(lockRows[0]?.acquired) !== 1) throw new BulkImportError([issue('busy', 'Another import is finishing. Try again shortly.', 'Otra importación está finalizando. Inténtalo nuevamente en breve.')], 409);
    locked = true;
    await connection.beginTransaction(); begun = true;
    const conflicts = await findDatabaseConflicts(connection, rows);
    if (conflicts.length) throw new BulkImportError(conflicts, 409);
    for (const row of rows) {
      const [inserted] = await connection.query(`INSERT INTO article (title_en, title_es, subtitle_en, subtitle_es, content_en, content_es, author, author_gender, publication_date, priority, article_status_id, slug_en, slug_es, show_image_en, show_image_es) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [row.titleEnglish, row.titleSpanish, row.subtitleEnglish || null, row.subtitleSpanish || null, row.contentEnglish, row.contentSpanish,
          row.author, row.author_gender, row.date, row.priority, row.article_status_id, row.slugEnglish, row.slugSpanish, row.showImageEnglish ? 'Y' : 'N', row.showImageSpanish ? 'Y' : 'N']);
      const id = inserted.insertId; articleIds.push(id);
      await connection.query('INSERT INTO article_category (article_id, category_id, priority) VALUES ?', [row.categoryIds.map(value => [id, value, value === row.priority_category_id ? 'Y' : 'N'])]);
      if (row.filter.length) await connection.query('INSERT INTO article_filter (article_id, filter_id, priority) VALUES ?', [row.filter.map(value => [id, value, value === row.priority_filter_id ? 'Y' : 'N'])]);
      for (const [key, table, field] of [['tags', 'article_tags', 'tag_id'], ['genderIds', 'article_gender', 'gender_id'], ['ethnicityIds', 'article_ethnicity', 'ethnicity_id'], ['locationIds', 'article_location', 'location_id']]) {
        if (row[key].length) await connection.query(`INSERT INTO ${table} (article_id, ${field}) VALUES ?`, [row[key].map(value => [id, value])]);
      }
      if (row.ageRanges.length) await connection.query('INSERT INTO article_age_range (article_id, min_age, max_age) VALUES ?', [row.ageRanges.map(range => [id, range.min, range.max])]);
      const image = images.find(item => item.row === row.row);
      for (const language of ['en', 'es']) {
        const stored = uploaded.get(row.row)[language];
        await connection.query(`INSERT INTO article_images (article_id, image_type, s3_key, s3_key_small, s3_key_medium, s3_bucket, file_hash, original_filename, mime_type, file_size, width, height, alt_text_en, alt_text_es, caption_en, caption_es) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [id, `preview_${language}`, stored.originalKey, stored.smallKey, stored.mediumKey, bucketName, image.hash, image.name, image.type, image.size,
            image.metadata.width, image.metadata.height, '', '', row.imageCaptionEnglish || '', row.imageCaptionSpanish || '']);
      }
    }
    if (aborted()) throw new BulkImportError([issue('cancelled', 'The upload was interrupted; no articles were created.', 'La carga se interrumpió; no se crearon artículos.')], 400);
    commitAttempted = true; await connection.commit(); committed = true;
    return { createdCount: articleIds.length, articleIds };
  } catch (error) {
    if (begun && !committed) {
      try { await connection.rollback(); } catch (rollbackError) { logger.error('Bulk article rollback failed:', rollbackError); }
    }
    if (commitAttempted && !committed) {
      // A lost COMMIT response is ambiguous. Never delete images of committed rows.
      try {
        const [persisted] = await pool.query('SELECT DISTINCT article_id FROM article_images WHERE s3_key IN (?)', [keys]);
        if (persisted.length === rows.length) return { createdCount: persisted.length, articleIds: persisted.map(row => row.article_id) };
        safeToClean = persisted.length === 0;
      } catch { safeToClean = false; }
    }
    if (!committed && safeToClean) await cleanup();
    throw error;
  } finally {
    if (locked) try { await connection.query("SELECT RELEASE_LOCK('article_bulk_import')"); } catch (error) { logger.error('Bulk article lock release failed:', error); }
    connection?.release();
  }
}

module.exports = { LIMITS, COLUMNS, IMAGE_TYPES, BulkImportError, issue, loadCatalogs, parseCsv, validateRows, validateImageManifest,
  findDatabaseConflicts, validateImport, inspectImages, saveImport };
