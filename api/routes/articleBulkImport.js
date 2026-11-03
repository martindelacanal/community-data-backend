'use strict';

const multer = require('multer');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { pipeline, Transform } = require('node:stream');
const { LIMITS, COLUMNS, BulkImportError, issue, loadCatalogs, validateImport, inspectImages, saveImport } = require('../services/articleBulkImport');

function registerArticleBulkImportRoutes(router, deps) {
  const { pool, verifyToken, logger } = deps;
  const activeActors = new Set(); let activeUploads = 0;
  const access = (req, res, next) => {
    try {
      const actor = JSON.parse(req.data.data);
      if (!['admin', 'contentmanager'].includes(actor.role)) return res.status(403).json({ error: 'Forbidden' });
      req.bulkActor = `${actor.role}:${actor.id}`; next();
    } catch { return res.status(401).json({ error: 'Unauthorized' }); }
  };
  const publicResult = result => { const { rows, ...summary } = result; return summary; };
  const sendError = (res, error) => {
    if (!error.status || error.status >= 500) logger.error('Bulk article import failed:', error);
    if (res.headersSent || res.destroyed) return;
    const errors = error.errors || [issue('import_failed', 'The import could not be completed. Refresh the article list before retrying.', 'No se pudo completar la importación. Actualiza la tabla de artículos antes de volver a intentarlo.')];
    res.status(error.status || 500).json({ valid: false, errors });
  };
  const wrap = handler => async (req, res) => { try { await handler(req, res); } catch (error) { sendError(res, error); } };
  const storage = {
    _handleFile(req, file, callback) {
      const destination = path.join(req.bulkDirectory, randomUUID()); let size = 0;
      const counter = new Transform({ transform(chunk, _encoding, done) {
        size += chunk.length; req.bulkBytes += chunk.length;
        if (req.bulkBytes > LIMITS.maxTotalImageBytes + LIMITS.maxCsvBytes) return done(new BulkImportError([issue('images_too_large', 'The images may total at most 100 MiB.', 'Las imágenes pueden pesar como máximo 100 MiB en total.', null, 'images')]));
        if (file.fieldname === 'csv' && size > LIMITS.maxCsvBytes) return done(new BulkImportError([issue('csv_too_large', 'CSV files must be at most 2 MiB.', 'El CSV debe pesar como máximo 2 MiB.', null, 'csv')]));
        done(null, chunk);
      } });
      pipeline(file.stream, counter, fs.createWriteStream(destination, { flags: 'wx', mode: 0o600 }), error => {
        if (error) fsp.unlink(destination).catch(() => {}).finally(() => callback(error));
        else callback(null, { path: destination, size });
      });
    },
    _removeFile(_req, file, callback) { fs.unlink(file.path, error => callback(error?.code === 'ENOENT' ? null : error)); }
  };
  const multipart = multer({ storage, limits: { fileSize: LIMITS.maxImageBytes, files: LIMITS.maxRows + 1, fields: 1, fieldSize: 100, parts: LIMITS.maxRows + 3 },
    fileFilter: (_req, file, callback) => {
      if (file.fieldname === 'csv') return callback(/\.csv$/i.test(file.originalname) ? null : new BulkImportError([issue('csv_file_type', 'The CSV file must use the .csv extension.', 'El archivo CSV debe usar la extensión .csv.', null, 'csv')]), true);
      return callback(/^[1-9]\d*\.(jpg|jpeg|png|webp)$/i.test(file.originalname) ? null : new BulkImportError([issue('image_name', `Invalid image name: ${file.originalname}. Use 1.jpg, 2.png, etc.`, `Nombre de imagen inválido: ${file.originalname}. Usa 1.jpg, 2.png, etc.`, null, 'images')]), true);
    }
  }).fields([{ name: 'csv', maxCount: 1 }, { name: 'images', maxCount: LIMITS.maxRows }]);

  router.get('/article/bulk/schema', verifyToken, access, wrap(async (_req, res) => {
    res.json({ columns: COLUMNS, catalogs: await loadCatalogs(pool), limits: LIMITS });
  }));
  router.post('/article/bulk/validate', verifyToken, access, wrap(async (req, res) => {
    res.json(publicResult(await validateImport(pool, req.body?.csv, req.body?.images)));
  }));
  router.post('/article/bulk', verifyToken, access, async (req, res) => {
    if (activeUploads >= 2 || activeActors.has(req.bulkActor)) return sendError(res, new BulkImportError([issue('busy', 'An import is already running. Please wait before starting another.', 'Ya hay una importación en curso. Espera antes de iniciar otra.')], 429));
    activeUploads++; activeActors.add(req.bulkActor);
    let interrupted = false; let created; let finalError;
    const onAbort = () => { interrupted = true; };
    const onClose = () => { if (!res.writableFinished) interrupted = true; };
    req.once('aborted', onAbort); res.once('close', onClose);
    try {
      if (Number(req.headers['content-length'] || 0) > LIMITS.maxTotalImageBytes + LIMITS.maxCsvBytes + 1024 * 1024) throw new BulkImportError([issue('upload_too_large', 'The upload exceeds 100 MiB of images plus 2 MiB of CSV.', 'La carga supera los 100 MiB de imágenes más 2 MiB de CSV.')], 413);
      req.bulkDirectory = await fsp.mkdtemp(path.join(os.tmpdir(), 'bienestar-article-import-')); req.bulkBytes = 0;
      await new Promise((resolve, reject) => multipart(req, res, error => error ? reject(error) : resolve()));
      const csvFile = req.files?.csv?.[0];
      const csv = csvFile ? await fsp.readFile(csvFile.path, 'utf8') : '';
      const imageFiles = req.files?.images || [];
      const descriptors = imageFiles.map(file => ({ name: file.originalname, size: file.size, type: file.mimetype === 'application/octet-stream' ? '' : file.mimetype }));
      let result = await validateImport(pool, csv, descriptors);
      if (!result.valid) throw new BulkImportError(result.errors);
      const inspected = await inspectImages(imageFiles);
      result = await validateImport(pool, csv, inspected.images);
      result.errors.push(...inspected.errors); result.valid = !result.errors.length;
      if (!result.valid) throw new BulkImportError(result.errors);
      created = await saveImport({ ...deps, rows: result.rows, images: inspected.images, aborted: () => interrupted });
    } catch (error) {
      if (error instanceof multer.MulterError) {
        error = new BulkImportError([issue('upload_limit', error.code === 'LIMIT_FILE_SIZE' ? 'Each image must be at most 5 MiB.' : 'Use one CSV and at most 100 image files, with no unexpected fields.', error.code === 'LIMIT_FILE_SIZE' ? 'Cada imagen debe pesar como máximo 5 MiB.' : 'Usa un CSV y como máximo 100 imágenes, sin campos adicionales inesperados.')]);
      }
      finalError = error;
    } finally {
      if (req.bulkDirectory) try { await fsp.rm(req.bulkDirectory, { recursive: true, force: true }); } catch (error) { logger.error('Could not remove temporary article import files:', error); }
      req.removeListener('aborted', onAbort); res.removeListener('close', onClose);
      activeUploads--; activeActors.delete(req.bulkActor);
    }
    if (finalError) sendError(res, finalError);
    else if (!interrupted) res.status(201).json(created);
  });
}

module.exports = { registerArticleBulkImportRoutes };
