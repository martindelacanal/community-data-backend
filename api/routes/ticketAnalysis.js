const express = require('express');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const {
  MAX_IMAGES, MAX_IMAGE_BYTES, TicketAnalysisError, createTicketAnalysisService
} = require('../services/ticketAnalysis');

function positiveInteger(value) {
  return Number.isSafeInteger(value) && value > 0;
}

function parseAnalysisForm(raw, newImageCount) {
  let form;
  try { form = JSON.parse(raw); } catch { throw new TicketAnalysisError('ai_invalid_request', 400); }
  if (!form || typeof form !== 'object' || Array.isArray(form)
      || Object.keys(form).some((key) => !['ticket_id', 'existing_image_ids', 'image_order', 'language'].includes(key))
      || (form.ticket_id !== undefined && !positiveInteger(form.ticket_id))
      || !['en', 'es'].includes(form.language || 'en')) throw new TicketAnalysisError('ai_invalid_request', 400);
  const existingIds = form.existing_image_ids ?? [];
  if (!Array.isArray(existingIds) || existingIds.some((id) => !positiveInteger(id))
      || new Set(existingIds).size !== existingIds.length
      || (existingIds.length && !form.ticket_id)
      || existingIds.length + newImageCount < 1 || existingIds.length + newImageCount > MAX_IMAGES) {
    throw new TicketAnalysisError('ai_invalid_request', 400);
  }
  const order = form.image_order ?? [
    ...existingIds.map((id) => ({ kind: 'existing', id })),
    ...Array.from({ length: newImageCount }, (_, index) => ({ kind: 'new', index }))
  ];
  if (!Array.isArray(order) || order.length !== existingIds.length + newImageCount) throw new TicketAnalysisError('ai_invalid_request', 400);
  const seen = new Set();
  for (const reference of order) {
    const validExisting = reference?.kind === 'existing' && positiveInteger(reference.id) && existingIds.includes(reference.id)
      && Object.keys(reference).every((key) => ['kind', 'id'].includes(key));
    const validNew = reference?.kind === 'new' && Number.isSafeInteger(reference.index) && reference.index >= 0 && reference.index < newImageCount
      && Object.keys(reference).every((key) => ['kind', 'index'].includes(key));
    if (!validExisting && !validNew) throw new TicketAnalysisError('ai_invalid_request', 400);
    const key = `${reference.kind}:${validExisting ? reference.id : reference.index}`;
    if (seen.has(key)) throw new TicketAnalysisError('ai_invalid_request', 400);
    seen.add(key);
  }
  return { ticketId: form.ticket_id, existingIds, order, language: form.language || 'en' };
}

function createAnalysisLimiter({ clock = Date.now, perUser = 6, globalLimit = 60, maxConcurrent = 4 } = {}) {
  const entries = new Map();
  let active = 0;
  let globalCount = 0;
  let windowEnd = 0;
  return {
    acquire(userId) {
      const now = clock();
      if (now >= windowEnd) {
        entries.clear();
        globalCount = 0;
        windowEnd = now + 60000;
      }
      if (active >= maxConcurrent) throw new TicketAnalysisError('ai_busy', 429);
      const count = entries.get(userId) || 0;
      if (count >= perUser || globalCount >= globalLimit) throw new TicketAnalysisError('ai_rate_limited', 429);
      entries.set(userId, count + 1);
      globalCount++;
      active++;
      let released = false;
      return () => { if (!released) { active--; released = true; } };
    }
  };
}

function createStoredImageReader(env) {
  // Credentials remain on the backend. Images are resolved only through authorized DB rows.
  const s3 = new S3Client({
    credentials: { accessKeyId: env.ACCESS_KEY, secretAccessKey: env.SECRET_ACCESS_KEY },
    region: env.BUCKET_REGION
  });
  return async (key) => {
    let body;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 20000);
    timeout.unref?.();
    try {
      const object = await s3.send(new GetObjectCommand({ Bucket: env.BUCKET_NAME, Key: key }), { abortSignal: controller.signal });
      body = object.Body;
      if (!body || object.ContentLength > MAX_IMAGE_BYTES) throw new TicketAnalysisError('ai_image_too_large', 413);
      const chunks = [];
      let size = 0;
      for await (const chunk of body) {
        size += chunk.length;
        if (size > MAX_IMAGE_BYTES) throw new TicketAnalysisError('ai_image_too_large', 413);
        chunks.push(Buffer.from(chunk));
      }
      return Buffer.concat(chunks);
    } catch (error) {
      body?.destroy?.();
      if (error instanceof TicketAnalysisError) throw error;
      throw new TicketAnalysisError(controller.signal.aborted ? 'ai_timeout' : 'ai_image_not_found', controller.signal.aborted ? 504 : 400);
    } finally { clearTimeout(timeout); }
  };
}

function createTicketAnalysisRouter({ pool, env = process.env, service, readStoredImage, logger, limiterOptions } = {}) {
  const router = express.Router();
  const analysis = service || createTicketAnalysisService({ env });
  const limiter = createAnalysisLimiter(limiterOptions);
  let storedImageReader = readStoredImage;
  const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: MAX_IMAGE_BYTES, files: MAX_IMAGES, fields: 1, fieldSize: 4096, parts: MAX_IMAGES + 1 },
    fileFilter: (req, file, callback) => callback(['image/jpeg', 'image/png'].includes(file.mimetype) ? null : new TicketAnalysisError('ai_invalid_image', 400), true)
  }).array('ticket[]', MAX_IMAGES);

  function respondError(res, error) {
    const safeError = error instanceof TicketAnalysisError ? error : new TicketAnalysisError('ai_provider_unavailable', 503);
    if (!(error instanceof TicketAnalysisError)) logger?.error('Ticket analysis failed', { code: 'TICKET_ANALYSIS_ERROR' });
    if (safeError.status === 429) res.set('Retry-After', '60');
    return res.status(safeError.status).json({ error: safeError.code, message: safeError.code });
  }

  function authenticate(req, res, next) {
    res.set('Cache-Control', 'no-store');
    try {
      const authorization = req.headers.authorization;
      if (typeof authorization !== 'string' || !authorization.startsWith('Bearer ') || authorization.length > 8192 || !env.JWT_SECRET) throw new Error('Invalid token');
      const claims = jwt.verify(authorization.slice(7), env.JWT_SECRET, { algorithms: ['HS256'] });
      const user = typeof claims.data === 'string' ? JSON.parse(claims.data) : null;
      if (!positiveInteger(Number(user?.id))) throw new Error('Invalid user');
      req.analysisUser = { id: Number(user.id), role: user.role };
    } catch { return respondError(res, new TicketAnalysisError('ai_authentication_required', 401)); }
    if (!['admin', 'opsmanager', 'stocker', 'auditor'].includes(req.analysisUser.role)) return respondError(res, new TicketAnalysisError('ai_forbidden', 403));
    if (!analysis.isConfigured()) return respondError(res, new TicketAnalysisError('ai_not_configured', 503));
    try {
      req.releaseAnalysis = limiter.acquire(req.analysisUser.id);
      res.once('close', () => { if (!req.analysisProcessing) req.releaseAnalysis(); });
      next();
    } catch (error) { return respondError(res, error); }
  }

  function readUpload(req, res, next) {
    upload(req, res, (error) => {
      if (!error) return next();
      req.releaseAnalysis();
      return respondError(res, error instanceof TicketAnalysisError ? error
        : new TicketAnalysisError(error.code === 'LIMIT_FILE_SIZE' ? 'ai_image_too_large' : 'ai_invalid_request', error.code === 'LIMIT_FILE_SIZE' ? 413 : 400));
    });
  }

  router.post('/upload/ticket/analyze', authenticate, readUpload, async (req, res) => {
    req.analysisProcessing = true;
    try {
      const files = req.files || [];
      const form = parseAnalysisForm(req.body?.form, files.length);
      if (!form.ticketId && req.analysisUser.role === 'auditor') throw new TicketAnalysisError('ai_forbidden', 403);
      let existingImages = [];
      if (form.ticketId) {
        const ownOnly = req.analysisUser.role === 'stocker';
        const [tickets] = await pool.query(
          `SELECT dt.id FROM donation_ticket AS dt WHERE dt.id = ? AND dt.enabled = 'Y'
           ${ownOnly ? 'AND EXISTS (SELECT 1 FROM stocker_log AS sl WHERE sl.donation_ticket_id = dt.id AND sl.operation_id = 5 AND sl.user_id = ?)' : ''}`,
          ownOnly ? [form.ticketId, req.analysisUser.id] : [form.ticketId]
        );
        if (tickets.length !== 1) throw new TicketAnalysisError('ai_ticket_not_found', 404);
        if (form.existingIds.length) {
          [existingImages] = await pool.query('SELECT id, file FROM donation_ticket_image WHERE donation_ticket_id = ?', [form.ticketId]);
          if (form.existingIds.some((id) => !existingImages.some((image) => Number(image.id) === id))) throw new TicketAnalysisError('ai_image_not_found', 400);
        }
      }
      const images = [];
      for (const reference of form.order) {
        if (reference.kind === 'new') images.push(files[reference.index].buffer);
        else {
          storedImageReader ||= createStoredImageReader(env);
          const image = existingImages.find(({ id }) => Number(id) === reference.id);
          images.push(await storedImageReader(image.file));
        }
      }
      const [[products], [productTypes]] = await Promise.all([
        pool.query('SELECT id, name, product_type_id FROM product ORDER BY name'),
        pool.query('SELECT id, name, name_es FROM product_type ORDER BY id')
      ]);
      const result = await analysis.analyze({ images, products, productTypes, language: form.language });
      return res.status(200).json(result);
    } catch (error) { return respondError(res, error); }
    finally {
      req.analysisProcessing = false;
      req.releaseAnalysis();
    }
  });
  return router;
}

module.exports = { createTicketAnalysisRouter, parseAnalysisForm, createAnalysisLimiter };
