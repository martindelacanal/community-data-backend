'use strict';

const multer = require('multer');
const sharp = require('sharp');
const { randomUUID } = require('node:crypto');
const { createBoundedAsyncCache } = require('../services/boundedAsyncCache');
const { BLOCKS, IMAGE_SIZES, readSettings, validateCard, validateSettings, changedFields, positiveId, normalizeEvent,
  normalizeMetricFilters, transaction, addHistory, fetchCampaignMetrics, fail } = require('../services/homeCampaigns');

function registerHomeCampaignRoutes(router, deps) {
  const { pool, verifyToken, ensureAdmin, getSignedUrlsForImages, uploadImageWithVariants, deleteS3Objects, uploadBannerImage, logger } = deps;
  const publicCache = createBoundedAsyncCache({ maxEntries: 8, maxBytes: 2 * 1024 * 1024 });
  const metricsCache = createBoundedAsyncCache({ maxEntries: 40, maxBytes: 4 * 1024 * 1024 });
  const clickBuckets = new Map(); let clickWindow = 0; let globalClicks = 0;
  function allowClick(req, res, next) {
    const window = Math.floor(Date.now() / 60000);
    if (window !== clickWindow) { clickWindow = window; globalClicks = 0; clickBuckets.clear(); }
    const ip = String(req.ip || req.socket?.remoteAddress || 'unknown').slice(0, 64); const count = clickBuckets.get(ip) || 0;
    if (count >= 60 || globalClicks >= 3000 || (!clickBuckets.has(ip) && clickBuckets.size >= 5000)) {
      return res.set('Retry-After', '60').status(429).json({ error: 'Too many campaign events; retry shortly' });
    }
    clickBuckets.set(ip, count + 1); globalClicks++; next();
  }
  const invalidate = () => { publicCache.clear(); metricsCache.clear(); };
  const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 5 * 1024 * 1024, files: 1 },
    fileFilter: (_req, file, callback) => callback(['image/jpeg', 'image/png', 'image/webp'].includes(file.mimetype) ? null : Object.assign(new Error('Use JPG, PNG or WebP images'), { status: 400 }), true)
  }).single('image');
  const uploadMiddleware = (req, res, next) => upload(req, res, error => error ? res.status(400).json({ error: error.code === 'LIMIT_FILE_SIZE' ? 'Maximum image size is 5 MB' : error.message }) : next());
  const admin = (req, res, next) => { try { const actor = ensureAdmin(req, res); if (actor) { req.campaignActor = actor; next(); } } catch { res.status(401).json({ error: 'Unauthorized' }); } };
  const wrap = handler => async (req, res) => {
    try { await handler(req, res); } catch (error) {
      if (!error.status || error.status >= 500) logger.error('Home campaign request failed:', error);
      res.status(error.status || 500).json({ error: error.status ? error.message : 'Internal server error' });
    }
  };
  const actorId = req => Number.isSafeInteger(Number(req.campaignActor?.id)) ? Number(req.campaignActor.id) : null;
  const imageKeys = card => [card.image_url, card.image_url_small, card.image_url_medium].filter(Boolean);
  const cleanup = async keys => { try { await deleteS3Objects(keys); } catch (error) { logger.error('Unable to clean up replaced campaign images:', error); } };
  async function getSettings(connection = pool, lock = false) {
    const [rows] = await connection.query(`SELECT settings_json, DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ') AS created_at FROM home_promotion_settings WHERE id = 1${lock ? ' FOR UPDATE' : ''}`);
    if (!rows.length) fail('Campaign settings migration is required', 503);
    return readSettings(rows[0]);
  }
  async function saveSettings(connection, settings) {
    const persisted = { ...settings }; delete persisted.trackingStartedAt;
    await connection.query('UPDATE home_promotion_settings SET settings_json = ?, updated_at = UTC_TIMESTAMP() WHERE id = 1', [JSON.stringify(persisted)]);
  }
  async function formatSettings(settings) {
    const keys = Object.keys(IMAGE_SIZES).map(size => settings.images[size]);
    const urls = await getSignedUrlsForImages(keys.filter(Boolean)); let index = 0;
    const imageUrls = keys.map(key => key ? urls[index++] || null : null);
    const { images, ...publicSettings } = settings;
    return { ...publicSettings, imageBanner: { ...settings.imageBanner, imageMobileUrl: imageUrls[0], imageTabletUrl: imageUrls[1], imageDesktopUrl: imageUrls[2] } };
  }
  async function formatCards(rows, language) {
    const keys = [...new Set(rows.flatMap(imageKeys))]; const urls = await getSignedUrlsForImages(keys); const urlMap = new Map(keys.map((key, index) => [key, urls[index]]));
    return rows.map(card => ({ id: card.id, titleEn: card.title_en, titleEs: card.title_es, ...(language ? { title: language === 'es' ? card.title_es : card.title_en } : {}),
      backgroundColor: card.background_color, textColor: card.text_color, linkUrl: card.link_url || null, sortOrder: Number(card.sort_order), enabled: card.enabled === 'Y', targetPlatform: card.target_platform || 'all',
      imageUrl: urlMap.get(card.image_url) || null, imageOriginalUrl: urlMap.get(card.image_url) || null,
      imageSmallUrl: urlMap.get(card.image_url_small || card.image_url) || null, imageMediumUrl: urlMap.get(card.image_url_medium || card.image_url) || null,
      createdAt: card.creation_date, updatedAt: card.modification_date }));
  }
  async function selectCard(connection, id, lock = false) {
    const [rows] = await connection.query(`SELECT * FROM home_cards WHERE id = ?${lock ? ' FOR UPDATE' : ''}`, [id]);
    if (!rows.length) fail('Home card not found', 404); return rows[0];
  }
  const cardState = row => ({ titleEn: row.title_en, titleEs: row.title_es, backgroundColor: row.background_color, textColor: row.text_color,
    linkUrl: row.link_url, sortOrder: Number(row.sort_order), enabled: row.enabled, targetPlatform: row.target_platform || 'all' });
  async function cardHistory(connection, req, row, action, before, after) {
    await addHistory(connection, { entityType: 'card', entityId: row.id, titleEn: row.title_en, titleEs: row.title_es, action, before, after, actorId: actorId(req) });
  }
  async function checkImage(file) {
    if (!file) fail('No image file provided');
    let metadata;
    try { metadata = await sharp(file.buffer, { limitInputPixels: 40000000 }).metadata(); } catch { fail('The uploaded file is not a valid image'); }
    if (!['jpeg', 'png', 'webp'].includes(metadata.format) || !metadata.width || !metadata.height || (metadata.pages || 1) > 1) fail('Upload a static JPG, PNG or WebP image');
    return metadata;
  }

  router.get('/home-cards/public', wrap(async (req, res) => {
    const language = req.query.language === 'es' ? 'es' : 'en'; const platform = req.query.platform === 'web' ? 'web' : 'app';
    const result = await publicCache.getOrCompute(`${language}:${platform}`, 30000, async () => {
      const settings = await getSettings();
      const [cards] = settings[platform].cards.enabled ? await pool.query(`SELECT * FROM home_cards WHERE enabled = 'Y' AND target_platform IN ('all', ?) ORDER BY sort_order, id LIMIT 500`, [platform]) : [[]];
      return { cards: await formatCards(cards, language), settings: await formatSettings(settings) };
    });
    res.set('Cache-Control', 'public, max-age=15').json(result);
  }));
  router.get('/home-cards/settings', verifyToken, admin, wrap(async (_req, res) => res.json(await formatSettings(await getSettings()))));
  router.put('/home-cards/settings', verifyToken, admin, wrap(async (req, res) => {
    await transaction(pool, async connection => {
      const before = await getSettings(connection, true); const after = validateSettings(req.body, before);
      await saveSettings(connection, after);
      for (const platform of ['web', 'app']) {
        for (const block of BLOCKS) {
          const oldState = before[platform][block]; const newState = after[platform][block];
          const action = oldState.enabled !== newState.enabled ? newState.enabled ? 'enabled' : 'disabled' : 'layout_updated';
          await addHistory(connection, { entityType: block === 'cards' ? 'card' : block, titleEn: block === 'cards' ? 'Home cards block' : block === 'app_banner' ? 'Download the app banner' : after.imageBanner.titleEn,
            titleEs: block === 'cards' ? 'Bloque de tarjetas de inicio' : block === 'app_banner' ? 'Banner de descarga de apps' : after.imageBanner.titleEs,
            action, before: oldState, after: newState, actorId: actorId(req), platform });
        }
      }
      await addHistory(connection, { entityType: 'image_banner', titleEn: after.imageBanner.titleEn, titleEs: after.imageBanner.titleEs,
        action: 'updated', before: before.imageBanner, after: after.imageBanner, actorId: actorId(req) });
    });
    invalidate(); res.json(await formatSettings(await getSettings()));
  }));
  router.post('/home-cards/banner/image/:size', verifyToken, admin, uploadMiddleware, wrap(async (req, res) => {
    const size = req.params.size; if (!Object.hasOwn(IMAGE_SIZES, size)) fail('Invalid banner size');
    await checkImage(req.file);
    const key = `home-campaigns/${randomUUID()}-${size}.webp`;
    const buffer = await sharp(req.file.buffer, { limitInputPixels: 40000000 }).rotate().resize({ width: IMAGE_SIZES[size], withoutEnlargement: true, fit: 'inside' }).webp({ quality: 84 }).toBuffer();
    let oldKey;
    try {
      await uploadBannerImage(key, buffer, 'image/webp');
      await transaction(pool, async connection => {
        const settings = await getSettings(connection, true); oldKey = settings.images[size]; settings.images[size] = key;
        await saveSettings(connection, settings);
        await addHistory(connection, { entityType: 'image_banner', titleEn: settings.imageBanner.titleEn, titleEs: settings.imageBanner.titleEs,
          action: 'image_updated', before: { [size]: oldKey ? 'uploaded' : null }, after: { [size]: `uploaded ${new Date().toISOString()}` }, actorId: actorId(req) });
      });
    } catch (error) { await cleanup([key]); throw error; }
    invalidate(); await cleanup([oldKey]); res.json(await formatSettings(await getSettings()));
  }));
  router.delete('/home-cards/banner/image/:size', verifyToken, admin, wrap(async (req, res) => {
    const size = req.params.size; if (!Object.hasOwn(IMAGE_SIZES, size)) fail('Invalid banner size'); let oldKey;
    await transaction(pool, async connection => {
      const settings = await getSettings(connection, true);
      if (settings.web.image_banner.enabled || settings.app.image_banner.enabled) fail('Disable the image banner before removing an image');
      oldKey = settings.images[size]; settings.images[size] = null; await saveSettings(connection, settings);
      await addHistory(connection, { entityType: 'image_banner', titleEn: settings.imageBanner.titleEn, titleEs: settings.imageBanner.titleEs,
        action: 'image_removed', before: { [size]: 'uploaded' }, after: { [size]: null }, actorId: actorId(req) });
    });
    invalidate(); await cleanup([oldKey]); res.json(await formatSettings(await getSettings()));
  }));
  router.get('/home-cards', verifyToken, admin, wrap(async (_req, res) => {
    const [rows] = await pool.query('SELECT * FROM home_cards ORDER BY sort_order, id LIMIT 501');
    res.json({ cards: await formatCards(rows.slice(0, 500)), truncated: rows.length > 500 });
  }));
  router.post('/home-cards', verifyToken, admin, wrap(async (req, res) => {
    const card = validateCard(req.body);
    const row = await transaction(pool, async connection => {
      await getSettings(connection, true); // Serialize the bounded card count with concurrent creates.
      const [count] = await connection.query('SELECT COUNT(*) AS total FROM home_cards');
      if (Number(count[0].total) >= 500) fail('The campaign limit is 500 cards; disable or delete obsolete cards');
      const [result] = await connection.query(`INSERT INTO home_cards (title_en, title_es, background_color, text_color, link_url, sort_order, enabled, target_platform)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, [card.titleEn, card.titleEs, card.backgroundColor, card.textColor, card.linkUrl, card.sortOrder, card.enabled, card.targetPlatform]);
      const created = await selectCard(connection, result.insertId);
      await cardHistory(connection, req, created, 'created', {}, card); return created;
    });
    invalidate(); res.status(201).json((await formatCards([row]))[0]);
  }));
  router.put('/home-cards/:id', verifyToken, admin, wrap(async (req, res) => {
    const id = positiveId(req.params.id);
    const row = await transaction(pool, async connection => {
      const before = await selectCard(connection, id, true); const card = validateCard({ ...req.body, targetPlatform: req.body.targetPlatform ?? before.target_platform });
      await connection.query(`UPDATE home_cards SET title_en = ?, title_es = ?, background_color = ?, text_color = ?, link_url = ?, sort_order = ?, enabled = ?, target_platform = ? WHERE id = ?`,
        [card.titleEn, card.titleEs, card.backgroundColor, card.textColor, card.linkUrl, card.sortOrder, card.enabled, card.targetPlatform, id]);
      const updated = await selectCard(connection, id); const oldState = cardState(before);
      const { enabled: beforeEnabled, ...oldFields } = oldState; const { enabled: afterEnabled, ...newFields } = card;
      if (Object.keys(changedFields(oldFields, newFields).after).length) await cardHistory(connection, req, updated, 'updated', oldFields, newFields);
      if (beforeEnabled !== afterEnabled) await cardHistory(connection, req, updated, afterEnabled === 'Y' ? 'enabled' : 'disabled', { enabled: beforeEnabled }, { enabled: afterEnabled });
      return updated;
    });
    invalidate(); res.json((await formatCards([row]))[0]);
  }));
  router.delete('/home-cards/:id', verifyToken, admin, wrap(async (req, res) => {
    const id = positiveId(req.params.id);
    if (req.query.confirmMetricsDeletion !== 'true') fail('Confirm permanent deletion of this card and all associated metrics, or disable it to preserve history');
    let row;
    await transaction(pool, async connection => { row = await selectCard(connection, id, true); await connection.query('DELETE FROM home_cards WHERE id = ?', [id]); });
    // Click logs, daily counters and history are deleted atomically by FK cascades.
    invalidate(); await cleanup(imageKeys(row)); res.json({ success: true });
  }));
  router.post('/home-cards/:id/image', verifyToken, admin, uploadMiddleware, wrap(async (req, res) => {
    const id = positiveId(req.params.id); await checkImage(req.file); await selectCard(pool, id);
    const originalKey = randomUUID(); let oldKeys = []; let row;
    try {
      const uploaded = await uploadImageWithVariants({ originalKey, buffer: req.file.buffer, contentType: req.file.mimetype, presetName: 'trustedResource' });
      await transaction(pool, async connection => {
        const before = await selectCard(connection, id, true); oldKeys = imageKeys(before);
        await connection.query('UPDATE home_cards SET image_url = ?, image_url_small = ?, image_url_medium = ? WHERE id = ?', [uploaded.originalKey, uploaded.smallKey, uploaded.mediumKey, id]);
        row = await selectCard(connection, id); await cardHistory(connection, req, row, 'image_updated', { image: before.image_url ? 'uploaded' : null }, { image: `uploaded ${new Date().toISOString()}` });
      });
    } catch (error) { await cleanup([originalKey, `${originalKey}__small`, `${originalKey}__medium`]); throw error; }
    invalidate(); await cleanup(oldKeys); res.json((await formatCards([row]))[0]);
  }));
  router.delete('/home-cards/:id/image', verifyToken, admin, wrap(async (req, res) => {
    const id = positiveId(req.params.id); let oldKeys;
    await transaction(pool, async connection => {
      const row = await selectCard(connection, id, true); oldKeys = imageKeys(row);
      await connection.query('UPDATE home_cards SET image_url = NULL, image_url_small = NULL, image_url_medium = NULL WHERE id = ?', [id]);
      await cardHistory(connection, req, row, 'image_removed', { image: row.image_url ? 'uploaded' : null }, { image: null });
    });
    invalidate(); await cleanup(oldKeys); res.json({ success: true });
  }));

  router.post('/home-cards/events', allowClick, wrap(async (req, res) => {
    const event = normalizeEvent(req.body);
    await transaction(pool, async connection => {
      const settings = await getSettings(connection);
      const block = event.entityType === 'card' ? 'cards' : event.entityType;
      if (!settings[event.platform][block].enabled) fail('Campaign is not available on this platform', 409);
      if (event.entityType === 'card') {
        // Shared lock closes the race with deletion while allowing concurrent readers.
        const [cards] = await connection.query("SELECT enabled, target_platform FROM home_cards WHERE id = ? LOCK IN SHARE MODE", [event.entityId]);
        if (!cards.length || cards[0].enabled !== 'Y' || !['all', event.platform].includes(cards[0].target_platform)) fail('Campaign is not available on this platform', 409);
      }
      const [result] = await connection.query(`INSERT IGNORE INTO home_campaign_clicks
        (event_id, entity_type, entity_id, card_id, session_id, platform, device_type, access_channel, destination, clicked_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP())`, [event.eventId, event.entityType, event.entityId, event.entityType === 'card' ? event.entityId : null,
        event.sessionId, event.platform, event.deviceType, event.accessChannel, event.destination]);
      if (result.affectedRows) await connection.query(`INSERT INTO home_campaign_daily
        (click_date, entity_type, entity_id, card_id, platform, device_type, access_channel, clicks, last_clicked_at)
        VALUES (UTC_DATE(), ?, ?, ?, ?, ?, ?, 1, UTC_TIMESTAMP())
        ON DUPLICATE KEY UPDATE clicks = clicks + 1, last_clicked_at = UTC_TIMESTAMP()`,
      [event.entityType, event.entityId, event.entityType === 'card' ? event.entityId : null, event.platform, event.deviceType, event.accessChannel]);
    });
    res.status(202).json({ accepted: true });
  }));
  router.post('/metrics/interaction/campaigns', verifyToken, admin, wrap(async (req, res) => {
    const filters = normalizeMetricFilters(req.body);
    const result = await metricsCache.getOrCompute(JSON.stringify(filters), 30000, async () => fetchCampaignMetrics(pool, filters, await getSettings()));
    res.json(result);
  }));
}

module.exports = { registerHomeCampaignRoutes };
