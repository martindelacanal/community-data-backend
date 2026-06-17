const express = require('express');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const crypto = require('crypto');
const path = require('path');
const admin = require('firebase-admin');
const { S3Client, PutObjectCommand, DeleteObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const mysqlConnection = require('../connection/connection');
const logger = require('../utils/logger.js');

const router = express.Router();
const MAX_NOTIFICATION_IMAGE_SIZE_BYTES = 1 * 1024 * 1024;
const DEFAULT_ANDROID_NOTIFICATION_ICON = process.env.PUSH_NOTIFICATION_ANDROID_ICON || 'ic_stat_push_notification';
const DEFAULT_ANDROID_NOTIFICATION_COLOR = process.env.PUSH_NOTIFICATION_ANDROID_COLOR || '#DF3D7A';

const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: MAX_NOTIFICATION_IMAGE_SIZE_BYTES,
    files: 1
  }
}).single('image');

const bucketName = process.env.BUCKET_NAME;
const bucketRegion = process.env.BUCKET_REGION;
const accessKey = process.env.ACCESS_KEY;
const secretAccessKey = process.env.SECRET_ACCESS_KEY;

const s3 = new S3Client({
  credentials: {
    accessKeyId: accessKey,
    secretAccessKey: secretAccessKey
  },
  region: bucketRegion
});

const INVALID_PUSH_TOKEN_CODES = new Set([
  'messaging/invalid-registration-token',
  'messaging/registration-token-not-registered'
]);

function randomImageName(bytes = 32) {
  return crypto.randomBytes(bytes).toString('hex');
}

function resolveImageExtension(file) {
  const extensionFromName = path.extname(String(file?.originalname || '')).trim().toLowerCase();
  if (extensionFromName) {
    return extensionFromName.slice(0, 10);
  }

  switch (String(file?.mimetype || '').toLowerCase()) {
    case 'image/jpeg':
      return '.jpg';
    case 'image/png':
      return '.png';
    case 'image/gif':
      return '.gif';
    case 'image/webp':
      return '.webp';
    default:
      return '';
  }
}

function getBearerToken(req) {
  if (!req.headers.authorization || !req.headers.authorization.startsWith('Bearer ')) {
    return null;
  }

  const token = req.headers.authorization.slice(7).trim();
  if (!token || token === 'null' || token === 'undefined') {
    return null;
  }

  return token;
}

function getOptionalAuthUser(req) {
  try {
    const token = getBearerToken(req);
    if (!token) {
      return null;
    }

    const authData = jwt.verify(token, process.env.JWT_SECRET);
    return JSON.parse(authData.data);
  } catch (error) {
    return null;
  }
}

function verifyToken(req, res, next) {
  const token = getBearerToken(req);

  if (!token) {
    return res.status(401).json({ error: 'Unauthorized', message: 'Authentication required' });
  }

  jwt.verify(token, process.env.JWT_SECRET, (error, authData) => {
    if (error) {
      return res.status(403).json({ error: 'Forbidden', message: 'Invalid token' });
    }

    req.data = authData;
    next();
  });
}

function verifyAdmin(req, res, next) {
  try {
    const user = JSON.parse(req.data.data);
    if (user.role !== 'admin') {
      return res.status(403).json({ error: 'Forbidden', message: 'Insufficient permissions' });
    }

    req.currentUser = user;
    next();
  } catch (error) {
    logger.error('Error verifying admin for push notifications:', error);
    return res.status(403).json({ error: 'Forbidden', message: 'Insufficient permissions' });
  }
}

function parseBoolean(value) {
  return value === true || value === 'true' || value === 1 || value === '1';
}

function safeJsonParse(value, fallback) {
  if (value === undefined || value === null || value === '') {
    return fallback;
  }

  if (typeof value === 'object') {
    return value;
  }

  try {
    return JSON.parse(value);
  } catch (error) {
    return fallback;
  }
}

function normalizePositiveInteger(value) {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  const parsed = Number.parseInt(String(value), 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function normalizeDateString(value) {
  if (!value) {
    return null;
  }

  const trimmedValue = String(value).trim();
  if (!trimmedValue) {
    return null;
  }

  const parsedDate = new Date(trimmedValue);
  if (Number.isNaN(parsedDate.getTime())) {
    return null;
  }

  return parsedDate.toISOString().slice(0, 10);
}

function normalizeNumberArray(values) {
  if (!Array.isArray(values)) {
    return [];
  }

  const normalized = values
    .map((value) => normalizePositiveInteger(value))
    .filter((value) => value !== null);

  return [...new Set(normalized)];
}

function normalizeZipcodes(values) {
  const sourceValues = Array.isArray(values)
    ? values
    : (values !== undefined && values !== null && values !== '' ? [values] : []);

  const normalized = sourceValues
    .map((value) => String(value).trim())
    .filter((value) => value.length > 0)
    .map((value) => value.slice(0, 30));

  return [...new Set(normalized)];
}

function normalizeRegisterForm(rawRegisterForm) {
  const registerForm = safeJsonParse(rawRegisterForm, {});
  const normalized = {};

  if (!registerForm || typeof registerForm !== 'object' || Array.isArray(registerForm)) {
    return normalized;
  }

  for (const [questionIdRaw, answerValue] of Object.entries(registerForm)) {
    const questionId = normalizePositiveInteger(questionIdRaw);
    if (!questionId) {
      continue;
    }

    if (Array.isArray(answerValue)) {
      const answerIds = normalizeNumberArray(answerValue);
      if (answerIds.length > 0) {
        normalized[questionId] = answerIds;
      }
      continue;
    }

    const answerId = normalizePositiveInteger(answerValue);
    if (answerId) {
      normalized[questionId] = answerId;
    }
  }

  return normalized;
}

function normalizeFilterChips(rawFilterChips) {
  const filterChips = safeJsonParse(rawFilterChips, []);
  if (!Array.isArray(filterChips)) {
    return [];
  }

  return filterChips
    .map((chip) => {
      const code = String(chip?.code || '').trim();
      const value = Array.isArray(chip?.value)
        ? chip.value.map((item) => String(item).trim()).filter(Boolean).join(', ')
        : String(chip?.value ?? '').trim();

      if (!code || !value) {
        return null;
      }

      return { code, value };
    })
    .filter((chip) => chip !== null);
}

function normalizeAudienceFilters(rawFilters) {
  const filters = safeJsonParse(rawFilters, {});
  return {
    from_date: normalizeDateString(filters.from_date),
    to_date: normalizeDateString(filters.to_date),
    locations: normalizeNumberArray(filters.locations),
    genders: normalizeNumberArray(filters.genders),
    ethnicities: normalizeNumberArray(filters.ethnicities),
    min_age: normalizePositiveInteger(filters.min_age),
    max_age: normalizePositiveInteger(filters.max_age),
    zipcode: normalizeZipcodes(filters.zipcodes ?? filters.zipcode),
    register_form: normalizeRegisterForm(filters.register_form),
    // Locaciones objetivo (opcionales) de una notificación masiva (audience_type = 'all').
    // Vacío => masiva genérica para todos los dispositivos.
    mass_location_ids: normalizeNumberArray(filters.mass_location_ids)
  };
}

function hasAnyAudienceFilter(filters) {
  const zipcodes = normalizeZipcodes(filters.zipcodes ?? filters.zipcode);
  return Boolean(
    filters.from_date ||
    filters.to_date ||
    filters.locations.length ||
    filters.genders.length ||
    filters.ethnicities.length ||
    zipcodes.length ||
    filters.min_age !== null ||
    filters.max_age !== null ||
    Object.keys(filters.register_form).length
  );
}

function validateNotificationPayload(payload, fileProvided = false) {
  const errors = [];

  if (!payload.audience_type || !['all', 'filtered'].includes(payload.audience_type)) {
    errors.push({ field: 'audience_type', message: 'audience_type must be "all" or "filtered"' });
  }

  if (!payload.message_en || typeof payload.message_en !== 'string' || payload.message_en.trim().length === 0) {
    errors.push({ field: 'message_en', message: 'message_en is required' });
  }

  if (!payload.message_es || typeof payload.message_es !== 'string' || payload.message_es.trim().length === 0) {
    errors.push({ field: 'message_es', message: 'message_es is required' });
  }

  if (payload.message_en && payload.message_en.length > 500) {
    errors.push({ field: 'message_en', message: 'message_en must not exceed 500 characters' });
  }

  if (payload.message_es && payload.message_es.length > 500) {
    errors.push({ field: 'message_es', message: 'message_es must not exceed 500 characters' });
  }

  if (payload.filters.min_age !== null && payload.filters.max_age !== null && payload.filters.min_age > payload.filters.max_age) {
    errors.push({ field: 'age_range', message: 'min_age must be less than or equal to max_age' });
  }

  if (payload.audience_type === 'filtered' && !hasAnyAudienceFilter(payload.filters)) {
    errors.push({ field: 'filters', message: 'At least one filter is required when audience_type is "filtered"' });
  }

  if (fileProvided && !payload.has_valid_image) {
    errors.push({ field: 'image', message: 'Only image files are allowed' });
  }

  return errors;
}

function buildParticipantAudienceScope(filters) {
  const zipcodes = normalizeZipcodes(filters.zipcodes ?? filters.zipcode);
  const whereClauses = [
    'u.role_id = 5',
    'u.enabled = "Y"',
    'u.deleted = "N"'
  ];
  const params = [];

  if (filters.from_date) {
    whereClauses.push(`CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles') >= ?`);
    params.push(filters.from_date);
  }

  if (filters.to_date) {
    whereClauses.push(`CONVERT_TZ(u.creation_date, '+00:00', 'America/Los_Angeles') < DATE_ADD(?, INTERVAL 1 DAY)`);
    params.push(filters.to_date);
  }

  if (filters.locations.length > 0) {
    const locationPlaceholders = filters.locations.map(() => '?').join(',');
    // "Filtered users" por locación = usuarios que se registraron en esa locación
    // (user.first_location_id) O que alguna vez recibieron una entrega aprobada ahí
    // (delivery_beneficiary con approved = 'Y').
    whereClauses.push(`(
      u.first_location_id IN (${locationPlaceholders})
      OR EXISTS (
        SELECT 1 FROM delivery_beneficiary db
        WHERE db.receiving_user_id = u.id
          AND db.approved = 'Y'
          AND db.location_id IN (${locationPlaceholders})
      )
    )`);
    params.push(...filters.locations, ...filters.locations);
  }

  if (filters.genders.length > 0) {
    whereClauses.push(`u.gender_id IN (${filters.genders.map(() => '?').join(',')})`);
    params.push(...filters.genders);
  }

  if (filters.ethnicities.length > 0) {
    whereClauses.push(`u.ethnicity_id IN (${filters.ethnicities.map(() => '?').join(',')})`);
    params.push(...filters.ethnicities);
  }

  if (filters.min_age !== null) {
    whereClauses.push(`TIMESTAMPDIFF(YEAR, u.date_of_birth, DATE(CONVERT_TZ(UTC_TIMESTAMP(), '+00:00', 'America/Los_Angeles'))) >= ?`);
    params.push(filters.min_age);
  }

  if (filters.max_age !== null) {
    whereClauses.push(`TIMESTAMPDIFF(YEAR, u.date_of_birth, DATE(CONVERT_TZ(UTC_TIMESTAMP(), '+00:00', 'America/Los_Angeles'))) <= ?`);
    params.push(filters.max_age);
  }

  if (zipcodes.length > 0) {
    whereClauses.push(`u.zipcode IN (${zipcodes.map(() => '?').join(',')})`);
    params.push(...zipcodes);
  }

  for (const [questionIdRaw, answerValue] of Object.entries(filters.register_form)) {
    const questionId = normalizePositiveInteger(questionIdRaw);
    if (!questionId) {
      continue;
    }

    if (Array.isArray(answerValue) && answerValue.length > 0) {
      whereClauses.push(`
        EXISTS (
          SELECT 1
          FROM user_question uqf
          INNER JOIN user_question_answer uqaf ON uqaf.user_question_id = uqf.id
          WHERE uqf.user_id = u.id
            AND uqf.question_id = ?
            AND uqaf.answer_id IN (${answerValue.map(() => '?').join(',')})
        )
      `);
      params.push(questionId, ...answerValue);
      continue;
    }

    const answerId = normalizePositiveInteger(answerValue);
    if (answerId) {
      whereClauses.push(`
        EXISTS (
          SELECT 1
          FROM user_question uqf
          INNER JOIN user_question_answer uqaf ON uqaf.user_question_id = uqf.id
          WHERE uqf.user_id = u.id
            AND uqf.question_id = ?
            AND uqaf.answer_id = ?
        )
      `);
      params.push(questionId, answerId);
    }
  }

  return {
    whereSql: whereClauses.join(' AND '),
    params
  };
}

function dedupeDeviceRows(rows) {
  const deviceMap = new Map();

  for (const row of rows) {
    const pushToken = String(row.push_token || '').trim();
    if (!pushToken) {
      continue;
    }

    const existingRow = deviceMap.get(pushToken);
    if (!existingRow || Number(row.id) > Number(existingRow.id)) {
      deviceMap.set(pushToken, row);
    }
  }

  return Array.from(deviceMap.values());
}

async function getFirebaseApp() {
  if (admin.apps.length > 0) {
    return admin.app();
  }

  const projectId = process.env.FIREBASE_PROJECT_ID;
  const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;
  const privateKey = process.env.FIREBASE_PRIVATE_KEY;

  if (!projectId || !clientEmail || !privateKey) {
    throw new Error('Firebase environment variables are not configured');
  }

  return admin.initializeApp({
    credential: admin.credential.cert({
      projectId,
      clientEmail,
      privateKey: privateKey.replace(/\\n/g, '\n')
    })
  });
}

async function getSignedImageUrl(imageKey, expiresIn = 3600) {
  if (!imageKey) {
    return null;
  }

  const command = new GetObjectCommand({
    Bucket: bucketName,
    Key: imageKey
  });

  return getSignedUrl(s3, command, { expiresIn });
}

async function uploadImageToS3(file) {
  const imageKey = `${randomImageName()}${resolveImageExtension(file)}`;

  await s3.send(new PutObjectCommand({
    Bucket: bucketName,
    Key: imageKey,
    Body: file.buffer,
    ContentType: file.mimetype
  }));

  return imageKey;
}

async function deleteImageFromS3(imageKey) {
  if (!imageKey) {
    return;
  }

  await s3.send(new DeleteObjectCommand({
    Bucket: bucketName,
    Key: imageKey
  }));
}

async function mapNotificationRow(row, imageExpiry = 3600) {
  return {
    id: row.id,
    audience_type: row.audience_type,
    message_en: row.message_en,
    message_es: row.message_es,
    filters: normalizeAudienceFilters(row.filters_json),
    filters_chip: safeJsonParse(row.filters_chip_json, []),
    image_key: row.image_s3_key || null,
    image_url: row.image_s3_key ? await getSignedImageUrl(row.image_s3_key, imageExpiry) : null,
    created_at: row.created_at,
    updated_at: row.updated_at,
    last_executed_at: row.last_executed_at
  };
}

async function getNotificationById(notificationId) {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT id, audience_type, message_en, message_es, filters_json, filters_chip_json,
            image_s3_key, created_at, updated_at, last_executed_at
     FROM push_notifications
     WHERE id = ?
     LIMIT 1`,
    [notificationId]
  );

  if (rows.length === 0) {
    return null;
  }

  return mapNotificationRow(rows[0]);
}

// ----- Inbox por usuario (historial de notificaciones de la app nativa) -----
const APP_NOTIFICATION_TITLE = process.env.PUSH_NOTIFICATION_TITLE || 'Bienestar Community';

/** Id del usuario autenticado a partir del token (req.data.data es un JSON string). */
function getAuthUserId(req) {
  try {
    const user = JSON.parse(req.data.data);
    const id = Number(user.id);
    return Number.isInteger(id) && id > 0 ? id : null;
  } catch (error) {
    return null;
  }
}

/** Filas del inbox -> objeto para el cliente (con URL de imagen firmada y estado de lectura). */
async function mapInboxNotificationRow(row, imageExpiry = 60 * 60 * 24) {
  return {
    id: row.id,
    // El push usa un título fijo de la app; el cuerpo es message_en/message_es.
    title: APP_NOTIFICATION_TITLE,
    message_en: row.message_en,
    message_es: row.message_es,
    image_url: row.image_s3_key ? await getSignedImageUrl(row.image_s3_key, imageExpiry) : null,
    delivered_at: row.delivered_at,
    read: row.read_at !== null && row.read_at !== undefined,
    read_at: row.read_at || null
  };
}

async function getUnreadInboxCount(userId) {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT COUNT(*) AS total
       FROM push_notification_recipients
      WHERE user_id = ? AND read_at IS NULL`,
    [userId]
  );
  return rows[0]?.total || 0;
}

/**
 * Resumen para los indicadores de la app: cantidad de no leídas + id de la notificación
 * entregada más recientemente (latest_id). El cliente usa latest_id para el "puntito" de
 * actividad nueva en el botón My Wellbeing (se compara contra el último id descartado).
 */
async function getInboxSummary(userId) {
  const [rows] = await mysqlConnection.promise().query(
    `SELECT
       COALESCE(SUM(read_at IS NULL), 0) AS unread,
       (SELECT push_notification_id
          FROM push_notification_recipients
         WHERE user_id = ?
         ORDER BY delivered_at DESC, id DESC
         LIMIT 1) AS latest_id
     FROM push_notification_recipients
     WHERE user_id = ?`,
    [userId, userId]
  );

  return {
    unread_count: Number(rows[0]?.unread || 0),
    latest_id: rows[0]?.latest_id != null ? Number(rows[0].latest_id) : null
  };
}

/**
 * Registra/actualiza una fila de inbox por cada usuario distinto alcanzado por una ejecución.
 * Idempotente por (push_notification_id, user_id): un re-envío vuelve a marcar como no leída
 * y sube el item al tope (delivered_at = NOW()). No interrumpe el envío si falla.
 */
async function recordNotificationRecipients(connection, notificationId, executionId, audienceDevices) {
  const targetedUserIds = [...new Set(
    audienceDevices
      .map((row) => row.audience_user_id)
      .filter((value) => value !== null && value !== undefined)
      .map((value) => Number(value))
      .filter((value) => Number.isInteger(value) && value > 0)
  )];

  if (targetedUserIds.length === 0) {
    return;
  }

  for (const chunk of chunkArray(targetedUserIds, 500)) {
    const placeholders = chunk.map(() => '(?, ?, ?)').join(',');
    const params = [];
    chunk.forEach((userId) => params.push(notificationId, userId, executionId));
    await connection.query(
      `INSERT INTO push_notification_recipients (push_notification_id, user_id, execution_id)
       VALUES ${placeholders}
       ON DUPLICATE KEY UPDATE execution_id = VALUES(execution_id), delivered_at = NOW(), read_at = NULL`,
      params
    );
  }
}

async function getTotalRegisteredDevices(connection) {
  const [rows] = await connection.query(
    `SELECT COUNT(DISTINCT push_token) AS total
     FROM push_device_tokens
     WHERE deleted = 'N'
       AND token_status = 'ACTIVE'
       AND push_token IS NOT NULL
       AND push_token <> ''`
  );

  return rows[0]?.total || 0;
}

async function getAudienceDevices(connection, audienceType, filters) {
  if (audienceType === 'all') {
    const massLocationIds = Array.isArray(filters.mass_location_ids) ? filters.mass_location_ids : [];
    const whereClauses = [
      `d.deleted = 'N'`,
      `d.token_status = 'ACTIVE'`,
      `d.push_token IS NOT NULL`,
      `d.push_token <> ''`,
      `d.platform IN ('android', 'ios')`,
      // Respetar el opt-out del usuario. Dispositivos sin usuario asociado igual reciben.
      `(u.id IS NULL OR COALESCE(u.notifications_enabled, 'Y') <> 'N')`
    ];
    const params = [];

    if (massLocationIds.length > 0) {
      // La masiva fue apuntada a locaciones específicas. Le llega a:
      //  - dispositivos sin usuario asociado (sin preferencia de locación),
      //  - usuarios que NO eligieron locaciones (por defecto reciben todas las masivas),
      //  - usuarios cuya elección intersecta las locaciones de la masiva.
      // NO le llega a usuarios que eligieron recibir SOLO de otras locaciones.
      const placeholders = massLocationIds.map(() => '?').join(',');
      whereClauses.push(`(
        u.id IS NULL
        OR NOT EXISTS (SELECT 1 FROM user_notification_location unl WHERE unl.user_id = u.id)
        OR EXISTS (
          SELECT 1 FROM user_notification_location unl2
          WHERE unl2.user_id = u.id AND unl2.location_id IN (${placeholders})
        )
      )`);
      params.push(...massLocationIds);
    }

    const [rows] = await connection.query(
      `SELECT d.id, d.installation_id, d.push_token, d.platform, d.language AS device_language,
              u.id AS audience_user_id, u.language AS user_language
       FROM push_device_tokens d
       LEFT JOIN user u ON u.id = COALESCE(d.user_id, d.last_user_id)
       WHERE ${whereClauses.join(' AND ')}`,
      params
    );

    return dedupeDeviceRows(rows);
  }

  const scope = buildParticipantAudienceScope(filters);
  const [rows] = await connection.query(
    `SELECT d.id, d.installation_id, d.push_token, d.platform, d.language AS device_language,
            u.id AS audience_user_id, u.language AS user_language
     FROM push_device_tokens d
     INNER JOIN user u ON u.id = d.last_user_id
     WHERE d.deleted = 'N'
       AND d.token_status = 'ACTIVE'
       AND d.push_token IS NOT NULL
       AND d.push_token <> ''
       AND d.platform IN ('android', 'ios')
       AND COALESCE(u.notifications_enabled, 'Y') <> 'N'
       AND ${scope.whereSql}`,
    scope.params
  );

  return dedupeDeviceRows(rows);
}

function chunkArray(items, chunkSize = 500) {
  const chunks = [];
  for (let index = 0; index < items.length; index += chunkSize) {
    chunks.push(items.slice(index, index + chunkSize));
  }
  return chunks;
}

function resolveAudienceLanguage(row) {
  return row.user_language === 'es' ? 'es' : (row.device_language === 'es' ? 'es' : 'en');
}

function buildMulticastMessage(notification, language, imageUrl, tokens) {
  const body = language === 'es' ? notification.message_es : notification.message_en;
  const appTitle = process.env.PUSH_NOTIFICATION_TITLE || 'Bienestar Community';
  const message = {
    tokens,
    notification: {
      title: appTitle,
      body
    },
    data: {
      push_notification_id: String(notification.id),
      audience_type: notification.audience_type,
      language
    },
    android: {
      priority: 'high',
      notification: {
        icon: DEFAULT_ANDROID_NOTIFICATION_ICON,
        color: DEFAULT_ANDROID_NOTIFICATION_COLOR,
        sound: 'default'
      }
    },
    apns: {
      headers: {
        'apns-priority': '10'
      },
      payload: {
        aps: {
          sound: 'default'
        }
      }
    }
  };

  if (imageUrl) {
    message.notification.imageUrl = imageUrl;
    message.android.notification.imageUrl = imageUrl;
    message.apns.payload.aps['mutable-content'] = 1;
    message.apns.fcm_options = { image: imageUrl };
  }

  return message;
}

async function markInvalidTokens(connection, deviceIds) {
  if (!deviceIds.length) {
    return;
  }

  await connection.query(
    `UPDATE push_device_tokens
     SET token_status = 'INVALID',
         user_id = NULL,
         updated_at = NOW(),
         last_error_at = NOW(),
         last_error_code = 'INVALID_TOKEN'
     WHERE id IN (${deviceIds.map(() => '?').join(',')})`,
    deviceIds
  );
}

async function executeNotification(connection, notification, executedByUserId) {
  const firebaseApp = await getFirebaseApp();
  const totalRegisteredDevices = await getTotalRegisteredDevices(connection);
  const audienceDevices = await getAudienceDevices(connection, notification.audience_type, notification.filters || {});
  const targetedDevices = audienceDevices.length;
  const imageUrl = notification.image_key ? await getSignedImageUrl(notification.image_key, 60 * 60 * 24 * 7) : null;

  let successfulDevices = 0;
  let failedDevices = 0;
  const invalidDeviceIds = [];
  const errorSamples = [];

  if (targetedDevices > 0) {
    const deviceGroups = {
      en: [],
      es: []
    };

    for (const deviceRow of audienceDevices) {
      deviceGroups[resolveAudienceLanguage(deviceRow)].push(deviceRow);
    }

    for (const [language, groupRows] of Object.entries(deviceGroups)) {
      for (const chunkRows of chunkArray(groupRows, 500)) {
        const message = buildMulticastMessage(
          notification,
          language,
          imageUrl,
          chunkRows.map((row) => row.push_token)
        );

        const response = await firebaseApp.messaging().sendEachForMulticast(message);

        response.responses.forEach((result, index) => {
          const deviceRow = chunkRows[index];
          if (result.success) {
            successfulDevices += 1;
            return;
          }

          failedDevices += 1;
          const errorCode = result.error?.code || 'unknown';
          if (INVALID_PUSH_TOKEN_CODES.has(errorCode)) {
            invalidDeviceIds.push(deviceRow.id);
          }

          if (errorSamples.length < 10) {
            errorSamples.push({
              device_id: deviceRow.id,
              installation_id: deviceRow.installation_id,
              error_code: errorCode,
              error_message: result.error?.message || 'Firebase rejected the message'
            });
          }
        });
      }
    }
  }

  await markInvalidTokens(connection, [...new Set(invalidDeviceIds)]);

  const executionPayload = {
    audience_type: notification.audience_type,
    filters: notification.filters || {},
    invalidated_device_ids: [...new Set(invalidDeviceIds)],
    error_samples: errorSamples
  };

  const [executionResult] = await connection.query(
    `INSERT INTO push_notification_executions (
      push_notification_id,
      executed_by_user_id,
      audience_type,
      total_registered_devices,
      targeted_devices,
      successful_devices,
      failed_devices,
      invalidated_devices,
      result_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      notification.id,
      executedByUserId,
      notification.audience_type,
      totalRegisteredDevices,
      targetedDevices,
      successfulDevices,
      failedDevices,
      [...new Set(invalidDeviceIds)].length,
      JSON.stringify(executionPayload)
    ]
  );

  // Historial por usuario (inbox de la app nativa). No debe tumbar el envío si falla.
  try {
    await recordNotificationRecipients(connection, notification.id, executionResult.insertId, audienceDevices);
  } catch (recipientError) {
    logger.error('Error recording push notification recipients:', recipientError);
  }

  await connection.query(
    'UPDATE push_notifications SET last_executed_at = NOW() WHERE id = ?',
    [notification.id]
  );

  return {
    execution_id: executionResult.insertId,
    total_registered_devices: totalRegisteredDevices,
    targeted_devices: targetedDevices,
    successful_devices: successfulDevices,
    failed_devices: failedDevices,
    invalidated_devices: [...new Set(invalidDeviceIds)].length
  };
}

router.post('/push-notifications/devices/sync', async (req, res) => {
  const installationId = String(req.body?.installation_id || '').trim();
  const pushToken = String(req.body?.push_token || '').trim();
  const platform = String(req.body?.platform || '').trim();
  const language = String(req.body?.language || 'en').trim().toLowerCase();
  const clearUser = parseBoolean(req.body?.clear_user);
  const authenticatedUser = getOptionalAuthUser(req);

  if (!installationId) {
    return res.status(400).json({
      error: 'Validation Error',
      message: 'installation_id is required'
    });
  }

  if (!pushToken) {
    return res.status(400).json({
      error: 'Validation Error',
      message: 'push_token is required'
    });
  }

  if (!['android', 'ios'].includes(platform)) {
    return res.status(400).json({
      error: 'Validation Error',
      message: 'platform must be "android" or "ios"'
    });
  }

  if (!['en', 'es'].includes(language)) {
    return res.status(400).json({
      error: 'Validation Error',
      message: 'language must be "en" or "es"'
    });
  }

  let connection;

  try {
    connection = await mysqlConnection.promise().getConnection();

    const [installationRows] = await connection.query(
      `SELECT id, last_user_id
       FROM push_device_tokens
       WHERE installation_id = ?
       LIMIT 1`,
      [installationId]
    );

    let deviceRow = installationRows[0] || null;

    if (!deviceRow) {
      const [tokenRows] = await connection.query(
        `SELECT id, last_user_id
         FROM push_device_tokens
         WHERE push_token = ?
         ORDER BY updated_at DESC
         LIMIT 1`,
        [pushToken]
      );

      deviceRow = tokenRows[0] || null;
    }

    const currentUserId = clearUser ? null : (authenticatedUser?.id || null);
    const lastUserId = authenticatedUser?.id || deviceRow?.last_user_id || null;

    if (deviceRow) {
      await connection.query(
        `UPDATE push_device_tokens
         SET installation_id = ?,
             push_token = ?,
             platform = ?,
             language = ?,
             user_id = ?,
             last_user_id = ?,
             token_status = 'ACTIVE',
             deleted = 'N',
             last_error_at = NULL,
             last_error_code = NULL,
             last_seen_at = NOW(),
             last_registered_at = NOW(),
             updated_at = NOW()
         WHERE id = ?`,
        [
          installationId,
          pushToken,
          platform,
          language,
          currentUserId,
          lastUserId,
          deviceRow.id
        ]
      );

      return res.status(200).json({
        id: deviceRow.id,
        installation_id: installationId,
        user_id: currentUserId,
        last_user_id: lastUserId,
        token_status: 'ACTIVE'
      });
    }

    const [result] = await connection.query(
      `INSERT INTO push_device_tokens (
        installation_id,
        push_token,
        platform,
        language,
        user_id,
        last_user_id,
        token_status,
        deleted,
        last_registered_at,
        last_seen_at
      ) VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', 'N', NOW(), NOW())`,
      [
        installationId,
        pushToken,
        platform,
        language,
        currentUserId,
        lastUserId
      ]
    );

    return res.status(201).json({
      id: result.insertId,
      installation_id: installationId,
      user_id: currentUserId,
      last_user_id: lastUserId,
      token_status: 'ACTIVE'
    });
  } catch (error) {
    logger.error('Error syncing push device token:', error);
    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Could not sync push device token'
    });
  } finally {
    connection?.release();
  }
});

// ============ USER-FACING INBOX (historial de notificaciones por usuario) ============
// Historial de notificaciones push del usuario logueado para la app nativa (Android/iOS).
// IMPORTANTE: estas rutas '/push-notifications/inbox*' se declaran ANTES de
// '/push-notifications/:id' (admin) para que 'inbox' no se interprete como un :id.

// GET /push-notifications/inbox?page=&pageSize= - lista paginada (más reciente primero)
router.get('/push-notifications/inbox', verifyToken, async (req, res) => {
  const userId = getAuthUserId(req);
  if (!userId) {
    return res.status(401).json({ error: 'Unauthorized', message: 'Authentication required' });
  }

  try {
    const page = Math.max(Number.parseInt(req.query.page, 10) || 1, 1);
    const pageSize = Math.min(Math.max(Number.parseInt(req.query.pageSize, 10) || 10, 1), 50);
    const offset = (page - 1) * pageSize;

    const [countRows] = await mysqlConnection.promise().query(
      'SELECT COUNT(*) AS total FROM push_notification_recipients WHERE user_id = ?',
      [userId]
    );
    const total = countRows[0]?.total || 0;
    const totalPages = total > 0 ? Math.ceil(total / pageSize) : 0;

    const [rows] = await mysqlConnection.promise().query(
      `SELECT pn.id, pn.message_en, pn.message_es, pn.image_s3_key,
              r.delivered_at, r.read_at
         FROM push_notification_recipients r
         INNER JOIN push_notifications pn ON pn.id = r.push_notification_id
        WHERE r.user_id = ?
        ORDER BY r.delivered_at DESC, r.id DESC
        LIMIT ? OFFSET ?`,
      [userId, pageSize, offset]
    );

    const notifications = await Promise.all(rows.map((row) => mapInboxNotificationRow(row)));
    const unreadCount = await getUnreadInboxCount(userId);

    return res.status(200).json({
      notifications,
      total,
      page,
      pageSize,
      totalPages,
      unread_count: unreadCount
    });
  } catch (error) {
    logger.error('Error retrieving notification inbox:', error);
    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Could not retrieve notifications'
    });
  }
});

// GET /push-notifications/inbox/unread-count - sólo el contador (para el badge)
router.get('/push-notifications/inbox/unread-count', verifyToken, async (req, res) => {
  const userId = getAuthUserId(req);
  if (!userId) {
    return res.status(401).json({ error: 'Unauthorized', message: 'Authentication required' });
  }

  try {
    const summary = await getInboxSummary(userId);
    return res.status(200).json({ unread_count: summary.unread_count, latest_id: summary.latest_id });
  } catch (error) {
    logger.error('Error retrieving inbox unread count:', error);
    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Could not retrieve unread count'
    });
  }
});

// POST /push-notifications/inbox/read-all - marca todas como leídas
router.post('/push-notifications/inbox/read-all', verifyToken, async (req, res) => {
  const userId = getAuthUserId(req);
  if (!userId) {
    return res.status(401).json({ error: 'Unauthorized', message: 'Authentication required' });
  }

  try {
    await mysqlConnection.promise().query(
      `UPDATE push_notification_recipients
          SET read_at = NOW()
        WHERE user_id = ? AND read_at IS NULL`,
      [userId]
    );
    return res.status(200).json({ unread_count: 0 });
  } catch (error) {
    logger.error('Error marking all notifications as read:', error);
    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Could not update notifications'
    });
  }
});

// POST /push-notifications/inbox/:id/read - marca una notificación como leída (:id = push_notification_id)
router.post('/push-notifications/inbox/:id/read', verifyToken, async (req, res) => {
  const userId = getAuthUserId(req);
  if (!userId) {
    return res.status(401).json({ error: 'Unauthorized', message: 'Authentication required' });
  }

  const notificationId = normalizePositiveInteger(req.params.id);
  if (!notificationId) {
    return res.status(400).json({ error: 'Validation Error', message: 'Invalid notification id' });
  }

  try {
    await mysqlConnection.promise().query(
      `UPDATE push_notification_recipients
          SET read_at = NOW()
        WHERE user_id = ? AND push_notification_id = ? AND read_at IS NULL`,
      [userId, notificationId]
    );

    const unreadCount = await getUnreadInboxCount(userId);
    return res.status(200).json({
      notification_id: notificationId,
      read: true,
      unread_count: unreadCount
    });
  } catch (error) {
    logger.error('Error marking notification as read:', error);
    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Could not update notification'
    });
  }
});

router.get('/push-notifications', verifyToken, verifyAdmin, async (req, res) => {
  try {
    const page = Math.max(Number.parseInt(req.query.page, 10) || 1, 1);
    const pageSize = Math.min(Math.max(Number.parseInt(req.query.pageSize, 10) || 10, 1), 100);
    const offset = (page - 1) * pageSize;

    const [countRows] = await mysqlConnection.promise().query(
      'SELECT COUNT(*) AS total FROM push_notifications'
    );
    const total = countRows[0]?.total || 0;
    const totalPages = total > 0 ? Math.ceil(total / pageSize) : 0;

    const [rows] = await mysqlConnection.promise().query(
      `SELECT id, audience_type, message_en, message_es, filters_json, filters_chip_json,
              image_s3_key, created_at, updated_at, last_executed_at
       FROM push_notifications
       ORDER BY created_at DESC
       LIMIT ? OFFSET ?`,
      [pageSize, offset]
    );

    const notifications = await Promise.all(rows.map((row) => mapNotificationRow(row)));

    return res.status(200).json({
      notifications,
      total,
      page,
      pageSize,
      totalPages
    });
  } catch (error) {
    logger.error('Error retrieving push notifications:', error);
    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Could not retrieve push notifications'
    });
  }
});

router.get('/push-notifications/:id', verifyToken, verifyAdmin, async (req, res) => {
  const notificationId = normalizePositiveInteger(req.params.id);
  if (!notificationId) {
    return res.status(400).json({
      error: 'Validation Error',
      message: 'Invalid notification id'
    });
  }

  try {
    const notification = await getNotificationById(notificationId);
    if (!notification) {
      return res.status(404).json({
        error: 'Not Found',
        message: 'Push notification not found'
      });
    }

    return res.status(200).json(notification);
  } catch (error) {
    logger.error('Error retrieving push notification:', error);
    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Could not retrieve push notification'
    });
  }
});

router.post('/push-notifications', verifyToken, verifyAdmin, (req, res) => {
  upload(req, res, async (uploadError) => {
    if (uploadError) {
      return res.status(400).json({
        error: 'Validation Error',
        message: 'Image upload failed',
        details: [{ field: 'image', message: uploadError.message }]
      });
    }

    const normalizedPayload = {
      audience_type: String(req.body?.audience_type || '').trim(),
      message_en: String(req.body?.message_en || '').trim(),
      message_es: String(req.body?.message_es || '').trim(),
      filters: normalizeAudienceFilters(req.body?.filters),
      filters_chip: normalizeFilterChips(req.body?.filters_chip),
      has_valid_image: !req.file || String(req.file.mimetype || '').startsWith('image/')
    };

    const validationErrors = validateNotificationPayload(normalizedPayload, Boolean(req.file));
    if (validationErrors.length > 0) {
      return res.status(400).json({
        error: 'Validation Error',
        message: 'Invalid push notification payload',
        details: validationErrors
      });
    }

    let uploadedImageKey = null;

    try {
      if (req.file) {
        uploadedImageKey = await uploadImageToS3(req.file);
      }

      const [result] = await mysqlConnection.promise().query(
        `INSERT INTO push_notifications (
          audience_type,
          message_en,
          message_es,
          filters_json,
          filters_chip_json,
          image_s3_key,
          created_by_user_id,
          updated_by_user_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          normalizedPayload.audience_type,
          normalizedPayload.message_en,
          normalizedPayload.message_es,
          JSON.stringify(normalizedPayload.filters),
          JSON.stringify(normalizedPayload.filters_chip),
          uploadedImageKey,
          req.currentUser.id,
          req.currentUser.id
        ]
      );

      const notification = await getNotificationById(result.insertId);
      return res.status(201).json(notification);
    } catch (error) {
      if (uploadedImageKey) {
        try {
          await deleteImageFromS3(uploadedImageKey);
        } catch (cleanupError) {
          logger.error('Error cleaning up uploaded push notification image:', cleanupError);
        }
      }

      logger.error('Error creating push notification:', error);
      return res.status(500).json({
        error: 'Internal Server Error',
        message: 'Could not create push notification'
      });
    }
  });
});

router.put('/push-notifications/:id', verifyToken, verifyAdmin, (req, res) => {
  upload(req, res, async (uploadError) => {
    if (uploadError) {
      return res.status(400).json({
        error: 'Validation Error',
        message: 'Image upload failed',
        details: [{ field: 'image', message: uploadError.message }]
      });
    }

    const notificationId = normalizePositiveInteger(req.params.id);
    if (!notificationId) {
      return res.status(400).json({
        error: 'Validation Error',
        message: 'Invalid notification id'
      });
    }

    const removeImage = parseBoolean(req.body?.remove_image);
    const normalizedPayload = {
      audience_type: String(req.body?.audience_type || '').trim(),
      message_en: String(req.body?.message_en || '').trim(),
      message_es: String(req.body?.message_es || '').trim(),
      filters: normalizeAudienceFilters(req.body?.filters),
      filters_chip: normalizeFilterChips(req.body?.filters_chip),
      has_valid_image: !req.file || String(req.file.mimetype || '').startsWith('image/')
    };

    const validationErrors = validateNotificationPayload(normalizedPayload, Boolean(req.file));
    if (validationErrors.length > 0) {
      return res.status(400).json({
        error: 'Validation Error',
        message: 'Invalid push notification payload',
        details: validationErrors
      });
    }

    let uploadedImageKey = null;

    try {
      const [existingRows] = await mysqlConnection.promise().query(
        'SELECT image_s3_key FROM push_notifications WHERE id = ? LIMIT 1',
        [notificationId]
      );

      if (existingRows.length === 0) {
        return res.status(404).json({
          error: 'Not Found',
          message: 'Push notification not found'
        });
      }

      if (req.file) {
        uploadedImageKey = await uploadImageToS3(req.file);
      }

      const existingImageKey = existingRows[0].image_s3_key || null;
      const nextImageKey = removeImage
        ? null
        : (uploadedImageKey || existingImageKey);

      await mysqlConnection.promise().query(
        `UPDATE push_notifications
         SET audience_type = ?,
             message_en = ?,
             message_es = ?,
             filters_json = ?,
             filters_chip_json = ?,
             image_s3_key = ?,
             updated_by_user_id = ?,
             updated_at = NOW()
         WHERE id = ?`,
        [
          normalizedPayload.audience_type,
          normalizedPayload.message_en,
          normalizedPayload.message_es,
          JSON.stringify(normalizedPayload.filters),
          JSON.stringify(normalizedPayload.filters_chip),
          nextImageKey,
          req.currentUser.id,
          notificationId
        ]
      );

      if ((req.file || removeImage) && existingImageKey && existingImageKey !== nextImageKey) {
        try {
          await deleteImageFromS3(existingImageKey);
        } catch (cleanupError) {
          logger.error('Error deleting previous push notification image:', cleanupError);
        }
      }

      const notification = await getNotificationById(notificationId);
      return res.status(200).json(notification);
    } catch (error) {
      if (uploadedImageKey) {
        try {
          await deleteImageFromS3(uploadedImageKey);
        } catch (cleanupError) {
          logger.error('Error cleaning up replacement push notification image:', cleanupError);
        }
      }

      logger.error('Error updating push notification:', error);
      return res.status(500).json({
        error: 'Internal Server Error',
        message: 'Could not update push notification'
      });
    }
  });
});

router.delete('/push-notifications/:id', verifyToken, verifyAdmin, async (req, res) => {
  const notificationId = normalizePositiveInteger(req.params.id);
  if (!notificationId) {
    return res.status(400).json({
      error: 'Validation Error',
      message: 'Invalid notification id'
    });
  }

  try {
    const [rows] = await mysqlConnection.promise().query(
      'SELECT image_s3_key FROM push_notifications WHERE id = ? LIMIT 1',
      [notificationId]
    );

    if (rows.length === 0) {
      return res.status(404).json({
        error: 'Not Found',
        message: 'Push notification not found'
      });
    }

    await mysqlConnection.promise().query(
      'DELETE FROM push_notifications WHERE id = ?',
      [notificationId]
    );

    if (rows[0].image_s3_key) {
      try {
        await deleteImageFromS3(rows[0].image_s3_key);
      } catch (cleanupError) {
        logger.error('Error deleting push notification image from S3:', cleanupError);
      }
    }

    return res.status(200).json({
      message: 'Push notification deleted successfully',
      deleted_id: notificationId
    });
  } catch (error) {
    logger.error('Error deleting push notification:', error);
    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Could not delete push notification'
    });
  }
});

router.post('/push-notifications/:id/execute', verifyToken, verifyAdmin, async (req, res) => {
  const notificationId = normalizePositiveInteger(req.params.id);
  if (!notificationId) {
    return res.status(400).json({
      error: 'Validation Error',
      message: 'Invalid notification id'
    });
  }

  let connection;

  try {
    connection = await mysqlConnection.promise().getConnection();

    const [rows] = await connection.query(
      `SELECT id, audience_type, message_en, message_es, filters_json, filters_chip_json,
              image_s3_key, created_at, updated_at, last_executed_at
       FROM push_notifications
       WHERE id = ?
       LIMIT 1`,
      [notificationId]
    );

    if (rows.length === 0) {
      return res.status(404).json({
        error: 'Not Found',
        message: 'Push notification not found'
      });
    }

    const notification = await mapNotificationRow(rows[0], 60 * 60 * 24 * 7);
    const execution = await executeNotification(connection, notification, req.currentUser.id);

    return res.status(200).json({
      notification_id: notification.id,
      ...execution
    });
  } catch (error) {
    logger.error('Error executing push notification:', error);
    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Could not execute push notification'
    });
  } finally {
    connection?.release();
  }
});

module.exports = router;
