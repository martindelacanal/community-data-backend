/**
 * Recurrencia y uso de app por participante.
 *
 * Responde dos preguntas que hasta ahora la tabla de Participants no podía
 * contestar: "¿cada cuánto viene esta persona a esta sede?" y "¿usa la app
 * nativa o solo la web?".
 *
 * Contratos que conviene no olvidar:
 *
 * - Una visita = un DÍA de calendario en America/Los_Angeles con al menos una
 *   fila en delivery_beneficiary. NO es COUNT(*): la tabla no tiene unique key
 *   por (usuario, sede, día) y un doble escaneo del mismo voluntario, o dos
 *   voluntarios escaneando a la vez, generan filas repetidas. Ver
 *   services/sameDayDelivery.js, que existe justamente para detectar eso.
 *
 * - "Visitas con retiro" marca un día como retiro cuando alguna fila de ese día
 *   tiene `delivering_user_id IS NOT NULL` (es decir, hubo un voluntario que
 *   escaneó). Usa el mismo criterio que la serie "Food pickups" de los gráficos
 *   (user.js:18264) pero NO da el mismo número: aquel cuenta filas y este
 *   cuenta días, así que no se pueden conciliar uno contra otro. `approved` no
 *   se usa porque el codebase se contradice sobre qué significa.
 *
 * - Cuando hay sedes seleccionadas la recurrencia se cuenta SOLO en esas sedes:
 *   la pregunta del usuario siempre es "cuánto viene a esta sede", no "cuánto
 *   viene en total".
 *
 * - `location_match` existe porque el filtro de sedes histórico significaba
 *   `first_location_id` (la sede donde se registró), que no es donde la persona
 *   efectivamente va. Se mantiene 'registered' como default para no cambiar en
 *   silencio los números de una pantalla ya publicada.
 *
 * - App vs web solo se puede responder desde 2026-03-23 (primer release nativo)
 *   y no hay backfill. Por eso el valor negativo se llama `no_app_detected` y
 *   nunca "web only": ausencia de señal no es prueba de que no usa la app.
 */

const LA_TIME_ZONE_SQL = "'America/Los_Angeles'";
const UTC_TIME_ZONE_SQL = "'+00:00'";

const LOCATION_MATCH_MODES = new Set(['registered', 'attended', 'any']);
const APP_USAGE_MODES = new Set(['all', 'app', 'no_app']);

/** Día LA de una fila de delivery_beneficiary. */
const VISIT_DAY_SQL = `DATE(CONVERT_TZ(db.creation_date, ${UTC_TIME_ZONE_SQL}, ${LA_TIME_ZONE_SQL}))`;
/** Hoy en LA, para "días desde la última visita". */
const TODAY_LA_SQL = `DATE(CONVERT_TZ(UTC_TIMESTAMP(), ${UTC_TIME_ZONE_SQL}, ${LA_TIME_ZONE_SQL}))`;

const ISO_DATE_REGEX = /^\d{4}-\d{2}-\d{2}$/;

function toPositiveIntList(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  const ids = [];
  for (const raw of value) {
    const id = Number(raw);
    if (Number.isInteger(id) && id > 0) {
      ids.push(id);
    }
  }
  return [...new Set(ids)];
}

/**
 * Acepta 'YYYY-MM-DD', un Date, o el ISO que manda el datepicker de Angular, y
 * devuelve siempre una fecha de calendario. Nunca usa new Date(...) sobre un
 * string ya normalizado: eso reinterpreta la fecha en UTC y corre un día.
 */
function toCalendarDate(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  if (typeof value === 'string') {
    if (ISO_DATE_REGEX.test(value)) {
      return value;
    }
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString().slice(0, 10);
  }
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value.toISOString().slice(0, 10);
  }
  return null;
}

function toNonNegativeInt(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return null;
  }
  return Math.floor(parsed);
}

/**
 * Normaliza el cuerpo del POST en algo con lo que se pueda construir SQL sin
 * volver a desconfiar de los tipos.
 */
function normalizeEngagementFilters(filters = {}, options = {}) {
  const safeFilters = filters && typeof filters === 'object' ? filters : {};
  const locations = toPositiveIntList(safeFilters.locations);
  const requestedMatch = typeof safeFilters.location_match === 'string' ? safeFilters.location_match : '';

  let visitsFrom = toCalendarDate(safeFilters.visits_from_date);
  let visitsTo = toCalendarDate(safeFilters.visits_to_date);
  if (visitsFrom && visitsTo && visitsFrom > visitsTo) {
    [visitsFrom, visitsTo] = [visitsTo, visitsFrom];
  }

  let minVisits = toNonNegativeInt(safeFilters.min_visits);
  let maxVisits = toNonNegativeInt(safeFilters.max_visits);
  if (minVisits !== null && maxVisits !== null && minVisits > maxVisits) {
    [minVisits, maxVisits] = [maxVisits, minVisits];
  }

  // Scope de cliente: un admin de cliente solo puede ver asistencia en SUS
  // sedes. Sin esto, las visitas y "Locations visited" filtrarían a qué otras
  // organizaciones va un participante.
  const clientId = toNonNegativeInt(options.clientId);

  return {
    locations,
    // Sin sedes seleccionadas el modo no significa nada: se cae a 'registered'
    // para que el SQL no arrastre una condición vacía.
    locationMatch: locations.length > 0 && LOCATION_MATCH_MODES.has(requestedMatch) ? requestedMatch : 'registered',
    visitsFrom,
    visitsTo,
    minVisits,
    maxVisits,
    appUsage: APP_USAGE_MODES.has(safeFilters.app_usage) ? safeFilters.app_usage : 'all',
    clientId: clientId && clientId > 0 ? clientId : null
  };
}

/**
 * true si alguna condición referencia las tablas derivadas (rec / pdt / sess) y
 * por lo tanto los JOIN son obligatorios aunque no se pidan columnas.
 *
 * El filtro por sede en modo 'registered' NO entra acá: se resuelve contra
 * u.first_location_id y no necesita ningún JOIN.
 */
function engagementNeedsRecurrenceJoin(engagement) {
  return (engagement.locations.length > 0 && engagement.locationMatch !== 'registered')
    || engagement.minVisits !== null
    || engagement.maxVisits !== null;
}

/** Ídem para pdt/sess. Se decide aparte: el agregado de recurrencia recorre las
 *  173k filas de delivery_beneficiary y no hace falta para filtrar por app. */
function engagementNeedsAppJoin(engagement) {
  return engagement.appUsage !== 'all';
}

function engagementNeedsJoins(engagement) {
  return engagementNeedsRecurrenceJoin(engagement) || engagementNeedsAppJoin(engagement);
}

/**
 * Tabla derivada con la recurrencia por participante, acotada a las sedes y al
 * período de visitas elegidos. Se agrupa una sola vez y se joinea 1:1, en vez
 * de resolver con subconsultas correlacionadas (que sobre los 60k participantes
 * de producción tardan ~40s en lugar de ~0.6s).
 */
function buildRecurrenceJoin(engagement, { withVisitedLocations = false, userIds = null } = {}) {
  const params = [];
  const conditions = ['1 = 1'];

  // Al hidratar una página de la tabla el agregado se acota a esos ids: sobre
  // las 173k filas de delivery_beneficiary agrupar todo cuesta ~700ms, y para
  // 10 filas visibles no hace falta.
  if (Array.isArray(userIds) && userIds.length > 0) {
    conditions.push(`db.receiving_user_id IN (${userIds.join(',')})`);
  }
  if (engagement.locations.length > 0) {
    conditions.push(`db.location_id IN (${engagement.locations.join(',')})`);
  }
  // Un admin de cliente solo cuenta visitas en las sedes de su cliente: sin
  // esto vería que uno de sus participantes también asiste a otra organización.
  if (engagement.clientId) {
    conditions.push(
      'db.location_id IN (SELECT cl.location_id FROM client_location AS cl WHERE cl.client_id = ?)'
    );
    params.push(engagement.clientId);
  }
  // La columna se deja sin envolver para que sigan sirviendo los índices
  // (idx_delivery_metrics_created_location_user); se convierte el borde.
  if (engagement.visitsFrom) {
    conditions.push(
      `db.creation_date >= CONVERT_TZ(CONCAT(?, ' 00:00:00'), ${LA_TIME_ZONE_SQL}, ${UTC_TIME_ZONE_SQL})`
    );
    params.push(engagement.visitsFrom);
  }
  if (engagement.visitsTo) {
    conditions.push(
      `db.creation_date < CONVERT_TZ(CONCAT(DATE_ADD(?, INTERVAL 1 DAY), ' 00:00:00'), ${LA_TIME_ZONE_SQL}, ${UTC_TIME_ZONE_SQL})`
    );
    params.push(engagement.visitsTo);
  }

  // Cortado a 10 sedes a propósito: GROUP_CONCAT trunca en seco a los 1024
  // bytes de group_concat_max_len y partiría un nombre por la mitad sin avisar.
  const visitedLocationsSelect = withVisitedLocations
    ? `,
             SUBSTRING_INDEX(
               GROUP_CONCAT(DISTINCT loc.community_city ORDER BY loc.community_city SEPARATOR ' | '),
               ' | ', 10
             ) AS visited_locations`
    : '';
  const visitedLocationsJoin = withVisitedLocations
    ? '\n             LEFT JOIN location AS loc ON loc.id = db.location_id'
    : '';

  const sql = `
      LEFT JOIN (
        SELECT db.receiving_user_id AS uid,
               COUNT(DISTINCT ${VISIT_DAY_SQL}) AS visit_days,
               COUNT(DISTINCT CASE WHEN db.delivering_user_id IS NOT NULL THEN ${VISIT_DAY_SQL} END) AS visit_days_with_pickup,
               COUNT(DISTINCT DATE_FORMAT(CONVERT_TZ(db.creation_date, ${UTC_TIME_ZONE_SQL}, ${LA_TIME_ZONE_SQL}), '%Y-%m')) AS visit_months,
               MIN(${VISIT_DAY_SQL}) AS first_visit,
               MAX(${VISIT_DAY_SQL}) AS last_visit${visitedLocationsSelect}
        FROM delivery_beneficiary AS db${visitedLocationsJoin}
        WHERE ${conditions.join('\n          AND ')}
        GROUP BY db.receiving_user_id
      ) AS rec ON rec.uid = u.id`;

  return { sql, params };
}

/**
 * Señales de uso de app. `user.android_version` / `user.ios_version` cubren
 * prácticamente todo (las escribe PUT /mobile-app/version, que solo llama el
 * shell de Capacitor); push_device_tokens e interaction_sessions se suman como
 * respaldo para los casos en que el usuario nunca abrió la app estando logueado
 * pero sí registró el device o navegó contenido.
 */
function buildAppSignalsJoin({ userIds = null } = {}) {
  const idScope = Array.isArray(userIds) && userIds.length > 0 ? userIds.join(',') : null;
  const pdtScope = idScope ? `\n          AND COALESCE(d.user_id, d.last_user_id) IN (${idScope})` : '';
  const sessScope = idScope ? `\n          AND s.user_id IN (${idScope})` : '';
  return `
      LEFT JOIN (
        SELECT COALESCE(d.user_id, d.last_user_id) AS uid,
               MAX(d.last_seen_at) AS last_seen_at,
               -- MIN a secas escondería el caso de dos plataformas: si el mismo
               -- usuario registró un Android y un iPhone, esto dice 'both'.
               IF(MIN(d.platform) = MAX(d.platform), MIN(d.platform), 'both') AS platform
        FROM push_device_tokens AS d
        WHERE d.deleted = 'N' AND COALESCE(d.user_id, d.last_user_id) IS NOT NULL${pdtScope}
        GROUP BY uid
      ) AS pdt ON pdt.uid = u.id
      LEFT JOIN (
        SELECT s.user_id AS uid,
               MAX(s.started_at) AS last_app_session_at,
               IF(MIN(s.access_channel) = MAX(s.access_channel),
                  REPLACE(MIN(s.access_channel), 'capacitor_', ''),
                  'both') AS app_channel
        FROM interaction_sessions AS s
        WHERE s.user_id IS NOT NULL
          AND s.access_channel IN ('capacitor_android', 'capacitor_ios')${sessScope}
        GROUP BY s.user_id
      ) AS sess ON sess.uid = u.id`;
}

const HAS_APP_SIGNAL_SQL =
  '(u.android_version IS NOT NULL OR u.ios_version IS NOT NULL OR pdt.uid IS NOT NULL OR sess.uid IS NOT NULL)';

const APP_USAGE_SQL = `CASE
        WHEN u.android_version IS NOT NULL AND u.ios_version IS NOT NULL THEN 'app_both'
        WHEN u.android_version IS NOT NULL THEN 'app_android'
        WHEN u.ios_version IS NOT NULL THEN 'app_ios'
        WHEN pdt.platform IS NOT NULL THEN CONCAT('app_', pdt.platform)
        WHEN sess.app_channel IS NOT NULL THEN CONCAT('app_', sess.app_channel)
        ELSE 'no_app_detected'
      END`;

/**
 * Centinela para GREATEST: con un NULL adentro GREATEST devuelve NULL y se
 * perdería la fecha de las otras señales.
 *
 * El CAST a DATETIME no es decorativo: sin él GREATEST devuelve el string
 * '1000-01-01', NULLIF no lo reconoce contra '1000-01-01 00:00:00' y
 * DATE_FORMAT sobre ese string escupe '10/00/2000' en vez de NULL.
 */
const NEVER_SEEN_SENTINEL_SQL = "CAST('1000-01-01' AS DATETIME)";
const LAST_APP_SEEN_SQL = `NULLIF(GREATEST(
        COALESCE(u.android_version_updated_at, ${NEVER_SEEN_SENTINEL_SQL}),
        COALESCE(u.ios_version_updated_at, ${NEVER_SEEN_SENTINEL_SQL}),
        COALESCE(pdt.last_seen_at, ${NEVER_SEEN_SENTINEL_SQL}),
        COALESCE(sess.last_app_session_at, ${NEVER_SEEN_SENTINEL_SQL})
      ), ${NEVER_SEEN_SENTINEL_SQL})`;

/**
 * Columnas de recurrencia + app.
 *
 * @param {boolean} options.aggregated envuelve todo en MAX() para consultas que
 *   ya traen GROUP BY u.id (el CSV lo tiene por el LEFT JOIN a client_user).
 *   Como las tablas derivadas son 1:1 con u.id, MAX() es la identidad y evita
 *   depender de que ONLY_FULL_GROUP_BY esté apagado.
 */
function buildEngagementSelects({ aggregated = false, withVisitedLocations = false, dateFormat = '%m/%d/%Y' } = {}) {
  const wrap = (expression) => (aggregated ? `MAX(${expression})` : expression);
  const selects = [
    `${wrap('COALESCE(rec.visit_days, 0)')} AS visit_days`,
    `${wrap('COALESCE(rec.visit_days_with_pickup, 0)')} AS visit_days_with_pickup`,
    `${wrap('COALESCE(rec.visit_months, 0)')} AS visit_months`,
    `${wrap(`DATE_FORMAT(rec.first_visit, '${dateFormat}')`)} AS first_visit`,
    `${wrap(`DATE_FORMAT(rec.last_visit, '${dateFormat}')`)} AS last_visit`,
    `${wrap(`DATEDIFF(${TODAY_LA_SQL}, rec.last_visit)`)} AS days_since_last_visit`,
    // Promedio de días entre visitas: solo tiene sentido con 2 o más visitas.
    `${wrap('ROUND(DATEDIFF(rec.last_visit, rec.first_visit) / NULLIF(rec.visit_days - 1, 0), 1)')} AS avg_days_between_visits`,
    `${wrap(APP_USAGE_SQL)} AS app_usage`,
    `${wrap('COALESCE(u.android_version, u.ios_version)')} AS app_version`,
    // Las cuatro señales se guardan en UTC; se convierten a LA como el resto
    // de las fechas de la pantalla, o el día no coincidiría con "Última visita".
    `${wrap(`DATE_FORMAT(CONVERT_TZ(${LAST_APP_SEEN_SQL}, ${UTC_TIME_ZONE_SQL}, ${LA_TIME_ZONE_SQL}), '${dateFormat}')`)} AS app_last_seen`
  ];
  if (withVisitedLocations) {
    selects.push(`${wrap('rec.visited_locations')} AS visited_locations`);
  }
  return selects.join(',\n                ');
}

/** Condiciones WHERE de los filtros nuevos, con sus parámetros en orden. */
function buildEngagementConditions(engagement) {
  const conditions = [];
  const params = [];

  if (engagement.locations.length > 0) {
    const registeredHere = `u.first_location_id IN (${engagement.locations.join(',')})`;
    // rec ya viene acotado a esas sedes, así que "tiene visitas" == "fue ahí".
    const attendedHere = 'rec.visit_days > 0';
    if (engagement.locationMatch === 'attended') {
      conditions.push(`AND (${attendedHere})`);
    } else if (engagement.locationMatch === 'any') {
      conditions.push(`AND (${registeredHere} OR ${attendedHere})`);
    } else {
      conditions.push(`AND (${registeredHere})`);
    }
  }

  if (engagement.minVisits !== null) {
    conditions.push('AND COALESCE(rec.visit_days, 0) >= ?');
    params.push(engagement.minVisits);
  }
  if (engagement.maxVisits !== null) {
    conditions.push('AND COALESCE(rec.visit_days, 0) <= ?');
    params.push(engagement.maxVisits);
  }

  if (engagement.appUsage === 'app') {
    conditions.push(`AND ${HAS_APP_SIGNAL_SQL}`);
  } else if (engagement.appUsage === 'no_app') {
    conditions.push(`AND NOT ${HAS_APP_SIGNAL_SQL}`);
  }

  return { sql: conditions.join('\n      '), params };
}

/**
 * Todo lo que hace falta para enchufar recurrencia y app a una consulta que ya
 * tiene `FROM user AS u`.
 *
 * @param {boolean} options.withColumns false en los COUNT: ahí los JOIN solo se
 *   agregan si algún filtro realmente recorta filas (son 1:1, no cambian el total).
 */
function buildParticipantEngagementSql(engagement, options = {}) {
  const {
    withColumns = true,
    withVisitedLocations = false,
    aggregated = false,
    dateFormat = '%m/%d/%Y',
    // Para ORDER BY por columnas de recurrencia: el orden necesita los JOIN
    // aunque ningún filtro los pida.
    forceJoins = false
  } = options;

  // Las condiciones se devuelven siempre: el filtro por sede de registro no
  // depende de ningún JOIN y omitirlo dejaría pasar a todos los participantes.
  const conditions = buildEngagementConditions(engagement);

  // Recurrencia y señales de app se piden por separado. Filtrar por "usa la
  // app" no tiene por qué pagar el agregado de las 173k filas de reparto.
  const wantsRecurrence = withColumns || forceJoins || engagementNeedsRecurrenceJoin(engagement);
  const wantsAppSignals = withColumns || forceJoins || engagementNeedsAppJoin(engagement);

  const recurrence = wantsRecurrence
    ? buildRecurrenceJoin(engagement, { withVisitedLocations })
    : { sql: '', params: [] };
  const appSignals = wantsAppSignals ? buildAppSignalsJoin() : '';

  return {
    joins: `${recurrence.sql}${appSignals}`,
    joinParams: recurrence.params,
    selects: withColumns ? buildEngagementSelects({ aggregated, withVisitedLocations, dateFormat }) : '',
    conditions: conditions.sql,
    conditionParams: conditions.params
  };
}

/**
 * Recurrencia y uso de app para un puñado de participantes ya elegidos (la
 * página visible de la tabla). Evita agrupar delivery_beneficiary entera
 * cuando ningún filtro ni orden lo necesita.
 *
 * @returns {Promise<Map<number, object>>} indexado por user id.
 */
async function fetchParticipantEngagementByUserIds(connection, userIds, engagement) {
  const ids = toPositiveIntList(userIds);
  if (ids.length === 0) {
    return new Map();
  }

  const recurrence = buildRecurrenceJoin(engagement, { userIds: ids });
  const [rows] = await connection.promise().query(
    `SELECT u.id AS user_id,
            ${buildEngagementSelects()}
     FROM user AS u${recurrence.sql}${buildAppSignalsJoin({ userIds: ids })}
     WHERE u.id IN (${ids.join(',')})`,
    recurrence.params
  );

  return new Map(rows.map((row) => [row.user_id, row]));
}

/** Pega las columnas de recurrencia/app sobre las filas ya paginadas. */
async function hydrateParticipantEngagement(connection, rows, engagement) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return rows;
  }
  const byUserId = await fetchParticipantEngagementByUserIds(
    connection,
    rows.map((row) => row.id),
    engagement
  );
  for (const row of rows) {
    const engagementRow = byUserId.get(row.id);
    if (!engagementRow) {
      continue;
    }
    const { user_id, ...columns } = engagementRow;
    Object.assign(row, columns);
  }
  return rows;
}

/** Columnas ordenables que aporta este módulo, para buildTableOrder. */
const ENGAGEMENT_ORDER_COLUMNS = {
  visit_days: 'rec.visit_days',
  last_visit: 'rec.last_visit',
  app_usage: APP_USAGE_SQL
};

/**
 * Texto que se estampa en el CSV para que un archivo descargado diga siempre
 * sobre qué universo se contaron las visitas.
 */
function describeVisitScope(engagement, locationNames = []) {
  const where = engagement.locations.length > 0
    ? (locationNames.length > 0 ? locationNames.join(' / ') : `Location ids ${engagement.locations.join(', ')}`)
    : 'All locations';
  const from = engagement.visitsFrom || 'start';
  const to = engagement.visitsTo || 'today';
  return `${where} (${from} to ${to})`;
}

module.exports = {
  APP_USAGE_MODES,
  ENGAGEMENT_ORDER_COLUMNS,
  LOCATION_MATCH_MODES,
  buildParticipantEngagementSql,
  describeVisitScope,
  engagementNeedsAppJoin,
  engagementNeedsJoins,
  engagementNeedsRecurrenceJoin,
  fetchParticipantEngagementByUserIds,
  hydrateParticipantEngagement,
  normalizeEngagementFilters
};
