const DEFAULT_AUTH_SCOPE = 'all';

const PAGE_TYPE_LABELS = {
  articles_list: {
    en: 'Articles list',
    es: 'Listado de articulos'
  },
  article_detail: {
    en: 'Article detail',
    es: 'Detalle de articulo'
  },
  trusted_resources_list: {
    en: 'Trusted resources list',
    es: 'Listado de recursos confiables'
  },
  trusted_resource_detail: {
    en: 'Trusted resource detail',
    es: 'Detalle de recurso confiable'
  },
  calendar_month: {
    en: 'Calendar',
    es: 'Calendario'
  },
  calendar_event_detail: {
    en: 'Event detail',
    es: 'Detalle de evento'
  }
};

const ACTION_LABELS = {
  open_article_detail: {
    en: 'Open article detail',
    es: 'Abrir detalle de articulo'
  },
  open_trusted_resource_detail: {
    en: 'Open trusted resource detail',
    es: 'Abrir detalle de recurso confiable'
  },
  open_calendar_event_detail: {
    en: 'Open event detail',
    es: 'Abrir detalle de evento'
  },
  article_share_facebook: {
    en: 'Share article on Facebook',
    es: 'Compartir articulo en Facebook'
  },
  article_share_x: {
    en: 'Share article on X',
    es: 'Compartir articulo en X'
  },
  article_share_whatsapp: {
    en: 'Share article on WhatsApp',
    es: 'Compartir articulo en WhatsApp'
  },
  article_share_instagram: {
    en: 'Copy article for Instagram',
    es: 'Copiar articulo para Instagram'
  },
  trusted_resource_get_directions: {
    en: 'Open directions for resource',
    es: 'Abrir indicaciones del recurso'
  },
  trusted_resource_call_open: {
    en: 'Call trusted resource',
    es: 'Llamar al recurso confiable'
  },
  trusted_resource_call_copy: {
    en: 'Copy trusted resource phone',
    es: 'Copiar telefono del recurso confiable'
  },
  trusted_resource_web_open: {
    en: 'Open trusted resource website',
    es: 'Abrir sitio web del recurso confiable'
  },
  trusted_resource_web_copy: {
    en: 'Copy trusted resource website',
    es: 'Copiar sitio web del recurso confiable'
  },
  trusted_resource_email_open: {
    en: 'Send trusted resource email',
    es: 'Enviar email al recurso confiable'
  },
  trusted_resource_email_copy: {
    en: 'Copy trusted resource email',
    es: 'Copiar email del recurso confiable'
  },
  trusted_resource_share_facebook: {
    en: 'Share trusted resource on Facebook',
    es: 'Compartir recurso confiable en Facebook'
  },
  trusted_resource_share_x: {
    en: 'Share trusted resource on X',
    es: 'Compartir recurso confiable en X'
  },
  trusted_resource_share_whatsapp: {
    en: 'Share trusted resource on WhatsApp',
    es: 'Compartir recurso confiable en WhatsApp'
  },
  trusted_resource_share_instagram: {
    en: 'Copy trusted resource for Instagram',
    es: 'Copiar recurso confiable para Instagram'
  },
  calendar_event_share_facebook: {
    en: 'Share event on Facebook',
    es: 'Compartir evento en Facebook'
  },
  calendar_event_share_x: {
    en: 'Share event on X',
    es: 'Compartir evento en X'
  },
  calendar_event_share_whatsapp: {
    en: 'Share event on WhatsApp',
    es: 'Compartir evento en WhatsApp'
  },
  calendar_event_share_instagram: {
    en: 'Copy event for Instagram',
    es: 'Copiar evento para Instagram'
  },
  calendar_event_open_map: {
    en: 'Open event map',
    es: 'Abrir mapa del evento'
  }
};

const ACCESS_CHANNEL_LABELS = {
  web_desktop: {
    en: 'Web desktop',
    es: 'Web desktop'
  },
  web_mobile: {
    en: 'Web mobile',
    es: 'Web mobile'
  },
  capacitor_android: {
    en: 'App Android',
    es: 'App Android'
  },
  capacitor_ios: {
    en: 'App iOS',
    es: 'App iOS'
  },
  unknown: {
    en: 'Unknown',
    es: 'Desconocido'
  }
};

const OPERATING_SYSTEM_LABELS = {
  android: {
    en: 'Android',
    es: 'Android'
  },
  ios: {
    en: 'iOS',
    es: 'iOS'
  },
  windows: {
    en: 'Windows',
    es: 'Windows'
  },
  macos: {
    en: 'macOS',
    es: 'macOS'
  },
  linux: {
    en: 'Linux',
    es: 'Linux'
  },
  chromeos: {
    en: 'ChromeOS',
    es: 'ChromeOS'
  },
  unknown: {
    en: 'Unknown',
    es: 'Desconocido'
  }
};

const DEVICE_CATEGORY_LABELS = {
  desktop: {
    en: 'Desktop',
    es: 'Desktop'
  },
  mobile: {
    en: 'Mobile',
    es: 'Mobile'
  },
  tablet: {
    en: 'Tablet',
    es: 'Tablet'
  },
  unknown: {
    en: 'Unknown',
    es: 'Desconocido'
  }
};

function sanitizeDate(value) {
  if (!value) {
    return null;
  }

  const stringValue = String(value).trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(stringValue) ? stringValue : null;
}

function ensureArray(value) {
  if (Array.isArray(value)) {
    return value.filter((item) => item !== null && item !== undefined && String(item).trim() !== '');
  }

  if (typeof value === 'string' && value.trim() !== '') {
    return value
      .split(',')
      .map((item) => item.trim())
      .filter((item) => item !== '');
  }

  return [];
}

function normalizeAuthScope(value) {
  return value === 'authenticated' || value === 'anonymous'
    ? value
    : DEFAULT_AUTH_SCOPE;
}

function normalizeInteractionMetricFilters(filters = {}) {
  return {
    from_date: sanitizeDate(filters.from_date),
    to_date: sanitizeDate(filters.to_date),
    auth_scope: normalizeAuthScope(filters.auth_scope),
    page_types: ensureArray(filters.page_types)
  };
}

function buildScopedFilter({ filters, alias, dateColumn, useAuthenticatedColumn = false }) {
  const clauses = [];
  const params = [];

  if (filters.from_date) {
    clauses.push(`DATE(${alias}.${dateColumn}) >= ?`);
    params.push(filters.from_date);
  }

  if (filters.to_date) {
    clauses.push(`DATE(${alias}.${dateColumn}) <= ?`);
    params.push(filters.to_date);
  }

  if (filters.auth_scope === 'authenticated') {
    clauses.push(useAuthenticatedColumn ? `${alias}.is_authenticated = 1` : `${alias}.user_id IS NOT NULL`);
  } else if (filters.auth_scope === 'anonymous') {
    clauses.push(useAuthenticatedColumn ? `${alias}.is_authenticated = 0` : `${alias}.user_id IS NULL`);
  }

  if (filters.page_types.length > 0) {
    clauses.push(`${alias}.page_type IN (${filters.page_types.map(() => '?').join(',')})`);
    params.push(...filters.page_types);
  }

  return {
    whereClause: clauses.length ? `WHERE ${clauses.join(' AND ')}` : '',
    params
  };
}

function getPageTypeLabel(key, language) {
  return PAGE_TYPE_LABELS[key]?.[language] || key;
}

function getActionLabel(key, language) {
  return ACTION_LABELS[key]?.[language] || key.replace(/_/g, ' ');
}

function getCatalogLabel(catalog, key, language) {
  const normalizedKey = key || 'unknown';
  return catalog[normalizedKey]?.[language] || normalizedKey;
}

function buildLocationLabel(row, language) {
  const segments = [row.city_name, row.region_name, row.country_name]
    .map((value) => (value === null || value === undefined ? '' : String(value).trim()))
    .filter((value) => value !== '');

  if (!segments.length) {
    return language === 'es' ? 'Ubicacion sin resolver' : 'Unresolved location';
  }

  return segments.join(', ');
}

function roundMetric(value) {
  return Number(Number(value || 0).toFixed(2));
}

function getTrackedDurationExpression(alias = 's') {
  return `
    COALESCE(
      NULLIF(${alias}.duration_seconds, 0),
      CASE
        WHEN ${alias}.ended_at IS NOT NULL THEN TIMESTAMPDIFF(SECOND, ${alias}.started_at, ${alias}.ended_at)
        ELSE TIMESTAMPDIFF(SECOND, ${alias}.started_at, COALESCE(${alias}.updated_at, ${alias}.started_at))
      END,
      0
    )
  `;
}

function getTrackedActiveDurationExpression(alias = 's') {
  return `
    COALESCE(
      NULLIF(${alias}.active_duration_seconds, 0),
      NULLIF(${alias}.duration_seconds, 0),
      CASE
        WHEN ${alias}.ended_at IS NOT NULL THEN TIMESTAMPDIFF(SECOND, ${alias}.started_at, ${alias}.ended_at)
        ELSE TIMESTAMPDIFF(SECOND, ${alias}.started_at, COALESCE(${alias}.updated_at, ${alias}.started_at))
      END,
      0
    )
  `;
}

async function fetchInteractionSummary(connection, language = 'en', rawFilters = {}) {
  const filters = normalizeInteractionMetricFilters(rawFilters);
  const trackedDurationExpression = getTrackedDurationExpression('s');
  const trackedActiveDurationExpression = getTrackedActiveDurationExpression('s');
  const sessionScope = buildScopedFilter({
    filters,
    alias: 's',
    dateColumn: 'started_at',
    useAuthenticatedColumn: true
  });
  const eventScope = buildScopedFilter({
    filters,
    alias: 'e',
    dateColumn: 'occurred_at'
  });

  const [[summaryRow]] = await connection.query(
    `
      SELECT
        COUNT(*) AS total_views,
        COUNT(DISTINCT s.visitor_id) AS unique_visitors,
        COUNT(DISTINCT CASE WHEN s.user_id IS NOT NULL THEN s.user_id END) AS unique_logged_users,
        SUM(CASE WHEN s.is_authenticated = 1 THEN 1 ELSE 0 END) AS logged_in_views,
        SUM(CASE WHEN s.is_authenticated = 0 THEN 1 ELSE 0 END) AS anonymous_views,
        AVG(${trackedDurationExpression}) AS avg_duration_seconds,
        AVG(${trackedActiveDurationExpression}) AS avg_active_duration_seconds,
        SUM(${trackedActiveDurationExpression}) AS total_active_duration_seconds
      FROM interaction_sessions s
      ${sessionScope.whereClause}
    `,
    sessionScope.params
  );

  const [viewsByDayRows] = await connection.query(
    `
      SELECT
        DATE_FORMAT(s.started_at, '%Y-%m-%d') AS metric_date,
        COUNT(*) AS total
      FROM interaction_sessions s
      ${sessionScope.whereClause}
      GROUP BY DATE(s.started_at)
      ORDER BY DATE(s.started_at) ASC
    `,
    sessionScope.params
  );

  const [actionsByDayRows] = await connection.query(
    `
      SELECT
        DATE_FORMAT(e.occurred_at, '%Y-%m-%d') AS metric_date,
        COUNT(*) AS total
      FROM interaction_events e
      ${eventScope.whereClause}
      GROUP BY DATE(e.occurred_at)
      ORDER BY DATE(e.occurred_at) ASC
    `,
    eventScope.params
  );

  const [[actionRow]] = await connection.query(
    `
      SELECT COUNT(*) AS total_actions
      FROM interaction_events e
      ${eventScope.whereClause}
    `,
    eventScope.params
  );

  const [pageTypeRows] = await connection.query(
    `
      SELECT
        s.page_type,
        s.route_group,
        COUNT(*) AS total_views,
        AVG(${trackedActiveDurationExpression}) AS avg_active_duration_seconds
      FROM interaction_sessions s
      ${sessionScope.whereClause}
      GROUP BY s.page_type, s.route_group
      ORDER BY total_views DESC
    `,
    sessionScope.params
  );

  const timelineMap = new Map();
  const avgActiveDurationByRoute = pageTypeRows.length
    ? pageTypeRows.reduce((sum, row) => sum + Number(row.avg_active_duration_seconds || 0), 0) / pageTypeRows.length
    : 0;

  viewsByDayRows.forEach((row) => {
    timelineMap.set(row.metric_date, {
      views: Number(row.total),
      actions: 0
    });
  });

  actionsByDayRows.forEach((row) => {
    const current = timelineMap.get(row.metric_date) || { views: 0, actions: 0 };
    current.actions = Number(row.total);
    timelineMap.set(row.metric_date, current);
  });

  const timelineCategories = Array.from(timelineMap.keys()).sort((a, b) => a.localeCompare(b));

  return {
    totalViews: Number(summaryRow?.total_views || 0),
    uniqueVisitors: Number(summaryRow?.unique_visitors || 0),
    uniqueLoggedUsers: Number(summaryRow?.unique_logged_users || 0),
    loggedInViews: Number(summaryRow?.logged_in_views || 0),
    anonymousViews: Number(summaryRow?.anonymous_views || 0),
    avgDurationSeconds: roundMetric(summaryRow?.avg_duration_seconds),
    avgActiveDurationSeconds: roundMetric(avgActiveDurationByRoute),
    totalActiveDurationSeconds: roundMetric(summaryRow?.total_active_duration_seconds),
    totalActions: Number(actionRow?.total_actions || 0),
    timeline: {
      categories: timelineCategories,
      views: timelineCategories.map((key) => timelineMap.get(key)?.views || 0),
      actions: timelineCategories.map((key) => timelineMap.get(key)?.actions || 0)
    },
    pageTypes: pageTypeRows.map((row) => ({
      key: row.page_type,
      label: getPageTypeLabel(row.page_type, language),
      routeGroup: row.route_group,
      totalViews: Number(row.total_views || 0),
      avgActiveDurationSeconds: roundMetric(row.avg_active_duration_seconds)
    }))
  };
}

async function fetchInteractionRoutes(connection, language = 'en', rawFilters = {}) {
  const filters = normalizeInteractionMetricFilters(rawFilters);
  const trackedActiveDurationExpression = getTrackedActiveDurationExpression('s');
  const scope = buildScopedFilter({
    filters,
    alias: 's',
    dateColumn: 'started_at',
    useAuthenticatedColumn: true
  });

  const [routeRows] = await connection.query(
    `
      SELECT
        s.route_group,
        s.page_type,
        COUNT(*) AS total_views,
        COUNT(DISTINCT s.visitor_id) AS unique_visitors,
        AVG(${trackedActiveDurationExpression}) AS avg_active_duration_seconds
      FROM interaction_sessions s
      ${scope.whereClause}
      GROUP BY s.route_group, s.page_type
      ORDER BY total_views DESC
      LIMIT 10
    `,
    scope.params
  );

  const [durationRows] = await connection.query(
    `
      SELECT
        s.route_group,
        s.page_type,
        COUNT(*) AS total_views,
        AVG(${trackedActiveDurationExpression}) AS avg_active_duration_seconds
      FROM interaction_sessions s
      ${scope.whereClause}
      GROUP BY s.route_group, s.page_type
      HAVING COUNT(*) >= 1
      ORDER BY avg_active_duration_seconds DESC, total_views DESC
      LIMIT 10
    `,
    scope.params
  );

  return {
    topRoutes: routeRows.map((row) => ({
      routeGroup: row.route_group,
      pageType: row.page_type,
      label: getPageTypeLabel(row.page_type, language),
      totalViews: Number(row.total_views || 0),
      uniqueVisitors: Number(row.unique_visitors || 0),
      avgActiveDurationSeconds: roundMetric(row.avg_active_duration_seconds)
    })),
    topDurations: durationRows.map((row) => ({
      routeGroup: row.route_group,
      pageType: row.page_type,
      label: getPageTypeLabel(row.page_type, language),
      totalViews: Number(row.total_views || 0),
      avgActiveDurationSeconds: roundMetric(row.avg_active_duration_seconds)
    }))
  };
}

async function fetchInteractionContent(connection, language = 'en', rawFilters = {}) {
  const filters = normalizeInteractionMetricFilters(rawFilters);
  const trackedActiveDurationExpression = getTrackedActiveDurationExpression('s');
  const scope = buildScopedFilter({
    filters,
    alias: 's',
    dateColumn: 'started_at',
    useAuthenticatedColumn: true
  });

  const [articleRows] = await connection.query(
    `
      SELECT
        s.entity_id,
        ${language === 'es' ? 'a.title_es' : 'a.title_en'} AS label,
        COUNT(*) AS total_views,
        COUNT(DISTINCT s.visitor_id) AS unique_visitors,
        AVG(${trackedActiveDurationExpression}) AS avg_active_duration_seconds
      FROM interaction_sessions s
      INNER JOIN article a ON a.id = s.entity_id
      ${scope.whereClause ? `${scope.whereClause} AND s.entity_type = 'article'` : `WHERE s.entity_type = 'article'`}
      GROUP BY s.entity_id, label
      ORDER BY total_views DESC
      LIMIT 10
    `,
    scope.params
  );

  const [resourceRows] = await connection.query(
    `
      SELECT
        s.entity_id,
        ${language === 'es' ? 'tr.title_spanish' : 'tr.title_english'} AS label,
        COUNT(*) AS total_views,
        COUNT(DISTINCT s.visitor_id) AS unique_visitors,
        AVG(${trackedActiveDurationExpression}) AS avg_active_duration_seconds
      FROM interaction_sessions s
      INNER JOIN trusted_resources tr ON tr.id = s.entity_id
      ${scope.whereClause ? `${scope.whereClause} AND s.entity_type = 'trusted_resource'` : `WHERE s.entity_type = 'trusted_resource'`}
      GROUP BY s.entity_id, label
      ORDER BY total_views DESC
      LIMIT 10
    `,
    scope.params
  );

  const [eventRows] = await connection.query(
    `
      SELECT
        s.entity_id,
        CONCAT(
          l.community_city,
          ' - ',
          DATE_FORMAT(ce.date, '${language === 'es' ? '%d/%m/%Y' : '%m/%d/%Y'}')
        ) AS label,
        COUNT(*) AS total_views,
        COUNT(DISTINCT s.visitor_id) AS unique_visitors,
        AVG(${trackedActiveDurationExpression}) AS avg_active_duration_seconds
      FROM interaction_sessions s
      INNER JOIN calendar_event ce ON ce.id = s.entity_id
      INNER JOIN location l ON l.id = ce.location_id
      ${scope.whereClause ? `${scope.whereClause} AND s.entity_type = 'calendar_event'` : `WHERE s.entity_type = 'calendar_event'`}
      GROUP BY s.entity_id, label
      ORDER BY total_views DESC
      LIMIT 10
    `,
    scope.params
  );

  const mapRows = (rows, entityType) => rows.map((row) => ({
    entityType,
    entityId: Number(row.entity_id),
    label: row.label,
    totalViews: Number(row.total_views || 0),
    uniqueVisitors: Number(row.unique_visitors || 0),
    avgActiveDurationSeconds: roundMetric(row.avg_active_duration_seconds)
  }));

  return {
    articles: mapRows(articleRows, 'article'),
    trustedResources: mapRows(resourceRows, 'trusted_resource'),
    events: mapRows(eventRows, 'calendar_event')
  };
}

async function fetchInteractionUsers(connection, rawFilters = {}) {
  const filters = normalizeInteractionMetricFilters(rawFilters);
  const trackedActiveDurationExpression = getTrackedActiveDurationExpression('s');
  const sessionScope = buildScopedFilter({
    filters,
    alias: 's',
    dateColumn: 'started_at',
    useAuthenticatedColumn: true
  });
  const eventScope = buildScopedFilter({
    filters,
    alias: 'e',
    dateColumn: 'occurred_at'
  });

  const [rows] = await connection.query(
    `
      SELECT
        u.id AS user_id,
        CONCAT_WS(' ', u.firstname, u.lastname) AS label,
        u.email,
        r.name AS role_name,
        COUNT(DISTINCT s.id) AS total_views,
        COUNT(DISTINCT s.route_group) AS unique_routes,
        SUM(${trackedActiveDurationExpression}) AS total_active_duration_seconds,
        COALESCE(actions.total_actions, 0) AS total_actions
      FROM user u
      INNER JOIN interaction_sessions s
        ON s.user_id = u.id
      LEFT JOIN role r
        ON r.id = u.role_id
      LEFT JOIN (
        SELECT
          e.user_id,
          COUNT(*) AS total_actions
        FROM interaction_events e
        ${eventScope.whereClause ? `${eventScope.whereClause} AND e.user_id IS NOT NULL` : 'WHERE e.user_id IS NOT NULL'}
        GROUP BY e.user_id
      ) actions
        ON actions.user_id = u.id
      ${sessionScope.whereClause ? sessionScope.whereClause : ''}
      GROUP BY u.id, label, u.email, r.name, actions.total_actions
      ORDER BY (COUNT(DISTINCT s.id) + COALESCE(actions.total_actions, 0)) DESC,
               total_active_duration_seconds DESC
      LIMIT 10
    `,
    [...eventScope.params, ...sessionScope.params]
  );

  return rows.map((row) => {
    const totalViews = Number(row.total_views || 0);
    const totalActions = Number(row.total_actions || 0);
    return {
      userId: Number(row.user_id),
      label: row.label || row.email || `#${row.user_id}`,
      email: row.email,
      roleName: row.role_name || '',
      totalViews,
      totalActions,
      uniqueRoutes: Number(row.unique_routes || 0),
      totalActiveDurationSeconds: roundMetric(row.total_active_duration_seconds),
      interactionCount: totalViews + totalActions
    };
  });
}

async function fetchInteractionActions(connection, language = 'en', rawFilters = {}) {
  const filters = normalizeInteractionMetricFilters(rawFilters);
  const scope = buildScopedFilter({
    filters,
    alias: 'e',
    dateColumn: 'occurred_at'
  });

  const [actionRows] = await connection.query(
    `
      SELECT
        e.event_name,
        COUNT(*) AS total_actions
      FROM interaction_events e
      ${scope.whereClause}
      GROUP BY e.event_name
      ORDER BY total_actions DESC
      LIMIT 10
    `,
    scope.params
  );

  const [articleRows] = await connection.query(
    `
      SELECT
        e.entity_id,
        ${language === 'es' ? 'a.title_es' : 'a.title_en'} AS label,
        COUNT(*) AS total_actions
      FROM interaction_events e
      INNER JOIN article a ON a.id = e.entity_id
      ${scope.whereClause ? `${scope.whereClause} AND e.entity_type = 'article'` : `WHERE e.entity_type = 'article'`}
      GROUP BY e.entity_id, label
      ORDER BY total_actions DESC
      LIMIT 5
    `,
    scope.params
  );

  const [resourceRows] = await connection.query(
    `
      SELECT
        e.entity_id,
        ${language === 'es' ? 'tr.title_spanish' : 'tr.title_english'} AS label,
        COUNT(*) AS total_actions
      FROM interaction_events e
      INNER JOIN trusted_resources tr ON tr.id = e.entity_id
      ${scope.whereClause ? `${scope.whereClause} AND e.entity_type = 'trusted_resource'` : `WHERE e.entity_type = 'trusted_resource'`}
      GROUP BY e.entity_id, label
      ORDER BY total_actions DESC
      LIMIT 5
    `,
    scope.params
  );

  const [eventRows] = await connection.query(
    `
      SELECT
        e.entity_id,
        CONCAT(
          l.community_city,
          ' - ',
          DATE_FORMAT(ce.date, '${language === 'es' ? '%d/%m/%Y' : '%m/%d/%Y'}')
        ) AS label,
        COUNT(*) AS total_actions
      FROM interaction_events e
      INNER JOIN calendar_event ce ON ce.id = e.entity_id
      INNER JOIN location l ON l.id = ce.location_id
      ${scope.whereClause ? `${scope.whereClause} AND e.entity_type = 'calendar_event'` : `WHERE e.entity_type = 'calendar_event'`}
      GROUP BY e.entity_id, label
      ORDER BY total_actions DESC
      LIMIT 5
    `,
    scope.params
  );

  const topEntities = [
    ...articleRows.map((row) => ({
      entityType: 'article',
      entityId: Number(row.entity_id),
      label: row.label,
      totalActions: Number(row.total_actions || 0)
    })),
    ...resourceRows.map((row) => ({
      entityType: 'trusted_resource',
      entityId: Number(row.entity_id),
      label: row.label,
      totalActions: Number(row.total_actions || 0)
    })),
    ...eventRows.map((row) => ({
      entityType: 'calendar_event',
      entityId: Number(row.entity_id),
      label: row.label,
      totalActions: Number(row.total_actions || 0)
    }))
  ]
    .sort((a, b) => b.totalActions - a.totalActions)
    .slice(0, 10);

  return {
    breakdown: actionRows.map((row) => ({
      key: row.event_name,
      label: getActionLabel(row.event_name, language),
      totalActions: Number(row.total_actions || 0)
    })),
    topEntities
  };
}

async function fetchInteractionAudience(connection, language = 'en', rawFilters = {}) {
  const filters = normalizeInteractionMetricFilters(rawFilters);
  const trackedActiveDurationExpression = getTrackedActiveDurationExpression('s');
  const sessionScope = buildScopedFilter({
    filters,
    alias: 's',
    dateColumn: 'started_at',
    useAuthenticatedColumn: true
  });
  const eventScope = buildScopedFilter({
    filters,
    alias: 'e',
    dateColumn: 'occurred_at'
  });

  const eventJoin = `
    LEFT JOIN (
      SELECT
        e.session_id,
        COUNT(*) AS total_actions
      FROM interaction_events e
      ${eventScope.whereClause}
      GROUP BY e.session_id
    ) session_actions
      ON session_actions.session_id = s.id
  `;
  const combinedParams = [...eventScope.params, ...sessionScope.params];
  const geoWhereClause = sessionScope.whereClause
    ? `${sessionScope.whereClause} AND s.ip_latitude IS NOT NULL AND s.ip_longitude IS NOT NULL`
    : 'WHERE s.ip_latitude IS NOT NULL AND s.ip_longitude IS NOT NULL';

  const [accessChannelRows] = await connection.query(
    `
      SELECT
        COALESCE(NULLIF(s.access_channel, ''), 'unknown') AS metric_key,
        COUNT(*) AS total_views,
        COUNT(DISTINCT s.visitor_id) AS unique_visitors,
        AVG(${trackedActiveDurationExpression}) AS avg_active_duration_seconds,
        COALESCE(SUM(session_actions.total_actions), 0) AS total_actions
      FROM interaction_sessions s
      ${eventJoin}
      ${sessionScope.whereClause}
      GROUP BY metric_key
      ORDER BY total_views DESC, unique_visitors DESC
    `,
    combinedParams
  );

  const [operatingSystemRows] = await connection.query(
    `
      SELECT
        COALESCE(NULLIF(s.operating_system, ''), 'unknown') AS metric_key,
        COUNT(*) AS total_views,
        COUNT(DISTINCT s.visitor_id) AS unique_visitors,
        AVG(${trackedActiveDurationExpression}) AS avg_active_duration_seconds,
        COALESCE(SUM(session_actions.total_actions), 0) AS total_actions
      FROM interaction_sessions s
      ${eventJoin}
      ${sessionScope.whereClause}
      GROUP BY metric_key
      ORDER BY total_views DESC, unique_visitors DESC
    `,
    combinedParams
  );

  const [deviceCategoryRows] = await connection.query(
    `
      SELECT
        COALESCE(NULLIF(s.device_category, ''), 'unknown') AS metric_key,
        COUNT(*) AS total_views,
        COUNT(DISTINCT s.visitor_id) AS unique_visitors,
        AVG(${trackedActiveDurationExpression}) AS avg_active_duration_seconds,
        COALESCE(SUM(session_actions.total_actions), 0) AS total_actions
      FROM interaction_sessions s
      ${eventJoin}
      ${sessionScope.whereClause}
      GROUP BY metric_key
      ORDER BY total_views DESC, unique_visitors DESC
    `,
    combinedParams
  );

  const [heatmapRows] = await connection.query(
    `
      SELECT
        ROUND(s.ip_latitude, 4) AS latitude,
        ROUND(s.ip_longitude, 4) AS longitude,
        MAX(NULLIF(s.ip_city_name, '')) AS city_name,
        MAX(NULLIF(s.ip_region_name, '')) AS region_name,
        MAX(NULLIF(s.ip_country_name, '')) AS country_name,
        COUNT(*) AS total_views,
        COUNT(DISTINCT s.visitor_id) AS unique_visitors,
        COALESCE(SUM(session_actions.total_actions), 0) AS total_actions
      FROM interaction_sessions s
      ${eventJoin}
      ${geoWhereClause}
      GROUP BY ROUND(s.ip_latitude, 4), ROUND(s.ip_longitude, 4)
      ORDER BY (COUNT(*) + COALESCE(SUM(session_actions.total_actions), 0)) DESC,
               COUNT(DISTINCT s.visitor_id) DESC
      LIMIT 200
    `,
    combinedParams
  );

  const [[geolocatedSummaryRow]] = await connection.query(
    `
      SELECT
        COUNT(*) AS geolocated_views,
        COUNT(DISTINCT s.visitor_id) AS geolocated_unique_visitors
      FROM interaction_sessions s
      ${geoWhereClause}
    `,
    sessionScope.params
  );

  const mapDimensionRows = (rows, catalog) => rows.map((row) => ({
    key: row.metric_key || 'unknown',
    label: getCatalogLabel(catalog, row.metric_key, language),
    totalViews: Number(row.total_views || 0),
    uniqueVisitors: Number(row.unique_visitors || 0),
    totalActions: Number(row.total_actions || 0),
    avgActiveDurationSeconds: roundMetric(row.avg_active_duration_seconds)
  }));

  const heatmapPoints = heatmapRows.map((row) => {
    const totalViews = Number(row.total_views || 0);
    const totalActions = Number(row.total_actions || 0);

    return {
      latitude: Number(row.latitude),
      longitude: Number(row.longitude),
      cityName: row.city_name || null,
      regionName: row.region_name || null,
      countryName: row.country_name || null,
      label: buildLocationLabel(row, language),
      totalViews,
      uniqueVisitors: Number(row.unique_visitors || 0),
      totalActions,
      interactionWeight: totalViews + totalActions
    };
  });

  return {
    accessChannels: mapDimensionRows(accessChannelRows, ACCESS_CHANNEL_LABELS),
    operatingSystems: mapDimensionRows(operatingSystemRows, OPERATING_SYSTEM_LABELS),
    deviceCategories: mapDimensionRows(deviceCategoryRows, DEVICE_CATEGORY_LABELS),
    geolocatedViews: Number(geolocatedSummaryRow?.geolocated_views || 0),
    geolocatedUniqueVisitors: Number(geolocatedSummaryRow?.geolocated_unique_visitors || 0),
    geolocatedInteractions: heatmapPoints.reduce((sum, point) => sum + point.interactionWeight, 0),
    heatmapPoints,
    topLocations: heatmapPoints.slice(0, 10)
  };
}

module.exports = {
  normalizeInteractionMetricFilters,
  fetchInteractionSummary,
  fetchInteractionRoutes,
  fetchInteractionContent,
  fetchInteractionUsers,
  fetchInteractionActions,
  fetchInteractionAudience
};
