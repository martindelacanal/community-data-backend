const test = require('node:test');
const assert = require('node:assert');

const {
  buildParticipantEngagementSql,
  describeVisitScope,
  engagementNeedsAppJoin,
  engagementNeedsJoins,
  engagementNeedsRecurrenceJoin,
  normalizeEngagementFilters
} = require('./participantEngagement');

test('normalize: sin sedes el modo cae a registered', () => {
  const engagement = normalizeEngagementFilters({ location_match: 'attended' });
  assert.strictEqual(engagement.locationMatch, 'registered');
  assert.deepStrictEqual(engagement.locations, []);
});

test('normalize: descarta ids de sede no numéricos y deduplica', () => {
  const engagement = normalizeEngagementFilters({
    locations: [38, '38', 'client_IEHP', 0, -2, 12.7, 41]
  });
  assert.deepStrictEqual(engagement.locations, [38, 41]);
});

test('normalize: un modo inventado no habilita el filtro por asistencia', () => {
  const engagement = normalizeEngagementFilters({ locations: [38], location_match: 'todos' });
  assert.strictEqual(engagement.locationMatch, 'registered');
});

test('normalize: fechas y visitas invertidas se ordenan solas', () => {
  const engagement = normalizeEngagementFilters({
    visits_from_date: '2026-09-01',
    visits_to_date: '2026-03-01',
    min_visits: 20,
    max_visits: 5
  });
  assert.strictEqual(engagement.visitsFrom, '2026-03-01');
  assert.strictEqual(engagement.visitsTo, '2026-09-01');
  assert.strictEqual(engagement.minVisits, 5);
  assert.strictEqual(engagement.maxVisits, 20);
});

test('normalize: acepta el ISO del datepicker sin correr un día', () => {
  const engagement = normalizeEngagementFilters({ visits_from_date: '2026-03-01T00:00:00.000Z' });
  assert.strictEqual(engagement.visitsFrom, '2026-03-01');
});

test('normalize: app_usage desconocido cae a all', () => {
  assert.strictEqual(normalizeEngagementFilters({ app_usage: 'ios' }).appUsage, 'all');
  assert.strictEqual(normalizeEngagementFilters({ app_usage: 'no_app' }).appUsage, 'no_app');
});

test('needsJoins: el modo registered no necesita las tablas derivadas', () => {
  assert.strictEqual(engagementNeedsJoins(normalizeEngagementFilters({ locations: [38] })), false);
  assert.strictEqual(
    engagementNeedsJoins(normalizeEngagementFilters({ locations: [38], location_match: 'attended' })),
    true
  );
  assert.strictEqual(engagementNeedsJoins(normalizeEngagementFilters({ min_visits: 5 })), true);
  assert.strictEqual(engagementNeedsJoins(normalizeEngagementFilters({ app_usage: 'app' })), true);
});

test('sql: el filtro por sede de registro sobrevive aunque no haya JOIN', () => {
  // Regresión: si las condiciones se omitieran junto con los JOIN, un filtro
  // por sede en modo registered dejaría pasar a los 60k participantes.
  const engagement = normalizeEngagementFilters({ locations: [38] });
  const sql = buildParticipantEngagementSql(engagement, { withColumns: false });
  assert.strictEqual(sql.joins, '');
  assert.match(sql.conditions, /u\.first_location_id IN \(38\)/);
});

test('sql: attended no matchea por sede de registro', () => {
  const engagement = normalizeEngagementFilters({ locations: [38], location_match: 'attended' });
  const sql = buildParticipantEngagementSql(engagement, { withColumns: false });
  assert.match(sql.conditions, /rec\.visit_days > 0/);
  assert.doesNotMatch(sql.conditions, /first_location_id/);
});

test('sql: any acepta registro o asistencia', () => {
  const engagement = normalizeEngagementFilters({ locations: [38], location_match: 'any' });
  const sql = buildParticipantEngagementSql(engagement, { withColumns: false });
  assert.match(sql.conditions, /u\.first_location_id IN \(38\)/);
  assert.match(sql.conditions, /rec\.visit_days > 0/);
});

test('sql: la recurrencia se acota a las sedes filtradas', () => {
  const engagement = normalizeEngagementFilters({ locations: [38, 41], location_match: 'attended' });
  const sql = buildParticipantEngagementSql(engagement);
  assert.match(sql.joins, /db\.location_id IN \(38,41\)/);
});

test('sql: min/max visitas viajan como parámetros, no interpolados', () => {
  const engagement = normalizeEngagementFilters({ min_visits: 8, max_visits: 30 });
  const sql = buildParticipantEngagementSql(engagement, { withColumns: false });
  assert.deepStrictEqual(sql.conditionParams, [8, 30]);
  assert.match(sql.conditions, /COALESCE\(rec\.visit_days, 0\) >= \?/);
});

test('sql: la ventana de visitas viaja como parámetro y no envuelve la columna', () => {
  const engagement = normalizeEngagementFilters({
    visits_from_date: '2026-03-01',
    visits_to_date: '2026-09-01'
  });
  const sql = buildParticipantEngagementSql(engagement);
  assert.deepStrictEqual(sql.joinParams, ['2026-03-01', '2026-09-01']);
  // El índice idx_delivery_metrics_created_location_user solo sirve si la
  // columna queda sin CONVERT_TZ en el WHERE.
  assert.match(sql.joins, /db\.creation_date >= CONVERT_TZ\(CONCAT\(\?/);
  assert.doesNotMatch(sql.joins, /WHERE[\s\S]*CONVERT_TZ\(db\.creation_date/);
});

test('sql: forceJoins agrega los JOIN para ordenar aunque no haya filtros', () => {
  const engagement = normalizeEngagementFilters({});
  assert.strictEqual(buildParticipantEngagementSql(engagement, { withColumns: false }).joins, '');
  assert.notStrictEqual(
    buildParticipantEngagementSql(engagement, { withColumns: false, forceJoins: true }).joins,
    ''
  );
});

test('sql: aggregated envuelve las columnas para consultas con GROUP BY', () => {
  const engagement = normalizeEngagementFilters({});
  const plain = buildParticipantEngagementSql(engagement, { withColumns: true });
  const grouped = buildParticipantEngagementSql(engagement, { withColumns: true, aggregated: true });
  assert.match(plain.selects, /COALESCE\(rec\.visit_days, 0\) AS visit_days/);
  assert.match(grouped.selects, /MAX\(COALESCE\(rec\.visit_days, 0\)\) AS visit_days/);
});

test('sql: el centinela de last_app_seen se castea a DATETIME', () => {
  // Sin el CAST, DATE_FORMAT sobre el string devuelve '10/00/2000' en lugar de NULL.
  const sql = buildParticipantEngagementSql(normalizeEngagementFilters({}));
  assert.match(sql.selects, /CAST\('1000-01-01' AS DATETIME\)/);
});

test('sql: app_usage no filtra por defecto', () => {
  const sql = buildParticipantEngagementSql(normalizeEngagementFilters({}), { withColumns: false });
  assert.strictEqual(sql.conditions, '');
  assert.deepStrictEqual(sql.conditionParams, []);
});

test('scope de cliente: la recurrencia se limita a las sedes del cliente', () => {
  const engagement = normalizeEngagementFilters({}, { clientId: 7 });
  assert.strictEqual(engagement.clientId, 7);
  const sql = buildParticipantEngagementSql(engagement);
  assert.match(sql.joins, /db\.location_id IN \(SELECT cl\.location_id FROM client_location AS cl WHERE cl\.client_id = \?\)/);
  assert.deepStrictEqual(sql.joinParams, [7]);
});

test('scope de cliente: sin cliente no se agrega la subconsulta', () => {
  const sql = buildParticipantEngagementSql(normalizeEngagementFilters({}));
  assert.doesNotMatch(sql.joins, /client_location/);
  assert.deepStrictEqual(sql.joinParams, []);
});

test('scope de cliente: el id de cliente va antes que la ventana de visitas', () => {
  // El orden de joinParams tiene que seguir el orden de los ? en el SQL.
  const engagement = normalizeEngagementFilters(
    { visits_from_date: '2026-03-01', visits_to_date: '2026-09-01' },
    { clientId: 7 }
  );
  const sql = buildParticipantEngagementSql(engagement);
  assert.deepStrictEqual(sql.joinParams, [7, '2026-03-01', '2026-09-01']);
});

test('joins: filtrar por app no arrastra el agregado de recurrencia', () => {
  const engagement = normalizeEngagementFilters({ app_usage: 'app' });
  assert.strictEqual(engagementNeedsRecurrenceJoin(engagement), false);
  assert.strictEqual(engagementNeedsAppJoin(engagement), true);
  const sql = buildParticipantEngagementSql(engagement, { withColumns: false });
  assert.doesNotMatch(sql.joins, /delivery_beneficiary/);
  assert.match(sql.joins, /push_device_tokens/);
});

test('joins: filtrar por visitas no arrastra las señales de app', () => {
  const engagement = normalizeEngagementFilters({ min_visits: 5 });
  const sql = buildParticipantEngagementSql(engagement, { withColumns: false });
  assert.match(sql.joins, /delivery_beneficiary/);
  assert.doesNotMatch(sql.joins, /push_device_tokens/);
});

test('app: dos plataformas en push_device_tokens se reportan como both', () => {
  const sql = buildParticipantEngagementSql(normalizeEngagementFilters({}));
  assert.match(sql.joins, /IF\(MIN\(d\.platform\) = MAX\(d\.platform\), MIN\(d\.platform\), 'both'\) AS platform/);
});

test('app: last_app_seen se convierte a la zona horaria de las sedes', () => {
  const sql = buildParticipantEngagementSql(normalizeEngagementFilters({}));
  assert.match(sql.selects, /CONVERT_TZ\(NULLIF\(GREATEST\([\s\S]*?America\/Los_Angeles'\), '%m\/%d\/%Y'\) AS app_last_seen/);
});

test('visited_locations: se corta a 10 sedes en vez de truncar a mitad de nombre', () => {
  const sql = buildParticipantEngagementSql(normalizeEngagementFilters({}), { withVisitedLocations: true });
  assert.match(sql.joins, /SUBSTRING_INDEX\([\s\S]*?' \| ', 10\s*\) AS visited_locations/);
});

test('describeVisitScope: usa nombres de sede cuando los hay', () => {
  const engagement = normalizeEngagementFilters({
    locations: [38],
    visits_from_date: '2026-03-01'
  });
  assert.strictEqual(
    describeVisitScope(engagement, ['San Bernardino: Sierra High School']),
    'San Bernardino: Sierra High School (2026-03-01 to today)'
  );
  assert.strictEqual(describeVisitScope(normalizeEngagementFilters({}), []), 'All locations (start to today)');
});
