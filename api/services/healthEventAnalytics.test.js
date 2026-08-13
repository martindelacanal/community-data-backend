'use strict';

/**
 * Unit tests for the health-event analytics service.
 *
 * These exercise the pure layer (filters → aggregates → export tables) against a
 * hand-built snapshot, so they run without a database. The synthetic event mirrors
 * the real shape: an entry desk, a stand with check-out, and a resource table whose
 * scans carry a service id.
 */

const test = require('node:test');
const assert = require('node:assert/strict');

const analytics = require('./healthEventAnalytics');

const TZ = 'America/Los_Angeles';

// The scan query joins the stand and the service, so every scan row carries both
// language columns. The fixture mirrors that instead of restating them per scan.
const STANDS = {
  1: { name_en: 'Entry', name_es: 'Entrada', is_entry: 'Y', has_checkout: 'N', sort_order: 1 },
  2: { name_en: 'Dental', name_es: 'Dental', is_entry: 'N', has_checkout: 'Y', sort_order: 2 },
  3: { name_en: 'Resource Table', name_es: 'Mesa de Recursos', is_entry: 'N', has_checkout: 'N', sort_order: 3 }
};
const SERVICES = {
  10: { stand_id: 3, name_en: 'IHSS', name_es: 'IHSS', sort_order: 1 },
  11: { stand_id: 3, name_en: 'Salvation Army', name_es: 'Salvation Army', sort_order: 2 }
};

function scan(overrides) {
  const local = overrides.local;
  const stand = STANDS[overrides.stand_id];
  const service = overrides.service_id != null ? SERVICES[overrides.service_id] : null;
  return {
    scan_id: overrides.scan_id,
    scan_type: overrides.scan_type || 'checkin',
    paired_scan_id: overrides.paired_scan_id != null ? overrides.paired_scan_id : null,
    local,
    day: local.slice(0, 10),
    time: local.slice(11, 16),
    hour: Number.parseInt(local.slice(11, 13), 10),
    scanned_at_utc: local,
    stand_id: overrides.stand_id,
    stand_en: stand.name_en,
    stand_es: stand.name_es,
    is_entry: stand.is_entry === 'Y',
    has_checkout: stand.has_checkout === 'Y',
    stand_order: stand.sort_order,
    service_id: overrides.service_id != null ? overrides.service_id : null,
    service_en: service ? service.name_en : null,
    service_es: service ? service.name_es : null,
    service_order: service ? service.sort_order : null,
    registration_id: overrides.registration_id || 1,
    source: overrides.source || 'web',
    user_id: overrides.user_id,
    person_name: overrides.person_name || `User ${overrides.user_id}`,
    person_firstname: overrides.person_name || `User ${overrides.user_id}`,
    person_lastname: '',
    person_email: overrides.person_email || '',
    person_phone: overrides.person_phone || '',
    volunteer_user_id: overrides.volunteer_user_id,
    volunteer_name: overrides.volunteer_name,
    volunteer_username: overrides.volunteer_username || '',
    volunteer_email: ''
  };
}

/**
 * Two-day event. Day 1: Ana passes entry then Dental (30 min, closed).
 * Day 2: Ana returns to the resource table (IHSS); Beto does entry + an open
 * Dental visit. Carla registers for day 1 and never shows up.
 */
function buildSnapshot() {
  const stands = Object.entries(STANDS)
    .map(([id, stand]) => ({ id: Number(id), ...stand, enabled: 'Y' }));
  const services = Object.entries(SERVICES)
    .map(([id, service]) => ({ id: Number(id), ...service, enabled: 'Y' }));

  const scans = [
    scan({ scan_id: 1, local: '2026-08-08 08:00:00', stand_id: 1, user_id: 100, person_name: 'Ana', registration_id: 1, volunteer_user_id: 900, volunteer_name: 'Vol One' }),
    scan({ scan_id: 2, local: '2026-08-08 09:00:00', stand_id: 2, user_id: 100, person_name: 'Ana', registration_id: 1, volunteer_user_id: 901, volunteer_name: 'Vol Two' }),
    scan({ scan_id: 3, local: '2026-08-08 09:30:00', scan_type: 'checkout', paired_scan_id: 2, stand_id: 2, user_id: 100, person_name: 'Ana', registration_id: 1, volunteer_user_id: 901, volunteer_name: 'Vol Two' }),
    scan({ scan_id: 4, local: '2026-08-08 14:00:00', stand_id: 1, user_id: 101, person_name: 'Beto', registration_id: 2, source: 'walkin', volunteer_user_id: 900, volunteer_name: 'Vol One' }),
    // Open visit: check-in with no check-out at a stand that has check-outs.
    scan({ scan_id: 5, local: '2026-08-08 14:30:00', stand_id: 2, user_id: 101, person_name: 'Beto', registration_id: 2, source: 'walkin', volunteer_user_id: 901, volunteer_name: 'Vol Two' }),
    scan({ scan_id: 6, local: '2026-08-09 10:00:00', stand_id: 3, service_id: 10, user_id: 100, person_name: 'Ana', registration_id: 1, volunteer_user_id: 902, volunteer_name: 'Vol Three' })
  ];

  const registrations = [
    { registration_id: 1, registration_role: 'beneficiary', status: 'registered', source: 'web', contact_email: null, submitted_at_local: '2026-07-01 10:00:00', user_id: 100, firstname: 'Ana', lastname: 'Lopez', email: 'ana@example.com', username: 'ana', phone: '5550100', zipcode: '92223', household_size: 3, user_enabled: 'Y', app_language: 'es', date_of_birth: '1990-01-01', age: 36, gender: 'Female', ethnicity: 'Hispanic', other_ethnicity: null, second_ethnicity: null, preferred_language: 'Spanish', other_language: null, signed_up_days: '2026-08-08, 2026-08-09', priority_services: 'dental', appointments: 'dental 2026-08-08 09:00' },
    { registration_id: 2, registration_role: 'beneficiary', status: 'registered', source: 'walkin', contact_email: null, submitted_at_local: '2026-08-08 13:55:00', user_id: 101, firstname: 'Beto', lastname: 'Diaz', email: null, username: 'beto', phone: '5550101', zipcode: '92220', household_size: 1, user_enabled: 'Y', app_language: 'en', date_of_birth: '1980-05-05', age: 46, gender: 'Male', ethnicity: null, other_ethnicity: null, second_ethnicity: null, preferred_language: 'English', other_language: null, signed_up_days: null, priority_services: null, appointments: null },
    { registration_id: 3, registration_role: 'beneficiary', status: 'registered', source: 'web', contact_email: 'carla@example.com', submitted_at_local: '2026-07-02 10:00:00', user_id: 102, firstname: 'Carla', lastname: 'Ruiz', email: 'carla@example.com', username: 'carla', phone: '5550102', zipcode: '92223', household_size: 2, user_enabled: 'Y', app_language: 'en', date_of_birth: '2000-02-02', age: 26, gender: 'Female', ethnicity: null, other_ethnicity: null, second_ethnicity: null, preferred_language: 'English', other_language: null, signed_up_days: '2026-08-08', priority_services: null, appointments: null },
    { registration_id: 4, registration_role: 'volunteer', status: 'registered', source: 'web', contact_email: null, submitted_at_local: '2026-07-10 10:00:00', user_id: 900, firstname: 'Vol', lastname: 'One', email: 'v1@example.com', username: 'vol.one', phone: null, zipcode: null, household_size: null, user_enabled: 'Y', app_language: 'en', date_of_birth: null, age: null, gender: null, ethnicity: null, other_ethnicity: null, second_ethnicity: null, preferred_language: null, other_language: null, signed_up_days: null, priority_services: null, appointments: null },
    { registration_id: 5, registration_role: 'volunteer', status: 'registered', source: 'web', contact_email: null, submitted_at_local: '2026-07-10 10:00:00', user_id: 901, firstname: 'Vol', lastname: 'Two', email: 'v2@example.com', username: 'vol.two', phone: null, zipcode: null, household_size: null, user_enabled: 'N', app_language: 'en', date_of_birth: null, age: null, gender: null, ethnicity: null, other_ethnicity: null, second_ethnicity: null, preferred_language: null, other_language: null, signed_up_days: null, priority_services: null, appointments: null }
  ];

  const questions = [
    { id: 50, name_en: 'Do you have insurance?', name_es: '¿Tienes seguro?', question_type: 'single', sort_order: 1, audience: 'beneficiary', section_order: 1, form_en: 'Registration', form_es: 'Registro' }
  ];
  const answers = [
    { id: 1, registration_id: 1, question_id: 50, answer_text: null, answer_number: null, other_text: null, answer_date: null, options_en: 'Yes', options_es: 'Sí' },
    { id: 2, registration_id: 2, question_id: 50, answer_text: null, answer_number: null, other_text: null, answer_date: null, options_en: 'No', options_es: 'No' }
  ];

  const checkoutAnswers = [
    { scan_answer_id: 1, scan_id: 3, question_id: 60, answer_text: null, answer_number: null, question_en: 'Service status', question_es: 'Estado', question_type: 'single', sort_order: 1, stand_id: 2, options_en: 'Completed', options_es: 'Completado' },
    { scan_answer_id: 2, scan_id: 3, question_id: 61, answer_text: 'All good', answer_number: null, question_en: 'Notes', question_es: 'Notas', question_type: 'text', sort_order: 2, stand_id: 2, options_en: null, options_es: null }
  ];

  return {
    event: {
      id: 1, slug: 'test-event', name_en: 'Test Event', name_es: 'Evento de Prueba',
      timezone: TZ, start_date: '2026-08-08', end_date: '2026-08-09',
      location_name: 'Somewhere', client_name: 'Client'
    },
    scans, stands, services, registrations, questions, answers, checkoutAnswers
  };
}

const NO_FILTERS = analytics.parseAnalyticsFilters({});

function build(query = {}, lang = 'en') {
  const snapshot = buildSnapshot();
  const filters = analytics.parseAnalyticsFilters(query);
  return { snapshot, filters, result: analytics.buildAnalytics(snapshot, filters, lang) };
}

function table(key, query = {}, lang = 'en') {
  const { snapshot, filters, result } = build(query, lang);
  return analytics.buildTable(key, snapshot, result, filters, lang);
}

function rowsAsObjects(built) {
  return built.rows.map(row => {
    const object = {};
    built.headers.forEach((header, index) => { object[header] = row[index]; });
    return object;
  });
}

// ---------------------------------------------------------------------------
// Filter parsing
// ---------------------------------------------------------------------------

test('parseAnalyticsFilters normalizes ids, times and dates', () => {
  const filters = analytics.parseAnalyticsFilters({
    from: '2026-08-08', to: 'nonsense',
    time_from: '9:05', time_to: '25:00',
    stand_ids: '2, 3, x', service_ids: '10,none', volunteer_ids: '900',
    scan_type: 'bogus', sources: 'web, walkin', search: '  Ana  ', only_attendees: '1'
  });
  assert.equal(filters.from, '2026-08-08');
  assert.equal(filters.to, null, 'unparseable dates are dropped');
  assert.equal(filters.time_from, '09:05', 'times are zero-padded');
  assert.equal(filters.time_to, null, 'out-of-range times are dropped');
  assert.deepEqual(filters.stand_ids, [2, 3]);
  assert.deepEqual(filters.service_ids, [10, 'none']);
  assert.deepEqual(filters.volunteer_ids, [900]);
  assert.equal(filters.scan_type, null, 'only checkin/checkout are accepted');
  assert.deepEqual(filters.sources, ['web', 'walkin']);
  assert.equal(filters.search, 'Ana');
  assert.equal(filters.only_attendees, true);
});

test('stand_ids never keeps the "none" sentinel (only services can lack one)', () => {
  const filters = analytics.parseAnalyticsFilters({ stand_ids: 'none,2', volunteer_ids: 'none' });
  assert.deepEqual(filters.stand_ids, [2]);
  assert.deepEqual(filters.volunteer_ids, []);
});

// ---------------------------------------------------------------------------
// KPIs
// ---------------------------------------------------------------------------

test('KPIs count people once and exclude the entry desk from services', () => {
  const { result } = build();
  const k = result.kpis;
  assert.equal(k.total_scans, 6);
  assert.equal(k.checkins, 5);
  assert.equal(k.checkouts, 1);
  assert.equal(k.attended_beneficiaries, 2, 'Ana and Beto, each once');
  assert.equal(k.registered_beneficiaries, 3);
  assert.equal(k.registered_without_scans, 1, 'Carla never showed up');
  assert.equal(k.walkins, 1);
  assert.equal(k.registered_volunteers, 2);
  assert.equal(k.scanning_volunteers, 3, 'Vol Three scanned without a volunteer registration');
  // 5 check-ins minus the 2 at the entry desk.
  assert.equal(k.service_visits, 3);
  assert.equal(k.avg_service_minutes, 30);
  assert.equal(k.median_service_minutes, 30);
  assert.equal(k.open_visits, 1, "Beto's Dental visit was never closed");
  assert.equal(k.days, 2);
  // Ana: entry + dental + resource = 3 stands; Beto: entry + dental = 2.
  assert.equal(k.avg_stands_per_attendee, 2.5);
});

test('an open visit is only counted at stands that record check-outs', () => {
  // The resource table has has_checkout='N', so its check-in is not "open".
  const { result } = build({ stand_ids: '3' });
  assert.equal(result.kpis.open_visits, 0);
  assert.equal(result.kpis.service_visits, 1);
});

// ---------------------------------------------------------------------------
// Per-stand aggregates
// ---------------------------------------------------------------------------

test('by_stand splits the resource table per service and keeps stand order', () => {
  const { result } = build();
  const labels = result.by_stand.map(row => `${row.stand_name}${row.service_name ? '/' + row.service_name : ''}`);
  assert.deepEqual(labels, ['Entry', 'Dental', 'Resource Table/IHSS']);

  const dental = result.by_stand.find(row => row.stand_name === 'Dental');
  assert.equal(dental.checkins, 2);
  assert.equal(dental.checkouts, 1);
  assert.equal(dental.unique_people, 2);
  assert.equal(dental.avg_minutes, 30);
  assert.equal(dental.open_visits, 1);
  assert.equal(dental.share_of_attendees, 100, 'both attendees passed through Dental');
  assert.deepEqual(dental.per_day.map(cell => cell.checkins), [2, 0]);

  const entry = result.by_stand.find(row => row.stand_name === 'Entry');
  assert.equal(entry.avg_minutes, null, 'no check-out at the entry desk means no service time');
});

test('per_day columns line up with the reported day list', () => {
  const { result } = build();
  assert.deepEqual(result.days, ['2026-08-08', '2026-08-09']);
  for (const row of result.by_stand) {
    assert.deepEqual(row.per_day.map(cell => cell.day), result.days);
  }
  for (const row of result.by_volunteer) {
    assert.deepEqual(row.per_day.map(cell => cell.day), result.days);
  }
});

// ---------------------------------------------------------------------------
// Per-volunteer aggregates — the client's "how many people did I scan?" question
// ---------------------------------------------------------------------------

test('by_volunteer separates total scans from unique people', () => {
  const { result } = build();
  const volOne = result.by_volunteer.find(row => row.name === 'Vol One');
  assert.equal(volOne.total, 2);
  assert.equal(volOne.unique_people, 2);
  assert.equal(volOne.checkins, 2);
  assert.equal(volOne.checkouts, 0);
  assert.equal(volOne.stands, 1);

  const volTwo = result.by_volunteer.find(row => row.name === 'Vol Two');
  assert.equal(volTwo.total, 3, '2 check-ins + 1 check-out');
  assert.equal(volTwo.unique_people, 2, 'Ana counted once despite two scans');
});

test('active_minutes is summed per day, never across the overnight gap', () => {
  const { result } = build();
  // Vol Three scanned once, on day 2 only: a single scan spans zero minutes.
  const volThree = result.by_volunteer.find(row => row.name === 'Vol Three');
  assert.equal(volThree.days, 1);
  assert.equal(volThree.active_minutes, 0);

  // Vol One worked 08:00 and 14:00 on day 1 only => 360 minutes.
  const volOne = result.by_volunteer.find(row => row.name === 'Vol One');
  assert.equal(volOne.active_minutes, 360);
});

test('by_volunteer_stand attributes resource-table scans to the right partner', () => {
  const { result } = build();
  const resource = result.by_volunteer_stand.filter(row => row.stand_name === 'Resource Table');
  assert.equal(resource.length, 1);
  assert.equal(resource[0].volunteer_name, 'Vol Three');
  assert.equal(resource[0].service_name, 'IHSS');
  assert.equal(resource[0].total, 1);
  assert.equal(resource[0].unique_people, 1);
});

// ---------------------------------------------------------------------------
// Filtering
// ---------------------------------------------------------------------------

test('day filter narrows scans and keeps the full option catalogue', () => {
  const { result } = build({ from: '2026-08-09', to: '2026-08-09' });
  assert.equal(result.kpis.total_scans, 1);
  assert.equal(result.kpis.attended_beneficiaries, 1);
  assert.deepEqual(result.days, ['2026-08-09']);
  // Filter dropdowns must still offer every day/stand/volunteer of the event.
  assert.deepEqual(result.options.days, ['2026-08-08', '2026-08-09']);
  assert.equal(result.options.stands.length, 3);
  assert.equal(result.options.volunteers.length, 3);
});

test('service filter "none" selects scans recorded without a service', () => {
  const withoutService = build({ service_ids: 'none' }).result;
  assert.equal(withoutService.kpis.total_scans, 5);
  const withService = build({ service_ids: '10' }).result;
  assert.equal(withService.kpis.total_scans, 1);
});

test('time-of-day filter uses the event wall clock', () => {
  const morning = build({ time_from: '08:00', time_to: '10:00' }).result;
  assert.equal(morning.kpis.total_scans, 4, 'three day-1 morning scans + the day-2 10:00 one');
  const afternoon = build({ time_from: '13:00' }).result;
  assert.equal(afternoon.kpis.total_scans, 2);
});

test('filtering to check-ins leaves visits open instead of inventing durations', () => {
  const { result } = build({ scan_type: 'checkin' });
  assert.equal(result.kpis.checkouts, 0);
  assert.equal(result.kpis.avg_service_minutes, null);
  assert.equal(result.kpis.service_visits, 3, 'visits still exist, they are just unclosed');
});

test('volunteer filter keeps only that volunteer, both ends of a visit included', () => {
  const { result } = build({ volunteer_ids: '901' });
  assert.equal(result.kpis.scanning_volunteers, 1);
  assert.equal(result.kpis.total_scans, 3);
  // Vol Two did both ends of Ana's Dental visit, so the duration survives.
  assert.equal(result.kpis.avg_service_minutes, 30);
});

test('search matches participants and volunteers', () => {
  assert.equal(build({ search: 'ana' }).result.kpis.total_scans, 4);
  assert.equal(build({ search: 'Vol Three' }).result.kpis.total_scans, 1);
  assert.equal(build({ search: 'nobody' }).result.kpis.total_scans, 0);
});

test('source filter uses the registration source carried on the scan', () => {
  const { result } = build({ sources: 'walkin' });
  assert.equal(result.kpis.total_scans, 2);
  assert.equal(result.kpis.attended_beneficiaries, 1);
});

test('a filter combination that matches nothing degrades to empty, not to a crash', () => {
  const { result } = build({ stand_ids: '3', time_from: '23:00' });
  assert.equal(result.kpis.total_scans, 0);
  assert.equal(result.kpis.avg_service_minutes, null);
  assert.deepEqual(result.by_stand, []);
  assert.deepEqual(result.by_volunteer, []);
  assert.deepEqual(result.by_hour, []);
  assert.deepEqual(result.days, []);
});

// ---------------------------------------------------------------------------
// Attendance
// ---------------------------------------------------------------------------

test('attendance_by_day separates sign-ups, no-shows and unexpected arrivals', () => {
  const { result } = build();
  const [day1, day2] = result.attendance_by_day;

  assert.equal(day1.day, '2026-08-08');
  assert.equal(day1.signed_up, 2, 'Ana and Carla');
  assert.equal(day1.attended, 2, 'Ana and Beto');
  assert.equal(day1.signed_up_and_attended, 1, 'only Ana did both');
  assert.equal(day1.cancelled, 0);
  assert.equal(day1.no_show, 1, 'Carla');
  assert.equal(day1.walk_in_or_other_day, 1, 'Beto never signed up for a day');
  assert.equal(day1.show_rate, 50);
  assert.equal(day1.first_time_that_day, 2);

  assert.equal(day2.day, '2026-08-09');
  assert.equal(day2.attended, 1);
  assert.equal(day2.first_time_that_day, 0, 'Ana was already seen on day 1');
});

test('attendance_by_day separates cancelled-only appointments from no-shows', () => {
  const snapshot = buildSnapshot();
  snapshot.registrations.push({
    ...snapshot.registrations.find(row => row.user_id === 102),
    registration_id: 6,
    user_id: 103,
    firstname: 'Dana',
    lastname: 'Cancel',
    email: 'dana@example.com',
    username: 'dana',
    phone: '5550103',
    signed_up_days: '2026-08-08',
    booked_appointment_days: null,
    cancelled_appointment_days: '2026-08-08',
    appointments: 'dental 2026-08-08 10:00 [cancelled]'
  });

  const result = analytics.buildAnalytics(snapshot, NO_FILTERS, 'en');
  const day1 = result.attendance_by_day.find(row => row.day === '2026-08-08');

  assert.equal(day1.signed_up, 3, 'Ana, Carla and Dana are still visible in the sign-up total');
  assert.equal(day1.signed_up_and_attended, 1);
  assert.equal(day1.cancelled, 1, 'Dana is classified separately');
  assert.equal(day1.no_show, 1, 'only Carla is a no-show');
  assert.equal(day1.show_rate, 50, 'cancelled absences are removed from the show-rate denominator');

  const attendanceTable = analytics.buildTable('attendance', snapshot, result, NO_FILTERS, 'en');
  const exportedDay1 = rowsAsObjects(attendanceTable).find(row => row.Day === '2026-08-08');
  assert.equal(exportedDay1.Cancelled, 1, 'the attendance export exposes the separate bucket');
});

test('cancelling one appointment does not excuse another booked appointment that day', () => {
  const snapshot = buildSnapshot();
  snapshot.registrations.push({
    ...snapshot.registrations.find(row => row.user_id === 102),
    registration_id: 6,
    user_id: 103,
    firstname: 'Dana',
    lastname: 'Mixed',
    email: 'dana@example.com',
    username: 'dana',
    phone: '5550103',
    signed_up_days: '2026-08-08',
    booked_appointment_days: '2026-08-08',
    cancelled_appointment_days: '2026-08-08',
    appointments: 'dental 2026-08-08 10:00 [cancelled] | vision 2026-08-08 11:00 [booked]'
  });

  const result = analytics.buildAnalytics(snapshot, NO_FILTERS, 'en');
  const day1 = result.attendance_by_day.find(row => row.day === '2026-08-08');

  assert.equal(day1.cancelled, 0);
  assert.equal(day1.no_show, 2, 'Carla and Dana both retain an active no-show');
  assert.equal(day1.show_rate, 33.3);
});

// ---------------------------------------------------------------------------
// Hourly flow
// ---------------------------------------------------------------------------

test('by_hour buckets scans by event-local hour', () => {
  const { result } = build();
  const day1 = result.by_hour.filter(row => row.day === '2026-08-08');
  assert.deepEqual(day1.map(row => row.hour), [8, 9, 14]);
  const nine = day1.find(row => row.hour === 9);
  assert.equal(nine.checkins, 1);
  assert.equal(nine.checkouts, 1);
  assert.equal(nine.total, 2);
  assert.equal(nine.label, '09:00');
});

// ---------------------------------------------------------------------------
// Checkout answers
// ---------------------------------------------------------------------------

test('checkout_answers groups option answers and skips free text', () => {
  const { result } = build();
  assert.equal(result.checkout_answers.length, 1, 'the Notes text answer is not a bucket');
  assert.equal(result.checkout_answers[0].question, 'Service status');
  assert.equal(result.checkout_answers[0].value, 'Completed');
  assert.equal(result.checkout_answers[0].total, 1);
});

// ---------------------------------------------------------------------------
// Export tables
// ---------------------------------------------------------------------------

test('every dataset produces a rectangular table', () => {
  for (const key of analytics.DATASETS) {
    const built = table(key);
    assert.ok(built, `${key} must build`);
    assert.ok(built.headers.length > 0, `${key} needs headers`);
    for (const row of built.rows) {
      assert.equal(row.length, built.headers.length, `${key}: row width must match the header`);
    }
  }
});

test('participants export carries one scan column per stand and per service', () => {
  const built = table('participants');
  assert.ok(built.headers.includes('Check-ins · Entry'));
  assert.ok(built.headers.includes('Check-ins · Dental'));
  assert.ok(built.headers.includes('Check-ins · Resource Table'));
  assert.ok(built.headers.includes('Check-ins · Resource Table · IHSS'));
  // Services with no scans still get a column, so the layout is stable per event.
  assert.ok(built.headers.includes('Check-ins · Resource Table · Salvation Army'));
  // The registration form question is spread into its own column too.
  assert.ok(built.headers.includes('Do you have insurance?'));

  const rows = rowsAsObjects(built);
  const ana = rows.find(row => row['First name'] === 'Ana');
  assert.equal(ana['Attended'], 'Yes');
  assert.equal(ana['Check-ins · Dental'], 1);
  assert.equal(ana['Check-ins · Resource Table'], 1);
  assert.equal(ana['Check-ins · Resource Table · IHSS'], 1);
  assert.equal(ana['Check-ins · Resource Table · Salvation Army'], 0);
  assert.equal(ana['Stands visited'], 3);
  assert.equal(ana['Days attended'], 2);
  assert.equal(ana['Do you have insurance?'], 'Yes');

  const carla = rows.find(row => row['First name'] === 'Carla');
  assert.equal(carla['Attended'], 'No', 'no-shows are listed with zeros by default');
  assert.equal(carla['Total scans'], 0);
  assert.equal(carla['Check-ins · Dental'], 0);
});

test('only_attendees drops the no-shows from the participants export', () => {
  const all = rowsAsObjects(table('participants'));
  const attendeesOnly = rowsAsObjects(table('participants', { only_attendees: '1' }));
  assert.equal(all.length, 3);
  assert.equal(attendeesOnly.length, 2);
  assert.ok(!attendeesOnly.some(row => row['First name'] === 'Carla'));
});

test('participants scan columns follow the filters while the roster does not', () => {
  const rows = rowsAsObjects(table('participants', { from: '2026-08-09', to: '2026-08-09' }));
  assert.equal(rows.length, 3, 'the roster still lists everyone registered');
  const ana = rows.find(row => row['First name'] === 'Ana');
  assert.equal(ana['Check-ins · Dental'], 0, 'the day-1 Dental visit is outside the filter');
  assert.equal(ana['Check-ins · Resource Table · IHSS'], 1);
  const beto = rows.find(row => row['First name'] === 'Beto');
  assert.equal(beto['Attended'], 'No', 'Beto has no day-2 scans');
});

test('visits export pairs check-ins with check-outs and spreads checkout answers', () => {
  const built = table('visits');
  const rows = rowsAsObjects(built);
  assert.equal(rows.length, 5, 'one row per check-in');

  const closed = rows.find(row => row['Check-in scan ID'] === 2);
  assert.equal(closed['Stand'], 'Dental');
  assert.equal(closed['Check-in time'], '09:00');
  assert.equal(closed['Check-out time'], '09:30');
  assert.equal(closed['Minutes at the stand'], 30);
  assert.equal(closed['Closed with check-out'], 'Yes');
  assert.equal(closed['Checked in by'], 'Vol Two');
  assert.equal(closed['Service status'], 'Completed');
  assert.equal(closed['Notes'], 'All good');

  const open = rows.find(row => row['Check-in scan ID'] === 5);
  assert.equal(open['Closed with check-out'], 'No');
  assert.equal(open['Minutes at the stand'], '');
  assert.equal(open['Service status'], '');
});

test('volunteers export lists registered volunteers who never scanned, and scanners who never registered', () => {
  const rows = rowsAsObjects(table('volunteers'));
  const names = rows.map(row => row['Volunteer']);
  assert.ok(names.includes('Vol Three'), 'an admin who scanned without registering still appears');

  const volTwo = rows.find(row => row['Volunteer'] === 'Vol Two');
  assert.equal(volTwo['Total scans'], 3);
  assert.equal(volTwo['People scanned (unique)'], 2);
  assert.equal(volTwo['Account approved'], 'No');
  assert.equal(volTwo['Scans · Dental'], 3);
  assert.equal(volTwo['Scans · Resource Table · IHSS'], 0);

  const volThree = rows.find(row => row['Volunteer'] === 'Vol Three');
  assert.equal(volThree['Scans · Resource Table · IHSS'], 1);
});

test('volunteer_scans export is one row per volunteer, stand, service and day', () => {
  const rows = rowsAsObjects(table('volunteer_scans'));
  const resource = rows.find(row => row['Service / partner'] === 'IHSS');
  assert.equal(resource['Volunteer'], 'Vol Three');
  assert.equal(resource['Day'], '2026-08-09');
  assert.equal(resource['Total scans'], 1);
  assert.equal(resource['People scanned (unique)'], 1);
});

test('scan_log export keeps both the event-local and the UTC timestamp', () => {
  const built = table('scan_log');
  assert.ok(built.headers.includes('Local date & time'));
  assert.ok(built.headers.includes('UTC date & time'));
  assert.equal(built.rows.length, 6);
});

test('summary export stamps the filters it was generated with', () => {
  const rows = rowsAsObjects(table('summary', { stand_ids: '3', from: '2026-08-09' }));
  const applied = rows.find(row => row['Metric'] === 'Filters applied');
  assert.match(applied['Value'], /Resource Table/, 'stand names, not raw ids');
  assert.match(applied['Value'], /2026-08-09/);

  const unfiltered = rowsAsObjects(table('summary'));
  const none = unfiltered.find(row => row['Metric'] === 'Filters applied');
  assert.equal(none['Value'], 'None (whole event)');
});

test('an unknown dataset key builds nothing instead of throwing', () => {
  const { snapshot, filters, result } = build();
  assert.equal(analytics.buildTable('does_not_exist', snapshot, result, filters, 'en'), null);
});

// ---------------------------------------------------------------------------
// Localisation
// ---------------------------------------------------------------------------

test('Spanish output translates headers, stand names and yes/no values', () => {
  const { result } = build({}, 'es');
  assert.ok(result.by_stand.some(row => row.stand_name === 'Mesa de Recursos'));

  const built = table('participants', {}, 'es');
  assert.ok(built.headers.includes('Nombre'));
  assert.ok(built.headers.includes('Check-ins · Mesa de Recursos · IHSS'));
  assert.ok(built.headers.includes('¿Tienes seguro?'));
  const ana = rowsAsObjects(built).find(row => row['Nombre'] === 'Ana');
  assert.equal(ana['Asistió'], 'Sí');
  assert.equal(ana['¿Tienes seguro?'], 'Sí', 'the Spanish option label is used');
});

// ---------------------------------------------------------------------------
// Scan log paging
// ---------------------------------------------------------------------------

test('buildScanLog pages newest-first and reports the unpaged total', () => {
  const snapshot = buildSnapshot();
  const first = analytics.buildScanLog(snapshot, NO_FILTERS, 'en', 1, 2);
  assert.equal(first.total, 6);
  assert.equal(first.rows.length, 2);
  assert.equal(first.rows[0].scan_id, 6, 'newest scan first');

  const last = analytics.buildScanLog(snapshot, NO_FILTERS, 'en', 3, 2);
  assert.equal(last.rows.length, 2);
  assert.equal(last.rows[1].scan_id, 1, 'oldest scan last');

  const past = analytics.buildScanLog(snapshot, NO_FILTERS, 'en', 99, 2);
  assert.deepEqual(past.rows, [], 'a page beyond the end is empty, not an error');
});

test('buildScanLog attaches the checkout answers of the scan', () => {
  const snapshot = buildSnapshot();
  const log = analytics.buildScanLog(snapshot, NO_FILTERS, 'en', 1, 10);
  const checkout = log.rows.find(row => row.scan_id === 3);
  assert.deepEqual(checkout.checkout_answers, [
    { question: 'Service status', value: 'Completed' },
    { question: 'Notes', value: 'All good' }
  ]);
  const checkin = log.rows.find(row => row.scan_id === 1);
  assert.deepEqual(checkin.checkout_answers, []);
});

// ---------------------------------------------------------------------------
// CSV serialization
// ---------------------------------------------------------------------------

test('CSV output is BOM-prefixed, semicolon-delimited and quotes separators', () => {
  const csv = analytics.tableToCsv({
    key: 'x', title: 'x',
    headers: ['a', 'b'],
    rows: [['plain', 'has;semicolon'], ['has "quotes"', 'line\nbreak']]
  });
  assert.ok(csv.startsWith('﻿'), 'Excel needs the BOM to detect UTF-8');
  const body = csv.slice(1);
  assert.ok(body.startsWith('a;b\r\n'));
  assert.ok(body.includes('"has;semicolon"'), 'a value containing the delimiter is quoted');
  assert.ok(body.includes('"has ""quotes"""'), 'inner quotes are doubled');
  assert.ok(body.includes('"line\nbreak"'));
});

test('CSV neutralizes values Excel would read as a formula', () => {
  const csv = analytics.tableToCsv({
    key: 'x', title: 'x', headers: ['v'], rows: [['=1+1'], ['-5'], ['+A1'], ['@x'], ['ok']]
  });
  assert.ok(csv.includes("'=1+1"));
  assert.ok(csv.includes("'-5"));
  assert.ok(csv.includes("'+A1"));
  assert.ok(csv.includes("'@x"));
  assert.ok(csv.includes('\r\nok'), 'ordinary values are left alone');
});

test('null and undefined cells become empty strings', () => {
  const csv = analytics.tableToCsv({ key: 'x', title: 'x', headers: ['a', 'b', 'c'], rows: [[null, undefined, 0]] });
  assert.ok(csv.endsWith(';;0\r\n'));
});

// ---------------------------------------------------------------------------
// Workbook serialization
// ---------------------------------------------------------------------------

test('the workbook holds one sheet per dataset and reads back intact', () => {
  const XLSX = require('xlsx-js-style');
  const { snapshot, filters, result } = build();
  const tables = analytics.DATASETS.map(key => analytics.buildTable(key, snapshot, result, filters, 'en'));
  const buffer = analytics.tablesToWorkbook(tables);
  assert.ok(Buffer.isBuffer(buffer) && buffer.length > 0);

  const parsed = XLSX.read(buffer, { type: 'buffer' });
  assert.equal(parsed.SheetNames.length, analytics.DATASETS.length);
  for (const name of parsed.SheetNames) {
    assert.ok(name.length <= 31, `sheet name "${name}" must fit Excel's 31-char limit`);
    assert.ok(!/[[\]:*?/\\]/.test(name), `sheet name "${name}" must avoid characters Excel rejects`);
  }
  // Spot-check that a value survived the round trip.
  const volunteers = XLSX.utils.sheet_to_json(parsed.Sheets['Volunteers'], { defval: '' });
  const volTwo = volunteers.find(row => row['Volunteer'] === 'Vol Two');
  assert.equal(volTwo['Total scans'], 3);
});
