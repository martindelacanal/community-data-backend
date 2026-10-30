'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { copyHealthEventConfiguration } = require('./healthEventConfigurationCopy');
const { createEventDateCopy, dateString } = require('./healthEventCopyDates');

function fixture() {
  const data = {
    stands: [{ id: 1, name_en: 'Dental', name_es: 'Dental', is_entry: 'N', has_checkout: 'Y', enabled: 'Y', sort_order: 2, icon: 'dentistry' }],
    services: [{ id: 2, stand_id: 1, name_en: 'Cleaning', name_es: 'Limpieza', enabled: 'N', sort_order: 3 }],
    forms: [
      { id: 3, audience: 'checkout', stand_id: 1, title_en: 'Checkout', title_es: 'Salida', enabled: 'Y' },
      { id: 4, audience: 'beneficiary', stand_id: null, title_en: 'Registration', title_es: 'Registro', required_before_qr: 'Y', enabled: 'Y' },
      { id: 5, audience: 'volunteer', stand_id: null, title_en: 'Volunteers', title_es: 'Voluntarios', intro_es: 'Formulario', enabled: 'Y' }
    ],
    questions: [
      // Child precedes its parent, which is in another form.
      { id: 10, form_id: 4, question_type: 'appointment', name_en: 'Dental (Sunday, 8/9/26)', name_es: 'Dental (domingo 9/8/26)', required: 'Y', enabled: 'Y', depends_on_question_id: 11, depends_on_option_id: 21, config_json: '{"event_date":"2026-08-09","service_key":"dental"}' },
      { id: 11, form_id: 5, question_type: 'single', maps_to: 'attend_date', enabled: 'Y', config_json: { option_dates: { 20: '2026-08-08', 21: '2026-08-09' }, custom: 'untouched' } },
      { id: 12, form_id: 4, question_type: 'text', enabled: 'Y', depends_on_question_id: 10, depends_on_option_id: null },
      { id: 13, form_id: 3, question_type: 'notice', required: 'N', enabled: 'N', help_en: 'Existing instructions' }
    ],
    options: [
      { id: 20, question_id: 11, name_en: 'Saturday, August 8, 2026', name_es: 'Sábado 8 de agosto de 2026', event_date: '2026-08-08', profile_option_id: 77, enabled: 'Y' },
      { id: 21, question_id: 11, name_en: 'Sunday, August 9, 2026', name_es: 'Domingo 9 de agosto de 2026', event_date: '2026-08-09', is_other: 'N', enabled: 'Y' }
    ],
    slots: [
      { id: 30, service_key: 'dental', slot_date: '2026-08-08', start_time: '08:00:00', end_time: '09:00:00', capacity: 12, enabled: 'Y' },
      { id: 31, service_key: 'vision', slot_date: '2026-08-09', start_time: '09:00:00', end_time: '10:00:00', capacity: null, enabled: 'Y' },
      { id: 32, service_key: 'dental', slot_date: '2026-08-08', start_time: '10:00:00', end_time: '11:00:00', capacity: 0, enabled: 'N' }
    ]
  };
  const writes = [], statements = [];
  let nextId = 100;
  const connection = { async query(sql, params) {
    statements.push(sql);
    if (sql.startsWith('SELECT')) {
      const key = /FROM health_event_stand_service/.test(sql) ? 'services' : /FROM health_event_question_option/.test(sql) ? 'options' :
        /FROM health_event_question/.test(sql) ? 'questions' : /FROM health_event_stand/.test(sql) ? 'stands' : /FROM health_event_form/.test(sql) ? 'forms' : /FROM health_event_slot/.test(sql) ? 'slots' : null;
      assert.ok(key, sql); return [data[key]];
    }
    if (sql.startsWith('INSERT')) {
      const [, table, columns] = sql.match(/INSERT INTO (\w+)\(([^)]+)\)/);
      const row = Object.fromEntries(columns.split(',').map((c, i) => [c.trim(), params[i]]));
      row.id = nextId++; writes.push({ table, row }); return [{ insertId: row.id }];
    }
    if (sql.startsWith('UPDATE health_event_question SET')) {
      const row = writes.find(w => w.row.id === params[4]).row;
      [row.depends_on_question_id, row.depends_on_option_id, row.config_json, row.enabled] = params;
      return [{ affectedRows: 1 }];
    }
    throw new Error(sql);
  } };
  const source = { id: 1, start_date: '2026-08-08', end_date: '2026-08-09' };
  const target = { id: 3, start_date: '2026-12-05', end_date: '2026-12-06' };
  const rows = table => writes.filter(w => w.table === `health_event_${table}`).map(w => w.row);
  return { data, source, target, connection, rows, statements };
}

test('copies the entire editable configuration with independent IDs, dependencies, profile mappings and empty appointment capacity', async () => {
  const f = fixture(), before = JSON.stringify(f.data);
  assert.deepEqual(await copyHealthEventConfiguration(f), { stands: 1, services: 1, forms: 3, questions: 4, options: 2, slots: 3 });
  const [stand] = f.rows('stand'), [service] = f.rows('stand_service'), forms = f.rows('form');
  assert.equal(stand.health_event_id, 3); assert.equal(service.stand_id, stand.id);
  assert.equal(service.enabled, 'N'); assert.equal(stand.has_checkout, 'Y');
  assert.equal(forms[0].stand_id, stand.id); assert.equal(forms[1].stand_id, null);
  assert.deepEqual(forms.map(f => f.audience), ['checkout', 'beneficiary', 'volunteer']);
  assert.ok(forms.every(f => f.health_event_id === 3));
  const [child, parent, grandchild, notice] = f.rows('question'), [first, second] = f.rows('question_option');
  assert.equal(child.form_id, forms[1].id); assert.equal(parent.form_id, forms[2].id);
  assert.equal(child.depends_on_question_id, parent.id); assert.equal(child.depends_on_option_id, second.id);
  assert.equal(grandchild.depends_on_question_id, child.id); assert.equal(first.question_id, parent.id);
  assert.equal(first.profile_option_id, 77, 'global catalogue IDs are retained');
  assert.equal(first.event_date, '2026-12-05'); assert.equal(second.event_date, '2026-12-06');
  assert.equal(first.name_en, 'Saturday, December 5, 2026'); assert.equal(second.name_es, 'Domingo 6 de diciembre de 2026');
  assert.equal(child.name_en, 'Dental (Sunday, 12/6/26)'); assert.equal(child.name_es, 'Dental (domingo 6/12/26)');
  assert.deepEqual(JSON.parse(child.config_json), { event_date: '2026-12-06', service_key: 'dental' });
  assert.deepEqual(JSON.parse(parent.config_json), { option_dates: { [first.id]: '2026-12-05', [second.id]: '2026-12-06' }, custom: 'untouched' });
  assert.equal(notice.enabled, 'N'); assert.equal(notice.required, 'N');
  const slots = f.rows('slot');
  assert.deepEqual(slots.map(s => [s.slot_date, s.capacity, s.enabled]), [['2026-12-05', 12, 'Y'], ['2026-12-06', null, 'Y'], ['2026-12-05', 0, 'N']]);
  assert.equal(slots[0].start_time, '08:00:00'); assert.equal(slots[0].health_event_id, 3);
  assert.ok(f.statements.every(sql => !/health_event_(registration|appointment|answer|scan|notification|wristband|volunteer_assignment)\b/.test(sql)));
  assert.equal(JSON.stringify(f.data), before);
});

test('a shorter event disables out-of-range dates, slots and their question dependency chain', async () => {
  const f = fixture(); f.target.end_date = f.target.start_date;
  await copyHealthEventConfiguration(f);
  assert.equal(f.rows('slot')[1].enabled, 'N');
  assert.equal(f.rows('question_option')[1].enabled, 'N');
  assert.equal(f.rows('question')[0].enabled, 'N'); assert.equal(f.rows('question')[2].enabled, 'N');
  assert.equal(f.rows('question')[1].enabled, 'Y'); assert.equal(f.rows('slot')[0].enabled, 'Y');
});

test('refuses dependencies and checkout stands outside the source graph', async () => {
  for (const mutate of [f => { f.data.questions[0].depends_on_question_id = 999; }, f => { f.data.questions[0].depends_on_option_id = 999; }, f => { f.data.forms[0].stand_id = 999; }, f => { f.data.questions[1].config_json.option_dates[999] = '2026-08-08'; }]) {
    const f = fixture(); mutate(f);
    await assert.rejects(copyHealthEventConfiguration(f), /outside the source event/);
  }
});

test('moves labels once, across years and daylight saving, including volunteer setup day, without rewriting unrelated text', () => {
  const copy = createEventDateCopy({start_date:'2026-08-08',end_date:'2026-08-09'}, {start_date:'2026-12-05',end_date:'2026-12-06'});
  assert.equal(copy.text('Friday, August 7, 2026 (Set-Up)', 'en'), 'Friday, December 4, 2026 (Set-Up)');
  assert.equal(copy.text('Interés (sábado 8 de agosto): ¿qué rol?', 'es'), 'Interés (sábado 5 de diciembre): ¿qué rol?');
  assert.equal(copy.text('Have you attended a previous event? 12345', 'en'), 'Have you attended a previous event? 12345');
  const overlap = createEventDateCopy({start_date:'2026-08-08',end_date:'2026-08-09'}, {start_date:'2026-08-09',end_date:'2026-08-10'});
  assert.equal(overlap.text('8/8/26 and 8/9/26', 'en'), '8/9/26 and 8/10/26');
  const year = createEventDateCopy({start_date:'2026-12-31',end_date:'2027-01-01'}, {start_date:'2027-03-13',end_date:'2027-03-14'});
  assert.equal(year.shift('2027-01-01'), '2027-03-14');
  assert.equal(dateString(new Date(2026, 7, 8)), '2026-08-08');
  assert.equal(copy.shift(null), null);
  assert.throws(() => copy.shift('2026-02-30'), /Invalid/);
});
