'use strict';

const { createEventDateCopy } = require('./healthEventCopyDates');

async function readHealthEventConfiguration(connection, eventId, { lock = false } = {}) {
  const suffix = lock ? ' FOR UPDATE' : '';
  const [stands] = await connection.query('SELECT * FROM health_event_stand WHERE health_event_id = ? ORDER BY id' + suffix, [eventId]);
  const [services] = await connection.query(`SELECT ss.* FROM health_event_stand_service ss
    JOIN health_event_stand s ON s.id = ss.stand_id WHERE s.health_event_id = ? ORDER BY ss.id` + suffix, [eventId]);
  const [forms] = await connection.query('SELECT * FROM health_event_form WHERE health_event_id = ? ORDER BY id' + suffix, [eventId]);
  const [questions] = await connection.query(`SELECT q.* FROM health_event_question q
    JOIN health_event_form f ON f.id = q.form_id WHERE f.health_event_id = ? ORDER BY q.id` + suffix, [eventId]);
  const [options] = await connection.query(`SELECT o.*, DATE_FORMAT(o.event_date, '%Y-%m-%d') AS event_date FROM health_event_question_option o
    JOIN health_event_question q ON q.id = o.question_id JOIN health_event_form f ON f.id = q.form_id
    WHERE f.health_event_id = ? ORDER BY o.id` + suffix, [eventId]);
  const [slots] = await connection.query("SELECT *, DATE_FORMAT(slot_date, '%Y-%m-%d') AS slot_date FROM health_event_slot WHERE health_event_id = ? ORDER BY id" + suffix, [eventId]);
  return { stands, services, forms, questions, options, slots };
}

// Table/column names below are private allowlists, never request input.
async function insert(connection, table, columns, row) {
  const [result] = await connection.query(`INSERT INTO ${table}(${columns.join(',')}) VALUES (${columns.map(() => '?').join(',')})`, columns.map(key => row[key] ?? null));
  return result.insertId;
}
function mapped(map, id) {
  if (id == null) return null;
  if (!map.has(Number(id))) throw new Error('Event configuration references a row outside the source event');
  return map.get(Number(id));
}
function parsed(value) { return typeof value === 'string' ? JSON.parse(value) : value; }

/** Caller owns the transaction and must supply a new/empty destination. Never copies people or bookings. */
async function copyHealthEventConfiguration({ connection, source, target }) {
  const data = await readHealthEventConfiguration(connection, source.id, { lock: true });
  const dates = createEventDateCopy(source, target);
  const standIds = new Map(), formIds = new Map(), questionIds = new Map(), optionIds = new Map();
  const counts = Object.fromEntries(Object.entries(data).map(([key, rows]) => [key, rows.length]));
  const localized = row => {
    const copy = { ...row };
    for (const key of ['name', 'title', 'intro', 'help']) {
      for (const locale of ['en', 'es']) {
        if (copy[`${key}_${locale}`] != null) copy[`${key}_${locale}`] = dates.text(copy[`${key}_${locale}`], locale);
      }
    }
    return copy;
  };
  for (const row of data.stands) {
    const id = await insert(connection, 'health_event_stand', ['health_event_id','name_en','name_es','icon','is_entry','has_checkout','sort_order','enabled'], { ...row, health_event_id: target.id });
    standIds.set(row.id, id);
  }
  for (const row of data.services) {
    await insert(connection, 'health_event_stand_service', ['stand_id','name_en','name_es','sort_order','enabled'], { ...row, stand_id: mapped(standIds, row.stand_id) });
  }
  for (const row of data.forms) {
    const id = await insert(connection, 'health_event_form', ['health_event_id','audience','stand_id','title_en','title_es','intro_en','intro_es','section_order','required_before_qr','enabled'], {
      ...localized(row), health_event_id: target.id, stand_id: mapped(standIds, row.stand_id)
    });
    formIds.set(row.id, id);
  }
  // Insert the whole graph first: dependencies may point to questions in a later form.
  const disabledQuestions = new Set(), disabledOptions = new Set();
  for (const row of data.questions) {
    const config = parsed(row.config_json);
    const enabled = config?.event_date && !dates.inRange(dates.shift(config.event_date)) ? 'N' : row.enabled;
    if (enabled === 'N') disabledQuestions.add(row.id);
    const id = await insert(connection, 'health_event_question', ['form_id','question_type','name_en','name_es','help_en','help_es','required','allow_other','maps_to','config_json','sort_order','enabled'], {
      ...localized(row), form_id: mapped(formIds, row.form_id), config_json: null, enabled
    });
    questionIds.set(row.id, id);
  }
  for (const row of data.options) {
    const event_date = dates.shift(row.event_date);
    const enabled = dates.inRange(event_date) ? row.enabled : 'N';
    if (enabled === 'N') disabledOptions.add(row.id);
    const id = await insert(connection, 'health_event_question_option', ['question_id','name_en','name_es','is_other','event_date','service_key','profile_option_id','sort_order','enabled'], {
      ...localized(row), question_id: mapped(questionIds, row.question_id), event_date, enabled
    });
    optionIds.set(row.id, id);
  }
  // A shorter destination must not expose dependent questions for omitted dates.
  let changed;
  do {
    changed = false;
    for (const row of data.questions) {
      if (!disabledQuestions.has(row.id) && (disabledQuestions.has(row.depends_on_question_id) || disabledOptions.has(row.depends_on_option_id))) {
        disabledQuestions.add(row.id); changed = true;
      }
    }
  } while (changed);
  for (const row of data.questions) {
    const originalConfig = parsed(row.config_json);
    const config = originalConfig == null ? null : { ...originalConfig };
    if (config?.event_date) config.event_date = dates.shift(config.event_date);
    if (config?.option_dates) {
      config.option_dates = Object.fromEntries(Object.entries(config.option_dates).map(([optionId, value]) => [mapped(optionIds, Number(optionId)), dates.shift(value)]));
    }
    await connection.query('UPDATE health_event_question SET depends_on_question_id = ?, depends_on_option_id = ?, config_json = ?, enabled = ? WHERE id = ?', [
      mapped(questionIds, row.depends_on_question_id), mapped(optionIds, row.depends_on_option_id),
      config == null ? null : JSON.stringify(config), disabledQuestions.has(row.id) ? 'N' : row.enabled, mapped(questionIds, row.id)
    ]);
  }
  for (const row of data.slots) {
    const slot_date = dates.shift(row.slot_date);
    await insert(connection, 'health_event_slot', ['health_event_id','service_key','slot_date','start_time','end_time','capacity','enabled'], {
      ...row, health_event_id: target.id, slot_date, enabled: dates.inRange(slot_date) ? row.enabled : 'N'
    });
  }
  return counts;
}

module.exports = { copyHealthEventConfiguration, readHealthEventConfiguration };
