'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const templates = require('./clinicalTemplates');
const { clinicalData } = require('./clinicalValidation');
const { createFhirBundle } = require('./clinicalFhir');

const bySpecialty = Object.fromEntries(templates.catalog.templates.map(template => [template.specialty, template]));
const timestamp = '2026-10-06T15:30:00.000Z';
const patient = { id: 411, event_id: 412, is_synthetic: 1, synthetic_code: 'CPTEST-template-unit', display_name: 'Fictional Template Patient', date_of_birth: '1990-01-01', sex: 'Unknown' };
function valueFor(field, index) {
  if (field.type === 'number') return Math.max(field.min ?? -1000000, Math.min(field.max ?? 1000000, index % 2 ? 0 : 1.25));
  if (field.type === 'select') return field.options[index % field.options.length].value;
  if (field.type === 'date') return '2026-10-06';
  if (field.type === 'datetime') return timestamp;
  return `Fixture ${field.key}`.slice(0, field.maxLength ?? 2000).trim();
}
function fullData(template) {
  const data = { template_version: 2, template_id: template.id, template_fields: Object.fromEntries(templates.fields(template).map((field, index) => [field.key, valueFor(field, index)])) };
  if (template.tooth_chart) Object.assign(data, { tooth_notation: 'universal', teeth: [{ number: '1', condition: 'Fictional adult finding', treatment: 'Fictional adult treatment' }, { number: 'A', condition: 'Fictional primary finding', treatment: 'Fictional primary treatment' }] });
  return data;
}
function dataWith(specialty, answers = {}) { return { template_version: 2, template_id: bySpecialty[specialty].id, template_fields: answers }; }
function rejects(specialty, data, code = 'INVALID_DATA', status = 400) {
  assert.throws(() => clinicalData(specialty, data), error => error.code === code && error.status === status);
}
function sourceRecord(specialty, data, overrides = {}) {
  return { id: `record-${specialty}`, patient_id: patient.id, event_id: patient.event_id, specialty, status: 'final', revision: 2, recorded_by: 413, finalized_by: 414, created_at: timestamp, updated_at: timestamp, data, attachments: [], ...overrides };
}
function bundle(records) { return createFhirBundle({ patient, records, exportedAt: timestamp, exporterId: 414 }); }
function resources(result, type) { return result.entry.map(entry => entry.resource).filter(resource => resource.resourceType === type); }
function flatten(items) { return (items || []).flatMap(item => [item, ...flatten(item.item)]); }
function sourceAnswer(qr, key) { return flatten(qr.item).find(item => item.linkId === key); }
function observation(result, code) { return resources(result, 'Observation').find(item => item.code.coding.some(coding => coding.code === code)); }
function dataAtBytes(template, bytes) {
  const data = { template_version: 2, template_id: template.id, template_fields: {} };
  for (const field of templates.fields(template).filter(field => !field.type || field.type === 'textarea')) {
    data.template_fields[field.key] = '';
    const remaining = bytes - Buffer.byteLength(JSON.stringify(data));
    assert.ok(remaining >= 0, 'Fixture must leave room for a field key');
    const length = Math.min(field.maxLength ?? 2000, remaining);
    data.template_fields[field.key] = 'x'.repeat(length);
    if (length === remaining) return data;
  }
  throw new Error('Catalog has insufficient text capacity for the byte-boundary fixture');
}

test('all 524 catalog fields retain their typed values through validation and JSON storage', () => {
  assert.equal(templates.catalog.schema_version, 2);
  assert.equal(templates.catalog.templates.length, 4);
  assert.equal(templates.catalog.templates.reduce((sum, template) => sum + templates.fields(template).length, 0), 524);
  for (const template of templates.catalog.templates) {
    const input = fullData(template);
    const validated = clinicalData(template.specialty, input);
    assert.deepEqual(validated, input, template.id);
    assert.deepEqual(clinicalData(template.specialty, JSON.parse(JSON.stringify(validated))), input, template.id);
    assert.equal(templates.templateFor(template.specialty, input).id, template.id);
    assert.equal(new Set(templates.fields(template).map(field => field.key)).size, templates.fields(template).length);
    assert.ok(template.sources.length > 0);
  }
});

test('numeric answers are never coerced from strings, booleans, arrays or non-finite values', () => {
  for (const invalid of ['0', '-1.25', true, false, [], {}, NaN, Infinity, -Infinity]) rejects('optometry', dataWith('optometry', { MRODSPH: invalid }));
  assert.deepEqual(clinicalData('optometry', dataWith('optometry', { MRODSPH: 0, MROSSPH: -1.25 })).template_fields, { MRODSPH: 0, MROSSPH: -1.25 });
  rejects('dental', dataWith('dental', { dental_pain_score: -1 }));
  rejects('dental', dataWith('dental', { dental_pain_score: 11 }));
});

test('select membership, text limits, calendar dates and UTC datetimes are validated', () => {
  rejects('dental', dataWith('dental', { dental_hypertension: 'sometimes' }));
  rejects('general', dataWith('general', { anorexia: 'yes' }));
  rejects('clearance', dataWith('clearance', { consult_request_date: '2026-02-30' }));
  for (const invalid of ['2026-02-30T12:00:00Z', '2026-10-06T12:00:00', '2026-10-06T12:00:00-03:00', 123]) rejects('general', dataWith('general', { date: invalid }));
  assert.equal(clinicalData('general', dataWith('general', { date: '2026-10-06T12:00:00Z' })).template_fields.date, '2026-10-06T12:00:00.000Z');
  rejects('clearance', dataWith('clearance', { consult_report: '\u0001invalid' }));
  rejects('clearance', dataWith('clearance', { consult_to: '\u00e9'.repeat(251) }));
});

test('version, specialty and answer allowlists reject mixed or downgraded schemas', () => {
  for (const version of [null, 0, 1, 3, '2', true]) rejects('general', { ...dataWith('general'), template_version: version }, 'INVALID_CLINICAL_TEMPLATE');
  rejects('general', { ...dataWith('general'), template_id: 'unknown-v2' }, 'INVALID_CLINICAL_TEMPLATE');
  for (const template of templates.catalog.templates) for (const other of templates.catalog.templates) {
    if (template.specialty !== other.specialty) rejects(other.specialty, fullData(template), 'INVALID_CLINICAL_TEMPLATE');
  }
  rejects('general', dataWith('general', { dental_hypertension: 'yes' }));
  rejects('general', dataWith('general', { unexpected_answer: 'fictional' }));
  rejects('general', { ...dataWith('general'), assessment: 'Mixed v1 field' });
  rejects('general', { template_id: bySpecialty.general.id, template_fields: {} });
  rejects('general', { ...dataWith('general'), template_fields: [] });
  const legacy = { assessment: 'Legacy assessment', plan: 'Legacy plan' };
  assert.deepEqual(clinicalData('general', legacy), legacy);
  assert.equal(templates.sameTemplate(legacy, { ...legacy, findings: 'A v1 update' }), true);
  assert.equal(templates.sameTemplate(legacy, dataWith('general')), false);
  assert.equal(templates.sameTemplate(dataWith('general'), legacy), false);
  assert.equal(templates.sameTemplate(dataWith('general'), dataWith('optometry')), false);
});

test('blank, unknown and explicitly unassessed answers stay distinct without normal defaults', () => {
  const input = { ...dataWith('dental', { dental_hypertension: 'unknown', dental_diabetes: null, dental_pain_score: 0 }), not_assessed: ['dental_immune'] };
  const output = clinicalData('dental', input);
  assert.deepEqual(output.template_fields, input.template_fields);
  assert.deepEqual(output.not_assessed, ['dental_immune']);
  assert.equal(Object.hasOwn(output.template_fields, 'dental_heart_attack'), false);
  rejects('dental', { ...dataWith('dental'), not_assessed: ['invented-section'] }, 'INVALID_NOT_ASSESSED');
  rejects('dental', { ...dataWith('dental'), not_assessed: ['dental_immune', 'dental_immune'] }, 'INVALID_NOT_ASSESSED');
  rejects('dental', { ...dataWith('dental', { dental_hypertension: 'unknown' }), not_assessed: ['dental_circulatory'] }, 'INVALID_NOT_ASSESSED');
  assert.deepEqual(clinicalData('general', dataWith('general')).template_fields, {});
  for (const template of templates.catalog.templates) {
    const data = dataWith(template.specialty);
    assert.equal(templates.readyToFinalize(template.specialty, data), false);
    for (const key of template.required_to_finalize) data.template_fields[key] = 'Fictional reviewed narrative';
    assert.equal(templates.readyToFinalize(template.specialty, data), true);
    for (const key of template.required_to_finalize) {
      assert.equal(templates.readyToFinalize(template.specialty, { ...data, template_fields: { ...data.template_fields, [key]: '   ' } }), false, `${template.id}:${key}`);
    }
  }
});

test('the 64 KiB record budget is enforced after field validation at the exact byte boundary', () => {
  const allowed = dataAtBytes(bySpecialty.general, 64 * 1024);
  assert.equal(Buffer.byteLength(JSON.stringify(allowed)), 64 * 1024);
  assert.deepEqual(clinicalData('general', allowed), allowed);
  const oversized = dataAtBytes(bySpecialty.general, 64 * 1024 + 1);
  rejects('general', oversized, 'CLINICAL_RECORD_TOO_LARGE', 413);
});

test('adult and primary teeth retain an explicit numbering scheme independently of answers', () => {
  const universal = fullData(bySpecialty.dental);
  assert.deepEqual(clinicalData('dental', universal).teeth, universal.teeth);
  const fdi = { ...dataWith('dental'), tooth_notation: 'fdi', teeth: [{ number: '51', condition: 'Fictional primary tooth', treatment: null }] };
  assert.deepEqual(clinicalData('dental', fdi), fdi);
  rejects('dental', { ...dataWith('dental'), teeth: [{ number: '1' }] });
  rejects('dental', { ...dataWith('dental'), tooth_notation: 'universal', teeth: [{ number: '99' }] }, 'INVALID_TOOTH');
  rejects('general', { ...dataWith('general'), tooth_notation: 'universal', teeth: [] });
});

test('FHIR preserves every v2 field and resolves versioned Questionnaires alongside unchanged v1', () => {
  const source = templates.catalog.templates.map(template => sourceRecord(template.specialty, fullData(template)));
  source.push(sourceRecord('general', { assessment: 'Original v1 narrative', plan: 'Original v1 plan' }, { id: 'legacy-general' }));
  const result = bundle(source);
  const questionnaires = resources(result, 'Questionnaire');
  assert.equal(questionnaires.length, 5);
  assert.deepEqual(new Set(questionnaires.map(q => q.version)), new Set(['1.0.0', '2.0.0']));
  for (const record of source) {
    const qr = resources(result, 'QuestionnaireResponse').find(resource => resource.identifier.value === record.id);
    const [url, version] = qr.questionnaire.split('|');
    const definition = questionnaires.find(q => q.url === url && q.version === version);
    assert.ok(definition, `Resolved Questionnaire for ${record.id}`);
    if (record.id === 'legacy-general') {
      assert.equal(version, '1.0.0');
      assert.equal(sourceAnswer(qr, 'assessment').answer[0].valueString, 'Original v1 narrative');
      continue;
    }
    assert.equal(version, '2.0.0');
    const definedKeys = new Set(flatten(definition.item).map(item => item.linkId));
    for (const field of templates.fields(bySpecialty[record.specialty])) {
      assert.ok(definedKeys.has(field.key), `Questionnaire contains ${field.key}`);
      const answer = sourceAnswer(qr, field.key)?.answer?.[0];
      assert.ok(answer, `QuestionnaireResponse preserves ${field.key}`);
      const expected = record.data.template_fields[field.key];
      if (field.type === 'select') assert.equal(answer.valueCoding.code, `${field.key}--${expected}`);
      else assert.equal(Object.values(answer)[0], expected, field.key);
    }
  }
  const dentalResponse = resources(result, 'QuestionnaireResponse').find(qr => qr.identifier.value === 'record-dental');
  assert.equal(dentalResponse.item.filter(item => item.linkId === 'teeth').length, 2);
  assert.equal(sourceAnswer(dentalResponse, 'tooth_notation').answer[0].valueCoding.code, 'tooth_notation--universal');
});

test('FHIR keeps the original Fahrenheit, pounds, inches and measurement time without unit conversion', () => {
  const data = dataWith('general', { date: timestamp, temperature: 98.6, weight: 154, height: 68, bps: 120, bpd: 80, oxygen_saturation: 0 });
  const result = bundle([sourceRecord('general', data)]);
  for (const [code, value, unit] of [['8310-5', 98.6, '[degF]'], ['29463-7', 154, '[lb_av]'], ['8302-2', 68, '[in_i]'], ['2708-6', 0, '%']]) {
    const obs = observation(result, code);
    assert.deepEqual(obs.valueQuantity, { value, unit, system: 'http://unitsofmeasure.org', code: unit });
    assert.equal(obs.effectiveDateTime, timestamp);
    assert.equal(obs.effectivePeriod, undefined);
  }
  const q = resources(result, 'Questionnaire')[0];
  for (const [key, unit] of [['temperature', '[degF]'], ['weight', '[lb_av]'], ['height', '[in_i]']]) assert.ok(sourceAnswer(q, key).extension.some(extension => extension.valueCoding.code === unit));
  const bp = observation(result, '85354-9');
  assert.deepEqual(bp.component.map(component => component.valueQuantity.value), [120, 80]);
  const partial = observation(bundle([sourceRecord('general', dataWith('general', { bps: 120 }))]), '85354-9');
  assert.equal(partial.component[1].valueQuantity, undefined);
  assert.equal(partial.component[1].dataAbsentReason.coding[0].code, 'unknown');
});

test('FHIR preserves zero and negative eye quantities while palpation FTN remains text without mmHg', () => {
  const data = dataWith('optometry', { MRODSPH: 0, MROSSPH: -1.25, MRODCYL: -0.5, ODIOPAP: 17, OSIOPTPN: 16, ODIOPFTN: 'F/T/N', OSIOPFTN: 'Soft by palpation' });
  const result = bundle([sourceRecord('optometry', data)]);
  assert.equal(observation(result, 'MRODSPH').valueQuantity.value, 0);
  assert.equal(observation(result, 'MROSSPH').valueQuantity.value, -1.25);
  assert.equal(observation(result, 'MRODCYL').valueQuantity.code, '[diop]');
  assert.equal(observation(result, 'MRODSPH').bodySite.coding[0].code, 'right_eye');
  assert.equal(observation(result, 'MROSSPH').bodySite.coding[0].code, 'left_eye');
  assert.equal(observation(result, 'ODIOPAP').valueQuantity.code, 'mm[Hg]');
  assert.equal(observation(result, 'ODIOPAP').method.text, 'Applanation tonometry');
  assert.equal(observation(result, 'OSIOPTPN').method.text, 'Tono-Pen tonometry');
  assert.equal(observation(result, 'ODIOPFTN'), undefined);
  assert.equal(observation(result, 'OSIOPFTN'), undefined);
  const qr = resources(result, 'QuestionnaireResponse')[0];
  assert.equal(sourceAnswer(qr, 'ODIOPFTN').answer[0].valueString, 'F/T/N');
  assert.equal(sourceAnswer(qr, 'OSIOPFTN').answer[0].valueString, 'Soft by palpation');
  for (const field of flatten(resources(result, 'Questionnaire')[0].item).filter(field => ['ODIOPFTN', 'OSIOPFTN'].includes(field.linkId))) assert.equal(field.extension, undefined);
});

test('FHIR does not turn null, unknown or unassessed answers into diagnoses or negative findings', () => {
  const data = { ...dataWith('dental', { dental_hypertension: 'unknown', dental_diabetes: null }), not_assessed: ['dental_immune'] };
  const result = bundle([sourceRecord('dental', data, { status: 'draft', finalized_by: null })]);
  const qr = resources(result, 'QuestionnaireResponse')[0];
  assert.equal(qr.status, 'in-progress');
  assert.equal(sourceAnswer(qr, 'dental_hypertension').answer[0].valueCoding.code, 'dental_hypertension--unknown');
  assert.equal(sourceAnswer(qr, 'dental_diabetes').answer, undefined);
  assert.equal(sourceAnswer(qr, 'dental_heart_attack'), undefined);
  assert.deepEqual(sourceAnswer(qr, 'not_assessed').answer, [{ valueString: 'dental_immune' }]);
  assert.equal(resources(result, 'Condition').length, 0);
  assert.equal(resources(result, 'Observation').length, 0);
});
