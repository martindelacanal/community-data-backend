'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const express = require('express');
const jwt = require('jsonwebtoken');
const mysql = require('mysql2/promise');
const { databaseConfig } = require('../../scripts/migrateHealthWristbands');
const { createClinicalSandboxRouter } = require('./clinicalSandbox');
const { catalog, fields } = require('../services/clinicalTemplates');

function fullData(template) {
  const templateFields = Object.fromEntries(fields(template).map((field, index) => {
    let value;
    if (field.type === 'number') value = Math.max(field.min ?? -1000000, Math.min(field.max ?? 1000000, index % 2 ? 0 : 1.25));
    else if (field.type === 'select') value = field.options[index % field.options.length].value;
    else if (field.type === 'date') value = '2026-10-06';
    else if (field.type === 'datetime') value = '2026-10-06T15:30:00.000Z';
    else value = `Synthetic ${field.key}`.slice(0, field.maxLength ?? 2000).trim();
    return [field.key, value];
  }));
  const data = { template_version: 2, template_id: template.id, template_fields: templateFields };
  if (template.tooth_chart) Object.assign(data, { tooth_notation: 'universal', teeth: [{ number: 'A', condition: 'Fictional primary tooth finding', treatment: 'Fictional follow-up' }, { number: '32', condition: 'Fictional adult tooth finding', treatment: null }] });
  return data;
}
function oversizedData(template) {
  const data = { template_version: 2, template_id: template.id, template_fields: {} };
  for (const field of fields(template).filter(field => !field.type || field.type === 'textarea')) {
    data.template_fields[field.key] = '';
    const remaining = 64 * 1024 + 1 - Buffer.byteLength(JSON.stringify(data));
    const length = Math.min(field.maxLength ?? 2000, remaining);
    assert.ok(length >= 0);
    data.template_fields[field.key] = 'x'.repeat(length);
    if (length === remaining) return data;
  }
  throw new Error('Insufficient text capacity for oversized fixture');
}
function flatten(items) { return (items || []).flatMap(item => [item, ...flatten(item.item)]); }

test('clinical templates v2: authenticated catalog, development DB roundtrips, immutable versions, bridge payloads and mixed-version FHIR', {
  skip: process.env.RUN_CLINICAL_SANDBOX_INTEGRATION !== 'development' ? 'Set RUN_CLINICAL_SANDBOX_INTEGRATION=development after migration' : false,
  timeout: 300000,
}, async () => {
  const config = databaseConfig('development'), production = databaseConfig('production');
  assert.notEqual(config.host.toLowerCase(), production.host.toLowerCase(), 'The development target must be distinct from production');
  const db = mysql.createPool({ ...config, multipleStatements: false, connectionLimit: 5, dateStrings: true });
  const suffix = crypto.randomBytes(8).toString('hex'), users = [], events = [];
  const secret = crypto.randomBytes(32).toString('hex');
  const env = { JWT_SECRET: secret, CLINICAL_SANDBOX_ENABLED: 'true' };
  const nativeWrites = new Map();
  let server, nativePatientCalls = 0;
  const adapter = {
    configured: () => true,
    status: async () => ({ mode: 'synthetic', attachments: { scanner: { healthy: true } } }),
    upsertPatient: async patient => { nativePatientCalls++; assert.equal(patient.is_synthetic, true); assert.match(patient.synthetic_code, /^CPTEST-/); return { patient_uuid: `patient-${patient.id}` }; },
    createEncounter: async (patientId, record) => ({ encounter_uuid: `encounter-${record.id}` }),
    saveRecord: async (payload, actor) => {
      assert.equal(payload.is_test, true);
      assert.equal(actor.can_finalize, true);
      assert.ok(actor.id && actor.original_recorder.id);
      assert.equal(payload.schema_version, payload.data.template_version === 2 ? 2 : 1);
      if (payload.supersedes_record_uuid) {
        const original = [...nativeWrites.values()].find(value => value.result.record_uuid === payload.supersedes_record_uuid);
        assert.ok(original);
        assert.equal(payload.encounter_uuid, original.payload.encounter_uuid);
      }
      const existing = nativeWrites.get(payload.idempotency_key);
      if (existing) { assert.deepEqual(existing.payload, payload); return existing.result; }
      const result = { record_uuid: `native-${payload.external_record_id}` };
      nativeWrites.set(payload.idempotency_key, { payload: structuredClone(payload), actor: structuredClone(actor), result });
      return result;
    },
    download: async () => { throw new Error('No test record has an attachment to download'); },
  };
  try {
    const [roles] = await db.query("SELECT id,name FROM role WHERE name IN ('admin','eventvolunteer','beneficiary')");
    const roleIds = Object.fromEntries(roles.map(role => [role.name, role.id]));
    async function createUser(label, role) {
      const [result] = await db.query("INSERT INTO user(username,firstname,lastname,role_id,enabled,deleted,language) VALUES(?,?,?,?,'Y','N','en')", [`__ctpl_${suffix}_${label}`, label, 'Synthetic Template Test', roleIds[role]]);
      const user = { id: result.insertId, role }; users.push(user); return user;
    }
    const admin = await createUser('Admin', 'admin'), clinician = await createUser('Clinician', 'eventvolunteer'), outsider = await createUser('Other', 'eventvolunteer'), beneficiary = await createUser('Beneficiary', 'beneficiary');
    const tokens = new Map(users.map(user => [user.id, jwt.sign({ data: JSON.stringify({ ...user, role: 'admin' }) }, secret, { expiresIn: '15m' })]));
    const app = express(); app.use(express.json({ limit: '80kb' })); app.use('/api/clinical-sandbox', createClinicalSandboxRouter({ db, env, adapter }));
    server = app.listen(0, '127.0.0.1'); await new Promise(resolve => server.once('listening', resolve));
    const base = `http://127.0.0.1:${server.address().port}/api/clinical-sandbox`;
    async function call(path, actor = admin, method = 'GET', body) {
      const response = await fetch(base + path, { method, headers: { ...(actor ? { Authorization: `Bearer ${tokens.get(actor.id)}` } : {}), ...(body ? { 'Content-Type': 'application/json' } : {}) }, body: body ? JSON.stringify(body) : undefined });
      const text = await response.text(); let data; try { data = JSON.parse(text); } catch { data = text; }
      return { status: response.status, data, headers: response.headers };
    }
    function expect(response, status, error) { assert.equal(response.status, status, JSON.stringify(response.data)); if (error) assert.equal(response.data.error, error); return response.data; }
    expect(await call('/templates', null), 401);
    expect(await call('/templates', beneficiary), 403);
    const available = await call('/templates', clinician); expect(available, 200);
    assert.deepEqual(available.data, catalog);
    assert.equal(available.headers.get('cache-control'), 'no-store, private');
    env.CLINICAL_SANDBOX_ENABLED = 'false'; expect(await call('/templates'), 503); env.CLINICAL_SANDBOX_ENABLED = 'true';
    const event = expect(await call('/events', admin, 'POST', { name: `Synthetic template testing ${suffix}`, start_date: '2026-10-06', end_date: '2026-10-07', grant_self: true }), 201).event;
    events.push(event.id);
    const patient = expect(await call(`/events/${event.id}/patients`), 200).patients[0];
    const createPath = `/events/${event.id}/patients/${patient.id}/records`;
    const recordPath = record => `/events/${event.id}/records/${record.id}`;
    const bySpecialty = Object.fromEntries(catalog.templates.map(template => [template.specialty, template]));
    const dataBySpecialty = Object.fromEntries(catalog.templates.map(template => [template.specialty, fullData(template)]));
    let serial = 0;
    const draft = (specialty, data) => ({ specialty, data, synthetic_confirmed: true, idempotency_key: `template_${suffix}_${serial++}` });
    expect(await call(createPath, outsider, 'POST', draft('general', dataBySpecialty.general)), 403);
    expect(await call(`/events/${event.id}/grants`, admin, 'POST', { user_id: clinician.id, specialties: ['general'], can_write: true, can_finalize: true }), 200);
    expect(await call(createPath, clinician, 'POST', draft('dental', dataBySpecialty.dental)), 403);
    expect(await call(`/events/${event.id}/grants`, admin, 'POST', { user_id: clinician.id, specialties: ['general', 'dental', 'optometry', 'clearance'], can_write: true, can_finalize: true }), 200);
    const invalid = [
      ['general', { ...dataBySpecialty.general, template_version: 1 }, 'INVALID_CLINICAL_TEMPLATE'],
      ['general', { ...dataBySpecialty.general, template_version: '2' }, 'INVALID_CLINICAL_TEMPLATE'],
      ['general', dataBySpecialty.dental, 'INVALID_CLINICAL_TEMPLATE'],
      ['general', { ...dataBySpecialty.general, template_fields: { bps: '120' } }, 'INVALID_DATA'],
      ['dental', { ...dataBySpecialty.dental, template_fields: { dental_hypertension: 'sometimes' } }, 'INVALID_DATA'],
      ['general', { ...dataBySpecialty.general, template_fields: { unknown_answer: 'Fictional' } }, 'INVALID_DATA'],
      ['general', { ...dataBySpecialty.general, assessment: 'Mixed schema answer' }, 'INVALID_DATA'],
      ['general', { template_id: bySpecialty.general.id, template_fields: {} }, 'INVALID_DATA'],
      ['general', { ...dataBySpecialty.general, not_assessed: ['invented-section'] }, 'INVALID_NOT_ASSESSED'],
    ];
    for (const [specialty, data, error] of invalid) expect(await call(createPath, clinician, 'POST', draft(specialty, data)), 400, error);
    expect(await call(createPath, clinician, 'POST', draft('general', oversizedData(bySpecialty.general))), 413, 'CLINICAL_RECORD_TOO_LARGE');
    expect(await call(createPath, clinician, 'POST', { ...draft('general', dataBySpecialty.general), is_test: false }), 403, 'REAL_DATA_FORBIDDEN');
    const records = {};
    for (const template of catalog.templates) {
      const created = expect(await call(createPath, clinician, 'POST', draft(template.specialty, dataBySpecialty[template.specialty])), 201).record;
      assert.deepEqual(created.data, dataBySpecialty[template.specialty]);
      assert.deepEqual(expect(await call(recordPath(created), clinician), 200).record.data, dataBySpecialty[template.specialty]);
      records[template.specialty] = created;
      const empty = { template_version: 2, template_id: template.id, template_fields: {} };
      const incomplete = expect(await call(createPath, clinician, 'POST', draft(template.specialty, empty)), 201).record;
      expect(await call(recordPath(incomplete) + '/finalize', clinician, 'POST', { revision: incomplete.revision, synthetic_confirmed: true, idempotency_key: `incomplete_${suffix}_${template.specialty}` }), 400, 'CLINICAL_TEMPLATE_REVIEW_REQUIRED');
    }
    assert.equal(nativePatientCalls, 0, 'Missing required narratives must fail before native work');
    const legacyData = { assessment: 'Fictional original v1 assessment', plan: 'Fictional original v1 plan' };
    const legacy = expect(await call(createPath, clinician, 'POST', draft('general', legacyData)), 201).record;
    expect(await call(recordPath(records.general), clinician, 'PATCH', { revision: 1, data: legacyData, synthetic_confirmed: true }), 409, 'CLINICAL_TEMPLATE_MISMATCH');
    expect(await call(recordPath(legacy), clinician, 'PATCH', { revision: 1, data: dataBySpecialty.general, synthetic_confirmed: true }), 409, 'CLINICAL_TEMPLATE_MISMATCH');
    expect(await call(recordPath(records.general), clinician, 'PATCH', { revision: 1, data: { ...dataBySpecialty.general, template_version: 3 }, synthetic_confirmed: true }), 400, 'INVALID_CLINICAL_TEMPLATE');
    const updatedData = { ...dataBySpecialty.general, template_fields: { ...dataBySpecialty.general.template_fields, soap_subjective: 'Fictional updated source narrative' } };
    records.general = expect(await call(recordPath(records.general), clinician, 'PATCH', { revision: 1, data: updatedData, synthetic_confirmed: true }), 200).record;
    dataBySpecialty.general = updatedData;
    for (const [specialty, record] of Object.entries(records)) {
      const finalized = expect(await call(recordPath(record) + '/finalize', clinician, 'POST', { revision: record.revision, synthetic_confirmed: true, idempotency_key: `final_template_${suffix}_${specialty}` }), 200).record;
      assert.equal(finalized.status, 'final'); assert.equal(finalized.sync_status, 'synced');
      assert.deepEqual(finalized.data, dataBySpecialty[specialty]);
      const native = nativeWrites.get(`final_${record.id}`);
      assert.equal(native.payload.schema_version, 2);
      assert.deepEqual(native.payload.data, dataBySpecialty[specialty], 'Every clinical source answer reaches the native bridge');
      assert.equal(native.actor.id, clinician.id);
      records[specialty] = finalized;
      expect(await call(recordPath(finalized), clinician, 'PATCH', { revision: finalized.revision, data: dataBySpecialty[specialty], synthetic_confirmed: true }), 409, 'FINAL_RECORD_IMMUTABLE');
    }
    expect(await call(recordPath(legacy) + '/finalize', clinician, 'POST', { revision: legacy.revision, synthetic_confirmed: true, idempotency_key: `legacy_final_${suffix}` }), 200);
    assert.equal(nativeWrites.get(`final_${legacy.id}`).payload.schema_version, 1);
    const original = records.general;
    expect(await call(recordPath(original) + '/amendments', clinician, 'POST', { data: legacyData, reason: 'Fictional correction', synthetic_confirmed: true, idempotency_key: `bad_amendment_${suffix}` }), 409, 'CLINICAL_TEMPLATE_MISMATCH');
    const amendmentData = { ...updatedData, template_fields: { ...updatedData.template_fields, soap_plan: 'Fictional corrected v2 plan' } };
    const amendment = expect(await call(recordPath(original) + '/amendments', clinician, 'POST', { data: amendmentData, reason: 'Fictional correction', synthetic_confirmed: true, idempotency_key: `amendment_${suffix}` }), 201).record;
    assert.equal(amendment.supersedes_record_id, original.id);
    assert.deepEqual(amendment.data, amendmentData);
    expect(await call(recordPath(amendment) + '/finalize', clinician, 'POST', { revision: amendment.revision, synthetic_confirmed: true, idempotency_key: `amendment_final_${suffix}` }), 200);
    assert.deepEqual(expect(await call(recordPath(original), clinician), 200).record.data, updatedData, 'An amendment cannot overwrite its source');
    const revisions = expect(await call(recordPath(original) + '/revisions', clinician), 200).revisions;
    assert.equal(revisions.length, 3);
    assert.equal(revisions[0].data.template_fields.soap_subjective, fullData(bySpecialty.general).template_fields.soap_subjective);
    assert.equal(revisions[1].data.template_fields.soap_subjective, updatedData.template_fields.soap_subjective);
    assert.equal(revisions[2].status, 'final');
    const history = expect(await call(`/patients/${patient.id}/records/export`, clinician), 200);
    for (const record of Object.values(records)) assert.deepEqual(history.records.find(item => item.id === record.id).data, dataBySpecialty[record.specialty]);
    const fhir = await call(`/patients/${patient.id}/export/fhir`, clinician); expect(fhir, 200);
    assert.match(fhir.headers.get('content-type'), /^application\/fhir\+json\b/);
    const resources = fhir.data.entry.map(entry => entry.resource), questionnaires = resources.filter(resource => resource.resourceType === 'Questionnaire');
    assert.deepEqual(new Set(questionnaires.map(questionnaire => questionnaire.version)), new Set(['1.0.0', '2.0.0']));
    for (const qr of resources.filter(resource => resource.resourceType === 'QuestionnaireResponse')) {
      const [url, version] = qr.questionnaire.split('|');
      const definition = questionnaires.find(questionnaire => questionnaire.url === url && questionnaire.version === version);
      assert.ok(definition, `The response ${qr.identifier.value} resolves to its exact questionnaire version`);
      if (qr.identifier.value === legacy.id) assert.equal(version, '1.0.0'); else assert.equal(version, '2.0.0');
      if (qr.identifier.value === amendment.id) assert.equal(qr.status, 'amended');
      const source = Object.values(records).find(record => record.id === qr.identifier.value);
      if (source) {
        const answers = new Map(flatten(qr.item).map(item => [item.linkId, item.answer?.[0]]));
        for (const field of fields(bySpecialty[source.specialty])) {
          const actual = answers.get(field.key), expected = dataBySpecialty[source.specialty].template_fields[field.key];
          assert.ok(actual, `FHIR retained ${field.key}`);
          if (field.type === 'select') assert.equal(actual.valueCoding.code, `${field.key}--${expected}`);
          else assert.equal(Object.values(actual)[0], expected, field.key);
        }
      }
    }
    const [[audits]] = await db.query("SELECT COUNT(*) total FROM clinical_sandbox_audit WHERE event_id=? AND action='history.fhir.export'", [event.id]);
    assert.equal(audits.total, 1);
    expect(await call(`/events/${event.id}/grants/${clinician.id}`, admin, 'DELETE'), 200);
    expect(await call(`/patients/${patient.id}/export/fhir`, clinician), 403);
    expect(await call(recordPath(records.general), clinician), 403);
    await db.query("UPDATE user SET enabled='N' WHERE id=?", [outsider.id]);
    expect(await call('/templates', outsider), 403, 'FORBIDDEN');
    const [[leaks]] = await db.query('SELECT COUNT(*) total FROM health_event WHERE slug LIKE ?', [`%${suffix}%`]);
    assert.equal(leaks.total, 0, 'Template tests never create a public health event');
  } finally {
    if (server) await new Promise(resolve => server.close(resolve));
    if (events.length) await db.query('DELETE FROM clinical_sandbox_event WHERE id IN (?)', [events]);
    if (users.length) {
      await db.query('DELETE FROM clinical_sandbox_audit WHERE actor_user_id IN (?)', [users.map(user => user.id)]);
      await db.query('DELETE FROM user WHERE id IN (?)', [users.map(user => user.id)]);
    }
    await db.end();
  }
});
