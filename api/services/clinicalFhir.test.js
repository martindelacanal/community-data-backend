'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const { createFhirBundle, SYSTEM } = require('./clinicalFhir');

const EXPORTED_AT = '2026-10-05T18:45:00.000Z';
const patient = {
  id: 17, event_id: 9, is_synthetic: 1, synthetic_code: 'CPTEST-fhir-unit-17',
  display_name: 'Alex <Fictitious> & Example', date_of_birth: '1990-03-12', sex: 'Unknown',
};
const common = {
  complaint: 'Fictional preventive visit', findings: 'Recorded finding',
  assessment: 'Uncoded assessment', plan: 'Clinician plan text', referral: 'Referral text',
  follow_up_date: '2026-12-01', medications: 'Medication narrative only',
  allergies: 'Allergy narrative only', not_assessed: ['clinical_history'],
};
const vitals = {
  systolic_mm_hg: 120, diastolic_mm_hg: 80, pulse_bpm: 65,
  respiratory_rate_per_min: 16, temperature_celsius: 36.5,
  oxygen_saturation_pct: 0, weight_kg: 70.2, height_cm: 175, glucose_mg_dl: 91,
};
function record(specialty, data = {}, extra = {}) {
  return {
    id: `record-${specialty}`, patient_id: patient.id, event_id: patient.event_id,
    specialty, status: 'final', revision: 1, recorded_by: 41, finalized_by: 42,
    created_at: '2026-10-05T14:00:00.000Z', updated_at: '2026-10-05T15:00:00.000Z',
    data, attachments: [], ...extra,
  };
}
function bundle(records, options = {}) {
  return createFhirBundle({ patient, records, exportedAt: EXPORTED_AT, exporterId: 43, ...options });
}
function resources(value, type) {
  return value.entry.map(entry => entry.resource).filter(resource => resource.resourceType === type);
}
function byCode(value, code) {
  return resources(value, 'Observation').filter(resource => resource.code.coding.some(item => item.code === code));
}
function response(value, recordId) {
  return resources(value, 'QuestionnaireResponse').find(resource => resource.identifier.value === recordId);
}
function resolved(value, reference) {
  const entry = value.entry.find(item => item.fullUrl === reference.reference);
  assert.ok(entry, `Expected resolved reference ${reference.reference}`);
  return entry.resource;
}
function answer(items, linkId) {
  const item = items.find(candidate => candidate.linkId === linkId);
  assert.ok(item, `Expected response item ${linkId}`);
  return item.answer;
}
function walk(value, visit) {
  if (value && typeof value === 'object') {
    visit(value);
    for (const child of Object.values(value)) walk(child, visit);
  }
}
function assertClinicalError(operation, code, status) {
  assert.throws(operation, error => error.code === code && error.status === status);
}

test('all four specialties preserve every shared source answer with its original meaning', () => {
  const source = ['general', 'dental', 'optometry', 'clearance'].map(specialty => record(specialty, common));
  const result = bundle(source);
  assert.equal(result.resourceType, 'Bundle');
  assert.equal(result.type, 'collection');
  assert.equal(result.timestamp, EXPORTED_AT);
  assert.equal(resources(result, 'Questionnaire').length, 4);
  assert.equal(resources(result, 'QuestionnaireResponse').length, 4);
  for (const item of source) {
    const qr = response(result, item.id);
    assert.equal(qr.status, 'completed');
    for (const key of ['complaint', 'findings', 'assessment', 'plan', 'referral', 'medications', 'allergies']) {
      assert.deepEqual(answer(qr.item, key), [{ valueString: common[key] }]);
    }
    assert.deepEqual(answer(qr.item, 'follow_up_date'), [{ valueDate: '2026-12-01' }]);
    assert.deepEqual(answer(qr.item, 'not_assessed'), [{ valueString: 'clinical_history' }]);
    const [url, version] = qr.questionnaire.split('|');
    assert.ok(resources(result, 'Questionnaire').some(q => q.url === url && q.version === version));
  }
  const p = resources(result, 'Patient')[0];
  assert.equal(p.identifier[0].value, patient.synthetic_code);
  assert.equal(p.name[0].text, patient.display_name);
  assert.equal(p.birthDate, patient.date_of_birth);
  assert.equal(p.gender, 'unknown');
  assert.match(p.text.div, /Alex &lt;Fictitious&gt; &amp; Example/);
});

test('general measurements use specific LOINC codes and UCUM units without discarding zero', () => {
  const result = bundle([record('general', { ...common, vitals })]);
  const qr = response(result, 'record-general');
  const group = qr.item.find(item => item.linkId === 'vitals');
  for (const [key, value] of Object.entries(vitals)) assert.deepEqual(answer(group.item, key), [{ valueDecimal: value }]);
  const bp = byCode(result, '85354-9')[0];
  assert.equal(bp.component.length, 2);
  assert.deepEqual(bp.component.map(component => [component.code.coding[0].code, component.valueQuantity.value]), [['8480-6', 120], ['8462-4', 80]]);
  for (const component of bp.component) {
    assert.equal(component.valueQuantity.system, 'http://unitsofmeasure.org');
    assert.equal(component.valueQuantity.code, 'mm[Hg]');
  }
  for (const [code, value, unit] of [
    ['8867-4', 65, '/min'], ['9279-1', 16, '/min'], ['8310-5', 36.5, 'Cel'],
    ['2708-6', 0, '%'], ['29463-7', 70.2, 'kg'], ['8302-2', 175, 'cm'],
    ['glucose_mg_dl', 91, 'mg/dL'],
  ]) {
    const observation = byCode(result, code)[0];
    assert.ok(observation, `Expected observation ${code}`);
    assert.deepEqual(observation.valueQuantity, { value, unit, system: 'http://unitsofmeasure.org', code: unit });
  }
  // The source does not identify the glucose specimen/method; no specific LOINC is invented.
  assert.equal(byCode(result, 'glucose_mg_dl')[0].code.coding[0].system, SYSTEM);
  for (const code of ['85354-9', '8867-4', '9279-1', '8310-5', '2708-6', '29463-7', '8302-2']) {
    const observation = byCode(result, code)[0];
    assert.ok(observation.category.some(category => category.coding.some(item => item.system === 'http://terminology.hl7.org/CodeSystem/observation-category' && item.code === 'vital-signs')));
    assert.equal(observation.effectiveDateTime, undefined);
    assert.ok(observation.effectivePeriod.extension.some(extension => extension.url === 'http://hl7.org/fhir/StructureDefinition/data-absent-reason' && extension.valueCode === 'unknown'));
  }
});

test('a partial blood pressure does not manufacture the missing measurement', () => {
  const result = bundle([record('general', { vitals: { systolic_mm_hg: 115, diastolic_mm_hg: null } })]);
  const bp = byCode(result, '85354-9')[0];
  assert.equal(bp.component[0].valueQuantity.value, 115);
  assert.equal(bp.component[1].valueQuantity, undefined);
  assert.equal(bp.component[1].dataAbsentReason.coding[0].code, 'unknown');
  assert.equal(resources(result, 'Observation').length, 1);
});

test('dental output preserves tooth notation, repeated teeth, free text and a zero pain score', () => {
  for (const [notation, numbers] of [['universal', ['1', 'A']], ['fdi', ['11', '51']]]) {
    const teeth = numbers.map((number, index) => ({ number, condition: `Fictional finding ${index}`, treatment: `Fictional treatment ${index}` }));
    const result = bundle([record('dental', { ...common, tooth_notation: notation, teeth, pain_score: 0 })]);
    const qr = response(result, 'record-dental');
    assert.equal(answer(qr.item, 'tooth_notation')[0].valueCoding.code, notation);
    assert.equal(answer(qr.item, 'pain_score')[0].valueDecimal, 0);
    const toothGroups = qr.item.filter(item => item.linkId === 'teeth');
    assert.equal(toothGroups.length, 2);
    teeth.forEach((tooth, index) => {
      for (const key of ['number', 'condition', 'treatment']) assert.deepEqual(answer(toothGroups[index].item, key), [{ valueString: tooth[key] }]);
    });
    assert.deepEqual(byCode(result, 'pain_score')[0].valueQuantity, { value: 0 });
    assert.equal(byCode(result, 'pain_score')[0].code.coding[0].system, SYSTEM);
  }
});

test('optometry keeps the two eyes separate, preserves negative and zero values and all measurements', () => {
  const eyes = {
    right_eye: { visual_acuity_uncorrected: '20/40', visual_acuity_corrected: '20/20', sphere_diopters: -2.5, cylinder_diopters: -0.75, axis_degrees: 0, add_diopters: -1, intraocular_pressure_mm_hg: 0 },
    left_eye: { visual_acuity_uncorrected: '20/60', visual_acuity_corrected: '20/25', sphere_diopters: 1.25, cylinder_diopters: 0, axis_degrees: 180, add_diopters: 2, intraocular_pressure_mm_hg: 17 },
  };
  const result = bundle([record('optometry', { ...common, ...eyes, pupillary_distance_mm: 62.5, acuity_context: 'both' })]);
  const qr = response(result, 'record-optometry');
  const units = { sphere_diopters: '[diop]', cylinder_diopters: '[diop]', axis_degrees: 'deg', add_diopters: '[diop]', intraocular_pressure_mm_hg: 'mm[Hg]' };
  for (const [side, values] of Object.entries(eyes)) {
    const group = qr.item.find(item => item.linkId === side);
    for (const [key, value] of Object.entries(values)) {
      const observation = byCode(result, key).find(item => item.bodySite.coding[0].code === side);
      assert.ok(observation, `Expected ${side} ${key}`);
      assert.equal(observation.bodySite.coding[0].system, SYSTEM);
      assert.deepEqual(answer(group.item, `${side}.${key}`), [{ [typeof value === 'number' ? 'valueDecimal' : 'valueString']: value }]);
      if (typeof value === 'number') assert.deepEqual(observation.valueQuantity, { value, unit: units[key], system: 'http://unitsofmeasure.org', code: units[key] });
      else assert.equal(observation.valueString, value);
    }
  }
  assert.equal(byCode(result, 'pupillary_distance_mm')[0].valueQuantity.value, 62.5);
  assert.equal(byCode(result, 'pupillary_distance_mm')[0].valueQuantity.code, 'mm');
  assert.equal(answer(qr.item, 'pupillary_distance_mm')[0].valueDecimal, 62.5);
  assert.equal(answer(qr.item, 'acuity_context')[0].valueCoding.code, 'both');
  assert.equal(resources(result, 'Observation').length, 15);
});

test('clearance preserves decision, reason, restrictions and vital fields as recorded', () => {
  const data = { ...common, vitals, decision: 'deferred', restrictions: 'Fictional restriction', reason: 'Fictional reason' };
  const result = bundle([record('clearance', data)]);
  const qr = response(result, 'record-clearance');
  assert.equal(answer(qr.item, 'decision')[0].valueCoding.code, 'deferred');
  assert.deepEqual(answer(qr.item, 'restrictions'), [{ valueString: data.restrictions }]);
  assert.deepEqual(answer(qr.item, 'reason'), [{ valueString: data.reason }]);
  const group = qr.item.find(item => item.linkId === 'vitals');
  for (const [key, value] of Object.entries(vitals)) assert.deepEqual(answer(group.item, key), [{ valueDecimal: value }]);
  assert.equal(resources(result, 'Encounter')[0].status, 'unknown');
});

test('empty and null answers do not become zero, negative findings or clinical assertions', () => {
  const result = bundle([
    record('general', { complaint: null, allergies: '', medications: null, follow_up_date: null, vitals: { systolic_mm_hg: null, pulse_bpm: null }, not_assessed: ['allergies', 'medications'] }),
    record('optometry', { right_eye: { sphere_diopters: null, visual_acuity_corrected: '' }, left_eye: null, pupillary_distance_mm: null }),
    record('dental', { pain_score: null, tooth_notation: null, teeth: null }),
    record('clearance', { decision: null, reason: null, restrictions: null }),
  ]);
  assert.equal(resources(result, 'Observation').length, 0);
  for (const qr of resources(result, 'QuestionnaireResponse')) {
    walk(qr.item, object => {
      if (object.answer) assert.ok(object.linkId === 'not_assessed', `Invented answer for ${object.linkId}`);
      for (const [key, value] of Object.entries(object)) {
        if (key.startsWith('value')) assert.notEqual(value, null);
      }
    });
  }
  assert.deepEqual(answer(response(result, 'record-general').item, 'not_assessed'), [{ valueString: 'allergies' }, { valueString: 'medications' }]);
});

test('every internal reference resolves once and resource identifiers remain stable across exports', () => {
  const source = [record('general', { vitals }), record('optometry', { right_eye: { sphere_diopters: -2 } })];
  const first = bundle(source);
  const second = bundle([...source].reverse(), { exportedAt: '2026-10-06T18:45:00.000Z' });
  for (const output of [first, second]) {
    const urls = new Set(output.entry.map(entry => entry.fullUrl));
    assert.equal(urls.size, output.entry.length);
    for (const entry of output.entry) {
      assert.equal(entry.fullUrl, `urn:uuid:${entry.resource.id}`);
      walk(entry.resource, object => {
        if (object.reference) {
          assert.match(object.reference, /^urn:uuid:[0-9a-f-]{36}$/);
          assert.ok(urls.has(object.reference), `Dangling reference ${object.reference}`);
        }
      });
    }
    for (const q of resources(output, 'Questionnaire')) {
      const links = [];
      walk(q.item, object => { if (object.linkId) links.push(object.linkId); });
      assert.equal(links.length, new Set(links).size, 'Questionnaire linkIds must be globally unique');
    }
  }
  for (const type of ['Patient', 'Encounter', 'Questionnaire', 'QuestionnaireResponse', 'Observation', 'Practitioner', 'CodeSystem']) {
    assert.deepEqual(resources(first, type).map(item => item.id).sort(), resources(second, type).map(item => item.id).sort());
  }
  assert.notEqual(first.id, second.id, 'Each export is independently identifiable');
  assert.deepEqual(bundle(source), first, 'Same source and export instant give a deterministic bundle');
});

test('draft and final status describe documentation, and amendments keep the original encounter and provenance', () => {
  const original = record('general', { vitals: { pulse_bpm: 70 } });
  const amendment = record('general', { vitals: { pulse_bpm: 72 } }, { id: 'record-amendment', supersedes_record_id: original.id, amendment_reason: 'Fictional correction', revision: 2, recorded_by: 44, finalized_by: 45 });
  const draft = record('dental', { pain_score: 0 }, { status: 'draft', finalized_by: null });
  const result = bundle([original, amendment, draft]);
  assert.equal(resources(result, 'Encounter').length, 2);
  assert.ok(resources(result, 'Encounter').every(encounter => encounter.status === 'unknown' && encounter.period === undefined));
  const originalQr = response(result, original.id), amendmentQr = response(result, amendment.id), draftQr = response(result, draft.id);
  assert.equal(originalQr.status, 'completed');
  assert.equal(amendmentQr.status, 'amended');
  assert.equal(draftQr.status, 'in-progress');
  assert.deepEqual(originalQr.encounter, amendmentQr.encounter);
  const amendmentProvenance = resources(result, 'Provenance').find(item => item.entity);
  assert.equal(amendmentProvenance.entity[0].role, 'revision');
  assert.equal(amendmentProvenance.entity[0].what.reference, `urn:uuid:${originalQr.id}`);
  assert.equal(amendmentProvenance.reason[0].text, 'Fictional correction');
  for (const [qr, expected] of [[originalQr, 'final'], [amendmentQr, 'amended'], [draftQr, 'preliminary']]) {
    const observations = resources(result, 'Observation').filter(item => item.derivedFrom[0].reference === `urn:uuid:${qr.id}`);
    assert.ok(observations.length);
    assert.ok(observations.every(item => item.status === expected));
    const provenance = resources(result, 'Provenance').find(item => item.target.some(target => target.reference === `urn:uuid:${qr.id}`) && item.agent.some(agent => agent.type));
    assert.equal(provenance.agent.some(agent => agent.type?.coding[0].code === 'verifier'), qr.status !== 'in-progress');
  }
});

test('attachments include the exact bytes, size, MIME type and base64 SHA-1 required by FHIR R4', () => {
  const buffer = Buffer.from([0, 1, 127, 128, 254, 255]);
  const attachment = { id: 'attachment-1', buffer, size_bytes: buffer.length, sha256: crypto.createHash('sha256').update(buffer).digest('hex'), mime_type: 'application/pdf', filename: 'fictional-sample.pdf', created_at: '2026-10-05T15:30:00.000Z', uploaded_by: 46 };
  const result = bundle([record('general', {}, { attachments: [attachment] })]);
  const document = resources(result, 'DocumentReference')[0];
  const exported = document.content[0].attachment;
  assert.deepEqual(Buffer.from(exported.data, 'base64'), buffer);
  assert.equal(exported.size, buffer.length);
  assert.equal(exported.contentType, attachment.mime_type);
  assert.equal(exported.title, attachment.filename);
  assert.equal(exported.creation, undefined, 'An upload timestamp is not the file creation timestamp');
  assert.equal(document.author, undefined, 'The uploader is not necessarily the author of the attached clinical document');
  assert.equal(document.date, attachment.created_at);
  assert.equal(exported.hash, crypto.createHash('sha1').update(buffer).digest('base64'));
  assert.equal(Buffer.from(exported.hash, 'base64').length, 20);
  assert.equal(exported.url, undefined);
  assert.equal(document.context.related[0].reference, `urn:uuid:${response(result, 'record-general').id}`);
  const provenance = resources(result, 'Provenance').find(item => item.target.some(target => target.reference === `urn:uuid:${document.id}`));
  assert.ok(provenance);
  const uploadProvenance = resources(result, 'Provenance').find(item => item.activity?.text === 'Attachment upload' && item.target.some(target => target.reference === `urn:uuid:${document.id}`));
  assert.ok(uploadProvenance, 'Upload provenance must identify who uploaded the document and when');
  assert.equal(uploadProvenance.recorded, attachment.created_at);
  assert.ok(uploadProvenance.agent.some(agent => resolved(result, agent.who).identifier.some(identifier => identifier.value === String(attachment.uploaded_by))));
});

test('current content author and original recorder remain distinct from the finalizer', () => {
  const source = record('general', common, {
    recorded_by: 101, content_author_id: 104, finalized_by: 107,
    content_authored_at: '2026-10-05T14:30:00.000Z',
  });
  const result = bundle([source]);
  const qr = response(result, source.id);
  assert.equal(resolved(result, qr.author).identifier[0].value, '104');
  assert.equal(qr.authored, source.content_authored_at);
  assert.notEqual(qr.authored, source.updated_at, 'Finalizing a record does not establish when its clinical content was authored');
  const provenance = resources(result, 'Provenance').find(item => item.target.some(target => target.reference === `urn:uuid:${qr.id}`) && item.agent.some(agent => agent.type?.coding?.some(coding => coding.code === 'author')));
  assert.ok(provenance);
  for (const [role, userId] of [['enterer', '101'], ['author', '104'], ['verifier', '107']]) {
    const agent = provenance.agent.find(item => item.type?.coding?.some(coding => coding.code === role));
    assert.ok(agent, `Expected provenance ${role}`);
    assert.equal(resolved(result, agent.who).identifier[0].value, userId);
  }
});

test('legacy records use the original recorder and creation date when content authorship is unavailable', () => {
  const source = record('general', common, { recorded_by: 101, finalized_by: 107 });
  const result = bundle([source]);
  const qr = response(result, source.id);
  assert.equal(resolved(result, qr.author).identifier[0].value, '101');
  assert.equal(qr.authored, source.created_at);
  assert.notEqual(qr.authored, source.updated_at);
});

test('corrupt attachments are rejected instead of silently producing an incomplete history', () => {
  const buffer = Buffer.from('fictitious bytes');
  const valid = { id: 'attachment-1', buffer, size_bytes: buffer.length, sha256: crypto.createHash('sha256').update(buffer).digest('hex'), mime_type: 'image/png', filename: 'fictional.png', created_at: EXPORTED_AT, uploaded_by: 46 };
  for (const corruption of [{ buffer: 'not a buffer' }, { size_bytes: buffer.length + 1 }, { sha256: '0'.repeat(64) }]) {
    assertClinicalError(() => bundle([record('general', {}, { attachments: [{ ...valid, ...corruption }] })]), 'ATTACHMENT_INTEGRITY_ERROR', 502);
  }
});

test('real patient data and records from another patient or event are rejected', () => {
  for (const change of [{ is_synthetic: false }, { is_synthetic: 0 }, { synthetic_code: 'BENEFICIARY-17' }]) {
    assertClinicalError(() => bundle([], { patient: { ...patient, ...change } }), 'REAL_DATA_FORBIDDEN', 403);
  }
  for (const change of [{ patient_id: 18 }, { event_id: 10 }]) {
    assertClinicalError(() => bundle([record('general', {}, change)]), 'FHIR_SOURCE_PATIENT_MISMATCH', 500);
  }
});

test('unsupported statuses, invalid authors and broken revision chains fail closed', () => {
  assertClinicalError(() => bundle([record('general', {}, { status: 'signed' })]), 'FHIR_SOURCE_STATUS_INVALID', 500);
  assertClinicalError(() => bundle([record('general', {}, { recorded_by: 0 })]), 'FHIR_SOURCE_AUTHOR_INVALID', 500);
  assertClinicalError(() => bundle([record('general', {}, { finalized_by: null })]), 'FHIR_SOURCE_AUTHOR_INVALID', 500);
  assertClinicalError(() => bundle([], { exporterId: 'not-a-user' }), 'FHIR_SOURCE_AUTHOR_INVALID', 500);
  assertClinicalError(() => bundle([record('general', {}, { supersedes_record_id: 'missing' })]), 'FHIR_SOURCE_REVISION_INVALID', 500);
  assertClinicalError(() => bundle([record('general', {}, { supersedes_record_id: 'record-general' })]), 'FHIR_SOURCE_REVISION_INVALID', 500);
  assertClinicalError(() => bundle([record('general'), record('dental', {}, { supersedes_record_id: 'record-general' })]), 'FHIR_SOURCE_REVISION_INVALID', 500);
});

test('free-text narratives never create diagnoses, allergy assertions or prescriptions, and documentation time is not measurement time', () => {
  const result = bundle([record('general', JSON.stringify({ ...common, vitals }))]);
  for (const type of ['Condition', 'AllergyIntolerance', 'MedicationStatement', 'MedicationRequest', 'Procedure']) assert.equal(resources(result, type).length, 0);
  for (const observation of resources(result, 'Observation')) {
    assert.equal(observation.effectiveDateTime, undefined);
    assert.equal(observation.effectivePeriod?.start, undefined);
    assert.equal(observation.effectivePeriod?.end, undefined);
    assert.equal(observation.method, undefined);
    assert.equal(observation.issued, '2026-10-05T15:00:00.000Z');
    assert.match(observation.note[0].text, /documentation time/);
  }
  walk(result, object => {
    if (object.meta) assert.ok(object.meta.tag.some(tag => tag.system === SYSTEM && tag.code === 'synthetic'));
  });
});
