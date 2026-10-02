'use strict';
const crypto = require('node:crypto');
const sharp = require('sharp');
const SPECIALTIES = ['general', 'dental', 'optometry', 'clearance'];
class ClinicalError extends Error { constructor(code, status = 400) { super(code); this.code = code; this.status = status; } }
function fail(code = 'INVALID_DATA', status = 400) { throw new ClinicalError(code, status); }
function text(value, max = 4000, required = false) {
  if (value == null && !required) return null;
  if (typeof value !== 'string' || Buffer.byteLength(value, 'utf8') > max || (required && !value.trim()) || /[\x00-\x08\x0b\x0c\x0e-\x1f]/.test(value)) fail();
  return value.trim();
}
function date(value, required = false) {
  if (value == null || value === '') { if (required) fail(); return null; }
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value) || !Number.isFinite(Date.parse(value)) || new Date(value).toISOString().slice(0, 10) !== value) fail();
  return value;
}
function keys(value, allowed) { if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).some(k => !allowed.includes(k))) fail(); }
function number(value, min, max, integer = false) {
  if (value == null || value === '') return null;
  if (typeof value !== 'number' || !Number.isFinite(value) || value < min || value > max || (integer && !Number.isInteger(value))) fail();
  return value;
}
function choice(value, allowed) { if (value == null || value === '') return null; if (!allowed.includes(value)) fail(); return value; }
function clinicalData(specialty, input) {
  if (!SPECIALTIES.includes(specialty)) fail('INVALID_SPECIALTY');
  const common = ['complaint','findings','assessment','plan','referral','follow_up_date','medications','allergies','not_assessed'];
  const extra = {general:['vitals'],dental:['tooth_notation','teeth','pain_score'],optometry:['right_eye','left_eye','pupillary_distance_mm','acuity_context'],clearance:['vitals','decision','restrictions','reason']}[specialty];
  keys(input, [...common, ...extra]);
  const output = {};
  for (const key of common.filter(k => !['follow_up_date','not_assessed'].includes(k))) if (key in input) output[key] = text(input[key]);
  if ('follow_up_date' in input) output.follow_up_date = date(input.follow_up_date);
  if ('not_assessed' in input) { if (!Array.isArray(input.not_assessed) || input.not_assessed.length > 30) fail(); output.not_assessed = input.not_assessed.map(v => text(v, 100, true)); }
  if (input.vitals != null) {
    const fields = {systolic_mm_hg:[1,400],diastolic_mm_hg:[1,300],pulse_bpm:[1,350],respiratory_rate_per_min:[1,150],temperature_celsius:[15,50],oxygen_saturation_pct:[0,100],weight_kg:[0.1,700],height_cm:[10,300],glucose_mg_dl:[1,2000]};
    keys(input.vitals, Object.keys(fields)); output.vitals = {};
    for (const [key, value] of Object.entries(input.vitals)) output.vitals[key] = number(value, ...fields[key]);
  }
  if (specialty === 'dental') {
    if ('tooth_notation' in input) output.tooth_notation = choice(input.tooth_notation, ['universal','fdi']);
    if ('pain_score' in input) output.pain_score = number(input.pain_score, 0, 10);
    if (input.teeth != null) {
      if (!Array.isArray(input.teeth) || input.teeth.length > 52 || !output.tooth_notation) fail();
      output.teeth = input.teeth.map(tooth => { keys(tooth, ['number','condition','treatment']); const n = text(tooth.number, 2, true);
        const valid = output.tooth_notation === 'universal' ? /^(?:[1-9]|[12][0-9]|3[0-2]|[A-T])$/.test(n) : /^(?:[1-4][1-8]|[5-8][1-5])$/.test(n);
        if (!valid) fail('INVALID_TOOTH'); return {number:n,condition:text(tooth.condition,1000),treatment:text(tooth.treatment,1000)}; });
    }
  }
  if (specialty === 'optometry') {
    for (const side of ['right_eye','left_eye']) if (input[side] != null) {
      const ranges = {sphere_diopters:[-40,40],cylinder_diopters:[-20,20],axis_degrees:[0,180],add_diopters:[-10,20],intraocular_pressure_mm_hg:[0,100]};
      keys(input[side], [...Object.keys(ranges),'visual_acuity_uncorrected','visual_acuity_corrected']); output[side] = {};
      for (const [key,value] of Object.entries(input[side])) output[side][key] = key.startsWith('visual_acuity') ? text(value,80) : number(value,...ranges[key]);
    }
    if ('pupillary_distance_mm' in input) output.pupillary_distance_mm = number(input.pupillary_distance_mm,15,100);
    if ('acuity_context' in input) output.acuity_context = choice(input.acuity_context,['distance','near','both']);
  }
  if (specialty === 'clearance') {
    if ('decision' in input) output.decision = choice(input.decision,['cleared','not_cleared','deferred']);
    for (const key of ['restrictions','reason']) if (key in input) output[key] = text(input[key],2000);
  }
  return output;
}
function idempotency(value) { if (typeof value !== 'string' || !/^[A-Za-z0-9_-]{8,100}$/.test(value)) fail('INVALID_IDEMPOTENCY_KEY'); return value; }
function synthetic(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) fail();
  if (body.synthetic_confirmed !== true && body.synthetic_confirmed !== 'true') fail('SYNTHETIC_CONFIRMATION_REQUIRED');
  if ((body.mode && body.mode !== 'synthetic') || body.is_test === false || body.is_synthetic === false || ['beneficiary_id','user_id','patient_uuid','openemr_patient_uuid'].some(k => k in body)) fail('REAL_DATA_FORBIDDEN',403);
}
function hash(value) { return crypto.createHash('sha256').update(typeof value === 'string' || Buffer.isBuffer(value) ? value : JSON.stringify(value)).digest('hex'); }
async function validateAttachment(file, options = {}) {
  if (!file || !file.buffer || !file.size || file.size > 5*1024*1024) fail('INVALID_ATTACHMENT');
  let buffer, mime, ext;
  if (file.buffer.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10])) || file.buffer.subarray(0,3).equals(Buffer.from([255,216,255]))) {
    const picture = sharp(file.buffer, {limitInputPixels:16000000, failOn:'warning'});
    const metadata = await picture.metadata().catch(() => fail('INVALID_ATTACHMENT'));
    if (!['jpeg','png'].includes(metadata.format) || (metadata.pages || 1) !== 1) fail('INVALID_ATTACHMENT');
    mime = metadata.format === 'png' ? 'image/png' : 'image/jpeg'; ext = metadata.format === 'png' ? 'png' : 'jpg';
    if (file.mimetype !== mime) fail('ATTACHMENT_TYPE_MISMATCH');
    buffer = await (ext === 'png' ? picture.png() : picture.jpeg({quality:92})).toBuffer().catch(() => fail('INVALID_ATTACHMENT'));
  } else if (file.buffer.subarray(0,5).toString() === '%PDF-') {
    // These bytes are still quarantined in memory. OpenEMR must scan them before storing/releasing them.
    if (!options.pdfScannerAvailable) fail('PDF_SCANNER_UNAVAILABLE',503);
    if (file.mimetype !== 'application/pdf') fail('ATTACHMENT_TYPE_MISMATCH');
    const source=file.buffer.toString('latin1').replace(/#([a-fA-F0-9]{2})/g,(_,hex)=>String.fromCharCode(parseInt(hex,16)));
    // Object streams can hide action dictionaries from a byte-level inspection.
    // This pilot accepts static PDFs only; complex/interactive PDFs must be flattened or sent as images.
    if (!/^%PDF-(1\.[0-7]|2\.0)/.test(source) || !/%%EOF\s*$/.test(source) || /\/(?:JavaScript|JS|Launch|EmbeddedFile|EmbeddedFiles|RichMedia|XFA|OpenAction|AA|Encrypt|ObjStm|URI|GoToR|GoToE|SubmitForm|ImportData|Rendition|Movie|Sound|AcroForm|Collection)\b/.test(source)) fail('UNSAFE_PDF',415);
    buffer=file.buffer;mime='application/pdf';ext='pdf';
  } else fail('UNSUPPORTED_ATTACHMENT_TYPE',415);
  if (buffer.length > 5*1024*1024) fail('ATTACHMENT_TOO_LARGE',413);
  return {buffer,mime_type:mime,filename:`synthetic-attachment.${ext}`,size_bytes:buffer.length,sha256:hash(buffer)};
}
module.exports = { SPECIALTIES, ClinicalError, fail, text, date, keys, number, choice, clinicalData, idempotency, synthetic, hash, validateAttachment };
