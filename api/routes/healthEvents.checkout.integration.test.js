'use strict';

/** KAN-17 integration checks use temporary fixtures in the development database only. */

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const moment = require('moment-timezone');

const RUN_INTEGRATION = process.env.RUN_HEALTH_SCAN_INTEGRATION === 'development';
const BACKEND_ROOT = path.resolve(__dirname, '..', '..');
const ENV_PATH = path.join(BACKEND_ROOT, '.env');

function normalizeEnvValue(value) {
  return String(value == null ? '' : value).trim().replace(/^(['"])(.*)\1$/, '$2').trim().toLowerCase();
}

function readDatabaseBlock(envText, heading, commentedAssignments) {
  const lines = String(envText).split(/\r?\n/);
  const headingPattern = new RegExp(`^\\s*#\\s*${heading}\\s*$`, 'i');
  const assignmentPattern = commentedAssignments
    ? /^\s*#\s*(DB_(?:HOST|USER|PASSWORD|DATABASE|PORT))\s*=\s*(.*?)\s*$/i
    : /^\s*(DB_(?:HOST|USER|PASSWORD|DATABASE|PORT))\s*=\s*(.*?)\s*$/i;
  const values = {};
  let inside = false;
  let foundAssignment = false;
  for (const line of lines) {
    if (!inside) {
      inside = headingPattern.test(line);
      continue;
    }
    const match = line.match(assignmentPattern);
    if (match) {
      values[match[1].toUpperCase()] = match[2].trim();
      foundAssignment = true;
      continue;
    }
    if (foundAssignment && line.trim() === '') break;
    if (foundAssignment && /^\s*#\s*[A-Z][A-Z ]+\s*$/i.test(line)) break;
  }
  return values;
}

function assertDevelopmentDatabaseTarget() {
  const envText = fs.readFileSync(ENV_PATH, 'utf8');
  const development = readDatabaseBlock(envText, 'DEVELOPMENT DATABASE', false);
  const production = readDatabaseBlock(envText, 'PRODUCTION DATABASE', true);
  const keys = ['DB_HOST', 'DB_USER', 'DB_DATABASE', 'DB_PORT'];
  const active = Object.fromEntries(keys.map(key => [key, process.env[key]]));
  if (normalizeEnvValue(active.DB_HOST) === normalizeEnvValue(production.DB_HOST)) {
    throw new Error('Integration test refused: the active database host matches the PRODUCTION DATABASE block.');
  }
  if (!keys.every(key => normalizeEnvValue(active[key]) === normalizeEnvValue(development[key]))) {
    throw new Error('Integration test refused: the active target does not match the DEVELOPMENT DATABASE block.');
  }
}

async function listen(app) {
  return new Promise((resolve, reject) => {
    const server = app.listen(0, '127.0.0.1');
    server.once('listening', () => resolve(server));
    server.once('error', reject);
  });
}

async function postJson(baseUrl, route, payload) {
  const response = await fetch(`${baseUrl}${route}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(payload)
  });
  const text = await response.text();
  let body = null;
  try { body = text ? JSON.parse(text) : null; } catch (error) { assert.fail(`Invalid JSON (HTTP ${response.status})`); }
  return { status: response.status, body };
}

test('KAN-17: atomic event exit, separate consent, family identities and concurrent duplicate prevention', {
  skip: RUN_INTEGRATION ? false : 'Set RUN_HEALTH_SCAN_INTEGRATION=development', timeout: 90000
}, async () => {
  require('dotenv').config({ path: ENV_PATH, quiet: true });
  assertDevelopmentDatabaseTarget();
  const pool = require('../connection/connection');
  const db = pool.promise();
  const emailPath = require.resolve('../email/email');
  require.cache[emailPath] = { id: emailPath, filename: emailPath, loaded: true, exports: {} };
  const express = require('express');
  const jwt = require('jsonwebtoken');
  const app = express(); app.use(express.json()); app.use('/api', require('./healthEvents'));
  const suffix = crypto.randomBytes(8).toString('hex');
  const slug = `qa-kan-${suffix}`;
  const lastName = 'Test ' + suffix.replace(/[0-9]/g, d => String.fromCharCode(97 + Number(d)));
  const usernames = [];
  let eventId, server;
  try {
    const [[location]] = await db.query('SELECT id FROM location LIMIT 1');
    const [event] = await db.query(`INSERT INTO health_event(slug,name_en,name_es,location_id,start_date,end_date,timezone,enabled,landing_enabled) VALUES (?,'QA checkout','QA salida',?,CURDATE(),DATE_ADD(CURDATE(),INTERVAL 1 DAY),'America/Los_Angeles','Y','N')`, [slug, location.id]);
    eventId = event.insertId;
    const [stand] = await db.query(`INSERT INTO health_event_stand(health_event_id,name_en,name_es,is_entry,has_checkout,enabled) VALUES (?,'QA Entry','QA Entrada','Y','N','Y')`, [eventId]);
    const [form] = await db.query(`INSERT INTO health_event_form(health_event_id,audience,title_en,title_es,required_before_qr,enabled) VALUES (?,'beneficiary','QA Survey','QA Encuesta','Y','Y')`, [eventId]);
    const [q1] = await db.query(`INSERT INTO health_event_question(form_id,question_type,name_en,name_es,required,enabled) VALUES (?,'single','QA Choice','QA Opción','Y','Y')`, [form.insertId]);
    const [o1] = await db.query(`INSERT INTO health_event_question_option(question_id,name_en,name_es,enabled) VALUES (?,'Yes','Sí','Y')`, [q1.insertId]);
    const [o2] = await db.query(`INSERT INTO health_event_question_option(question_id,name_en,name_es,enabled) VALUES (?,'No','No','Y')`, [q1.insertId]);
    const [q2] = await db.query(`INSERT INTO health_event_question(form_id,question_type,name_en,name_es,required,depends_on_question_id,depends_on_option_id,enabled) VALUES (?,'text','QA detail','QA detalle','Y',?,?,'Y')`, [form.insertId,q1.insertId,o1.insertId]);
    const validAnswers = [{ question_id:q1.insertId,answer:o2.insertId }];
    const operatorName = `qa_operator_${suffix}`; usernames.push(operatorName);
    const [operator] = await db.query(`INSERT INTO user(username,firstname,lastname,role_id,enabled,deleted) VALUES (?,'QA','Operator',1,'Y','N')`,[operatorName]);
    const token = jwt.sign({data:JSON.stringify({id:operator.insertId,role:'admin'})},process.env.JWT_SECRET);
    server = await listen(app); const base = `http://127.0.0.1:${server.address().port}/api/health-events`;
    const post = async (path, body, auth=token) => {
      const response = await fetch(base+path,{method:'POST',headers:{'Content-Type':'application/json',...(auth?{Authorization:`Bearer ${auth}`}:{})},body:JSON.stringify(body)});
      return { status:response.status, body:await response.json() };
    };
    const username = `qa_parent_${suffix}`; usernames.push(username);
    const account = { username, firstName:'Parent',lastName,dateOfBirth:'1985-04-14',phone:'5551234567' };
    assert.equal((await post(`/${slug}/register`,{account:{...account,firstName:'Irma123'},answers:validAnswers},null)).body.error,'INVALID_NAME_CHARACTERS');
    assert.equal((await post(`/${slug}/register`,{account:{...account,firstName:'Irma１２３'},answers:validAnswers},null)).body.error,'INVALID_NAME_CHARACTERS');
    const parent = await post(`/${slug}/register`,{account,answers:validAnswers},null);
    assert.equal(parent.status,200,JSON.stringify(parent.body));
    const [[parentUser]] = await db.query('SELECT id FROM user WHERE username=?',[username]);
    // Different contacts cannot evade the identity match; no record is merged or disclosed.
    const dup = await post(`/${slug}/register`,{account:{...account,username:undefined,phone:'5551234568'},answers:validAnswers},null);
    assert.equal(dup.status,409); assert.equal(dup.body.error,'PARTICIPANT_EXISTS');
    // A logged-in parent can register a child using the same phone, without modifying their own registration.
    const parentToken = jwt.sign({data:JSON.stringify({id:parentUser.id,role:'beneficiary'})},process.env.JWT_SECRET);
    const childName = `qa_child_${suffix}`; usernames.push(childName);
    const child = await post(`/${slug}/register`,{registering_for:'other',account:{...account,username:childName,firstName:'Child',dateOfBirth:'2015-04-14'},answers:validAnswers},parentToken);
    assert.equal(child.status,200,JSON.stringify(child.body)); assert.notEqual(child.body.registration_id,parent.body.registration_id); assert.equal(child.body.token,null);
    const parallelAccount={...account,username:undefined,firstName:'Parallel',dateOfBirth:'1990-01-01'};
    const attempts=await Promise.all(Array.from({length:5},(_,i)=>post(`/${slug}/register`,{account:{...parallelAccount,phone:`555123456${i}`},answers:validAnswers},null)));
    assert.equal(attempts.filter(a=>a.status===200).length,1);
    assert.equal(attempts.filter(a=>a.body.error==='PARTICIPANT_EXISTS').length,4);
    // Remove survey answers to simulate a walk-in with an incomplete questionnaire.
    await db.query('DELETE FROM health_event_answer WHERE registration_id=?',[parent.body.registration_id]);
    const scanBody={event_id:eventId,stand_id:stand.insertId,phone:account.phone,user_id:parentUser.id};
    assert.equal((await post('/scan',{...scanBody,event_checkout:true})).body.error,'EVENT_ENTRY_REQUIRED');
    const entry=await post('/scan',{...scanBody,confirmed:true}); assert.equal(entry.status,200,JSON.stringify(entry.body));
    const preview=await post('/scan',{...scanBody,event_checkout:true});
    assert.equal(preview.status,200,JSON.stringify(preview.body)); assert.equal(preview.body.pending_event_checkout,true);
    const checkout={event_id:eventId,stand_id:stand.insertId,registration_id:parent.body.registration_id,answers:[]};
    assert.equal((await post('/event-checkout',checkout)).status,400);
    assert.equal((await post('/event-checkout',{...checkout,answers:[{question_id:q1.insertId,answer:999999}]})).status,400);
    assert.equal((await post('/event-checkout',{...checkout,answers:[{question_id:q1.insertId,answer:o1.insertId}]})).status,400);
    const [[before]] = await db.query("SELECT COUNT(*) total FROM health_event_scan WHERE health_event_id=? AND scan_type='checkout'",[eventId]); assert.equal(before.total,0);
    const exits=await Promise.all(Array.from({length:4},()=>post('/event-checkout',{...checkout,answers:[{question_id:q1.insertId,answer:o1.insertId},{question_id:q2.insertId,answer:'Completed'}]})));
    exits.forEach(r=>assert.equal(r.status,200,JSON.stringify(r.body))); assert.equal(new Set(exits.map(r=>r.body.scan_id)).size,1);
    const [[after]]=await db.query("SELECT COUNT(*) total FROM health_event_scan WHERE health_event_id=? AND scan_type='checkout'",[eventId]); assert.equal(after.total,1);
    const replay=await post('/scan',{...scanBody,event_checkout:true});assert.equal(replay.body.pending_event_checkout,false);assert.equal(replay.body.duplicate,true);
    // Consent is additive to the original terms, and both are required.
    const volunteer={account:{firstName:'QA',lastName,email:`qa-${suffix}@example.invalid`,phone:'5551234567',legalConsentAccepted:true},answers:[]};
    assert.equal((await post(`/${slug}/register/volunteer`,volunteer,null)).body.error,'PRIVACY_CONSENT_REQUIRED');
    volunteer.account.privacyConsent={accepted:true,version:'2026-10-30',fullName:'QA Volunteer',role:'volunteer',deviceModel:'QA device'};
    assert.equal((await post(`/${slug}/register/volunteer`,{...volunteer,account:{...volunteer.account,legalConsentAccepted:false}},null)).body.error,'LEGAL_CONSENT_REQUIRED');
    const registeredVolunteer=await post(`/${slug}/register/volunteer`,volunteer,null);assert.equal(registeredVolunteer.status,200,JSON.stringify(registeredVolunteer.body));
    const [[consent]]=await db.query('SELECT document_version,full_name,accepted_at FROM volunteer_privacy_consent WHERE registration_id=?',[registeredVolunteer.body.registration_id]);
    assert.equal(consent.document_version,'2026-10-30');assert.equal(consent.full_name,'QA Volunteer');assert.ok(consent.accepted_at);
  } finally {
    if (server) await new Promise(resolve=>server.close(resolve));
    if(eventId) {
      const [users]=await db.query('SELECT user_id FROM health_event_registration WHERE health_event_id=?',[eventId]);
      await db.query('DELETE FROM volunteer_privacy_consent WHERE registration_id IN (SELECT id FROM health_event_registration WHERE health_event_id=?)',[eventId]);
      await db.query("DELETE FROM health_event_scan WHERE health_event_id=? AND scan_type='checkout'",[eventId]);
      await db.query('DELETE FROM health_event_scan WHERE health_event_id=?',[eventId]);
      await db.query('DELETE FROM health_event WHERE id=?',[eventId]);
      if(users.length) await db.query('DELETE FROM user WHERE id IN (?)',[users.map(u=>u.user_id)]);
    }
    if(usernames.length) await db.query('DELETE FROM user WHERE username IN (?)',[usernames]);
    await db.end();
  }
});
