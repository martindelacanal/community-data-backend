'use strict';
const test=require('node:test');const assert=require('node:assert/strict');const crypto=require('node:crypto');
const express=require('express');const jwt=require('jsonwebtoken');const mysql=require('mysql2/promise');const sharp=require('sharp');
const {databaseConfig}=require('../../scripts/migrateHealthWristbands');
const {createClinicalSandboxRouter}=require('./clinicalSandbox');
const {ClinicalError}=require('../services/clinicalValidation');
test('synthetic clinical pilot: DB authorization, isolation, revocation, concurrency, immutable finals, real bridge failures and protected attachments',{
 skip:process.env.RUN_CLINICAL_SANDBOX_INTEGRATION!=='development'?'Set RUN_CLINICAL_SANDBOX_INTEGRATION=development after migration':false,timeout:300000
},async()=>{
 const config=databaseConfig('development'),production=databaseConfig('production');
 assert.notEqual(config.host.toLowerCase(),production.host.toLowerCase(),'Development must be distinct from production');
 const db=mysql.createPool({...config,multipleStatements:false,connectionLimit:5,dateStrings:true});
 const suffix=crypto.randomBytes(8).toString('hex'),users=[],events=[];let server;
 const secret=crypto.randomBytes(32).toString('hex');const env={JWT_SECRET:secret,CLINICAL_SANDBOX_ENABLED:'true'};
 let failBridge=false,failAfterNativeCommit=false,scannerHealthy=true,nativeSaved=0;const attachments=new Map(),nativeRecords=new Map(),attachmentKeys=new Set();
 const adapter={configured:()=>true,status:async()=>({mode:'synthetic',attachments:{scanner:{healthy:scannerHealthy}}}),upsertPatient:async(p)=>{if(failBridge)throw new ClinicalError('OPENEMR_UNAVAILABLE',503);assert.match(p.synthetic_code,/^CPTEST-/);return {patient_uuid:`patient-${p.id}`};},createEncounter:async(p,r)=>({encounter_uuid:`encounter-${r.id}`}),saveRecord:async(p,actor)=>{
  assert.equal(p.is_test,true);assert.ok(actor.id);assert.equal(actor.can_finalize,true);assert.ok(actor.original_recorder.id);assert.match(p.external_record_id,/^CPTEST-/);assert.equal(p.idempotency_key,`final_${p.external_record_id.slice(7)}`);
  const payload=JSON.stringify([p,actor]),existing=nativeRecords.get(p.idempotency_key);if(existing){assert.equal(existing.payload,payload,'Native idempotency includes human actors');return existing.result;}
  if(p.supersedes_record_uuid){const parent=[...nativeRecords.values()].find(row=>row.result.record_uuid===p.supersedes_record_uuid);assert.ok(parent);assert.equal(p.encounter_uuid,parent.encounter_uuid,'Amendments must reuse the original native encounter');}
  const result={record_uuid:`record-${p.external_record_id}`};nativeRecords.set(p.idempotency_key,{payload,result,encounter_uuid:p.encounter_uuid});nativeSaved++;
  if(failAfterNativeCommit){failAfterNativeCommit=false;throw new ClinicalError('OPENEMR_UNAVAILABLE',503);}return result;
 },upload:async(r,p)=>{assert.match(p.idempotency_key,/^attach_[0-9a-f-]{36}_[0-9a-f]{64}$/);assert.equal(attachmentKeys.has(p.idempotency_key),false,'Different records must have distinct native attachment keys');attachmentKeys.add(p.idempotency_key);const id=crypto.randomUUID();attachments.set(id,Buffer.from(p.content_base64,'base64'));return {attachment_uuid:id};},download:async(r,id)=>attachments.get(id)};
 try{
  const [roles]=await db.query("SELECT id,name FROM role WHERE name IN ('admin','eventvolunteer','beneficiary')");const roleMap=Object.fromEntries(roles.map(r=>[r.name,r.id]));
  async function user(label,role){const [insert]=await db.query("INSERT INTO user(username,firstname,lastname,role_id,enabled,deleted,language) VALUES(?,?,?,?,'Y','N','en')",[`__clinical_${suffix}_${label}`,label,'Synthetic Pilot Test',roleMap[role]]);const u={id:insert.insertId,role};users.push(u);return u;}
  const admin=await user('Admin','admin'),vol=await user('Clinician','eventvolunteer'),outsider=await user('Other','eventvolunteer'),beneficiary=await user('Beneficiary','beneficiary');
  const tokens=new Map(users.map(u=>[u.id,jwt.sign({data:JSON.stringify({...u,role:'admin'})},secret,{expiresIn:'15m'})]));
  const app=express();app.use(express.json({limit:'80kb'}));app.use('/api/clinical-sandbox',createClinicalSandboxRouter({db,env,adapter}));
  server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));const base=`http://127.0.0.1:${server.address().port}/api/clinical-sandbox`;
  async function call(path,actor=admin,method='GET',body){const response=await fetch(base+path,{method,headers:{...(actor?{Authorization:`Bearer ${tokens.get(actor.id)}`}:{ }),...(body?{'Content-Type':'application/json'}:{})},body:body?JSON.stringify(body):undefined});const text=await response.text();let data;try{data=JSON.parse(text);}catch{data=text;}return {status:response.status,data,headers:response.headers};}
  assert.equal((await call('/events',null)).status,401);assert.equal((await call('/events',beneficiary)).status,403);
  env.CLINICAL_SANDBOX_ENABLED='false';assert.equal((await call('/events')).status,503);assert.equal((await call('/status')).data.enabled,false);env.CLINICAL_SANDBOX_ENABLED='true';
  assert.equal((await call('/events',admin,'POST',{name:'Synthetic test',start_date:'2026-10-02',end_date:'2026-10-03',mode:'real'})).status,403);
  async function event(label){const response=await call('/events',admin,'POST',{name:`Synthetic ${suffix} ${label}`,start_date:'2026-10-02',end_date:'2026-10-03',grant_self:true});assert.equal(response.status,201,JSON.stringify(response.data));events.push(response.data.event.id);return response.data.event;}
  const eventA=await event('A'),eventB=await event('B');
  const patientsA=await call(`/events/${eventA.id}/patients`);assert.equal(patientsA.status,200);assert.equal(patientsA.data.patients.length,3);assert.equal(patientsA.headers.get('cache-control'),'no-store, private');
  const patient=patientsA.data.patients[0],patientB=(await call(`/events/${eventB.id}/patients`)).data.patients[0];
  assert.equal((await call(`/events/${eventA.id}/patients`,vol)).status,403,'spoofed admin claim cannot bypass DB role');
  assert.equal((await call(`/events/${eventA.id}/grants`,vol)).status,403);
  const grant={user_id:vol.id,specialties:['general'],can_write:true,can_finalize:true};
  assert.equal((await call(`/events/${eventA.id}/grants`,admin,'POST',grant)).status,200);
  const createPath=`/events/${eventA.id}/patients/${patient.id}/records`;
  const draft={specialty:'general',data:{complaint:'Fictional training visit',assessment:'Synthetic assessment',plan:'Synthetic plan',vitals:{pulse_bpm:72}},idempotency_key:`draft_${suffix}`,synthetic_confirmed:true};
  assert.equal((await call(`/events/${eventA.id}/patients/${patientB.id}/records`,vol,'POST',draft)).status,404);
  assert.equal((await call(createPath,vol,'POST',{...draft,is_test:false})).status,403);
  assert.equal((await call(createPath,vol,'POST',{...draft,beneficiary_id:5})).status,403);
  assert.equal((await call(createPath,vol,'POST',{...draft,specialty:'dental'})).status,403);
  assert.equal((await call(createPath,vol,'POST',{...draft,synthetic_confirmed:false})).status,400);
  await db.query('UPDATE clinical_sandbox_patient SET is_synthetic=0 WHERE id=?',[patient.id]);assert.equal((await call(createPath,vol,'POST',draft)).status,404);await db.query('UPDATE clinical_sandbox_patient SET is_synthetic=1 WHERE id=?',[patient.id]);
  const created=await call(createPath,vol,'POST',draft);assert.equal(created.status,201,JSON.stringify(created.data));const record=created.data.record;
  assert.equal((await call(createPath,vol,'POST',draft)).data.record.id,record.id);
  assert.equal((await call(createPath,vol,'POST',{...draft,data:{assessment:'Changed'}})).status,409);
  const parallelKey=`parallel_${suffix}`;const parallel=await Promise.all([call(createPath,vol,'POST',{...draft,idempotency_key:parallelKey}),call(createPath,vol,'POST',{...draft,idempotency_key:parallelKey})]);assert.deepEqual(parallel.map(r=>r.status).sort(),[200,201]);assert.equal(parallel[0].data.record.id,parallel[1].data.record.id);
  const recordPath=`/events/${eventA.id}/records/${record.id}`;
  assert.equal((await call(recordPath,outsider)).status,403);
  const edits=await Promise.all([call(recordPath,vol,'PATCH',{revision:1,data:draft.data,synthetic_confirmed:true}),call(recordPath,vol,'PATCH',{revision:1,data:{...draft.data,findings:'Synthetic changed finding'},synthetic_confirmed:true})]);
  assert.deepEqual(edits.map(r=>r.status).sort(),[200,409]);const revision=edits.find(r=>r.status===200).data.record.revision;
  failBridge=true;const finalBody={revision,idempotency_key:`final_${suffix}`,synthetic_confirmed:true};assert.equal((await call(recordPath+'/finalize',vol,'POST',finalBody)).status,503);assert.equal((await call(recordPath,vol)).data.record.status,'draft');assert.equal((await call(recordPath,vol,'PATCH',{revision,data:draft.data,synthetic_confirmed:true})).data.error,'FINALIZATION_PENDING_RETRY');failBridge=false;
  assert.equal((await call(recordPath,vol)).data.record.finalization_pending,true);assert.equal((await call(recordPath,vol)).data.record.finalization_actor_id,vol.id);assert.equal((await call(recordPath+'/finalize',admin,'POST',finalBody)).status,403);
  failAfterNativeCommit=true;assert.equal((await call(recordPath+'/finalize',vol,'POST',finalBody)).status,503);assert.equal(nativeSaved,1);assert.equal((await call(recordPath,vol)).data.record.status,'draft');
  const finalized=await call(recordPath+'/finalize',vol,'POST',finalBody);assert.equal(finalized.status,200,JSON.stringify(finalized.data));assert.equal(finalized.data.record.status,'final');assert.equal(finalized.data.record.sync_status,'synced');assert.equal(finalized.data.record.finalization_pending,false);
  assert.equal((await call(recordPath+'/finalize',vol,'POST',finalBody)).status,200);assert.equal(nativeSaved,1);
  assert.equal((await call(recordPath,vol,'PATCH',{revision:finalized.data.record.revision,data:draft.data,synthetic_confirmed:true})).status,409);
  const versions=await call(recordPath+'/revisions',vol);assert.equal(versions.status,200);assert.equal(versions.data.revisions.length,3);assert.equal(versions.data.revisions[0].status,'draft');assert.deepEqual(versions.data.revisions[0].data,draft.data);assert.equal(versions.data.revisions[2].status,'final');
  const amended=await call(recordPath+'/amendments',vol,'POST',{reason:'Correct synthetic demonstration',data:{...draft.data,findings:'Corrected fixture'},idempotency_key:`amend_${suffix}`,synthetic_confirmed:true});assert.equal(amended.status,201);assert.equal(amended.data.record.supersedes_record_id,record.id);assert.equal((await call(recordPath,vol)).data.record.status,'final');
  const amendmentPath=`/events/${eventA.id}/records/${amended.data.record.id}`;const amendedFinal=await call(amendmentPath+'/finalize',vol,'POST',{revision:1,idempotency_key:`amend_final_${suffix}`,synthetic_confirmed:true});assert.equal(amendedFinal.status,200,JSON.stringify(amendedFinal.data));assert.equal(amendedFinal.data.record.openemr_encounter_uuid,finalized.data.record.openemr_encounter_uuid);
  const picture=await sharp({create:{width:2,height:2,channels:3,background:'#0f0'}}).png().toBuffer();
  function imageForm(){const form=new FormData();form.append('synthetic_confirmed','true');form.append('file',new Blob([picture],{type:'image/png'}),'fixture.png');return form;}
  scannerHealthy=false;const quarantined=await fetch(base+recordPath+'/attachments',{method:'POST',headers:{Authorization:`Bearer ${tokens.get(vol.id)}`},body:imageForm()});assert.equal(quarantined.status,503);assert.equal(attachments.size,0);scannerHealthy=true;
  const form=imageForm();
  const attached=await fetch(base+recordPath+'/attachments',{method:'POST',headers:{Authorization:`Bearer ${tokens.get(vol.id)}`},body:form});const attachedBody=await attached.json();assert.equal(attached.status,201,JSON.stringify(attachedBody));
  const otherAttachment=await fetch(base+amendmentPath+'/attachments',{method:'POST',headers:{Authorization:`Bearer ${tokens.get(vol.id)}`},body:imageForm()});assert.equal(otherAttachment.status,201);assert.equal(attachmentKeys.size,2);
  const downloadPath=recordPath+`/attachments/${attachedBody.attachment.id}/download`;assert.equal((await call(downloadPath,outsider)).status,403);
  const download=await fetch(base+downloadPath,{headers:{Authorization:`Bearer ${tokens.get(vol.id)}`}});assert.equal(download.status,200);assert.equal(download.headers.get('content-type'),'image/png');
  assert.equal((await call(`/patients/${patient.id}/records`,vol)).data.records.length,3);
  assert.equal((await call(createPath,admin,'POST',{specialty:'dental',data:{assessment:'Synthetic dental',plan:'Synthetic plan'},idempotency_key:`dental_${suffix}`,synthetic_confirmed:true})).status,201);
  const historyExport=await call(`/patients/${patient.id}/records/export`,vol);assert.equal(historyExport.status,200);assert.equal(historyExport.data.schema_version,1);assert.equal(historyExport.data.records.length,3);assert.ok(historyExport.data.records.every(r=>r.specialty==='general'));assert.match(historyExport.headers.get('content-disposition'),/attachment/);
  assert.equal((await call(`/events/${eventA.id}/feedback`,vol,'POST',{category:'clinical_form',message:'Synthetic field suggestion',rating:4})).status,201);assert.equal((await call(`/events/${eventA.id}/feedback`,admin)).data.feedback.length,1);assert.equal((await call(`/events/${eventA.id}/feedback`,vol)).status,403);
  await call(`/events/${eventA.id}/grants/${vol.id}`,admin,'DELETE');assert.equal((await call(`/patients/${patient.id}/records`,vol)).status,403);assert.equal((await call(downloadPath,vol)).status,403);
  await call(`/events/${eventA.id}/grants`,admin,'POST',grant);await db.query("UPDATE user SET enabled='N' WHERE id=?",[vol.id]);assert.equal((await call(recordPath,vol)).status,403);await db.query("UPDATE user SET enabled='Y' WHERE id=?",[vol.id]);
  const [[audit]]=await db.query('SELECT COUNT(*) total FROM clinical_sandbox_audit WHERE event_id=?',[eventA.id]);assert.ok(audit.total>=10);
  const [[nativeHealthCount]]=await db.query('SELECT COUNT(*) total FROM health_event WHERE slug LIKE ?',[`%${suffix}%`]);assert.equal(nativeHealthCount.total,0);
 }finally{
  if(server)await new Promise(resolve=>server.close(resolve));
  if(events.length)await db.query('DELETE FROM clinical_sandbox_event WHERE id IN (?)',[events]);
  if(users.length){await db.query('DELETE FROM clinical_sandbox_audit WHERE actor_user_id IN (?)',[users.map(u=>u.id)]);await db.query('DELETE FROM user WHERE id IN (?)',[users.map(u=>u.id)]);}
  await db.end();
 }
});
