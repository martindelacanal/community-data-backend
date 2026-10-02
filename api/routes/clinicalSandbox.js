'use strict';
const express = require('express');
const jwt = require('jsonwebtoken');
const crypto = require('node:crypto');
const multer = require('multer');
const v = require('../services/clinicalValidation');
const {createOpenEmrAdapter} = require('../services/clinicalOpenEmr');
const MANAGERS = ['admin','opsmanager'];
const json = value => typeof value === 'string' ? JSON.parse(value) : value;
const bool = value => value === true || value === 1;
const isoDate = value => value instanceof Date ? value.toISOString().slice(0,10) : String(value).slice(0,10);
function createClinicalSandboxRouter(options = {}) {
  const router = express.Router();
  const db = options.db || require('../connection/connection').promise();
  const env = options.env || process.env;
  const adapter = options.adapter || createOpenEmrAdapter(env);
  const enabled = () => env.CLINICAL_SANDBOX_ENABLED === 'true';
  const requestWindows = new Map();
  let uploadsInFlight=0;
  const upload = multer({storage:multer.memoryStorage(),limits:{fileSize:5*1024*1024,files:1,fields:3,fieldSize:500}}).single('file');
  const wrap = fn => (req,res,next) => Promise.resolve(fn(req,res)).catch(next);
  async function audit(actor,event,action,record=null,revision=null,connection=db) {
    await connection.query('INSERT INTO clinical_sandbox_audit(actor_user_id,event_id,record_id,action,revision) VALUES(?,?,?,?,?)',[actor.id,event,record,action,revision]);
  }
  async function snapshot(connection,record,actor,action) {
    await connection.query('INSERT INTO clinical_sandbox_revision(record_id,revision,status,data,actor_user_id,action) VALUES(?,?,?,?,?,?)',[record.id,record.revision,record.status,typeof record.data==='string'?record.data:JSON.stringify(record.data),actor.id,action]);
  }
  async function transaction(fn) {
    const c = await db.getConnection();
    try { await c.beginTransaction(); const result=await fn(c); await c.commit(); return result; }
    catch(e) { await c.rollback(); throw e; } finally {c.release();}
  }
  function manager(actor) { if (!MANAGERS.includes(actor.role)) v.fail('FORBIDDEN',403); }
  async function grantFor(eventId,actor,connection=db) {
    const [[grant]] = await connection.query('SELECT * FROM clinical_sandbox_grant WHERE event_id=? AND user_id=? AND revoked_at IS NULL',[eventId,actor.id]);
    return grant ? {...grant,specialties:json(grant.specialties),can_write:bool(grant.can_write),can_finalize:bool(grant.can_finalize)} : null;
  }
  async function access(eventId,actor,specialty=null,capability='read',connection=db) {
    const [[event]] = await connection.query("SELECT * FROM clinical_sandbox_event WHERE id=? AND mode='synthetic'",[eventId]);
    if (!event) v.fail('NOT_FOUND',404);
    const grant = await grantFor(eventId,actor,connection);
    const adminRead = capability === 'read' && MANAGERS.includes(actor.role);
    if (!adminRead && (!grant || (specialty && !grant.specialties.includes(specialty)) || (capability==='write' && !grant.can_write) || (capability==='finalize' && !grant.can_finalize))) v.fail('FORBIDDEN',403);
    return {event,grant};
  }
  async function patientFor(eventId,patientId,actor,connection=db) {
    await access(eventId,actor,null,'read',connection);
    const [[patient]]=await connection.query('SELECT * FROM clinical_sandbox_patient WHERE id=? AND event_id=? AND is_synthetic=1 AND synthetic_code LIKE ?',[patientId,eventId,'CPTEST-%']);
    if (!patient) v.fail('NOT_FOUND',404); return {...patient,date_of_birth:isoDate(patient.date_of_birth),is_synthetic:true};
  }
  async function recordFor(eventId,recordId,actor,capability='read',connection=db,lock=false) {
    const [[record]]=await connection.query(`SELECT * FROM clinical_sandbox_record WHERE id=? AND event_id=?${lock?' FOR UPDATE':''}`,[recordId,eventId]);
    if (!record) v.fail('NOT_FOUND',404);
    await access(eventId,actor,record.specialty,capability,connection);
    await patientFor(eventId,record.patient_id,actor,connection); return record;
  }
  async function shapeRecord(record,connection=db) {
    const [attachments]=await connection.query('SELECT id,filename,mime_type,size_bytes,created_at FROM clinical_sandbox_attachment WHERE record_id=? ORDER BY created_at',[record.id]);
    const {request_hash,idempotency_key,finalize_key,finalization_context,...rest}=record;
    const context=finalization_context?json(finalization_context):null;
    return {...rest,data:json(record.data),attachments,finalization_pending:record.status==='draft'&&!!finalize_key,finalization_actor_id:context?.id||null};
  }
  router.use((req,res,next)=>{res.set('Cache-Control','no-store, private');res.set('Pragma','no-cache');res.set('X-Content-Type-Options','nosniff');next();});
  // Read current DB identity on every request; role or account revocation takes effect immediately.
  router.use((req,res,next)=>{
    (async()=>{
      const match=/^Bearer (.+)$/.exec(req.headers.authorization || ''); if(!match) v.fail('UNAUTHORIZED',401);
      let claims;try{claims=jwt.verify(match[1],env.JWT_SECRET,{algorithms:['HS256']});}catch{v.fail('UNAUTHORIZED',401);}
      let identity;try{identity=typeof claims.data==='string'?JSON.parse(claims.data):claims.data;}catch{v.fail('UNAUTHORIZED',401);}
      if(!Number.isInteger(Number(identity?.id)) || Number(identity.id)<1) v.fail('UNAUTHORIZED',401);
      const [[actor]]=await db.query("SELECT u.id,r.name role FROM user u INNER JOIN role r ON r.id=u.role_id WHERE u.id=? AND u.enabled='Y' AND u.deleted='N'",[identity.id]);
      if(!actor || ![...MANAGERS,'eventvolunteer'].includes(actor.role)) v.fail('FORBIDDEN',403);
      req.clinicalActor=actor;next();
    })().catch(next);
  });
  router.use((req,res,next)=>{
    const now=Date.now(),kind=req.path.endsWith('/attachments')&&req.method==='POST'?'upload':req.path.endsWith('/finalize')?'finalize':'request';
    const key=`${req.clinicalActor.id}:${kind}`,max=kind==='upload'?10:kind==='finalize'?20:180;
    for(const [k,b] of requestWindows)if(b.end<=now)requestWindows.delete(k);
    const bucket=requestWindows.get(key)||{end:now+60000,n:0};bucket.n++;requestWindows.set(key,bucket);
    if(bucket.n>max){res.set('Retry-After',String(Math.ceil((bucket.end-now)/1000)));return next(new v.ClinicalError('RATE_LIMITED',429));}
    next();
  });
  router.get('/status',wrap(async(req,res)=>{
    let available=false,status=null; if(enabled() && adapter.configured()) {try{status=await adapter.status();available=true;}catch{}}
    const scannerHealthy=status?.attachments?.scanner?.healthy===true;
    res.json({enabled:enabled(),mode:'synthetic',real_data_enabled:false,openemr:{configured:adapter.configured(),available},attachments:{max_bytes:5*1024*1024,allowed_types:scannerHealthy?['image/png','image/jpeg','application/pdf']:[],scanner_available:scannerHealthy,pdf_available:scannerHealthy}});
  }));
  router.use((req,res,next)=>enabled()?next():next(new v.ClinicalError('CLINICAL_SANDBOX_DISABLED',503)));
  router.get('/events',wrap(async(req,res)=>{
    const actor=req.clinicalActor;
    const [rows]=await db.query(`SELECT e.* FROM clinical_sandbox_event e WHERE e.mode='synthetic'${MANAGERS.includes(actor.role)?'':" AND EXISTS(SELECT 1 FROM clinical_sandbox_grant g WHERE g.event_id=e.id AND g.user_id=? AND g.revoked_at IS NULL)"} ORDER BY e.created_at DESC,e.id DESC LIMIT 100`,MANAGERS.includes(actor.role)?[]:[actor.id]);
    const events=[];for(const e of rows)events.push({...e,start_date:isoDate(e.start_date),end_date:isoDate(e.end_date),can_manage:MANAGERS.includes(actor.role),grant:await grantFor(e.id,actor)});
    await audit(actor,null,'events.read');res.json({events});
  }));
  router.post('/events',wrap(async(req,res)=>{
    const actor=req.clinicalActor;manager(actor);if(req.body.mode && req.body.mode!=='synthetic')v.fail('REAL_DATA_FORBIDDEN',403);
    const name=v.text(req.body.name,120,true),start=v.date(req.body.start_date,true),end=v.date(req.body.end_date,true);if(end<start)v.fail();
    const event=await transaction(async c=>{
      const [insert]=await c.query('INSERT INTO clinical_sandbox_event(name,start_date,end_date,created_by) VALUES(?,?,?,?)',[name,start,end,actor.id]);const id=insert.insertId;
      const fixtures=[['Alex Demo','1988-04-12','Male'],['Sam Example','1994-09-03','Female'],['Taylor Sample','1972-01-24','Unknown']];
      for(let i=0;i<fixtures.length;i++)await c.query('INSERT INTO clinical_sandbox_patient(event_id,synthetic_code,display_name,date_of_birth,sex) VALUES(?,?,?,?,?)',[id,`CPTEST-${crypto.randomUUID()}`,...fixtures[i]]);
      if(req.body.grant_self===true)await c.query('INSERT INTO clinical_sandbox_grant(event_id,user_id,specialties,can_write,can_finalize,granted_by) VALUES(?,?,?,1,1,?)',[id,actor.id,JSON.stringify(v.SPECIALTIES),actor.id]);
      await audit(actor,id,'event.create',null,null,c);return {id,name,start_date:start,end_date:end,mode:'synthetic',can_manage:true};
    });event.grant=await grantFor(event.id,actor);res.status(201).json({event});
  }));
  router.get('/staff',wrap(async(req,res)=>{
    manager(req.clinicalActor);const search=v.text(req.query.search,100,true);if(search.length<2)v.fail('SEARCH_TOO_SHORT');
    const term=`%${search.replace(/[\\%_]/g,'\\$&')}%`;
    const [staff]=await db.query("SELECT u.id,TRIM(CONCAT(COALESCE(u.firstname,''),' ',COALESCE(u.lastname,''))) display_name,r.name role FROM user u JOIN role r ON r.id=u.role_id WHERE u.enabled='Y' AND u.deleted='N' AND r.name IN ('admin','opsmanager','eventvolunteer') AND (u.firstname LIKE ? OR u.lastname LIKE ? OR u.username LIKE ?) ORDER BY u.id LIMIT 20",[term,term,term]);
    await audit(req.clinicalActor,null,'staff.search');res.json({staff});
  }));
  router.get('/events/:eventId/grants',wrap(async(req,res)=>{
    const actor=req.clinicalActor;manager(actor);await access(req.params.eventId,actor);
    const [rows]=await db.query("SELECT g.*,TRIM(CONCAT(COALESCE(u.firstname,''),' ',COALESCE(u.lastname,''))) display_name,r.name role FROM clinical_sandbox_grant g JOIN user u ON u.id=g.user_id JOIN role r ON r.id=u.role_id WHERE event_id=? ORDER BY granted_at DESC",[req.params.eventId]);
    await audit(actor,req.params.eventId,'grants.read');res.json({grants:rows.map(x=>({...x,specialties:json(x.specialties),can_write:bool(x.can_write),can_finalize:bool(x.can_finalize)}))});
  }));
  router.post('/events/:eventId/grants',wrap(async(req,res)=>{
    const actor=req.clinicalActor;manager(actor);await access(req.params.eventId,actor);
    const body=req.body;const userId=Number(body.user_id);if(!Number.isInteger(userId)||userId<1 || !Array.isArray(body.specialties)||!body.specialties.length||body.specialties.some(s=>!v.SPECIALTIES.includes(s)))v.fail();
    if(typeof body.can_write!=='boolean'||typeof body.can_finalize!=='boolean'||(body.can_finalize&&!body.can_write))v.fail();
    const [[target]]=await db.query("SELECT u.id FROM user u JOIN role r ON r.id=u.role_id WHERE u.id=? AND u.enabled='Y' AND u.deleted='N' AND r.name IN ('admin','opsmanager','eventvolunteer')",[userId]);if(!target)v.fail('INVALID_STAFF');
    await transaction(async c=>{await c.query('INSERT INTO clinical_sandbox_grant(event_id,user_id,specialties,can_write,can_finalize,granted_by) VALUES(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE specialties=VALUES(specialties),can_write=VALUES(can_write),can_finalize=VALUES(can_finalize),granted_by=VALUES(granted_by),granted_at=CURRENT_TIMESTAMP(3),revoked_at=NULL',[req.params.eventId,userId,JSON.stringify([...new Set(body.specialties)]),body.can_write,body.can_finalize,actor.id]);await audit(actor,req.params.eventId,'grant.upsert',null,null,c);});res.json({granted:true});
  }));
  router.delete('/events/:eventId/grants/:userId',wrap(async(req,res)=>{
    const actor=req.clinicalActor;manager(actor);await access(req.params.eventId,actor);
    await transaction(async c=>{await c.query('UPDATE clinical_sandbox_grant SET revoked_at=CURRENT_TIMESTAMP(3) WHERE event_id=? AND user_id=?',[req.params.eventId,req.params.userId]);await audit(actor,req.params.eventId,'grant.revoke',null,null,c);});res.json({revoked:true});
  }));
  async function patients(req,res,eventId=null){
    const actor=req.clinicalActor;if(eventId)await access(eventId,actor);
    const params=[];let filter='';if(eventId){filter+=' AND p.event_id=?';params.push(eventId);}
    if(!MANAGERS.includes(actor.role)){filter+=' AND EXISTS(SELECT 1 FROM clinical_sandbox_grant g WHERE g.event_id=p.event_id AND g.user_id=? AND g.revoked_at IS NULL)';params.push(actor.id);}
    const [rows]=await db.query(`SELECT p.*,e.name event_name FROM clinical_sandbox_patient p JOIN clinical_sandbox_event e ON e.id=p.event_id WHERE p.is_synthetic=1 AND e.mode='synthetic'${filter} ORDER BY p.id LIMIT 300`,params);
    await audit(actor,eventId,'patients.read');res.json({patients:rows.map(x=>({...x,date_of_birth:isoDate(x.date_of_birth),is_synthetic:true}))});
  }
  router.get('/patients',wrap((req,res)=>patients(req,res)));
  router.get('/events/:eventId/patients',wrap((req,res)=>patients(req,res,req.params.eventId)));
  async function history(req,res,eventId,patientId,download=false){
    const actor=req.clinicalActor;const patient=await patientFor(eventId,patientId,actor);const {grant}=await access(eventId,actor);
    const [rows]=await db.query('SELECT * FROM clinical_sandbox_record WHERE event_id=? AND patient_id=? ORDER BY created_at DESC,id DESC',[eventId,patientId]);
    const allowed=MANAGERS.includes(actor.role)?rows:rows.filter(r=>grant.specialties.includes(r.specialty));
    const records=[];for(const r of allowed)records.push(await shapeRecord(r));
    await audit(actor,eventId,download?'history.download':'history.read');
    if(download){res.set('Content-Disposition','attachment; filename="synthetic-clinical-history.json"');return res.json({schema_version:1,mode:'synthetic',exported_at:new Date().toISOString(),patient,units:{systolic_mm_hg:'mm[Hg]',diastolic_mm_hg:'mm[Hg]',pulse_bpm:'/min',respiratory_rate_per_min:'/min',temperature_celsius:'Cel',oxygen_saturation_pct:'%',weight_kg:'kg',height_cm:'cm',glucose_mg_dl:'mg/dL'},records});}
    res.json({records});
  }
  router.get('/patients/:patientId/records',wrap(async(req,res)=>{const [[p]]=await db.query('SELECT event_id FROM clinical_sandbox_patient WHERE id=? AND is_synthetic=1',[req.params.patientId]);if(!p)v.fail('NOT_FOUND',404);return history(req,res,p.event_id,req.params.patientId);}));
  router.get('/events/:eventId/patients/:patientId/records',wrap((req,res)=>history(req,res,req.params.eventId,req.params.patientId)));
  router.get(['/patients/:patientId/records/export','/patients/:patientId/export'],wrap(async(req,res)=>{const [[p]]=await db.query('SELECT event_id FROM clinical_sandbox_patient WHERE id=? AND is_synthetic=1',[req.params.patientId]);if(!p)v.fail('NOT_FOUND',404);return history(req,res,p.event_id,req.params.patientId,true);}));
  router.get(['/events/:eventId/patients/:patientId/records/export','/events/:eventId/patients/:patientId/export'],wrap((req,res)=>history(req,res,req.params.eventId,req.params.patientId,true)));
  async function createRecord(req,res,parent=null){
    const actor=req.clinicalActor,body=req.body;v.synthetic(body);const eventId=req.params.eventId;
    const specialty=parent?parent.specialty:body.specialty,patientId=parent?parent.patient_id:req.params.patientId;
    await access(eventId,actor,specialty,'write');await patientFor(eventId,patientId,actor);
    const data=v.clinicalData(specialty,body.data),key=v.idempotency(body.idempotency_key),reason=parent?v.text(body.reason,1000,true):null;
    const requestHash=v.hash({patientId:Number(patientId),specialty,data,parent:parent?.id||null,reason});
    let created=false;
    const result=await transaction(async c=>{
      // Serialize creation per synthetic event so simultaneous retries cannot race on a missing idempotency row.
      await c.query('SELECT id FROM clinical_sandbox_event WHERE id=? FOR UPDATE',[eventId]);
      const [[existing]]=await c.query('SELECT * FROM clinical_sandbox_record WHERE event_id=? AND recorded_by=? AND idempotency_key=? FOR UPDATE',[eventId,actor.id,key]);
      if(existing){if(existing.request_hash!==requestHash)v.fail('IDEMPOTENCY_CONFLICT',409);return shapeRecord(existing,c);}
      const id=crypto.randomUUID();await c.query('INSERT INTO clinical_sandbox_record(id,event_id,patient_id,specialty,data,recorded_by,supersedes_record_id,amendment_reason,idempotency_key,request_hash) VALUES(?,?,?,?,?,?,?,?,?,?)',[id,eventId,patientId,specialty,JSON.stringify(data),actor.id,parent?.id||null,reason,key,requestHash]);
      await audit(actor,eventId,parent?'record.amendment.create':'record.create',id,1,c);created=true;
      const [[record]]=await c.query('SELECT * FROM clinical_sandbox_record WHERE id=?',[id]);await snapshot(c,record,actor,parent?'amendment.create':'draft.create');return shapeRecord(record,c);
    });res.status(created?201:200).json({record:result});
  }
  router.post('/events/:eventId/patients/:patientId/records',wrap((req,res)=>createRecord(req,res)));
  router.get('/events/:eventId/records/:recordId',wrap(async(req,res)=>{const record=await recordFor(req.params.eventId,req.params.recordId,req.clinicalActor);await audit(req.clinicalActor,record.event_id,'record.read',record.id,record.revision);res.json({record:await shapeRecord(record)});}));
  router.get('/events/:eventId/records/:recordId/revisions',wrap(async(req,res)=>{const record=await recordFor(req.params.eventId,req.params.recordId,req.clinicalActor);const [rows]=await db.query('SELECT id,revision,status,data,actor_user_id,action,created_at FROM clinical_sandbox_revision WHERE record_id=? ORDER BY revision',[record.id]);await audit(req.clinicalActor,record.event_id,'revisions.read',record.id,record.revision);res.json({revisions:rows.map(r=>({...r,data:json(r.data)}))});}));
  router.patch('/events/:eventId/records/:recordId',wrap(async(req,res)=>{
    v.synthetic(req.body);const actor=req.clinicalActor;
    const record=await transaction(async c=>{const r=await recordFor(req.params.eventId,req.params.recordId,actor,'write',c,true);if(r.status!=='draft')v.fail('FINAL_RECORD_IMMUTABLE',409);if(r.finalize_key)v.fail('FINALIZATION_PENDING_RETRY',409);if(req.body.revision!==r.revision)v.fail('REVISION_CONFLICT',409);
      const data=v.clinicalData(r.specialty,req.body.data);await c.query("UPDATE clinical_sandbox_record SET data=?,revision=revision+1,updated_at=CURRENT_TIMESTAMP(3),sync_status='pending' WHERE id=?",[JSON.stringify(data),r.id]);await audit(actor,r.event_id,'record.update',r.id,r.revision+1,c);const [[next]]=await c.query('SELECT * FROM clinical_sandbox_record WHERE id=?',[r.id]);await snapshot(c,next,actor,'draft.update');return shapeRecord(next,c);});res.json({record});
  }));
  router.post('/events/:eventId/records/:recordId/finalize',wrap(async(req,res)=>{
    v.synthetic(req.body);const actor=req.clinicalActor,key=v.idempotency(req.body.idempotency_key);
    // Persist the finalization boundary before a remote call. An ambiguous native commit
    // cannot be followed by editing the draft into a different document on retry.
    await transaction(async c=>{const r=await recordFor(req.params.eventId,req.params.recordId,actor,'finalize',c,true);if(r.status==='final')return;
      if(req.body.revision!==r.revision)v.fail('REVISION_CONFLICT',409);const data=json(r.data);if(!data.assessment?.trim()||!data.plan?.trim())v.fail('ASSESSMENT_AND_PLAN_REQUIRED');
      if(r.finalization_context && json(r.finalization_context).id!==actor.id)v.fail('FINALIZATION_OWNED_BY_ANOTHER_ACTOR',403);
      if(!r.finalize_key){
        const [[recorder]]=await c.query('SELECT r.name role FROM user u JOIN role r ON r.id=u.role_id WHERE u.id=?',[r.recorded_by]);
        const context={...actor,can_finalize:true,original_recorder:{id:r.recorded_by,name:`Sandbox operator ${r.recorded_by}`,role:recorder?.role||'recorder'}};
        await c.query('UPDATE clinical_sandbox_record SET finalize_key=?,finalization_context=? WHERE id=?',[key,JSON.stringify(context),r.id]);await audit(actor,r.event_id,'record.finalization.begin',r.id,r.revision,c);
      }
    });
    let record;
    try { record=await transaction(async c=>{
      const r=await recordFor(req.params.eventId,req.params.recordId,actor,'finalize',c,true);
      if(r.status==='final'){if(r.finalize_key!==key)v.fail('FINAL_RECORD_IMMUTABLE',409);return shapeRecord(r,c);}
      if(req.body.revision!==r.revision)v.fail('REVISION_CONFLICT',409);
      const data=json(r.data);if(!data.assessment?.trim()||!data.plan?.trim())v.fail('ASSESSMENT_AND_PLAN_REQUIRED');
      const p=await patientFor(r.event_id,r.patient_id,actor,c);
      const nativePatient=await adapter.upsertPatient(p,actor);if(!nativePatient.patient_uuid)v.fail('OPENEMR_UNAVAILABLE',503);
      let supersedes=null,encounter;
      if(r.supersedes_record_id){
        const [[parent]]=await c.query("SELECT openemr_record_uuid,openemr_encounter_uuid FROM clinical_sandbox_record WHERE id=? AND patient_id=? AND event_id=? AND specialty=? AND status='final'",[r.supersedes_record_id,r.patient_id,r.event_id,r.specialty]);
        supersedes=parent?.openemr_record_uuid;encounter={encounter_uuid:parent?.openemr_encounter_uuid};
        if(!supersedes||!encounter.encounter_uuid)v.fail('AMENDMENT_PARENT_UNAVAILABLE',409);
        const [[newer]]=await c.query("SELECT id FROM clinical_sandbox_record WHERE supersedes_record_id=? AND status='final' AND id<>? LIMIT 1",[r.supersedes_record_id,r.id]);if(newer)v.fail('AMEND_LATEST_REVISION',409);
      }else encounter=await adapter.createEncounter(nativePatient.patient_uuid,r,actor);
      if(!encounter.encounter_uuid)v.fail('OPENEMR_UNAVAILABLE',503);
      const finalActor=r.finalization_context?json(r.finalization_context):null;
      if(!finalActor||finalActor.id!==actor.id)v.fail('FINALIZATION_OWNED_BY_ANOTHER_ACTOR',403);
      const saved=await adapter.saveRecord({is_test:true,patient_uuid:nativePatient.patient_uuid,encounter_uuid:encounter.encounter_uuid,specialty:r.specialty,schema_version:1,external_record_id:`CPTEST-${r.id}`,idempotency_key:`final_${r.id}`,status:'final',data,supersedes_record_uuid:supersedes,amendment_reason:r.amendment_reason},finalActor);
      if(!saved.record_uuid)v.fail('OPENEMR_UNAVAILABLE',503);
      await c.query('UPDATE clinical_sandbox_patient SET openemr_patient_uuid=? WHERE id=?',[nativePatient.patient_uuid,p.id]);
      await c.query("UPDATE clinical_sandbox_record SET status='final',sync_status='synced',revision=revision+1,finalized_by=?,finalized_at=CURRENT_TIMESTAMP(3),updated_at=CURRENT_TIMESTAMP(3),finalize_key=?,openemr_record_uuid=?,openemr_encounter_uuid=? WHERE id=?",[actor.id,key,saved.record_uuid,encounter.encounter_uuid,r.id]);
      await audit(actor,r.event_id,'record.finalize',r.id,r.revision+1,c);const [[next]]=await c.query('SELECT * FROM clinical_sandbox_record WHERE id=?',[r.id]);await snapshot(c,next,actor,'record.finalize');return shapeRecord(next,c);
    }); } catch(error) {
      if(error.status===503)await db.query("UPDATE clinical_sandbox_record SET sync_status='error' WHERE id=? AND event_id=? AND status='draft' AND finalize_key IS NOT NULL",[req.params.recordId,req.params.eventId]);
      throw error;
    }
    res.json({record});
  }));
  router.post('/events/:eventId/records/:recordId/amendments',wrap(async(req,res)=>{const parent=await recordFor(req.params.eventId,req.params.recordId,req.clinicalActor,'write');if(parent.status!=='final')v.fail('AMENDMENT_REQUIRES_FINAL',409);return createRecord(req,res,parent);}));
  // Authenticate/authorize before allocating upload memory. No file is served from a public directory.
  router.post('/events/:eventId/records/:recordId/attachments',(req,res,next)=>{
    if(uploadsInFlight>=2)return next(new v.ClinicalError('UPLOAD_BUSY',429));
    if(Number(req.headers['content-length'])>5*1024*1024+16384)return next(new v.ClinicalError('ATTACHMENT_TOO_LARGE',413));
    recordFor(req.params.eventId,req.params.recordId,req.clinicalActor,'write').then(r=>{if(r.status!=='final'||!r.openemr_record_uuid)v.fail('ATTACHMENT_REQUIRES_SYNCED_FINAL',409);if(uploadsInFlight>=2)v.fail('UPLOAD_BUSY',429);uploadsInFlight++;let released=false;const release=()=>{if(!released){released=true;uploadsInFlight--;}};res.once('finish',release);res.once('close',release);req.clinicalRecord=r;upload(req,res,next);}).catch(next);
  },wrap(async(req,res)=>{
    v.synthetic(req.body);const actor=req.clinicalActor,r=req.clinicalRecord;
    let scannerHealthy=false;try{scannerHealthy=(await adapter.status()).attachments?.scanner?.healthy===true;}catch{}
    if(!scannerHealthy)v.fail('ATTACHMENT_SCANNER_UNAVAILABLE',503);
    const file=await v.validateAttachment(req.file,{pdfScannerAvailable:scannerHealthy});
    await access(r.event_id,actor,r.specialty,'write');
    const [[existing]]=await db.query('SELECT id,filename,mime_type,size_bytes FROM clinical_sandbox_attachment WHERE record_id=? AND sha256=?',[r.id,file.sha256]);if(existing)return res.json({attachment:existing});
    const saved=await adapter.upload(r.openemr_record_uuid,{is_test:true,idempotency_key:`attach_${r.id}_${file.sha256}`,filename:file.filename,mime_type:file.mime_type,content_base64:file.buffer.toString('base64')},actor);
    const uuid=saved.attachment_uuid||saved.document_uuid;if(!uuid)v.fail('OPENEMR_UNAVAILABLE',503);
    const id=crypto.randomUUID();await transaction(async c=>{await c.query('INSERT INTO clinical_sandbox_attachment(id,record_id,openemr_attachment_uuid,filename,mime_type,size_bytes,sha256,uploaded_by) VALUES(?,?,?,?,?,?,?,?)',[id,r.id,uuid,file.filename,file.mime_type,file.size_bytes,file.sha256,actor.id]);await audit(actor,r.event_id,'attachment.create',r.id,r.revision,c);});res.status(201).json({attachment:{id,filename:file.filename,mime_type:file.mime_type,size_bytes:file.size_bytes}});
  }));
  router.get('/events/:eventId/records/:recordId/attachments/:attachmentId/download',wrap(async(req,res)=>{
    const actor=req.clinicalActor,r=await recordFor(req.params.eventId,req.params.recordId,actor);
    const [[a]]=await db.query('SELECT * FROM clinical_sandbox_attachment WHERE id=? AND record_id=?',[req.params.attachmentId,r.id]);if(!a)v.fail('NOT_FOUND',404);
    const buffer=await adapter.download(r.openemr_record_uuid,a.openemr_attachment_uuid,actor);if(v.hash(buffer)!==a.sha256)v.fail('ATTACHMENT_INTEGRITY_ERROR',502);
    await audit(actor,r.event_id,'attachment.download',r.id,r.revision);res.set('Content-Type',a.mime_type);res.set('Content-Disposition',`attachment; filename="${a.filename}"`);res.send(buffer);
  }));
  router.post('/events/:eventId/feedback',wrap(async(req,res)=>{
    const actor=req.clinicalActor;await access(req.params.eventId,actor);const category=v.choice(req.body.category,['usability','clinical_form','technical','other']);if(!category)v.fail();const message=v.text(req.body.message,3000,true),rating=v.number(req.body.rating,1,5,true);
    const feedbackId=await transaction(async c=>{const [insert]=await c.query('INSERT INTO clinical_sandbox_feedback(event_id,user_id,category,message,rating) VALUES(?,?,?,?,?)',[req.params.eventId,actor.id,category,message,rating]);await audit(actor,req.params.eventId,'feedback.create',null,null,c);return insert.insertId;});res.status(201).json({feedback_id:feedbackId});
  }));
  router.get('/events/:eventId/feedback',wrap(async(req,res)=>{manager(req.clinicalActor);await access(req.params.eventId,req.clinicalActor);const [feedback]=await db.query('SELECT id,event_id,user_id,category,message,rating,created_at FROM clinical_sandbox_feedback WHERE event_id=? ORDER BY created_at DESC,id DESC LIMIT 200',[req.params.eventId]);await audit(req.clinicalActor,req.params.eventId,'feedback.read');res.json({feedback});}));
  router.use((error,req,res,next)=>{
    if(res.headersSent)return next(error);
    const code=error instanceof v.ClinicalError?error.code:error.code==='LIMIT_FILE_SIZE'?'ATTACHMENT_TOO_LARGE':error instanceof multer.MulterError?'INVALID_ATTACHMENT':error.code==='ER_DUP_ENTRY'?'IDEMPOTENCY_CONFLICT':'CLINICAL_REQUEST_FAILED';
    const status=error instanceof v.ClinicalError?error.status:code==='ATTACHMENT_TOO_LARGE'?413:code==='INVALID_ATTACHMENT'?400:code==='IDEMPOTENCY_CONFLICT'?409:500;
    // Never log request bodies, SQL messages, external responses, or credentials.
    res.status(status).json({error:code,message:code});
  });
  return router;
}
module.exports = {createClinicalSandboxRouter};
