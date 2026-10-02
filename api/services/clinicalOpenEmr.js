'use strict';
const crypto = require('node:crypto');
const fs = require('node:fs');
const https = require('node:https');
const axios = require('axios');
const {ClinicalError} = require('./clinicalValidation');
function createOpenEmrAdapter(env = process.env, http = axios) {
  function configured() { return !!(env.CLINICAL_OPENEMR_BRIDGE_URL && env.CLINICAL_OPENEMR_BRIDGE_SECRET); }
  async function request(method, path, payload, actor, binary = false) {
    if (!configured()) throw new ClinicalError('OPENEMR_UNAVAILABLE',503);
    const url = new URL(env.CLINICAL_OPENEMR_BRIDGE_URL);
    if (url.protocol !== 'https:') throw new ClinicalError('OPENEMR_TLS_REQUIRED',503);
    url.searchParams.set('route',path);
    const body = payload == null ? '' : JSON.stringify(payload);
    const timestamp = String(Math.floor(Date.now()/1000)); const nonce = crypto.randomUUID();
    // Native RequestSecurity requires string actor IDs; DB/UI IDs remain numeric.
    const identity = actor ? {id:String(actor.id),name:`Sandbox operator ${actor.id}`,role:actor.role} : {id:'0',name:'Sandbox status',role:'system'};
    const recorder = actor?.original_recorder ? {...actor.original_recorder,id:String(actor.original_recorder.id)} : identity;
    const actors = Buffer.from(JSON.stringify({recorded_by:recorder,clinician:actor?.can_finalize === true ? identity : null})).toString('base64url');
    const digest = crypto.createHash('sha256').update(body).digest('hex');
    const signature = crypto.createHmac('sha256',env.CLINICAL_OPENEMR_BRIDGE_SECRET).update([method,path,timestamp,nonce,actors,digest].join('\n')).digest('base64url');
    try {
      const ca = env.CLINICAL_OPENEMR_CA_FILE ? fs.readFileSync(env.CLINICAL_OPENEMR_CA_FILE) : undefined;
      const response = await http({method,url:url.toString(),data:body || undefined,timeout:30000,maxRedirects:0,maxContentLength:8*1024*1024,maxBodyLength:8*1024*1024,responseType:binary?'arraybuffer':'json',httpsAgent:new https.Agent({ca,rejectUnauthorized:true}),headers:{'Content-Type':'application/json','X-CP-Timestamp':timestamp,'X-CP-Nonce':nonce,'X-CP-Actors':actors,'X-CP-Signature':signature}});
      if (binary) return Buffer.from(response.data);
      if (!response.data || response.data.internalErrors?.length || response.data.validationErrors?.length || !response.data.data) throw new Error('INVALID_BRIDGE_RESPONSE');
      return response.data.data;
    } catch { throw new ClinicalError('OPENEMR_UNAVAILABLE',503); }
  }
  return {configured, status:()=>request('GET','/v1/status'),
    upsertPatient:(patient,actor)=>request('POST','/v1/patients/upsert',{is_test:true,external_patient_id:patient.synthetic_code,patient:{fname:patient.display_name,lname:'Synthetic',DOB:String(patient.date_of_birth).slice(0,10),sex:['Male','Female'].includes(patient.sex)?patient.sex:'Unknown'}},actor),
    createEncounter:(patientUuid,record,actor)=>request('POST','/v1/encounters',{is_test:true,patient_uuid:patientUuid,external_encounter_id:`CPTEST-${record.id}`,date:(record.created_at instanceof Date?record.created_at.toISOString():String(record.created_at)).slice(0,10),reason:'Synthetic screening'},actor),
    saveRecord:(payload,actor)=>request('POST','/v1/records',payload,actor),
    upload:(uuid,payload,actor)=>request('POST',`/v1/records/${encodeURIComponent(uuid)}/attachments`,payload,actor),
    download:(uuid,attachmentUuid,actor)=>request('GET',`/v1/records/${encodeURIComponent(uuid)}/attachments/${encodeURIComponent(attachmentUuid)}`,null,actor,true)
  };
}
module.exports = {createOpenEmrAdapter};
