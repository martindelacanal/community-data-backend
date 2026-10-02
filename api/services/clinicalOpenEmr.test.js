'use strict';
const test=require('node:test');const assert=require('node:assert/strict');const crypto=require('node:crypto');
const {createOpenEmrAdapter}=require('./clinicalOpenEmr');
test('bridge signs exact payload, verifies TLS, and distinguishes recorder from explicit final reviewer',async()=>{
 const calls=[];const secret=crypto.randomBytes(32).toString('hex');
 const adapter=createOpenEmrAdapter({CLINICAL_OPENEMR_BRIDGE_URL:'https://openemr.test/bridge.php',CLINICAL_OPENEMR_BRIDGE_SECRET:secret},async config=>{calls.push(config);return{data:{data:{record_uuid:'synthetic-native-record'}}};});
 const payload={is_test:true,data:{assessment:'Synthetic'}};await adapter.saveRecord(payload,{id:2,role:'eventvolunteer'});
 const first=calls[0],headers=first.headers;assert.equal(first.maxRedirects,0);assert.equal(first.httpsAgent.options.rejectUnauthorized,true);
 assert.equal(JSON.parse(Buffer.from(headers['X-CP-Actors'],'base64url')).clinician,null);
 const canonical=['POST','/v1/records',headers['X-CP-Timestamp'],headers['X-CP-Nonce'],headers['X-CP-Actors'],crypto.createHash('sha256').update(first.data).digest('hex')].join('\n');
 assert.equal(headers['X-CP-Signature'],crypto.createHmac('sha256',secret).update(canonical).digest('base64url'));
 await adapter.saveRecord(payload,{id:3,role:'eventvolunteer',can_finalize:true,original_recorder:{id:2,name:'Sandbox operator 2',role:'eventvolunteer'}});
 const actors=JSON.parse(Buffer.from(calls[1].headers['X-CP-Actors'],'base64url'));assert.equal(actors.recorded_by.id,'2');assert.equal(actors.clinician.id,'3');
 assert.notEqual(calls[0].headers['X-CP-Nonce'],calls[1].headers['X-CP-Nonce']);
 await adapter.status();const statusActors=JSON.parse(Buffer.from(calls[2].headers['X-CP-Actors'],'base64url'));assert.equal(statusActors.recorded_by.id,'0');assert.equal(statusActors.clinician,null);
 await adapter.upsertPatient({synthetic_code:'CPTEST-fixture',display_name:'Taylor Sample',date_of_birth:'1972-01-24',sex:'Other'},{id:2,role:'eventvolunteer'});assert.equal(JSON.parse(calls[3].data).patient.sex,'Unknown');
});
test('missing configuration, cleartext transport and invalid bridge response fail closed',async()=>{
 await assert.rejects(createOpenEmrAdapter({}).status(),/OPENEMR_UNAVAILABLE/);
 await assert.rejects(createOpenEmrAdapter({CLINICAL_OPENEMR_BRIDGE_URL:'http://openemr.test',CLINICAL_OPENEMR_BRIDGE_SECRET:'test'}).status(),/OPENEMR_TLS_REQUIRED/);
 const adapter=createOpenEmrAdapter({CLINICAL_OPENEMR_BRIDGE_URL:'https://openemr.test',CLINICAL_OPENEMR_BRIDGE_SECRET:'test'},async()=>({data:{ok:true}}));
 await assert.rejects(adapter.status(),/OPENEMR_UNAVAILABLE/);
});
