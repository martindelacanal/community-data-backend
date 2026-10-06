'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const {createFhirBundle} = require('./clinicalFhir');
const {catalog,fields} = require('./clinicalTemplates');
const {SYSTEM} = require('./clinicalFhirTemplates');

test('all template Coding displays agree with the exported local CodeSystem while questionnaire labels retain source wording', () => {
  const at='2026-10-06T16:00:00.000Z';
  const patient={id:1,event_id:1,is_synthetic:true,synthetic_code:'CPTEST-00000000-0000-4000-8000-000000000061',display_name:'Fictitious terminology test',date_of_birth:'1990-01-01',sex:'Unknown'};
  const records=catalog.templates.map((template,index)=>({
    id:`terminology-test-${index}`,patient_id:1,event_id:1,specialty:template.specialty,schema_version:2,
    data:{template_version:2,template_id:template.id,template_fields:Object.fromEntries(template.required_to_finalize.map(key=>[key,'Fictitious testing only']))},
    status:'final',revision:1,recorded_by:101,finalized_by:102,created_at:at,updated_at:at,finalized_at:at,attachments:[],
  }));
  const bundle=createFhirBundle({patient,records,exportedAt:at,exporterId:103});
  const resources=bundle.entry.map(entry=>entry.resource);
  const terminology=resources.find(resource=>resource.resourceType==='CodeSystem'&&resource.url===SYSTEM);
  assert(terminology);
  const concepts=new Map(terminology.concept.map(concept=>[concept.code,concept.display]));
  let codingCount=0;
  const walk=value=>{
    if(!value||typeof value!=='object')return;
    if(value.system===SYSTEM&&typeof value.code==='string'){
      assert(concepts.has(value.code),`Code ${value.code} must be declared`);
      if(value.display!==undefined)assert.equal(value.display,concepts.get(value.code),`Canonical display for ${value.code}`);
      codingCount++;
    }
    for(const child of Object.values(value))walk(child);
  };
  walk(bundle);
  assert(codingCount>=catalog.templates.reduce((n,template)=>n+fields(template).length,0));
  const itemMap=items=>new Map((items||[]).flatMap(item=>[[item.linkId,item],...itemMap(item.item)]));
  for(const template of catalog.templates){
    const questionnaire=resources.find(resource=>resource.resourceType==='Questionnaire'&&resource.url.endsWith(`/${template.id}`));
    assert.match(questionnaire.name,/^[A-Z][A-Za-z0-9_]{0,254}$/);
    const items=itemMap(questionnaire.item);
    for(const field of fields(template))assert.equal(items.get(field.key).text,field.en,`Keep source label ${template.id}/${field.key}`);
  }
});
