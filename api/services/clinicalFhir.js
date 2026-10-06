'use strict';
const crypto = require('node:crypto');
const {v5: uuid} = require('uuid');
const v = require('./clinicalValidation');
const templates = require('./clinicalTemplates');
const templateFhir = require('./clinicalFhirTemplates');

// These are stable terminology identifiers, not public API endpoints.
const BASE = 'https://bienestarcommunity.org/fhir/sandbox';
const SYSTEM = `${BASE}/CodeSystem/clinical-form`;
const VERSION = '1.0.0';
const NS = '1f28dbca-c8bc-5bab-a03b-9b34952c3430';
const LOINC = 'http://loinc.org';
const UCUM = 'http://unitsofmeasure.org';
const LABELS = {
  general:'General assessment', dental:'Dental assessment', optometry:'Optometry assessment', clearance:'Clinical clearance assessment',
  complaint:'Reason for consultation', findings:'Findings', assessment:'Assessment (free text)', plan:'Plan (free text)',
  referral:'Referral (free text)', follow_up_date:'Planned follow-up date', medications:'Medication notes', allergies:'Allergy notes', not_assessed:'Reported as not assessed',
  vitals:'Recorded measurements', systolic_mm_hg:'Systolic blood pressure', diastolic_mm_hg:'Diastolic blood pressure', pulse_bpm:'Heart rate',
  respiratory_rate_per_min:'Respiratory rate', temperature_celsius:'Body temperature', oxygen_saturation_pct:'Oxygen saturation',
  weight_kg:'Body weight', height_cm:'Body height', glucose_mg_dl:'Glucose (specimen and method unspecified)',
  tooth_notation:'Tooth numbering system', universal:'Universal tooth numbering', fdi:'FDI tooth numbering', teeth:'Tooth', number:'Tooth number',
  condition:'Tooth findings (free text)', treatment:'Tooth treatment notes (free text)', pain_score:'Pain score (0 to 10; instrument unspecified)',
  right_eye:'Right eye', left_eye:'Left eye', visual_acuity_uncorrected:'Uncorrected visual acuity (recorded text)', visual_acuity_corrected:'Corrected visual acuity (recorded text)',
  sphere_diopters:'Recorded spherical lens power', cylinder_diopters:'Recorded cylindrical lens power', axis_degrees:'Recorded cylinder axis',
  add_diopters:'Recorded additional lens power', intraocular_pressure_mm_hg:'Intraocular pressure', pupillary_distance_mm:'Pupillary distance',
  acuity_context:'Visual acuity context', distance:'Distance', near:'Near', both:'Distance and near',
  decision:'Clinician-recorded clearance decision', cleared:'Cleared', not_cleared:'Not cleared', deferred:'Deferred', restrictions:'Restrictions (free text)', reason:'Reason (free text)',
  synthetic:'Synthetic test data', documentation:'Clinical form documentation', source_form:'Clinical assessment form',
};
const VITALS = {
  systolic_mm_hg:['8480-6','mm[Hg]'], diastolic_mm_hg:['8462-4','mm[Hg]'], pulse_bpm:['8867-4','/min'],
  respiratory_rate_per_min:['9279-1','/min'], temperature_celsius:['8310-5','Cel'], oxygen_saturation_pct:['2708-6','%'],
  weight_kg:['29463-7','kg'], height_cm:['8302-2','cm'], glucose_mg_dl:[null,'mg/dL'],
};
const EYE = {visual_acuity_uncorrected:null,visual_acuity_corrected:null,sphere_diopters:'[diop]',cylinder_diopters:'[diop]',axis_degrees:'deg',add_diopters:'[diop]',intraocular_pressure_mm_hg:'mm[Hg]'};
const coding = code => ({system:SYSTEM,code,display:LABELS[code]});
const concept = code => ({coding:[coding(code)]});
const escape = value => String(value).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&apos;'}[c]));
const present = value => value !== null && value !== undefined && value !== '';
// Production source timestamps are UTC. The HTTP adapter converts SQL timestamps
// to Date using the DB session timezone before calling this mapper.
const instant = value => {const source=typeof value==='string'&&/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$/.test(value)?value.replace(' ','T')+'Z':value;const d = new Date(source); if(!value || !Number.isFinite(d.getTime()))v.fail('FHIR_SOURCE_DATE_INVALID',500);return d.toISOString();};
const narrative = text => ({status:'generated',div:`<div xmlns="http://www.w3.org/1999/xhtml"><p>${escape(text)}</p></div>`});
function field(key,type='text',options={}) {return {linkId:key,text:LABELS[key],type,...options};}
function fields(specialty) {
  const items=['complaint','findings','assessment','plan','referral','medications','allergies'].map(k=>field(k));
  items.push(field('follow_up_date','date'),field('not_assessed','string',{repeats:true}));
  if(['general','clearance'].includes(specialty))items.push(field('vitals','group',{item:Object.keys(VITALS).map(k=>field(k,'decimal'))}));
  if(specialty==='dental')items.push(field('tooth_notation','choice',{answerOption:['universal','fdi'].map(k=>({valueCoding:coding(k)}))}),field('pain_score','decimal'),field('teeth','group',{repeats:true,item:['number','condition','treatment'].map(k=>field(k,k==='number'?'string':'text'))}));
  if(specialty==='optometry'){
    for(const side of ['right_eye','left_eye'])items.push(field(side,'group',{item:Object.entries(EYE).map(([k,unit])=>field(`${side}.${k}`,unit?'decimal':'string'))}));
    items.push(field('pupillary_distance_mm','decimal'),field('acuity_context','choice',{answerOption:['distance','near','both'].map(k=>({valueCoding:coding(k)}))}));
  }
  if(specialty==='clearance')items.push(field('decision','choice',{answerOption:['cleared','not_cleared','deferred'].map(k=>({valueCoding:coding(k)}))}),field('restrictions'),field('reason'));
  // Nested eye linkIds must be globally unique, while their labels use the source key.
  for(const item of items)if(item.item)for(const child of item.item)child.text=LABELS[child.linkId.split('.').at(-1)];
  return items;
}
function responseItems(definitions,data){
  const result=[];
  for(const def of definitions){
    const key=def.linkId.split('.').at(-1),value=data?.[key];
    if(value===undefined)continue;
    if(def.type==='group'){
      for(const group of (def.repeats?(value||[]):[value])){
        const nested=responseItems(def.item,group);
        if(nested.length)result.push({linkId:def.linkId,text:def.text,item:nested});
      }
    }else{
      const item={linkId:def.linkId,text:def.text};
      const values=(def.repeats?(value||[]):[value]).filter(present);
      if(values.length)item.answer=values.map(x=>def.type==='choice'?{valueCoding:coding(x)}:def.type==='decimal'?{valueDecimal:x}:def.type==='date'?{valueDate:x}:{valueString:x});
      result.push(item);
    }
  }
  return result;
}

/** Export authorized synthetic records. All references are internal; no native credentials/URLs are serialized. */
function createFhirBundle({patient,records,exportedAt=new Date(),exporterId}) {
  if(![true,1].includes(patient.is_synthetic)||!/^CPTEST-[A-Za-z0-9-]+$/.test(patient.synthetic_code))v.fail('REAL_DATA_FORBIDDEN',403);
  const timestamp=instant(exportedAt),entries=new Map();
  const id=(type,key)=>uuid(`${patient.synthetic_code}:${type}:${key}`,NS);
  const ref=(type,key)=>({reference:`urn:uuid:${id(type,key)}`});
  const add=(type,key,resource)=>{
    const resourceId=id(type,key),fullUrl=`urn:uuid:${resourceId}`;
    const result={resourceType:type,id:resourceId,meta:{tag:[coding('synthetic')]},...resource};
    entries.set(fullUrl,{fullUrl,resource:result});return {reference:fullUrl};
  };
  const patientRef=add('Patient','patient',{
    text:narrative(`Synthetic test patient: ${patient.display_name}. Not for clinical use.`),
    identifier:[{system:`${BASE}/identifier/patient`,value:patient.synthetic_code}],
    name:[{text:patient.display_name}],birthDate:String(patient.date_of_birth).slice(0,10),
    gender:({male:'male',female:'female',other:'other',unknown:'unknown'})[String(patient.sex).toLowerCase()]||'unknown',
  });
  add('CodeSystem','terminology',{text:narrative('Local clinical form vocabulary. These codes do not assert equivalence to SNOMED CT or LOINC.'),url:SYSTEM,version:VERSION,name:'CommunityClinicalSandbox',status:'draft',experimental:true,caseSensitive:true,content:'complete',concept:Object.entries(LABELS).map(([code,display])=>({code,display}))});
  const sourceData=new Map(records.map(record=>[record.id,v.clinicalData(record.specialty,typeof record.data==='string'?JSON.parse(record.data):record.data)]));
  if([...sourceData.values()].some(data=>data.template_version===2))add('CodeSystem','template-terminology',{text:narrative('Local identifiers for the versioned clinical documentation templates. Their source references do not establish clinical validation.'),...templateFhir.terminology()});
  const practitioner = userId => {
    if(!Number.isSafeInteger(Number(userId))||Number(userId)<1)v.fail('FHIR_SOURCE_AUTHOR_INVALID',500);
    return add('Practitioner',String(userId),{text:narrative(`System operator ${userId}; professional qualification not asserted.`),identifier:[{system:`${BASE}/identifier/operator`,value:String(userId)}]});
  };
  const questionnaires=new Map();
  for(const specialty of new Set(records.filter(r=>sourceData.get(r.id).template_version!==2).map(r=>r.specialty))){
    const definition=fields(specialty),url=`${BASE}/Questionnaire/${specialty}`;
    add('Questionnaire',specialty,{text:narrative(`Synthetic ${LABELS[specialty]}. Form version ${VERSION}; requires clinician review before real use.`),url,version:VERSION,name:`Community_${specialty}`,title:LABELS[specialty],status:'draft',experimental:true,subjectType:['Patient'],item:definition});
    questionnaires.set(specialty,{definition,url,version:VERSION});
  }
  for(const record of records){
    const data=sourceData.get(record.id),template=templates.templateFor(record.specialty,data);
    if(!template||questionnaires.has(template.id))continue;
    const url=`${BASE}/Questionnaire/${template.id}`;
    const sources=template.sources.map(source=>`${source.title}: ${source.url}`).join('\n');
    add('Questionnaire',template.id,{text:narrative(`${template.en}. Adaptation for synthetic testing; not a clinically validated instrument. ${sources}`),url,version:templateFhir.VERSION,name:'Community_'+template.id.replace(/-/g,'_'),title:template.en,status:'draft',experimental:true,subjectType:['Patient'],description:`${template.description_en}\n${sources}`,item:templateFhir.definition(template)});
    questionnaires.set(template.id,{url,version:templateFhir.VERSION,template});
  }
  const byId=new Map(records.map(r=>[r.id,r]));
  const rootOf = record => {
    const seen=new Set();let current=record;
    while(current.supersedes_record_id){
      if(seen.has(current.id))v.fail('FHIR_SOURCE_REVISION_INVALID',500);seen.add(current.id);
      const parent=byId.get(current.supersedes_record_id);
      if(!parent||parent.specialty!==record.specialty)v.fail('FHIR_SOURCE_REVISION_INVALID',500);current=parent;
    }return current;
  };
  for(const record of records){
    if(Number(record.patient_id)!==Number(patient.id)||Number(record.event_id)!==Number(patient.event_id))v.fail('FHIR_SOURCE_PATIENT_MISMATCH',500);
    if(!['draft','final'].includes(record.status))v.fail('FHIR_SOURCE_STATUS_INVALID',500);
    const data=sourceData.get(record.id),template=templates.templateFor(record.specialty,data);
    const root=rootOf(record),isFinal=record.status==='final',amended=isFinal&&!!record.supersedes_record_id;
    const encounterRef=add('Encounter',root.id,{text:narrative(`Synthetic ${LABELS[record.specialty]}. Encounter time and clinical encounter status were not captured; document status is recorded separately.`),identifier:[{system:`${BASE}/identifier/encounter`,value:root.id}],status:'unknown',class:{system:'http://terminology.hl7.org/CodeSystem/v3-ActCode',code:'AMB',display:'ambulatory'},type:[concept(record.specialty)],subject:patientRef});
    const recorder=practitioner(record.recorded_by),author=practitioner(record.content_author_id||record.recorded_by),q=questionnaires.get(template?.id||record.specialty),recorded=instant(record.updated_at||record.created_at);
    const answers=template?templateFhir.answers(template,data):responseItems(q.definition,data);
    const qrRef=add('QuestionnaireResponse',record.id,{
      text:narrative(`Synthetic ${LABELS[record.specialty]}; ${record.status}; revision ${record.revision}. Narrative allergy, medication, diagnosis and treatment entries are clinician text, not coded assertions. See structured answers.`),
      identifier:{system:`${BASE}/identifier/record`,value:record.id},questionnaire:`${q.url}|${q.version}`,
      status:amended?'amended':isFinal?'completed':'in-progress',subject:patientRef,encounter:encounterRef,authored:instant(record.content_authored_at||record.created_at),author,
      ...(answers.length?{item:answers}:{}),
    });
    const targets=[qrRef];
    const observation=(key,code,value,unit,side=null,components=null,context={})=>{
      const resource={text:narrative(`Synthetic ${context.label||LABELS[key]||key}. See source questionnaire for measurement context.`),status:amended?'amended':isFinal?'final':'preliminary',
        code:code?{coding:[{system:LOINC,code}]}:context.label?{coding:[templateFhir.code(key,context.label)]}:concept(key),subject:patientRef,encounter:encounterRef,issued:recorded,derivedFrom:[qrRef],
        note:[{text:context.label?'Issued is documentation time. Measurement time is included only when explicitly entered in the source form.':'Source form does not capture measurement time or method. Issued is documentation time.'}],
      };
      // R4's vital-sign rules apply to these LOINC codes. Explicitly represent the
      // missing measurement time rather than misusing the documentation timestamp.
      if(code){
        resource.category=[{coding:[{system:'http://terminology.hl7.org/CodeSystem/observation-category',code:'vital-signs'}]}];
        resource.effectivePeriod={extension:[{url:'http://hl7.org/fhir/StructureDefinition/data-absent-reason',valueCode:'unknown'}]};
      }
      if(context.effectiveDateTime){delete resource.effectivePeriod;resource.effectiveDateTime=context.effectiveDateTime;}
      if(context.method)resource.method={text:context.method};
      if(side)resource.bodySite=concept(side);
      if(components)resource.component=components;
      else if(unit)resource.valueQuantity={value,unit,system:UCUM,code:unit};
      else if(typeof value==='number')resource.valueQuantity={value};
      else resource.valueString=value;
      targets.push(add('Observation',`${record.id}:${side||''}:${key}`,resource));
    };
    const vital=data.vitals||{};
    if(present(vital.systolic_mm_hg)||present(vital.diastolic_mm_hg)){
      const components=['systolic_mm_hg','diastolic_mm_hg'].map(key=>({code:{coding:[{system:LOINC,code:VITALS[key][0]}]},...(present(vital[key])?{valueQuantity:{value:vital[key],unit:'mm[Hg]',system:UCUM,code:'mm[Hg]'}}:{dataAbsentReason:{coding:[{system:'http://terminology.hl7.org/CodeSystem/data-absent-reason',code:'unknown'}]}})}));
      observation('Blood pressure','85354-9',null,null,null,components);
    }
    for(const [key,[code,unit]] of Object.entries(VITALS))if(!['systolic_mm_hg','diastolic_mm_hg'].includes(key)&&present(vital[key]))observation(key,code,vital[key],unit);
    for(const side of ['right_eye','left_eye'])for(const [key,unit] of Object.entries(EYE))if(present(data[side]?.[key]))observation(key,null,data[side][key],unit,side);
    if(present(data.pupillary_distance_mm))observation('pupillary_distance_mm',null,data.pupillary_distance_mm,'mm');
    if(present(data.pain_score))observation('pain_score',null,data.pain_score,null);
    if(template){
      const definitions=templates.fields(template),values=data.template_fields;
      const effectiveKey=definitions.find(field=>field.fhir?.kind==='effectiveDateTime')?.key;
      const effectiveDateTime=effectiveKey&&values[effectiveKey]||undefined;
      const pressure=definitions.filter(field=>['8480-6','8462-4'].includes(field.fhir?.loinc));
      if(pressure.some(field=>present(values[field.key]))){
        const components=['8480-6','8462-4'].map(code=>{const field=pressure.find(f=>f.fhir.loinc===code),value=field&&values[field.key];return {code:{coding:[{system:LOINC,code}]},...(present(value)?{valueQuantity:{value,unit:'mm[Hg]',system:UCUM,code:'mm[Hg]'}}:{dataAbsentReason:{coding:[{system:'http://terminology.hl7.org/CodeSystem/data-absent-reason',code:'unknown'}]}})};});
        observation('blood_pressure','85354-9',null,null,null,components,{label:'Blood pressure',effectiveDateTime});
      }
      for(const field of definitions){
        const value=values[field.key],mapping=field.fhir;
        if(!mapping||mapping.kind==='effectiveDateTime'||pressure.includes(field)||!present(value))continue;
        const side=({OD:'right_eye',OS:'left_eye',right:'right_eye',left:'left_eye',right_eye:'right_eye',left_eye:'left_eye'})[mapping.bodySite]||null;
        observation(field.key,mapping.loinc||null,value,mapping.unit||null,side,null,{label:field.en,effectiveDateTime,method:mapping.method});
      }
    }
    for(const attachment of record.attachments||[]){
      if(!Buffer.isBuffer(attachment.buffer)||attachment.buffer.length!==Number(attachment.size_bytes)||v.hash(attachment.buffer)!==attachment.sha256)v.fail('ATTACHMENT_INTEGRITY_ERROR',502);
      const a={contentType:attachment.mime_type,data:attachment.buffer.toString('base64'),size:attachment.buffer.length,title:attachment.filename,hash:crypto.createHash('sha1').update(attachment.buffer).digest('base64')};
      const documentRef=add('DocumentReference',attachment.id,{text:narrative(`Synthetic attachment: ${attachment.filename}. Original document author and creation time were not captured.`),identifier:[{system:`${BASE}/identifier/attachment`,value:attachment.id}],status:'current',subject:patientRef,date:instant(attachment.created_at),content:[{attachment:a}],context:{encounter:[encounterRef],related:[qrRef]}});
      add('Provenance',`attachment:${attachment.id}`,{text:narrative('Attachment upload; the uploader is not asserted to be the original document author.'),target:[documentRef],recorded:instant(attachment.created_at),activity:{text:'Attachment upload'},agent:[{who:practitioner(attachment.uploaded_by)}]});
    }
    const agents=[{type:{coding:[{system:'http://terminology.hl7.org/CodeSystem/provenance-participant-type',code:'enterer'}]},who:recorder},{type:{coding:[{system:'http://terminology.hl7.org/CodeSystem/provenance-participant-type',code:'author'}]},who:author}];
    if(isFinal)agents.push({type:{coding:[{system:'http://terminology.hl7.org/CodeSystem/provenance-participant-type',code:'verifier'}]},who:practitioner(record.finalized_by)});
    add('Provenance',record.id,{text:narrative(`Clinical form revision ${record.revision}. Enterer identifies the original recorder; author identifies the last content editor; verifier identifies the operator who finalized it. This is not a digital signature.`),target:targets,recorded,activity:concept('documentation'),agent:agents,
      ...(record.supersedes_record_id?{entity:[{role:'revision',what:ref('QuestionnaireResponse',record.supersedes_record_id)}],reason:[{text:record.amendment_reason||'Amendment'}]}:{}),
    });
  }
  const exporter=practitioner(exporterId);
  add('Provenance',`export:${timestamp}`,{text:narrative(`FHIR R4 ${VERSION} export of synthetic source records. Local terminology is preserved without inferring clinical codes. Draft edit history is available separately in the application.`),target:[patientRef,...records.map(r=>ref('QuestionnaireResponse',r.id))],recorded:timestamp,agent:[{who:exporter}],activity:{text:'FHIR R4 serialization'}});
  return {resourceType:'Bundle',id:uuid(`${patient.synthetic_code}:export:${timestamp}`,NS),meta:{tag:[coding('synthetic')]},identifier:{system:`${BASE}/identifier/export`,value:`${patient.synthetic_code}:${timestamp}`},type:'collection',timestamp,entry:[...entries.values()]};
}
module.exports={createFhirBundle,BASE,SYSTEM,VERSION};
