'use strict';
const {catalog,fields} = require('./clinicalTemplates');
const BASE='https://bienestarcommunity.org/fhir/sandbox';
const SYSTEM=`${BASE}/CodeSystem/clinical-template-v2`;
const VERSION='2.0.0';
// Shared SOAP fields have one meaning but different form labels. Coding.display
// must match our CodeSystem; the original source label remains in item.text.
const displays=new Map();
for(const template of catalog.templates)for(const field of fields(template)){
  if(!displays.has(field.key))displays.set(field.key,field.en);
  for(const option of field.options||[])if(!displays.has(`${field.key}--${option.value}`))displays.set(`${field.key}--${option.value}`,option.en);
}
const code=(key,display)=>({system:SYSTEM,code:key,display:displays.get(key)||display});
const answered=value=>value!==null&&value!==undefined&&value!=='';
function definition(template){
  const items=template.sections.map(section=>({linkId:section.id,text:section.en,type:'group',item:section.fields.map(field=>({
    linkId:field.key,text:field.en,type:({number:'decimal',select:'choice',date:'date',datetime:'dateTime',textarea:'text'})[field.type]||'string',
    code:[code(field.key,field.en)],
    ...(field.type==='select'?{answerOption:field.options.map(option=>({valueCoding:code(`${field.key}--${option.value}`,option.en)}))}:{}),
    ...(field.fhir?.unit?{extension:[{url:'http://hl7.org/fhir/StructureDefinition/questionnaire-unit',valueCoding:{system:'http://unitsofmeasure.org',code:field.fhir.unit}}]}:{}),
  }))}));
  items.push({linkId:'not_assessed',text:'Sections explicitly not assessed',type:'string',repeats:true});
  if(template.tooth_chart){
    items.push({linkId:'tooth_notation',text:'Tooth numbering convention',type:'choice',answerOption:['universal','fdi'].map(value=>({valueCoding:code(`tooth_notation--${value}`,value==='universal'?'Universal (US)':'FDI')}))});
    items.push({linkId:'teeth',text:'Per-tooth documentation adapted from SF603',type:'group',repeats:true,item:[{linkId:'tooth_number',text:'Tooth number',type:'string'},{linkId:'tooth_condition',text:'Tooth findings',type:'text'},{linkId:'tooth_treatment',text:'Treatment notes',type:'text'}]});
  }
  return items;
}
function answers(template,data){
  const result=[];
  for(const section of template.sections){
    const item=[];
    for(const field of section.fields){
      const value=data.template_fields[field.key];
      if(value===undefined)continue;
      const response={linkId:field.key,text:field.en};
      if(answered(value)){
        const property=({number:'valueDecimal',date:'valueDate',datetime:'valueDateTime'})[field.type]||'valueString';
        const option=field.type==='select'?field.options.find(o=>o.value===value):null;
        response.answer=[option?{valueCoding:code(`${field.key}--${value}`,option.en)}:{[property]:value}];
      }
      item.push(response);
    }
    if(item.length)result.push({linkId:section.id,text:section.en,item});
  }
  if(data.not_assessed?.length)result.push({linkId:'not_assessed',text:'Sections explicitly not assessed',answer:data.not_assessed.map(value=>({valueString:value}))});
  if(data.tooth_notation)result.push({linkId:'tooth_notation',text:'Tooth numbering convention',answer:[{valueCoding:code(`tooth_notation--${data.tooth_notation}`,data.tooth_notation==='universal'?'Universal (US)':'FDI')}]});
  for(const tooth of data.teeth||[])result.push({linkId:'teeth',text:'Per-tooth documentation adapted from SF603',item:['number','condition','treatment'].filter(key=>answered(tooth[key])).map(key=>({linkId:`tooth_${key}`,answer:[{valueString:tooth[key]}]}))});
  return result;
}
function terminology(){
  const concepts=new Map();
  const add=(key,display)=>{if(!concepts.has(key))concepts.set(key,{code:key,display});};
  for(const template of catalog.templates)for(const field of fields(template)){
    add(field.key,field.en);
    for(const option of field.options||[])add(`${field.key}--${option.value}`,option.en);
  }
  add('tooth_notation--universal','Universal (US)');add('tooth_notation--fdi','FDI');
  return {url:SYSTEM,version:VERSION,name:'CommunityClinicalTemplatesV2',status:'draft',experimental:true,caseSensitive:true,content:'complete',concept:[...concepts.values()]};
}
module.exports={SYSTEM,VERSION,definition,answers,terminology,code};
