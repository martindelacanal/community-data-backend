'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const {createChatbotService, safeSource} = require('./chatbot');
const {retrieveContext} = require('./chatbotKnowledge');
const {healthFairRequest, asksAboutProduceTotal, asksAboutWeeklyProduce, impactReply} = require('./chatbotPublicListings');

const example = {id:71, slug:'sample-fair', name_es:'Feria de salud', name_en:'Health fair',
  start_date:'2099-12-10', end_date:'2099-12-12', timezone:'America/Los_Angeles', community_city:'Example City: Hall',
  address:'123 Example St, 00000', internal_note:'private'};
function published(rows) {
  return {query:async sql=>{
    for (const flag of ['enabled','landing_enabled','public_home_visible']) assert.ok(sql.includes("he."+flag+" = 'Y'"));
    return [rows];
  }};
}
test('basic health fair lookups use current public events in the requested city without Google', async ()=>{
  const db = published([example,{...example,id:72,slug:'old',end_date:'2000-01-01'}]);
  const service = createChatbotService({env:{}});
  const retrieve = args=>retrieveContext({...args,db});
  for (const question of ['¿Cuál es la próxima feria de salud de Bienestar Community?', 'Health fairs in Example City', 'Example City Health Fair', 'Health events in 00000']) {
    const answer = await service.answer({question, retrieve});
    assert.equal(answer.model,null);
    assert.deepEqual(answer.sources.map(s=>s.url),['/health-event/sample-fair']);
    assert.match(answer.content,/2099-12-10/);
  }
  const empty = await service.answer({question:'Any health fairs in Elsewhere?',retrieve});
  assert.deepEqual(empty.sources.map(s=>s.url),['/home']);
  assert.match(empty.content,/no health fair/i);
  const nearby = await service.answer({question:'Health fairs near me',retrieve:()=>assert.fail('No city provided')});
  assert.match(nearby.content,/city|ZIP/i);
});
test('qualified health fair searches give the generator only public event fields', async ()=>{
  for (const question of ['Health fairs in November', 'How can I organize a health fair?', 'What is a health fair?', 'Find a medical clinic']) {
    assert.equal(healthFairRequest(question),null,question);
  }
  const passages = await retrieveContext({db:published([example]),question:'What address is the event at?',route:'health_fairs',locale:'en'});
  assert.equal(safeSource(passages[0]).url,'/health-event/sample-fair');
  assert.match(passages[0].text,/123 Example St/);
  assert.doesNotMatch(passages[0].text,/private|internal_note/);
});
test('cumulative food figures stay separate from weekly, personal and third-party figures', async ()=>{
  for (const q of ['How many pounds of food has Bienestar Community distributed historically?', '¿Cuántas libras de alimentos ha distribuido Bienestar?']) {
    assert.ok(asksAboutProduceTotal(q));
  }
  const weekly = 'How many pounds of produce does Bienestar distribute each week?';
  assert.equal(asksAboutWeeklyProduce(weekly),true);
  for (const q of [weekly,'How many pounds of food does my family get?', 'How many pounds of food has Another Organization distributed?', 'How many pounds of produce distributed in 2024?']) assert.equal(asksAboutProduceTotal(q),false,q);
  const source = await impactReply('en',{fetcher:async url=>{
    assert.equal(url,'https://bienestarcommunity.org/assets/i18n/en.json');
    return {ok:true,json:async()=>({who_we_are_our_story_highlight:'We have distributed 12,345 pounds of food.'})};
  }});
  assert.match(source.content,/12,345/);
  assert.equal(source.sources[0].url,'/who-we-are');
  const unavailable = await impactReply('es',{fetcher:async()=>{throw Error('offline');}});
  assert.equal(unavailable.sources[0].url,'/who-we-are');
  assert.doesNotMatch(unavailable.content,/\d/);
  const answer = await createChatbotService({env:{}}).answer({question:weekly});
  assert.equal(answer.model,null);
  assert.match(answer.content,/per week/);
  assert.doesNotMatch(answer.content,/\d/);
});
