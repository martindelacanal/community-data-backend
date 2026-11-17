'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createProviderUsage } = require('./chatbotProviderUsage');
const { createChatbotService } = require('./chatbot');
const { createDecisionRouter } = require('./chatbotDecisions');
const { reuseCatalogDecisions } = require('./chatbotDecisionCache');
const { embed } = require('./chatbotKnowledge');

const env = { GEMINI_API_KEY: 'fixture-key' };
const choice = value => ({ type:'choice', choice:value, confidence:.99 });
function decision(source) {
  return { data:{ answers:{ policy:choice('allowed'), source:choice(source), language:choice('en'), locality:choice('none') },
    usage:{total_input_tokens:73, total_output_tokens:0} } };
}
function generation(text, usageMetadata) {
  return { data:{candidates:[{finishReason:'STOP', content:{parts:[{text}]}}], usageMetadata} };
}

test('audit keeps billed stages when fallback succeeds but the final answer is invalid', async () => {
  const usage = createProviderUsage();
  const quota = Object.assign(new Error('private provider message'), {response:{status:429}});
  const outputs = [
    quota,
    generation(JSON.stringify({allowed:true, medicalEmergency:false, language:'en'}),
      {promptTokenCount:50,candidatesTokenCount:4,thoughtsTokenCount:2,cachedContentTokenCount:10}),
    generation('private malformed output', {promptTokenCount:60,candidatesTokenCount:3})
  ];
  const service = createChatbotService({env,http:{post:async () => {
    const response = outputs.shift();
    if (response instanceof Error) throw response;
    return response;
  }}});
  await assert.rejects(service.answer({question:'Explain the public hydration material.',providerUsage:usage,
    retrieve:async () => [{id:'article:1:0',title:'Community',url:'/article/example',text:'private source excerpt'}]}),
    {code:'chatbot_invalid_response'});
  const [failed,scope,answer] = usage.calls;
  assert.deepEqual(usage.calls.map(c=>c.stage),['decisions','scope','answer']);
  assert.equal(failed.httpStatus,429);
  assert.equal(failed.outcome,'failed');
  assert.equal(failed.inputTokens,null);
  assert.equal(scope.inputTokens,50);
  assert.equal(scope.outputTokens,6);
  assert.equal(scope.cachedInputTokens,10);
  assert.equal(scope.thinkingTokens,2);
  assert.equal(answer.outcome,'received');
  assert.equal(answer.inputTokens,60);
  assert.equal(answer.outputTokens,3);
  assert.doesNotMatch(JSON.stringify(usage), /fixture-key|private|hydration|malformed/i);

  const next = createProviderUsage();
  outputs.push(generation(JSON.stringify({allowed:false,medicalEmergency:false,language:'en'}),{}));
  await service.answer({question:'Give me stock forecasts.',providerUsage:next,retrieve:()=>assert.fail()});
  assert.equal(next.decisionCooldown,true);
  assert.deepEqual(next.calls.map(c=>c.stage),['scope']);
  assert.equal(next.calls[0].outputTokens,null);
});

test('shared and cached decisions incur a single call for the initiating request', async () => {
  let release;
  const deferred = new Promise(resolve => { release = resolve; });
  const decide = reuseCatalogDecisions(createDecisionRouter({post:()=>deferred},()=>env.GEMINI_API_KEY));
  const input = {question:'Find support organizations.',history:[],fallbackLanguage:'en'};
  const first = createProviderUsage(), shared = createProviderUsage(), cached = createProviderUsage();
  const leader = decide(input,first);
  const follower = decide(input,shared);
  release(decision('resources'));
  await Promise.all([leader,follower]);
  await decide(input,cached);
  assert.equal(first.calls.length,1);
  assert.equal(first.calls[0].inputTokens,73);
  assert.equal(first.calls[0].outputTokens,0);
  assert.deepEqual(shared,{version:1,calls:[],decisionCache:'coalesced'});
  assert.deepEqual(cached,{version:1,calls:[],decisionCache:'hit'});
});

test('educational retrieval keeps extraction failures and counts query embeddings with unknown input usage', async () => {
  const providerUsage = createProviderUsage();
  const service = createChatbotService({env,http:{post:async url => {
    if (url.endsWith('/decisions')) return decision('education');
    throw Object.assign(new Error('timeout'),{code:'ETIMEDOUT'});
  }}});
  const answer = await service.answer({question:'What is blood pressure?',providerUsage,retrieve:async () => {
    await embed(['private lookup text'],'RETRIEVAL_QUERY',env,{
      post:async () => ({data:{embeddings:[{values:Array(768).fill(1)}]}})
    },providerUsage);
    return [];
  }});
  assert.equal(answer.status,'no_context');
  assert.equal(providerUsage.classifiedRoute,'education');
  assert.deepEqual(providerUsage.calls.map(c=>c.stage),['decisions','extract','embedding']);
  assert.equal(providerUsage.calls[1].outcome,'failed');
  assert.equal(providerUsage.calls[1].outputTokens,null);
  assert.equal(providerUsage.calls[2].inputTokens,null);
  assert.equal(providerUsage.calls[2].outputTokens,0);
  assert.doesNotMatch(JSON.stringify(providerUsage),/private|pressure|timeout/);
});
