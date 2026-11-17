'use strict';

const {test} = require('node:test');
const assert = require('node:assert/strict');
const {createDecisionRouter} = require('./chatbotDecisions');

const choice = (value, confidence = 0.99) => ({type:'choice', choice:value, confidence});
async function classify(question, overrides, history = []) {
  const answers = {policy:choice('allowed'), language:choice('es'), source:choice('calendar'), locality:choice('none'), ...overrides};
  const calls = [];
  const decide = createDecisionRouter({post:async (url, body) => {
    calls.push({url, body});
    return {data:{answers,usage:{total_input_tokens:490,total_output_tokens:0}}};
  }}, () => 'test-key');
  const result = await decide({question,history,fallbackLanguage:'es'});
  assert.equal(calls.length,1);
  assert.deepEqual(Object.keys(calls[0].body.questions).sort(), ['language','locality','policy','source']);
  assert.equal(result.inputTokens,490);
  return result;
}

test('a high-confidence request for a public landing page uses its section without enrichment', async () => {
  for (const [question, route] of [
    ['Quisiera visitar la página del calendario.', 'calendar'],
    ['Take me to the site’s articles section.', 'articles'],
    ['¿Me pasas el enlace al directorio de servicios?', 'resources']
  ]) {
    const result = await classify(question,{source:choice('open_' + route)});
    assert.equal(result.route,route);
    assert.equal(result.data.openSection,true);
    assert.equal(result.data.allowed,true);
    assert.equal(result.enrich,false);
  }
});

test('topic, locality and uncertain navigation keep the search and existing fallback paths', async () => {
  for (const [question, source, locality, policy, route, enrich] of [
    ['¿Qué publicaron sobre diabetes?', choice('articles'), choice('none'), choice('allowed'), 'articles', false],
    ['¿Dónde retiro alimentos mañana?', choice('calendar'), choice('none'), choice('allowed'), 'calendar', false],
    ['Llévame a recursos de Riverside.', choice('open_resources'), choice('explicit'), choice('allowed'), 'resources', true],
    ['¿Qué hay cerca?', choice('open_resources'), choice('nearby'), choice('allowed'), 'resources', false],
    ['Muéstrame eso.', choice('open_calendar',.82), choice('none'), choice('allowed'), 'calendar', false],
    ['Me llevas al directorio?', choice('open_resources'), choice('none'), choice('allowed',.85), 'resources', false],
    ['Open the article?', choice('open_articles'), choice('none',.5), choice('allowed'), 'articles', true],
    ['Muéstrame eso.', choice('open_calendar',.4), choice('none'), choice('allowed'), undefined, undefined],
    ['Show me…', choice(['open_articles']), choice('none'), choice('allowed'), undefined, undefined]
  ]) {
    const result = await classify(question,{source,locality,policy});
    assert.equal(result.data?.openSection,undefined);
    assert.equal(result.route,route);
    assert.equal(result.enrich,enrich);
    if (route) assert.equal(result.data.locationMentioned,locality.choice==='explicit');
  }
});

test('standalone appreciation and farewells require agreement between policy and source', async () => {
  for (const social of ['thanks','goodbye']) {
    const result = await classify('Thank you very much!',{policy:choice('social'),source:choice(social)});
    assert.equal(result.data.social,social); assert.equal(result.data.allowed,true); assert.equal(result.enrich,false);
    for (const [policy,source] of [[choice('social',.85),choice(social)],[choice('social'),choice('articles')],[choice('social'),choice(social,.61)]]) {
      const uncertain = await classify('Thanks, and that article?',{policy,source});
      assert.equal(uncertain.data,undefined);
    }
  }
});

test('equally plausible supported destinations request clarification only after the policy check', async () => {
  const ambiguous = await classify('Dame la cartelera y los artículos.',{source:choice('clarify')});
  assert.equal(ambiguous.data.clarify,true); assert.equal(ambiguous.enrich,false);
  for (const source of [choice('clarify',.81),choice('other')]) {
    const uncertain = await classify('Muéstrame eso.',{source});
    assert.equal(uncertain.data,undefined);
  }
  const danger = await classify('First help with this emergency, then open those.',{policy:choice('emergency'),source:choice('clarify')});
  assert.equal(danger.data.medicalEmergency,true); assert.equal(danger.data.clarify,false);
});

test('split probabilities can support a source or a social response without opening a page', async () => {
  const split = {...choice('open_resources',.72),probabilities:{open_resources:.72,resources:.25}};
  const lookup = await classify('Open the services near me.',{source:split,locality:choice('nearby')});
  assert.equal(lookup.route,'resources'); assert.equal(lookup.data.openSection,undefined); assert.equal(lookup.data.needsLocality,true);
  const thanks = {...choice('thanks',.63),probabilities:{thanks:.63,goodbye:.34}};
  const social = await classify('Mil gracias por todo.',{policy:choice('social'),source:thanks});
  assert.equal(social.data.social,'thanks');
  for (const invalid of [null, -.1, 1.5, .9]) {
    const result = await classify('Show me...', {source:{...split,probabilities:{open_resources:.72,resources:invalid}}});
    assert.equal(result.data,undefined);
  }
});

test('language uncertainty preserves a confident safety decision and falls back for ordinary requests', async () => {
  for (const policy of ['emergency','refused','allowed']) {
    const result = await classify('SOS',{policy:choice(policy),language:choice('en',.6)});
    if (policy==='allowed') assert.equal(result.data,undefined);
    else {
      assert.equal(result.data.language,'es');
      assert.equal(result.data.allowed,false);
      assert.equal(result.data.medicalEmergency,policy==='emergency');
    }
  }
});

test('emergencies, policy refusals and uncertainty take precedence over public navigation', async () => {
  for (const policy of ['emergency','refused','clarify']) {
    const result = await classify('One more thing before you show the calendar…',{source:choice('open_calendar'),policy:choice(policy)});
    assert.equal(result.data.allowed,false); assert.equal(result.data.openSection,undefined);
    assert.equal(result.data.social,undefined); assert.equal(result.route,undefined);
    assert.equal(result.data.medicalEmergency,policy==='emergency');
    assert.equal(result.data.clarify,policy==='clarify');
  }
});

test('nearby catalog ambiguity asks for the city without guessing a source or forgetting an earlier location', async () => {
  const overrides = {
    source:{...choice('calendar',.58), probabilities:{calendar:.58,open_calendar:.14,resources:.13,open_resources:.01}},
    locality:choice('nearby',.98)
  };
  const question = 'Is there a food pickup near me?';
  const first = await classify(question, overrides);
  assert.equal(first.data.needsLocality,true); assert.equal(first.route,undefined); assert.equal(first.enrich,false);
  const followup = await classify(question, overrides, [{role:'user',text:'I live in Riverside.'}]);
  assert.equal(followup.data,undefined);
  assert.equal((await classify('Where is the next pickup?',overrides)).data,undefined);
  const uncertain = {...overrides, source:{...choice('calendar',.48),probabilities:{calendar:.48,open_calendar:.14,resources:.13,open_resources:.01}}};
  assert.equal((await classify(question,uncertain)).data,undefined);
});
