'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const express=require('express');
const jwt=require('jsonwebtoken');
const crypto=require('node:crypto');
const {createChatbotRouter}=require('./chatbot');
const {MAX_SOURCES,MAX_TOTAL_CHUNKS}=require('../services/chatbotLimits');

const env={JWT_SECRET:'test-key-chatbot-not-real',GEMINI_API_KEY:'test-key'};
const token=(role='admin',id=1)=>jwt.sign({data:JSON.stringify({id,role})},env.JWT_SECRET,{algorithm:'HS256',expiresIn:'1h'});
const privateId='fe0b7a88-7eec-4f93-8552-6f1510b9f34e';
async function fixture({role='admin',publicMode='hidden',enabled=true,deleted=false,replayRows=null}={}){
  const calls=[];
  const db={query:async(sql,params=[])=>{
    calls.push({sql,params});
    if(sql.includes('FROM user u JOIN role'))return[deleted?[]:[{id:1,role,enabled:'Y',deleted:'N'}]];
    if(sql.includes('FROM chatbot_settings'))return[[{id:1,enabled:enabled?1:0,public_mode:publicMode,allowed_roles:JSON.stringify(['admin','contentmanager']),system_prompt:'PRIVATE ADMIN PROMPT',revision:1}]];
    if(sql.includes('FROM role'))return[[{id:1,name:'admin'},{id:2,name:'beneficiary'},{id:3,name:'contentmanager'}]];
    if(sql.includes('FROM chatbot_conversation c')){
      const correct=crypto.createHmac('sha256',env.JWT_SECRET).update('user:1').digest('hex');
      if(params[0]===privateId&&params[1]===correct)return[[{id:privateId,owner_key:correct,title:'Mine',preview:'Hello',message_count:2}]];
      return[[]];
    }
    if(sql.includes('FROM chatbot_message'))return[replayRows||[{id:'m1',role:'assistant',content:'Hello',sources:'[]',status:'complete',model:'private-model',input_tokens:99}]];
    throw new Error(`Unexpected SQL: ${sql}`);
  }};
  const app=express();app.use(express.json());app.use('/api/chatbot',createChatbotRouter({pool:db,env}));
  const server=await new Promise(resolve=>{const listening=app.listen(0,'127.0.0.1',()=>resolve(listening));});
  const base=`http://127.0.0.1:${server.address().port}/api/chatbot`;
  return{calls,request:(path,options={})=>fetch(base+path,options),close:async()=>{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}};
}
test('public rollout is hidden and does not leak the secret prompt or role settings',async()=>{
  const f=await fixture();try{
    const response=await f.request('/config'),data=await response.json();assert.equal(response.status,200);assert.equal(data.visible,false);assert.equal(data.canChat,false);
    assert.equal(data.systemPrompt,undefined);assert.equal(data.allowedRoles,undefined);assert.equal(data.maxMessageChars,2000);
    const conversation=await f.request('/conversations');assert.equal(conversation.status,401);
  }finally{await f.close();}
});
test('public login-required mode exposes only the locked launcher',async()=>{
  const f=await fixture({publicMode:'login_required'});try{
    const data=await(await f.request('/config')).json();assert.equal(data.visible,true);assert.equal(data.requiresLogin,true);assert.equal(data.canChat,false);
  }finally{await f.close();}
});
test('current database role wins over stale elevated JWT role',async()=>{
  const f=await fixture({role:'beneficiary'});try{
    const headers={Authorization:`Bearer ${token('admin')}`};
    const data=await(await f.request('/config',{headers})).json();assert.equal(data.visible,false);
    assert.equal((await f.request('/admin/settings',{headers})).status,403);
    assert.equal((await f.request('/conversations',{headers})).status,403);
  }finally{await f.close();}
});
test('disabled/deleted accounts and invalid JWTs cannot obtain config or admin access',async()=>{
  const f=await fixture({deleted:true});try{
    assert.equal((await f.request('/admin/settings',{headers:{Authorization:`Bearer ${token()}`}})).status,401);
    assert.equal((await f.request('/config',{headers:{Authorization:'Bearer broken'}})).status,401);
  }finally{await f.close();}
});
test('admin prompt is available only at admin endpoint; ordinary history omits audit metadata',async()=>{
  const f=await fixture();try{
    const headers={Authorization:`Bearer ${token()}`};
    const data=await(await f.request('/admin/settings',{headers})).json();assert.equal(data.systemPrompt,'PRIVATE ADMIN PROMPT');
    const history=await(await f.request(`/conversations/${privateId}`,{headers})).json();assert.equal(history.messages[0].content,'Hello');assert.equal(history.messages[0].model,undefined);
    assert.ok(f.calls.find(call=>call.sql.includes('AND c.owner_key=?')));
  }finally{await f.close();}
});
test('public users with valid random sessions cannot read another owner conversation',async()=>{
  const f=await fixture({publicMode:'enabled'});try{
    const headers={'X-Chatbot-Session':crypto.randomUUID()};
    assert.equal((await f.request(`/conversations/${privateId}`,{headers})).status,404);
    assert.equal((await f.request('/conversations',{headers:{'X-Chatbot-Session':'predictable'}})).status,400);
    assert.equal((await f.request(`/conversations/${privateId}`)).status,400);
    assert.equal((await f.request('/admin/settings',{headers})).status,403);
  }finally{await f.close();}
});
test('admin write validates all settings before SQL writes; public upload refused before body parsing',async()=>{
  const f=await fixture();try{
    assert.equal((await f.request('/admin/settings',{method:'PUT',headers:{Authorization:`Bearer ${token()}`,'Content-Type':'application/json'},body:JSON.stringify({enabled:true,publicMode:'surprise',allowedRoles:['admin'],systemPrompt:'test'})})).status,400);
    assert.equal((await f.request('/admin/sources',{method:'POST',headers:{'Content-Type':'application/pdf'},body:'invalid-file'})).status,403);
    assert.equal(f.calls.some(call=>call.sql.startsWith('UPDATE')),false);
  }finally{await f.close();}
});
test('stable client request UUID replays saved pair without another provider or quota write',async()=>{
  const replayRows=[{id:'user-original',role:'user',content:'Hola',status:'complete',sources:[]},{id:'assistant-original',role:'assistant',content:'¡Hola!',status:'complete',sources:[]}];
  const f=await fixture({replayRows});try{
    const request={method:'POST',headers:{Authorization:`Bearer ${token()}`,'Content-Type':'application/json'},body:JSON.stringify({message:'Hola',locale:'es',clientRequestId:crypto.randomUUID()})};
    const response=await f.request(`/conversations/${privateId}/messages`,request),data=await response.json();
    assert.equal(response.status,200);assert.equal(data.userMessage.id,'user-original');assert.equal(data.assistantMessage.id,'assistant-original');
    assert.equal(f.calls.some(call=>/^(INSERT|UPDATE)/.test(call.sql)),false);
    const conflict=await f.request(`/conversations/${privateId}/messages`,{...request,body:JSON.stringify({...JSON.parse(request.body),message:'Different text'})});
    assert.equal(conflict.status,409);
  }finally{await f.close();}
});

async function sourceFixture({sourceCount=0,chunkCount=0,chunksPerSource=2,concurrentSourceCount}={}){
  const calls=[],sources=new Map();let ingestions=0,commits=0,rollbacks=0;
  const initialSourceCount=sourceCount,initialChunkCount=chunkCount;
  const query=async(sql,params=[])=>{
    calls.push({sql,params});
    if(sql.includes('FROM user u JOIN role'))return[[{id:1,role:'admin',enabled:'Y',deleted:'N'}]];
    if(sql.includes('FROM chatbot_settings'))return[[{id:1}]];
    if(sql.includes('COUNT(*) AS total FROM chatbot_source')){
      assert.match(sql,/status<>'deleted'/);
      return[[{total:sourceCount}]];
    }
    if(sql.includes('COUNT(*) AS total FROM chatbot_chunk'))return[[{total:chunkCount}]];
    if(sql.startsWith('INSERT INTO chatbot_source')){
      const [id,title,kind,url,filename,sha256,chunk_count,metadata]=params;
      sources.set(id,{id,title,kind,url,filename,sha256,chunk_count,metadata,enabled:1,status:'ready'});
      sourceCount++;return[{affectedRows:1}];
    }
    if(sql.startsWith('INSERT INTO chatbot_chunk')){chunkCount+=params[0].length;return[{affectedRows:params[0].length}];}
    if(sql.startsWith('INSERT INTO chatbot_audit'))return[{affectedRows:1}];
    if(sql.includes('SELECT * FROM chatbot_source WHERE id=?'))return[[sources.get(params[0])]];
    if(sql.includes("SELECT * FROM chatbot_source WHERE status<>'deleted'")){
      return[[...Array.from({length:initialSourceCount},(_,i)=>({id:`existing-${i}`,title:`Reference ${i}`,enabled:1,status:'ready',chunk_count:1})),...sources.values()]];
    }
    throw new Error(`Unexpected SQL: ${sql}`);
  };
  const db={query,getConnection:async()=>({query,
    beginTransaction:async()=>{if(concurrentSourceCount!==undefined)sourceCount=concurrentSourceCount;},
    commit:async()=>{commits++;},
    rollback:async()=>{rollbacks++;sourceCount=concurrentSourceCount??initialSourceCount;chunkCount=initialChunkCount;sources.clear();},
    release:()=>{}
  })};
  const knowledge={ingestSource:async input=>{
    ingestions++;
    return{title:'Nutrition reference',kind:input.file?'pdf':'url',url:input.url,filename:input.file?.originalname,
      chunks:Array.from({length:chunksPerSource},(_,i)=>({text:`Nutrition passage ${i}`,embedding:[.1,.2]}))};
  }};
  const app=express();app.use(express.json());app.use('/api/chatbot',createChatbotRouter({pool:db,env,knowledge}));
  const server=await new Promise(resolve=>{const listening=app.listen(0,'127.0.0.1',()=>resolve(listening));});
  const base=`http://127.0.0.1:${server.address().port}/api/chatbot`;
  const headers={Authorization:`Bearer ${token()}`};
  return{calls,sources,stats:()=>({sourceCount,chunkCount,ingestions,commits,rollbacks}),
    list:()=>fetch(`${base}/admin/sources`,{headers}),
    create:(body={url:'https://www.cdc.gov/nutrition/'})=>fetch(`${base}/admin/sources`,{
      method:'POST',headers:body instanceof FormData?headers:{...headers,'Content-Type':'application/json'},
      body:body instanceof FormData?body:JSON.stringify(body)
    }),
    close:async()=>{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
  };
}

test('source uploads pass the former 100 cap, 43 topics with 10 references, and 500 references',async()=>{
  for(const sourceCount of [100,429,500,1000,MAX_SOURCES-1]){
    const f=await sourceFixture({sourceCount,chunkCount:25000,chunksPerSource:150});try{
      const response=await f.create(),data=await response.json();
      assert.equal(response.status,201,JSON.stringify(data));
      assert.equal(data.source.chunkCount,150);
      assert.equal(f.stats().sourceCount,sourceCount+1);
      assert.equal(f.stats().chunkCount,25150);
      assert.equal(f.stats().commits,1);
      assert.equal(f.calls.filter(call=>call.sql.startsWith('INSERT INTO chatbot_chunk')).length,2);
      assert.ok(f.calls.find(call=>call.sql.startsWith('INSERT INTO chatbot_audit')&&call.params[2]==='source_created'));
    }finally{await f.close();}
  }
});

test('PDF uploads use the same expanded capacity as URLs',async()=>{
  const f=await sourceFixture({sourceCount:500});try{
    const form=new FormData();form.append('file',new Blob(['%PDF-1.4 mocked extraction'],{type:'application/pdf'}),'reference.pdf');
    const response=await f.create(form),data=await response.json();
    assert.equal(response.status,201);assert.equal(data.source.kind,'pdf');
    assert.equal(data.source.filename,'reference.pdf');assert.equal(f.stats().sourceCount,501);
  }finally{await f.close();}
});

test('source list exposes server capacity and all references beyond 500',async()=>{
  const f=await sourceFixture({sourceCount:2000});try{
    const response=await f.list(),data=await response.json();
    assert.equal(response.status,200);assert.equal(data.items.length,2000);
    assert.equal(data.maxSources,2000);assert.equal(data.maxChunks,200000);
    assert.ok(data.items.some(source=>source.id==='existing-1999'));
  }finally{await f.close();}
});

test('full source capacity rejects before indexing and releases the upload reservation',async()=>{
  const f=await sourceFixture({sourceCount:MAX_SOURCES});try{
    for(let i=0;i<2;i++){
      const response=await f.create();assert.equal(response.status,400);
      assert.equal((await response.json()).error,'chatbot_source_limit');
    }
    assert.equal(f.stats().ingestions,0);assert.equal(f.stats().commits,0);
    assert.equal(f.calls.some(call=>call.sql.startsWith('INSERT')),false);
  }finally{await f.close();}
});

test('transaction rechecks capacity after a concurrent import fills the final slot',async()=>{
  const f=await sourceFixture({sourceCount:MAX_SOURCES-1,concurrentSourceCount:MAX_SOURCES});try{
    const response=await f.create();assert.equal(response.status,400);
    assert.equal((await response.json()).error,'chatbot_source_limit');
    assert.equal(f.stats().ingestions,1);assert.equal(f.stats().rollbacks,1);
    const lockIndex=f.calls.findIndex(call=>call.sql.includes('FOR UPDATE'));
    const countIndex=f.calls.findLastIndex(call=>call.sql.includes('COUNT(*) AS total FROM chatbot_source'));
    assert.ok(lockIndex>=0&&countIndex>lockIndex);
    assert.equal(f.calls.some(call=>call.sql.startsWith('INSERT')),false);
  }finally{await f.close();}
});

test('expanded chunk capacity accepts its boundary and rolls back overflow with a distinct error',async()=>{
  const allowed=await sourceFixture({sourceCount:550,chunkCount:MAX_TOTAL_CHUNKS-2});try{
    assert.equal((await allowed.create()).status,201);
    assert.equal(allowed.stats().chunkCount,MAX_TOTAL_CHUNKS);
  }finally{await allowed.close();}
  const full=await sourceFixture({sourceCount:550,chunkCount:MAX_TOTAL_CHUNKS-1});try{
    const response=await full.create();assert.equal(response.status,400);
    assert.equal((await response.json()).error,'chatbot_chunk_limit');
    assert.equal(full.stats().commits,0);assert.equal(full.stats().rollbacks,1);
    assert.equal(full.stats().sourceCount,550);assert.equal(full.stats().chunkCount,MAX_TOTAL_CHUNKS-1);
    assert.equal(full.sources.size,0);
  }finally{await full.close();}
});

test('new internal citations pass route validation and every calendar overview row is rechecked',async()=>{
  for(const revoked of [false,true]) {
    let processingToken;const messages=[];
    const reply={content:'Calendar overview',status:'complete',model:null,sources:[{id:'calendar:current',url:'/calendar',title:'Calendar'}],
      validationSources:[{id:'calendar:1'},{id:'calendar:2'}],citationIds:['calendar:1','calendar:2']};
    let checked=[];
    const query=async(sql,params=[])=>{
      if(sql.includes('FROM user u JOIN role'))return[[{id:1,role:'admin'}]];
      if(sql.includes('FROM chatbot_settings'))return[[{enabled:1,public_mode:'hidden',allowed_roles:'["admin"]',system_prompt:'Style',revision:1}]];
      if(sql.includes('FROM chatbot_conversation c'))return[[{id:privateId,message_count:messages.length,locale:'en'}]];
      if(sql.includes('FROM chatbot_message'))return[sql.includes('id IN')?messages:[]];
      if(sql.startsWith('UPDATE chatbot_conversation SET processing_token=?'))processingToken=params[0];
      if(sql.includes('SELECT processing_token'))return[[{processing_token:processingToken}]];
      if(sql.startsWith('SELECT ce.id')){checked=params[0];return[revoked?[{id:1}]:[{id:1},{id:2}]];}
      if(sql.startsWith('INSERT INTO chatbot_message')) {
        const assistant=sql.includes("'assistant'");
        messages.push({id:params[0],role:assistant?'assistant':'user',content:params[2],sources:assistant?params[3]:'[]',status:assistant?params[4]:'complete'});
      }
      return[{affectedRows:1}];
    };
    const connection={query,beginTransaction:async()=>{},commit:async()=>{},rollback:async()=>{},release:()=>{}};
    const app=express();app.use(express.json());
    app.use('/api/chatbot',createChatbotRouter({pool:{query,getConnection:async()=>connection},env,service:{answer:async()=>reply,model:()=>null}}));
    const server=await new Promise(resolve=>{const listening=app.listen(0,'127.0.0.1',()=>resolve(listening));});
    try {
      const response=await fetch(`http://127.0.0.1:${server.address().port}/api/chatbot/conversations/${privateId}/messages`,{
        method:'POST',headers:{Authorization:`Bearer ${token()}`,'Content-Type':'application/json'},
        body:JSON.stringify({message:'What are the upcoming distributions?',clientRequestId:crypto.randomUUID()})});
      const data=await response.json();
      assert.equal(response.status,200);assert.deepEqual(checked,[1,2]);
      assert.equal(data.assistantMessage.status,revoked?'no_context':'complete');
      assert.equal(data.assistantMessage.validationSources,undefined);
    } finally {server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
  }
});
