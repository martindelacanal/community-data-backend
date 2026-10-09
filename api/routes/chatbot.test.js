'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const express=require('express');
const jwt=require('jsonwebtoken');
const crypto=require('node:crypto');
const {createChatbotRouter}=require('./chatbot');

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
