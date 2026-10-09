'use strict';
const express = require('express');
const jwt = require('jsonwebtoken');
const crypto = require('node:crypto');
const multer = require('multer');
const { buildRestoreAuthBinding } = require('../utils/restoreAuthBinding');
const { ChatbotError,fail,MAX_MESSAGE_CHARS,MAX_OUTPUT_TOKENS,DAILY_MESSAGE_LIMIT,DISCLAIMERS,
  validateMessage,chooseLocale,createChatbotService,createChatbotLimiter,localText } = require('../services/chatbot');

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const json = (value,fallback) => { if(value==null)return fallback;try{return typeof value==='string'?JSON.parse(value):value;}catch{return fallback;} };
const boolean = value => value===true || value===1;
const identifier = value => { if(typeof value!=='string'||!UUID.test(value))fail('chatbot_not_found',404);return value; };
const hash = (value,secret) => crypto.createHmac('sha256',secret).update(value).digest('hex');
const digest = value => crypto.createHash('sha256').update(value).digest('hex');
const iso = value => value instanceof Date?value.toISOString():value;
const cleanSearch = value => typeof value==='string'?value.trim().slice(0,160):'';
const searchLike = value => `%${value.replace(/[\\%_]/g,'\\$&')}%`;

function conversationDTO(row) {
  return {id:row.id,title:row.title,preview:row.preview,locale:row.locale,messageCount:Number(row.message_count),
    createdAt:iso(row.created_at),updatedAt:iso(row.updated_at),
    ...(Object.hasOwn(row,'user_id')?{userId:row.user_id,userRole:row.user_role,userName:row.user_name||null}:{})};
}
function messageDTO(row,admin=false) {
  return {id:row.id,role:row.role,content:row.content,sources:json(row.sources,[]),status:row.status,createdAt:iso(row.created_at),
    ...(admin?{model:row.model,inputTokens:Number(row.input_tokens),outputTokens:Number(row.output_tokens),latencyMs:Number(row.latency_ms),errorCode:row.error_code,requestId:row.request_id,settingsRevision:row.settings_revision}:{})};
}
function sourceDTO(row) {
  return {id:row.id,title:row.title,kind:row.kind,url:row.url,filename:row.filename,enabled:boolean(row.enabled),status:row.status,
    chunkCount:Number(row.chunk_count),metadata:json(row.metadata,{}),createdAt:iso(row.created_at),updatedAt:iso(row.updated_at)};
}
function pagination(query) {
  const page=query.page===undefined?1:Number(query.page),pageSize=query.pageSize===undefined?20:Number(query.pageSize);
  if(!Number.isInteger(page)||page<1||page>10000||!Number.isInteger(pageSize)||pageSize<1||pageSize>50)fail('chatbot_invalid_request',400);
  return {page,pageSize,offset:(page-1)*pageSize};
}

function createChatbotRouter({pool,env=process.env,service,knowledge,logger,limiterOptions}={}) {
  const router=express.Router(),db=pool || require('../connection/connection').promise();
  const ai=service || createChatbotService({env});
  let knowledgeService=knowledge;
  const getKnowledge=()=>knowledgeService ||= require('../services/chatbotKnowledge');
  const limiter=createChatbotLimiter(limiterOptions);
  const requestWindows=new Map();let ingestionCount=0,uploadReservations=0;
  const wrap=fn=>(req,res,next)=>Promise.resolve(fn(req,res,next)).catch(next);
  const secret=()=>env.CHATBOT_SESSION_SECRET || env.JWT_SECRET;
  async function transaction(fn) {
    const connection=await db.getConnection();
    try {await connection.beginTransaction();const result=await fn(connection);await connection.commit();return result;}
    catch(error){await connection.rollback();throw error;}finally{connection.release();}
  }
  async function audit(actor,action,entityId,details={},connection=db,requestId=null) {
    await connection.query('INSERT INTO chatbot_audit(actor_user_id,actor_role,action,entity_id,request_id,details) VALUES(?,?,?,?,?,?)',
      [actor?.id||null,actor?.role||'public',action,entityId||null,requestId,JSON.stringify(details)]);
  }
  async function citationsCurrent(sources) {
    const groups={source:[],article:[],resource:[],calendar:[]};
    for(const source of sources){
      const match=/^(source):([0-9a-f-]{36}):(\d+)$/.exec(source.id);
      if(match){groups.source.push({id:source.id,chunkId:Number(match[3])});continue;}
      const internal=/^(article|resource|calendar):(\d+)(?::\d+)?$/.exec(source.id);
      if(internal){groups[internal[1]].push(Number(internal[2]));continue;}
      if(source.id!=='calendar:current')return false;
    }
    if(groups.source.length){
      const [rows]=await db.query("SELECT CONCAT('source:',s.id,':',c.id) AS id FROM chatbot_chunk c JOIN chatbot_source s ON s.id=c.source_id WHERE c.id IN (?) AND s.enabled=1 AND s.status='ready'",[groups.source.map(s=>s.chunkId)]);
      if(groups.source.some(source=>!rows.some(row=>row.id===source.id)))return false;
    }
    const checks={article:"SELECT id FROM article WHERE id IN (?) AND article_status_id=2 AND (publication_date IS NULL OR publication_date<=NOW())",
      resource:'SELECT id FROM trusted_resources WHERE id IN (?) AND is_active=1',
      calendar:"SELECT ce.id FROM calendar_event ce JOIN location l ON l.id=ce.location_id WHERE ce.id IN (?) AND ce.enabled='Y' AND l.enabled='Y'"};
    for(const name of ['article','resource','calendar'])if(groups[name].length){const [rows]=await db.query(checks[name],[groups[name]]);if(groups[name].some(id=>!rows.some(row=>Number(row.id)===id)))return false;}
    return true;
  }
  async function settings(connection=db) {
    const [[row]]=await connection.query('SELECT * FROM chatbot_settings WHERE id=1');
    if(!row)fail('chatbot_unavailable',503);
    return {...row,enabled:boolean(row.enabled),allowed_roles:json(row.allowed_roles,[])};
  }
  const allowed=(actor,configuration)=>configuration.enabled && (actor.id?configuration.allowed_roles.includes(actor.role):configuration.public_mode==='enabled');
  const requireAdmin=(req,res,next)=>req.chatbotActor.id && req.chatbotActor.role==='admin'?next():next(new ChatbotError('chatbot_forbidden',403));
  const requireAccess=wrap(async(req,res,next)=>{req.chatbotSettings=await settings();if(!allowed(req.chatbotActor,req.chatbotSettings))fail(req.chatbotActor.id?'chatbot_forbidden':'chatbot_authentication_required',req.chatbotActor.id?403:401);if(!req.chatbotActor.ownerKey)fail('chatbot_session_required',400);next();});

  router.use((req,res,next)=>{res.set('Cache-Control','no-store, private');res.set('Pragma','no-cache');res.set('X-Content-Type-Options','nosniff');next();});
  router.use(wrap(async(req,res,next)=>{
    if(!secret())fail('chatbot_unavailable',503);
    const authorization=req.headers.authorization;
    const ipKey=hash(`ip:${req.ip || req.socket.remoteAddress || 'unknown'}`,secret());
    req.chatbotActor={id:null,role:'public',ownerKey:null,quotaKey:ipKey};
    if(authorization){
      let identity,claims;
      try {
        if(typeof authorization!=='string'||authorization.length>8192||!authorization.startsWith('Bearer '))throw new Error();
        claims=jwt.verify(authorization.slice(7),env.JWT_SECRET,{algorithms:['HS256']});
        identity=typeof claims.data==='string'?JSON.parse(claims.data):claims.data;
        if(!Number.isSafeInteger(Number(identity?.id))||Number(identity.id)<1)throw new Error();
      }catch{fail('chatbot_authentication_required',401);}
      const [[row]]=await db.query(`SELECT u.id,u.password,u.enabled,u.deleted,u.reset_password,u.creation_date,r.name AS role
        FROM user u JOIN role r ON r.id=u.role_id WHERE u.id=? AND u.enabled='Y' AND u.deleted='N'`,[identity.id]);
      if(!row)fail('chatbot_authentication_required',401);
      if(claims.restore_auth_binding){
        const binding=buildRestoreAuthBinding(row,env.JWT_SECRET),actual=claims.restore_auth_binding;
        if(typeof actual!=='string'||!/^[a-f0-9]{64}$/.test(actual)||!crypto.timingSafeEqual(Buffer.from(binding,'hex'),Buffer.from(actual,'hex')))fail('chatbot_authentication_required',401);
      }
      // Authorization is always the current database role, never an editable client value or stale JWT role.
      const ownerKey=hash(`user:${row.id}`,secret());
      req.chatbotActor={id:Number(row.id),role:row.role,ownerKey,quotaKey:ownerKey};
    } else {
      const session=req.headers['x-chatbot-session'];
      if(session!==undefined){if(typeof session!=='string'||!UUID.test(session))fail('chatbot_invalid_request',400);req.chatbotActor.ownerKey=hash(`guest:${session}`,secret());}
    }
    // Bound reads and admin operations too. Public clients cannot bypass this by rotating session IDs.
    const now=Date.now(),key=req.chatbotActor.quotaKey;
    if(requestWindows.size>5000)for(const [k,value]of requestWindows)if(value.until<=now)requestWindows.delete(k);
    const bucket=requestWindows.get(key);const current=!bucket||bucket.until<=now?{n:0,until:now+60000}:bucket;
    if(requestWindows.size>=10000&&!requestWindows.has(key))fail('chatbot_busy',429);
    current.n++;requestWindows.set(key,current);if(current.n>180)fail('chatbot_rate_limited',429);
    next();
  }));

  router.get('/config',wrap(async(req,res)=>{
    const configuration=await settings(),actor=req.chatbotActor;
    res.json({enabled:configuration.enabled,visible:configuration.enabled && (actor.id?configuration.allowed_roles.includes(actor.role):configuration.public_mode!=='hidden'),
      canChat:allowed(actor,configuration),requiresLogin:configuration.enabled&&!actor.id&&configuration.public_mode==='login_required',
      publicMode:configuration.public_mode,maxMessageChars:MAX_MESSAGE_CHARS,disclaimer:DISCLAIMERS});
  }));

  async function readConversation(id,ownerKey=null,connection=db) {
    const [[row]]=await connection.query(`SELECT c.*,CONCAT_WS(' ',u.firstname,u.lastname) AS user_name FROM chatbot_conversation c
      LEFT JOIN user u ON u.id=c.user_id WHERE c.id=? ${ownerKey?'AND c.owner_key=?':''}`,[identifier(id),...(ownerKey?[ownerKey]:[])]);
    if(!row)fail('chatbot_not_found',404);return row;
  }
  async function listConversations(req,res,admin=false) {
    const paging=pagination(req.query),query=cleanSearch(req.query.q),conditions=[],params=[];
    if(!admin){conditions.push('c.owner_key=?');params.push(req.chatbotActor.ownerKey);}
    if(query){conditions.push('(c.title LIKE ? OR c.preview LIKE ? OR EXISTS (SELECT 1 FROM chatbot_message m WHERE m.conversation_id=c.id AND m.content LIKE ?))');params.push(searchLike(query),searchLike(query),searchLike(query));}
    if(admin){
      if(req.query.userId){const id=Number(req.query.userId);if(!Number.isSafeInteger(id)||id<1)fail('chatbot_invalid_request',400);conditions.push('c.user_id=?');params.push(id);}
      if(req.query.role){if(typeof req.query.role!=='string'||!/^[a-z]{1,45}$/.test(req.query.role))fail('chatbot_invalid_request',400);conditions.push('c.user_role=?');params.push(req.query.role);}
      for(const name of ['from','to'])if(req.query[name]){
        if(typeof req.query[name]!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(req.query[name])||!Number.isFinite(Date.parse(req.query[name])))fail('chatbot_invalid_request',400);
        conditions.push(name==='from'?'c.created_at>=?':'c.created_at<DATE_ADD(?, INTERVAL 1 DAY)');params.push(req.query[name]);
      }
    }
    const where=conditions.length?`WHERE ${conditions.join(' AND ')}`:'';
    const [[count]]=await db.query(`SELECT COUNT(*) AS total FROM chatbot_conversation c ${where}`,params);
    const [rows]=await db.query(`SELECT c.*,CONCAT_WS(' ',u.firstname,u.lastname) AS user_name FROM chatbot_conversation c LEFT JOIN user u ON u.id=c.user_id ${where} ORDER BY c.updated_at DESC,c.id DESC LIMIT ? OFFSET ?`,[...params,paging.pageSize,paging.offset]);
    res.json({items:rows.map(conversationDTO),total:Number(count.total),page:paging.page,pageSize:paging.pageSize});
  }
  router.get('/conversations',requireAccess,wrap((req,res)=>listConversations(req,res)));
  router.post('/conversations',requireAccess,wrap(async(req,res)=>{
    const actor=req.chatbotActor,locale=req.body?.locale==='es'?'es':'en',id=crypto.randomUUID();
    // Public clients cannot evade creation quotas by rotating their random session ID.
    const creationKey=hash(`conversation-create:${actor.quotaKey}`,secret());
    await transaction(async connection=>{
      await connection.query('INSERT IGNORE INTO chatbot_usage(owner_key,usage_date,message_count) VALUES(?,UTC_DATE(),0)',[creationKey]);
      const [usage]=await connection.query('UPDATE chatbot_usage SET message_count=message_count+1 WHERE owner_key=? AND usage_date=UTC_DATE() AND message_count<?',[creationKey,actor.id?60:20]);
      if(usage.affectedRows!==1)fail('chatbot_daily_limit',429);
      await connection.query('INSERT INTO chatbot_conversation(id,owner_key,user_id,user_role,locale) VALUES(?,?,?,?,?)',[id,actor.ownerKey,actor.id,actor.role,locale]);
      await audit(actor,'conversation_created',id,{},connection);
    });
    res.status(201).json({conversation:conversationDTO(await readConversation(id,actor.ownerKey))});
  }));
  router.get('/conversations/:id',requireAccess,wrap(async(req,res)=>{
    const conversation=await readConversation(req.params.id,req.chatbotActor.ownerKey);
    const [rows]=await db.query('SELECT * FROM chatbot_message WHERE conversation_id=? ORDER BY sequence',[conversation.id]);
    res.json({conversation:conversationDTO(conversation),messages:rows.map(row=>messageDTO(row))});
  }));

  router.post('/conversations/:id/messages',requireAccess,wrap(async(req,res)=>{
    const question=validateMessage(req.body?.message),locale=chooseLocale(question,req.body?.locale),actor=req.chatbotActor;
    const conversation=await readConversation(req.params.id,actor.ownerKey),configuration=req.chatbotSettings;
    if(req.body?.clientRequestId!==undefined&&(typeof req.body.clientRequestId!=='string'||!UUID.test(req.body.clientRequestId)))fail('chatbot_invalid_request',400);
    const requestId=req.body?.clientRequestId||crypto.randomUUID(),processingToken=crypto.randomUUID(),assistantMessageId=crypto.randomUUID();
    let userMessageId=crypto.randomUUID();
    async function priorAttempt(connection=db) {
      const [rows]=await connection.query('SELECT * FROM chatbot_message WHERE conversation_id=? AND request_id=? ORDER BY sequence',[conversation.id,requestId]);
      const user=rows.find(row=>row.role==='user'),assistant=rows.find(row=>row.role==='assistant');
      if(user&&user.content!==question)fail('chatbot_idempotency_conflict',409);
      return {user,assistant};
    }
    async function replay(prior) {return res.json({userMessage:messageDTO(prior.user),assistantMessage:messageDTO(prior.assistant),conversation:conversationDTO(await readConversation(conversation.id,actor.ownerKey))});}
    const prior=await priorAttempt();if(prior.user&&prior.assistant)return replay(prior);
    if(!prior.user&&Number(conversation.message_count)>=200)fail('chatbot_conversation_limit',400);
    const release=limiter.acquire(actor.quotaKey);let acquired=false,completed=false;
    const started=Date.now();
    try {
      const raced=await transaction(async connection=>{
        const [lock]=await connection.query(`UPDATE chatbot_conversation SET processing_token=?,processing_until=DATE_ADD(UTC_TIMESTAMP(3),INTERVAL 180 SECOND)
          WHERE id=? AND owner_key=? AND (processing_until IS NULL OR processing_until<UTC_TIMESTAMP(3))`,[processingToken,conversation.id,actor.ownerKey]);
        if(lock.affectedRows!==1)fail('chatbot_busy',429);
        const existing=await priorAttempt(connection);
        if(existing.user&&existing.assistant){await connection.query('UPDATE chatbot_conversation SET processing_token=NULL,processing_until=NULL WHERE id=? AND processing_token=?',[conversation.id,processingToken]);return existing;}
        if(existing.user)userMessageId=existing.user.id;
        else {
          await connection.query('INSERT IGNORE INTO chatbot_usage(owner_key,usage_date,message_count) VALUES(?,UTC_DATE(),0)',[actor.quotaKey]);
          const [usage]=await connection.query('UPDATE chatbot_usage SET message_count=message_count+1 WHERE owner_key=? AND usage_date=UTC_DATE() AND message_count<?',[actor.quotaKey,actor.id?DAILY_MESSAGE_LIMIT:20]);
          if(usage.affectedRows!==1)fail('chatbot_daily_limit',429);
          await connection.query(`INSERT INTO chatbot_message(id,conversation_id,role,content,status,request_id,settings_revision) VALUES(?,?,'user',?,'complete',?,?)`,[userMessageId,conversation.id,question,requestId,configuration.revision]);
          await connection.query(`UPDATE chatbot_conversation SET title=CASE WHEN message_count=0 THEN ? ELSE title END,preview=?,locale=?,message_count=message_count+1 WHERE id=?`,[question.slice(0,200),question.slice(0,300),locale,conversation.id]);
        }
        return null;
      });
      if(raced)return replay(raced);
      acquired=true;
      const [history]=await db.query('SELECT role,content FROM chatbot_message WHERE conversation_id=? AND id<>? AND status NOT IN (\'error\',\'processing\') ORDER BY sequence DESC LIMIT 6',[conversation.id,userMessageId]);
      let reply,errorCode=null;
      try {
        reply=await ai.answer({question,locale,systemPrompt:configuration.system_prompt,history:history.reverse(),
          retrieve:args=>getKnowledge().retrieveContext({...args,db,env})});
      } catch(error) {
        errorCode=error instanceof ChatbotError?error.code:'chatbot_unavailable';
        logger?.warn?.('Chatbot answer failed',{code:errorCode,requestId});
        reply={content:localText('unavailable',locale),sources:[],status:'error',model:ai.model(),inputTokens:0,outputTokens:0};
      }
      // Re-check permissions after a slow provider call so a concurrent revocation cannot release a reply.
      const currentSettings=await settings();
      let stillAllowed=allowed(actor,currentSettings);
      if(actor.id){const [[currentUser]]=await db.query("SELECT r.name AS role FROM user u JOIN role r ON r.id=u.role_id WHERE u.id=? AND u.enabled='Y' AND u.deleted='N'",[actor.id]);stillAllowed=Boolean(currentUser)&&allowed({...actor,role:currentUser.role},currentSettings);}
      if(!stillAllowed)reply={content:localText('refusal',locale),sources:[],status:'refused',model:reply.model,inputTokens:reply.inputTokens,outputTokens:reply.outputTokens};
      if(reply.sources?.length&&!await citationsCurrent(reply.sources))reply={content:localText('noContext',locale),sources:[],status:'no_context',model:reply.model,inputTokens:reply.inputTokens,outputTokens:reply.outputTokens};
      await transaction(async connection=>{
        const [[current]]=await connection.query('SELECT processing_token FROM chatbot_conversation WHERE id=? FOR UPDATE',[conversation.id]);
        if(current?.processing_token!==processingToken)fail('chatbot_busy',429);
        await connection.query(`INSERT INTO chatbot_message(id,conversation_id,role,content,sources,status,model,input_tokens,output_tokens,latency_ms,error_code,request_id,settings_revision)
          VALUES(?,?,'assistant',?,?,?,?,?,?,?,?,?,?)`,[assistantMessageId,conversation.id,reply.content,JSON.stringify(reply.sources),reply.status,reply.model,reply.inputTokens||0,reply.outputTokens||0,Date.now()-started,errorCode,requestId,configuration.revision]);
        await connection.query('UPDATE chatbot_conversation SET message_count=message_count+1,preview=?,processing_token=NULL,processing_until=NULL WHERE id=? AND processing_token=?',[reply.content.slice(0,300),conversation.id,processingToken]);
        await audit(actor,'message_answered',conversation.id,{status:reply.status,model:reply.model,inputTokens:reply.inputTokens,outputTokens:reply.outputTokens,citationIds:reply.citationIds||[],errorCode},connection,requestId);
      });
      completed=true;
      const [messages]=await db.query('SELECT * FROM chatbot_message WHERE id IN (?,?)',[userMessageId,assistantMessageId]);
      res.json({userMessage:messageDTO(messages.find(m=>m.id===userMessageId)),assistantMessage:messageDTO(messages.find(m=>m.id===assistantMessageId)),conversation:conversationDTO(await readConversation(conversation.id,actor.ownerKey))});
    } finally {
      release();
      if(acquired&&!completed)await db.query('UPDATE chatbot_conversation SET processing_token=NULL,processing_until=NULL WHERE id=? AND processing_token=?',[conversation.id,processingToken]).catch(()=>{});
    }
  }));

  router.use('/admin',requireAdmin);
  async function adminSettings() {
    const row=await settings();const [roles]=await db.query('SELECT id,name FROM role WHERE name IS NOT NULL ORDER BY id');
    return {enabled:row.enabled,publicMode:row.public_mode,allowedRoles:row.allowed_roles,systemPrompt:row.system_prompt,revision:row.revision,
      availableRoles:roles,model:ai.model(),scopeModel:ai.scopeModel?.(),providerConfigured:ai.isConfigured(),maxMessageChars:MAX_MESSAGE_CHARS,maxOutputTokens:MAX_OUTPUT_TOKENS,dailyMessageLimit:DAILY_MESSAGE_LIMIT};
  }
  router.get('/admin/settings',wrap(async(req,res)=>res.json(await adminSettings())));
  router.put('/admin/settings',wrap(async(req,res)=>{
    const body=req.body;
    if(!body||typeof body.enabled!=='boolean'||!['hidden','login_required','enabled'].includes(body.publicMode)||!Array.isArray(body.allowedRoles)||body.allowedRoles.length>50
      ||body.allowedRoles.some(role=>typeof role!=='string')||new Set(body.allowedRoles).size!==body.allowedRoles.length
      ||typeof body.systemPrompt!=='string'||!body.systemPrompt.trim()||body.systemPrompt.length>6000)fail('chatbot_invalid_request',400);
    const [roles]=await db.query('SELECT name FROM role WHERE name IS NOT NULL');
    if(body.allowedRoles.some(role=>!roles.some(row=>row.name===role)))fail('chatbot_invalid_request',400);
    await transaction(async connection=>{
      await connection.query('UPDATE chatbot_settings SET enabled=?,public_mode=?,allowed_roles=?,system_prompt=?,revision=revision+1,updated_by=? WHERE id=1',
        [body.enabled?1:0,body.publicMode,JSON.stringify(body.allowedRoles),body.systemPrompt.trim(),req.chatbotActor.id]);
      await audit(req.chatbotActor,'settings_updated','1',{enabled:body.enabled,publicMode:body.publicMode,allowedRoles:body.allowedRoles,promptSha256:digest(body.systemPrompt.trim())},connection);
    });
    res.json(await adminSettings());
  }));

  const upload=multer({storage:multer.memoryStorage(),limits:{fileSize:10*1024*1024,files:1,fields:3,fieldSize:4096,parts:4},
    fileFilter:(req,file,callback)=>callback(file.mimetype==='application/pdf'?null:new ChatbotError('chatbot_invalid_pdf',400),true)}).single('file');
  const readUpload=(req,res,next)=>{
    // Reserve before multer allocates a PDF buffer, and keep it reserved through indexing/persistence.
    if(uploadReservations>=1||ingestionCount>=1)return next(new ChatbotError('chatbot_ingestion_busy',429));
    uploadReservations++;let released=false;
    req.releaseChatbotUpload=()=>{if(!released){released=true;uploadReservations--;}};
    upload(req,res,error=>{
      if(error){req.releaseChatbotUpload();return next(error instanceof ChatbotError?error:new ChatbotError(error.code==='LIMIT_FILE_SIZE'?'chatbot_file_too_large':'chatbot_invalid_request',error.code==='LIMIT_FILE_SIZE'?413:400));}
      next();
    });
  };
  const wrapUpload=fn=>wrap(async(req,res)=>{try{return await fn(req,res);}finally{req.releaseChatbotUpload?.();}});
  async function ingest(input) {
    if(ingestionCount>=1)fail('chatbot_ingestion_busy',429);ingestionCount++;
    try{return await getKnowledge().ingestSource(input,{env});}finally{ingestionCount--;}
  }
  async function saveChunks(connection,id,chunks) {
    if(!Array.isArray(chunks)||!chunks.length||chunks.length>1000)fail('chatbot_invalid_source',400);
    // Serialize corpus growth with settings changes/other imports across Node processes.
    await connection.query('SELECT id FROM chatbot_settings WHERE id=1 FOR UPDATE');
    const [[total]]=await connection.query('SELECT COUNT(*) AS total FROM chatbot_chunk');
    if(Number(total.total)+chunks.length>20000)fail('chatbot_source_limit',400);
    for(let offset=0;offset<chunks.length;offset+=100){
      const rows=chunks.slice(offset,offset+100).map((chunk,index)=>[id,offset+index,String(chunk.text),chunk.embedding?JSON.stringify(chunk.embedding):null]);
      await connection.query('INSERT INTO chatbot_chunk(source_id,ordinal,content,embedding) VALUES ?',[rows]);
    }
  }
  router.get('/admin/sources',wrap(async(req,res)=>{
    const [rows]=await db.query("SELECT * FROM chatbot_source WHERE status<>'deleted' ORDER BY created_at DESC");res.json({items:rows.map(sourceDTO)});
  }));
  router.post('/admin/sources',readUpload,wrapUpload(async(req,res)=>{
    if(!req.file&&!req.body?.url)fail('chatbot_invalid_source',400);
    if(req.file&&req.body?.url)fail('chatbot_invalid_source',400);
    if(req.body?.url!==undefined&&(typeof req.body.url!=='string'||req.body.url.length>2048))fail('chatbot_invalid_url',400);
    if(req.body?.title!==undefined&&(typeof req.body.title!=='string'||req.body.title.length>200))fail('chatbot_invalid_request',400);
    const [[count]]=await db.query("SELECT COUNT(*) AS total FROM chatbot_source WHERE status<>'deleted'");if(Number(count.total)>=100)fail('chatbot_source_limit',400);
    const data=await ingest({file:req.file,url:req.body?.url,title:req.body?.title});
    const id=crypto.randomUUID();
    await transaction(async connection=>{
      await connection.query('SELECT id FROM chatbot_settings WHERE id=1 FOR UPDATE');
      const [[currentCount]]=await connection.query("SELECT COUNT(*) AS total FROM chatbot_source WHERE status<>'deleted'");
      if(Number(currentCount.total)>=100)fail('chatbot_source_limit',400);
      await connection.query('INSERT INTO chatbot_source(id,title,kind,url,filename,sha256,chunk_count,metadata,created_by) VALUES(?,?,?,?,?,?,?,?,?)',
        [id,data.title,data.kind,data.url||null,data.filename||null,data.sha256||null,data.chunks.length,JSON.stringify(data.metadata||{}),req.chatbotActor.id]);
      await saveChunks(connection,id,data.chunks);
      await audit(req.chatbotActor,'source_created',id,{kind:data.kind,title:data.title,sha256:data.sha256,chunkCount:data.chunks.length},connection);
    });
    const [[row]]=await db.query('SELECT * FROM chatbot_source WHERE id=?',[id]);res.status(201).json({source:sourceDTO(row)});
  }));
  router.patch('/admin/sources/:id',wrap(async(req,res)=>{
    const id=identifier(req.params.id);if(typeof req.body?.enabled!=='boolean')fail('chatbot_invalid_request',400);
    await transaction(async connection=>{
      const [result]=await connection.query("UPDATE chatbot_source SET enabled=? WHERE id=? AND status<>'deleted'",[req.body.enabled?1:0,id]);if(result.affectedRows!==1)fail('chatbot_not_found',404);
      await audit(req.chatbotActor,'source_toggled',id,{enabled:req.body.enabled},connection);
    });
    const [[row]]=await db.query('SELECT * FROM chatbot_source WHERE id=?',[id]);res.json({source:sourceDTO(row)});
  }));
  router.delete('/admin/sources/:id',wrap(async(req,res)=>{
    const id=identifier(req.params.id);
    await transaction(async connection=>{
      const [result]=await connection.query("UPDATE chatbot_source SET enabled=0,status='deleted',chunk_count=0 WHERE id=? AND status<>'deleted'",[id]);if(result.affectedRows!==1)fail('chatbot_not_found',404);
      await connection.query('DELETE FROM chatbot_chunk WHERE source_id=?',[id]);
      await audit(req.chatbotActor,'source_deleted',id,{},connection);
    });
    res.json({deleted:true});
  }));
  router.post('/admin/sources/:id/reindex',wrap(async(req,res)=>{
    const id=identifier(req.params.id);const [[old]]=await db.query("SELECT * FROM chatbot_source WHERE id=? AND status<>'deleted'",[id]);
    if(!old)fail('chatbot_not_found',404);if(old.kind!=='url')fail('chatbot_pdf_reupload_required',400);
    const data=await ingest({url:old.url,title:old.title});
    await transaction(async connection=>{
      const [result]=await connection.query("UPDATE chatbot_source SET sha256=?,chunk_count=?,metadata=?,status='ready' WHERE id=? AND status<>'deleted'",[data.sha256||null,data.chunks.length,JSON.stringify(data.metadata||{}),id]);
      if(result.affectedRows!==1)fail('chatbot_not_found',404);
      await connection.query('DELETE FROM chatbot_chunk WHERE source_id=?',[id]);await saveChunks(connection,id,data.chunks);
      await audit(req.chatbotActor,'source_reindexed',id,{sha256:data.sha256,chunkCount:data.chunks.length},connection);
    });
    const [[row]]=await db.query('SELECT * FROM chatbot_source WHERE id=?',[id]);res.json({source:sourceDTO(row)});
  }));
  router.get('/admin/conversations',wrap((req,res)=>listConversations(req,res,true)));
  router.get('/admin/conversations/:id',wrap(async(req,res)=>{
    const conversation=await readConversation(req.params.id);
    const [messages]=await db.query('SELECT * FROM chatbot_message WHERE conversation_id=? ORDER BY sequence',[conversation.id]);
    await audit(req.chatbotActor,'conversation_reviewed',conversation.id);
    const [events]=await db.query('SELECT id,actor_user_id AS actorUserId,actor_role AS actorRole,action,details,created_at AS createdAt FROM chatbot_audit WHERE entity_id=? ORDER BY created_at DESC LIMIT 100',[conversation.id]);
    res.json({conversation:conversationDTO(conversation),messages:messages.map(row=>messageDTO(row,true)),events:events.map(event=>({...event,details:json(event.details,{})}))});
  }));
  router.use((error,req,res,next)=>{
    if(res.headersSent)return next(error);
    const safe=error instanceof ChatbotError || (typeof error.code==='string'&&/^chatbot_[a-z_]+$/.test(error.code)&&Number.isInteger(error.status)&&error.status>=400&&error.status<=599);
    const code=safe?error.code:'chatbot_unavailable',status=safe?error.status:503;
    if(!safe)logger?.error?.('Chatbot request failed',{code:error.code||'CHATBOT_ERROR'});
    if(status===429)res.set('Retry-After','60');res.status(status).json({error:code,message:code});
  });
  return router;
}
module.exports={createChatbotRouter,conversationDTO,messageDTO,pagination};
