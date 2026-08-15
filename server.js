/**
 * YL Messenger v3
 * Render-ready Node.js server with:
 * - PostgreSQL persistence (required on Render for reliable data)
 * - WebSocket realtime messaging with reconnect/heartbeat
 * - Web Push notifications
 * - Local JSON fallback for development only
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { URL } = require('url');
const { Pool } = require('pg');
const webpush = require('web-push');

const PORT = Number(process.env.PORT || 3001);
const DATA_FILE = path.join(__dirname, 'data.json');
const MAX_BODY = 25 * 1024 * 1024;
const HAS_DB = !!process.env.DATABASE_URL;

let pool = null;
if (HAS_DB) {
  pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false },
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000
  });
}

const vapidPublic = process.env.VAPID_PUBLIC_KEY || '';
const vapidPrivate = process.env.VAPID_PRIVATE_KEY || '';
const vapidSubject = process.env.VAPID_SUBJECT || 'mailto:admin@example.com';
if (vapidPublic && vapidPrivate) webpush.setVapidDetails(vapidSubject, vapidPublic, vapidPrivate);

let db = { users:{}, messages:{}, statuses:{}, groups:{} };
function loadLocal(){
  try { if(fs.existsSync(DATA_FILE)) db=JSON.parse(fs.readFileSync(DATA_FILE,'utf8')); } catch(e) { console.warn('Local DB load failed:',e.message); }
  db.users ||= {}; db.messages ||= {}; db.statuses ||= {}; db.groups ||= {};
}
function saveLocal(){ try{ fs.writeFileSync(DATA_FILE, JSON.stringify(db)); }catch(e){ console.warn('Local DB save failed:',e.message); } }
if(!HAS_DB) loadLocal();

async function initDB(){
  if(!pool) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (user_id TEXT PRIMARY KEY, data JSONB NOT NULL);
    CREATE TABLE IF NOT EXISTS messages (chat_key TEXT NOT NULL, ts BIGINT NOT NULL, data JSONB NOT NULL, PRIMARY KEY(chat_key, ts));
    CREATE INDEX IF NOT EXISTS messages_chat_ts_idx ON messages(chat_key, ts DESC);
    CREATE TABLE IF NOT EXISTS groups_data (group_id TEXT PRIMARY KEY, data JSONB NOT NULL);
    CREATE TABLE IF NOT EXISTS statuses (user_id TEXT PRIMARY KEY, ts BIGINT NOT NULL, data JSONB NOT NULL);
    CREATE TABLE IF NOT EXISTS push_subscriptions (endpoint TEXT PRIMARY KEY, user_id TEXT NOT NULL, subscription JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
    CREATE INDEX IF NOT EXISTS push_user_idx ON push_subscriptions(user_id);
  `);
}

async function getUser(id){
  if(!id) return null;
  if(!pool) return db.users[id] || null;
  const r=await pool.query('SELECT data FROM users WHERE user_id=$1',[id]); return r.rows[0]?.data || null;
}
async function upsertUser(u){
  if(!pool){ db.users[u.userId]={...(db.users[u.userId]||{}),...u}; saveLocal(); return db.users[u.userId]; }
  const old=await getUser(u.userId); const merged={...(old||{}),...u};
  await pool.query('INSERT INTO users(user_id,data) VALUES($1,$2) ON CONFLICT(user_id) DO UPDATE SET data=EXCLUDED.data',[u.userId,merged]); return merged;
}
async function findUserByHandle(handle){
  if(!pool) return Object.values(db.users).find(x=>x.handle?.toLowerCase()===handle.toLowerCase())||null;
  const r=await pool.query("SELECT data FROM users WHERE lower(data->>'handle')=lower($1) LIMIT 1",[handle]); return r.rows[0]?.data||null;
}
async function listUsers(){ if(!pool) return Object.values(db.users); const r=await pool.query('SELECT data FROM users'); return r.rows.map(x=>x.data); }
async function getMessages(k){
  if(!pool) return db.messages[k]||[];
  const r=await pool.query('SELECT data FROM messages WHERE chat_key=$1 ORDER BY ts ASC LIMIT 500',[k]); return r.rows.map(x=>x.data);
}
async function insertMessage(k,m){
  if(!pool){ db.messages[k] ||= []; if(!db.messages[k].some(x=>String(x.ts)===String(m.ts))){ db.messages[k].push(m); if(db.messages[k].length>500) db.messages[k]=db.messages[k].slice(-500); saveLocal(); } return; }
  await pool.query('INSERT INTO messages(chat_key,ts,data) VALUES($1,$2,$3) ON CONFLICT(chat_key,ts) DO NOTHING',[k,String(m.ts),m]);
  await pool.query(`DELETE FROM messages WHERE chat_key=$1 AND ts NOT IN (SELECT ts FROM messages WHERE chat_key=$1 ORDER BY ts DESC LIMIT 500)`,[k]);
}
async function getGroup(id){ if(!pool) return db.groups[id]||null; const r=await pool.query('SELECT data FROM groups_data WHERE group_id=$1',[id]); return r.rows[0]?.data||null; }
async function upsertGroup(g){ if(!pool){ db.groups[g.id]={...(db.groups[g.id]||{}),...g}; saveLocal(); return db.groups[g.id]; } await pool.query('INSERT INTO groups_data(group_id,data) VALUES($1,$2) ON CONFLICT(group_id) DO UPDATE SET data=EXCLUDED.data',[g.id,g]); return g; }
async function listGroups(){ if(!pool) return Object.values(db.groups); const r=await pool.query('SELECT data FROM groups_data'); return r.rows.map(x=>x.data); }
async function getStatuses(){
  const cutoff=Date.now()-24*60*60*1000;
  if(!pool) return Object.values(db.statuses).filter(s=>Number(s.ts)>cutoff);
  const r=await pool.query('SELECT data FROM statuses WHERE ts>$1',[String(cutoff)]); return r.rows.map(x=>x.data);
}
async function upsertStatus(s){ if(!pool){ db.statuses[s.userId]=s; saveLocal(); return; } await pool.query('INSERT INTO statuses(user_id,ts,data) VALUES($1,$2,$3) ON CONFLICT(user_id) DO UPDATE SET ts=EXCLUDED.ts,data=EXCLUDED.data',[s.userId,String(s.ts),s]); }

async function getPushSubs(userId){
  if(!pool) return [];
  const r=await pool.query('SELECT endpoint,subscription FROM push_subscriptions WHERE user_id=$1',[userId]); return r.rows;
}
async function savePushSub(userId,sub){
  await pool.query('INSERT INTO push_subscriptions(endpoint,user_id,subscription) VALUES($1,$2,$3) ON CONFLICT(endpoint) DO UPDATE SET user_id=EXCLUDED.user_id,subscription=EXCLUDED.subscription,updated_at=NOW()',[sub.endpoint,userId,sub]);
}
async function deletePushSub(endpoint){ if(pool) await pool.query('DELETE FROM push_subscriptions WHERE endpoint=$1',[endpoint]); }
async function sendPushToUser(userId,payload){
  if(!pool || !vapidPublic || !vapidPrivate) return;
  const rows=await getPushSubs(userId);
  for(const row of rows){
    try { await webpush.sendNotification(row.subscription,JSON.stringify(payload)); }
    catch(e){ if(e.statusCode===404||e.statusCode===410) await deletePushSub(row.endpoint); else console.warn('Push error:',e.statusCode||e.message); }
  }
}

const clients=new Map(); // socket -> {userId,username,handle,activeChat}
function convKey(a,b){ return [a,b].sort().join('::'); }
function parseQuery(req){ return Object.fromEntries(new URL(req.url,'http://localhost').searchParams.entries()); }
function safeOrigin(req){ const configured=process.env.APP_ORIGIN; return configured || (req.headers.origin || '*'); }
function headers(res,req){
  res.setHeader('Access-Control-Allow-Origin', safeOrigin(req));
  res.setHeader('Vary','Origin');
  res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers','Content-Type');
  res.setHeader('Cache-Control','no-store');
  res.setHeader('X-Content-Type-Options','nosniff');
  res.setHeader('Referrer-Policy','strict-origin-when-cross-origin');
}
function json(res,req,data,code=200){ headers(res,req); res.writeHead(code,{'Content-Type':'application/json; charset=utf-8'}); res.end(JSON.stringify(data)); }
function readBody(req){ return new Promise((resolve,reject)=>{ let b=''; let too=false; req.on('data',c=>{ b+=c; if(b.length>MAX_BODY){too=true; req.destroy(); reject(new Error('body_too_large'));} }); req.on('end',()=>{ if(too)return; try{resolve(JSON.parse(b||'{}'));}catch{reject(new Error('invalid_json'));} }); req.on('error',reject); }); }

const server=http.createServer(async(req,res)=>{
  headers(res,req);
  if(req.method==='OPTIONS'){res.writeHead(204);return res.end();}
  const u=new URL(req.url,'http://localhost');
  if(u.pathname.startsWith('/api/')) return handleAPI(req,res,u);
  if(u.pathname==='/sw.js') return serveFile(res,path.join(__dirname,'sw.js'),'application/javascript; charset=utf-8',false);
  if(u.pathname==='/manifest.webmanifest') return serveFile(res,path.join(__dirname,'manifest.webmanifest'),'application/manifest+json',false);
  const rel=u.pathname==='/'?'/index.html':u.pathname;
  const file=path.normalize(path.join(__dirname,rel));
  if(!file.startsWith(__dirname)) return serveFile(res,'', 'text/plain',true,403);
  const ext=path.extname(file); const mime={'.html':'text/html; charset=utf-8','.js':'application/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json; charset=utf-8','.webmanifest':'application/manifest+json','.png':'image/png','.svg':'image/svg+xml'}[ext]||'application/octet-stream';
  return serveFile(res,file,mime,false);
});
function serveFile(res,file,mime,empty=false,code=404){ if(empty){res.writeHead(code);return res.end('Not Found');} fs.readFile(file,(e,d)=>{if(e){res.writeHead(404);return res.end('Not Found');}res.writeHead(200,{'Content-Type':mime});res.end(d);}); }

async function handleAPI(req,res,u){
  try{
    if(u.pathname==='/api/health') return json(res,req,{ok:true,db:!!pool,push:!!(vapidPublic&&vapidPrivate),time:new Date().toISOString()});
    if(u.pathname==='/api/ping') return json(res,req,{ok:true});
    if(u.pathname==='/api/notifications/vapid-public-key'&&req.method==='GET') return json(res,req,{publicKey:vapidPublic||null});
    if(u.pathname==='/api/notifications/subscribe'&&req.method==='POST'){
      if(!pool) return json(res,req,{error:'database_required'},503);
      const b=await readBody(req); if(!b.userId||!b.subscription?.endpoint) return json(res,req,{error:'missing'},400); await savePushSub(b.userId,b.subscription); return json(res,req,{ok:true});
    }
    if(u.pathname==='/api/notifications/unsubscribe'&&req.method==='POST'){
      if(!pool) return json(res,req,{ok:true}); const b=await readBody(req); if(b.endpoint) await deletePushSub(b.endpoint); return json(res,req,{ok:true});
    }
    if(u.pathname==='/api/user'&&req.method==='GET'){
      const p=parseQuery(req); if(p.id) return json(res,req,await getUser(p.id)); if(p.handle) return json(res,req,await findUserByHandle(p.handle)); return json(res,req,null);
    }
    if(u.pathname==='/api/user'&&req.method==='POST'){
      const b=await readBody(req); if(!b.userId) return json(res,req,{error:'userId required'},400); if(b.handle){const ex=await findUserByHandle(b.handle);if(ex&&ex.userId!==b.userId)return json(res,req,{error:'handle_taken'},409);} const user=await upsertUser(b); broadcastAll({type:'profile_update',user:{userId:user.userId,username:user.username,handle:user.handle,avatarData:user.avatarData||null}}); return json(res,req,user);
    }
    if(u.pathname==='/api/users/search'&&req.method==='GET'){ const q=(parseQuery(req).q||'').toLowerCase(); const r=(await listUsers()).filter(x=>(x.username||'').toLowerCase().includes(q)||(x.handle||'').toLowerCase().includes(q)).slice(0,20); return json(res,req,r); }
    if(u.pathname==='/api/messages'&&req.method==='GET'){ const p=parseQuery(req); return json(res,req,await getMessages(convKey(p.a,p.b))); }
    if(u.pathname==='/api/messages'&&req.method==='POST'){
      const b=await readBody(req); if(!b.fromId||!b.toId) return json(res,req,{error:'missing'},400); const su=await getUser(b.fromId); const m={...b,fromName:su?.username||b.fromName,id:'m_'+b.ts}; await insertMessage(convKey(b.fromId,b.toId),m); return json(res,req,m);
    }
    if(u.pathname==='/api/groups/search'&&req.method==='GET'){const q=(parseQuery(req).q||'').toLowerCase();return json(res,req,(await listGroups()).filter(g=>(g.name||'').toLowerCase().includes(q)).slice(0,20));}
    if(u.pathname==='/api/groups'&&req.method==='POST'){const b=await readBody(req);if(!b.id||!b.name)return json(res,req,{error:'missing'},400);const g=await upsertGroup(b);return json(res,req,g);}
    if(u.pathname.startsWith('/api/groups/')&&req.method==='GET'){return json(res,req,await getGroup(u.pathname.split('/').pop()));}
    if(u.pathname==='/api/group-messages'&&req.method==='GET'){return json(res,req,await getMessages('gc::'+parseQuery(req).chatId));}
    if(u.pathname==='/api/group-messages'&&req.method==='POST'){const b=await readBody(req);if(!b.fromId||!b.chatId)return json(res,req,{error:'missing'},400);const su=await getUser(b.fromId);const m={...b,fromName:su?.username||b.fromName,id:'m_'+b.ts};await insertMessage('gc::'+b.chatId,m);return json(res,req,m);}
    if(u.pathname==='/api/status'&&req.method==='POST'){const b=await readBody(req);if(!b.userId||!b.videoData)return json(res,req,{error:'missing'},400);const s={userId:b.userId,username:b.username,avatarData:b.avatarData||null,videoData:b.videoData,ts:Date.now()};await upsertStatus(s);broadcastAll({type:'new_status',status:s});return json(res,req,{ok:true});}
    if(u.pathname==='/api/statuses'&&req.method==='GET')return json(res,req,await getStatuses());
    if(u.pathname==='/api/online'&&req.method==='GET')return json(res,req,[...clients.values()].filter(c=>c.userId).map(c=>({userId:c.userId,username:c.username,handle:c.handle})));
    return json(res,req,{error:'not found'},404);
  }catch(e){ console.error('API error',u.pathname,e); return json(res,req,{error:'server_error',message:process.env.NODE_ENV==='development'?e.message:'internal error'},500); }
}

server.on('upgrade',(req,socket)=>{
  const key=req.headers['sec-websocket-key']; if(!key){socket.destroy();return;}
  const accept=crypto.createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');
  clients.set(socket,{userId:null,username:null,handle:null,activeChat:null}); let buf=Buffer.alloc(0);
  socket.on('data',chunk=>{buf=Buffer.concat([buf,chunk]);for(;;){const txt=frame();if(txt===null)break;if(!txt)continue;try{handleWS(socket,JSON.parse(txt));}catch{}}});
  function frame(){if(buf.length<2)return null;let len=buf[1]&127,off=2;const masked=!!(buf[1]&128);if(len===126){if(buf.length<4)return null;len=buf.readUInt16BE(2);off=4;}else if(len===127){if(buf.length<10)return null;len=Number(buf.readBigUInt64BE(2));off=10;}if(len>MAX_BODY){socket.destroy();return null;}const total=off+(masked?4:0)+len;if(buf.length<total)return null;let mask;if(masked){mask=buf.slice(off,off+4);off+=4;}let data=buf.slice(off,off+len);if(masked){const d=Buffer.alloc(len);for(let i=0;i<len;i++)d[i]=data[i]^mask[i%4];data=d;}buf=buf.slice(total);return data.toString('utf8');}
  socket.on('close',()=>{const i=clients.get(socket);if(i?.userId)broadcast({type:'user_offline',userId:i.userId},socket);clients.delete(socket);});
  socket.on('error',()=>clients.delete(socket));
});

async function handleWS(socket,msg){
  const info=clients.get(socket); if(!info)return;
  if(msg.type==='ping'||msg.type==='pong'){wsSend(socket,{type:'pong'});return;}
  if(msg.type==='set_active_chat'){info.activeChat=msg.chatId||null;return;}
  if(msg.type==='join'){
    if(!msg.userId)return; Object.assign(info,{userId:msg.userId,username:msg.username||'',handle:msg.handle||null,activeChat:null}); await upsertUser({userId:msg.userId,username:msg.username,handle:msg.handle||null,...(msg.avatarData?{avatarData:msg.avatarData}:{})});
    wsSend(socket,{type:'init',onlineUsers:[...clients.values()].filter(c=>c.userId).map(c=>({userId:c.userId,username:c.username,handle:c.handle}))});
    broadcast({type:'user_online',userId:msg.userId,username:msg.username,handle:msg.handle||null,avatarData:msg.avatarData||null},socket); return;
  }
  if(msg.type==='profile_update'){
    if(!info.userId||!msg.user||msg.user.userId!==info.userId)return; const u=await getUser(info.userId); if(u){const patch={userId:info.userId}; if(msg.user.username)patch.username=msg.user.username;if(msg.user.handle)patch.handle=msg.user.handle;if(msg.user.avatarData!==undefined)patch.avatarData=msg.user.avatarData;await upsertUser({...u,...patch});} broadcast({type:'profile_update',user:{userId:info.userId,username:msg.user.username,handle:msg.user.handle,avatarData:msg.user.avatarData||null}},socket);return;
  }
  if(msg.type==='chat_message'){
    if(!info.userId)return; const ts=msg.ts||Date.now()*1000; const su=await getUser(info.userId); const payload={type:'chat_message',fromId:info.userId,fromName:su?.username||info.username,toId:msg.toId||null,chatId:msg.chatId||null,text:msg.text||'',mediaData:msg.mediaData||null,mediaType:msg.mediaType||null,fileName:msg.fileName||null,voiceDuration:msg.voiceDuration||null,time:msg.time||new Date().toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit'}),ts,uploadId:msg.uploadId||null};
    if(msg.toId){const k=convKey(info.userId,msg.toId);await insertMessage(k,{...payload,type:undefined});for(const [s,c] of clients){if(c.userId===msg.toId){wsSend(s,payload);}} const recipient=[...clients.values()].find(c=>c.userId===msg.toId); if(!recipient||recipient.activeChat!==info.userId){await sendPushToUser(msg.toId,{title:payload.fromName||'Новое сообщение',body:payload.text||'Вам отправили медиа',data:{chatId:info.userId}});} }
    if(msg.chatId&&!msg.toId){const k='gc::'+msg.chatId;await insertMessage(k,{...payload,type:undefined});const g=await getGroup(msg.chatId);const ids=new Set((g?.members||[]).map(m=>m.id));for(const [s,c] of clients){if(c.userId&&c.userId!==info.userId&&ids.has(c.userId))wsSend(s,payload);}for(const id of ids){if(id===info.userId)continue;const c=[...clients.values()].find(x=>x.userId===id);if(!c||c.activeChat!==msg.chatId)await sendPushToUser(id,{title:payload.fromName||'Новое сообщение',body:payload.text||'Новое сообщение в группе',data:{chatId:msg.chatId}});}}
    wsSend(socket,{...payload,echo:true});return;
  }
  if(msg.type==='upload_start'){
    if(!info.userId)return;const p={type:'upload_start',uploadId:msg.uploadId,fromId:info.userId,chatId:msg.chatId||null,fileName:msg.fileName||null,mediaType:msg.mediaType||null};if(msg.toId)for(const [s,c]of clients)if(c.userId===msg.toId)wsSend(s,p);else if(msg.chatId){const g=await getGroup(msg.chatId);const ids=new Set((g?.members||[]).map(m=>m.id));for(const [s,c]of clients)if(c.userId&&c.userId!==info.userId&&ids.has(c.userId))wsSend(s,p);}return;
  }
  if(msg.type==='typing'){if(!info.userId)return;if(msg.toId)for(const [s,c]of clients)if(c.userId===msg.toId)wsSend(s,{type:'typing',fromId:info.userId,fromName:info.username});if(msg.chatId){const g=await getGroup(msg.chatId);const ids=new Set((g?.members||[]).map(m=>m.id));for(const [s,c]of clients)if(c.userId&&c.userId!==info.userId&&ids.has(c.userId))wsSend(s,{type:'typing',fromId:info.userId,fromName:info.username,chatId:msg.chatId});}return;}
  if(msg.type==='msg_read'){if(!info.userId||!msg.fromId)return;for(const [s,c]of clients)if(c.userId===msg.fromId)wsSend(s,{type:'msg_read',ts:msg.ts,byUserId:info.userId});return;}
  if(msg.type==='group_update'){if(!info.userId||!msg.group?.id||!msg.group.name)return;const g=await upsertGroup(msg.group);const ids=new Set((g.members||[]).map(m=>m.id));for(const [s,c]of clients)if(c.userId&&c.userId!==info.userId&&ids.has(c.userId))wsSend(s,{type:'group_update',group:g});}
}
function wsSend(socket,data){try{if(!socket.writable)return;const p=Buffer.from(JSON.stringify(data));let h;if(p.length<126)h=Buffer.from([0x81,p.length]);else if(p.length<65536)h=Buffer.from([0x81,126,(p.length>>8)&255,p.length&255]);else{h=Buffer.alloc(10);h[0]=0x81;h[1]=127;h.writeBigUInt64BE(BigInt(p.length),2);}socket.write(Buffer.concat([h,p]));}catch{}}
function broadcast(data,excl){for(const [s,c]of clients)if(s!==excl&&c.userId)wsSend(s,data);} function broadcastAll(data){for(const [s,c]of clients)if(c.userId)wsSend(s,data);}

async function shutdown(){try{for(const s of clients.keys())s.destroy();await pool?.end();}finally{process.exit(0);}}
process.on('SIGTERM',shutdown);process.on('SIGINT',shutdown);
initDB().then(()=>server.listen(PORT,'0.0.0.0',()=>console.log(`YL Messenger v3 listening on ${PORT} | DB: ${!!pool} | Push: ${!!(vapidPublic&&vapidPrivate)}`))).catch(e=>{console.error('Database initialization failed:',e);process.exit(1);});
