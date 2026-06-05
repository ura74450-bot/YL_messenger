/**
 * YL Messenger — server.js
 * Pure Node.js, zero external dependencies.
 * HTTP + WebSocket (RFC 6455 manual) + persistent data.json
 */
const http   = require('http');
const fs     = require('fs');
const path   = require('path');
const crypto = require('crypto');

const PORT      = process.env.PORT || 3001;
const DATA_FILE = path.join(__dirname, 'data.json');

// ── DB ───────────────────────────────────────────────────────
let db = { users: {}, messages: {}, statuses: {} };
function loadDB() {
  try { if (fs.existsSync(DATA_FILE)) db = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch {}
  db.users    = db.users    || {};
  db.messages = db.messages || {};
  db.statuses = db.statuses || {};
}
function saveDB() { try { fs.writeFileSync(DATA_FILE, JSON.stringify(db)); } catch {} }
loadDB();

// ── Connected clients ────────────────────────────────────────
const clients = new Map(); // socket → { userId, username, handle }

// ── HTTP ─────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  const [urlPath] = req.url.split('?');
  if (urlPath.startsWith('/api/')) { handleAPI(req, res, urlPath); return; }

  const filePath = path.join(__dirname, urlPath === '/' ? 'index.html' : urlPath);
  const mimes = { '.html':'text/html;charset=utf-8', '.js':'application/javascript',
    '.css':'text/css', '.png':'image/png', '.jpg':'image/jpeg', '.ico':'image/x-icon' };
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not Found'); return; }
    res.writeHead(200, { 'Content-Type': mimes[path.extname(filePath)] || 'text/plain' });
    res.end(data);
  });
});

function readBody(req) {
  return new Promise(resolve => {
    let b = '';
    req.on('data', c => { b += c; if (b.length > 50*1024*1024) b = b.slice(0, 50*1024*1024); });
    req.on('end', () => { try { resolve(JSON.parse(b)); } catch { resolve({}); } });
  });
}
function json(res, data, code = 200) {
  res.writeHead(code, { 'Content-Type':'application/json','Access-Control-Allow-Origin':'*' });
  res.end(JSON.stringify(data));
}
function convKey(a, b) { return [a, b].sort().join('::'); }

async function handleAPI(req, res, urlPath) {
  if (urlPath === '/api/ping') return json(res, { ok: true });

  // GET /api/user?handle=xxx  or  ?id=xxx
  if (urlPath === '/api/user' && req.method === 'GET') {
    const p = Object.fromEntries((req.url.split('?')[1]||'').split('&').map(x=>x.split('=')));
    if (p.id) return json(res, db.users[p.id] || null);
    if (p.handle) {
      const u = Object.values(db.users).find(x => x.handle && x.handle.toLowerCase() === decodeURIComponent(p.handle).toLowerCase());
      return json(res, u || null);
    }
    return json(res, null);
  }

  // POST /api/user
  if (urlPath === '/api/user' && req.method === 'POST') {
    const b = await readBody(req);
    if (!b.userId) return json(res, { error: 'userId required' }, 400);
    // handle uniqueness check
    if (b.handle) {
      const existing = Object.values(db.users).find(x => x.handle && x.handle.toLowerCase() === b.handle.toLowerCase() && x.userId !== b.userId);
      if (existing) return json(res, { error: 'handle_taken' }, 409);
    }
    db.users[b.userId] = { ...db.users[b.userId], ...b };
    saveDB();
    broadcastAll({ type: 'profile_update', user: { userId: b.userId, username: b.username, handle: b.handle, avatarData: b.avatarData || null } });
    return json(res, db.users[b.userId]);
  }

  // GET /api/users/search?q=
  if (urlPath === '/api/users/search' && req.method === 'GET') {
    const p = Object.fromEntries((req.url.split('?')[1]||'').split('&').map(x=>x.split('=').map(decodeURIComponent)));
    const q = (p.q||'').toLowerCase();
    const results = Object.values(db.users).filter(u =>
      (u.username && u.username.toLowerCase().includes(q)) ||
      (u.handle   && u.handle.toLowerCase().includes(q))
    ).slice(0, 20);
    return json(res, results);
  }

  // GET /api/messages?a=&b=
  if (urlPath === '/api/messages' && req.method === 'GET') {
    const p = Object.fromEntries((req.url.split('?')[1]||'').split('&').map(x=>x.split('=')));
    return json(res, db.messages[convKey(p.a, p.b)] || []);
  }

  // POST /api/messages
  if (urlPath === '/api/messages' && req.method === 'POST') {
    const b = await readBody(req);
    const k = convKey(b.fromId, b.toId);
    if (!db.messages[k]) db.messages[k] = [];
    const m = { id: 'm_'+Date.now()+'_'+Math.random().toString(36).slice(2,5),
      fromId:b.fromId, fromName:b.fromName, toId:b.toId,
      text:b.text||'', mediaData:b.mediaData||null, mediaType:b.mediaType||null,
      fileName:b.fileName||null, time:b.time, ts:b.ts||Date.now() };
    db.messages[k].push(m);
    if (db.messages[k].length > 500) db.messages[k] = db.messages[k].slice(-500);
    saveDB();
    return json(res, m);
  }

  // POST /api/status  { userId, videoData, ts }
  if (urlPath === '/api/status' && req.method === 'POST') {
    const b = await readBody(req);
    if (!b.userId || !b.videoData) return json(res, { error: 'missing' }, 400);
    db.statuses[b.userId] = { userId:b.userId, username:b.username, avatarData:b.avatarData||null,
      videoData:b.videoData, ts:Date.now() };
    saveDB();
    broadcastAll({ type: 'new_status', status: db.statuses[b.userId] });
    return json(res, { ok: true });
  }

  // GET /api/statuses
  if (urlPath === '/api/statuses' && req.method === 'GET') {
    const now = Date.now();
    const fresh = Object.values(db.statuses).filter(s => now - s.ts < 24*60*60*1000);
    return json(res, fresh);
  }

  // GET /api/online
  if (urlPath === '/api/online' && req.method === 'GET') {
    return json(res, [...clients.values()].filter(c=>c.userId).map(c=>({userId:c.userId,username:c.username,handle:c.handle})));
  }

  json(res, { error: 'not found' }, 404);
}

// ── WebSocket ─────────────────────────────────────────────────
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');

  clients.set(socket, { userId:null, username:null, handle:null });
  let buf = Buffer.alloc(0);

  socket.on('data', chunk => {
    buf = Buffer.concat([buf, chunk]);
    for (;;) {
      const txt = extractFrame();
      if (txt === null) break;
      if (txt === '') continue;
      let msg; try { msg = JSON.parse(txt); } catch { continue; }
      handleWS(socket, msg);
    }
  });

  function extractFrame() {
    if (buf.length < 2) return null;
    const masked = !!(buf[1]&0x80);
    let pLen = buf[1]&0x7f, off = 2;
    if (pLen===126) { if(buf.length<4) return null; pLen=buf.readUInt16BE(2); off=4; }
    else if (pLen===127) { if(buf.length<10) return null; pLen=Number(buf.readBigUInt64BE(2)); off=10; }
    const total = off+(masked?4:0)+pLen;
    if (buf.length < total) return null;
    let mask; if (masked) { mask=buf.slice(off,off+4); off+=4; }
    let data = buf.slice(off, off+pLen);
    if (masked) { const d=Buffer.alloc(pLen); for(let i=0;i<pLen;i++) d[i]=data[i]^mask[i%4]; data=d; }
    buf = buf.slice(total);
    const op = (buf[0]||0)&0x0f; // already sliced, use first byte before
    return data.toString('utf8');
  }

  socket.on('close', () => {
    const info = clients.get(socket);
    if (info?.userId) broadcast({ type:'user_offline', userId:info.userId }, socket);
    clients.delete(socket);
  });
  socket.on('error', () => clients.delete(socket));
});

function handleWS(socket, msg) {
  const info = clients.get(socket);
  switch (msg.type) {
    case 'join': {
      if (!info) return;
      Object.assign(info, { userId:msg.userId, username:msg.username, handle:msg.handle||null });
      if (!db.users[msg.userId]) db.users[msg.userId] = {};
      Object.assign(db.users[msg.userId], { userId:msg.userId, username:msg.username, handle:msg.handle||null });
      if (msg.avatarData) db.users[msg.userId].avatarData = msg.avatarData;
      saveDB();
      wsSend(socket, { type:'init',
        onlineUsers: [...clients.values()].filter(c=>c.userId).map(c=>({userId:c.userId,username:c.username,handle:c.handle})) });
      broadcast({ type:'user_online', userId:msg.userId, username:msg.username, handle:msg.handle||null, avatarData:msg.avatarData||null }, socket);
      break;
    }
    case 'chat_message': {
      if (!info?.userId) return;
      const payload = { type:'chat_message', id:'m_'+Date.now(),
        fromId:info.userId, fromName:info.username, toId:msg.toId,
        text:msg.text||'', mediaData:msg.mediaData||null, mediaType:msg.mediaType||null,
        fileName:msg.fileName||null,
        time:new Date().toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit'}), ts:Date.now() };
      // persist
      const k = convKey(info.userId, msg.toId);
      if (!db.messages[k]) db.messages[k]=[];
      db.messages[k].push(payload);
      if (db.messages[k].length>500) db.messages[k]=db.messages[k].slice(-500);
      saveDB();
      // deliver
      for (const [s,c] of clients) { if (c.userId===msg.toId) wsSend(s, payload); }
      wsSend(socket, { ...payload, echo:true });
      break;
    }
    case 'typing': {
      if (!info?.userId || !msg.toId) return;
      for (const [s,c] of clients) { if (c.userId===msg.toId) wsSend(s,{type:'typing',fromId:info.userId,fromName:info.username}); }
      break;
    }
    case 'msg_read': {
      // Forward read receipt to the original sender
      if (!info?.userId || !msg.fromId) return;
      for (const [s,c] of clients) {
        if (c.userId===msg.fromId) {
          wsSend(s, { type:'msg_read', ts:msg.ts, byUserId:info.userId });
        }
      }
      break;
    }
  }
}

function wsSend(socket, data) {
  try {
    if (!socket.writable) return;
    const p = Buffer.from(JSON.stringify(data),'utf8'); const len=p.length;
    let h;
    if (len<126) h=Buffer.from([0x81,len]);
    else if (len<65536) h=Buffer.from([0x81,126,(len>>8)&0xff,len&0xff]);
    else { h=Buffer.alloc(10); h[0]=0x81; h[1]=127; h.writeBigUInt64BE(BigInt(len),2); }
    socket.write(Buffer.concat([h,p]));
  } catch {}
}
function broadcast(data, excl) { for(const[s,c]of clients){if(s!==excl&&c.userId)wsSend(s,data);} }
function broadcastAll(data) { for(const[s,c]of clients){if(c.userId)wsSend(s,data);} }

server.listen(PORT, '0.0.0.0', () => {
  const os = require('os');
  const nets = Object.values(os.networkInterfaces()).flat().filter(i=>i.family==='IPv4'&&!i.internal);
  console.log('\n💬  YL Messenger\n');
  console.log(`    Локально:  http://localhost:${PORT}`);
  nets.forEach(n => console.log(`    По сети:   http://${n.address}:${PORT}`));
  console.log();
});
