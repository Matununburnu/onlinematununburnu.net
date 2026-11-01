// Azizin Minik Kuşu - çok oyunculu oda sunucusu
// Kurulum: npm i socket.io  |  Çalıştır: node server.js  (PORT ortam değişkeni ile port seçilir)
const http = require('http');
const { Server } = require('socket.io');
// Düz HTTP isteklerine "ok" döner (Render sağlık kontrolü / uyandırma pingi için); socket.io kendi yolunu yönetir
const httpServer = http.createServer((req, res) => { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); });
const io = new Server(httpServer, {
  cors: { origin: '*' },
  pingInterval: 10000, pingTimeout: 10000,   // kopan bağlantı ~20 sn yerine ~10-20 sn içinde anlaşılır
  perMessageDeflate: false,                  // küçük paketlerde sıkıştırma gecikme yaratır
  maxHttpBufferSize: 1e5
});
httpServer.listen(process.env.PORT || 3000);

const MAX = 10, MAX_ROOMS = 500, TICK = 33, rooms = new Map();
const code = () => { let c; do { c = Array.from({ length: 4 }, () => 'ABCDEFGHJKLMNPRSTUVYZ'[Math.random() * 21 | 0]).join(''); } while (rooms.has(c)); return c; };
const num = (v, lo, hi) => (typeof v === 'number' && isFinite(v)) ? Math.max(lo, Math.min(hi, v)) : 0;
const pub = r => ({ code: r.code, started: r.started, public: !!r.public, name: r.name || '', players: [...r.players.values()].map(p => ({ id: p.id, name: p.name, bird: p.bird, host: p.id === r.host, score: p.score, alive: p.alive, ready: !!p.ready })) });
const push = r => io.to(r.code).emit('room', pub(r));
function leave(s) {
  const r = rooms.get(s.data.room); s.data.room = null;
  if (!r) return;
  r.players.delete(s.id); s.leave(r.code);
  if (!r.players.size) { rooms.delete(r.code); return; }
  if (r.host === s.id) r.host = r.players.keys().next().value;
  push(r); check(r);
}
function check(r) {
  if (r.started && ![...r.players.values()].some(p => p.alive)) {
    r.started = false;
    r.players.forEach(p => { p.ready = false; });   // tur bitti: sonraki oyun için herkes yeniden hazır olmalı
    io.to(r.code).emit('over', pub(r).players.sort((a, b) => b.score - a.score));
    push(r);
  }
}
function join(s, r, d) {
  const cid = String(d.cid || '').slice(0, 40);
  if (cid && r.banned.has(cid)) return s.emit('err', 'Bu odadan atıldın, tekrar giremezsin');
  if (r.started && !(cid && r.members.has(cid))) return s.emit('err', 'Oyun başladı');   // yanlışlıkla çıkan eski üye oyun sürerken de geri girebilir (izleyici olarak)
  if (r.players.size >= MAX) return s.emit('err', 'Oda dolu (10/10)');
  const name = String(d.name || 'Oyuncu').replace(/[<>&]/g, '').trim().slice(0, 12) || 'Oyuncu';
  if (cid) r.members.add(cid);
  r.players.set(s.id, { id: s.id, cid, name, bird: num(d.bird, 0, 999) | 0, score: 0, alive: !r.started, ready: false, last: Date.now(), y: 0, vy: 0, st: 0, dirty: false });
  s.join(r.code); s.data.room = r.code; push(r);
}
// Aktif (sekmesi açık/görünür) oyuncu sayısı
let lastOn = -1;
const online = () => { let n = 0, now = Date.now(); io.sockets.sockets.forEach(x => { if (now - (x.data.hb || x.data.t0 || now) < 30000 && (x.data.vis !== false || x.data.room)) n++; }); return n; };
const pushOnline = () => { const n = online(); if (n !== lastOn) { lastOn = n; io.emit('online', n); } };
setInterval(pushOnline, 3000);
setInterval(() => { lastOn = -1; pushOnline(); }, 15000);

io.on('connection', s => {
  s.data.t0 = Date.now(); s.data.hb = Date.now();
  s.on('hb', () => { s.data.hb = Date.now(); });
  s.emit('online', online()); pushOnline();
  s.on('vis', v => { s.data.vis = !!v; pushOnline(); });
  s.on('t', (c, cb) => { if (typeof cb === 'function') cb(Date.now()); });   // saat eşitleme
  s.on('create', d => {
    d = (d && typeof d === 'object') ? d : {};
    if (s.data.room) leave(s);
    if (rooms.size >= MAX_ROOMS) return s.emit('err', 'Sunucu şu an dolu, biraz sonra dene');
    const rname = String(d.rname || '').replace(/[<>&]/g, '').trim().slice(0, 15);
    const r = { code: code(), host: s.id, started: false, public: d.public === true || d.public === 1, name: rname, players: new Map(), slots: {}, claimed: new Set(), banned: new Set(), members: new Set() };
    rooms.set(r.code, r); join(s, r, d);
  });
  s.on('join', d => {
    d = (d && typeof d === 'object') ? d : {};
    if (s.data.room) leave(s);
    const r = rooms.get(String(d.code || '').trim().toUpperCase());
    r ? join(s, r, d) : s.emit('err', 'Oda bulunamadı');
  });
  // Herkese açık odalar: henüz başlamamış ve dolu olmayan odalar listelenir
  s.on('list', cb => {
    if (typeof cb !== 'function') return;
    const out = [];
    // Özel odalar da listelenir (kilitli), ama kodları ASLA gönderilmez; herkese açık odaların kodu gönderilir
    rooms.forEach(r => { const h = r.players.get(r.host); out.push({ code: r.public ? r.code : undefined, pub: !!r.public, n: r.players.size, host: h ? h.name : 'Oyuncu', started: r.started, name: r.name || '' }); });
    const rank = x => (!x.started && x.n < MAX ? 0 : 1) * 2 + (x.pub ? 0 : 1);
    out.sort((a, b) => rank(a) - rank(b) || b.n - a.n);
    cb(out.slice(0, 40));
  });
  // Oda sahibi odayı herkese açık / özel yapabilir
  s.on('setpub', v => { const r = rooms.get(s.data.room); if (!r || r.host !== s.id) return; r.public = !!v; push(r); });
  // Hazır ol sistemi: oda sahibi dışındaki oyuncular hazır olduğunu bildirir
  s.on('ready', v => {
    const r = rooms.get(s.data.room), p = r && r.players.get(s.id);
    if (!p || r.started || s.id === r.host) return;
    p.ready = !!v; push(r);
  });
  s.on('start', () => {
    const r = rooms.get(s.data.room); if (!r || r.host !== s.id || r.started) return;
    const others = [...r.players.values()].filter(p => p.id !== r.host);
    if (others.length && !others.every(p => p.ready)) return s.emit('err', 'Herkes hazır olmadan başlatamazsın');
    r.started = true; r.claimed = new Set(); r.slots = {}; let i = 0;
    r.players.forEach(p => { p.alive = true; p.score = 0; p.dirty = false; p.last = Date.now() + 4000; r.slots[p.id] = i++; });
    // Herkes aynı sunucu saatinde başlar: 3 sn geri sayım + 0.5 sn tampon
    io.to(r.code).emit('start', { seed: Math.random() * 4294967296 >>> 0, startAt: Date.now() + 3500, slots: r.slots, n: i }); push(r);
  });
  // Konum paketi: yalnızca saklanır; odaya 30 Hz'lik TOPLU pakette ('S') gönderilir (oyuncu başına ayrı paket yok → ağ yükü ~10 kat azalır)
  s.on('s', a => {
    const r = rooms.get(s.data.room), p = r && r.players.get(s.id);
    if (!p || !p.alive || !r.started || !Array.isArray(a)) return;
    p.y = num(a[0], -5000, 20000) | 0; p.vy = num(a[1], -2000, 2000) | 0; p.score = num(a[2], 0, 1e7) | 0; p.st = num(a[3], 0, 1e9) | 0; p.dirty = true; p.last = Date.now();
  });
  s.on('dead', sc => {
    const r = rooms.get(s.data.room), p = r && r.players.get(s.id);
    if (!p || !r.started || !p.alive) return;
    p.alive = false; p.score = num(sc, 0, 1e7) | 0;
    io.to(r.code).emit('gone', { id: s.id, score: p.score }); push(r); check(r);
  });
  // Eklenti: ilk alan kazanır, sonuç odadaki herkese gider (haritadan herkes için kalkar)
  s.on('pu', d => {
    const r = rooms.get(s.data.room), p = r && r.players.get(s.id);
    if (!r || !p || !p.alive || !r.started || !d || typeof d.id !== 'number' || r.claimed.has(d.id)) return;
    r.claimed.add(d.id); io.to(r.code).emit('pu', { id: d.id, by: s.id, type: String(d.type).slice(0, 20) });
  });
  // Oda sahibi oyuncuyu atar: aynı istemci kimliği bu odaya bir daha giremez
  s.on('kick', id => {
    const r = rooms.get(s.data.room); if (!r || r.host !== s.id || r.started || typeof id !== 'string' || id === s.id) return;
    const p = r.players.get(id), t = io.sockets.sockets.get(id); if (!p) return;
    if (p.cid) r.banned.add(p.cid);
    if (t) { t.emit('kicked'); leave(t); } else { r.players.delete(id); push(r); }
  });
  s.on('leave', () => leave(s));
  s.on('disconnect', () => { leave(s); setTimeout(pushOnline, 50); });
});

// 30 Hz toplu konum yayını: [slot, y, vy, skor, oyun adımı, ...] (adım numarası sayesinde herkes konumu aynı oyun zamanında çizer)
setInterval(() => rooms.forEach(r => {
  if (!r.started) return;
  const out = [];
  r.players.forEach(p => { if (p.alive && p.dirty) { p.dirty = false; const sl = r.slots[p.id]; if (sl !== undefined) out.push(sl, p.y, p.vy, p.score, p.st); } });
  if (out.length) io.to(r.code).volatile.emit('P', out);
}), TICK);

// 8 sn hiç veri göndermeyen (bağlantısı kopmuş/donmuş) canlı oyuncuyu elenmiş say
setInterval(() => rooms.forEach(r => {
  if (!r.started) return;
  const now = Date.now(), dead = [];
  r.players.forEach(p => { if (p.alive && now - p.last > 8000) { p.alive = false; dead.push(p); } });
  if (!dead.length) return;
  dead.forEach(p => io.to(r.code).emit('gone', { id: p.id, score: p.score }));
  push(r); check(r);
}), 2000);
console.log('MP sunucu hazır');
