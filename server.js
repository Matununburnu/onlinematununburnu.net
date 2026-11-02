// Azizin Minik Kuşu - çok oyunculu oda sunucusu
// Kurulum: npm i socket.io  |  Çalıştır: node server.js  (PORT ortam değişkeni ile port seçilir)
const http = require('http');
// Tek bir bozuk paket / beklenmeyen hata tüm sunucuyu (ve açık tüm odaları) düşürmesin
process.on('uncaughtException', e => console.error('[uncaught]', e && e.stack || e));
process.on('unhandledRejection', e => console.error('[unhandled]', e));
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
// Aynı IP'den çok fazla bağlantıyı reddet (siteyi yoğunlaştırıp çökertmeyi zorlaştırır)
io.use((s, next) => { const ip = ipOf(s); if ((connsByIp.get(ip) || 0) >= MAX_CONN_PER_IP) return next(new Error('Çok fazla bağlantı')); s.data.ip = ip; next(); });

// Ortak olay takvimi: herkes için aynı; t = tur başından ms, d = süre ms (ilk ~2-3 sn uyarı süresidir)
// wind: yukarı/aşağı rüzgar · rwind: yönü sürekli değişen TERS RÜZGAR · storm: FIRTINA (savurma + şimşek)
// moon: AY YERÇEKİMİ (hafif) · heavy: AĞIR YERÇEKİMİ · grav: TERS YERÇEKİMİ
function makeEvents() {
  const ev = [], types = ['wind', 'rwind', 'storm', 'moon', 'heavy', 'grav'];
  const dur = { wind: () => 9000 + Math.random() * 3000, rwind: () => 12000 + Math.random() * 3000, storm: () => 10000 + Math.random() * 3000, moon: () => 10000 + Math.random() * 2000, heavy: () => 8000 + Math.random() * 2000, grav: () => 14000 };
  let t = 20000 + Math.random() * 8000, last = '';
  while (t < 20 * 60 * 1000) {
    let type; do { type = types[Math.random() * types.length | 0]; } while (type === last);
    last = type;
    const d = Math.round(dur[type]());
    ev.push({ type, t: Math.round(t), d, dir: Math.random() < 0.5 ? -1 : 1 });
    t += d + 4000 + 12000 + Math.random() * 10000;   // olaylar çakışmaz
  }
  return ev;
}

const MAX = 10, MAX_ROOMS = 500, TICK = 33, rooms = new Map();
// --- Kötüye kullanım / aşırı yük koruması ---
const MAX_CONN_PER_IP = 40;        // aynı IP'den en fazla eşzamanlı bağlantı (okul / yurt gibi ortak IP'ler için 12 çok azdı)
const MAX_ROOMS_PER_IP = 3;        // aynı IP'nin aynı anda açık tutabileceği oda (okul / ev ağı payı bırakıldı)
const CREATE_COOLDOWN = 5000;      // iki oda kurma arası en az süre (ms)
const connsByIp = new Map(), lastCreate = new Map();
const ipOf = s => { const h = s.handshake || {}, xf = h.headers && h.headers['x-forwarded-for']; return String((xf ? String(xf).split(',')[0] : h.address) || 'x').trim(); };
// Olay başına bekleme süresi (flood koruması): izin yoksa false döner
const gate = (s, key, ms) => { const now = Date.now(), g = s.data.gate || (s.data.gate = {}); if (now - (g[key] || 0) < ms) return false; g[key] = now; return true; };
const clean = (v, n) => String(v || '').replace(/[<>&"'`\u0000-\u001f]/g, '').trim().slice(0, n);
// Bu odanın sahibi (kurucusu) artık odada değilse oda kimsenin kota sayısına girmez
const disown = (r, id) => { if (r.ownerId === id) { r.ownerId = null; r.ownerCid = ''; r.ownerIp = ''; } };
const code = () => { let c; do { c = Array.from({ length: 4 }, () => 'ABCDEFGHJKLMNPRSTUVYZ'[Math.random() * 21 | 0]).join(''); } while (rooms.has(c)); return c; };
const num = (v, lo, hi) => (typeof v === 'number' && isFinite(v)) ? Math.max(lo, Math.min(hi, v)) : 0;
const pub = r => ({ code: r.code, started: r.started, public: !!r.public, name: r.name || '', opts: r.opts, players: [...r.players.values()].map(p => ({ id: p.id, name: p.name, bird: p.bird, host: p.id === r.host, score: p.score, alive: p.alive, ready: !!p.ready })) });
const push = r => io.to(r.code).emit('room', pub(r));
function leave(s) {
  const r = rooms.get(s.data.room); s.data.room = null;
  if (!r) return;
  r.players.delete(s.id); s.leave(r.code); disown(r, s.id);
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
  const name = clean(d.name, 12) || 'Oyuncu';
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
  connsByIp.set(s.data.ip, (connsByIp.get(s.data.ip) || 0) + 1);
  s.emit('ver', 4);   // istemci sunucunun güncel olup olmadığını anlasın
  s.on('hb', () => { s.data.hb = Date.now(); });
  s.emit('online', online());   // yayın yalnızca 3 sn'lik zamanlayıcıda (her bağlantıda herkese yayın = O(n²) yük)
  s.on('vis', v => { s.data.vis = !!v; });
  s.on('t', (c, cb) => { if (typeof cb === 'function') cb(Date.now()); });   // saat eşitleme (istemci art arda ölçüm yapar; sınırlamıyoruz)   // saat eşitleme
  s.on('create', d => {
    d = (d && typeof d === 'object') ? d : {};
    const ip = s.data.ip, cid = String(d.cid || '').slice(0, 40), now = Date.now();
    // 1) Zaten bir odadaysa yeni oda kuramaz: önce odadan çıkmalı / odayı kapatmalı
    if (s.data.room && rooms.has(s.data.room)) return s.emit('err', 'Zaten bir odadasın. Yeni oda kurmak için önce odadan çık');
    // 2) Çok hızlı üst üste oda kurma engellenir
    if (now - (lastCreate.get(ip) || 0) < CREATE_COOLDOWN) return s.emit('err', 'Çok hızlı oda kuruyorsun, birkaç saniye bekle');
    // 3) Aynı oyuncunun (istemci kimliği) hâlâ açık bir odası varsa yenisini kuramaz
    if (cid) {
      for (const r0 of rooms.values()) {
        if (r0.ownerCid !== cid || !r0.ownerId) continue;
        const old = io.sockets.sockets.get(r0.ownerId);
        if (old && old.id !== s.id && now - (old.data.hb || 0) < 15000) return s.emit('err', 'Zaten açık bir odan var. Yeni oda kurmak için önce onu kapat');
        if (old && old.id !== s.id) { old.emit('err', 'Bağlantın koptuğu için odandan çıkarıldın'); leave(old); }   // kopmuş (donmuş) eski bağlantı: odayı serbest bırak
        else disown(r0, r0.ownerId);
      }
    }
    // 4) Aynı ağdan aynı anda en fazla MAX_ROOMS_PER_IP oda
    let own = 0; rooms.forEach(r0 => { if (r0.ownerIp === ip && r0.ownerId) own++; });
    if (own >= MAX_ROOMS_PER_IP) return s.emit('err', 'Bu ağdan en fazla ' + MAX_ROOMS_PER_IP + ' oda açılabilir. Önce açık odalarını kapat');
    if (rooms.size >= MAX_ROOMS) return s.emit('err', 'Sunucu şu an dolu, biraz sonra dene');
    const rname = clean(d.rname, 15);
    const r = { code: code(), uid: Math.random().toString(36).slice(2, 10), opts: { gap: 'normal', speed: 'normal', ev: true }, host: s.id, started: false, public: d.public === true || d.public === 1, name: rname, players: new Map(), slots: {}, claimed: new Set(), banned: new Set(), members: new Set(), ownerId: s.id, ownerCid: cid, ownerIp: ip };
    lastCreate.set(ip, now);
    rooms.set(r.code, r); join(s, r, d);
  });
  s.on('join', d => {
    d = (d && typeof d === 'object') ? d : {};
    if (!gate(s, 'join', 400)) return;
    if (s.data.room) leave(s);
    const r = rooms.get(String(d.code || '').trim().toUpperCase());
    if (!r) return s.emit('err', 'Oda bulunamadı');
    if (d.rid && d.rid !== r.uid) return s.emit('err', 'Kod yanlış, tekrar dene');   // listedeki kilitli odaya yanlış kod girildi
    join(s, r, d);
  });
  // Herkese açık odalar: henüz başlamamış ve dolu olmayan odalar listelenir
  s.on('list', cb => {
    if (typeof cb !== 'function' || !gate(s, 'list', 700)) return;
    const out = [];
    // Özel odalar da listelenir (kilitli), ama kodları ASLA gönderilmez; herkese açık odaların kodu gönderilir
    rooms.forEach(r => { const h = r.players.get(r.host); out.push({ code: r.public ? r.code : undefined, pub: !!r.public, n: r.players.size, host: h ? h.name : 'Oyuncu', started: r.started, name: r.name || '', rid: r.uid }); });
    const rank = x => (!x.started && x.n < MAX ? 0 : 1) * 2 + (x.pub ? 0 : 1);
    out.sort((a, b) => rank(a) - rank(b) || b.n - a.n);
    cb(out.slice(0, 40));
  });
  // "Son odaya dön" butonu için: oda hâlâ açık ve girilebilir mi?
  s.on('exists', (d, cb) => {
    if (typeof cb !== 'function' || !gate(s, 'exists', 500)) return;
    d = (d && typeof d === 'object') ? d : {};
    const r = rooms.get(String(d.code || '').trim().toUpperCase()), cid = String(d.cid || '').slice(0, 40);
    cb(!!r && !(cid && r.banned.has(cid)) && (!r.started || !!(cid && r.members.has(cid))) && r.players.size < MAX);
  });
  // Oda sahibi odayı herkese açık / özel yapabilir
  s.on('setpub', v => { const r = rooms.get(s.data.room); if (!r || r.host !== s.id) return; r.public = !!v; push(r); });
  // Oda ayarları (yalnızca oda sahibi, oyun başlamadan): boru aralığı, hız, olaylar
  s.on('opts', d => {
    const r = rooms.get(s.data.room); if (!r || r.host !== s.id || r.started || !d || typeof d !== 'object') return;
    const k = d.k, v = d.v;
    if (k === 'gap' && ['easy', 'normal', 'hard'].includes(v)) r.opts.gap = v;
    else if (k === 'speed' && ['slow', 'normal', 'fast'].includes(v)) r.opts.speed = v;
    else if (k === 'ev') r.opts.ev = (v === 'on' || v === true);
    else return;
    push(r);
  });
  // Hazır ol sistemi: oda sahibi dışındaki oyuncular hazır olduğunu bildirir
  s.on('ready', v => {
    const r = rooms.get(s.data.room), p = r && r.players.get(s.id);
    if (!p || r.started) return;
    p.ready = !!v; push(r);
  });
  s.on('start', () => {
    const r = rooms.get(s.data.room); if (!r || r.host !== s.id || r.started) return;
    if (r.players.size < 2) return s.emit('err', 'Oyunu başlatmak için en az 2 oyuncu gerekir');
    if (![...r.players.values()].every(p => p.ready)) return s.emit('err', 'Oda sahibi dahil herkes hazır olmadan başlatamazsın');
    r.started = true; r.claimed = new Set(); r.slots = {}; let i = 0;
    r.players.forEach(p => { p.alive = true; p.score = 0; p.dirty = false; p.last = Date.now() + 4000; r.slots[p.id] = i++; });
    // Herkes aynı sunucu saatinde başlar: 3 sn geri sayım + 0.5 sn tampon
    io.to(r.code).emit('start', { seed: Math.random() * 4294967296 >>> 0, startAt: Date.now() + 3500, slots: r.slots, n: i, opts: r.opts, events: r.opts.ev === false ? [] : makeEvents() }); push(r);
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
    if (!r || !p || !p.alive || !r.started || !d || !Number.isInteger(d.id) || d.id < 0 || d.id > 100000 || r.claimed.has(d.id) || r.claimed.size > 3000 || !gate(s, 'pu', 40)) return;
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
  s.on('disconnect', () => {
    leave(s);
    const n = (connsByIp.get(s.data.ip) || 1) - 1; if (n > 0) connsByIp.set(s.data.ip, n); else connsByIp.delete(s.data.ip);
  });
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
// Bellek temizliği: eski oda kurma kayıtları silinir
setInterval(() => { const now = Date.now(); lastCreate.forEach((t, ip) => { if (now - t > 60000) lastCreate.delete(ip); }); }, 60000);
console.log('MP sunucu hazır');
