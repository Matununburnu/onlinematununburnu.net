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
// glide: SÜZÜLME (düşüş hızı sınırlanır)
const EV_PAUSE = { low: 2.2, normal: 1, high: 0.4 };   // olaylar arası bekleme çarpanı (az / normal / sık)
function makeEvents(freq) {
  const mul = EV_PAUSE[freq] || 1;
  const ev = [], types = ['wind', 'rwind', 'storm', 'moon', 'heavy', 'grav', 'glide'];   // glide: SÜZÜLME
  const dur = { wind: () => 9000 + Math.random() * 3000, rwind: () => 12000 + Math.random() * 3000, storm: () => 10000 + Math.random() * 3000, moon: () => 10000 + Math.random() * 2000, heavy: () => 8000 + Math.random() * 2000, grav: () => 14000, glide: () => 10000 + Math.random() * 3000 };
  let t = (20000 + Math.random() * 8000) * Math.min(1.5, mul), last = '';
  while (t < 20 * 60 * 1000) {
    let type; do { type = types[Math.random() * types.length | 0]; } while (type === last);
    last = type;
    const d = Math.round(dur[type]());
    ev.push({ type, t: Math.round(t), d, dir: Math.random() < 0.5 ? -1 : 1 });
    t += d + (4000 + 12000 + Math.random() * 10000) * mul;   // olaylar çakışmaz (bekleme süresi sıklık ayarına göre değişir)
  }
  return ev;
}

const MAX = 12, MAX_ROOMS = 500, TICK = 25, rooms = new Map();   // oda başına en fazla oyuncu (istemciye 'cfg' ile bildirilir)
const BOSS_MS = 20000, BOSS_REST = 4000;   // boss süresi (istemcideki BOSS_DURATION ile aynı) ve iki boss arası en az bekleme
const BOSS_STEP = { low: 100, normal: 60, high: 30 };   // sonraki boss için gereken skor farkı (sıklık ayarı: az / normal / sık)
const STEP_MS = 1000 / 60, BOSS_SCORE = 50, BOSS_LEAD = 1500, SPEED_MUL = { slow: 0.7, normal: 1, fast: 1.7 };
const SILENT_MS = 4000;   // bu kadar süre konum paketi gelmeyen (interneti giden / donan) oyuncu elenmiş sayılır
// Sunucu tarafı skor doğrulaması: boru sıklığı oda ayarından ve oyuncu sayısından istemcideki formülle aynı hesaplanır;
// geçen süreye göre ulaşılabilecek en yüksek skor (2 sn tolerans + 3 boru payı) bulunur
function maxScore(r, now) {
  const dw = Math.round(3300 + 160 * (Math.max(1, r.n0 || 1) - 1)), base = Math.round(85 * Math.sqrt(dw / 1920));
  const iv = Math.max(20, Math.round(base / (SPEED_MUL[r.opts.speed] || 1)));
  const steps = Math.max(0, (now - r.startAt) / STEP_MS) + 120;
  return Math.floor(steps / (iv + 1)) + 3;
}
// --- Kötüye kullanım / aşırı yük koruması ---
const MAX_CONN_PER_IP = 40;        // aynı IP'den en fazla eşzamanlı bağlantı (okul / yurt gibi ortak IP'ler için 12 çok azdı)
const MAX_ROOMS_PER_IP = 3;        // aynı IP'nin aynı anda açık tutabileceği oda (okul / ev ağı payı bırakıldı)
const CREATE_COOLDOWN = 5000;      // iki oda kurma arası en az süre (ms)
const connsByIp = new Map(), lastCreate = new Map();
// İstemci IP'si. X-Forwarded-For başlığının SOL tarafı istemci tarafından yazılabilir (sahte IP ile limitler aşılabilirdi);
// yalnızca güvenilir proxy'nin (Render vb.) EKLEDİĞİ sağdaki değerlere güvenilir.
// TRUST_PROXY_HOPS: sunucunun önündeki güvenilir proxy sayısı (Render için 1 = varsayılan; proxy yoksa 0 yazın)
const net = require('net');
const TRUST_PROXY_HOPS = Math.max(0, parseInt(process.env.TRUST_PROXY_HOPS || '1', 10) || 0);
const normIp = v => { v = String(v || '').trim().replace(/^::ffff:/i, ''); return net.isIP(v) ? v : ''; };
const ipOf = s => {
  const h = s.handshake || {}, direct = normIp(h.address) || 'x';
  if (!TRUST_PROXY_HOPS) return direct;
  const xf = h.headers && h.headers['x-forwarded-for'];
  if (!xf) return direct;
  const parts = String(xf).split(',').map(x => x.trim());
  // sağdan TRUST_PROXY_HOPS. değer = en dıştaki güvenilir proxy'nin gördüğü gerçek istemci
  return normIp(parts[parts.length - TRUST_PROXY_HOPS]) || direct;
};
// Olay başına bekleme süresi (flood koruması): izin yoksa false döner
const gate = (s, key, ms) => { const now = Date.now(), g = s.data.gate || (s.data.gate = {}); if (now - (g[key] || 0) < ms) return false; g[key] = now; return true; };
const clean = (v, n) => String(v || '').replace(/[<>&"'`\u0000-\u001f]/g, '').trim().slice(0, n);
// Bu odanın sahibi (kurucusu) artık odada değilse oda kimsenin kota sayısına girmez
const disown = (r, id) => { if (r.ownerId === id) { r.ownerId = null; r.ownerCid = ''; r.ownerIp = ''; } };
// --- Oda sohbeti: son 40 mesaj odada tutulur (yeni giren görsün); sistem mesajları (katıldı / ayrıldı) aynı kanaldan gider
const cleanMsg = (v, n) => String(v || '').replace(/[\u0000-\u001f\u007f<>]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, n);
function addChat(r, m) { m.t = Date.now(); (r.chat || (r.chat = [])).push(m); if (r.chat.length > 40) r.chat.shift(); io.to(r.code).emit('chat', m); }

// Oda listesi (önbellekli): özel odaların kodu ASLA gönderilmez; herkese açık odaların kodu gönderilir
const listCache = { t: 0, data: null };
function roomList() {
  const now = Date.now();
  if (listCache.data && now - listCache.t < 250) return listCache.data;
  const out = [];
  rooms.forEach(r => { const h = r.players.get(r.host); out.push({ code: r.public ? r.code : undefined, pub: !!r.public, n: r.players.size, host: h ? h.name : 'Oyuncu', started: r.started, name: r.name || '', rid: r.uid }); });
  const rank = x => (!x.started && x.n < MAX ? 0 : 1) * 2 + (x.pub ? 0 : 1);
  out.sort((a, b) => rank(a) - rank(b) || b.n - a.n);
  listCache.t = now; listCache.data = out.slice(0, 40);
  return listCache.data;
}
// Oyuncunun profil özeti (istemci bildirir; yalnızca GÖSTERİM içindir, sayılar sınırlandırılır)
const cnum = (v, hi) => (typeof v === 'number' && isFinite(v)) ? Math.max(0, Math.min(hi, Math.round(v))) : 0;
function cleanProf(d) {
  d = (d && typeof d === 'object') ? d : {};
  return {
    lv: Math.max(1, cnum(d.lv, 999)), xp: cnum(d.xp, 1e9), rk: clean(d.rk, 24), rc: /^#[0-9a-fA-F]{3,8}$/.test(String(d.rc || '')) ? String(d.rc) : '',
    hs: cnum(d.hs, 1e7), g: cnum(d.g, 1e7), avg: cnum(d.avg, 1e7), pp: cnum(d.pp, 1e9), bs: cnum(d.bs, 1e6), ch: cnum(d.ch, 999), chT: cnum(d.chT, 999),
    pm: cnum(d.pm, 1e11), st: cnum(d.st, 99999), bst: cnum(d.bst, 99999), dth: cnum(d.dth, 1e8), fav: cnum(d.fav, 999)
  };
}
const code = () => { let c; do { c = Array.from({ length: 4 }, () => 'ABCDEFGHJKLMNPRSTUVYZ'[Math.random() * 21 | 0]).join(''); } while (rooms.has(c)); return c; };
const num = (v, lo, hi) => (typeof v === 'number' && isFinite(v)) ? Math.max(lo, Math.min(hi, v)) : 0;
const pub = r => ({ code: r.code, started: r.started, public: !!r.public, name: r.name || '', opts: r.opts, players: [...r.players.values()].map(p => ({ id: p.id, name: p.name, bird: p.bird, host: p.id === r.host, score: p.score, alive: p.alive, ready: !!p.ready })) });
const push = r => io.to(r.code).emit('room', pub(r));
const connected = r => [...r.players.values()].filter(p => !p.gone);
function leave(s, why) {
  const r = rooms.get(s.data.room); s.data.room = null;
  if (!r) return;
  const p = r.players.get(s.id);
  s.leave(r.code); disown(r, s.id);
  if (r.started && p) {
    // Oyun sürerken çıkan / bağlantısı kopan oyuncu ÖLMÜŞ sayılır: haritadan kalkar, tur bitene kadar listede "elendi" görünür, geri giremez
    p.gone = true; p.dirty = false;
    if (p.alive) { p.alive = false; io.to(r.code).emit('gone', { id: p.id, score: p.score }); }
  } else r.players.delete(s.id);
  if (!connected(r).length) { rooms.delete(r.code); return; }
  if (p) addChat(r, { s: 1, k: 'l', x: p.name + (why === 'kick' ? ' odadan atıldı' : ' odadan ayrıldı') });
  if (r.host === s.id || !r.players.has(r.host) || r.players.get(r.host).gone) r.host = connected(r)[0].id;
  push(r); check(r);
}
function check(r) {
  if (r.started && ![...r.players.values()].some(p => p.alive)) {
    r.started = false;
    r.players.forEach(p => { p.ready = false; });   // tur bitti: sonraki oyun için herkes yeniden hazır olmalı
    io.to(r.code).emit('over', pub(r).players.sort((a, b) => b.score - a.score));
    r.players.forEach((p, id) => { if (p.gone) r.players.delete(id); });   // turda kopan oyuncular listeden silinir
    if (!r.players.has(r.host)) { const c = connected(r)[0]; if (c) r.host = c.id; }
    if (!r.players.size) { rooms.delete(r.code); return; }
    push(r);
  }
}
// Skor doğrulama: skor asla düşmez, geçen süreyle ulaşılabilecek değeri aşamaz. Aşan değer kırpılır; art arda aşanlar elenir
function okScore(r, p, sc, now, s) {
  const mx = maxScore(r, now);
  if (sc > mx) {
    p.strikes++; sc = mx;
    if (s && p.strikes >= 30 && p.alive) {   // ~1 sn boyunca imkansız skor gönderdi: hileli istemci
      p.alive = false; p.score = Math.max(p.score, sc);
      io.to(r.code).emit('gone', { id: p.id, score: p.score }); s.emit('err', 'Geçersiz skor tespit edildi, elendin'); push(r); check(r);
      return false;
    }
  }
  if (sc > p.score) p.score = sc;
  return true;
}
function join(s, r, d) {
  const cid = String(d.cid || '').slice(0, 40);
  if (cid && r.banned.has(cid)) return s.emit('err', 'Bu odadan atıldın, tekrar giremezsin');
  if (r.started) return s.emit('err', 'Oyun başladı');   // oyun sürerken kimse (bağlantısı kopan eski oyuncu dahil) giremez
  if (r.players.size >= MAX) return s.emit('err', 'Oda dolu (' + MAX + '/' + MAX + ')');
  const name = clean(d.name, 12) || 'Oyuncu';
  if (cid) r.members.add(cid);
  r.players.set(s.id, { id: s.id, cid, name, bird: num(d.bird, 0, 999) | 0, score: 0, alive: !r.started, ready: false, last: Date.now(), y: 0, vy: 0, st: 0, dirty: false, gone: false, strikes: 0 });
  s.join(r.code); s.data.room = r.code; push(r);
  s.emit('chath', r.chat || []);   // son mesajlar
  if (r.players.size > 1) addChat(r, { s: 1, k: 'j', x: name + ' odaya katıldı' });
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
  s.emit('ver', 7);   // istemci sunucunun güncel olup olmadığını anlasın
  s.emit('cfg', { max: MAX });   // oda kapasitesi (istemci göstergeleri buna göre)
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
    const r = { code: code(), uid: Math.random().toString(36).slice(2, 10), opts: { gap: 'normal', speed: 'normal', ev: true, evf: 'normal', boss: true, bossf: 'normal' }, host: s.id, started: false, public: d.public === true || d.public === 1, name: rname, players: new Map(), slots: {}, claimed: new Set(), banned: new Set(), members: new Set(), ownerId: s.id, ownerCid: cid, ownerIp: ip };
    lastCreate.set(ip, now);
    rooms.set(r.code, r); join(s, r, d);
  });
  s.on('join', d => {
    d = (d && typeof d === 'object') ? d : {};
    if (!gate(s, 'join', 250)) return;
    if (s.data.room) leave(s);
    const r = rooms.get(String(d.code || '').trim().toUpperCase());
    if (!r) return s.emit('err', 'Oda bulunamadı');
    if (d.rid && d.rid !== r.uid) return s.emit('err', 'Kod yanlış, tekrar dene');   // listedeki kilitli odaya yanlış kod girildi
    join(s, r, d);
  });
  // Herkese açık odalar: henüz başlamamış ve dolu olmayan odalar listelenir
  // Liste 250 ms önbelleğe alınır (çok kişi aynı anda yenilerken sunucu yükü artmasın); sınıra takılan istek de son listeyle yanıtlanır
  s.on('list', cb => {
    if (typeof cb !== 'function') return;
    if (!gate(s, 'list', 250)) return cb(listCache.data || []);
    cb(roomList());
  });
  // "Son odaya dön" butonu için: oda hâlâ açık ve girilebilir mi?
  s.on('exists', (d, cb) => {
    if (typeof cb !== 'function' || !gate(s, 'exists', 500)) return;
    d = (d && typeof d === 'object') ? d : {};
    const r = rooms.get(String(d.code || '').trim().toUpperCase()), cid = String(d.cid || '').slice(0, 40);
    cb(!!r && !(cid && r.banned.has(cid)) && !r.started && r.players.size < MAX);
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
    else if (k === 'evf' && ['low', 'normal', 'high'].includes(v)) r.opts.evf = v;
    else if (k === 'boss') r.opts.boss = (v === 'on' || v === true);   // boss olayı aç / kapat
    else if (k === 'bossf' && ['low', 'normal', 'high'].includes(v)) r.opts.bossf = v;   // boss sıklığı
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
    r.startAt = Date.now() + 3500;   // herkes aynı sunucu saatinde başlar: 3 sn geri sayım + 0.5 sn tampon
    r.bossNext = BOSS_SCORE;         // sonraki boss için gereken skor: ilk boss ASLA 50'den önce çıkmaz (sıklık ayarı yalnızca sonraki bosslara uygulanır)
    r.bossEnd = 0;                   // çalışan bossun bitiş zamanı (oyun saati, ms): bosslar üst üste binmez
    r.players.forEach(p => { p.alive = true; p.gone = false; p.strikes = 0; p.score = 0; p.dirty = false; p.q = []; p.last = Date.now() + 4000; r.slots[p.id] = i++; });
    r.n0 = i;
    io.to(r.code).emit('start', { seed: Math.random() * 4294967296 >>> 0, startAt: r.startAt, slots: r.slots, n: i, opts: r.opts, events: r.opts.ev === false ? [] : makeEvents(r.opts.evf) }); push(r);
  });
  // Konum paketi: yalnızca saklanır; odaya 40 Hz'lik TOPLU pakette ('S') gönderilir (oyuncu başına ayrı paket yok → ağ yükü ~10 kat azalır)
  s.on('s', a => {
    const r = rooms.get(s.data.room), p = r && r.players.get(s.id);
    if (!p || !p.alive || !r.started || !Array.isArray(a)) return;
    const now = Date.now();
    p.y = Math.round(num(a[0], -5000, 20000) * 10) / 10; p.vy = num(a[1], -2000, 2000) | 0; p.st = num(a[3], 0, 1e9) | 0; p.dirty = true; p.last = now;
    // Örnek kuyruğa da eklenir: yayın zamanlayıcısı ile istemci gönderim zamanlayıcısı birbirine denk gelmediğinde örnekler kaybolmaz / ezilmez (takılmanın ana nedeniydi)
    const q = p.q || (p.q = []); if (!q.length || p.st > q[q.length - 1][3]) { q.push([p.y, p.vy, 0, p.st]); if (q.length > 6) q.shift(); }
    if (!okScore(r, p, num(a[2], 0, 1e7) | 0, now, s)) return;
    if (p.q && p.q.length) p.q[p.q.length - 1][2] = p.score;
    // Herkesle aynı anda boss: odada ilk 50 skora ulaşan oyuncu boss'u herkes için tetikler (başlangıç zamanı oyun saatine göre)
    // Boss açıksa: odada ilk 50 skora ulaşan oyuncu ilk boss'u herkes için tetikler (50'den sonra boss DAİMA çıkar). Sonraki bosslar sıklık ayarına göre skor aralığıyla gelir
    if (r.opts.boss !== false && p.score >= Math.max(BOSS_SCORE, r.bossNext)) {
      const bt = Math.max(0, now - r.startAt) + BOSS_LEAD;
      if (bt >= r.bossEnd + BOSS_REST) {   // önceki boss bitmeden yenisi başlamaz
        r.bossEnd = bt + BOSS_MS; r.bossNext = p.score + (BOSS_STEP[r.opts.bossf] || BOSS_STEP.normal);
        io.to(r.code).emit('boss', { t: bt });
      }
    }
  });
  s.on('dead', sc => {
    const r = rooms.get(s.data.room), p = r && r.players.get(s.id);
    if (!p || !r.started || !p.alive) return;
    okScore(r, p, num(sc, 0, 1e7) | 0, Date.now(), null);   // ölüm skoru da doğrulanır (geçersizse mümkün olan en yüksek değere indirilir)
    p.alive = false;
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
    if (t) { t.emit('kicked'); leave(t, 'kick'); } else { r.players.delete(id); push(r); }
  });
  // Oyuncu kendi profil özetini bildirir (odadaki diğerleri üstüne tıklayınca görebilsin)
  s.on('prof', d => {
    const r = rooms.get(s.data.room), p = r && r.players.get(s.id);
    if (!p || !gate(s, 'prof', 1000)) return;
    p.prof = cleanProf(d);
  });
  // Odadaki bir oyuncunun profilini iste (yalnızca aynı odadakiler birbirinin profilini görebilir)
  s.on('getprof', (id, cb) => {
    if (typeof cb !== 'function') return;
    if (!gate(s, 'getprof', 150)) return cb(null);
    const r = rooms.get(s.data.room), p = r && typeof id === 'string' && r.players.get(id);
    if (!p || p.gone) return cb(null);
    cb({ id: p.id, name: p.name, bird: p.bird, host: p.id === r.host, ready: !!p.ready, prof: p.prof || null });
  });
  // Lobi liderliğini (oda sahipliği) başka bir oyuncuya ver: yalnızca mevcut lider, oyun başlamadan
  s.on('transfer', id => {
    const r = rooms.get(s.data.room); if (!r || r.host !== s.id || r.started || typeof id !== 'string' || id === s.id || !gate(s, 'transfer', 600)) return;
    const p = r.players.get(id), old = r.players.get(s.id); if (!p || p.gone) return;
    r.host = id;
    // Kota sahipliği de yeni lidere geçer (eski liderin başka oda kurması engellenmesin)
    r.ownerId = id; r.ownerCid = p.cid || ''; const t = io.sockets.sockets.get(id); r.ownerIp = t ? t.data.ip : '';
    addChat(r, { s: 1, k: 'j', x: (old ? old.name : 'Lider') + ' lobi liderliğini ' + p.name + ' oyuncusuna verdi' });
    if (t) t.emit('newhost');
    push(r);
  });
  s.on('chat', txt => {
    const r = rooms.get(s.data.room), p = r && r.players.get(s.id);
    if (!p || p.gone || typeof txt !== 'string' || !gate(s, 'chat', 700)) return;
    const x = cleanMsg(txt, 140); if (!x) return;
    addChat(r, { id: s.id, n: p.name, x, d: (r.started && !p.alive) ? 1 : 0 });   // d: ölmüş / izleyici
  });
  s.on('leave', () => leave(s));
  s.on('disconnect', () => {
    leave(s);
    const n = (connsByIp.get(s.data.ip) || 1) - 1; if (n > 0) connsByIp.set(s.data.ip, n); else connsByIp.delete(s.data.ip);
  });
});

// 40 Hz toplu konum yayını (kuyruktaki tüm örneklerle): [slot, y, vy, skor, oyun adımı, ...] (adım numarası sayesinde herkes konumu aynı oyun zamanında çizer)
setInterval(() => rooms.forEach(r => {
  if (!r.started) return;
  const out = [];
  r.players.forEach(p => { if (p.alive && p.dirty) { p.dirty = false; const sl = r.slots[p.id], q = p.q; if (sl !== undefined && q && q.length) { for (let i = 0; i < q.length; i++) out.push(sl, q[i][0], q[i][1], q[i][2], q[i][3]); } if (q) q.length = 0; } });
  if (out.length) io.to(r.code).volatile.emit('P', out);
}), TICK);

// SILENT_MS (4 sn) hiç veri göndermeyen (interneti giden / donmuş) canlı oyuncuyu elenmiş say
setInterval(() => rooms.forEach(r => {
  if (!r.started) return;
  const now = Date.now(), dead = [];
  r.players.forEach(p => { if (p.alive && now - p.last > SILENT_MS) { p.alive = false; dead.push(p); } });
  if (!dead.length) return;
  dead.forEach(p => io.to(r.code).emit('gone', { id: p.id, score: p.score }));
  push(r); check(r);
}), 1000);
// Bellek temizliği: eski oda kurma kayıtları silinir
setInterval(() => { const now = Date.now(); lastCreate.forEach((t, ip) => { if (now - t > 60000) lastCreate.delete(ip); }); }, 60000);
console.log('MP sunucu hazır');
