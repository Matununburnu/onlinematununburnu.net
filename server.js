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
  transports: ['websocket'],                 // polling → websocket yükseltmesi gecikme/sıçrama yaratır; istemci zaten yalnızca websocket kullanır
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
const EV_HORIZON = 3 * 60 * 60 * 1000;   // olay takvimi 3 saat boyunca üretilir (çok uzun turlarda da olaylar sürsün)
const EV_TYPES = ['wind', 'rwind', 'storm', 'moon', 'heavy', 'grav', 'glide'];   // glide: SÜZÜLME
// Olay seçici: torba (shuffle-bag) + ailelerin art arda gelmemesi. Her tür sırayla bir kez çıkar, aynı tür ya da benzer olay (rüzgâr/yerçekimi ailesi) art arda gelmez
const EV_FAM = { wind: 'w', rwind: 'w', storm: 's', moon: 'g', heavy: 'g', grav: 'g', glide: 'g' };
function makeEvPicker(types) {
  let bag = []; const hist = [], n = types.length;
  return () => {
    if (!bag.length) { bag = types.slice(); for (let i = bag.length - 1; i > 0; i--) { const j = Math.random() * (i + 1) | 0; const x = bag[i]; bag[i] = bag[j]; bag[j] = x; } }
    const last = hist[hist.length - 1], rec = hist.slice(-Math.min(2, n - 1));
    const okF = t => !rec.includes(t) && (n < 3 || EV_FAM[t] !== EV_FAM[last]);
    let t = bag.find(okF);
    if (t === undefined) { const alt = types.filter(okF); if (alt.length) t = alt[Math.random() * alt.length | 0]; }   // torbada uygun kalmadıysa tüm türlerden seç
    if (t === undefined) t = bag.find(x => !rec.includes(x));
    if (t === undefined) t = bag.find(x => x !== last);
    if (t === undefined) t = bag[0];
    const bi = bag.indexOf(t); if (bi >= 0) bag.splice(bi, 1); hist.push(t); if (hist.length > 4) hist.shift(); return t;
  };
}
function makeEvents(freq, off) {
  const mul = EV_PAUSE[freq] || 1;
  const ev = [], types = EV_TYPES.filter(t => !(Array.isArray(off) && off.includes(t)));   // oda sahibinin kapattığı olaylar çıkmaz
  if (!types.length) return ev;
  const dur = { wind: () => 9000 + Math.random() * 3000, rwind: () => 12000 + Math.random() * 3000, storm: () => 10000 + Math.random() * 3000, moon: () => 10000 + Math.random() * 2000, heavy: () => 8000 + Math.random() * 2000, grav: () => 14000, glide: () => 10000 + Math.random() * 3000 };
  let t = (20000 + Math.random() * 8000) * Math.min(1.5, mul), last = '', pick = makeEvPicker(types);
  while (t < EV_HORIZON) {
    const type = pick(); last = type;
    const d = Math.round(dur[type]());
    ev.push({ type, t: Math.round(t), d, dir: Math.random() < 0.5 ? -1 : 1 });
    t += d + (4000 + 12000 + Math.random() * 10000) * mul;   // olaylar çakışmaz (bekleme süresi sıklık ayarına göre değişir)
  }
  return ev;
}

const MAX_SPECS = 30;   // oda başına en fazla canlı izleyici
const MAX = 12, MIN_PLAYERS = 2, MAX_ROOMS = 500, TICK = 33, rooms = new Map();   // oda başına en fazla oyuncu (istemciye 'cfg' ile bildirilir)
const BOSS_MS = 20000, BOSS_REST = 4000;   // boss süresi (istemcideki BOSS_DURATION ile aynı) ve iki boss arası en az bekleme
const BOSS_STEP = { low: 100, normal: 60, high: 30 };   // sonraki boss için gereken skor farkı (sıklık ayarı: az / normal / sık)
const STEP_MS = 1000 / 60, BOSS_SCORE = 50, BOSS_LEAD = 1500, SPEED_MUL = { slow: 0.7, normal: 1, fast: 1.7 };
const SILENT_MS = 4000;   // bu kadar süre konum paketi gelmeyen (interneti giden / donan) oyuncu elenmiş sayılır
// Sunucu tarafı skor doğrulaması: boru sıklığı oda ayarından ve oyuncu sayısından istemcideki formülle aynı hesaplanır;
// geçen süreye göre ulaşılabilecek en yüksek skor (2 sn tolerans + 3 boru payı) bulunur
// Yatay doğuş konumları: oyuncular aynı hizada başlamaz. Oyuncu sayısına göre birkaç "sütun" (geri → ileri) oluşturulur ve slotlara rastgele dağıtılır.
// Daha ileride duran kuş borulara daha erken ulaşır; bu fark skor tavanına eklenir (öndeki oyuncu hileci sanılmasın)
const LANE_BASE = 190, LANE_PIPE_SPEED = 13;   // istemcideki en geri kuş x'i ve boru hızı (px / adım, hız çarpanı 1)
function makeLanes(n) {
  const dw = Math.round(3300 + 160 * (n - 1)), C = Math.max(1, Math.min(n, 6));
  const span = C > 1 ? Math.min(330 * (C - 1), dw * 0.4 - LANE_BASE) : 0, G = C > 1 ? span / (C - 1) : 0;
  const xs = [];
  for (let j = 0; j < n; j++) {
    const col = j % C; let x = LANE_BASE + col * G;
    if (col > 0) x += (Math.random() - 0.5) * G * 0.3;   // ızgara gibi durmasın
    xs.push(Math.round(Math.max(LANE_BASE, x)));
  }
  for (let i = xs.length - 1; i > 0; i--) { const k = Math.random() * (i + 1) | 0; const t = xs[i]; xs[i] = xs[k]; xs[k] = t; }
  return xs;
}
function maxScore(r, now, p) {
  const dw = Math.round(3300 + 160 * (Math.max(1, r.n0 || 1) - 1)), base = Math.round(85 * Math.sqrt(dw / 1920));
  const iv = Math.max(20, Math.round(base / (SPEED_MUL[r.opts.speed] || 1)));
  const steps = Math.max(0, (now - r.startAt) / STEP_MS) + 120 + ((p && p.lead) || 0);
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
  rooms.forEach(r => { const h = r.players.get(r.host); out.push({ code: r.public ? r.code : undefined, pub: !!r.public, n: r.players.size, mx: r.max, host: h ? h.name : 'Oyuncu', started: r.started, name: r.name || '', rid: r.uid, sp: r.specs.size }); });
  const rank = x => (!x.started && x.n < x.mx ? 0 : 1) * 2 + (x.pub ? 0 : 1);
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
const cleanCol = v => /^#[0-9a-fA-F]{6}$/.test(String(v || '')) ? String(v).toLowerCase() : '';   // isim rengi (#rrggbb) veya boş = varsayılan
const code = () => { let c; do { c = Array.from({ length: 4 }, () => 'ABCDEFGHJKLMNPRSTUVYZ'[Math.random() * 21 | 0]).join(''); } while (rooms.has(c)); return c; };
const num = (v, lo, hi) => (typeof v === 'number' && isFinite(v)) ? Math.max(lo, Math.min(hi, v)) : 0;
const pub = r => ({ code: r.code, started: r.started, public: !!r.public, max: r.max, name: r.name || '', opts: r.opts, sp: r.specs ? r.specs.size : 0, players: [...r.players.values()].map(p => ({ id: p.id, name: p.name, bird: p.bird, host: p.id === r.host, score: p.score, alive: p.alive, ready: !!p.ready, gone: !!p.gone, vr: !!p.vr, col: p.col || '' })) });
// Anlık oda listesi: oda sayısı/oyuncu sayısı/durum değişince lobi listesini açık olan herkese hemen (80 ms toplulaştırılarak) gönderilir
let _listT = 0;
function listDirty() {
  listCache.data = null;
  if (_listT) return;
  _listT = setTimeout(() => { _listT = 0; io.to('lobbyWatch').emit('rl', roomList()); }, 80);
}
// Bekleyen konum örneklerini odaya tek toplu pakette gönderir. Ölüm / çıkış öncesi da çağrılır: son örnekler kaybolmaz
function flushRoom(r, reliable) {
  const out = [];
  r.players.forEach(p => {
    const q = p.q; if (!q || !q.length) return;
    const sl = r.slots[p.id];
    if (sl !== undefined) for (let i = 0; i < q.length; i++) out.push(sl, q[i][0], q[i][1], q[i][2], q[i][3], q[i][4] | 0);
    q.length = 0; p.dirty = false;
  });
  if (out.length) (reliable ? io.to(r.code) : io.to(r.code).volatile).emit('P6', out);
}
// Ölüm bildirimi: önce o oyuncunun son örnekleri (güvenilir) gider, sonra 'gone' (ölüm adımı + son konum ile) → herkes düşüşü aynı oyun adımında başlatır
function goneOf(r, p) {
  flushRoom(r, true);
  const m = { id: p.id, score: p.score };
  if (p.st >= 0) { m.st = p.st; m.y = p.y; m.vy = p.vy; }
  io.to(r.code).emit('gone', m);
}
// Konum örneklerini doğrula + kuyruğa ekle. a = düz dizi: [y, vy*10, skor, adım, zıplama sayacı] × n. false → oyuncu elendi
function ingest(r, p, s, a, now) {
  const n = Math.min((a.length / 5) | 0, 32), maxSt = Math.max(0, (now - r.startAt) / STEP_MS) + 600;   // gelecekten (10 sn ötesi) adım numarası = sahte paket
  const q = p.q || (p.q = []), added = [];
  let lastSc = 0;
  for (let i = 0; i < n; i++) {
    const o = i * 5, st = num(a[o + 3], 0, 1e9) | 0;
    if (st > maxSt || st <= p.st) continue;   // eski / yinelenen / sahte adım
    const y = Math.round(num(a[o], -5000, 20000) * 10) / 10, vy = num(a[o + 1], -2000, 2000) | 0, jc = num(a[o + 4], 0, 1e7) | 0, sc = num(a[o + 2], 0, 1e7) | 0;
    p.y = y; p.vy = vy; p.st = st; p.jc = jc; p.dirty = true; p.last = now;
    const e = [y, vy, 0, st, jc, sc]; q.push(e); added.push(e); lastSc = sc;
  }
  while (q.length > 24) q.shift();
  if (!added.length) return true;
  if (!okScore(r, p, lastSc, now, s)) return false;
  for (const e of added) e[2] = Math.min(e[5], p.score);
  return true;
}
const push = r => { io.to(r.code).emit('room', pub(r)); listDirty(); };
const connected = r => [...r.players.values()].filter(p => !p.gone);
// Tur bitince (ya da oda kapanınca) canlı izleyiciler odadan çıkarılır; istemci onları menüye döndürür
function endSpecs(r, res) {
  if (!r.specs || !r.specs.size) return;
  const ids = [...r.specs.keys()]; r.specs.clear();
  const w = res && res[0] ? { w: res[0].name, sc: res[0].score | 0 } : null;
  ids.forEach(id => { const t = io.sockets.sockets.get(id); if (!t) return; t.leave(r.code); t.data.room = null; t.emit('specend', w); });
}
function leave(s, why) {
  const r = rooms.get(s.data.room); s.data.room = null;
  if (!r) return;
  if (r.specs.has(s.id)) { r.specs.delete(s.id); s.leave(r.code); push(r); return; }   // canlı izleyici ayrıldı: oyuncu işlemleri yapılmaz
  const p = r.players.get(s.id);
  s.leave(r.code); disown(r, s.id);
  if (r.started && p) {
    // Oyun sürerken çıkan / bağlantısı kopan oyuncu ÖLMÜŞ sayılır: haritadan kalkar, tur bitene kadar listede "elendi" görünür, geri giremez
    p.gone = true; p.dirty = false;
    if (p.alive) { p.alive = false; goneOf(r, p); }
  } else r.players.delete(s.id);
  if (!connected(r).length) { endSpecs(r, null); rooms.delete(r.code); listDirty(); return; }
  if (p) addChat(r, { s: 1, k: 'l', x: p.name + (why === 'kick' ? ' odadan atıldı' : ' odadan ayrıldı') });
  if (r.host === s.id || !r.players.has(r.host) || r.players.get(r.host).gone) r.host = connected(r)[0].id;
  push(r); check(r);
}
function check(r) {
  if (r.started && ![...r.players.values()].some(p => p.alive)) {
    r.started = false;
    r.players.forEach(p => { p.ready = false; p.vr = false; });   // tur bitti: sonraki oyun için herkes yeniden hazır olmalı (vr: sonuç ekranını görüntüleme durumu istemciler bildirince açılır)
    const res = pub(r).players.sort((a, b) => b.score - a.score);
    endSpecs(r, res);   // izleyiciler 'over' yerine kendi 'specend' mesajını alır
    io.to(r.code).emit('over', res);
    r.players.forEach((p, id) => { if (p.gone) r.players.delete(id); });   // turda kopan oyuncular listeden silinir
    if (!r.players.has(r.host)) { const c = connected(r)[0]; if (c) r.host = c.id; }
    if (!r.players.size) { rooms.delete(r.code); listDirty(); return; }
    push(r);
  }
}
// Skor doğrulama: skor asla düşmez, geçen süreyle ulaşılabilecek değeri aşamaz. Aşan değer kırpılır; art arda aşanlar elenir
function okScore(r, p, sc, now, s) {
  const mx = maxScore(r, now, p);
  if (sc > mx) {
    p.strikes++; sc = mx;
    if (s && p.strikes >= 30 && p.alive) {   // ~1 sn boyunca imkansız skor gönderdi: hileli istemci
      p.alive = false; p.score = Math.max(p.score, sc);
      goneOf(r, p); s.emit('err', 'Geçersiz skor tespit edildi, elendin'); push(r); check(r);
      return false;
    }
  }
  if (sc > p.score) p.score = sc;
  return true;
}
// Aynı kullanıcı (istemci kimliği) başka bir sekmeden ZATEN bir odadaysa ikinci kez oda kuramaz / odaya giremez.
// true döner ve hata gönderir → çağıran işlem durdurulur. Kopmuş (donmuş) eski bağlantı varsa onu odadan çıkarıp yola devam eder.
function otherTabInRoom(s, cid) {
  if (!cid) { s.emit('err', 'Geçersiz istemci, sayfayı yenileyip tekrar dene'); return true; }
  const now = Date.now(), stale = [];
  for (const r0 of rooms.values()) {
    for (const p of r0.players.values()) {
      if (p.cid !== cid || p.id === s.id || p.gone) continue;
      const old = io.sockets.sockets.get(p.id);
      if (!old) continue;
      // Eskiden burada son 'hb' paketinin 15 sn içinde gelip gelmediğine bakılıyordu; arka plana alınmış sekmelerde
      // tarayıcı setInterval'ı yavaşlattığı için hb gecikebiliyor ve hâlâ bağlı olan sekme yanlışlıkla "koptu" sayılıp
      // atılıyordu (yeni sekme de bu yüzden aynı odaya girebiliyordu). old.connected socket.io'nun KENDİ tuttuğu gerçek
      // bağlantı durumudur, istemci zamanlayıcısından etkilenmez: bağlantı gerçekten açıksa her zaman doğru sonucu verir.
      if (old.connected) { s.emit('err', 'Bu hesapla zaten başka bir sekmede odadasın. Önce oradan çık, sonra tekrar dene'); return true; }
      stale.push(old);
    }
    // İzleyiciler de aynı kontrole dahil: aksi halde biri bir odayı izlerken başka sekmeden
    // ikinci bir oda kurabilir / oyuncu olarak katılabilirdi (tek sekme kuralı atlatılırdı)
    for (const p of r0.specs.values()) {
      if (p.cid !== cid || p.id === s.id) continue;
      const old = io.sockets.sockets.get(p.id);
      if (!old) continue;
      if (old.connected) { s.emit('err', 'Bu hesapla zaten başka bir sekmede odadasın. Önce oradan çık, sonra tekrar dene'); return true; }
      stale.push(old);
    }
  }
  stale.forEach(old => { old.emit('err', 'Bağlantın koptuğu için odandan çıkarıldın'); leave(old); });
  return false;
}
function bossCheck(r, p, now) {
  // Boss açıksa: odada ilk 50 skora ulaşan oyuncu ilk boss'u herkes için tetikler (50'den sonra boss DAİMA çıkar). Sonraki bosslar sıklık ayarına göre skor aralığıyla gelir
  if (r.opts.boss !== false && p.score >= Math.max(BOSS_SCORE, r.bossNext)) {
    const bt = Math.max(0, now - r.startAt) + BOSS_LEAD;
    if (bt >= r.bossEnd + BOSS_REST) {   // önceki boss bitmeden yenisi başlamaz
      r.bossEnd = bt + BOSS_MS; r.lastBoss = bt; r.bossNext = p.score + (BOSS_STEP[r.opts.bossf] || BOSS_STEP.normal);
      io.to(r.code).emit('boss', { t: bt });
    }
  }
}
function join(s, r, d) {
  const cid = String(d.cid || '').slice(0, 40);
  if (otherTabInRoom(s, cid)) return;   // aynı kullanıcı yan sekmeden ikinci kez giremez
  if (cid && r.banned.has(cid)) return s.emit('err', 'Bu odadan atıldın, tekrar giremezsin');
  if (r.started) return s.emit('err', 'Oyun başladı');   // oyun sürerken kimse (bağlantısı kopan eski oyuncu dahil) giremez
  if (r.players.size >= r.max) return s.emit('err', 'Oda dolu (' + r.max + '/' + r.max + ')');
  const name = clean(d.name, 15) || 'Oyuncu';
  if (cid) r.members.add(cid);
  r.players.set(s.id, { id: s.id, cid, name, col: cleanCol(d.col), bird: num(d.bird, 0, 999) | 0, score: 0, alive: !r.started, ready: false, last: Date.now(), y: 0, vy: 0, st: -1, dirty: false, gone: false, strikes: 0 });
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
  s.emit('ver', 13);   // istemci sunucunun güncel olup olmadığını anlasın
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
    if (otherTabInRoom(s, cid)) return;   // başka sekmede zaten odadaysa oda kuramaz (boş oda da kalmasın diye oda oluşturulmadan önce kontrol)
    // 2) Çok hızlı üst üste oda kurma engellenir
    if (now - (lastCreate.get(ip) || 0) < CREATE_COOLDOWN) return s.emit('err', 'Çok hızlı oda kuruyorsun, birkaç saniye bekle');
    // 3) Aynı oyuncunun (istemci kimliği) hâlâ açık bir odası varsa yenisini kuramaz
    if (cid) {
      for (const r0 of rooms.values()) {
        if (r0.ownerCid !== cid || !r0.ownerId) continue;
        const old = io.sockets.sockets.get(r0.ownerId);
        if (old && old.id !== s.id && old.connected) return s.emit('err', 'Zaten açık bir odan var. Yeni oda kurmak için önce onu kapat');
        if (old && old.id !== s.id) { old.emit('err', 'Bağlantın koptuğu için odandan çıkarıldın'); leave(old); }   // kopmuş (donmuş) eski bağlantı: odayı serbest bırak
        else disown(r0, r0.ownerId);
      }
    }
    // 4) Aynı ağdan aynı anda en fazla MAX_ROOMS_PER_IP oda
    let own = 0; rooms.forEach(r0 => { if (r0.ownerIp === ip && r0.ownerId) own++; });
    if (own >= MAX_ROOMS_PER_IP) return s.emit('err', 'Bu ağdan en fazla ' + MAX_ROOMS_PER_IP + ' oda açılabilir. Önce açık odalarını kapat');
    if (rooms.size >= MAX_ROOMS) return s.emit('err', 'Sunucu şu an dolu, biraz sonra dene');
    const rname = clean(d.rname, 30);
    const rmax = Math.max(MIN_PLAYERS, Math.min(MAX, (parseInt(d.max, 10) || MAX)));   // lobi lideri seçer (2–12); dokunmazsa 12
    const r = { code: code(), uid: Math.random().toString(36).slice(2, 10), opts: { gap: 'normal', speed: 'normal', ev: true, evf: 'normal', off: [], boss: true, bossf: 'normal', video: true, map: (/^[0-5]$/.test(String(d.map)) ? parseInt(d.map, 10) : 0) }, host: s.id, started: false, max: rmax, public: d.public === true || d.public === 1, name: rname, players: new Map(), specs: new Map(), slots: {}, claimed: new Set(), banned: new Set(), members: new Set(), ownerId: s.id, ownerCid: cid, ownerIp: ip };
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
  // Lobi listesi açıkken canlı güncelleme: 'watch' 1 → dinlemeye başla, 0 → bırak (bağlantı kopunca otomatik düşer)
  // Bekleme odasında karakter değiştirme (yalnızca oyun başlamadan): herkesin listesinde avatar anında güncellenir
  s.on('bird', v => { const r = rooms.get(s.data.room), p = r && r.players.get(s.id); if (!p || r.started || !gate(s, 'bird', 200)) return; p.bird = num(v, 0, 999) | 0; push(r); });
  // Oyuncu adını değiştirince odadaki herkesin listesinde / skor tablosunda anında güncellenir
  s.on('rename', v => {
    const r = rooms.get(s.data.room), p = r && (r.players.get(s.id) || r.specs.get(s.id));
    if (!p || p.gone || !gate(s, 'rename', 400)) return;
    const n = clean(v, 15);
    if (!n || !/^[a-zA-Z0-9çÇğĞıİöÖşŞüÜ]+$/.test(n) || n === p.name) return;
    p.name = n; push(r);
  });
  // Oyuncunun isim rengi: sohbette ve oyuncu listesinde herkese bu renkte görünür (boş = varsayılan renk)
  s.on('color', v => {
    const r = rooms.get(s.data.room), p = r && r.players.get(s.id);
    if (!p || p.gone || !gate(s, 'color', 15)) return;
    const c = cleanCol(v); if (c === (p.col || '')) return;
    p.col = c;   // renk hemen kaydedilir (sohbet mesajları anında yeni renkle gider); oda yayını kısa süre toplulaştırılır
    clearTimeout(s.data.cpT); s.data.cpT = setTimeout(() => { const rr = rooms.get(s.data.room); if (rr) push(rr); }, 80);
  });
  s.on('watch', on => { if (on) s.join('lobbyWatch'); else s.leave('lobbyWatch'); });
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
    cb(!!r && !(cid && r.banned.has(cid)) && !r.started && r.players.size < r.max);
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
    else if (k === 'evt' && EV_TYPES.includes(v)) { const off = Array.isArray(r.opts.off) ? r.opts.off : []; r.opts.off = off.includes(v) ? off.filter(x => x !== v) : off.concat(v); }   // tek tek hava olayı aç / kapat
    else if (k === 'boss') r.opts.boss = (v === 'on' || v === true);   // boss olayı aç / kapat
    else if (k === 'bossf' && ['low', 'normal', 'high'].includes(v)) r.opts.bossf = v;   // boss sıklığı
    else if (k === 'video') r.opts.video = (v === 'on' || v === true);   // videolar çıksın / çıkmasın
    else if (k === 'max') { const n = parseInt(v, 10); if (!(n >= MIN_PLAYERS && n <= MAX) || n < r.players.size) return push(r); r.max = n; }   // geçersizse istemci gerçek değerle yeniden eşitlenir   // oyuncu sayısı (oda kurulduktan sonra, mevcut oyuncu sayısının altına inemez)
    else if (k === 'map' && /^[0-5]$/.test(String(v))) r.opts.map = parseInt(v, 10);   // harita (arka plan) 0-5
    else return push(r);
    push(r);
  });
  // Hazır ol sistemi: oda sahibi dışındaki oyuncular hazır olduğunu bildirir
  // Sonuç ekranını görüntüleme durumu: oyuncu o an SONUÇLAR ekranındaysa true (oda ekranına geçince / kapatınca false)
  s.on('vr', v => {
    const r = rooms.get(s.data.room), p = r && r.players.get(s.id);
    if (!p || r.started || !gate(s, 'vr', 150)) return;
    v = !!v; if (p.vr === v) return;
    p.vr = v; push(r);
  });
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
    r.lanes = makeLanes(r.players.size);
    r.players.forEach(p => { p.vr = false; p.alive = true; p.gone = false; p.strikes = 0; p.score = 0; p.dirty = false; p.jc = 0; p.q = []; p.st = -1; p.last = Date.now() + 4000; const sl = i++; r.slots[p.id] = sl; p.lead = Math.max(0, r.lanes[sl] - LANE_BASE) / (LANE_PIPE_SPEED * (SPEED_MUL[r.opts.speed] || 1)); });
    r.n0 = i;
    r.lastBoss = undefined;
    r.startMsg = { seed: Math.random() * 4294967296 >>> 0, startAt: r.startAt, slots: r.slots, lx: r.lanes, n: i, opts: r.opts, events: r.opts.ev === false ? [] : makeEvents(r.opts.evf, r.opts.off) };   // sonradan giren izleyiciye de aynısı gönderilir
    io.to(r.code).emit('start', r.startMsg); push(r);
  });
  // Konum paketi: yalnızca saklanır; odaya 40 Hz'lik TOPLU pakette ('S') gönderilir (oyuncu başına ayrı paket yok → ağ yükü ~10 kat azalır)
  s.on('s', a => {   // eski tek örnekli paket (geri sayım sırasındaki nabız + eski istemciler)
    const r = rooms.get(s.data.room), p = r && r.players.get(s.id);
    if (!p || !p.alive || !r.started || !Array.isArray(a) || a.length < 5) return;
    const now = Date.now();
    if (!ingest(r, p, s, [a[0], a[1], a[2], a[3], a[4]], now)) return;
    p.last = now; bossCheck(r, p, now);
  });
  // Toplu konum paketi (~30 Hz): her oyun adımının örneği ayrı ayrı gelir, kare hızı / zamanlayıcı titreşiminden etkilenmez
  s.on('b', a => {
    const r = rooms.get(s.data.room), p = r && r.players.get(s.id);
    if (!p || !p.alive || !r.started || !Array.isArray(a) || a.length < 5 || a.length > 5 * 64) return;
    const now = Date.now();
    if (!ingest(r, p, s, a, now)) return;
    p.last = now; bossCheck(r, p, now);
  });
  s.on('dead', (sc, y, vy, st) => {
    const r = rooms.get(s.data.room), p = r && r.players.get(s.id);
    if (!p || !r.started || !p.alive) return;
    const now = Date.now();
    // Ölüm anındaki son konum + oyun adımı da kuyruğa girer: herkes düşüşü aynı adımda aynı yerden başlatır
    if (typeof y === 'number' && typeof st === 'number') ingest(r, p, null, [y, vy, sc, st, p.jc], now);
    okScore(r, p, num(sc, 0, 1e7) | 0, now, null);   // ölüm skoru da doğrulanır (geçersizse mümkün olan en yüksek değere indirilir)
    p.alive = false;
    goneOf(r, p); push(r); check(r);
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
    const r = rooms.get(s.data.room), p = r && (r.players.get(s.id) || r.specs.get(s.id));   // izleyiciler de yazabilir
    if (!p || p.gone || typeof txt !== 'string' || !gate(s, 'chat', 700)) return;
    const x = cleanMsg(txt, 140); if (!x) return;
    addChat(r, { id: s.id, n: p.name, c: p.col || '', x, d: (r.started && !p.alive) ? 1 : 0 });   // d: ölmüş / izleyici
  });
  // Canlı izleme: yalnızca herkese açık ve OYUNDA olan odalar. İzleyici oyuncu sayılmaz (hazır/skor/ölüm işlemlerine girmez), sohbete yazabilir
  s.on('spectate', d => {
    d = (d && typeof d === 'object') ? d : {};
    if (!gate(s, 'spec', 500)) return;
    if (s.data.room) leave(s);
    const r = rooms.get(String(d.code || '').trim().toUpperCase());
    if (!r) return s.emit('err', 'Oda bulunamadı');
    if (!r.public) return s.emit('err', 'Bu oda izlemeye açık değil');
    if (!r.started || !r.startMsg) return s.emit('err', 'Oyun şu an sürmüyor, odaya katılabilirsin');
    if (r.specs.size >= MAX_SPECS) return s.emit('err', 'İzleyici sınırı doldu (' + MAX_SPECS + ')');
    const cid = String(d.cid || '').slice(0, 40);
    if (otherTabInRoom(s, cid)) return;
    if (cid && r.banned.has(cid)) return s.emit('err', 'Bu odadan atıldın, tekrar giremezsin');
    r.specs.set(s.id, { id: s.id, cid, name: clean(d.name, 15) || 'İzleyici', bird: num(d.bird, 0, 999) | 0 });
    s.join(r.code); s.data.room = r.code;
    s.emit('sstart', Object.assign({}, r.startMsg, { claimed: r.claimed.size <= 3000 ? [...r.claimed] : [], boss: r.lastBoss }));
    push(r);
    s.emit('chath', r.chat || []);   // 'room' olayından SONRA: istemci oda değişince sohbeti temizler
  });
  s.on('leave', () => leave(s));
  s.on('disconnect', () => {
    leave(s);
    const n = (connsByIp.get(s.data.ip) || 1) - 1; if (n > 0) connsByIp.set(s.data.ip, n); else connsByIp.delete(s.data.ip);
  });
});

// ~30 Hz toplu konum yayını (kuyruktaki TÜM örneklerle): [slot, y, vy, skor, oyun adımı, zıplama sayacı, ...]. Adım numarası sayesinde herkes konumu aynı oyun zamanında çizer
setInterval(() => rooms.forEach(r => { if (r.started) flushRoom(r, false); }), TICK);

// SILENT_MS (4 sn) hiç veri göndermeyen (interneti giden / donmuş) canlı oyuncuyu elenmiş say
setInterval(() => rooms.forEach(r => {
  if (!r.started) return;
  const now = Date.now(), dead = [];
  r.players.forEach(p => { if (p.alive && now - p.last > SILENT_MS) { p.alive = false; dead.push(p); } });
  if (!dead.length) return;
  dead.forEach(p => goneOf(r, p));
  push(r); check(r);
}), 1000);
// Bellek temizliği: eski oda kurma kayıtları silinir
setInterval(() => { const now = Date.now(); lastCreate.forEach((t, ip) => { if (now - t > 60000) lastCreate.delete(ip); }); }, 60000);
console.log('MP sunucu hazır');
