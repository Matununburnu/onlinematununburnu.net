// Azizin Minik Kuşu - çok oyunculu oda sunucusu
// Kurulum: npm i socket.io  |  Çalıştır: node server.js  (PORT ortam değişkeni ile port seçilir)
const { Server } = require('socket.io');
const io = new Server(process.env.PORT || 3000, { cors: { origin: '*' } });
const MAX = 10, rooms = new Map();
const code = () => { let c; do { c = Array.from({ length: 4 }, () => 'ABCDEFGHJKLMNPRSTUVYZ'[Math.random() * 21 | 0]).join(''); } while (rooms.has(c)); return c; };
const pub = r => ({ code: r.code, started: r.started, players: [...r.players.values()].map(p => ({ id: p.id, name: p.name, bird: p.bird, host: p.id === r.host, score: p.score, alive: p.alive })) });
const push = r => io.to(r.code).emit('room', pub(r));
function leave(s) {
  const r = rooms.get(s.data.room); if (!r) return;
  r.players.delete(s.id); s.leave(r.code); s.data.room = null;
  if (!r.players.size) return rooms.delete(r.code);
  if (r.host === s.id) r.host = r.players.keys().next().value;
  push(r); check(r);
}
function check(r) { if (r.started && ![...r.players.values()].some(p => p.alive)) { r.started = false; io.to(r.code).emit('over', pub(r).players.sort((a, b) => b.score - a.score)); push(r); } }
function join(s, r, d) {
  if (r.players.size >= MAX || r.started) return s.emit('err', r.started ? 'Oyun başladı' : 'Oda dolu (10/10)');
  r.players.set(s.id, { id: s.id, name: String(d.name || 'Oyuncu').slice(0, 12), bird: d.bird | 0, score: 0, alive: true, last: Date.now() });
  s.join(r.code); s.data.room = r.code; push(r);
}
io.on('connection', s => {
  s.on('t', (c, cb) => { if (typeof cb === 'function') cb(Date.now()); });   // saat eşitleme
  s.on('create', d => { if (s.data.room) leave(s); const r = { code: code(), host: s.id, started: false, players: new Map() }; rooms.set(r.code, r); join(s, r, d || {}); });
  s.on('join', d => { if (s.data.room) leave(s); const r = rooms.get(String(d && d.code || '').toUpperCase()); r ? join(s, r, d) : s.emit('err', 'Oda bulunamadı'); });
  s.on('start', () => {
    const r = rooms.get(s.data.room); if (!r || r.host !== s.id || r.started) return;
    r.started = true; const slots = {}; let i = 0;
    r.players.forEach(p => { p.alive = true; p.score = 0; p.last = Date.now() + 4000; slots[p.id] = i++; });
    // Herkes aynı sunucu saatinde başlar: 3 sn geri sayım + 0.5 sn tampon
    io.to(r.code).emit('start', { seed: Math.random() * 4294967296 >>> 0, startAt: Date.now() + 3500, slots, n: i }); push(r);
  });
  s.on('s', a => { const r = rooms.get(s.data.room), p = r && r.players.get(s.id); if (!p || !Array.isArray(a)) return; p.last = Date.now(); p.score = a[2] | 0; s.to(r.code).volatile.emit('s', [s.id, a[0], a[1], a[2], a[3]]); });
  s.on('dead', sc => { const r = rooms.get(s.data.room), p = r && r.players.get(s.id); if (!p) return; p.alive = false; p.score = sc | 0; push(r); check(r); });
  s.on('leave', () => leave(s));
  s.on('disconnect', () => leave(s));
});
// 15 sn hiç veri göndermeyen (bağlantısı kopmuş/donmuş) canlı oyuncuyu elenmiş say
setInterval(() => rooms.forEach(r => { if (!r.started) return; let ch = false; r.players.forEach(p => { if (p.alive && Date.now() - p.last > 15000) { p.alive = false; ch = true; } }); if (ch) { push(r); check(r); } }), 3000);
console.log('MP sunucu hazır');
