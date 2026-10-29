// Server papan tulis kelas: Cloudflare Worker + Durable Object (satu objek = satu ruang).
// Server menjadi penengah: hanya guru dan murid berizin yang goresannya diteruskan.
async function sha(s) {
  const b = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
  return [...new Uint8Array(b)].slice(0, 4).map(x => x.toString(16).padStart(2, '0')).join('');
}

export class Room {
  constructor(state, env) {
    this.s = state; this.env = env;
    state.blockConcurrencyWhile(async () => {
      this.hist = (await state.storage.get('hist')) || [];
      this.teacher = (await state.storage.get('teacher')) || null;
      this.allowed = new Set((await state.storage.get('allowed')) || []);
      this.speak = new Set((await state.storage.get('speak')) || []);
      this.locked = !!(await state.storage.get('locked'));
      this.seen = new Set((await state.storage.get('seen')) || []);
      this.banned = new Set((await state.storage.get('banned')) || []);
      this.ended = (await state.storage.get('ended')) || null; // {at, msg} bila guru mengakhiri kelas
      const pg = (await state.storage.get('pg')) || {}; this.page = pg.page || 0; this.npages = pg.npages || 1;
    });
  }
  async fetch(req) {
    if (req.headers.get('Upgrade') !== 'websocket') return this.img(req);
    const u = new URL(req.url), t = u.searchParams.get('t') || '';
    // Pembuat ruang pertama yang membawa token menjadi guru; token disimpan di server.
    if (!this.teacher && t.length >= 16) { this.teacher = t; await this.s.storage.put('teacher', t); }
    const role = this.teacher && t === this.teacher ? 'guru' : 'murid';
    // Id stabil: diturunkan dari rahasia klien (c), jadi izin bertahan saat sambungan putus-sambung.
    const c = u.searchParams.get('c') || '';
    const id = c.length >= 16 ? await sha(c) : crypto.randomUUID().slice(0, 8);
    // Kunci ruang: hanya yang sudah pernah masuk (atau guru) boleh tersambung lagi. Dikeluarkan: ditolak.
    const refuse = code => { const [c2, s2] = Object.values(new WebSocketPair()); s2.accept(); s2.close(code, 'ditolak'); return new Response(null, { status: 101, webSocket: c2 }); };
    if (this.banned.has(id)) return refuse(4002);
    if (this.locked && role !== 'guru' && !this.seen.has(id)) return refuse(4001);
    // Kelas berakhir: murid yang belum pernah masuk ditolak; yang sudah pernah masuk boleh melihat papan (hanya-baca).
    if (this.ended && role !== 'guru' && !this.seen.has(id)) return refuse(4003);
    if (role !== 'guru' && !this.seen.has(id) && this.seen.size < 300) { this.seen.add(id); await this.s.storage.put('seen', [...this.seen]); }
    for (const x of this.s.getWebSockets()) if (x.deserializeAttachment().id === id) { try { x.close(4000, 'diganti'); } catch {} }
    const name = (u.searchParams.get('n') || 'Peserta').slice(0, 24);
    const [client, server] = Object.values(new WebSocketPair());
    this.s.acceptWebSocket(server);
    server.serializeAttachment({ id, name, role });
    server.send(JSON.stringify({ k: 'init', you: id, role, hist: this.hist, allowed: [...this.allowed], speak: [...this.speak], locked: this.locked, page: this.page, npages: this.npages, ended: this.ended }));
    this.pushPeers();
    return new Response(null, { status: 101, webSocket: client });
  }
  // Gambar kitab: disimpan terpisah dari riwayat papan (hanya guru yang boleh mengunggah).
  async img(req) {
    const id = new URL(req.url).pathname.split('/')[3];
    if (!id) return new Response('Butuh WebSocket', { status: 426 });
    if (req.method === 'GET') {
      const d = await this.s.storage.get('img:' + id);
      return d ? new Response(d.b, { headers: { 'content-type': d.t, 'cache-control': 'public, max-age=31536000, immutable' } }) : new Response('Tidak ada', { status: 404 });
    }
    if (req.method !== 'POST') return new Response('Metode tidak didukung', { status: 405 });
    if (!this.teacher || req.headers.get('x-t') !== this.teacher) return new Response('Hanya guru yang boleh mengunggah', { status: 403 });
    if (this.ended) return new Response('Kelas sudah berakhir', { status: 403 });
    const type = req.headers.get('content-type') || '';
    if (!/^image\/(jpeg|png|webp)$/.test(type)) return new Response('Jenis berkas tidak didukung', { status: 415 });
    const b = await req.arrayBuffer();
    if (b.byteLength > 1500000) return new Response('Gambar terlalu besar (maksimal 1,5 MB)', { status: 413 });
    const n = (await this.s.storage.get('nimg')) || 0;
    if (n >= 60) return new Response('Kuota gambar ruang ini penuh (60)', { status: 507 });
    await this.s.storage.put('img:' + id, { b, t: type });
    await this.s.storage.put('nimg', n + 1);
    return new Response('ok');
  }
  peers(except) { return this.s.getWebSockets().filter(w => w !== except).map(w => w.deserializeAttachment()); }
  pushPeers(except) { this.all({ k: 'peers', peers: this.peers(except) }); }
  all(m, except) {
    const j = JSON.stringify(m);
    for (const w of this.s.getWebSockets()) if (w !== except) { try { w.send(j); } catch {} }
  }
  async webSocketMessage(ws, raw) {
    if (typeof raw !== 'string' || raw.length > 30000) return;
    let m; try { m = JSON.parse(raw); } catch { return; }
    const me = ws.deserializeAttachment(), guru = me.role === 'guru', ok = guru || this.allowed.has(me.id);
    // Setelah kelas diakhiri, papan hanya-baca: semua pesan selain ini diabaikan.
    if (this.ended && !['hello', 'leave', 'reopen', 'end'].includes(m.k)) return;
    switch (m.k) {
      case 'hello':
        me.name = String(m.name || '').slice(0, 24) || me.name;
        ws.serializeAttachment(me); this.pushPeers(); break;
      case 'add':
        if (!ok || !m.it || this.hist.length > 20000) return;
        this.hist.push(m.it); await this.saveSoon();
        this.all({ k: 'add', it: m.it, from: me.id }, ws); break;
      case 'pt': {
        if (!ok || !Array.isArray(m.pts)) return;
        for (let i = this.hist.length - 1; i >= 0; i--) {
          if (this.hist[i].sid === m.sid) { this.hist[i].p.push(...m.pts); break; }
        }
        await this.saveSoon();
        this.all({ k: 'pt', sid: m.sid, pts: m.pts, from: me.id }, ws); break; }
      case 'clr': {
        if (!guru) return;
        const at = Date.now(); this.hist.push({ k: 'c', at, g: this.page }); await this.saveSoon();
        this.all({ k: 'clr', at, g: this.page, from: me.id }, ws); break; }
      case 'perm':
        if (!guru || !Array.isArray(m.allowed)) return;
        this.allowed = new Set(m.allowed.filter(x => typeof x === 'string'));
        await this.s.storage.put('allowed', [...this.allowed]);
        this.all({ k: 'perm', allowed: [...this.allowed], from: me.id }, ws); break;
      case 'undo': { // urungkan karya terakhir peserta ini (satu goresan, atau satu kelompok tempelan), tidak melewati "bersihkan"
        if (!ok) return;
        let i = this.hist.length - 1;
        while (i >= 0 && ((this.hist[i].g || 0) !== this.page || (this.hist[i].k !== 'c' && this.hist[i].u !== me.id))) i--;
        if (i < 0 || this.hist[i].k === 'c') return;
        const { u, at } = this.hist[i];
        this.hist = this.hist.filter(x => !(x.u === u && x.at === at));
        await this.saveSoon(); this.all({ k: 'undo', u, at }, ws); break; }
      case 'st': // status suara: v = ikut suara, h = angkat tangan
        me.v = !!m.v; me.h = !!m.h; ws.serializeAttachment(me); this.pushPeers(); break;
      case 'spk': // izin bicara (hanya guru); memberi izin otomatis menurunkan tangan
        if (!guru || !Array.isArray(m.allowed)) return;
        this.speak = new Set(m.allowed.filter(x => typeof x === 'string'));
        await this.s.storage.put('speak', [...this.speak]);
        for (const x of this.s.getWebSockets()) { const a = x.deserializeAttachment(); if (a.h && this.speak.has(a.id)) { a.h = false; x.serializeAttachment(a); } }
        this.all({ k: 'spk', allowed: [...this.speak] }); this.pushPeers(); break;
      case 'mute': // bisukan semua murid (hanya guru)
        if (!guru) return;
        this.speak = new Set(); await this.s.storage.put('speak', []); this.all({ k: 'spk', allowed: [] }); break;
      case 'kick': { // keluarkan satu murid; id-nya ditolak bila mencoba masuk lagi
        if (!guru || typeof m.id !== 'string') return;
        for (const x of this.s.getWebSockets()) {
          const a = x.deserializeAttachment();
          if (a.id === m.id && a.role !== 'guru') { this.banned.add(a.id); this.allowed.delete(a.id); this.speak.delete(a.id); try { x.close(4002, 'dikeluarkan'); } catch {} }
        }
        await this.s.storage.put('banned', [...this.banned]); await this.s.storage.put('allowed', [...this.allowed]); await this.s.storage.put('speak', [...this.speak]);
        this.all({ k: 'perm', allowed: [...this.allowed] }); this.all({ k: 'spk', allowed: [...this.speak] }); break; }
      case 'lock':
        if (!guru) return;
        this.locked = !!m.v; await this.s.storage.put('locked', this.locked); this.all({ k: 'lock', v: this.locked }); break;
      case 'pg': // guru berpindah halaman papan; semua peserta mengikuti
        if (!guru || !Number.isInteger(m.n) || m.n < 0 || m.n > 49) return;
        this.page = m.n; this.npages = Math.max(this.npages, m.n + 1);
        await this.s.storage.put('pg', { page: this.page, npages: this.npages });
        this.all({ k: 'pg', n: m.n }, ws); break;
      case 'end': { // guru mengakhiri kelas untuk semua: papan hanya-baca, suara diputus
        if (!guru) return;
        this.ended = { at: Date.now(), msg: String(m.msg || '').trim().slice(0, 140) };
        await this.s.storage.put('ended', this.ended);
        await this.s.storage.put('hist', this.hist);
        for (const x of this.s.getWebSockets()) { const a = x.deserializeAttachment(); if (a.v || a.h) { a.v = false; a.h = false; x.serializeAttachment(a); } }
        this.all({ k: 'end', at: this.ended.at, msg: this.ended.msg });
        this.pushPeers(); break; }
      case 'reopen': { // guru membuka kembali kelas yang sudah diakhiri
        if (!guru || !this.ended) return;
        if (Number.isInteger(m.n) && m.n >= 0 && m.n < this.npages) { this.page = m.n; await this.s.storage.put('pg', { page: this.page, npages: this.npages }); }
        this.ended = null; await this.s.storage.delete('ended');
        this.all({ k: 'reopen', page: this.page, npages: this.npages }); break; }
      case 'leave': { // murid keluar atas kemauan sendiri: guru diberi tahu, izin tetap tersimpan
        if (!guru) for (const x of this.s.getWebSockets()) { if (x.deserializeAttachment().role === 'guru') { try { x.send(JSON.stringify({ k: 'note', t: 'leave', name: me.name })); } catch {} } }
        me.v = false; me.h = false; ws.serializeAttachment(me);
        try { ws.close(1000, 'keluar'); } catch {}
        this.pushPeers(ws); break; }
      case 'sig': // dipakai Tahap 3 (suara): meneruskan pesan WebRTC ke satu peserta
        for (const w of this.s.getWebSockets()) {
          if (w.deserializeAttachment().id === m.to) w.send(JSON.stringify({ k: 'sig', from: me.id, data: m.data }));
        } break;
    }
  }
  webSocketClose(ws) {
    try { ws.close(); } catch {}
    this.pushPeers(ws);
  }
  webSocketError(ws) { this.webSocketClose(ws); }
  async saveSoon() { if (!(await this.s.storage.getAlarm())) await this.s.storage.setAlarm(Date.now() + 5000); }
  async alarm() { try { await this.s.storage.put('hist', this.hist); } catch {} }
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const allow = (env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
    const origin = req.headers.get('Origin');
    if (url.pathname === '/turn') { // kredensial TURN berumur pendek (Cloudflare Realtime TURN); kunci tetap di server
      const cors = { 'Access-Control-Allow-Origin': origin || '*', 'Access-Control-Allow-Methods': 'GET, OPTIONS', 'Vary': 'Origin' };
      const js = (o, s = 200) => new Response(JSON.stringify(o), { status: s, headers: { ...cors, 'content-type': 'application/json', 'cache-control': 'no-store' } });
      if (allow.length && !allow.includes(origin)) return new Response('Asal tidak diizinkan', { status: 403 });
      if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors });
      if (!env.TURN_KEY_ID || !env.TURN_KEY_TOKEN) return js({ iceServers: [] }); // belum diatur: klien memakai STUN saja
      try {
        const r = await fetch(`https://rtc.live.cloudflare.com/v1/turn/keys/${env.TURN_KEY_ID}/credentials/generate-ice-servers`, {
          method: 'POST', headers: { authorization: `Bearer ${env.TURN_KEY_TOKEN}`, 'content-type': 'application/json' }, body: JSON.stringify({ ttl: 21600 })
        });
        if (!r.ok) return js({ iceServers: [] });
        const d = await r.json();
        for (const s of d.iceServers || []) if (Array.isArray(s.urls)) s.urls = s.urls.filter(x => !/:53(\?|$)/.test(x));
        return js({ iceServers: d.iceServers || [] });
      } catch { return js({ iceServers: [] }); }
    }
    const im = url.pathname.match(/^\/img\/([A-Za-z0-9]{4,12})\/([a-f0-9]{8,32})$/);
    if (im) {
      const cors = { 'Access-Control-Allow-Origin': origin || '*', 'Access-Control-Allow-Headers': 'x-t, content-type', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Vary': 'Origin' };
      if (allow.length && (origin ? !allow.includes(origin) : req.method !== 'GET')) return new Response('Asal tidak diizinkan', { status: 403 });
      if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: cors });
      const r = await env.ROOM.get(env.ROOM.idFromName(im[1].toUpperCase())).fetch(req);
      const out = new Response(r.body, r);
      for (const k in cors) out.headers.set(k, cors[k]);
      return out;
    }
    const m = url.pathname.match(/^\/room\/([A-Za-z0-9]{4,12})$/);
    if (!m) return new Response('Server papan tulis kelas aktif');
    if (allow.length && !allow.includes(origin)) return new Response('Asal tidak diizinkan', { status: 403 });
    return env.ROOM.get(env.ROOM.idFromName(m[1].toUpperCase())).fetch(req);
  }
};
