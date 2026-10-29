// Server papan tulis kelas: Cloudflare Worker + Durable Object (satu objek = satu ruang).
// Server menjadi penengah: hanya guru dan murid berizin yang goresannya diteruskan.
export class Room {
  constructor(state, env) {
    this.s = state; this.env = env;
    state.blockConcurrencyWhile(async () => {
      this.hist = (await state.storage.get('hist')) || [];
      this.teacher = (await state.storage.get('teacher')) || null;
      this.allowed = new Set();
    });
  }
  async fetch(req) {
    if (req.headers.get('Upgrade') !== 'websocket') return new Response('Butuh WebSocket', { status: 426 });
    const u = new URL(req.url), t = u.searchParams.get('t') || '';
    // Pembuat ruang pertama yang membawa token menjadi guru; token disimpan di server.
    if (!this.teacher && t.length >= 16) { this.teacher = t; await this.s.storage.put('teacher', t); }
    const role = this.teacher && t === this.teacher ? 'guru' : 'murid';
    const id = crypto.randomUUID().slice(0, 6);
    const name = (u.searchParams.get('n') || 'Peserta').slice(0, 24);
    const [client, server] = Object.values(new WebSocketPair());
    this.s.acceptWebSocket(server);
    server.serializeAttachment({ id, name, role });
    server.send(JSON.stringify({ k: 'init', you: id, role, hist: this.hist, allowed: [...this.allowed] }));
    this.pushPeers();
    return new Response(null, { status: 101, webSocket: client });
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
        const at = Date.now(); this.hist.push({ k: 'c', at }); await this.saveSoon();
        this.all({ k: 'clr', at, from: me.id }, ws); break; }
      case 'perm':
        if (!guru || !Array.isArray(m.allowed)) return;
        this.allowed = new Set(m.allowed.filter(x => typeof x === 'string'));
        this.all({ k: 'perm', allowed: [...this.allowed], from: me.id }, ws); break;
      case 'sig': // dipakai Tahap 3 (suara): meneruskan pesan WebRTC ke satu peserta
        for (const w of this.s.getWebSockets()) {
          if (w.deserializeAttachment().id === m.to) w.send(JSON.stringify({ k: 'sig', from: me.id, data: m.data }));
        } break;
    }
  }
  webSocketClose(ws) {
    const me = ws.deserializeAttachment(); this.allowed.delete(me.id);
    try { ws.close(); } catch {}
    this.pushPeers(ws);
  }
  webSocketError(ws) { this.webSocketClose(ws); }
  async saveSoon() { if (!(await this.s.storage.getAlarm())) await this.s.storage.setAlarm(Date.now() + 5000); }
  async alarm() { try { await this.s.storage.put('hist', this.hist); } catch {} }
}

export default {
  async fetch(req, env) {
    const m = new URL(req.url).pathname.match(/^\/room\/([A-Za-z0-9]{4,12})$/);
    if (!m) return new Response('Server papan tulis kelas aktif');
    const allow = (env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
    const origin = req.headers.get('Origin');
    if (allow.length && !allow.includes(origin)) return new Response('Asal tidak diizinkan', { status: 403 });
    return env.ROOM.get(env.ROOM.idFromName(m[1].toUpperCase())).fetch(req);
  }
};
