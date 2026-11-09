import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { randomBytes, randomInt } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { WebSocketServer, WebSocket } from 'ws';

export function createApp() {
  const rooms = new Map();
  const server = http.createServer(async (req, res) => {
    const path = new URL(req.url, 'http://localhost').pathname;
    const files = { '/': ['index.html', 'text/html; charset=utf-8'], '/app.js': ['app.js', 'text/javascript'], '/style.css': ['style.css', 'text/css'] };
    const file = files[path];
    if (!file) { res.writeHead(404); return res.end('Not found'); }
    try { res.writeHead(200, { 'Content-Type': file[1], 'X-Content-Type-Options': 'nosniff' }); res.end(await readFile(new URL(`./public/${file[0]}`, import.meta.url))); }
    catch { res.writeHead(500); res.end('Server error'); }
  });
  const wss = new WebSocketServer({ server, maxPayload: 4096 });
  const send = (ws, data) => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data)); };
  const broadcast = room => {
    for (const p of room.players.values()) if (p.ws) send(p.ws, { type: 'state', room: {
      code: room.code, count: room.count, broken: room.broken, host: room.host, round: room.round,
      players: [...room.players.values()].map(({ id, name, ws, pick }) => ({ id, name, online: !!ws, pick })),
      matches: room.matches.map((m, index) => ({ index, owner: m.owner, ...(m.owner ? { broken: m.broken } : {}) })),
      complete: room.matches.every(m => m.owner), you: p.id
    } });
  };
  function reset(room) {
    room.matches = Array.from({ length: room.count }, (_, i) => ({ broken: i < room.broken, owner: null }));
    for (let i = room.matches.length - 1; i > 0; i--) { const j = randomInt(i + 1); [room.matches[i], room.matches[j]] = [room.matches[j], room.matches[i]]; }
    for (const p of room.players.values()) p.pick = null;
    room.round++;
  }
  wss.on('connection', ws => {
    ws.alive = true;
    ws.on('pong', () => { ws.alive = true; });
    ws.on('message', raw => {
      try {
        const msg = JSON.parse(raw);
        if (msg.type === 'create' || msg.type === 'join') {
          if (ws.room) throw Error('Вы уже в комнате');
          const name = String(msg.name || '').trim().slice(0, 24);
          if (!name) throw Error('Введите имя');
          let room;
          if (msg.type === 'create') {
            if (rooms.size >= 1000) throw Error('Сервер занят. Попробуйте позже');
            const count = Number(msg.count), broken = Number(msg.broken);
            if (!Number.isInteger(count) || count < 2 || count > 20 || !Number.isInteger(broken) || broken < 1 || broken >= count) throw Error('Нужно от 2 до 20 игроков и меньше поломанных спичек, чем игроков');
            let code; do { code = randomBytes(3).toString('hex').toUpperCase(); } while (rooms.has(code));
            room = { code, count, broken, players: new Map(), host: null, round: 0, touched: Date.now() };
            reset(room); rooms.set(code, room);
          } else {
            room = rooms.get(String(msg.code || '').trim().toUpperCase());
            if (!room) throw Error('Комната не найдена. Возможно, сервер был перезапущен');
          }
          let p = [...room.players.values()].find(p => p.token === msg.token);
          if (!p) {
            if (room.players.size >= room.count) throw Error('В комнате уже достаточно игроков');
            p = { id: randomBytes(8).toString('hex'), token: randomBytes(24).toString('hex'), name, ws: null, pick: null };
            room.players.set(p.id, p);
          }
          if (p.ws) { p.ws.room = null; p.ws.close(4001, 'Opened in another tab'); }
          p.ws = ws; ws.room = room; ws.player = p;
          room.host ||= p.id; room.touched = Date.now();
          send(ws, { type: 'joined', token: p.token, code: room.code }); broadcast(room); return;
        }
        const room = ws.room, p = ws.player;
        if (!room || p.ws !== ws) throw Error('Сначала войдите в комнату');
        room.touched = Date.now();
        if (msg.type === 'pick') {
          if (p.pick !== null) throw Error('Вы уже вытянули спичку');
          if (!Number.isInteger(msg.index) || !room.matches[msg.index] || room.matches[msg.index].owner) throw Error('Эту спичку уже забрали. Выберите другую');
          room.matches[msg.index].owner = p.id; p.pick = msg.index; broadcast(room);
        } else if (msg.type === 'reset') {
          if (p.id !== room.host) throw Error('Новый раунд запускает ведущий');
          if (!room.matches.every(m => m.owner)) throw Error('Дождитесь окончания раунда');
          reset(room); broadcast(room);
        }
      } catch (e) { send(ws, { type: 'error', message: e.message }); }
    });
    ws.on('close', () => {
      const room = ws.room, p = ws.player;
      if (room && p.ws === ws) { p.ws = null; room.touched = Date.now(); broadcast(room); }
    });
  });
  const timer = setInterval(() => {
    for (const ws of wss.clients) { if (!ws.alive) ws.terminate(); else { ws.alive = false; ws.ping(); } }
    for (const [code, room] of rooms) if (Date.now() - room.touched > 86400000 && [...room.players.values()].every(p => !p.ws)) rooms.delete(code);
  }, 30000);
  timer.unref();
  server.on('close', () => { clearInterval(timer); wss.close(); });
  return { server, wss };
}
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const { server } = createApp();
  server.listen(Number(process.env.PORT || 3000), '0.0.0.0', () => console.log(`Matches: http://localhost:${server.address().port}`));
}
