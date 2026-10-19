"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const LOG_FILE = path.join(DATA_DIR, "messages.jsonl");
const PUBLIC_DIR = path.join(__dirname, "public");

const KEEP_IN_MEMORY = 500;      // messages sent to a new visitor
const MAX_NAME = 20;
const MAX_TEXT = 500;
const MIN_GAP_MS = 700;          // minimum time between messages from one connection
const IP_WINDOW_MS = 10000;      // per-IP window
const IP_MAX_IN_WINDOW = 12;     // messages allowed per IP per window
const MAX_CONNS_PER_IP = 8;

fs.mkdirSync(DATA_DIR, { recursive: true });

// ---- message storage: append-only JSON lines, last N kept in memory ----
let messages = [];
try {
  const lines = fs.readFileSync(LOG_FILE, "utf8").split("\n").filter(Boolean);
  for (const line of lines.slice(-KEEP_IN_MEMORY)) {
    try { messages.push(JSON.parse(line)); } catch (_) { /* skip bad line */ }
  }
} catch (_) { /* first run */ }

const logStream = fs.createWriteStream(LOG_FILE, { flags: "a" });

function saveMessage(m) {
  messages.push(m);
  if (messages.length > KEEP_IN_MEMORY) messages = messages.slice(-KEEP_IN_MEMORY);
  logStream.write(JSON.stringify(m) + "\n");
}

// ---- static files ----
const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".txt": "text/plain; charset=utf-8",
  ".xml": "application/xml; charset=utf-8",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
};

const server = http.createServer((req, res) => {
  const urlPath = (req.url || "/").split("?")[0];
  if (urlPath === "/healthz") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    return res.end("ok");
  }
  let rel = urlPath === "/" ? "/index.html" : urlPath;
  const file = path.normalize(path.join(PUBLIC_DIR, rel));
  if (!file.startsWith(PUBLIC_DIR + path.sep)) {
    res.writeHead(403);
    return res.end("Forbidden");
  }
  fs.readFile(file, (err, buf) => {
    if (err) {
      res.writeHead(404, { "Content-Type": "text/plain" });
      return res.end("Not found");
    }
    res.writeHead(200, {
      "Content-Type": TYPES[path.extname(file)] || "application/octet-stream",
      "X-Content-Type-Options": "nosniff",
      "Cache-Control": "no-cache",
    });
    res.end(buf);
  });
});

// ---- websocket ----
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 4096 });
const connsPerIp = new Map();
const ipHits = new Map();

function clientIp(req) {
  const fwd = req.headers["x-forwarded-for"];
  if (fwd) return String(fwd).split(",")[0].trim();
  return req.socket.remoteAddress || "unknown";
}

function broadcast(obj) {
  const data = JSON.stringify(obj);
  for (const c of wss.clients) {
    if (c.readyState === 1) c.send(data);
  }
}
function broadcastOnline() {
  broadcast({ type: "online", n: wss.clients.size });
}

function cleanName(s) {
  return String(s || "").replace(/[\u0000-\u001f\u007f]/g, "").replace(/\s+/g, " ").trim();
}
function cleanText(s) {
  return String(s || "").replace(/[\u0000-\u0009\u000b-\u001f\u007f]/g, "").trim();
}

wss.on("connection", (ws, req) => {
  const ip = clientIp(req);
  const open = (connsPerIp.get(ip) || 0) + 1;
  if (open > MAX_CONNS_PER_IP) {
    ws.close(1008, "Too many connections");
    return;
  }
  connsPerIp.set(ip, open);

  ws.isAlive = true;
  ws.lastSent = 0;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.send(JSON.stringify({ type: "history", messages }));
  broadcastOnline();

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (_) { return; }
    if (!msg || msg.type !== "msg") return;

    const name = cleanName(msg.name);
    const text = cleanText(msg.text);
    if (name.length < 2 || name.length > MAX_NAME) {
      return ws.send(JSON.stringify({ type: "error", reason: "name" }));
    }
    if (!text || text.length > MAX_TEXT) {
      return ws.send(JSON.stringify({ type: "error", reason: "text" }));
    }

    const now = Date.now();
    if (now - ws.lastSent < MIN_GAP_MS) {
      return ws.send(JSON.stringify({ type: "error", reason: "slow" }));
    }
    const hits = (ipHits.get(ip) || []).filter((t) => now - t < IP_WINDOW_MS);
    if (hits.length >= IP_MAX_IN_WINDOW) {
      ipHits.set(ip, hits);
      return ws.send(JSON.stringify({ type: "error", reason: "slow" }));
    }
    hits.push(now);
    ipHits.set(ip, hits);
    ws.lastSent = now;

    const m = { name, text, ts: now };
    saveMessage(m);
    broadcast({ type: "msg", message: m });
  });

  ws.on("close", () => {
    const left = (connsPerIp.get(ip) || 1) - 1;
    if (left <= 0) connsPerIp.delete(ip); else connsPerIp.set(ip, left);
    broadcastOnline();
  });
  ws.on("error", () => {});
});

// drop dead connections, tidy rate-limit memory
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
  const now = Date.now();
  for (const [ip, hits] of ipHits) {
    const fresh = hits.filter((t) => now - t < IP_WINDOW_MS);
    if (fresh.length) ipHits.set(ip, fresh); else ipHits.delete(ip);
  }
}, 30000);

server.listen(PORT, () => {
  console.log("Open Lobby listening on port " + PORT);
});

function shutdown() {
  logStream.end(() => process.exit(0));
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
