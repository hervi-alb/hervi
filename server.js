"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const LOG_FILE = path.join(DATA_DIR, "messages.jsonl");
const PUBLIC_DIR = path.join(__dirname, "public");

// Admin access. Set ADMIN_PASSWORD in the host's environment settings.
// If it is not set, admin mode is switched off completely.
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "";

const KEEP = 2000;               // messages stored and sent to a new visitor
const MAX_NAME = 20;
const MAX_TEXT = 500;
const MIN_GAP_MS = 700;          // minimum time between messages from one connection
const IP_WINDOW_MS = 10000;      // per-IP window
const IP_MAX_IN_WINDOW = 12;     // messages allowed per IP per window
const MAX_CONNS_PER_IP = 8;
const LOGIN_FAIL_WINDOW_MS = 10 * 60 * 1000;
const LOGIN_MAX_FAILS = 5;       // wrong passwords allowed per IP per window
const MAX_ADMIN_TOKENS = 20;

fs.mkdirSync(DATA_DIR, { recursive: true });

// ---- message storage: JSON lines file, everything kept in memory ----
let messages = [];
let nextId = 1;
try {
  const lines = fs.readFileSync(LOG_FILE, "utf8").split("\n").filter(Boolean);
  for (const line of lines.slice(-KEEP)) {
    try { messages.push(JSON.parse(line)); } catch (_) { /* skip bad line */ }
  }
} catch (_) { /* first run */ }
for (const m of messages) {
  if (typeof m.id !== "number") m.id = nextId++;
  else if (m.id >= nextId) nextId = m.id + 1;
}

// Rewrite the whole file safely (write a temp file, then swap it in).
function persistAll() {
  const tmp = LOG_FILE + ".tmp";
  const body = messages.map((m) => JSON.stringify(m)).join("\n") + (messages.length ? "\n" : "");
  fs.writeFileSync(tmp, body);
  fs.renameSync(tmp, LOG_FILE);
}

function saveMessage(m) {
  m.id = nextId++;
  messages.push(m);
  if (messages.length > KEEP) {
    messages = messages.slice(-KEEP);
    persistAll();
  } else {
    fs.appendFileSync(LOG_FILE, JSON.stringify(m) + "\n");
  }
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

// ---- admin auth ----
const sha = (s) => crypto.createHash("sha256").update(String(s)).digest();
function passwordOk(p) {
  if (!ADMIN_PASSWORD) return false;
  return crypto.timingSafeEqual(sha(p), sha(ADMIN_PASSWORD));
}
const adminTokens = new Set();   // lives in memory; a server restart logs the admin out
const loginFails = new Map();    // ip -> timestamps of wrong passwords

function recentFails(ip, now) {
  const fresh = (loginFails.get(ip) || []).filter((t) => now - t < LOGIN_FAIL_WINDOW_MS);
  if (fresh.length) loginFails.set(ip, fresh); else loginFails.delete(ip);
  return fresh;
}

// ---- websocket ----
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 4096 });
const connsPerIp = new Map();
const ipHits = new Map();

function clientIp(req) {
  const fwd = req.headers["x-forwarded-for"];
  if (fwd) return String(fwd).split(",")[0].trim();
  return req.socket.remoteAddress || "unknown";
}

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
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
  ws.isAdmin = false;
  ws.lastSent = 0;
  ws.on("pong", () => { ws.isAlive = true; });

  send(ws, { type: "history", messages });
  broadcastOnline();

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (_) { return; }
    if (!msg || typeof msg.type !== "string") return;
    const now = Date.now();

    // ----- admin: log in, resume, log out -----
    if (msg.type === "login") {
      if (!ADMIN_PASSWORD) return send(ws, { type: "admin", ok: false, reason: "disabled" });
      if (recentFails(ip, now).length >= LOGIN_MAX_FAILS) {
        return send(ws, { type: "admin", ok: false, reason: "locked" });
      }
      if (typeof msg.password === "string" && passwordOk(msg.password)) {
        const token = crypto.randomBytes(32).toString("hex");
        adminTokens.add(token);
        if (adminTokens.size > MAX_ADMIN_TOKENS) adminTokens.delete(adminTokens.values().next().value);
        ws.isAdmin = true;
        return send(ws, { type: "admin", ok: true, token });
      }
      const fails = recentFails(ip, now);
      fails.push(now);
      loginFails.set(ip, fails);
      return send(ws, { type: "admin", ok: false, reason: "wrong" });
    }
    if (msg.type === "resume") {
      if (typeof msg.token === "string" && adminTokens.has(msg.token)) {
        ws.isAdmin = true;
        return send(ws, { type: "admin", ok: true });
      }
      return send(ws, { type: "admin", ok: false, resume: true });
    }
    if (msg.type === "logout") {
      if (typeof msg.token === "string") adminTokens.delete(msg.token);
      ws.isAdmin = false;
      return send(ws, { type: "admin", ok: false, loggedOut: true });
    }

    // ----- admin-only actions -----
    if (msg.type === "clear" || msg.type === "edit" || msg.type === "delete") {
      if (!ws.isAdmin) return send(ws, { type: "error", reason: "forbidden" });

      if (msg.type === "clear") {
        messages = [];
        persistAll();
        return broadcast({ type: "clear" });
      }

      const target = messages.find((m) => m.id === msg.id);
      if (!target) return send(ws, { type: "error", reason: "missing" });

      if (msg.type === "delete") {
        messages = messages.filter((m) => m.id !== msg.id);
        persistAll();
        return broadcast({ type: "delete", id: msg.id });
      }

      const text = cleanText(msg.text);
      if (!text || text.length > MAX_TEXT) return send(ws, { type: "error", reason: "text" });
      target.text = text;
      target.edited = true;
      persistAll();
      return broadcast({ type: "edit", id: target.id, text });
    }

    // ----- everyone: send a chat message -----
    if (msg.type !== "msg") return;

    const name = cleanName(msg.name);
    const text = cleanText(msg.text);
    if (name.length < 2 || name.length > MAX_NAME) {
      return send(ws, { type: "error", reason: "name" });
    }
    if (!text || text.length > MAX_TEXT) {
      return send(ws, { type: "error", reason: "text" });
    }

    if (now - ws.lastSent < MIN_GAP_MS) {
      return send(ws, { type: "error", reason: "slow" });
    }
    const hits = (ipHits.get(ip) || []).filter((t) => now - t < IP_WINDOW_MS);
    if (hits.length >= IP_MAX_IN_WINDOW) {
      ipHits.set(ip, hits);
      return send(ws, { type: "error", reason: "slow" });
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
  for (const ip of [...loginFails.keys()]) recentFails(ip, now);
}, 30000);

server.listen(PORT, () => {
  console.log("Open Lobby listening on port " + PORT + (ADMIN_PASSWORD ? " (admin enabled)" : " (admin disabled: no ADMIN_PASSWORD set)"));
});

function shutdown() { process.exit(0); }
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
