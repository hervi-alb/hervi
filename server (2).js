"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const LOG_FILE = path.join(DATA_DIR, "messages.jsonl");
const UPLOAD_DIR = path.join(DATA_DIR, "uploads");
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

// Uploads
const MAX_UPLOAD = 10 * 1024 * 1024;        // 10 MB per file
const MAX_STORAGE = 300 * 1024 * 1024;      // 300 MB of uploads in total
const UPLOAD_WINDOW_MS = 5 * 60 * 1000;
const UPLOAD_MAX_IN_WINDOW = 10;            // uploads per IP per 5 minutes
const PENDING_TTL_MS = 15 * 60 * 1000;      // an uploaded file not yet posted is deleted after this

fs.mkdirSync(DATA_DIR, { recursive: true });
fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// ---- uploaded files ----
// fileIndex: every file on disk we know about (id -> info). pending: uploaded but not posted yet.
const fileIndex = new Map();
const pending = new Map();
const ID_RE = /^[a-f0-9]{24}$/;
const EXT_RE = /^[a-z0-9]{1,5}$/;

function filePath(meta) {
  return path.join(UPLOAD_DIR, meta.id + "." + meta.ext);
}
function removeFile(meta) {
  if (!meta || !ID_RE.test(meta.id) || !EXT_RE.test(meta.ext)) return;
  fs.unlink(filePath(meta), () => {});
  fileIndex.delete(meta.id);
  pending.delete(meta.id);
}
function storageUsed() {
  let n = 0;
  for (const m of fileIndex.values()) n += m.size;
  return n;
}

// Look at the first bytes of the file to decide what it really is. The name only
// helps tell apart formats that share a container. Anything not listed is refused.
function detect(buf, name) {
  if (buf.length < 4) return null;
  const m = /\.([A-Za-z0-9]{1,5})$/.exec(name);
  const ext = m ? m[1].toLowerCase() : "";
  const a = (s, e) => buf.toString("latin1", s, e);
  if (buf[0] === 0x89 && a(1, 4) === "PNG") return { kind: "image", mime: "image/png", ext: "png" };
  if (buf[0] === 0xff && buf[1] === 0xd8 && buf[2] === 0xff) return { kind: "image", mime: "image/jpeg", ext: "jpg" };
  if (a(0, 4) === "GIF8") return { kind: "image", mime: "image/gif", ext: "gif" };
  if (a(0, 4) === "RIFF" && buf.length > 12) {
    const t = a(8, 12);
    if (t === "WEBP") return { kind: "image", mime: "image/webp", ext: "webp" };
    if (t === "WAVE") return { kind: "audio", mime: "audio/wav", ext: "wav" };
  }
  if (a(0, 4) === "OggS") return { kind: "audio", mime: "audio/ogg", ext: "ogg" };
  if (buf[0] === 0x1a && buf[1] === 0x45 && buf[2] === 0xdf && buf[3] === 0xa3) {
    return { kind: "video", mime: "video/webm", ext: "webm" };
  }
  if (buf.length > 12 && a(4, 8) === "ftyp") {
    if (ext === "m4a") return { kind: "audio", mime: "audio/mp4", ext: "m4a" };
    if (ext === "mp4" || ext === "m4v") return { kind: "video", mime: "video/mp4", ext: "mp4" };
    if (ext === "mov") return { kind: "video", mime: "video/quicktime", ext: "mov" };
    return null;
  }
  if ((a(0, 3) === "ID3" || (buf[0] === 0xff && (buf[1] & 0xe0) === 0xe0)) && ext === "mp3") {
    return { kind: "audio", mime: "audio/mpeg", ext: "mp3" };
  }
  if (a(0, 4) === "%PDF") return { kind: "file", mime: "application/pdf", ext: "pdf" };
  if (buf[0] === 0x50 && buf[1] === 0x4b && buf[2] === 3 && buf[3] === 4 &&
      ["zip", "docx", "xlsx", "pptx"].includes(ext)) {
    return { kind: "file", mime: "application/octet-stream", ext: ext };
  }
  if (["txt", "csv", "md", "log", "json"].includes(ext)) {
    const n = Math.min(buf.length, 8192);
    for (let i = 0; i < n; i++) if (buf[i] === 0) return null;
    return { kind: "file", mime: "text/plain", ext: ext };
  }
  return null;
}

function cleanFileName(s) {
  let n = String(s || "file")
    .replace(/[\u0000-\u001f\u007f\\\/:*?"<>|]/g, "")
    .replace(/\s+/g, " ")
    .trim();
  if (!n) n = "file";
  if (n.length > 100) {
    const m = /\.[A-Za-z0-9]{1,5}$/.exec(n);
    const ext = m ? m[0] : "";
    n = n.slice(0, n.length - ext.length).slice(0, 90) + ext;
  }
  return n;
}

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
  if (m.file) {
    const f = m.file;
    if (f && ID_RE.test(f.id) && EXT_RE.test(f.ext) && typeof f.size === "number") fileIndex.set(f.id, f);
    else delete m.file;
  }
}
// delete files on disk that no message points to (left over from a crash)
try {
  for (const fn of fs.readdirSync(UPLOAD_DIR)) {
    const id = fn.split(".")[0];
    if (!fileIndex.has(id)) fs.unlink(path.join(UPLOAD_DIR, fn), () => {});
  }
} catch (_) {}

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
    const dropped = messages.slice(0, messages.length - KEEP);
    messages = messages.slice(-KEEP);
    dropped.forEach((d) => removeFile(d.file));
    persistAll();
  } else {
    fs.appendFileSync(LOG_FILE, JSON.stringify(m) + "\n");
  }
}

// the part of a message's file info that visitors need
function publicMessage(m) {
  if (!m.file) return m;
  const f = m.file;
  return Object.assign({}, m, { file: { id: f.id, name: f.name, kind: f.kind, mime: f.mime, size: f.size } });
}

// ---- helpers for http ----
function clientIp(req) {
  const fwd = req.headers["x-forwarded-for"];
  if (fwd) return String(fwd).split(",")[0].trim();
  return req.socket.remoteAddress || "unknown";
}
function jsonRes(res, code, obj) {
  res.writeHead(code, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  res.end(JSON.stringify(obj));
}

const uploadHits = new Map();

function handleUpload(req, res) {
  if (req.method !== "POST") return jsonRes(res, 405, { error: "method" });
  const origin = req.headers.origin;
  if (origin) {
    let ok = false;
    try { ok = new URL(origin).host === req.headers.host; } catch (_) {}
    if (!ok) return jsonRes(res, 403, { error: "origin" });
  }
  const ip = clientIp(req);
  const now = Date.now();
  const hits = (uploadHits.get(ip) || []).filter((t) => now - t < UPLOAD_WINDOW_MS);
  if (hits.length >= UPLOAD_MAX_IN_WINDOW) { req.resume(); return jsonRes(res, 429, { error: "rate" }); }

  const len = Number(req.headers["content-length"]);
  if (!Number.isFinite(len) || len <= 0) { req.resume(); return jsonRes(res, 411, { error: "length" }); }
  if (len > MAX_UPLOAD) { req.resume(); return jsonRes(res, 413, { error: "size" }); }
  if (storageUsed() + len > MAX_STORAGE) { req.resume(); return jsonRes(res, 507, { error: "storage" }); }
  hits.push(now);
  uploadHits.set(ip, hits);

  let name = "file";
  try { name = cleanFileName(new URL(req.url, "http://x").searchParams.get("name")); } catch (_) {}

  const chunks = [];
  req.on("data", (c) => chunks.push(c));
  req.on("end", () => {
    const buf = Buffer.concat(chunks);
    const d = detect(buf, name);
    if (!d) return jsonRes(res, 415, { error: "type" });
    const id = crypto.randomBytes(12).toString("hex");
    const meta = { id, name, kind: d.kind, mime: d.mime, ext: d.ext, size: buf.length };
    fs.writeFile(filePath(meta), buf, (err) => {
      if (err) return jsonRes(res, 500, { error: "write" });
      fileIndex.set(id, meta);
      pending.set(id, Date.now());
      jsonRes(res, 200, { id, name, kind: d.kind, mime: d.mime, size: buf.length });
    });
  });
  req.on("error", () => {});
}

function handleFile(req, res, urlPath) {
  const id = urlPath.slice("/files/".length);
  const meta = ID_RE.test(id) ? fileIndex.get(id) : null;
  if (!meta || (req.method !== "GET" && req.method !== "HEAD")) {
    res.writeHead(404, { "Content-Type": "text/plain" });
    return res.end("Not found");
  }
  const fp = filePath(meta);
  fs.stat(fp, (err, st) => {
    if (err) {
      res.writeHead(404, { "Content-Type": "text/plain" });
      return res.end("Not found");
    }
    // Pictures, video and audio show inside the chat. Everything else is download only.
    const inline = meta.kind === "image" || meta.kind === "video" || meta.kind === "audio";
    const headers = {
      "Content-Type": inline ? meta.mime : "application/octet-stream",
      "Content-Disposition": (inline ? "inline" : "attachment") + "; filename*=UTF-8''" + encodeURIComponent(meta.name),
      "X-Content-Type-Options": "nosniff",
      "Content-Security-Policy": "default-src 'none'; sandbox",
      "Cache-Control": "public, max-age=86400, immutable",
      "Accept-Ranges": "bytes",
    };
    let start = 0, end = st.size - 1, code = 200;
    const r = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || "");
    if (r && (r[1] !== "" || r[2] !== "")) {
      if (r[1] === "") start = Math.max(0, st.size - Number(r[2]));
      else { start = Number(r[1]); if (r[2] !== "") end = Math.min(end, Number(r[2])); }
      if (start > end || start >= st.size) {
        res.writeHead(416, { "Content-Range": "bytes */" + st.size });
        return res.end();
      }
      code = 206;
      headers["Content-Range"] = "bytes " + start + "-" + end + "/" + st.size;
    }
    headers["Content-Length"] = end - start + 1;
    res.writeHead(code, headers);
    if (req.method === "HEAD") return res.end();
    const stream = fs.createReadStream(fp, { start, end });
    stream.on("error", () => res.destroy());
    stream.pipe(res);
  });
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
  if (urlPath === "/upload") return handleUpload(req, res);
  if (urlPath.startsWith("/files/")) return handleFile(req, res, urlPath);

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

  send(ws, { type: "history", messages: messages.map(publicMessage) });
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
        messages.forEach((m) => removeFile(m.file));
        messages = [];
        persistAll();
        return broadcast({ type: "clear" });
      }

      const target = messages.find((m) => m.id === msg.id);
      if (!target) return send(ws, { type: "error", reason: "missing" });

      if (msg.type === "delete") {
        removeFile(target.file);
        messages = messages.filter((m) => m.id !== msg.id);
        persistAll();
        return broadcast({ type: "delete", id: msg.id });
      }

      const text = cleanText(msg.text);
      if ((!text && !target.file) || text.length > MAX_TEXT) return send(ws, { type: "error", reason: "text" });
      target.text = text;
      target.edited = true;
      persistAll();
      return broadcast({ type: "edit", id: target.id, text });
    }

    // ----- everyone: send a chat message -----
    if (msg.type !== "msg") return;

    const name = cleanName(msg.name);
    const text = cleanText(msg.text);
    const fileId = typeof msg.fileId === "string" ? msg.fileId : "";
    if (name.length < 2 || name.length > MAX_NAME) {
      return send(ws, { type: "error", reason: "name" });
    }
    let file = null;
    if (fileId) {
      file = pending.has(fileId) ? fileIndex.get(fileId) : null;
      if (!file) return send(ws, { type: "error", reason: "file" });
    }
    if ((!text && !file) || text.length > MAX_TEXT) {
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
    if (file) {
      m.file = file;
      pending.delete(file.id);
    }
    saveMessage(m);
    broadcast({ type: "msg", message: publicMessage(m) });
  });

  ws.on("close", () => {
    const left = (connsPerIp.get(ip) || 1) - 1;
    if (left <= 0) connsPerIp.delete(ip); else connsPerIp.set(ip, left);
    broadcastOnline();
  });
  ws.on("error", () => {});
});

// drop dead connections, tidy rate-limit memory, delete uploads nobody posted
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
  for (const [ip, hits] of uploadHits) {
    const fresh = hits.filter((t) => now - t < UPLOAD_WINDOW_MS);
    if (fresh.length) uploadHits.set(ip, fresh); else uploadHits.delete(ip);
  }
  for (const ip of [...loginFails.keys()]) recentFails(ip, now);
  for (const [id, ts] of pending) {
    if (now - ts > PENDING_TTL_MS) removeFile(fileIndex.get(id));
  }
}, 30000);

server.listen(PORT, () => {
  console.log("Open Lobby listening on port " + PORT + (ADMIN_PASSWORD ? " (admin enabled)" : " (admin disabled: no ADMIN_PASSWORD set)"));
});

function shutdown() { process.exit(0); }
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
