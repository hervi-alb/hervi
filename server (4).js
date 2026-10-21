"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { once } = require("events");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const LOG_FILE = path.join(DATA_DIR, "messages.jsonl");   // the Lobby's messages
const BOX_DIR = path.join(DATA_DIR, "boxes");
const META_FILE = path.join(DATA_DIR, "meta.json");
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

// Boxes
const MAX_BOXES = 50;
const BOX_NAME_MAX = 24;
const BOX_PASS_MIN = 4;
const BOX_PASS_MAX = 64;
const MAX_PINS = 5;
const POLL_MIN_OPTIONS = 2;         // polls: options per poll, and text lengths
const POLL_MAX_OPTIONS = 8;
const POLL_Q_MAX = 200;
const POLL_OPT_MAX = 60;                 // pinned messages per room
const JOIN_MAX_FAILS = 8;           // wrong box passwords allowed per IP per 10 minutes
const MAX_BOX_TOKENS = 200;

// Uploads
const MAX_UPLOAD = 10 * 1024 * 1024;        // 10 MB for pictures, documents and other files
const MAX_VIDEO = 50 * 1024 * 1024;         // 50 MB for video and audio
const MAX_STORAGE = 500 * 1024 * 1024;      // 500 MB of uploads in total
const UPLOAD_WINDOW_MS = 5 * 60 * 1000;
const UPLOAD_MAX_IN_WINDOW = 10;            // uploads per IP per 5 minutes
const PENDING_TTL_MS = 15 * 60 * 1000;      // an uploaded file not yet posted is deleted after this

fs.mkdirSync(DATA_DIR, { recursive: true });
fs.mkdirSync(UPLOAD_DIR, { recursive: true });
fs.mkdirSync(BOX_DIR, { recursive: true });

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

// ---- rooms: the public Lobby plus password-protected boxes ----
const BOX_ID_RE = /^[a-f0-9]{12}$/;
const rooms = new Map();

function newRoom(o) {
  return Object.assign({ messages: [], nextId: 1, pins: [], tokens: new Set(), pass: null }, o);
}

let meta = { lobbyPins: [], boxes: [] };
try {
  const j = JSON.parse(fs.readFileSync(META_FILE, "utf8"));
  if (j && typeof j === "object") {
    meta = { lobbyPins: Array.isArray(j.lobbyPins) ? j.lobbyPins : [], boxes: Array.isArray(j.boxes) ? j.boxes : [] };
  }
} catch (_) { /* first run */ }

rooms.set("lobby", newRoom({ id: "lobby", name: "Lobby", file: LOG_FILE, pins: meta.lobbyPins }));
for (const b of meta.boxes) {
  if (b && BOX_ID_RE.test(b.id) && typeof b.name === "string" && typeof b.salt === "string" && typeof b.hash === "string") {
    rooms.set(b.id, newRoom({
      id: b.id, name: b.name, file: path.join(BOX_DIR, b.id + ".jsonl"),
      pass: { salt: b.salt, hash: b.hash }, pins: Array.isArray(b.pins) ? b.pins : [],
    }));
  }
}

function loadMessages(room) {
  try {
    const lines = fs.readFileSync(room.file, "utf8").split("\n").filter(Boolean);
    for (const line of lines.slice(-KEEP)) {
      try { room.messages.push(JSON.parse(line)); } catch (_) { /* skip bad line */ }
    }
  } catch (_) { /* nothing saved yet */ }
  for (const m of room.messages) {
    if (typeof m.id !== "number") m.id = room.nextId++;
    else if (m.id >= room.nextId) room.nextId = m.id + 1;
    if (m.file) {
      const f = m.file;
      if (f && ID_RE.test(f.id) && EXT_RE.test(f.ext) && typeof f.size === "number") fileIndex.set(f.id, f);
      else delete m.file;
    }
  }
  room.pins = room.pins.filter((id) => room.messages.some((m) => m.id === id));
}
rooms.forEach(loadMessages);

// delete files on disk that no message points to (left over from a crash)
try {
  for (const fn of fs.readdirSync(UPLOAD_DIR)) {
    const id = fn.split(".")[0];
    if (!fileIndex.has(id)) fs.unlink(path.join(UPLOAD_DIR, fn), () => {});
  }
} catch (_) {}

// Rewrite a room's message file safely (write a temp file, then swap it in).
function persistRoom(room) {
  const tmp = room.file + ".tmp";
  const body = room.messages.map((m) => JSON.stringify(m)).join("\n") + (room.messages.length ? "\n" : "");
  fs.writeFileSync(tmp, body);
  fs.renameSync(tmp, room.file);
}

// Save the list of boxes (names, hashed passwords, pins) and the Lobby's pins.
function persistMeta() {
  const boxes = [];
  for (const r of rooms.values()) {
    if (r.id !== "lobby") boxes.push({ id: r.id, name: r.name, salt: r.pass.salt, hash: r.pass.hash, pins: r.pins });
  }
  const tmp = META_FILE + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify({ lobbyPins: rooms.get("lobby").pins, boxes }));
  fs.renameSync(tmp, META_FILE);
}

function saveMessage(room, m) {
  m.id = room.nextId++;
  room.messages.push(m);
  if (room.messages.length > KEEP) {
    // drop the oldest message that is not pinned
    const i = room.messages.findIndex((x) => !room.pins.includes(x.id));
    if (i >= 0) removeFile(room.messages.splice(i, 1)[0].file);
    persistRoom(room);
  } else {
    fs.appendFileSync(room.file, JSON.stringify(m) + "\n");
  }
}

// box passwords are stored as a salted scrypt hash, never as text
function makePass(pw) {
  const salt = crypto.randomBytes(16);
  return { salt: salt.toString("hex"), hash: crypto.scryptSync(pw, salt, 32).toString("hex") };
}
function passMatches(room, pw) {
  const h = crypto.scryptSync(pw, Buffer.from(room.pass.salt, "hex"), 32);
  return crypto.timingSafeEqual(h, Buffer.from(room.pass.hash, "hex"));
}

function roomInfo(room) {
  return { id: room.id, name: room.name, box: room.id !== "lobby", pins: room.pins };
}
function boxList() {
  const list = [];
  for (const r of rooms.values()) if (r.id !== "lobby") list.push({ id: r.id, name: r.name });
  return list;
}

// the part of a message's file info that visitors need
function publicMessage(m) {
  if (!m.file && !m.poll) return m;
  const out = Object.assign({}, m);
  if (m.file) {
    const f = m.file;
    out.file = { id: f.id, name: f.name, kind: f.kind, mime: f.mime, size: f.size };
  }
  // who voted stays on the server; visitors only get the counts
  if (m.poll) out.poll = { options: m.poll.options.map((o) => ({ t: o.t, n: o.n })) };
  return out;
}
const shortHash = (s) => crypto.createHash("sha256").update(String(s)).digest("hex").slice(0, 20);

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

let inFlight = 0;   // bytes of uploads still arriving, counted against the storage limit

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
  if (len > MAX_VIDEO) { req.resume(); return jsonRes(res, 413, { error: "size" }); }
  if (storageUsed() + inFlight + len > MAX_STORAGE) { req.resume(); return jsonRes(res, 507, { error: "storage" }); }
  hits.push(now);
  uploadHits.set(ip, hits);

  let name = "file";
  try { name = cleanFileName(new URL(req.url, "http://x").searchParams.get("name")); } catch (_) {}

  inFlight += len;
  receiveUpload(req, res, name, len)
    .catch(() => { if (!res.headersSent) jsonRes(res, 500, { error: "write" }); })
    .then(() => { inFlight -= len; });
}

// Reads the first few KB to find out what the file really is, then streams the rest
// straight to disk so even a large video never sits in memory.
async function receiveUpload(req, res, name, len) {
  const it = req[Symbol.asyncIterator]();
  let head = Buffer.alloc(0);
  let ended = false;
  try {
    while (head.length < 8192 && !ended) {
      const r = await it.next();
      if (r.done) ended = true; else head = Buffer.concat([head, r.value]);
    }
  } catch (_) { return; }   // the visitor disconnected

  const d = detect(head, name);
  if (!d) { req.resume(); return jsonRes(res, 415, { error: "type" }); }
  const limit = (d.kind === "video" || d.kind === "audio") ? MAX_VIDEO : MAX_UPLOAD;
  if (len > limit) { req.resume(); return jsonRes(res, 413, { error: "size" }); }

  const id = crypto.randomBytes(12).toString("hex");
  const meta = { id, name, kind: d.kind, mime: d.mime, ext: d.ext, size: 0 };
  const fp = filePath(meta);
  const out = fs.createWriteStream(fp);
  let written = 0;
  try {
    if (!out.write(head)) await once(out, "drain");
    written += head.length;
    while (!ended) {
      const r = await it.next();
      if (r.done) break;
      written += r.value.length;
      if (written > limit) throw new Error("too big");
      if (!out.write(r.value)) await once(out, "drain");
    }
    out.end();
    await once(out, "finish");
  } catch (_) {
    out.destroy();
    fs.unlink(fp, () => {});
    if (!res.headersSent && !req.destroyed) jsonRes(res, 400, { error: "aborted" });
    return;
  }
  meta.size = written;
  fileIndex.set(id, meta);
  pending.set(id, Date.now());
  jsonRes(res, 200, { id, name, kind: d.kind, mime: d.mime, size: written });
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
const loginFails = new Map();    // ip -> timestamps of wrong admin passwords
const joinFails = new Map();     // ip -> timestamps of wrong box passwords

function recent(map, ip, now) {
  const fresh = (map.get(ip) || []).filter((t) => now - t < LOGIN_FAIL_WINDOW_MS);
  if (fresh.length) map.set(ip, fresh); else map.delete(ip);
  return fresh;
}

// ---- websocket ----
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 4096 });
const connsPerIp = new Map();
const ipHits = new Map();

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function broadcastRoom(room, obj) {
  const data = JSON.stringify(obj);
  for (const c of wss.clients) {
    if (c.readyState === 1 && c.roomId === room.id) c.send(data);
  }
}
function broadcastAll(obj) {
  const data = JSON.stringify(obj);
  for (const c of wss.clients) if (c.readyState === 1) c.send(data);
}
function broadcastOnline() {
  const counts = {};
  for (const c of wss.clients) counts[c.roomId] = (counts[c.roomId] || 0) + 1;
  for (const c of wss.clients) send(c, { type: "online", n: counts[c.roomId] || 0 });
}
function getRoom(ws) {
  return rooms.get(ws.roomId) || rooms.get("lobby");
}
// put a visitor in a room and give them its history
function joinRoom(ws, room) {
  ws.roomId = room.id;
  send(ws, { type: "history", room: roomInfo(room), messages: room.messages.map(publicMessage) });
  broadcastOnline();
}

function cleanName(s) {
  return String(s || "").replace(/[\u0000-\u001f\u007f]/g, "").replace(/\s+/g, " ").trim();
}
function cleanText(s) {
  return String(s || "").replace(/[\u0000-\u0009\u000b-\u001f\u007f]/g, "").trim();
}
function boxNameError(name, exceptId) {
  if (name.length < 2 || name.length > BOX_NAME_MAX) return "name";
  const low = name.toLowerCase();
  if (low === "lobby") return "taken";
  for (const r of rooms.values()) if (r.id !== exceptId && r.name.toLowerCase() === low) return "taken";
  return null;
}
function passError(pw) {
  return (typeof pw !== "string" || pw.length < BOX_PASS_MIN || pw.length > BOX_PASS_MAX) ? "password" : null;
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
  ws.adminEntered = false;   // true while in a box only because they are the admin
  ws.lastSent = 0;
  ws.roomId = "lobby";
  ws.on("pong", () => { ws.isAlive = true; });

  send(ws, { type: "boxes", boxes: boxList() });
  joinRoom(ws, rooms.get("lobby"));

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (_) { return; }
    if (!msg || typeof msg.type !== "string") return;
    const now = Date.now();

    // ----- admin: log in, resume, log out -----
    if (msg.type === "login") {
      if (!ADMIN_PASSWORD) return send(ws, { type: "admin", ok: false, reason: "disabled" });
      if (recent(loginFails, ip, now).length >= LOGIN_MAX_FAILS) {
        return send(ws, { type: "admin", ok: false, reason: "locked" });
      }
      if (typeof msg.password === "string" && passwordOk(msg.password)) {
        const token = crypto.randomBytes(32).toString("hex");
        adminTokens.add(token);
        if (adminTokens.size > MAX_ADMIN_TOKENS) adminTokens.delete(adminTokens.values().next().value);
        ws.isAdmin = true;
        return send(ws, { type: "admin", ok: true, token });
      }
      const fails = recent(loginFails, ip, now);
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
      send(ws, { type: "admin", ok: false, loggedOut: true });
      if (ws.adminEntered) {          // they only got into this box as admin
        ws.adminEntered = false;
        send(ws, { type: "kicked", reason: "logout" });
        joinRoom(ws, rooms.get("lobby"));
      }
      return;
    }

    // ----- everyone: enter a room -----
    if (msg.type === "join") {
      const target = typeof msg.box === "string" ? rooms.get(msg.box) : null;
      if (!target) return send(ws, { type: "joinerror", box: msg.box, reason: "missing" });
      if (target.id === "lobby") {
        ws.adminEntered = false;
        send(ws, { type: "joined", box: "lobby" });
        return joinRoom(ws, target);
      }
      if (ws.isAdmin) {               // the admin can enter any box without its password
        ws.adminEntered = true;
        send(ws, { type: "joined", box: target.id });
        return joinRoom(ws, target);
      }
      if (typeof msg.token === "string" && target.tokens.has(msg.token)) {
        ws.adminEntered = false;
        send(ws, { type: "joined", box: target.id, token: msg.token });
        return joinRoom(ws, target);
      }
      if (typeof msg.password !== "string") return send(ws, { type: "joinerror", box: target.id, reason: "expired" });
      if (recent(joinFails, ip, now).length >= JOIN_MAX_FAILS) {
        return send(ws, { type: "joinerror", box: target.id, reason: "locked" });
      }
      if (msg.password.length <= BOX_PASS_MAX && passMatches(target, msg.password)) {
        const token = crypto.randomBytes(24).toString("hex");
        target.tokens.add(token);
        if (target.tokens.size > MAX_BOX_TOKENS) target.tokens.delete(target.tokens.values().next().value);
        ws.adminEntered = false;
        send(ws, { type: "joined", box: target.id, token });
        return joinRoom(ws, target);
      }
      const fails = recent(joinFails, ip, now);
      fails.push(now);
      joinFails.set(ip, fails);
      return send(ws, { type: "joinerror", box: target.id, reason: "wrong" });
    }

    // ----- admin-only: boxes, pins, moderation -----
    const ADMIN_ONLY = ["clear", "edit", "delete", "createBox", "renameBox", "setBoxPassword", "deleteBox", "pin", "unpin", "createPoll"];
    if (ADMIN_ONLY.includes(msg.type)) {
      if (!ws.isAdmin) return send(ws, { type: "error", reason: "forbidden" });
      const room = getRoom(ws);

      if (msg.type === "createBox") {
        const name = cleanName(msg.name);
        const err = boxNameError(name) || passError(msg.password);
        if (err) return send(ws, { type: "boxError", reason: err });
        if (rooms.size - 1 >= MAX_BOXES) return send(ws, { type: "boxError", reason: "limit" });
        const id = crypto.randomBytes(6).toString("hex");
        rooms.set(id, newRoom({ id, name, file: path.join(BOX_DIR, id + ".jsonl"), pass: makePass(msg.password) }));
        persistMeta();
        broadcastAll({ type: "boxes", boxes: boxList() });
        return send(ws, { type: "boxCreated", id });
      }

      if (msg.type === "renameBox") {
        if (room.id === "lobby") return send(ws, { type: "boxError", reason: "lobby" });
        const name = cleanName(msg.name);
        const err = boxNameError(name, room.id);
        if (err) return send(ws, { type: "boxError", reason: err });
        room.name = name;
        persistMeta();
        broadcastAll({ type: "boxes", boxes: boxList() });
        broadcastRoom(room, { type: "roominfo", room: roomInfo(room) });
        return send(ws, { type: "boxUpdated", what: "name" });
      }

      if (msg.type === "setBoxPassword") {
        if (room.id === "lobby") return send(ws, { type: "boxError", reason: "lobby" });
        const err = passError(msg.password);
        if (err) return send(ws, { type: "boxError", reason: err });
        room.pass = makePass(msg.password);
        room.tokens.clear();
        persistMeta();
        // everyone who is not the admin has to leave and come back with the new password
        for (const c of wss.clients) {
          if (c.roomId === room.id && !c.isAdmin) {
            send(c, { type: "kicked", reason: "password" });
            joinRoom(c, rooms.get("lobby"));
          }
        }
        return send(ws, { type: "boxUpdated", what: "password" });
      }

      if (msg.type === "deleteBox") {
        if (room.id === "lobby") return send(ws, { type: "boxError", reason: "lobby" });
        room.messages.forEach((m) => removeFile(m.file));
        fs.unlink(room.file, () => {});
        rooms.delete(room.id);
        persistMeta();
        for (const c of wss.clients) {
          if (c.roomId === room.id) {
            c.adminEntered = false;
            send(c, { type: "kicked", reason: "deleted" });
            joinRoom(c, rooms.get("lobby"));
          }
        }
        return broadcastAll({ type: "boxes", boxes: boxList() });
      }

      if (msg.type === "createPoll") {
        const name = cleanName(msg.name);
        const question = cleanText(msg.question);
        const raw = Array.isArray(msg.options) ? msg.options : [];
        const options = [];
        for (const o of raw) {
          const t = cleanName(o);
          if (t && !options.some((x) => x.t.toLowerCase() === t.toLowerCase())) options.push({ t, n: 0 });
        }
        if (name.length < 2 || name.length > MAX_NAME) return send(ws, { type: "error", reason: "name" });
        if (!question || question.length > POLL_Q_MAX) return send(ws, { type: "pollError", reason: "question" });
        if (options.length < POLL_MIN_OPTIONS || options.length > POLL_MAX_OPTIONS || options.some((o) => o.t.length > POLL_OPT_MAX)) {
          return send(ws, { type: "pollError", reason: "options" });
        }
        const m = { name, text: question, ts: now, poll: { options, voters: [], ips: [] } };
        saveMessage(room, m);
        return broadcastRoom(room, { type: "msg", message: publicMessage(m) });
      }

      if (msg.type === "pin" || msg.type === "unpin") {
        const id = Number(msg.id);
        if (!room.messages.some((m) => m.id === id)) return send(ws, { type: "error", reason: "missing" });
        if (msg.type === "pin") {
          if (!room.pins.includes(id)) {
            if (room.pins.length >= MAX_PINS) return send(ws, { type: "error", reason: "pinlimit" });
            room.pins.push(id);
          }
        } else {
          room.pins = room.pins.filter((x) => x !== id);
        }
        persistMeta();
        return broadcastRoom(room, { type: "pins", pins: room.pins });
      }

      if (msg.type === "clear") {
        room.messages.forEach((m) => removeFile(m.file));
        room.messages = [];
        room.pins = [];
        persistRoom(room);
        persistMeta();
        return broadcastRoom(room, { type: "clear" });
      }

      const target = room.messages.find((m) => m.id === msg.id);
      if (!target) return send(ws, { type: "error", reason: "missing" });

      if (msg.type === "delete") {
        removeFile(target.file);
        room.messages = room.messages.filter((m) => m.id !== msg.id);
        persistRoom(room);
        broadcastRoom(room, { type: "delete", id: msg.id });
        if (room.pins.includes(msg.id)) {
          room.pins = room.pins.filter((x) => x !== msg.id);
          persistMeta();
          broadcastRoom(room, { type: "pins", pins: room.pins });
        }
        return;
      }

      // edit
      const text = cleanText(msg.text);
      if ((!text && !target.file) || text.length > MAX_TEXT) return send(ws, { type: "error", reason: "text" });
      target.text = text;
      target.edited = true;
      persistRoom(room);
      return broadcastRoom(room, { type: "edit", id: target.id, text });
    }

    // ----- everyone: vote in a poll (one vote per browser and per address) -----
    if (msg.type === "vote") {
      const room = getRoom(ws);
      const target = room.messages.find((m) => m.id === msg.id);
      if (!target || !target.poll) return send(ws, { type: "error", reason: "missing" });
      const opt = Number(msg.option);
      const voter = typeof msg.voter === "string" ? msg.voter : "";
      if (!Number.isInteger(opt) || opt < 0 || opt >= target.poll.options.length || !/^[A-Za-z0-9_-]{8,64}$/.test(voter)) {
        return send(ws, { type: "error", reason: "text" });
      }
      const vh = shortHash("v:" + voter), ih = shortHash("i:" + ip);
      if (target.poll.voters.includes(vh) || target.poll.ips.includes(ih)) {
        return send(ws, { type: "voted", id: target.id, already: true });
      }
      target.poll.voters.push(vh);
      target.poll.ips.push(ih);
      target.poll.options[opt].n++;
      persistRoom(room);
      send(ws, { type: "voted", id: target.id, option: opt });
      return broadcastRoom(room, { type: "poll", id: target.id, counts: target.poll.options.map((o) => o.n) });
    }

    // ----- everyone: send a chat message -----
    if (msg.type !== "msg") return;
    const room = getRoom(ws);

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
    saveMessage(room, m);
    broadcastRoom(room, { type: "msg", message: publicMessage(m) });
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
  for (const ip of [...loginFails.keys()]) recent(loginFails, ip, now);
  for (const ip of [...joinFails.keys()]) recent(joinFails, ip, now);
  for (const [id, ts] of pending) {
    if (now - ts > PENDING_TTL_MS) removeFile(fileIndex.get(id));
  }
}, 30000);

server.listen(PORT, () => {
  console.log("Open Lobby listening on port " + PORT + (ADMIN_PASSWORD ? " (admin enabled)" : " (admin disabled: no ADMIN_PASSWORD set)") + ", " + (rooms.size - 1) + " box(es)");
});

function shutdown() { process.exit(0); }
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
