/**
 * jsonl + zstd run log. Node snapshots coalesce. Caps fail closed.
 */
import fs from "fs";
import path from "path";
import { spawn, spawnSync } from "child_process";

function dict() {
  return Object.create(null);
}

const INT_KEYS = [
  "per_run_bytes",
  "total_bytes",
  "min_free_bytes",
  "queue_events",
  "rss_bytes",
  "coalesce_node_ms",
];

export function parseRecordingYaml(text) {
  const out = dict();
  for (const raw of String(text).split("\n")) {
    const line = raw.replace(/#.*$/, "").trim();
    if (!line) continue;
    const m = line.match(/^([A-Za-z0-9_]+):\s*(.+)$/);
    if (!m) throw new Error(`malformed recording yaml: ${raw}`);
    const k = m[1];
    const v = m[2].trim().replace(/^["']|["']$/g, "");
    if (INT_KEYS.includes(k)) {
      if (!/^[1-9][0-9]*$/.test(v)) throw new Error(`recording.yaml ${k} must be a positive integer`);
      const n = Number(v);
      if (!Number.isSafeInteger(n) || n <= 0) throw new Error(`recording.yaml ${k} must be a positive integer`);
      out[k] = n;
    } else {
      out[k] = v;
    }
  }
  for (const k of ["dir", "zstd", ...INT_KEYS]) {
    if (out[k] == null) throw new Error(`recording.yaml missing ${k}`);
  }
  return out;
}

export function loadRecordingConfig(filePath) {
  const p = filePath || "/app/config/recording.yaml";
  let text;
  try {
    text = fs.readFileSync(p, "utf8");
  } catch (err) {
    throw new Error(`recording.yaml unreadable: ${p}: ${err.message}`);
  }
  return parseRecordingYaml(text);
}

function dirSize(root) {
  let n = 0;
  if (!fs.existsSync(root)) return 0;
  for (const name of fs.readdirSync(root)) {
    const p = path.join(root, name);
    const st = fs.statSync(p);
    if (st.isDirectory()) n += dirSize(p);
    else n += st.size;
  }
  return n;
}

function runZstd(bin, args) {
  return new Promise((resolve, reject) => {
    const p = spawn(bin, args, { stdio: ["ignore", "pipe", "pipe"] });
    const chunks = [];
    p.stdout.on("data", (b) => chunks.push(b));
    let err = "";
    p.stderr.on("data", (b) => {
      err += b;
    });
    p.on("error", reject);
    p.on("close", (code) => {
      if (code === 0) resolve(Buffer.concat(chunks));
      else reject(new Error(`zstd exit ${code} ${err}`));
    });
  });
}

export function createRecorder(options = {}) {
  const cfg = options.config || loadRecordingConfig(options.configPath);
  const nowFn = options.now || (() => Date.now());
  const rssFn = options.rss || (() => process.memoryUsage().rss);
  const freeFn =
    options.freeDisk ||
    (() => {
      const s = fs.statfsSync(cfg.dir);
      return Number(s.bavail) * Number(s.bsize);
    });
  const compress =
    options.compress ||
    (async (src, dest) => {
      const buf = await runZstd(cfg.zstd, ["-f", "-q", "-c", src]);
      fs.writeFileSync(dest, buf);
    });
  const decompress =
    options.decompress ||
    (async (src) => {
      const buf = await runZstd(cfg.zstd, ["-d", "-q", "-c", src]);
      return buf.toString("utf8");
    });
  fs.mkdirSync(cfg.dir, { recursive: true });

  const runs = dict();
  const pendingNodes = dict();
  let lastFlush = nowFn();
  let reserved = 0;

  function safeId(run_id) {
    if (typeof run_id !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(run_id)) {
      throw new Error("bad run_id");
    }
    if (run_id === "." || run_id === "..") throw new Error("bad run_id");
    const root = path.resolve(cfg.dir);
    const p = path.resolve(root, run_id);
    const rel = path.relative(root, p);
    if (!rel || rel.startsWith("..") || path.isAbsolute(rel)) throw new Error("bad run_id");
    return p;
  }

  function metaPath(id) {
    return path.join(safeId(id), "run.json");
  }
  function jsonlPath(id) {
    return path.join(safeId(id), "events.jsonl");
  }

  function readMeta(id) {
    return JSON.parse(fs.readFileSync(metaPath(id), "utf8"));
  }

  function writeMeta(id, meta) {
    fs.writeFileSync(metaPath(id), JSON.stringify(meta) + "\n");
  }

  function evictUntil(need) {
    const dirs = listRuns()
      .filter((m) => m.status === "final" && !runs[m.run_id])
      .map((m) => {
        try {
          return { id: m.run_id, meta: m, mtime: fs.statSync(path.join(cfg.dir, m.run_id)).mtimeMs };
        } catch {
          return null;
        }
      })
      .filter(Boolean)
      .sort((a, b) => a.mtime - b.mtime);
    let total = dirSize(cfg.dir);
    for (const d of dirs) {
      if (total + need <= cfg.total_bytes) break;
      fs.rmSync(path.join(cfg.dir, d.id), { recursive: true, force: true });
      total = dirSize(cfg.dir);
    }
    return total;
  }

  function assertRoom(addBytes) {
    const rss = rssFn();
    if (!(rss <= cfg.rss_bytes)) throw new Error(`hub rss ${rss} exceeds ${cfg.rss_bytes}`);
    const free = freeFn();
    if (!(free - addBytes - reserved >= cfg.min_free_bytes)) {
      throw new Error(`free disk ${free} below reserve after write`);
    }
    let total = dirSize(cfg.dir) + reserved;
    if (total + addBytes > cfg.total_bytes) total = evictUntil(addBytes) + reserved;
    if (total + addBytes > cfg.total_bytes) throw new Error("recording total cap exceeded");
  }

  function hasRun(run_id) {
    try {
      const p = jsonlPath(run_id);
      return fs.existsSync(metaPath(run_id)) || fs.existsSync(p) || fs.existsSync(p + ".zst");
    } catch {
      return false;
    }
  }

  function openRun(run_id, hash) {
    if (hasRun(run_id)) throw new Error(`run exists ${run_id}`);
    assertRoom(4096);
    const dir = safeId(run_id);
    fs.mkdirSync(dir, { recursive: true });
    const meta = {
      run_id,
      status: "incomplete",
      schema_bundle_hash: hash,
      started_at_ms: nowFn(),
      bytes: 0,
    };
    try {
      writeMeta(run_id, meta);
    } catch (err) {
      try {
        fs.rmSync(dir, { recursive: true, force: true });
      } catch {
        /* ignore */
      }
      throw err;
    }
    runs[run_id] = { bytes: 0, count: 0, hash, closing: false };
    return meta;
  }

  function append(run_id, event) {
    const st = runs[run_id];
    if (!st || st.closing) throw new Error(`no open run ${run_id}`);
    const line = JSON.stringify(event) + "\n";
    const add = Buffer.byteLength(line) + 512;
    if (st.bytes + add > cfg.per_run_bytes) throw new Error("per-run cap exceeded");
    assertRoom(add);
    fs.appendFileSync(jsonlPath(run_id), line);
    st.bytes += add;
    st.count += 1;
    try {
      const meta = readMeta(run_id);
      meta.bytes = st.bytes;
      writeMeta(run_id, meta);
    } catch {
      /* jsonl is the commit; manifest is advisory */
    }
  }

  function markClosing(run_id) {
    if (runs[run_id]) runs[run_id].closing = true;
  }

  async function finish(run_id) {
    markClosing(run_id);
    const src = jsonlPath(run_id);
    const dest = src + ".zst";
    try {
      const extra = fs.existsSync(src) ? fs.statSync(src).size : 0;
      assertRoom(extra);
      reserved += extra;
      try {
        if (fs.existsSync(src)) await compress(src, dest);
        const meta = readMeta(run_id);
        meta.status = "final";
        meta.finished_at_ms = nowFn();
        writeMeta(run_id, meta);
        if (fs.existsSync(dest) && fs.existsSync(src)) fs.unlinkSync(src);
      } finally {
        reserved = Math.max(0, reserved - extra);
      }
    } finally {
      delete runs[run_id];
    }
  }

  function parseJsonlText(text, rewritePath) {
    const ended = text.endsWith("\n");
    const parts = text.split("\n");
    if (parts.length && parts[parts.length - 1] === "") parts.pop();
    const good = [];
    let last = null;
    for (let i = 0; i < parts.length; i++) {
      const line = parts[i];
      const isTail = i === parts.length - 1 && !ended;
      try {
        last = JSON.parse(line);
        good.push(line);
      } catch {
        break;
      }
      if (isTail) break;
    }
    const rewritten = good.length ? good.join("\n") + "\n" : "";
    if (rewritePath && rewritten !== text) fs.writeFileSync(rewritePath, rewritten);
    return {
      order: last?.hub_order ?? -1,
      finished: last?.type === "run.finished",
      bytes: Buffer.byteLength(rewritten),
      events: good.map((l) => JSON.parse(l)),
    };
  }

  function decompressSync(src) {
    if (options.decompressSync) return options.decompressSync(src);
    const r = spawnSync(cfg.zstd, ["-d", "-q", "-c", src], {
      encoding: "utf8",
      maxBuffer: 64 * 1024 * 1024,
    });
    if (r.status !== 0 || r.error) return null;
    return r.stdout || "";
  }

  function lastCommitted(run_id) {
    const p = jsonlPath(run_id);
    const z = p + ".zst";
    if (fs.existsSync(p)) return parseJsonlText(fs.readFileSync(p, "utf8"), p);
    if (fs.existsSync(z)) {
      const text = decompressSync(z);
      if (text == null) {
        return {
          order: -1,
          finished: true,
          bytes: fs.statSync(z).size,
          events: [],
          unreadable: true,
        };
      }
      return parseJsonlText(text, null);
    }
    return { order: -1, finished: false, bytes: 0, events: [] };
  }

  function listRuns() {
    if (!fs.existsSync(cfg.dir)) return [];
    const out = [];
    for (const id of fs.readdirSync(cfg.dir)) {
      try {
        if (!fs.statSync(path.join(cfg.dir, id)).isDirectory()) continue;
        safeId(id);
        let meta = null;
        try {
          meta = readMeta(id);
        } catch {
          meta = null;
        }
        const committed = lastCommitted(id);
        out.push({
          run_id: id,
          status: committed.unreadable || committed.finished || meta?.status === "final" ? "final" : "incomplete",
          schema_bundle_hash: meta?.schema_bundle_hash || null,
          bytes: committed.bytes || 0,
        });
      } catch {
        /* skip */
      }
    }
    return out;
  }

  async function readEvents(run_id) {
    const p = jsonlPath(run_id);
    const z = p + ".zst";
    if (fs.existsSync(p)) {
      const committed = parseJsonlText(fs.readFileSync(p, "utf8"), p);
      if (committed.events.length) return committed.events;
    }
    if (fs.existsSync(z)) {
      const text = await decompress(z);
      return parseJsonlText(text, null).events;
    }
    return [];
  }

  function abortRun(run_id) {
    delete runs[run_id];
    try {
      fs.rmSync(safeId(run_id), { recursive: true, force: true });
    } catch {
      /* ignore */
    }
  }

  function adoptIncomplete() {
    for (const meta of listRuns()) {
      try {
        safeId(meta.run_id);
      } catch {
        continue;
      }
      const committed = lastCommitted(meta.run_id);
      if (committed.unreadable) continue;
      if (committed.finished || meta.status === "final") {
        if (meta.status !== "final") {
          try {
            const m = readMeta(meta.run_id);
            m.status = "final";
            writeMeta(meta.run_id, m);
          } catch {
            /* leave on disk */
          }
        }
        continue;
      }
      if (meta.status !== "incomplete") continue;
      runs[meta.run_id] = {
        bytes: committed.bytes || 0,
        count: 0,
        hash: meta.schema_bundle_hash,
        closing: false,
      };
    }
  }
  adoptIncomplete();

  return {
    cfg,
    openRun,
    append,
    finish,
    markClosing,
    listRuns,
    readEvents,
    lastCommitted,
    abortRun,
    hasRun,
    isOpen: (id) => Boolean(runs[id]) && !runs[id].closing,
    isClosing: (id) => Boolean(runs[id]?.closing),
    activeIds: () => Object.keys(runs).filter((id) => !runs[id].closing),
    holdNode(run_id, node, payload) {
      const keys = Object.keys(pendingNodes).filter((k) => k.startsWith(`${run_id}:`));
      if (keys.length >= cfg.queue_events && !pendingNodes[`${run_id}:${node}`]) {
        delete pendingNodes[keys[0]];
      }
      pendingNodes[`${run_id}:${node}`] = payload;
    },
    takeNodes(run_id, force = false) {
      const t = nowFn();
      if (!force && t - lastFlush < cfg.coalesce_node_ms) return [];
      lastFlush = t;
      const out = [];
      for (const [k, payload] of Object.entries(pendingNodes)) {
        if (k.startsWith(`${run_id}:`)) {
          out.push(payload);
          delete pendingNodes[k];
        }
      }
      return out;
    },
  };
}
