/**
 * Collect-once hub: inbound NodeSnapshots, leases, Prometheus rollups.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Ajv from "ajv/dist/2020.js";

function dict() {
  return Object.create(null);
}

export const STALE_MS = 5000;
const SCHEMA_PATH = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "contracts/node-snapshot-v1.schema.json"
);

export const ROLLUP_METRIC_NAMES = new Set([
  "gpu.util.pct",
  "gpu.power.w",
  "gpu.temp.c",
  "gpu.clock.sm.mhz",
  "system.power.w",
  "system.cpu.util.pct",
  "memory.total.bytes",
  "memory.available.bytes",
  "fabric.roce.rx.bytes_total",
  "fabric.roce.tx.bytes_total",
  "vm.swap_in.pages_total",
  "vm.major_faults.total",
  "storage.nvme.read.bytes_total",
]);

const ajv = new Ajv({
  allErrors: true,
  strict: false,
  validateFormats: false,
});
const nodeSchema = JSON.parse(fs.readFileSync(SCHEMA_PATH, "utf8"));
const validateNodeSnapshot = ajv.compile(nodeSchema);

export function snapshotErrors(snap) {
  if (validateNodeSnapshot(snap)) return null;
  return (validateNodeSnapshot.errors || []).map((e) => `${e.instancePath || "/"} ${e.message}`);
}

export function parseRollupYaml(text) {
  const blocks = dict();
  const burst = [];
  let section = null;
  let cur = null;
  const lines = String(text).split("\n");
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    const stripped = raw.replace(/#.*$/, "");
    if (!stripped.trim()) continue;
    if (stripped === "blocks:") {
      section = "blocks";
      cur = null;
      continue;
    }
    if (stripped === "burst:") {
      section = "burst";
      cur = null;
      continue;
    }
    const block = stripped.match(/^  ([A-Za-z0-9_]+):\s*$/);
    if (block && section === "blocks") {
      cur = block[1];
      blocks[cur] = [];
      continue;
    }
    const nested = stripped.match(/^    - ([A-Za-z][A-Za-z0-9._]*)\s*$/);
    if (nested && section === "blocks" && cur) {
      if (!ROLLUP_METRIC_NAMES.has(nested[1])) {
        throw new Error(`unknown rollup metric ${nested[1]} at line ${i + 1}`);
      }
      blocks[cur].push(nested[1]);
      continue;
    }
    const burstItem = stripped.match(/^  - ([A-Za-z][A-Za-z0-9._]*)\s*$/);
    if (burstItem && section === "burst") {
      if (!ROLLUP_METRIC_NAMES.has(burstItem[1])) {
        throw new Error(`unknown burst metric ${burstItem[1]} at line ${i + 1}`);
      }
      burst.push(burstItem[1]);
      continue;
    }
    throw new Error(`malformed rollup yaml at line ${i + 1}: ${raw}`);
  }
  if (Object.keys(blocks).length === 0) {
    throw new Error("rollups.yaml has no blocks");
  }
  return { blocks, burst };
}

export function loadRollupBlocks(filePath) {
  const p = filePath || "/app/config/rollups.yaml";
  let text;
  try {
    text = fs.readFileSync(p, "utf8");
  } catch (err) {
    throw new Error(`rollups.yaml unreadable: ${p}: ${err.message}`);
  }
  return parseRollupYaml(text);
}

function mib(bytes) {
  if (bytes == null) return null;
  return Math.round(bytes / (1024 * 1024));
}

export function metricIsLive(met, nowMs, receivedAtMs, observedAtNs, staleMs = STALE_MS) {
  if (!met || met.quality === "unavailable" || met.value == null) return false;
  const receiptAge = receivedAtMs == null ? 0 : nowMs - receivedAtMs;
  const observedAge = observedAtNs == null ? 0 : nowMs - observedAtNs / 1e6;
  const sourceAge = (met.age_ms || 0) + receiptAge;
  if (receiptAge > staleMs) return false;
  if (observedAtNs != null && observedAge > staleMs) return false;
  if (sourceAge > staleMs) return false;
  return true;
}

export function snapshotToPanels(snap, clock = {}) {
  const m = snap.metrics || {};
  const now = clock.now != null ? clock.now : Date.now();
  const receivedAt = clock.receivedAt != null ? clock.receivedAt : now;
  const staleMs = clock.staleMs != null ? clock.staleMs : STALE_MS;
  const n = (k) => {
    const met = m[k];
    if (!metricIsLive(met, now, receivedAt, snap.observed_at_ns, staleMs)) return null;
    return met.value;
  };
  const totalB = n("memory.total.bytes");
  const availB = n("memory.available.bytes");
  const total = mib(totalB);
  const available = mib(availB);
  const used = total != null && available != null ? total - available : null;
  const percentage =
    total != null && total > 0 && used != null ? Math.round((used / total) * 100) : null;
  return {
    gpu: {
      temperature: n("gpu.temp.c"),
      usage: n("gpu.util.pct"),
      power: { draw: n("gpu.power.w"), limit: null, systemDraw: n("system.power.w") },
      clockMhz: n("gpu.clock.sm.mhz"),
    },
    cpu: { usage: n("system.cpu.util.pct"), temperature: null, draw: null, tdp: null },
    unifiedMemory: { total, used, available, percentage },
    quality: snap.quality,
    observed_at_ns: snap.observed_at_ns,
  };
}

export function prometheusText(snapshots, block, extraLabels = {}) {
  const lines = [];
  for (const snap of Object.values(snapshots)) {
    const node = snap.node;
    for (const name of block) {
      const met = snap.metrics?.[name];
      if (!met || met.quality === "unavailable" || met.value == null) continue;
      const metricName = name.replace(/\./g, "_");
      const labels = [`node="${node}"`];
      for (const [k, v] of Object.entries(extraLabels)) labels.push(`${k}="${v}"`);
      lines.push(`# TYPE ${metricName} gauge`);
      lines.push(`${metricName}{${labels.join(",")}} ${met.value}`);
    }
  }
  return lines.join("\n") + (lines.length ? "\n" : "");
}

function requiredString(v) {
  return typeof v === "string" && v.length > 0;
}

function acceptSequence(prevSeq, snap) {
  if (prevSeq == null) return true;
  if (snap.seq > prevSeq) return true;
  if (snap.seq === 0 && prevSeq > 0) return true;
  return false;
}

export function createObserveHub(options = {}) {
  const snapshots = dict();
  const meta = dict();
  const leases = dict();
  const nowFn = options.now || (() => Date.now());
  const staleMs = options.staleMs != null ? options.staleMs : STALE_MS;
  const blockName = options.blockName || "fleet_steady";
  let blocks;
  let burstAllowlist;
  if (options.blocks) {
    blocks = options.blocks;
    burstAllowlist = options.burstAllowlist || [];
  } else {
    const parsed = loadRollupBlocks(options.rollupPath);
    blocks = parsed.blocks;
    burstAllowlist = options.burstAllowlist || parsed.burst;
  }
  const selected = blocks[blockName];
  if (!selected || selected.length === 0) {
    throw new Error(`rollup block ${blockName} missing or empty`);
  }

  function pruneLeases(t) {
    for (const [id, l] of Object.entries(leases)) {
      if (l.exp < t || l.refs <= 0) delete leases[id];
    }
  }

  return {
    ingest(snap) {
      const errors = snapshotErrors(snap);
      if (errors) return `invalid snapshot: ${errors[0]}`;
      const rec = meta[snap.node] || { producers: dict(), received_at: 0 };
      const prevSeq = rec.producers[snap.producer_id];
      if (prevSeq != null && snap.seq === prevSeq) return { duplicate: true };
      if (!acceptSequence(prevSeq, snap)) {
        return `stale sequence producer=${snap.producer_id} seq=${snap.seq}`;
      }
      snapshots[snap.node] = snap;
      rec.producers[snap.producer_id] = snap.seq;
      rec.received_at = nowFn();
      meta[snap.node] = rec;
      return null;
    },
    get(node) {
      return snapshots[node] || null;
    },
    all() {
      return snapshots;
    },
    grantLease({ run_id, holder, ttl_ms, node } = {}) {
      if (!requiredString(run_id) || !requiredString(holder) || !requiredString(node)) {
        return { error: "run_id, holder, and node are required" };
      }
      if (!Number.isFinite(ttl_ms) || ttl_ms <= 0) {
        return { error: "ttl_ms must be a finite positive number" };
      }
      const t = nowFn();
      pruneLeases(t);
      const id = `${run_id}:${holder}:${node}`;
      const existing = leases[id];
      if (existing) {
        existing.refs += 1;
        existing.exp = t + ttl_ms;
        existing.ttl_ms = ttl_ms;
        return { id, refs: existing.refs, exp: existing.exp, burst: burstAllowlist };
      }
      leases[id] = { run_id, holder, node, ttl_ms, exp: t + ttl_ms, refs: 1 };
      return { id, refs: 1, exp: t + ttl_ms, burst: burstAllowlist };
    },
    renewLease({ run_id, holder, ttl_ms, node } = {}) {
      if (!requiredString(run_id) || !requiredString(holder) || !requiredString(node)) {
        return { error: "run_id, holder, and node are required" };
      }
      if (!Number.isFinite(ttl_ms) || ttl_ms <= 0) {
        return { error: "ttl_ms must be a finite positive number" };
      }
      const t = nowFn();
      pruneLeases(t);
      const id = `${run_id}:${holder}:${node}`;
      const existing = leases[id];
      if (!existing) return { error: "lease not found" };
      existing.exp = t + ttl_ms;
      existing.ttl_ms = ttl_ms;
      return { id, refs: existing.refs, exp: existing.exp, burst: burstAllowlist };
    },
    releaseLease({ run_id, holder, node } = {}) {
      if (!requiredString(run_id) || !requiredString(holder) || !requiredString(node)) {
        return "run_id, holder, and node are required";
      }
      const id = `${run_id}:${holder}:${node}`;
      const existing = leases[id];
      if (!existing) return "lease not found";
      existing.refs -= 1;
      if (existing.refs <= 0) delete leases[id];
      return null;
    },
    activeLease(node) {
      const t = nowFn();
      pruneLeases(t);
      for (const l of Object.values(leases)) {
        if (l.node === node) return { ...l, burst: burstAllowlist };
      }
      return null;
    },
    metrics() {
      const t = nowFn();
      const live = dict();
      for (const [node, snap] of Object.entries(snapshots)) {
        const rec = meta[node];
        const receiptAge = rec ? t - rec.received_at : Infinity;
        const observedAge = t - snap.observed_at_ns / 1e6;
        if (receiptAge > staleMs || observedAge > staleMs) continue;
        const metrics = dict();
        const receivedAt = rec ? rec.received_at : t;
        for (const [name, met] of Object.entries(snap.metrics || {})) {
          if (!metricIsLive(met, t, receivedAt, snap.observed_at_ns, staleMs)) continue;
          metrics[name] = met;
        }
        live[node] = { ...snap, metrics };
      }
      return prometheusText(live, selected, { block: blockName });
    },
    snapshotToPanels,
    burstAllowlist,
  };
}

/** HTTP/fallback ingest outcome. apply() runs only on a new accept. */
export function dispatchSnapshot(hub, body, { apply, now } = {}) {
  const result = hub.ingest(body);
  if (typeof result === "string") return { status: 400, body: { error: result } };
  if (result?.duplicate) return { status: 200, body: { ok: true, duplicate: true } };
  const stored = hub.get(body?.node);
  if (apply && stored) {
    const t = now ? now() : Date.now();
    apply(stored, t);
  }
  return { status: 200, body: { ok: true } };
}

let _singleton;
export function getObserveHub() {
  if (!_singleton) _singleton = createObserveHub();
  return _singleton;
}

export function resetObserveHubForTests() {
  _singleton = null;
}
