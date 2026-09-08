/**
 * Parse node-exporter / DCGM text into a NodeSnapshot metrics map.
 * Used by the hub-side fallback. The Spark pusher is a Python twin.
 */

const MEM_KEYS = ["memory.total.bytes", "memory.available.bytes", "system.cpu.util.pct"];
const GPU_KEYS = ["gpu.util.pct", "gpu.power.w", "gpu.temp.c", "gpu.clock.sm.mhz"];

export function metric(value, unit, source, quality, age_ms) {
  if (quality === "unavailable" || value == null) {
    return { value: null, unit, source, quality: "unavailable", age_ms };
  }
  return { value, unit, source, quality, age_ms };
}

function unavailable(unit, source, age_ms) {
  return { value: null, unit, source, quality: "unavailable", age_ms };
}

/** Last whitespace-separated number is the value; optional Prometheus timestamp before it. */
export function parsePromLineValue(line) {
  const parts = String(line).trim().split(/\s+/);
  if (parts.length < 2) return { value: null, ts: null };
  const last = Number(parts[parts.length - 1]);
  if (!Number.isFinite(last)) return { value: null, ts: null };
  if (parts.length >= 3) {
    const maybeVal = Number(parts[parts.length - 2]);
    if (Number.isFinite(maybeVal)) return { value: maybeVal, ts: last };
  }
  return { value: last, ts: null };
}

export function parsePromValue(text, prefix) {
  if (!text) return { value: null, ts: null };
  for (const line of String(text).split("\n")) {
    if (!line || line.startsWith("#")) continue;
    if (line.startsWith(`${prefix} `) || line.startsWith(`${prefix}{`)) {
      return parsePromLineValue(line);
    }
  }
  return { value: null, ts: null };
}

export function cpuCounters(text) {
  let idle = 0;
  let total = 0;
  let found = false;
  if (!text) return null;
  for (const line of String(text).split("\n")) {
    if (!line.startsWith("node_cpu_seconds_total")) continue;
    const { value } = parsePromLineValue(line);
    if (value == null) continue;
    found = true;
    total += value;
    if (line.includes('mode="idle"') || line.includes("mode=idle")) idle += value;
  }
  return found ? { idle, total } : null;
}

/**
 * Two-sample CPU util. First sample or non-increasing totals → null (caller emits unavailable).
 * `state` is a mutable `{ idle, total }`.
 */
export function cpuUtilFromCounters(state, counters) {
  if (!counters || !(counters.total > 0)) return null;
  const prev = state.prev;
  state.prev = { idle: counters.idle, total: counters.total };
  if (!prev) return null;
  const dTotal = counters.total - prev.total;
  const dIdle = counters.idle - prev.idle;
  if (!(dTotal > 0) || dIdle < 0 || dIdle > dTotal) return null;
  return (1 - dIdle / dTotal) * 100;
}

function ageMs(collectedMono, stampMono, promTs, nowMs) {
  const collect = Math.max(0, (stampMono - collectedMono) * 1000);
  if (promTs == null || !Number.isFinite(promTs)) return collect;
  const tsMs = promTs > 1e12 ? promTs : promTs * 1000;
  return Math.max(collect, nowMs - tsMs);
}

export function metricsFromExporters({
  nodeText,
  dcgmText,
  nodeOk,
  dcgmOk,
  nodeAgeMs,
  dcgmAgeMs,
  cpuState,
  nodeSource = "node-exporter",
  dcgmSource = "dcgm",
  nowMs = Date.now(),
  stampMono = 0,
  nodeCollectedMono = 0,
  dcgmCollectedMono = 0,
}) {
  const metrics = Object.create(null);
  const nAge = nodeAgeMs != null ? nodeAgeMs : ageMs(nodeCollectedMono, stampMono, null, nowMs);
  const dAge = dcgmAgeMs != null ? dcgmAgeMs : ageMs(dcgmCollectedMono, stampMono, null, nowMs);

  if (!nodeOk) {
    metrics["memory.total.bytes"] = unavailable("bytes", nodeSource, nAge);
    metrics["memory.available.bytes"] = unavailable("bytes", nodeSource, nAge);
    metrics["system.cpu.util.pct"] = unavailable("pct", nodeSource, nAge);
  } else {
    const memT = parsePromValue(nodeText, "node_memory_MemTotal_bytes");
    const memA = parsePromValue(nodeText, "node_memory_MemAvailable_bytes");
    const tAge = ageMs(nodeCollectedMono, stampMono, memT.ts, nowMs) || nAge;
    const aAge = ageMs(nodeCollectedMono, stampMono, memA.ts, nowMs) || nAge;
    metrics["memory.total.bytes"] =
      memT.value != null
        ? metric(memT.value, "bytes", nodeSource, "measured", tAge)
        : unavailable("bytes", nodeSource, nAge);
    metrics["memory.available.bytes"] =
      memA.value != null
        ? metric(memA.value, "bytes", nodeSource, "measured", aAge)
        : unavailable("bytes", nodeSource, nAge);
    const util = cpuUtilFromCounters(cpuState || {}, cpuCounters(nodeText));
    metrics["system.cpu.util.pct"] =
      util != null
        ? metric(util, "pct", "derived", "derived", nAge)
        : unavailable("pct", "derived", nAge);
  }

  const gpuSeries = [
    ["gpu.util.pct", "DCGM_FI_DEV_GPU_UTIL", "pct"],
    ["gpu.power.w", "DCGM_FI_DEV_POWER_USAGE", "w"],
    ["gpu.temp.c", "DCGM_FI_DEV_GPU_TEMP", "c"],
    ["gpu.clock.sm.mhz", "DCGM_FI_DEV_SM_CLOCK", "mhz"],
  ];
  if (!dcgmOk) {
    for (const [key, , unit] of gpuSeries) {
      metrics[key] = unavailable(unit, dcgmSource, dAge);
    }
  } else {
    for (const [key, prefix, unit] of gpuSeries) {
      const s = parsePromValue(dcgmText, prefix);
      const a = ageMs(dcgmCollectedMono, stampMono, s.ts, nowMs) || dAge;
      metrics[key] =
        s.value != null ? metric(s.value, unit, dcgmSource, "measured", a) : unavailable(unit, dcgmSource, dAge);
    }
  }

  return metrics;
}

export function snapshotQuality(metrics, { nodeOk, dcgmOk, fallback = false }) {
  if (fallback) return "degraded";
  const measured = Object.values(metrics).some(
    (m) => m && (m.quality === "measured" || m.quality === "derived") && m.value != null
  );
  if (!nodeOk && !dcgmOk) return "unavailable";
  if (!measured) return "unavailable";
  if (!nodeOk || !dcgmOk) return "degraded";
  return "measured";
}

export function sourceAgeMs(metrics) {
  const ages = Object.values(metrics).map((m) => m?.age_ms).filter((n) => Number.isFinite(n));
  return ages.length ? Math.max(...ages) : 0;
}

export function buildNodeSnapshot({
  node,
  producerId,
  seq,
  nodeText,
  dcgmText,
  nodeOk,
  dcgmOk,
  nowNs,
  monoNs,
  cpuState,
  fallback = false,
  nodeSource,
  dcgmSource,
  nodeAgeMs,
  dcgmAgeMs,
  nowMs,
  stampMono,
  nodeCollectedMono,
  dcgmCollectedMono,
}) {
  const metrics = metricsFromExporters({
    nodeText,
    dcgmText,
    nodeOk,
    dcgmOk,
    nodeAgeMs,
    dcgmAgeMs,
    cpuState,
    nodeSource: fallback ? "exporter-http" : nodeSource || "node-exporter",
    dcgmSource: fallback ? "exporter-http" : dcgmSource || "dcgm",
    nowMs,
    stampMono,
    nodeCollectedMono,
    dcgmCollectedMono,
  });
  const quality = snapshotQuality(metrics, { nodeOk, dcgmOk, fallback });
  return {
    contract: "dgx.node-snapshot.v1",
    producer_id: producerId,
    seq,
    run_id: null,
    node,
    observed_at_ns: nowNs,
    monotonic_ns: monoNs,
    source_age_ms: sourceAgeMs(metrics),
    quality,
    capabilities: ["node.steady"],
    metrics,
  };
}

export { MEM_KEYS, GPU_KEYS };
