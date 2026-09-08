import assert from "node:assert/strict";
import { test } from "node:test";
import { runFallbackTick } from "./fallback.js";
import { buildNodeSnapshot } from "./exporters.js";
import { createObserveHub } from "./hub.js";

const NODE_TXT = `node_memory_MemTotal_bytes 137438953472
node_memory_MemAvailable_bytes 68719476736
node_cpu_seconds_total{cpu="0",mode="idle"} 80
node_cpu_seconds_total{cpu="0",mode="user"} 20
`;
const NODE_TXT_2 = `node_memory_MemTotal_bytes 137438953472
node_memory_MemAvailable_bytes 68719476736
node_cpu_seconds_total{cpu="0",mode="idle"} 81
node_cpu_seconds_total{cpu="0",mode="user"} 29
`;
const DCGM_TXT = `DCGM_FI_DEV_GPU_UTIL{gpu="0"} 12
DCGM_FI_DEV_POWER_USAGE{gpu="0"} 40
DCGM_FI_DEV_GPU_TEMP{gpu="0"} 41
DCGM_FI_DEV_SM_CLOCK{gpu="0"} 2200
`;

function ctx(over = {}) {
  const state = {
    lastInboundAt: 0,
    lastSshAt: 0,
    online: false,
    applied: [],
    ingested: [],
    seq: 0,
    sshCalls: 0,
    nodeReads: 0,
    dcgmReads: 0,
    cpuState: {},
  };
  const hub = createObserveHub({
    blocks: { fleet_steady: ["gpu.util.pct", "gpu.temp.c", "system.cpu.util.pct"] },
    now: () => 10_000,
    staleMs: 60_000,
  });
  const base = {
    nodeId: "spark1",
    lastInboundAt: state.lastInboundAt,
    lastSshAt: state.lastSshAt,
    silentMs: 4000,
    sshCooldownMs: 60000,
    now: () => 10_000,
    nowNs: () => 10_000 * 1e6,
    monoNs: () => 10_000,
    cpuState: state.cpuState,
    fetchNode: async () => {
      state.nodeReads += 1;
      return NODE_TXT;
    },
    fetchDcgm: async () => {
      state.dcgmReads += 1;
      return DCGM_TXT;
    },
    nextSeq: () => state.seq++,
    ingest: (snap) => {
      state.ingested.push(snap);
      return hub.ingest(snap);
    },
    apply: (snap) => state.applied.push(snap),
    setOnline: (v) => {
      state.online = v;
    },
    sshTest: async () => {
      state.sshCalls += 1;
      return { ok: false, message: "down" };
    },
    noteSsh: (t) => {
      state.lastSshAt = t;
    },
    ...over,
  };
  return { state, hub, ctx: { ...base, lastSshAt: state.lastSshAt } };
}

test("fallback HTTP reads exporter bodies and skips SSH", async () => {
  const { state, hub, ctx: c } = ctx();
  const result = await runFallbackTick(c);
  assert.equal(result.action, "http");
  assert.equal(state.nodeReads, 1);
  assert.equal(state.dcgmReads, 1);
  assert.equal(state.sshCalls, 0);
  assert.equal(state.online, true);
  assert.equal(state.applied.length, 1);
  assert.equal(hub.get("spark1").metrics["gpu.temp.c"].value, 41);
  assert.equal(state.applied[0].quality, "degraded");
});

test("dead node-exporter still uses DCGM", async () => {
  const { state, hub, ctx: c } = ctx({
    fetchNode: async () => {
      state.nodeReads += 1;
      return null;
    },
  });
  const result = await runFallbackTick(c);
  assert.equal(result.action, "http");
  assert.equal(hub.get("spark1").metrics["gpu.util.pct"].value, 12);
  assert.equal(hub.get("spark1").metrics["memory.total.bytes"].quality, "unavailable");
  assert.equal(state.sshCalls, 0);
});

test("sshTest ok:false marks offline; cooldown holds", async () => {
  const box = { lastSshAt: 0, online: true, sshCalls: 0 };
  const c = {
    nodeId: "spark1",
    lastInboundAt: 0,
    lastSshAt: 0,
    silentMs: 4000,
    sshCooldownMs: 60000,
    now: () => 10_000,
    cpuState: {},
    fetchNode: async () => null,
    fetchDcgm: async () => null,
    nextSeq: () => 0,
    ingest: () => null,
    apply: () => {},
    setOnline: (v) => {
      box.online = v;
    },
    sshTest: async () => {
      box.sshCalls += 1;
      return { ok: false, message: "refused" };
    },
    noteSsh: (t) => {
      box.lastSshAt = t;
    },
  };
  const first = await runFallbackTick(c);
  assert.equal(first.action, "ssh-fail");
  assert.equal(box.online, false);
  c.lastSshAt = box.lastSshAt;
  const second = await runFallbackTick(c);
  assert.equal(second.action, "ssh-cooldown");
  assert.equal(box.sshCalls, 1);
});

test("ssh completion does not clobber a recovered push", async () => {
  let lastInbound = 0;
  let release;
  const gate = new Promise((r) => {
    release = r;
  });
  const pending = runFallbackTick({
    nodeId: "spark1",
    lastInboundAt: () => lastInbound,
    lastSshAt: 0,
    now: () => 10_000,
    cpuState: {},
    fetchNode: async () => null,
    fetchDcgm: async () => null,
    nextSeq: () => 0,
    ingest: () => null,
    apply: () => {},
    setOnline: () => {},
    markUnknown: () => {
      throw new Error("must not mark unknown after recovery");
    },
    sshTest: async () => {
      lastInbound = 10_000;
      await gate;
      return { ok: false, message: "late" };
    },
    noteSsh: () => {},
  });
  while (lastInbound === 0) await Promise.resolve();
  release();
  const result = await pending;
  assert.equal(result.action, "skip-recovered");
});

test("sshTest ok:true marks online without fake metrics", async () => {
  let online = false;
  let applied = 0;
  const result = await runFallbackTick({
    nodeId: "spark1",
    lastInboundAt: 0,
    lastSshAt: 0,
    now: () => 10_000,
    cpuState: {},
    fetchNode: async () => null,
    fetchDcgm: async () => null,
    nextSeq: () => 0,
    ingest: () => null,
    apply: () => {
      applied += 1;
    },
    setOnline: (v) => {
      online = v;
    },
    sshTest: async () => ({ ok: true, message: "ok" }),
    noteSsh: () => {},
  });
  assert.equal(result.action, "ssh-ok");
  assert.equal(online, true);
  assert.equal(applied, 0);
});

test("fallback records per-source collection age", async () => {
  let mono = 1;
  const { hub, ctx: c } = ctx({
    mono: () => mono,
    fetchNode: async () => {
      mono = 3;
      return NODE_TXT;
    },
    fetchDcgm: async () => {
      mono = 3.05;
      return DCGM_TXT;
    },
  });
  const result = await runFallbackTick(c);
  assert.equal(result.action, "http");
  const memAge = hub.get("spark1").metrics["memory.total.bytes"].age_ms;
  const gpuAge = hub.get("spark1").metrics["gpu.temp.c"].age_ms;
  assert.ok(memAge > gpuAge);
  assert.ok(memAge > 0);
});

test("inflight guard and recovered push skip ingest", async () => {
  let inflight = false;
  let release;
  const gate = new Promise((r) => {
    release = r;
  });
  const held = ctx({
    isInflight: () => inflight,
    setInflight: (v) => {
      inflight = v;
    },
    fetchNode: async () => {
      await gate;
      return NODE_TXT;
    },
  });
  const first = runFallbackTick(held.ctx);
  const second = await runFallbackTick(held.ctx);
  assert.equal(second.action, "inflight");
  release();
  assert.equal((await first).action, "http");

  let lastInbound = 0;
  const recovered = await runFallbackTick({
    ...ctx().ctx,
    lastInboundAt: () => lastInbound,
    fetchNode: async () => {
      lastInbound = 10_000;
      return NODE_TXT;
    },
  });
  assert.equal(recovered.action, "skip-recovered");
});

test("fallback duplicate ingest does not apply or mark online", async () => {
  const { state, ctx: c } = ctx({
    ingest: () => ({ duplicate: true }),
  });
  const result = await runFallbackTick(c);
  assert.equal(result.action, "http-duplicate");
  assert.equal(state.applied.length, 0);
  assert.equal(state.online, false);
  assert.equal(state.sshCalls, 0);
});

test("fallback string reject does not apply or mark online", async () => {
  const { state, ctx: c } = ctx({
    ingest: () => "stale sequence",
  });
  const result = await runFallbackTick(c);
  assert.equal(result.action, "http-reject");
  assert.equal(result.error, "stale sequence");
  assert.equal(state.applied.length, 0);
  assert.equal(state.online, false);
  assert.equal(state.sshCalls, 0);
});

test("cpu util appears on the second HTTP sample", () => {
  const state = {};
  const first = buildNodeSnapshot({
    node: "spark1",
    producerId: "spark1-fallback-http",
    seq: 0,
    nodeText: NODE_TXT,
    dcgmText: DCGM_TXT,
    nodeOk: true,
    dcgmOk: true,
    nowNs: 1e6,
    monoNs: 1,
    cpuState: state,
    fallback: true,
  });
  assert.equal(first.metrics["system.cpu.util.pct"].quality, "unavailable");
  const second = buildNodeSnapshot({
    node: "spark1",
    producerId: "spark1-fallback-http",
    seq: 1,
    nodeText: NODE_TXT_2,
    dcgmText: DCGM_TXT,
    nodeOk: true,
    dcgmOk: true,
    nowNs: 2e6,
    monoNs: 2,
    cpuState: state,
    fallback: true,
  });
  assert.equal(second.metrics["system.cpu.util.pct"].quality, "derived");
  assert.equal(Math.round(second.metrics["system.cpu.util.pct"].value), 90);
});
