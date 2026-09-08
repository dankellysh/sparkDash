import assert from "node:assert/strict";
import { test } from "node:test";
import {
  createObserveHub,
  parseRollupYaml,
  prometheusText,
  snapshotToPanels,
  loadRollupBlocks,
} from "./hub.js";

const yaml = `blocks:
  fleet_steady:
    - gpu.util.pct
    - gpu.temp.c
`;

function snap(over = {}) {
  return {
    contract: "dgx.node-snapshot.v1",
    producer_id: "spark1-push",
    seq: 0,
    run_id: null,
    node: "spark1",
    observed_at_ns: 1_700_000_000_000_000,
    monotonic_ns: 1000,
    source_age_ms: 0,
    quality: "measured",
    capabilities: ["node.steady"],
    metrics: {
      "gpu.util.pct": {
        value: 10,
        unit: "pct",
        source: "dcgm",
        quality: "measured",
        age_ms: 0,
      },
      "gpu.temp.c": {
        value: 42,
        unit: "c",
        source: "dcgm",
        quality: "measured",
        age_ms: 0,
      },
    },
    ...over,
  };
}

test("parseRollupYaml named blocks", () => {
  const parsed = parseRollupYaml(yaml);
  assert.deepEqual(parsed.blocks.fleet_steady, ["gpu.util.pct", "gpu.temp.c"]);
  assert.deepEqual(parsed.burst, []);
});

test("parseRollupYaml rejects malformed lines", () => {
  assert.throws(() => parseRollupYaml("blocks:\n  fleet_steady:\n   nope\n"));
});

test("loadRollupBlocks fails loud on missing file", () => {
  assert.throws(() => loadRollupBlocks("/no/such/rollups.yaml"), /unreadable/);
});

test("missing selected block fails at hub create", () => {
  assert.throws(
    () => createObserveHub({ blocks: { other: ["gpu.util.pct"] } }),
    /missing or empty/
  );
});

test("ingest validates frozen schema", () => {
  const hub = createObserveHub({
    blocks: { fleet_steady: ["gpu.util.pct", "gpu.temp.c"] },
    now: () => 1_000,
  });
  assert.match(hub.ingest({ contract: "dgx.node-snapshot.v1", node: "spark1", unexpected: true }), /invalid snapshot/);
  assert.equal(hub.ingest(snap()), null);
});

test("ingest and prometheus rollups", () => {
  const hub = createObserveHub({
    blocks: { fleet_steady: ["gpu.util.pct", "gpu.temp.c"] },
    now: () => 1_000,
    staleMs: 60_000,
  });
  const err = hub.ingest(snap({ observed_at_ns: 1_000 * 1e6 }));
  assert.equal(err, null);
  const text = hub.metrics();
  assert.match(text, /gpu_util_pct\{node="spark1"/);
  assert.match(text, /42/);
});

test("stale cache is not exported as fresh", () => {
  const hub = createObserveHub({
    blocks: { fleet_steady: ["gpu.util.pct", "gpu.temp.c"] },
    now: () => 50_000,
    staleMs: 5_000,
  });
  assert.equal(hub.ingest(snap({ observed_at_ns: 1 })), null);
  assert.equal(hub.metrics(), "");
});

test("producer sequence and restart", () => {
  const hub = createObserveHub({
    blocks: { fleet_steady: ["gpu.util.pct", "gpu.temp.c"] },
    now: () => 1_000,
    staleMs: 60_000,
  });
  assert.equal(hub.ingest(snap({ seq: 2, metrics: snap().metrics })), null);
  assert.match(hub.ingest(snap({ seq: 1 })), /stale sequence/);
  assert.equal(hub.get("spark1").seq, 2);
  assert.equal(hub.ingest(snap({ seq: 0 })), null);
  assert.equal(hub.get("spark1").seq, 0);
  assert.equal(hub.ingest(snap({ seq: 0, producer_id: "spark1-fallback-http" })), null);
});

test("snapshotToPanels nulls stale metric ages", () => {
  const p = snapshotToPanels(
    {
      quality: "measured",
      observed_at_ns: 1_000 * 1e6,
      metrics: {
        "gpu.util.pct": {
          value: 99,
          unit: "pct",
          source: "dcgm",
          quality: "measured",
          age_ms: 60_000,
        },
        "gpu.temp.c": {
          value: 41,
          unit: "c",
          source: "dcgm",
          quality: "measured",
          age_ms: 1,
        },
      },
    },
    { now: 1_000, receivedAt: 1_000, staleMs: 5_000 }
  );
  assert.equal(p.gpu.usage, null);
  assert.equal(p.gpu.temperature, 41);
});

test("duplicate seq is idempotent success", () => {
  const hub = createObserveHub({
    blocks: { fleet_steady: ["gpu.util.pct", "gpu.temp.c"] },
    now: () => 1_000,
    staleMs: 60_000,
  });
  const first = snap({ seq: 1, observed_at_ns: 1_000 * 1e6 });
  assert.equal(hub.ingest(first), null);
  assert.deepEqual(hub.ingest(snap({ seq: 1, observed_at_ns: 1_000 * 1e6, metrics: first.metrics })), {
    duplicate: true,
  });
  assert.equal(hub.get("spark1").seq, 1);
});

test("cross-producer duplicate does not count as a new accept", () => {
  const hub = createObserveHub({
    blocks: { fleet_steady: ["gpu.util.pct", "gpu.temp.c"] },
    now: () => 1_000,
    staleMs: 60_000,
  });
  assert.equal(hub.ingest(snap({ seq: 10, producer_id: "spark1-push", observed_at_ns: 1_000 * 1e6 })), null);
  assert.equal(
    hub.ingest(snap({ seq: 0, producer_id: "spark1-fallback-http", observed_at_ns: 1_000 * 1e6 })),
    null
  );
  assert.deepEqual(
    hub.ingest(snap({ seq: 10, producer_id: "spark1-push", observed_at_ns: 1_000 * 1e6 })),
    { duplicate: true }
  );
  assert.equal(hub.get("spark1").producer_id, "spark1-fallback-http");
});

test("underscore metric names are valid rollup keys", () => {
  const parsed = parseRollupYaml("blocks:\n  fleet_steady:\n    - fabric.roce.rx.bytes_total\n");
  assert.deepEqual(parsed.blocks.fleet_steady, ["fabric.roce.rx.bytes_total"]);
});

test("snapshotToPanels uses UMA MiB not ram bytes", () => {
  const p = snapshotToPanels({
    quality: "measured",
    observed_at_ns: 1_000 * 1e6,
    metrics: {
      "gpu.temp.c": { value: 41, unit: "c", source: "dcgm", quality: "measured", age_ms: 1 },
      "memory.total.bytes": {
        value: 137438953472,
        unit: "bytes",
        source: "node-exporter",
        quality: "measured",
        age_ms: 2,
      },
      "memory.available.bytes": {
        value: 68719476736,
        unit: "bytes",
        source: "node-exporter",
        quality: "measured",
        age_ms: 2,
      },
    },
  }, { now: 1_000, receivedAt: 1_000, staleMs: 5_000 });
  assert.equal(p.gpu.temperature, 41);
  assert.equal(p.gpu.usage, null);
  assert.equal(p.ram, undefined);
  assert.equal(p.unifiedMemory.total, 131072);
  assert.equal(p.unifiedMemory.used, 65536);
  assert.equal(p.unifiedMemory.available, 65536);
  assert.equal(p.unifiedMemory.percentage, 50);
  assert.equal(p.quality, "measured");
});

test("lease grant release refcount expiry", () => {
  let t = 1000;
  const hub = createObserveHub({
    blocks: { fleet_steady: ["gpu.util.pct"] },
    now: () => t,
    burstAllowlist: ["gpu.util.pct"],
  });
  assert.equal(hub.grantLease({}).error, "run_id, holder, and node are required");
  assert.equal(hub.grantLease({ run_id: "r", holder: "viz", node: "spark1", ttl_ms: 0 }).error, "ttl_ms must be a finite positive number");
  const a = hub.grantLease({ run_id: "r", holder: "viz", node: "spark1", ttl_ms: 50 });
  assert.equal(a.refs, 1);
  assert.deepEqual(a.burst, ["gpu.util.pct"]);
  const b = hub.grantLease({ run_id: "r", holder: "viz", node: "spark1", ttl_ms: 50 });
  assert.equal(b.refs, 2);
  assert.ok(hub.activeLease("spark1"));
  assert.equal(hub.releaseLease({ run_id: "r", holder: "viz", node: "spark1" }), null);
  assert.ok(hub.activeLease("spark1"));
  assert.equal(hub.releaseLease({ run_id: "r", holder: "viz", node: "spark1" }), null);
  assert.equal(hub.activeLease("spark1"), null);
  hub.grantLease({ run_id: "r", holder: "a", node: "spark1", ttl_ms: 50 });
  hub.grantLease({ run_id: "r", holder: "b", node: "spark1", ttl_ms: 50 });
  assert.ok(hub.activeLease("spark1"));
  t = 2000;
  assert.equal(hub.activeLease("spark1"), null);
});

test("malformed rollup metric names fail", () => {
  assert.throws(() => parseRollupYaml("blocks:\n  fleet_steady:\n    - [\n"));
});

test("delayed seq after producer switch is rejected", () => {
  const hub = createObserveHub({
    blocks: { fleet_steady: ["gpu.util.pct", "gpu.temp.c"] },
    now: () => 1_000,
    staleMs: 60_000,
  });
  assert.equal(hub.ingest(snap({ seq: 10, observed_at_ns: 1_000 * 1e6 })), null);
  assert.equal(
    hub.ingest(snap({ seq: 0, producer_id: "spark1-fallback-http", observed_at_ns: 1_000 * 1e6 })),
    null
  );
  assert.match(hub.ingest(snap({ seq: 9, observed_at_ns: 1_000 * 1e6 })), /stale sequence/);
  assert.equal(hub.get("spark1").producer_id, "spark1-fallback-http");
});

test("old source_age is not exported as fresh", () => {
  const hub = createObserveHub({
    blocks: { fleet_steady: ["gpu.util.pct", "gpu.temp.c"] },
    now: () => 1_000,
    staleMs: 5_000,
  });
  const s = snap({
    observed_at_ns: 1_000 * 1e6,
    source_age_ms: 60_000,
    metrics: {
      "gpu.util.pct": {
        value: 10,
        unit: "pct",
        source: "dcgm",
        quality: "measured",
        age_ms: 60_000,
      },
      "gpu.temp.c": {
        value: 42,
        unit: "c",
        source: "dcgm",
        quality: "measured",
        age_ms: 1,
      },
    },
  });
  assert.equal(hub.ingest(s), null);
  const text = hub.metrics();
  assert.doesNotMatch(text, /gpu_util_pct/);
  assert.match(text, /gpu_temp_c/);
});

test("lease renew does not grow refs", () => {
  let t = 1000;
  const hub = createObserveHub({
    blocks: { fleet_steady: ["gpu.util.pct"] },
    now: () => t,
  });
  const g = hub.grantLease({ run_id: "r", holder: "viz", node: "spark1", ttl_ms: 50 });
  assert.equal(g.refs, 1);
  t = 1020;
  const r = hub.renewLease({ run_id: "r", holder: "viz", node: "spark1", ttl_ms: 50 });
  assert.equal(r.refs, 1);
  t = 1040;
  assert.ok(hub.activeLease("spark1"));
  assert.equal(hub.releaseLease({ run_id: "r", holder: "viz", node: "spark1" }), null);
  assert.equal(hub.activeLease("spark1"), null);
});

test("prometheusText skips unavailable", () => {
  const text = prometheusText(
    {
      spark1: {
        node: "spark1",
        metrics: {
          "gpu.util.pct": { value: null, quality: "unavailable" },
        },
      },
    },
    ["gpu.util.pct"]
  );
  assert.equal(text, "");
});
