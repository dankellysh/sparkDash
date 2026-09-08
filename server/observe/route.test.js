import assert from "node:assert/strict";
import { test } from "node:test";
import { SparkMonitor } from "../sparks/SparkMonitor.js";
import {
  createObserveHub,
  dispatchSnapshot,
  snapshotToPanels,
  ROLLUP_METRIC_NAMES,
  parseRollupYaml,
} from "./hub.js";

function gpu(age) {
  return {
    "gpu.util.pct": { value: 99, unit: "pct", source: "dcgm", quality: "measured", age_ms: age },
    "gpu.temp.c": { value: 41, unit: "c", source: "dcgm", quality: "measured", age_ms: age },
  };
}

function doc({ producer, seq, t, age = 0 }) {
  return {
    contract: "dgx.node-snapshot.v1",
    producer_id: producer,
    seq,
    run_id: null,
    node: "spark1",
    observed_at_ns: t * 1e6,
    monotonic_ns: t * 1e6,
    source_age_ms: age,
    quality: "measured",
    capabilities: ["node.steady"],
    metrics: gpu(age),
  };
}

test("dispatchSnapshot: cross-producer duplicate does not refresh monitor clock", () => {
  let t = 1000;
  const hub = createObserveHub({
    now: () => t,
    blocks: { fleet_steady: ["gpu.util.pct", "gpu.temp.c"] },
    staleMs: 5000,
  });
  const mon = new SparkMonitor({
    id: "spark1",
    name: "spark1",
    lanIp: "10.0.0.80",
    role: "worker",
  });
  try {
    const post = (body) =>
      dispatchSnapshot(hub, body, {
        now: () => t,
        apply(stored, nowMs) {
          mon.applyInboundSnapshot(
            stored,
            snapshotToPanels(stored, { now: nowMs, receivedAt: nowMs }),
            nowMs
          );
        },
      });

    const push = doc({ producer: "spark1-push", seq: 10, t: 1000, age: 0 });
    assert.deepEqual(post(push), { status: 200, body: { ok: true } });
    assert.equal(mon._lastInboundAt, 1000);

    t = 1500;
    assert.deepEqual(post(push), { status: 200, body: { ok: true, duplicate: true } });
    assert.equal(mon._lastInboundAt, 1000);

    t = 2000;
    const fallback = doc({ producer: "spark1-fallback-http", seq: 0, t: 2000, age: 4000 });
    assert.deepEqual(post(fallback), { status: 200, body: { ok: true } });
    assert.equal(mon._lastInboundAt, 2000);
    assert.equal(mon.snapshot(2000).metrics.gpu.usage, 99);

    t = 3500;
    assert.equal(mon.snapshot(3500).metrics.gpu.usage, null);
    assert.equal(mon.snapshot(3500).metricsFresh, false);
    assert.equal(hub.metrics(), "");

    for (const replay of [push, fallback, push]) {
      assert.deepEqual(post(replay), { status: 200, body: { ok: true, duplicate: true } });
      assert.equal(mon._lastInboundAt, 2000);
      assert.equal(hub.get("spark1").producer_id, "spark1-fallback-http");
      assert.equal(mon.snapshot(3500).metrics.gpu.usage, null);
      assert.equal(mon.snapshot(3500).metricsFresh, false);
      assert.equal(hub.metrics(), "");
    }

    assert.equal(post({ ...push, seq: 9 }).status, 400);
    assert.equal(post({ node: "spark1" }).status, 400);
    assert.equal(mon._lastInboundAt, 2000);

    t = 4000;
    assert.deepEqual(post(doc({ producer: "spark1-push", seq: 11, t: 4000, age: 0 })), {
      status: 200,
      body: { ok: true },
    });
    assert.equal(mon._lastInboundAt, 4000);
    assert.equal(mon.snapshot(4000).metrics.gpu.usage, 99);
  } finally {
    mon.stop();
  }
});

test("all frozen rollup metric names parse", () => {
  const names = [...ROLLUP_METRIC_NAMES];
  const yaml = ["blocks:", "  fleet_steady:"].concat(names.map((n) => `    - ${n}`)).join("\n") + "\n";
  const parsed = parseRollupYaml(yaml);
  assert.deepEqual(parsed.blocks.fleet_steady, names);
});
