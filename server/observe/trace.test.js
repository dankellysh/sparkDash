import assert from "node:assert/strict";
import { test } from "node:test";
import fs from "fs";
import os from "os";
import path from "path";
import { createRecorder } from "./record.js";
import { createTraceHub, BUNDLE_SHA256 } from "./trace.js";

function rec() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "tr-"));
  return createRecorder({
    config: {
      dir,
      per_run_bytes: 1_000_000,
      total_bytes: 10_000_000,
      min_free_bytes: 1,
      queue_events: 64,
      rss_bytes: 1e12,
      coalesce_node_ms: 1,
      zstd: "/usr/bin/zstd",
    },
    rss: () => 1,
    freeDisk: () => 1e12,
    compress: async (src, dest) => fs.copyFileSync(src, dest),
    decompress: async (src) => fs.readFileSync(src, "utf8"),
  });
}

function start(over = {}) {
  return {
    contract: "dgx.trace-event.v1",
    producer_id: "probe",
    seq: 0,
    run_id: "run-a",
    observed_at_ns: 1_000_000,
    type: "run.started",
    payload: {
      model_id: "probe-text",
      members: ["spark1", "spark2"],
      capabilities: ["output.text"],
      schema_version: 1,
      schema_bundle_hash: BUNDLE_SHA256,
      geometry: {
        layers: 1,
        experts: 0,
        routed_experts: 0,
        top_k: 0,
        top_k_per_layer: [0],
        ranks: [
          { member: "spark1", rank: 0 },
          { member: "spark2", rank: 1 },
        ],
        route_scope: "replicated",
      },
      topology: "spark1 head, spark2 worker",
    },
    ...over,
  };
}

test("stamp hub_order 0 on start and reject producer hub_order", async () => {
  const fan = [];
  const hub = createTraceHub({ recorder: rec(), broadcast: (e) => fan.push(e) });
  const bad = start();
  bad.hub_order = 9;
  const r0 = await hub.ingest(bad);
  assert.match(r0.error, /invalid ingest/);
  const r = await hub.ingest(start());
  assert.equal(r.event.hub_order, 0);
  assert.equal(fan[0].hub_order, 0);
});

test("output.delta then finish; duplicate seq is not applied", async () => {
  const hub = createTraceHub({ recorder: rec() });
  assert.equal((await hub.ingest(start())).event.hub_order, 0);
  const delta = {
    contract: "dgx.trace-event.v1",
    producer_id: "probe",
    seq: 1,
    run_id: "run-a",
    observed_at_ns: 2_000_000,
    type: "output.delta",
    payload: { request_id: "q1", text: "hi" },
  };
  const d = await hub.ingest(delta);
  assert.equal(d.event.hub_order, 1);
  assert.deepEqual(await hub.ingest(delta), { duplicate: true });
  const fin = await hub.ingest({
    contract: "dgx.trace-event.v1",
    producer_id: "probe",
    seq: 2,
    run_id: "run-a",
    observed_at_ns: 3_000_000,
    type: "run.finished",
    payload: { reason: "stop", error: null },
  });
  assert.equal(fin.event.type, "run.finished");
  const listed = hub.recorder.listRuns();
  assert.equal(listed[0].status, "final");
  const evs = await hub.recorder.readEvents("run-a");
  assert.equal(evs.map((e) => e.type).join(","), "run.started,output.delta,run.finished");
});

test("traversal run_id and restart reuse are rejected", async () => {
  const recorder = rec();
  const hub = createTraceHub({ recorder });
  const s = start();
  s.run_id = "../config";
  assert.match((await hub.ingest(s)).error, /bad run_id/);
  assert.equal((await hub.ingest(start())).event.hub_order, 0);
  const hub2 = createTraceHub({ recorder });
  assert.match((await hub2.ingest(start())).error, /already started/);
});

test("held node snapshots stamp after flush before later output", async () => {
  const fan = [];
  const hub = createTraceHub({ recorder: rec(), broadcast: (e) => fan.push(e.type + e.hub_order) });
  await hub.ingest(start());
  hub.noteNodeSnapshot({
    node: "spark1",
    monotonic_ns: 1,
    source_age_ms: 0,
    quality: "measured",
    capabilities: ["node.steady"],
    metrics: {},
    observed_at_ns: 1,
  });
  await hub.ingest({
    contract: "dgx.trace-event.v1",
    producer_id: "probe",
    seq: 1,
    run_id: "run-a",
    observed_at_ns: 2,
    type: "output.delta",
    payload: { request_id: "q1", text: "x" },
  });
  assert.ok(fan.includes("node.snapshot1"));
  assert.ok(fan.indexOf("node.snapshot1") < fan.indexOf("output.delta2"));
});

test("wrong bundle hash and gap ingest fail", async () => {
  const hub = createTraceHub({ recorder: rec() });
  const s = start();
  s.payload = { ...s.payload, schema_bundle_hash: "aa".repeat(32) };
  assert.match((await hub.ingest(s)).error, /schema_bundle_hash/);
  const gap = {
    contract: "dgx.trace-event.v1",
    producer_id: "probe",
    seq: 0,
    run_id: "run-a",
    observed_at_ns: 1,
    type: "gap",
    payload: { first_missing_hub_order: 1, last_missing_hub_order: 1, reason: "drop" },
  };
  assert.match((await hub.ingest(gap)).error, /invalid ingest/);
});
