import assert from "node:assert/strict";
import { test } from "node:test";
import {
  BUNDLE_SHA256,
  ReduceError,
  heatBlend,
  reduceAll,
  reduceEvent,
  emptyScene,
  sceneFingerprint,
} from "./frameReducer.js";
import {
  fixtureDsShaped,
  fixtureGap,
  fixtureGeomNoRouteCap,
  fixtureNoRouting,
  fixtureScaleBlend,
  fixtureSmall,
  fixtureTwoRanks,
  fixtureTwoRequests,
  fixtureUnknownVersion,
  FIXTURES,
} from "./fixtures.js";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

test("unknown schema_version fails closed", () => {
  assert.throws(() => reduceAll(fixtureUnknownVersion()), (err) => {
    assert.equal(err instanceof ReduceError, true);
    assert.equal(err.code, "schema");
    return true;
  });
});

test("wrong bundle hash fails closed", () => {
  const ev = fixtureSmall();
  ev[0].payload.schema_bundle_hash = "0".repeat(64);
  assert.throws(() => reduceAll(ev), (err) => err.code === "bundle_hash");
});

test("JSON round-trip replay matches all fixtures", () => {
  for (const [name, make] of Object.entries(FIXTURES)) {
    const a = reduceAll(make());
    const b = reduceAll(JSON.parse(JSON.stringify(make())));
    assert.equal(sceneFingerprint(a), sceneFingerprint(b), name);
  }
});

test("live vs replay identity on all fixtures", () => {
  for (const make of [
    fixtureDsShaped,
    fixtureSmall,
    fixtureNoRouting,
    fixtureScaleBlend,
    fixtureTwoRequests,
  ]) {
    const events = make();
    const a = reduceAll(events);
    const b = emptyScene();
    for (const ev of events) reduceEvent(b, ev);
    assert.equal(sceneFingerprint(a), sceneFingerprint(b), make.name);
    assert.equal(a.schema_bundle_hash, BUNDLE_SHA256);
    assert.equal(a.complete, true);
  }
});

test("no-routing leaves map unavailable", () => {
  const s = reduceAll(fixtureNoRouting());
  assert.equal(s.routing_unavailable, true);
  assert.equal(s.heat, null);
  const blend = heatBlend(s);
  assert.equal(blend.width, 0);
});

test("two request_ids share a step number", () => {
  const s = reduceAll(fixtureTwoRequests());
  assert.ok(s.requests.qA.steps["7"]);
  assert.ok(s.requests.qB.steps["7"]);
  assert.equal(s.text.qA, "a");
  assert.equal(s.text.qB, "b");
  assert.notEqual(s.requests.qA.steps["7"].token_rows[0].token_id, s.requests.qB.steps["7"].token_rows[0].token_id);
});

test("scale/heat-blend shrinks a huge map", () => {
  const s = reduceAll(fixtureScaleBlend());
  const blend = heatBlend(s, 1920, 1080);
  assert.ok(blend.scale >= 4);
  assert.ok(blend.width * 2 <= 1920);
  assert.ok(blend.height * 2 <= 1080);
  assert.ok(blend.cells.some((v) => v > 0));
});

test("ds-shaped geometry is 43x256 and speculative row is kept", () => {
  const s = reduceAll(fixtureDsShaped());
  assert.equal(s.geometry.layers, 43);
  assert.equal(s.geometry.experts, 256);
  assert.equal(s.speculative_unavailable, false);
  assert.equal(s.requests.q1.steps["0#0"].token_rows.length, 2);
});

test("geometry without routing.experts stays unavailable", () => {
  const s = reduceAll(fixtureGeomNoRouteCap());
  assert.equal(s.routing_unavailable, true);
  assert.equal(s.heat, null);
});

test("rank-scoped same step keeps both ranks", () => {
  const s = reduceAll(fixtureTwoRanks());
  assert.ok(s.requests.q1.steps["0#0"]);
  assert.ok(s.requests.q1.steps["0#1"]);
  assert.equal(s.heat.hits, 8);
});

test("gap covers skipped hub_order", () => {
  const s = reduceAll(fixtureGap());
  assert.equal(s.gaps.length, 1);
  assert.equal(s.gaps[0].first, 1);
  assert.equal(s.complete, true);
});

test("duplicate producer seq is dropped", () => {
  const ev = fixtureSmall();
  const s = emptyScene();
  reduceEvent(s, ev[0]);
  reduceEvent(s, ev[1]);
  const hits = s.heat.hits;
  const dup = { ...ev[1], hub_order: 2 };
  reduceEvent(s, dup);
  assert.equal(s.heat.hits, hits);
  const fin = { ...ev[2], hub_order: 3, seq: 2 };
  reduceEvent(s, fin);
  assert.equal(s.complete, true);
});

test("runtime schema copies match frozen contracts", () => {
  const here = dirname(fileURLToPath(import.meta.url));
  const frozenDirs = [
    join(here, "../contracts"),
    join(here, "../../../adv_viz/contracts"),
    join(here, "../../../../adv_viz/contracts"),
  ];
  let frozen;
  for (const d of frozenDirs) {
    try {
      readFileSync(join(d, "trace-event-v1.schema.json"));
      frozen = d;
      break;
    } catch {
      /* next */
    }
  }
  assert.ok(frozen);
  for (const name of ["node-snapshot-v1.schema.json", "trace-event-v1.schema.json"]) {
    const a = readFileSync(join(frozen, name));
    const b = readFileSync(join(here, "contracts", name));
    assert.equal(a.equals(b), true, name);
  }
  const serverNode = readFileSync(join(here, "../../server/observe/contracts/node-snapshot-v1.schema.json"));
  assert.equal(
    serverNode.equals(readFileSync(join(frozen, "node-snapshot-v1.schema.json"))),
    true,
    "server observe schema copy"
  );
  const serverTrace = readFileSync(join(here, "../../server/observe/contracts/trace-event-v1.schema.json"));
  assert.equal(
    serverTrace.equals(readFileSync(join(frozen, "trace-event-v1.schema.json"))),
    true,
    "server trace schema copy"
  );
});

test("BUNDLE_SHA256 matches MANIFEST.json", () => {
  const here = dirname(fileURLToPath(import.meta.url));
  const paths = [
    join(here, "MANIFEST.json"),
    join(here, "../contracts/MANIFEST.json"),
    join(here, "../../../adv_viz/contracts/MANIFEST.json"),
  ];
  let man;
  for (const p of paths) {
    try {
      man = JSON.parse(readFileSync(p, "utf8"));
      break;
    } catch {
      /* next */
    }
  }
  assert.ok(man);
  assert.equal(BUNDLE_SHA256, man.bundle_sha256);
});

test("unknown envelope field fails closed", () => {
  const ev = fixtureSmall();
  ev[1].nope = true;
  assert.throws(() => reduceAll(ev), (err) => err.code === "schema");
});

test("second run.started is rejected", () => {
  const ev = fixtureSmall();
  const s = emptyScene();
  reduceEvent(s, ev[0]);
  reduceEvent(s, ev[1]);
  const hits = s.heat.hits;
  const mid = s.model_id;
  const again = {
    ...ev[0],
    seq: 2,
    hub_order: 2,
    payload: { ...ev[0].payload, model_id: "changed" },
  };
  assert.throws(() => reduceEvent(s, again), (err) => err.code === "lifecycle");
  assert.equal(s.model_id, mid);
  assert.equal(s.heat.hits, hits);
});

test("unknown start payload field fails schema", () => {
  const ev = fixtureSmall();
  ev[0].payload = { ...ev[0].payload, nope: true };
  assert.throws(() => reduceAll(ev), (err) => err.code === "schema");
});

test("negative step fails schema", () => {
  const ev = fixtureSmall();
  ev[1].payload = { ...ev[1].payload, step: -1 };
  assert.throws(() => reduceAll(ev), (err) => err.code === "schema");
});

test("constructor is a usable request_id", () => {
  const ev = fixtureSmall();
  ev[1].payload.request_id = "constructor";
  const s = reduceAll(ev);
  assert.ok(s.requests.constructor.steps["0"]);
});
