import { BUNDLE_SHA256 } from "./frameReducer.js";

function envelope(hub_order, type, payload, extra = {}) {
  return {
    contract: "dgx.trace-event.v1",
    producer_id: extra.producer_id || "fixture",
    seq: hub_order,
    run_id: extra.run_id || "run-fix",
    hub_order,
    observed_at_ns: 1_000_000_000 + hub_order,
    type,
    payload,
  };
}

function started(geometry, capabilities, extra = {}) {
  return envelope(0, "run.started", {
    model_id: extra.model_id || "fixture-model",
    members: extra.members || ["spark1"],
    capabilities,
    schema_version: 1,
    schema_bundle_hash: extra.bundle || BUNDLE_SHA256,
    geometry,
  });
}

function ranks(members) {
  return members.map((member, rank) => ({ member, rank }));
}

function topK(layers, k) {
  return Array.from({ length: layers }, () => k);
}

function routesFor(layers, top_k, experts, seed) {
  const out = [];
  for (let L = 0; L < layers; L++) {
    const row = [];
    for (let t = 0; t < top_k; t++) {
      row.push((seed + L + t) % experts);
    }
    out.push(row);
  }
  return out;
}

export function fixtureDsShaped() {
  const members = ["spark1", "spark2"];
  const layers = 43;
  const experts = 256;
  const k = 6;
  const g = {
    layers,
    experts,
    routed_experts: 256,
    top_k: k,
    top_k_per_layer: topK(layers, k),
    ranks: ranks(members),
    route_scope: "rank",
  };
  const ev = [
    started(g, ["engine.step", "speculative.rows", "routing.experts", "output.text"], {
      members,
      model_id: "ds-shaped",
    }),
  ];
  let h = 1;
  ev.push(
    envelope(h++, "step.completed", {
      request_id: "q1",
      member: "spark1",
      rank: 0,
      step: 0,
      started_at_ns: 1,
      duration_ns: 10,
      token_rows: [
        { row: 0, accepted: true, token_id: 1, text: "A", routes: routesFor(layers, k, experts, 1) },
        { row: 1, accepted: false, token_id: 2, text: "x", routes: routesFor(layers, k, experts, 9) },
      ],
    })
  );
  ev.push(
    envelope(h++, "output.delta", { request_id: "q1", text: "A", step: 0 })
  );
  ev.push(envelope(h++, "run.finished", { reason: "stop" }));
  return ev;
}

export function fixtureSmall() {
  const members = ["spark1"];
  const layers = 8;
  const experts = 16;
  const k = 2;
  const g = {
    layers,
    experts,
    routed_experts: 16,
    top_k: k,
    top_k_per_layer: topK(layers, k),
    ranks: ranks(members),
    route_scope: "replicated",
  };
  return [
    started(g, ["engine.step", "routing.experts"], { members, model_id: "small" }),
    envelope(1, "step.completed", {
      request_id: "q1",
      member: "spark1",
      rank: 0,
      step: 0,
      started_at_ns: 1,
      duration_ns: 5,
      token_rows: [
        { row: 0, accepted: true, token_id: 7, text: "hi", routes: routesFor(layers, k, experts, 3) },
      ],
    }),
    envelope(2, "run.finished", { reason: "stop" }),
  ];
}

export function fixtureNoRouting() {
  const members = ["spark1"];
  const layers = 4;
  const g = {
    layers,
    experts: 0,
    routed_experts: 0,
    top_k: 0,
    top_k_per_layer: topK(layers, 0),
    ranks: ranks(members),
    route_scope: "replicated",
  };
  return [
    started(g, ["engine.step", "output.text"], { members, model_id: "dense" }),
    envelope(1, "step.completed", {
      request_id: "q1",
      member: null,
      rank: null,
      step: 0,
      started_at_ns: 1,
      duration_ns: 5,
      token_rows: [{ row: 0, accepted: true, token_id: 3, text: "z", routes: null }],
    }),
    envelope(2, "output.delta", { request_id: "q1", text: "z" }),
    envelope(3, "run.finished", { reason: "stop" }),
  ];
}

export function fixtureScaleBlend() {
  const members = ["spark1"];
  const layers = 128;
  const experts = 2048;
  const k = 1;
  const g = {
    layers,
    experts,
    routed_experts: 2048,
    top_k: k,
    top_k_per_layer: topK(layers, k),
    ranks: ranks(members),
    route_scope: "replicated",
  };
  return [
    started(g, ["routing.experts"], { members, model_id: "huge" }),
    envelope(1, "step.completed", {
      request_id: "q1",
      member: "spark1",
      rank: 0,
      step: 0,
      started_at_ns: 1,
      duration_ns: 1,
      token_rows: [
        { row: 0, accepted: true, token_id: 0, text: null, routes: routesFor(layers, k, experts, 0) },
      ],
    }),
    envelope(2, "run.finished", { reason: "stop" }),
  ];
}

export function fixtureTwoRequests() {
  const members = ["spark1"];
  const layers = 4;
  const experts = 8;
  const k = 1;
  const g = {
    layers,
    experts,
    routed_experts: 8,
    top_k: k,
    top_k_per_layer: topK(layers, k),
    ranks: ranks(members),
    route_scope: "replicated",
  };
  return [
    started(g, ["engine.step", "routing.experts", "output.text"], {
      members,
      model_id: "batch2",
    }),
    envelope(1, "step.completed", {
      request_id: "qA",
      member: "spark1",
      rank: 0,
      step: 7,
      started_at_ns: 1,
      duration_ns: 1,
      token_rows: [
        { row: 0, accepted: true, token_id: 10, text: "a", routes: routesFor(layers, k, experts, 1) },
      ],
    }),
    envelope(2, "step.completed", {
      request_id: "qB",
      member: "spark1",
      rank: 0,
      step: 7,
      started_at_ns: 1,
      duration_ns: 1,
      token_rows: [
        { row: 0, accepted: true, token_id: 11, text: "b", routes: routesFor(layers, k, experts, 2) },
      ],
    }),
    envelope(3, "output.delta", { request_id: "qA", text: "a", step: 7 }),
    envelope(4, "output.delta", { request_id: "qB", text: "b", step: 7 }),
    envelope(5, "run.finished", { reason: "stop" }),
  ];
}

export function fixtureUnknownVersion() {
  const members = ["spark1"];
  const g = {
    layers: 1,
    experts: 0,
    routed_experts: 0,
    top_k: 0,
    top_k_per_layer: [0],
    ranks: ranks(members),
    route_scope: "replicated",
  };
  const ev = started(g, ["engine.step"], { members });
  ev.payload.schema_version = 2;
  return [ev];
}

export function fixtureTwoRanks() {
  const members = ["spark1", "spark2"];
  const layers = 4;
  const experts = 8;
  const k = 1;
  const g = {
    layers,
    experts,
    routed_experts: 8,
    top_k: k,
    top_k_per_layer: topK(layers, k),
    ranks: ranks(members),
    route_scope: "rank",
  };
  return [
    started(g, ["engine.step", "routing.experts"], { members, model_id: "tp2" }),
    envelope(1, "step.completed", {
      request_id: "q1",
      member: "spark1",
      rank: 0,
      step: 0,
      started_at_ns: 1,
      duration_ns: 1,
      token_rows: [
        { row: 0, accepted: true, token_id: 1, text: "a", routes: routesFor(layers, k, experts, 1) },
      ],
    }),
    envelope(2, "step.completed", {
      request_id: "q1",
      member: "spark2",
      rank: 1,
      step: 0,
      started_at_ns: 1,
      duration_ns: 1,
      token_rows: [
        { row: 0, accepted: true, token_id: 1, text: "a", routes: routesFor(layers, k, experts, 2) },
      ],
    }),
    envelope(3, "run.finished", { reason: "stop" }),
  ];
}

export function fixtureGap() {
  const members = ["spark1"];
  const g = {
    layers: 1,
    experts: 0,
    routed_experts: 0,
    top_k: 0,
    top_k_per_layer: [0],
    ranks: ranks(members),
    route_scope: "replicated",
  };
  return [
    started(g, ["engine.step"], { members, model_id: "gap" }),
    envelope(2, "gap", {
      first_missing_hub_order: 1,
      last_missing_hub_order: 1,
      reason: "drop",
    }),
    envelope(3, "run.finished", { reason: "stop" }),
  ];
}

export function fixtureGeomNoRouteCap() {
  const members = ["spark1"];
  const g = {
    layers: 4,
    experts: 8,
    routed_experts: 8,
    top_k: 1,
    top_k_per_layer: topK(4, 1),
    ranks: ranks(members),
    route_scope: "replicated",
  };
  return [
    started(g, ["engine.step"], { members, model_id: "geom-only" }),
    envelope(1, "step.completed", {
      request_id: "q1",
      member: "spark1",
      rank: 0,
      step: 0,
      started_at_ns: 1,
      duration_ns: 1,
      token_rows: [{ row: 0, accepted: true, token_id: 1, text: "z", routes: null }],
    }),
    envelope(2, "run.finished", { reason: "stop" }),
  ];
}

export const FIXTURES = {
  ds: fixtureDsShaped,
  small: fixtureSmall,
  noroute: fixtureNoRouting,
  scale: fixtureScaleBlend,
  two: fixtureTwoRequests,
  ranks: fixtureTwoRanks,
  gap: fixtureGap,
  geomcap: fixtureGeomNoRouteCap,
};
