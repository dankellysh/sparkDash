/**
 * FrameReducer: fold hub-output dgx.trace-event.v1 into a scene.
 * Wire shape is Ajv vs frozen schemas (validateHub.js). This file is stream
 * semantics only. Canonical copy; sparkDash src/viz must stay in sync.
 */
import { hubEventErrors } from "./validateHub.js";

export const BUNDLE_SHA256 =
  "ec10961ac1edcc27a0ea29eebcd273fae0fac88fc276e26ddd41608ce99945c8";

export class ReduceError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
    this.name = "ReduceError";
  }
}

function dict() {
  return Object.create(null);
}

export function emptyScene() {
  return {
    run_id: null,
    model_id: null,
    members: [],
    capabilities: [],
    geometry: null,
    schema_bundle_hash: null,
    cursor: { run_id: null, hub_order: -1 },
    complete: false,
    gaps: [],
    requests: dict(),
    text: dict(),
    snapshots: dict(),
    heat: null,
    routing_unavailable: true,
    speculative_unavailable: true,
    last_error: null,
    _seenProdSeq: dict(),
    _seenStep: dict(),
  };
}

function cellKey(layer, expert) {
  return `${layer}:${expert}`;
}

function bumpHeat(scene, routes) {
  if (scene.routing_unavailable || !scene.heat || !routes) return;
  const g = scene.geometry;
  if (!Array.isArray(routes) || routes.length !== g.layers) {
    throw new ReduceError("routes", `routes length ${routes?.length} != layers ${g.layers}`);
  }
  for (let layer = 0; layer < routes.length; layer++) {
    const sel = routes[layer];
    if (!Array.isArray(sel)) {
      throw new ReduceError("routes", `layer ${layer} routes must be an array`);
    }
    if (new Set(sel).size !== sel.length) {
      throw new ReduceError("routes", `layer ${layer} duplicate expert`);
    }
    const cap = g.top_k_per_layer[layer];
    if (sel.length > cap) {
      throw new ReduceError("top_k", `layer ${layer} selected ${sel.length} > ${cap}`);
    }
    for (const expert of sel) {
      if (!Number.isInteger(expert) || expert < 0 || expert >= g.experts) {
        throw new ReduceError("route_bounds", `expert ${expert} out of [0, ${g.experts})`);
      }
      const k = cellKey(layer, expert);
      scene.heat.counts[k] = (scene.heat.counts[k] || 0) + 1;
      scene.heat.hits += 1;
    }
  }
}

export function heatBlend(scene, maxW = 1920, maxH = 1080) {
  const g = scene.geometry;
  if (scene.routing_unavailable || !g || !scene.heat || g.experts <= 0) {
    return { scale: 1, width: 0, height: 0, cells: [] };
  }
  let scale = 1;
  while (
    Math.ceil(g.experts / scale) * 2 > maxW ||
    Math.ceil(g.layers / scale) * 2 > maxH
  ) {
    scale *= 4;
    if (scale > 1024) break;
  }
  const width = Math.ceil(g.experts / scale);
  const height = Math.ceil(g.layers / scale);
  const acc = new Float64Array(width * height);
  const n = new Uint32Array(width * height);
  for (const [k, v] of Object.entries(scene.heat.counts)) {
    const [ls, es] = k.split(":");
    const x = Math.floor(Number(es) / scale);
    const y = Math.floor(Number(ls) / scale);
    const i = y * width + x;
    acc[i] += v;
    n[i] += 1;
  }
  const cells = [];
  for (let i = 0; i < acc.length; i++) cells.push(n[i] ? acc[i] / n[i] : 0);
  return { scale, width, height, cells };
}

function applyStarted(scene, ev) {
  if (scene.run_id) {
    throw new ReduceError("lifecycle", "second run.started");
  }
  const p = ev.payload;
  if (p.schema_version !== 1) {
    throw new ReduceError("schema_version", `unknown schema_version ${p.schema_version}`);
  }
  if (p.schema_bundle_hash !== BUNDLE_SHA256) {
    throw new ReduceError("bundle_hash", "schema_bundle_hash does not match hub bundle");
  }
  const g = p.geometry;
  if (g.top_k_per_layer.length !== g.layers) {
    throw new ReduceError("geometry", "top_k_per_layer length != layers");
  }
  const mx = g.top_k_per_layer.reduce((a, b) => Math.max(a, b), 0);
  if (g.top_k !== mx) {
    throw new ReduceError("geometry", "top_k must be max(top_k_per_layer)");
  }
  scene.run_id = ev.run_id;
  scene.model_id = p.model_id;
  scene.members = p.members.slice();
  scene.capabilities = p.capabilities.slice();
  scene.geometry = {
    ...g,
    ranks: g.ranks.map((r) => ({ ...r })),
    top_k_per_layer: g.top_k_per_layer.slice(),
  };
  scene.schema_bundle_hash = p.schema_bundle_hash;
  scene.routing_unavailable = !p.capabilities.includes("routing.experts");
  scene.speculative_unavailable = !p.capabilities.includes("speculative.rows");
  if (!scene.routing_unavailable && g.experts > 0) {
    scene.heat = { counts: dict(), hits: 0 };
  } else {
    scene.heat = null;
  }
}

function stepStoreKey(scene, p) {
  if (scene.geometry.route_scope === "rank") {
    if (p.rank == null || p.member == null) {
      throw new ReduceError("rank", "rank-scoped step needs member and rank");
    }
    return `${p.step}#${p.rank}`;
  }
  return String(p.step);
}

function applyStep(scene, ev) {
  const p = ev.payload;
  if (scene.geometry.route_scope === "rank") {
    const ok = scene.geometry.ranks.some((r) => r.member === p.member && r.rank === p.rank);
    if (!ok) throw new ReduceError("rank", "member/rank not in geometry.ranks");
  }
  const sem = `${p.request_id}\0${stepStoreKey(scene, p)}`;
  if (Object.prototype.hasOwnProperty.call(scene._seenStep, sem)) {
    return;
  }
  if (!Object.prototype.hasOwnProperty.call(scene.requests, p.request_id)) {
    scene.requests[p.request_id] = { steps: dict() };
  }
  const key = stepStoreKey(scene, p);
  scene._seenStep[sem] = true;
  scene.requests[p.request_id].steps[key] = {
    request_id: p.request_id,
    step: p.step,
    member: p.member ?? null,
    rank: p.rank ?? null,
    token_rows: p.token_rows,
    duration_ns: p.duration_ns,
  };
  for (const row of p.token_rows) {
    if (scene.routing_unavailable) {
      if (row.routes != null) {
        throw new ReduceError("capability", "routes present without routing.experts");
      }
    } else {
      bumpHeat(scene, row.routes);
    }
  }
}

export function reduceEvent(scene, ev) {
  const schemaErrs = hubEventErrors(ev);
  if (schemaErrs) {
    throw new ReduceError("schema", schemaErrs.join("; "));
  }
  if (scene.cursor.hub_order >= 0 && ev.hub_order <= scene.cursor.hub_order) {
    return scene;
  }
  if (scene.complete) {
    throw new ReduceError("lifecycle", "event after run.finished");
  }
  if (scene.run_id && ev.run_id !== scene.run_id) {
    throw new ReduceError("run_id", "run_id mismatch");
  }
  const next = scene.cursor.hub_order + 1;
  const prodKey = `${ev.producer_id}\0${ev.seq}`;
  if (scene.cursor.hub_order === -1) {
    if (ev.hub_order !== 0 || ev.type !== "run.started") {
      throw new ReduceError("lifecycle", "first event must be run.started at hub_order 0");
    }
  } else if (ev.type === "gap") {
    const a = ev.payload.first_missing_hub_order;
    const b = ev.payload.last_missing_hub_order;
    if (a > b) throw new ReduceError("gap", "first > last");
    if (a !== next || b !== ev.hub_order - 1) {
      throw new ReduceError("gap", "gap must cover skipped hub_order before this stamp");
    }
  } else if (ev.hub_order !== next) {
    throw new ReduceError("order", `expected hub_order ${next}, got ${ev.hub_order}`);
  }
  if (Object.prototype.hasOwnProperty.call(scene._seenProdSeq, prodKey)) {
    scene.cursor = { run_id: ev.run_id, hub_order: ev.hub_order };
    return scene;
  }
  switch (ev.type) {
    case "run.started":
      applyStarted(scene, ev);
      break;
    case "step.completed":
      applyStep(scene, ev);
      break;
    case "output.delta":
      scene.text[ev.payload.request_id] =
        (scene.text[ev.payload.request_id] || "") + ev.payload.text;
      break;
    case "node.snapshot":
      scene.snapshots[ev.payload.node] = ev.payload;
      break;
    case "gap":
      scene.gaps.push({
        first: ev.payload.first_missing_hub_order,
        last: ev.payload.last_missing_hub_order,
        reason: ev.payload.reason,
      });
      break;
    case "run.finished":
      scene.complete = true;
      break;
    default:
      throw new ReduceError("type", `unknown type ${ev.type}`);
  }
  scene._seenProdSeq[prodKey] = true;
  scene.cursor = { run_id: ev.run_id, hub_order: ev.hub_order };
  return scene;
}

export function reduceAll(events) {
  const scene = emptyScene();
  for (const ev of events) reduceEvent(scene, ev);
  return scene;
}

export function sceneFingerprint(scene) {
  const { _seenProdSeq, _seenStep, ...rest } = scene;
  return JSON.stringify(rest);
}
