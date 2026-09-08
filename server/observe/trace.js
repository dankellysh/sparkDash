/**
 * Trace ingest: Ajv producer shape, stamp hub_order, record, fan-out.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Ajv from "ajv/dist/2020.js";
import { createRecorder } from "./record.js";

const DIR = path.dirname(fileURLToPath(import.meta.url));
const TRACE_PATH = path.join(DIR, "contracts/trace-event-v1.schema.json");
const NODE_PATH = path.join(DIR, "contracts/node-snapshot-v1.schema.json");
const MANIFEST_PATH = path.join(DIR, "contracts/MANIFEST.json");

const ajv = new Ajv({ allErrors: true, strict: false, validateFormats: false });
const nodeSchema = JSON.parse(fs.readFileSync(NODE_PATH, "utf8"));
const traceSchema = JSON.parse(fs.readFileSync(TRACE_PATH, "utf8"));
ajv.addSchema(nodeSchema);
ajv.addSchema(traceSchema);
const validateIngest = ajv.getSchema(traceSchema.$id) || ajv.compile(traceSchema);
const validateHub = ajv.getSchema(`${traceSchema.$id}#hub-output`);
if (!validateIngest || !validateHub) throw new Error("failed to compile trace schemas");

export const BUNDLE_SHA256 = JSON.parse(fs.readFileSync(MANIFEST_PATH, "utf8")).bundle_sha256;

function dict() {
  return Object.create(null);
}

function ajvErr(validate) {
  return (validate.errors || []).map((e) => `${e.instancePath || "/"} ${e.message}`).join("; ");
}

function safeRunId(run_id) {
  if (typeof run_id !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(run_id)) {
    throw new Error("bad run_id");
  }
}

export function createTraceHub(options = {}) {
  const recorder = options.recorder || createRecorder(options.record);
  const nowFn = options.now || (() => Date.now());
  const broadcast = options.broadcast || (() => {});
  const bundle = options.bundleHash || BUNDLE_SHA256;
  const seq = dict();
  const order = dict();

  for (const meta of recorder.listRuns()) {
    if (meta.status !== "incomplete") continue;
    if (!recorder.isOpen(meta.run_id)) continue;
    const committed = recorder.lastCommitted(meta.run_id);
    if (committed.unreadable) continue;
    order[meta.run_id] = committed.order;
  }

  function nextOrder(run_id) {
    const n = order[run_id] == null ? 0 : order[run_id] + 1;
    order[run_id] = n;
    return n;
  }

  function flushNodes(run_id, force) {
    const held = recorder.takeNodes(run_id, force);
    for (let i = 0; i < held.length; i++) {
      const payload = held[i];
      const observed_at_ns = payload.observed_at_ns || nowFn() * 1e6;
      const body = { ...payload };
      delete body.observed_at_ns;
      const hub_order = order[run_id] == null ? 0 : order[run_id] + 1;
      const event = {
        contract: "dgx.trace-event.v1",
        producer_id: "hub-coalesce",
        seq: hub_order,
        run_id,
        hub_order,
        observed_at_ns,
        type: "node.snapshot",
        payload: body,
      };
      const restoreRest = () => {
        recorder.holdNode(run_id, body.node, payload);
        for (let j = i + 1; j < held.length; j++) {
          const rest = held[j];
          recorder.holdNode(run_id, rest.node, rest);
        }
      };
      if (!validateHub(event)) {
        restoreRest();
        throw new Error(`invalid hub-output: ${ajvErr(validateHub)}`);
      }
      try {
        recorder.append(run_id, event);
      } catch (err) {
        restoreRest();
        throw err;
      }
      order[run_id] = hub_order;
      broadcast(event);
    }
  }

  async function ingest(raw) {
    if (!validateIngest(raw)) return { error: `invalid ingest: ${ajvErr(validateIngest)}` };
    try {
      safeRunId(raw.run_id);
    } catch (err) {
      return { error: err.message };
    }
    const run_id = raw.run_id;
    const key = `${run_id}:${raw.producer_id}`;
    const prev = seq[key];
    if (prev != null && raw.seq === prev) return { duplicate: true };
    if (prev != null && raw.seq !== prev + 1) {
      return { error: `stale sequence producer=${raw.producer_id} seq=${raw.seq}` };
    }

    if (raw.type === "run.started") {
      if (raw.payload.schema_bundle_hash !== bundle) {
        return { error: "schema_bundle_hash mismatch" };
      }
      if (recorder.hasRun(run_id) || order[run_id] != null) return { error: "run already started" };
      if (recorder.activeIds().length >= 1) return { error: "another run is recording" };
      try {
        recorder.openRun(run_id, bundle);
      } catch (err) {
        return { error: String(err.message || err) };
      }
    } else if (!recorder.isOpen(run_id)) {
      return { error: `no open run ${run_id}` };
    }

    if (raw.type !== "run.started") {
      try {
        flushNodes(run_id, true);
      } catch (err) {
        return { error: String(err.message || err) };
      }
    }

    const hub_order = order[run_id] == null ? 0 : order[run_id] + 1;
    if (raw.type === "run.started" && hub_order !== 0) {
      return { error: "run.started must be hub_order 0" };
    }
    const stamped = { ...raw, hub_order };
    if (!validateHub(stamped)) return { error: `invalid hub-output: ${ajvErr(validateHub)}` };

    try {
      recorder.append(run_id, stamped);
    } catch (err) {
      if (raw.type === "run.started") recorder.abortRun(run_id);
      return { error: String(err.message || err) };
    }
    order[run_id] = hub_order;
    seq[key] = raw.seq;
    broadcast(stamped);
    if (raw.type === "run.finished") {
      recorder.markClosing(run_id);
      try {
        await recorder.finish(run_id);
      } catch (err) {
        return { error: String(err.message || err) };
      }
    }
    return { event: stamped };
  }

  function noteNodeSnapshot(snap) {
    const ids = recorder.activeIds();
    if (!ids.length) return;
    const run_id = ids[0];
    if (recorder.isClosing(run_id)) return;
    recorder.holdNode(run_id, snap.node, {
      node: snap.node,
      monotonic_ns: snap.monotonic_ns,
      source_age_ms: snap.source_age_ms,
      quality: snap.quality,
      capabilities: snap.capabilities || ["node.steady"],
      metrics: snap.metrics || {},
      observed_at_ns: snap.observed_at_ns,
    });
    try {
      flushNodes(run_id, false);
    } catch {
      /* replaceable */
    }
  }

  return {
    ingest,
    noteNodeSnapshot,
    recorder,
    bundle,
  };
}

let _hub;
let _fanout = () => {};
export function setTraceFanout(fn) {
  _fanout = fn;
}
export function getTraceHub() {
  if (!_hub) _hub = createTraceHub({ broadcast: (ev) => _fanout(ev) });
  return _hub;
}

export function resetTraceHubForTests() {
  _hub = null;
}
