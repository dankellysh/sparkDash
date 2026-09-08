import { useMemo, useRef, useEffect, useState } from "react";
import { heatBlend, reduceAll } from "./frameReducer.js";
import { FIXTURES } from "./fixtures.js";

const NAMES = ["ds", "small", "noroute", "scale", "two", "ranks", "gap", "geomcap"] as const;

function pickFixture(): (typeof NAMES)[number] {
  const q = new URLSearchParams(window.location.search).get("f");
  if (q && (NAMES as readonly string[]).includes(q)) return q as (typeof NAMES)[number];
  return "small";
}

export function VizPage() {
  const [name, setName] = useState(pickFixture);
  const scene = useMemo(() => reduceAll(FIXTURES[name]()), [name]);
  const blend = useMemo(() => heatBlend(scene), [scene]);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const requestIds = Array.from(
    new Set([...Object.keys(scene.requests), ...Object.keys(scene.text)])
  );
  const [req, setReq] = useState(requestIds[0] || "");
  const stepKeys = req && scene.requests[req] ? Object.keys(scene.requests[req].steps) : [];
  const [stepKey, setStepKey] = useState(stepKeys[0] || "");
  const rec = req && stepKey ? scene.requests[req]?.steps[stepKey] : null;

  useEffect(() => {
    const ids = Array.from(
      new Set([...Object.keys(scene.requests), ...Object.keys(scene.text)])
    );
    setReq(ids[0] || "");
    const sk = ids[0] && scene.requests[ids[0]] ? Object.keys(scene.requests[ids[0]].steps) : [];
    setStepKey(sk[0] || "");
  }, [scene]);

  useEffect(() => {
    const c = canvasRef.current;
    if (!c) return;
    const ctx = c.getContext("2d");
    if (!ctx) return;
    const w = Math.max(blend.width, 1);
    const h = Math.max(blend.height, 1);
    c.width = w;
    c.height = h;
    ctx.fillStyle = "#1a1a18";
    ctx.fillRect(0, 0, w, h);
    if (scene.routing_unavailable) return;
    let max = 0;
    for (const v of blend.cells) if (v > max) max = v;
    for (let y = 0; y < blend.height; y++) {
      for (let x = 0; x < blend.width; x++) {
        const v = blend.cells[y * blend.width + x] || 0;
        const t = max > 0 ? v / max : 0;
        ctx.fillStyle = `oklch(${0.3 + 0.5 * t} ${0.04 + 0.12 * t} 35)`;
        ctx.fillRect(x, y, 1, 1);
      }
    }
  }, [blend, scene.routing_unavailable]);

  return (
    <div className="min-h-screen bg-base p-6 text-text">
      <header className="mb-4 flex flex-wrap items-center gap-3">
        <a href="/" className="logo-pill">
          spark<span className="logo-pill-dash">Dash</span> viz
        </a>
        <label className="text-xs text-muted">
          fixture{" "}
          <select
            className="rounded border border-border bg-surface px-2 py-1 text-text"
            value={name}
            onChange={(e) => {
              const n = e.target.value as (typeof NAMES)[number];
              setName(n);
              window.history.replaceState(null, "", `/viz?f=${n}`);
            }}
          >
            {NAMES.map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </label>
        <span className="text-xs text-muted">
          {scene.model_id} · cursor {scene.cursor.hub_order} · scale {blend.scale}:1
        </span>
      </header>
      {scene.gaps.length > 0 && (
        <p className="mb-2 text-xs text-muted">
          gaps {scene.gaps.map((g) => `${g.first}-${g.last} (${g.reason})`).join("; ")}
        </p>
      )}
      {req && scene.text[req] && (
        <p className="mb-2 text-sm text-text-strong">output: {scene.text[req]}</p>
      )}
      <p className="mb-2 text-xs text-muted">
        topology {scene.members.join(", ") || "—"} ·{" "}
        {scene.routing_unavailable
          ? "expert map unavailable"
          : `${scene.geometry?.layers}×${scene.geometry?.experts}`}
        {scene.speculative_unavailable ? " · speculative unavailable" : ""}
      </p>
      <div className="mb-3 flex flex-wrap gap-2 text-xs">
        <label>
          request{" "}
          <select
            className="rounded border border-border bg-surface px-2 py-1"
            value={req}
            onChange={(e) => setReq(e.target.value)}
          >
            {requestIds.map((id) => (
              <option key={id} value={id}>
                {id}
              </option>
            ))}
          </select>
        </label>
        <label>
          step{" "}
          <select
            className="rounded border border-border bg-surface px-2 py-1"
            value={stepKey}
            onChange={(e) => setStepKey(e.target.value)}
          >
            {stepKeys.map((id) => (
              <option key={id} value={id}>
                {id}
              </option>
            ))}
          </select>
        </label>
      </div>
      <ul className="mb-4 flex flex-wrap gap-2">
        {(rec?.token_rows || []).map((row) => (
          <li
            key={row.row}
            className={`rounded px-2 py-1 text-xs ${row.accepted ? "bg-success/20" : "bg-danger/20"}`}
          >
            row {row.row} {row.accepted ? "accept" : "reject"} {row.text ?? ""}
          </li>
        ))}
      </ul>
      <div className="overflow-auto rounded-xl border border-border bg-surface p-3">
        {scene.routing_unavailable ? (
          <p className="text-sm text-muted">Expert map unavailable (no routing.experts capability).</p>
        ) : (
          <canvas
            ref={canvasRef}
            className="max-h-[70vh] w-full"
            style={{ imageRendering: "pixelated" }}
          />
        )}
      </div>
    </div>
  );
}
