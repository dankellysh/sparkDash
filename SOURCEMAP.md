# Fork source map

- Upstream: `MiaAI-Lab/sparkDash` (MIT).
- This fork: `dankellysh/sparkDash` branch `production` (not `main`).
- Promote onto `production` is an explicit step (DEC-085).
- `server/authStub.js`: local no-op. Not upstream. LAN-trust; not a login product.
- TokenTrace: **translated**, not copied. Reference
  `muojp/sparkDash@f5b3a9c7ddfda4011a728988f5aab4f7671a37e3`
  `src/components/TokenTracePage/TokenTracePage.tsx` and
  `TokenTraceFullscreenCanvas.tsx` (MIT). Local stand-ins:
  `src/viz/VizPage.tsx` (step/request + heatmap), `src/viz/frameReducer.js`.
  Canonical reducer also at DGXSpark `adv_viz/viz/` (must stay in sync).
  Hub-output validation is Ajv vs frozen `src/viz/contracts/*.schema.json`.
- Collect-once hub: `server/observe/hub.js` Ajv-validates standalone NodeSnapshot
  from `server/observe/contracts/` (copied into the production image with `server/`).
  Fallback HTTP→SSH is `server/observe/fallback.js`. Spark UMA uses `unifiedMemory` (MiB).
- `server/authStub.js`: local no-op. Not upstream. LAN-trust; not a login product.
