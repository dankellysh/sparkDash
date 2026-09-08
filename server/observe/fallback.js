/**
 * Silent-pusher ladder: HTTP exporters → cooldown SSH → stale/unknown.
 * Does not restore the old per-domain SSH poll fan-out.
 */
import { buildNodeSnapshot } from "./exporters.js";

export const FALLBACK_SILENT_MS = 4000;
export const FALLBACK_SSH_COOLDOWN_MS = 60000;

function readTime(value) {
  return typeof value === "function" ? value() : value;
}

export async function fetchExporterText(url, fetchImpl = fetch) {
  try {
    const r = await fetchImpl(url, { signal: AbortSignal.timeout(2000) });
    if (!r.ok) return null;
    return await r.text();
  } catch {
    return null;
  }
}

export async function runFallbackTick(ctx) {
  if (ctx.isInflight?.()) return { action: "inflight" };
  ctx.setInflight?.(true);
  try {
    const now = ctx.now();
    const silentMs = ctx.silentMs ?? FALLBACK_SILENT_MS;
    const lastInboundAt = readTime(ctx.lastInboundAt);
    if (now - lastInboundAt < silentMs) return { action: "skip-fresh" };

    const mono = ctx.mono || (() => now / 1000);
    const n0 = mono();
    const nodeTxt = await ctx.fetchNode();
    const n1 = mono();
    const d0 = n1;
    const dcgmTxt = await ctx.fetchDcgm();
    const stamp = mono();
    const nodeOk = nodeTxt != null;
    const dcgmOk = dcgmTxt != null;

    if (readTime(ctx.lastInboundAt) > lastInboundAt && ctx.now() - readTime(ctx.lastInboundAt) < silentMs) {
      return { action: "skip-recovered" };
    }

    if (nodeOk || dcgmOk) {
      const snap = buildNodeSnapshot({
        node: ctx.nodeId,
        producerId: `${ctx.nodeId}-fallback-http`,
        seq: ctx.nextSeq(),
        nodeText: nodeOk ? nodeTxt : null,
        dcgmText: dcgmOk ? dcgmTxt : null,
        nodeOk,
        dcgmOk,
        nowNs: ctx.nowNs ? ctx.nowNs() : Date.now() * 1e6,
        monoNs: ctx.monoNs ? ctx.monoNs() : Number(process.hrtime.bigint()),
        cpuState: ctx.cpuState,
        fallback: true,
        nowMs: now,
        stampMono: stamp,
        nodeCollectedMono: n0,
        dcgmCollectedMono: d0,
      });
      if (readTime(ctx.lastInboundAt) > lastInboundAt) return { action: "skip-recovered" };
      const err = ctx.ingest(snap);
      if (typeof err === "string") return { action: "http-reject", error: err };
      if (err?.duplicate) return { action: "http-duplicate" };
      ctx.apply(snap);
      ctx.setOnline(true);
      return { action: "http", snap };
    }

    const cooldown = ctx.sshCooldownMs ?? FALLBACK_SSH_COOLDOWN_MS;
    const lastSshAt = readTime(ctx.lastSshAt);
    if (lastSshAt > 0 && now - lastSshAt < cooldown) return { action: "ssh-cooldown" };
    ctx.noteSsh(now);
    const ssh = await ctx.sshTest();
    const inboundAfter = readTime(ctx.lastInboundAt);
    if (inboundAfter > lastInboundAt && ctx.now() - inboundAfter < silentMs) {
      return { action: "skip-recovered" };
    }
    if (ssh && ssh.ok) {
      ctx.setOnline(true);
      ctx.markUnknown?.();
      return { action: "ssh-ok" };
    }
    ctx.setOnline(false);
    ctx.markUnknown?.();
    return { action: "ssh-fail", ssh };
  } finally {
    ctx.setInflight?.(false);
  }
}
