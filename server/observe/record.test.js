import assert from "node:assert/strict";
import { test } from "node:test";
import fs from "fs";
import os from "os";
import path from "path";
import { createRecorder, parseRecordingYaml } from "./record.js";

const yaml = `dir: /tmp/x
per_run_bytes: 1000
total_bytes: 5000
min_free_bytes: 10
queue_events: 8
rss_bytes: 100000
coalesce_node_ms: 5
zstd: /usr/bin/zstd
`;

test("parseRecordingYaml requires caps", () => {
  const c = parseRecordingYaml(yaml);
  assert.equal(c.per_run_bytes, 1000);
  assert.throws(() => parseRecordingYaml("dir: /tmp\n"));
  assert.throws(() => parseRecordingYaml(yaml.replace("1000", "banana")));
});

test("jsonl append then zstd finish; node coalesce", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rec-"));
  const rec = createRecorder({
    config: {
      dir,
      per_run_bytes: 10_000,
      total_bytes: 100_000,
      min_free_bytes: 1,
      queue_events: 8,
      rss_bytes: 1e12,
      coalesce_node_ms: 50,
      zstd: "/usr/bin/zstd",
    },
    now: () => 1000,
    rss: () => 1,
    freeDisk: () => 1e12,
    compress: async (src, dest) => {
      fs.copyFileSync(src, dest);
    },
    decompress: async (src) => fs.readFileSync(src, "utf8"),
  });
  rec.openRun("r1", "ab".repeat(32));
  rec.append("r1", { hub_order: 0, type: "run.started" });
  rec.holdNode("r1", "spark1", { node: "spark1" });
  assert.equal(rec.takeNodes("r1", false).length, 0);
  const flushed = rec.takeNodes("r1", true);
  assert.equal(flushed.length, 1);
  rec.append("r1", flushed[0]);
  await rec.finish("r1");
  const meta = rec.listRuns()[0];
  assert.equal(meta.status, "final");
  assert.ok(fs.existsSync(path.join(dir, "r1", "events.jsonl.zst")));
  const evs = await rec.readEvents("r1");
  assert.equal(evs.length, 2);
});

test("torn last newline is repaired before append", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rec-"));
  const rec = createRecorder({
    config: {
      dir,
      per_run_bytes: 10_000,
      total_bytes: 100_000,
      min_free_bytes: 1,
      queue_events: 8,
      rss_bytes: 1e12,
      coalesce_node_ms: 1,
      zstd: "/usr/bin/zstd",
    },
    rss: () => 1,
    freeDisk: () => 1e12,
    compress: async (src, dest) => fs.copyFileSync(src, dest),
    decompress: async (src) => fs.readFileSync(src, "utf8"),
    decompressSync: (src) => fs.readFileSync(src, "utf8"),
  });
  rec.openRun("r1", "ab".repeat(32));
  rec.append("r1", { hub_order: 0, type: "run.started" });
  const p = path.join(dir, "r1", "events.jsonl");
  fs.writeFileSync(p, fs.readFileSync(p, "utf8").replace(/\n$/, ""));
  const rec2 = createRecorder({
    config: rec.cfg,
    rss: () => 1,
    freeDisk: () => 1e12,
    compress: async (src, dest) => fs.copyFileSync(src, dest),
    decompress: async (src) => fs.readFileSync(src, "utf8"),
    decompressSync: (src) => fs.readFileSync(src, "utf8"),
  });
  rec2.append("r1", { hub_order: 1, type: "output.delta" });
  const evs = await rec2.readEvents("r1");
  assert.equal(evs.length, 2);
});

test("zst-only finished archive is not reopened", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rec-"));
  const cfg = {
    dir,
    per_run_bytes: 10_000,
    total_bytes: 100_000,
    min_free_bytes: 1,
    queue_events: 8,
    rss_bytes: 1e12,
    coalesce_node_ms: 1,
    zstd: "/usr/bin/zstd",
  };
  const rec = createRecorder({
    config: cfg,
    rss: () => 1,
    freeDisk: () => 1e12,
    compress: async (src, dest) => fs.copyFileSync(src, dest),
    decompress: async (src) => fs.readFileSync(src, "utf8"),
    decompressSync: (src) => fs.readFileSync(src, "utf8"),
  });
  rec.openRun("r1", "ab".repeat(32));
  rec.append("r1", { hub_order: 0, type: "run.started" });
  rec.append("r1", { hub_order: 1, type: "run.finished", payload: { reason: "stop" } });
  await rec.finish("r1");
  fs.writeFileSync(path.join(dir, "r1", "run.json"), JSON.stringify({ run_id: "r1", status: "incomplete" }));
  const rec2 = createRecorder({
    config: cfg,
    rss: () => 1,
    freeDisk: () => 1e12,
    compress: async (src, dest) => fs.copyFileSync(src, dest),
    decompress: async (src) => fs.readFileSync(src, "utf8"),
    decompressSync: (src) => fs.readFileSync(src, "utf8"),
  });
  assert.equal(rec2.isOpen("r1"), false);
  assert.equal((await rec2.readEvents("r1")).length, 2);
});

test("finish does not evict its own committed jsonl", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rec-"));
  const rec = createRecorder({
    config: {
      dir,
      per_run_bytes: 100000,
      total_bytes: 8000,
      min_free_bytes: 1,
      queue_events: 8,
      rss_bytes: 1e12,
      coalesce_node_ms: 1,
      zstd: "/usr/bin/zstd",
    },
    rss: () => 1,
    freeDisk: () => 1e12,
    compress: async (src, dest) => fs.copyFileSync(src, dest),
    decompress: async (src) => fs.readFileSync(src, "utf8"),
    decompressSync: (src) => fs.readFileSync(src, "utf8"),
  });
  rec.openRun("r1", "ab".repeat(32));
  rec.append("r1", { hub_order: 0, type: "run.started" });
  rec.append("r1", { hub_order: 1, type: "output.delta", payload: { text: "x".repeat(3500) } });
  rec.append("r1", { hub_order: 2, type: "run.finished" });
  try {
    await rec.finish("r1");
  } catch {
    /* cap may refuse compression extra; jsonl must remain */
  }
  const p = path.join(dir, "r1", "events.jsonl");
  assert.ok(fs.existsSync(p) || fs.existsSync(p + ".zst"));
  assert.equal((await rec.readEvents("r1")).length, 3);
  assert.equal(rec.isOpen("r1"), false);
  assert.equal(rec.isClosing("r1"), false);
});

test("finish releases hold if source stat throws", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rec-"));
  const rec = createRecorder({
    config: {
      dir,
      per_run_bytes: 100000,
      total_bytes: 4096,
      min_free_bytes: 1,
      queue_events: 8,
      rss_bytes: 1e12,
      coalesce_node_ms: 1,
      zstd: "/usr/bin/zstd",
    },
    rss: () => 1,
    freeDisk: () => 1e12,
    compress: async (src, dest) => fs.copyFileSync(src, dest),
    decompress: async (src) => fs.readFileSync(src, "utf8"),
    decompressSync: (src) => fs.readFileSync(src, "utf8"),
  });
  rec.openRun("r1", "ab".repeat(32));
  rec.append("r1", { hub_order: 0, type: "run.started" });
  rec.append("r1", { hub_order: 1, type: "run.finished" });
  const orig = fs.statSync;
  let once = true;
  fs.statSync = function patchedStat(p, ...rest) {
    if (once && String(p).endsWith(`${path.sep}events.jsonl`)) {
      once = false;
      const err = new Error("EIO");
      err.code = "EIO";
      throw err;
    }
    return orig.call(this, p, ...rest);
  };
  try {
    await assert.rejects(() => rec.finish("r1"), /EIO/);
  } finally {
    fs.statSync = orig;
  }
  assert.equal(rec.isClosing("r1"), false);
  assert.equal(rec.isOpen("r1"), false);
  assert.equal((await rec.readEvents("r1")).length, 2);
  rec.openRun("r2", "ab".repeat(32));
  assert.equal(rec.hasRun("r1"), false);
});

test("per-run cap refuses extra bytes", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rec-"));
  const rec = createRecorder({
    config: {
      dir,
      per_run_bytes: 600,
      total_bytes: 100000,
      min_free_bytes: 1,
      queue_events: 8,
      rss_bytes: 1e12,
      coalesce_node_ms: 1,
      zstd: "/usr/bin/zstd",
    },
    rss: () => 1,
    freeDisk: () => 1e12,
  });
  rec.openRun("r1", "ab".repeat(32));
  rec.append("r1", { hub_order: 0, x: "a" });
  assert.throws(() => rec.append("r1", { hub_order: 1, x: "bbbbbbbbbbbbbbbbbbbbbbbb" }));
});
