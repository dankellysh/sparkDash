import fs from "fs";
import path from "path";
import { SystemCollector } from "../collectors/SystemCollector.js";
import { LlmProbe } from "../collectors/LlmProbe.js";
import { ComfyProbe } from "../collectors/ComfyProbe.js";
import { HermesProbe } from "../collectors/HermesProbe.js";
import { TailscaleProbe } from "../collectors/TailscaleProbe.js";
import { llmDaily } from "../collectors/LlmDaily.js";
import { sshTest, sshExec } from "../collectors/ssh.js";
import { getObserveHub, snapshotToPanels } from "../observe/hub.js";
import { fetchExporterText, runFallbackTick } from "../observe/fallback.js";
import {
  POLL_INTERVAL_LLM,
  POLL_INTERVAL_COMFY,
  LLM_PORT,
  COMFY_PORT,
  HOST_PATHS,
} from "../config.js";

const ONLINE_GRACE_MS = 10000;

/**
 * SparkMonitor — one per Spark. Owns collectors + rate state + poll loop.
 * Exposes snapshot() for WebSocket pushed payload.
 */
export class SparkMonitor {
  /**
   * @param {object} spark
   * @param {{ onWolMac?: (sparkId: string, mac: string) => void }} [options]
   */
  constructor(spark, options = {}) {
    this.spark = spark;
    this._onWolMac = typeof options.onWolMac === "function" ? options.onWolMac : null;
    this._onHermesChange =
      typeof options.onHermesChange === "function" ? options.onHermesChange : null;
    this.collector = new SystemCollector(spark);

    // One LlmProbe per port — none when LLM monitoring is off
    this.llmProbes = new Map();
    if (this._llmMonitoringEnabled(spark)) {
      for (const port of this._llmPorts()) {
        this.llmProbes.set(port, new LlmProbe(spark, port));
      }
    }

    /** @type {ComfyProbe | null} */
    this.comfyProbe = this._comfyMonitoringEnabled(spark)
      ? new ComfyProbe(spark, this._comfyPort(spark))
      : null;

    /** @type {TailscaleProbe | null} */
    this.tailscaleProbe = this._tailscaleMonitoringEnabled(spark)
      ? new TailscaleProbe(spark)
      : null;

    /** @type {HermesProbe | null} */
    this.hermesProbe = this._hermesMonitoringEnabled(spark)
      ? new HermesProbe(spark)
      : null;
    // Hermes status is surfaced in the snapshot (not under `metrics`) and is
    // always present so the UI never has to special-case a missing field.
    this._hermes = {
      monitoring: this._hermesMonitoringEnabled(spark),
      installed: null,
      version: null,
      updateAvailable: null,
      behindCommits: null,
      checkedAt: null,
      // idle | running | success | error
      status: "idle",
      startedAt: null,
      finishedAt: null,
      error: null,
    };

    // Online status from dedicated liveness checks (not metric poll success)
    this.online = false;
    this.lastOnlineOk = 0;

    // System uptime seconds (from /proc/uptime), null when offline
    this._uptimeSeconds = null;

    // Cached metrics per domain — never null objects for UI safety
    this._metrics = {
      gpu: this.collector._defaultGpu(),
      cpu: this.collector._defaultCpu(),
      ram: this.collector._defaultRam(),
      storage: [],
      network: this.collector._defaultNetwork(),
      unifiedMemory: this.collector._defaultUnifiedMemory(),
      llm: [],
      comfy: null,
      tailscale: null,
    };
    this._lastUpdate = {};

    // Hardware summary: kind "spark" uses the static DGX Spark specs; kind
    // "host" (dedicated GPU Linux box) detects real hardware once in the
    // background so the header doesn't mislabel the machine as a Spark.
    this._hardwareSummary = this._staticHardwareSummary(spark);
    this._stopped = false;
    if (spark?.kind === "host") {
      void this.collector
        .detectHardware()
        .then((detected) => {
          if (this._stopped || !detected) return;
          this._hardwareSummary = { ...this._hardwareSummary, ...detected };
        })
        .catch(() => {});
    }

    // Timers
    this._intervals = [];
    /** @type {ReturnType<typeof setInterval> | null} */
    this._llmIntervalId = null;
    /** @type {ReturnType<typeof setInterval> | null} */
    this._comfyIntervalId = null;
    /** @type {ReturnType<typeof setInterval> | null} */
    this._hermesIntervalId = null;
    /** @type {ReturnType<typeof setInterval> | null} */
    this._tailscaleIntervalId = null;
    this._running = false;
    /** @type {Record<string, boolean>} in-flight domain guards */
    this._inflight = {};
    this._lastInboundAt = 0;
    this._lastFallbackSshAt = 0;
    this._fallbackSeq = 0;
    this._cpuState = {};
    this._observeQuality = null;
    this._observedAtNs = null;
    this._inboundSnap = null;
    this._observeHub = options.observeHub || null;
    this._fallbackInflight = false;
  }

  /** Hot-update config without tearing down poll loops / rate baselines. */
  updateConfig(spark) {
    const wasLlm = this._llmMonitoringEnabled(this.spark);
    const wasComfy = this._comfyMonitoringEnabled(this.spark);
    const prevComfyPort = this._comfyPort(this.spark);
    const wasHermes = this._hermesMonitoringEnabled(this.spark);
    const wasTailscale = this._tailscaleMonitoringEnabled(this.spark);
    this.spark = spark;
    this.collector.spark = spark;

    // Rebuild LLM probe map — add new ports, remove stale ones, update existing
    const ports = this._llmMonitoringEnabled() ? this._llmPorts() : [];
    const prevProbes = this.llmProbes;
    this.llmProbes = new Map();
    for (const port of ports) {
      const existing = prevProbes.get(port);
      if (existing) {
        existing.spark = spark;
        this.llmProbes.set(port, existing);
      } else {
        this.llmProbes.set(port, new LlmProbe(spark, port));
      }
    }
    if (!this._llmMonitoringEnabled()) {
      this._metrics.llm = [];
    }

    // ComfyUI probe — create / update / clear
    if (this._comfyMonitoringEnabled()) {
      const port = this._comfyPort();
      if (this.comfyProbe) {
        this.comfyProbe.setTarget(spark, port);
      } else {
        this.comfyProbe = new ComfyProbe(spark, port);
      }
    } else {
      if (this.comfyProbe) {
        try {
          this.comfyProbe.dispose();
        } catch {
          /* ignore */
        }
      }
      this.comfyProbe = null;
      this._metrics.comfy = null;
    }

    // Tailscale probe — create / update / clear
    if (this._tailscaleMonitoringEnabled()) {
      if (this.tailscaleProbe) {
        this.tailscaleProbe.setTarget(spark);
      } else {
        this.tailscaleProbe = new TailscaleProbe(spark);
      }
    } else {
      this.tailscaleProbe = null;
      this._metrics.tailscale = null;
    }

    // Hermes probe — create / update / clear
    if (this._hermesMonitoringEnabled()) {
      if (this.hermesProbe) {
        this.hermesProbe.setTarget(spark);
      } else {
        this.hermesProbe = new HermesProbe(spark);
      }
    } else {
      this.hermesProbe = null;
      this._hermes.status = "idle";
    }
    this._hermes.monitoring = this._hermesMonitoringEnabled();
    if (this._running && wasHermes !== this._hermesMonitoringEnabled()) {
      this._restartHermesPollInterval();
    }

    // Toggle LLM poll interval when monitoring enablement flips
    if (this._running && wasLlm !== this._llmMonitoringEnabled()) {
      this._restartLlmPollInterval();
    }
    const comfyOn = this._comfyMonitoringEnabled();
    const comfyPortChanged = comfyOn && prevComfyPort !== this._comfyPort();
    if (this._running && (wasComfy !== comfyOn || comfyPortChanged)) {
      this._restartComfyPollInterval();
    }
    if (this._running && wasTailscale !== this._tailscaleMonitoringEnabled()) {
      this._restartTailscalePollInterval();
    }
  }

  /**
   * Workers: never. Head: always. Standalone: llmMonitoring (default true).
   * @param {object} [spark]
   */
  _llmMonitoringEnabled(spark = this.spark) {
    const role = spark?.role || (spark?.workerNode ? "worker" : "standalone");
    if (role === "worker") return false;
    if (role === "head") return true;
    return spark?.llmMonitoring !== false;
  }

  /** Start or clear the LLM poll timer based on monitoring flag. */
  _restartLlmPollInterval() {
    if (this._llmIntervalId != null) {
      clearInterval(this._llmIntervalId);
      this._intervals = this._intervals.filter((id) => id !== this._llmIntervalId);
      this._llmIntervalId = null;
    }
    if (this._llmMonitoringEnabled() && this._running) {
      this._llmIntervalId = setInterval(() => this._pollDomain("llm"), POLL_INTERVAL_LLM);
      this._intervals.push(this._llmIntervalId);
      void this._pollDomain("llm");
    }
  }

  /**
   * Opt-in ComfyUI monitoring (all roles; default off).
   * @param {object} [spark]
   */
  _comfyMonitoringEnabled(spark = this.spark) {
    return Boolean(spark?.comfyMonitoring);
  }

  /** @param {object} [spark] */
  _comfyPort(spark = this.spark) {
    const n = Number(spark?.comfyPort);
    if (Number.isInteger(n) && n >= 1 && n <= 65535) return n;
    return COMFY_PORT;
  }

  /** Start or clear the ComfyUI poll timer based on monitoring flag. */
  _restartComfyPollInterval() {
    if (this._comfyIntervalId != null) {
      clearInterval(this._comfyIntervalId);
      this._intervals = this._intervals.filter((id) => id !== this._comfyIntervalId);
      this._comfyIntervalId = null;
    }
    if (this._comfyMonitoringEnabled() && this._running) {
      this._comfyIntervalId = setInterval(() => this._pollDomain("comfy"), POLL_INTERVAL_COMFY);
      this._intervals.push(this._comfyIntervalId);
      void this._pollDomain("comfy");
    }
  }

  /**
   * Opt-in tailnet monitoring (all roles; default off).
   * @param {object} [spark]
   */
  _tailscaleMonitoringEnabled(spark = this.spark) {
    return Boolean(spark?.tailscaleMonitoring);
  }

  /** Clear any leftover tailnet timer. Scheduled SSH is not restarted. */
  _restartTailscalePollInterval() {
    if (this._tailscaleIntervalId != null) {
      clearInterval(this._tailscaleIntervalId);
      this._intervals = this._intervals.filter((id) => id !== this._tailscaleIntervalId);
      this._tailscaleIntervalId = null;
    }
  }

  /**
   * Opt-in Hermes Agent monitoring (all roles; default off).
   * @param {object} [spark]
   */
  _hermesMonitoringEnabled(spark = this.spark) {
    return Boolean(spark?.hermesMonitoring);
  }

  /** Clear any leftover Hermes timer. Scheduled SSH is not restarted. */
  _restartHermesPollInterval() {
    if (this._hermesIntervalId != null) {
      clearInterval(this._hermesIntervalId);
      this._intervals = this._intervals.filter((id) => id !== this._hermesIntervalId);
      this._hermesIntervalId = null;
    }
  }

  /** Returns array of LLM ports from spark config. */
  _llmPorts() {
    const raw = this.spark?.llmPorts;
    if (Array.isArray(raw)) {
      const ports = raw
        .map((v) => (typeof v === "string" ? parseInt(v, 10) : Number(v)))
        .filter((n) => Number.isInteger(n) && n >= 1 && n <= 65535);
      return ports.length > 0 ? ports : [LLM_PORT];
    }
    // Legacy single port
    const n = Number(this.spark?.llmPort);
    if (Number.isInteger(n) && n >= 1 && n <= 65535) return [n];
    return [LLM_PORT];
  }

  /** Start background polling. */
  start() {
    if (this._running) return;
    this._running = true;
    this._stopped = false;
    // Slice 2: no scheduled SSH for gpu/cpu/net/storage/ram/memory.
    // Inbound NodeSnapshot + fallback HTTP then rare SSH (see _fallbackTick).
    this._lastInboundAt = 0;
    this._lastFallbackSshAt = 0;
    this._fallbackSeq = 0;
    this._cpuState = {};
    this._observeQuality = null;
    this._observedAtNs = null;
    this._intervals.push(setInterval(() => this._fallbackTick(), 2000));
    this._restartLlmPollInterval();
    this._restartComfyPollInterval();
    // No scheduled Hermes/Tailscale SSH — those stay on-demand.
    console.log(`[SparkMonitor] ${this.spark.id} started (system SSH timers off)`);
  }

  applyInboundSnapshot(snap, panels, nowMs = Date.now()) {
    this._lastInboundAt = nowMs;
    this._inboundSnap = snap;
    const projected =
      panels || snapshotToPanels(snap, { now: nowMs, receivedAt: nowMs });
    this._observeQuality = projected?.quality ?? snap?.quality ?? null;
    this._observedAtNs = projected?.observed_at_ns ?? snap?.observed_at_ns ?? null;
    if (projected?.gpu) {
      this._metrics.gpu = { ...this._metrics.gpu, ...projected.gpu, power: { ...this._metrics.gpu?.power, ...projected.gpu.power } };
    }
    if (projected?.cpu) this._metrics.cpu = { ...this._metrics.cpu, ...projected.cpu };
    if (projected?.unifiedMemory) {
      this._metrics.unifiedMemory = { ...this._metrics.unifiedMemory, ...projected.unifiedMemory };
    }
    this.online = true;
  }

  async _fallbackTick() {
    const ip = this.spark.lanIp;
    if (!ip) return;
    const hub = this._observeHub || getObserveHub();
    await runFallbackTick({
      nodeId: this.spark.id,
      lastInboundAt: () => this._lastInboundAt,
      lastSshAt: () => this._lastFallbackSshAt,
      now: () => Date.now(),
      mono: () => Number(process.hrtime.bigint()) / 1e9,
      cpuState: this._cpuState,
      isInflight: () => this._fallbackInflight,
      setInflight: (v) => {
        this._fallbackInflight = v;
      },
      fetchNode: () => fetchExporterText(`http://${ip}:9100/metrics`),
      fetchDcgm: () => fetchExporterText(`http://${ip}:9400/metrics`),
      nextSeq: () => this._fallbackSeq++,
      ingest: (snap) => hub.ingest(snap),
      apply: (snap) => this.applyInboundSnapshot(snap),
      setOnline: (v) => {
        this.online = v;
      },
      markUnknown: () => {
        this._observeQuality = "unavailable";
      },
      sshTest: () => sshTest(this.spark),
      noteSsh: (t) => {
        this._lastFallbackSshAt = t;
      },
    });
  }

  /** Stop background polling. */
  stop() {
    this._running = false;
    this._stopped = true;
    for (const id of this._intervals) clearInterval(id);
    this._intervals = [];
    this._llmIntervalId = null;
    this._comfyIntervalId = null;
    this._hermesIntervalId = null;
    this._tailscaleIntervalId = null;
    this._inflight = {};
    if (this.comfyProbe) {
      try {
        this.comfyProbe.dispose();
      } catch {
        /* ignore */
      }
    }
    console.log(`[SparkMonitor] ${this.spark.id} stopped`);
  }

  /** Return a full snapshot of this Spark's metrics. */
  snapshot(nowMs = Date.now()) {
    const ports = this._llmMonitoringEnabled() ? this._llmPorts() : [];
    const comfyOn = this._comfyMonitoringEnabled();
    const tailscaleOn = this._tailscaleMonitoringEnabled();
    const now = nowMs;
    let gpu = this._metrics.gpu;
    let cpu = this._metrics.cpu;
    let unifiedMemory = this._metrics.unifiedMemory;
    if (this._inboundSnap) {
      const p = snapshotToPanels(this._inboundSnap, { now, receivedAt: this._lastInboundAt });
      gpu = { ...gpu, ...p.gpu, power: { ...gpu?.power, ...p.gpu.power } };
      cpu = { ...cpu, ...p.cpu };
      unifiedMemory = { ...unifiedMemory, ...p.unifiedMemory };
    }
    const metricsFresh =
      this._observeQuality !== "unavailable" &&
      (gpu?.usage != null || gpu?.temperature != null || unifiedMemory?.total != null);
    return {
      id: this.spark.id,
      name: this.spark.name,
      kind: this.spark.kind || "spark",
      online: this.online,
      uptime: this._uptimeSeconds,
      lanIp: this.spark.lanIp || "",
      isLocal: Boolean(this.spark.isLocal),
      disabledDevices: this.spark.disabledDevices || [],
      disabledInterfaces: this.spark.disabledInterfaces || [],
      storagePollDisabled: Boolean(this.spark.storagePollDisabled),
      workerNode: Boolean(this.spark.workerNode),
      role: this.spark.role || (this.spark.workerNode ? "worker" : "standalone"),
      workerLabel: this.spark.workerLabel || null,
      workerHeadId: this.spark.workerHeadId || null,
      llmMonitoring: this._llmMonitoringEnabled(),
      llmPort: ports[0] ?? LLM_PORT,
      llmPorts: ports,
      llmApiKeyPorts: Array.isArray(this.spark.llmApiKeyPorts)
        ? this.spark.llmApiKeyPorts
        : Object.keys(this.spark.llmApiKeys || {})
            .map((p) => parseInt(p, 10))
            .filter((n) => Number.isInteger(n)),
      comfyMonitoring: comfyOn,
      comfyPort: this._comfyPort(),
      tailscaleMonitoring: tailscaleOn,
      hermes: this._hermes,
      hardware: this._hardwareSummary,
      observeQuality: this._observeQuality,
      observedAtNs: this._observedAtNs,
      metricsFresh,
      metrics: {
        // NOTE: no `timestamp` here on purpose. The broadcast path skips
        // snapshots whose JSON is byte-identical to the previous one (see
        // startBroadcast); a per-snapshot Date.now() would defeat that cache,
        // forcing a broadcast + frontend re-render every tick even when all
        // measured values are unchanged. The frontend does not consume a
        // metrics timestamp; the WS receive time can serve if one is ever
        // needed.
        gpu,
        cpu,
        ram: this._metrics.ram,
        storage: this._metrics.storage,
        network: this._metrics.network,
        unifiedMemory,
        llm: this._metrics.llm,
        comfy: comfyOn ? this._metrics.comfy : null,
        tailscale: tailscaleOn ? this._metrics.tailscale : null,
      },
    };
  }

  // ─── Uptime helper ─────────────────────────────────────────
  /** Read system uptime from /proc/uptime (local or via SSH). */
  async _readUptime() {
    let content;
    if (this.spark.isLocal) {
      const mapped = path.join(HOST_PATHS.PROC, "uptime");
      content = fs.readFileSync(mapped, "utf8");
    } else {
      content = await sshExec(this.spark, "cat /proc/uptime");
    }
    const parts = content.trim().split(/\s+/);
    const secs = parseFloat(parts[0]);
    return Number.isFinite(secs) ? Math.floor(secs) : null;
  }

  // ─── Liveness ─────────────────────────────────────────────
  async _checkOnline() {
    if (!this._running || this._inflight.online) return;
    this._inflight.online = true;
    try {
      if (this.spark.isLocal) {
        await this.collector.pingHost();
      } else {
        const result = await sshTest(this.spark);
        // Re-check after the (up to 10s) SSH await — `stop()` may have fired
        // mid-flight (removeSpark / updateSpark). Bail before mutating state or
        // running into a stopped registry entry.
        if (!this._running) return;
        if (!result.ok) throw new Error(result.message);
      }
      if (!this._running) return;
      this.online = true;
      this.lastOnlineOk = Date.now();

      // Collect system uptime
      try {
        this._uptimeSeconds = await this._readUptime();
      } catch {
        // Non-fatal — uptime stays at previous value or null
      }
    } catch {
      if (!this._running) return;
      if (!this.lastOnlineOk || Date.now() - this.lastOnlineOk > ONLINE_GRACE_MS) {
        this.online = false;
        this._uptimeSeconds = null;
      }
    } finally {
      this._inflight.online = false;
    }
  }

  // ─── Polling ──────────────────────────────────────────────
  async _poll() {
    if (!this._running) return;
    await Promise.all([
      this._checkOnline(),
      this._pollDomain("gpu"),
      this._pollDomain("cpu"),
      this._pollDomain("network"),
      this._pollDomain("storage"),
      this._pollDomain("ram"),
      this._pollDomain("memory"),
      this._pollDomain("llm"),
      this._pollDomain("comfy"),
      this._pollDomain("hermes"),
      this._pollDomain("tailscale"),
    ]);
  }

  async _pollDomain(domain) {
    if (!this._running || this._inflight[domain]) return;
    // Skip storage auto-poll when disabled for this spark
    if (domain === "storage" && this.spark.storagePollDisabled) return;
    // Worker nodes: no local LLM API
    if (domain === "llm" && !this._llmMonitoringEnabled()) return;
    if (domain === "comfy" && !this._comfyMonitoringEnabled()) return;
    if (domain === "hermes" && !this._hermesMonitoringEnabled()) return;
    if (domain === "tailscale" && !this._tailscaleMonitoringEnabled()) return;
    this._inflight[domain] = true;
    try {
      let result;
      switch (domain) {
        case "gpu":
          result = await this.collector.collectGpu();
          break;
        case "cpu":
          result = await this.collector.collectCpu();
          break;
        case "ram":
          result = await this.collector.collectRam();
          break;
        case "network":
          result = await this.collector.collectNetwork();
          break;
        case "storage":
          result = await this.collector.collectStorage();
          break;
        case "memory":
          result = await this.collector.collectUnifiedMemory();
          break;
        case "llm":
          // Probe all ports in parallel
          result = await Promise.all(
            Array.from(this.llmProbes.values()).map((probe) => probe.probe())
          );
          break;
        case "comfy":
          result = this.comfyProbe ? await this.comfyProbe.probe() : null;
          break;
        case "tailscale":
          result = this.tailscaleProbe ? await this.tailscaleProbe.probe() : null;
          break;
        case "hermes":
          result = this.hermesProbe ? await this.hermesProbe.check() : null;
          break;
      }
      // Re-check after the await — `stop()`/`updateSpark()` may have torn
      // this monitor down mid-flight. Writing `_metrics` on a dead monitor
      // isn't user-visible (monitors.delete already happened) but it's a
      // latent class of bug worth killing, and a replaced monitor could
      // otherwise race the tail-end await onto the wrong object.
      if (!this._running) return;
      switch (domain) {
        case "gpu":
          this._metrics.gpu = result;
          break;
        case "cpu":
          this._metrics.cpu = result;
          break;
        case "ram":
          this._metrics.ram = result;
          break;
        case "network":
          this._metrics.network = result;
          if (result?.wolMac && this._onWolMac) {
            try {
              this._onWolMac(this.spark.id, result.wolMac);
            } catch (err) {
              console.error(`[SparkMonitor] ${this.spark.id} wolMac persist error:`, err.message);
            }
          }
          break;
        case "storage":
          this._metrics.storage = result;
          break;
        case "memory":
          this._metrics.unifiedMemory = result;
          break;
        case "llm":
          this._metrics.llm = result;
          {
            const probes = Array.from(this.llmProbes.values());
            for (let i = 0; i < result.length; i++) {
              const probe = probes[i];
              if (probe) llmDaily.record(this.spark.id, probe.port, result[i]);
            }
          }
          break;
        case "comfy":
          this._metrics.comfy = result;
          break;
        case "tailscale":
          this._metrics.tailscale = result;
          break;
        case "hermes":
          this.applyHermesCheck(result);
          break;
      }
      this._lastUpdate[domain] = Date.now();
    } catch (err) {
      console.error(`[SparkMonitor] ${this.spark.id} ${domain} poll error:`, err.message);
    } finally {
      this._inflight[domain] = false;
    }
  }

  /** Manually refresh a single domain, bypassing auto-poll guards. */
  async refreshDomain(domain) {
    if (this._inflight[domain]) return;
    this._inflight[domain] = true;
    try {
      let result;
      switch (domain) {
        case "storage":
          result = await this.collector.collectStorage();
          break;
        default:
          // Fall back to _pollDomain for other domains
          this._inflight[domain] = false;
          return this._pollDomain(domain);
      }
      if (!this._running) return;
      this._metrics.storage = result;
      this._lastUpdate[domain] = Date.now();
    } catch (err) {
      console.error(`[SparkMonitor] ${this.spark.id} ${domain} refresh error:`, err.message);
    } finally {
      this._inflight[domain] = false;
    }
  }

  // ─── Hermes Agent ─────────────────────────────────────────
  /**
   * Apply a Hermes check result to monitor state. Fires onHermesChange (force
   * broadcast) only when a user-meaningful field actually changed, so idle
   * re-checks do not spam the WS.
   * @param {object|null} result
   */
  applyHermesCheck(result) {
    if (!result || !this._running) return;
    const prev = this._hermes;
    const changed =
      result.updateAvailable !== prev.updateAvailable ||
      result.installed !== prev.installed ||
      result.version !== prev.version;
    this._hermes = {
      ...prev,
      installed: result.installed,
      version: result.version,
      updateAvailable: result.updateAvailable,
      behindCommits: result.behindCommits,
      checkedAt: result.checkedAt,
      error: result.error ?? null,
    };
    // A clean check self-heals the transient one-shot update job status, so a
    // "success / error" flag never lingers past the following poll cycle.
    if (!result.error && this._hermes.status !== "running") {
      this._hermes.status = "idle";
    }
    if (changed) this._notifyHermesChange();
  }

  /**
   * Kick off `hermes update` in the background. Returns immediately; progress
   * and result are surfaced through the snapshot + onHermesChange broadcast.
   * @returns {{ started: boolean, reason?: string }}
   */
  runHermesUpdate() {
    if (!this.hermesProbe) {
      return { started: false, reason: "Hermes Agent monitoring is disabled for this Spark" };
    }
    if (this._hermes.status === "running") {
      return { started: false, reason: "An update is already running" };
    }
    this._hermes = {
      ...this._hermes,
      status: "running",
      startedAt: Date.now(),
      finishedAt: null,
      error: null,
    };
    this._notifyHermesChange();
    // Defer the long-running SSH work so the broadcast above lands first.
    void (async () => {
      try {
        const res = await this.hermesProbe.update();
        if (!this._running) return;
        if (res?.ok) {
          this._hermes = {
            ...this._hermes,
            status: "success",
            installed: res.installed,
            version: res.version,
            error: null,
            finishedAt: res.finishedAt ?? Date.now(),
          };
          // Refresh update availability right away (don't wait for the next poll).
          try {
            const check = await this.hermesProbe.check();
            if (this._running && check) {
              this._hermes = {
                ...this._hermes,
                installed: check.installed,
                version: check.version,
                updateAvailable: check.updateAvailable,
                behindCommits: check.behindCommits,
                checkedAt: check.checkedAt,
                error: check.error ?? null,
              };
            }
          } catch {
            /* keep the success result if the follow-up check fails */
          }
        } else {
          this._hermes = {
            ...this._hermes,
            status: "error",
            error: res?.error || res?.output?.slice(-400) || "hermes update failed",
            finishedAt: res?.finishedAt ?? Date.now(),
          };
        }
      } catch (err) {
        if (!this._running) return;
        this._hermes = {
          ...this._hermes,
          status: "error",
          error: err instanceof Error ? err.message : String(err),
          finishedAt: Date.now(),
        };
      }
      this._notifyHermesChange();
    })();
    return { started: true };
  }

  _notifyHermesChange() {
    if (typeof this._onHermesChange !== "function") return;
    try {
      this._onHermesChange(this.spark.id);
    } catch (err) {
      console.error(`[SparkMonitor] ${this.spark.id} hermes change error:`, err.message);
    }
  }

  // ─── Hardware summary ─────────────────────────────────────
  /**
   * Static summary used for kind "spark" (DGX Spark specs) and as the
   * pre-detection fallback for kind "host". kind "host" is then enriched
   * with real hardware from `detectHardware()` once available.
   */
  _staticHardwareSummary(spark) {
    if (spark?.kind === "host") {
      return {
        device: "Linux GPU host",
        cpuModel: null,
        cpuCores: null,
        totalMemoryGB: null,
        gpuChip: null,
        cudaDriver: null,
        storageModel: null,
      };
    }
    return {
      device: "NVIDIA DGX Spark",
      cpuModel: "GB10",
      cpuCores: 20,
      totalMemoryGB: 128,
      gpuChip: "GB10",
      cudaDriver: null,
      storageModel: null,
    };
  }
}

