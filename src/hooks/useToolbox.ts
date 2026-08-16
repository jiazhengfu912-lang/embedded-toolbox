import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  PayloadType,
  api,
  defaultSyntheticConfig,
  isNextControlEvent,
  isTauriRuntime,
  normalizeError,
  subscribeEvents,
  subscribeStream,
  type DecodedEnvelope,
} from "../ipc";
import { SampleRingBuffer, enforceSampleStoreLimit, type SampleStore } from "../buffers";
import type {
  AppEvent,
  AppSnapshot,
  DiagnosticEntry,
  PacketEntry,
  SerialConfig,
  SerialPortDescriptor,
  TerminalEntry,
  ToolboxError,
  TxRequest,
} from "../types";

const TERMINAL_MAX_BYTES = 16 * 1024 * 1024;
const TERMINAL_MAX_LINES = 100_000;
const PACKET_MAX_BYTES = 32 * 1024 * 1024;
const PACKET_MAX_ITEMS = 50_000;
const DIAGNOSTIC_MAX_ITEMS = 2_000;

const IDS = {
  project: "0198a3e8-1f48-7c11-8db8-f0416d471001",
  source: "0198a3e8-1f48-7c11-8db8-f0416d471002",
  setpoint: "0198a3e8-1f48-7c11-8db8-f0416d471003",
  measured: "0198a3e8-1f48-7c11-8db8-f0416d471004",
  output: "0198a3e8-1f48-7c11-8db8-f0416d471005",
  command: "0198a3e8-1f48-7c11-8db8-f0416d471006",
};

function previewSnapshot(): AppSnapshot {
  return {
    runtimeInstanceId: "browser-preview",
    appVersion: "0.1.0-preview",
    eventCursor: 1,
    project: {
      id: IDS.project,
      name: "PID Loop Demo",
      projectSchemaVersion: "1.0.0",
      pipelineSemanticVersion: "1.0.0",
      appVersion: "0.1.0",
      source: {
        id: IDS.source,
        name: "Primary device",
        serial: { id: "0198a3e8-1f48-7c11-8db8-f0416d471011", portName: "COM7", baudRate: 115200, dataBits: 8, stopBits: 1, parity: "none", flowControl: "none", timeoutMs: 20 },
      },
      channels: [
        { id: IDS.setpoint, name: "Setpoint", unit: "%", color: "#f1b761" },
        { id: IDS.measured, name: "Measured", unit: "%", color: "#46d8b1" },
        { id: IDS.output, name: "Output", unit: "%", color: "#7197ff" },
      ],
      commands: [{ id: IDS.command, name: "Set PID", textTemplate: "PID,{kp},{ki},{kd}\r\n", minGapMs: 20 }],
      pidProfiles: [{
        id: "0198a3e8-1f48-7c11-8db8-f0416d471007",
        name: "Main loop",
        setpointChannelId: IDS.setpoint,
        measuredChannelId: IDS.measured,
        outputChannelId: IDS.output,
        commandProfileId: IDS.command,
        kpMin: 0, kpMax: 100, kiMin: 0, kiMax: 100, kdMin: 0, kdMax: 100,
      }],
      views: ["terminal", "plotter", "packets", "pidTuner"].map((kind, index) => ({ id: `0198a3e8-1f48-7c11-8db8-f0416d47101${index + 2}`, name: kind, kind, settings: {} })),
    },
    sources: [{
      sourceId: IDS.source,
      name: "Primary device",
      status: "connected",
      transport: "synthetic",
      endpoint: "seed:24301 @ 50 Hz",
      sourceEpoch: 1,
      epochId: "0198a3e8-1f48-7c11-8db8-f0416d471008",
      queueDepths: { recorderBytes: 0, parserBytes: 0, terminalBytes: 0, ipcBytes: 0, txBytes: 0 },
      stats: { rxBytes: 0, txBytes: 0, parsedFrames: 0, checksumFailures: 0, parserGaps: 0, uiDroppedBytes: 0, uiDroppedBatches: 0 },
    }],
    session: { status: "idle", bytesWritten: 0, checkpointPending: false },
    latestChannelValues: {},
    recentErrors: [],
  };
}

function appendTerminal(current: TerminalEntry[], incoming: TerminalEntry[]): TerminalEntry[] {
  const next = [...current, ...incoming];
  let bytes = next.reduce((sum, entry) => sum + entry.bytes.byteLength, 0);
  let lines = next.reduce((sum, entry) => sum + Math.max(1, entry.bytes.reduce((count, byte) => count + Number(byte === 10), 0)), 0);
  let remove = 0;
  while (lines > TERMINAL_MAX_LINES || bytes > TERMINAL_MAX_BYTES) {
    bytes -= next[remove].bytes.byteLength;
    lines -= Math.max(1, next[remove].bytes.reduce((count, byte) => count + Number(byte === 10), 0));
    remove += 1;
  }
  return remove > 0 ? next.slice(remove) : next;
}

function appendPackets(current: PacketEntry[], incoming: PacketEntry[]): PacketEntry[] {
  const next = [...current, ...incoming];
  let bytes = next.reduce((sum, packet) => sum + packet.bytes.length + Object.keys(packet.fields).length * 24 + 64, 0);
  let remove = 0;
  while (next.length - remove > PACKET_MAX_ITEMS || bytes > PACKET_MAX_BYTES) {
    const packet = next[remove];
    bytes -= packet.bytes.length + Object.keys(packet.fields).length * 24 + 64;
    remove += 1;
  }
  return remove > 0 ? next.slice(remove) : next;
}

function appendDiagnostics(current: DiagnosticEntry[], incoming: DiagnosticEntry[]): DiagnosticEntry[] {
  const next = [...current];
  for (const diagnostic of incoming) {
    const last = next[next.length - 1];
    if (last && JSON.stringify(last.values) === JSON.stringify(diagnostic.values)) {
      next[next.length - 1] = { ...diagnostic, count: (last.count ?? 1) + 1 };
    } else {
      next.push({ ...diagnostic, count: 1 });
    }
  }
  return next.slice(-DIAGNOSTIC_MAX_ITEMS);
}

export function useToolbox() {
  const native = isTauriRuntime();
  const [snapshot, setSnapshot] = useState<AppSnapshot | null>(native ? null : previewSnapshot());
  const [ports, setPorts] = useState<SerialPortDescriptor[]>(native ? [] : [{ name: "COM7", kind: "usb", product: "USB-UART Preview" }]);
  const [terminal, setTerminal] = useState<TerminalEntry[]>([]);
  const [packets, setPackets] = useState<PacketEntry[]>([]);
  const sampleStore = useRef<SampleStore>({});
  const [sampleVersion, setSampleVersion] = useState(0);
  const [diagnostics, setDiagnostics] = useState<DiagnosticEntry[]>([]);
  const [lastEvent, setLastEvent] = useState<AppEvent | null>(null);
  const [notice, setNotice] = useState<ToolboxError | null>(null);
  const [busy, setBusy] = useState<string | null>(native ? "startup" : null);
  const snapshotRef = useRef(snapshot);
  const terminalPending = useRef<TerminalEntry[]>([]);
  const packetPending = useRef<PacketEntry[]>([]);
  const samplePending = useRef<Array<{ channelId: string; timeSeconds: number; value: number; sequence: number }>>([]);
  const diagnosticPending = useRef<DiagnosticEntry[]>([]);

  useEffect(() => { snapshotRef.current = snapshot; }, [snapshot]);

  const consumeEnvelope = useCallback((envelope: DecodedEnvelope) => {
    if (envelope.type === PayloadType.RawBatch) terminalPending.current.push(envelope.terminal);
    if (envelope.type === PayloadType.PacketBatch) {
      packetPending.current.push(...envelope.packets.map((packet) => ({ ...packet, id: `${envelope.sourceEpoch}:${packet.sequence}`, sourceEpoch: envelope.sourceEpoch })));
    }
    if (envelope.type === PayloadType.SampleBatch) {
      samplePending.current.push(...envelope.samples.map((sample) => ({
        channelId: sample.channelId,
        timeSeconds: sample.monotonicOffsetNs / 1e9,
        value: sample.value,
        sequence: sample.frameSequence,
      })));
    }
    if (envelope.type === PayloadType.DiagnosticBatch) {
      diagnosticPending.current.push({ id: `${envelope.sourceEpoch}:${envelope.sequence}`, timeSeconds: envelope.timeSeconds, values: envelope.diagnostic });
    }
  }, []);

  const refreshSnapshot = useCallback(async () => {
    if (!native) return snapshotRef.current;
    const next = await api.snapshot();
    snapshotRef.current = next;
    setSnapshot(next);
    return next;
  }, [native]);

  useEffect(() => {
    if (!native) return;
    const timer = window.setInterval(() => { void refreshSnapshot().catch((error) => setNotice(normalizeError(error))); }, 1000);
    return () => window.clearInterval(timer);
  }, [native, refreshSnapshot]);

  useEffect(() => {
    const timer = window.setInterval(() => {
      if (terminalPending.current.length) {
        const incoming = terminalPending.current.splice(0);
        setTerminal((current) => appendTerminal(current, incoming));
      }
      if (packetPending.current.length) {
        const incoming = packetPending.current.splice(0);
        setPackets((current) => appendPackets(current, incoming));
      }
      if (samplePending.current.length) {
        const incoming = samplePending.current.splice(0);
        const latest: Record<string, number> = {};
        for (const point of incoming) {
          const ring = sampleStore.current[point.channelId] ??= new SampleRingBuffer();
          ring.push({ timeSeconds: point.timeSeconds, value: point.value, sequence: point.sequence });
          latest[point.channelId] = point.value;
        }
        enforceSampleStoreLimit(sampleStore.current);
        setSampleVersion((version) => version + 1);
        setSnapshot((current) => current ? { ...current, latestChannelValues: { ...current.latestChannelValues, ...latest } } : current);
      }
      if (diagnosticPending.current.length) {
        const incoming = diagnosticPending.current.splice(0);
        setDiagnostics((current) => appendDiagnostics(current, incoming));
      }
    }, 1000 / 30);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!native) return;
    let disposed = false;
    let ready = false;
    let runtimeInstanceId = "";
    let cursor = 0;
    let refreshing = false;
    let refreshRequested = false;
    const pending: AppEvent[] = [];

    const forceSnapshot = async () => {
      refreshRequested = true;
      if (refreshing || disposed) return;
      refreshing = true;
      try {
        while (refreshRequested && !disposed) {
          refreshRequested = false;
          const next = await api.snapshot();
          if (!disposed) {
            runtimeInstanceId = next.runtimeInstanceId;
            cursor = next.eventCursor;
            snapshotRef.current = next;
            setSnapshot(next);
          }
        }
      } catch (error) {
        if (!disposed) setNotice(normalizeError(error));
      } finally {
        refreshing = false;
      }
    };

    const applyEvent = (event: AppEvent) => {
      if (!isNextControlEvent(runtimeInstanceId, cursor, event)) {
        void forceSnapshot();
        return;
      }
      cursor = event.cursor;
      setLastEvent(event);
      void forceSnapshot();
    };

    void (async () => {
      try {
        await subscribeEvents(null, (event) => ready ? applyEvent(event) : pending.push(event));
        const next = await api.snapshot();
        if (disposed) return;
        runtimeInstanceId = next.runtimeInstanceId;
        cursor = next.eventCursor;
        snapshotRef.current = next;
        setSnapshot(next);
        ready = true;
        for (const event of pending.sort((a, b) => a.cursor - b.cursor)) if (event.cursor > cursor) applyEvent(event);
        await subscribeStream(consumeEnvelope, (error) => setNotice(normalizeError(error)));
        setPorts(await api.ports());
      } catch (error) {
        setNotice(normalizeError(error));
      } finally {
        if (!disposed) setBusy(null);
      }
    })();
    return () => { disposed = true; };
  }, [consumeEnvelope, native]);

  useEffect(() => {
    if (native) return;
    let frame = 0;
    let measured = 22;
    const timer = window.setInterval(() => {
      if (!snapshotRef.current?.sources.some((source) => source.status === "connected")) return;
      frame += 1;
      const time = frame / 50;
      const setpoint = Math.floor(frame / 250) % 2 === 0 ? 25 : 75;
      measured += (setpoint - measured) * 0.035 + Math.sin(frame * 0.31) * 0.07;
      const output = Math.max(0, Math.min(100, (setpoint - measured) * 1.8 + 50));
      const line = `${setpoint.toFixed(3)},${measured.toFixed(3)},${output.toFixed(3)}\n`;
      const bytes = new TextEncoder().encode(line);
      terminalPending.current.push({ id: `preview:${frame}`, sequence: frame, timeSeconds: time, direction: "rx", bytes });
      packetPending.current.push({ id: `preview:${frame}`, sourceEpoch: 1, sequence: frame, monotonicOffsetNs: time * 1e9, direction: "rx", bytes: Array.from(bytes), valid: true, fields: {} });
      samplePending.current.push(
        { channelId: IDS.setpoint, timeSeconds: time, value: setpoint, sequence: frame },
        { channelId: IDS.measured, timeSeconds: time, value: measured, sequence: frame },
        { channelId: IDS.output, timeSeconds: time, value: output, sequence: frame },
      );
      setSnapshot((current) => {
        if (!current?.sources[0]) return current;
        const source = current.sources[0];
        return { ...current, sources: [{ ...source, stats: { ...source.stats, rxBytes: source.stats.rxBytes + bytes.length, parsedFrames: source.stats.parsedFrames + 1 } }] };
      });
    }, 20);
    return () => window.clearInterval(timer);
  }, [native]);

  const run = useCallback(async (name: string, action: () => Promise<void>) => {
    setBusy(name);
    setNotice(null);
    try { await action(); } catch (error) { setNotice(normalizeError(error)); } finally { setBusy(null); }
  }, []);

  const refreshPorts = useCallback(() => run("ports", async () => {
    if (native) setPorts(await api.ports());
  }), [native, run]);

  const connectSerial = useCallback((config: SerialConfig) => run("connect", async () => {
    if (native) {
      await api.connectSerial(config);
      await refreshSnapshot();
    } else {
      setSnapshot((current) => current ? { ...current, sources: current.sources.map((source) => ({ ...source, status: "connected", transport: "serial", endpoint: config.portName })) } : current);
    }
  }), [native, refreshSnapshot, run]);

  const connectSynthetic = useCallback(() => run("connect", async () => {
    if (native) {
      await api.connectSynthetic(defaultSyntheticConfig());
      await refreshSnapshot();
    } else {
      setSnapshot((current) => current ? { ...current, sources: current.sources.map((source) => ({ ...source, status: "connected", transport: "synthetic", endpoint: "seed:24301 @ 50 Hz" })) } : current);
    }
  }), [native, refreshSnapshot, run]);

  const disconnect = useCallback(() => run("disconnect", async () => {
    const sourceId = snapshotRef.current?.sources.find((source) => source.status !== "disconnected")?.sourceId;
    if (!sourceId) return;
    if (native) {
      await api.disconnect(sourceId);
      await refreshSnapshot();
    } else {
      setSnapshot((current) => current ? { ...current, sources: current.sources.map((source) => ({ ...source, status: "disconnected" })) } : current);
    }
  }), [native, refreshSnapshot, run]);

  const send = useCallback((payload: Uint8Array, origin: string, minGapMs = 0) => run("send", async () => {
    const sourceId = snapshotRef.current?.sources.find((source) => source.status === "connected")?.sourceId;
    if (!sourceId) throw new Error("No connected source");
    const request: TxRequest = { sourceId, payload: Array.from(payload), origin, minGapMs };
    if (native) await api.send(request);
    else terminalPending.current.push({ id: `preview-tx:${Date.now()}`, sequence: Date.now(), timeSeconds: performance.now() / 1000, direction: "tx", bytes: payload });
  }), [native, run]);

  const toggleRecording = useCallback(() => run("session", async () => {
    const recording = snapshotRef.current?.session.status === "recording" || snapshotRef.current?.session.status === "suspended";
    if (native) {
      if (recording) await api.sessionStop(); else await api.sessionStart();
      await refreshSnapshot();
    } else {
      setSnapshot((current) => current ? {
        ...current,
        session: recording
          ? { ...current.session, status: "closed", checkpointPending: false }
          : { ...current.session, status: "recording", sessionId: "preview-session", path: "Documents/Embedded Toolbox/Sessions/preview.etdb", epochOrdinal: 1 },
      } : current);
    }
  }), [native, refreshSnapshot, run]);

  const clearTerminal = useCallback(() => setTerminal([]), []);
  const clearPackets = useCallback(() => setPackets([]), []);
  const dismissNotice = useCallback(() => setNotice(null), []);
  const activeSource = useMemo(() => snapshot?.sources.find((source) => source.status === "connected" || source.status === "connecting") ?? snapshot?.sources[0], [snapshot]);

  return {
    native, snapshot, ports, terminal, packets, samples: sampleStore.current, sampleVersion, diagnostics, lastEvent, notice, busy, activeSource,
    refreshPorts, connectSerial, connectSynthetic, disconnect, send, toggleRecording, clearTerminal, clearPackets, dismissNotice,
  };
}
