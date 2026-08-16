import { useEffect, useMemo, useState } from "react";
import {
  Activity,
  AlertTriangle,
  Cable,
  CircleStop,
  Cpu,
  Database,
  Radio,
  RefreshCw,
  SlidersHorizontal,
  TableProperties,
  Terminal as TerminalIcon,
  Waves,
  X,
} from "lucide-react";
import "./App.css";
import { PacketsView } from "./components/PacketsView";
import { PidView } from "./components/PidView";
import { PlotterView } from "./components/PlotterView";
import { TerminalView } from "./components/TerminalView";
import { useToolbox } from "./hooks/useToolbox";
import type { SerialConfig } from "./types";

type TabId = "terminal" | "plotter" | "packets" | "pid";

const tabs = [
  { id: "terminal" as const, label: "Terminal", icon: TerminalIcon },
  { id: "plotter" as const, label: "Plotter", icon: Waves },
  { id: "packets" as const, label: "Packets", icon: TableProperties },
  { id: "pid" as const, label: "PID Tuner", icon: SlidersHorizontal },
];

function formatBytes(bytes = 0): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)} KiB`;
  return `${(bytes / 1024 ** 2).toFixed(1)} MiB`;
}

function App() {
  const toolbox = useToolbox();
  const [tab, setTab] = useState<TabId>("terminal");
  const [portName, setPortName] = useState("");
  const [baudRate, setBaudRate] = useState(115200);
  const snapshot = toolbox.snapshot;
  const source = toolbox.activeSource;
  const connected = source?.status === "connected";
  const runtimeAttached = source?.status === "connected" || source?.status === "connecting" || source?.status === "faulted";
  const sessionActive = snapshot?.session.status === "recording" || snapshot?.session.status === "suspended";
  const channelCount = snapshot?.project.channels.length ?? 0;

  useEffect(() => {
    if (!portName) setPortName(toolbox.ports[0]?.name ?? snapshot?.project.source.serial.portName ?? "");
  }, [portName, snapshot, toolbox.ports]);

  const serialConfig: SerialConfig = useMemo(() => ({
    id: snapshot?.project.source.serial.id ?? "00000000-0000-0000-0000-000000000000",
    portName,
    baudRate,
    dataBits: 8,
    stopBits: 1,
    parity: "none",
    flowControl: "none",
    timeoutMs: 20,
  }), [baudRate, portName, snapshot?.project.source.serial.id]);

  if (!snapshot) {
    return <main className="boot-screen"><div className="boot-mark"><Cpu size={27} /><span /></div><h1>Embedded Toolbox</h1><p>Starting Rust Core and synchronizing EventCursor…</p></main>;
  }

  const pidProfile = snapshot.project.pidProfiles[0];
  const pidCommand = snapshot.project.commands.find((command) => command.id === pidProfile?.commandProfileId);

  return (
    <main className="app-shell">
      <header className="titlebar">
        <div className="brand-mark"><Cpu size={21} strokeWidth={1.7} /><i /></div>
        <div className="brand-copy"><strong>EMBEDDED TOOLBOX</strong><span>DEVICE INSTRUMENTATION CONSOLE</span></div>
        <div className="project-chip"><Database size={14} /><span>{snapshot.project.name}</span><small>v{snapshot.project.pipelineSemanticVersion}</small></div>
        <div className="titlebar-spacer" />
        <div className="runtime-meta"><span>RUNTIME</span><code>{snapshot.runtimeInstanceId.slice(0, 8)}</code></div>
        <div className={`connection-led ${connected ? "online" : ""}`}><i />{connected ? "ONLINE" : "OFFLINE"}</div>
      </header>

      <section className="connection-rack" aria-label="Device connection">
        <div className="rack-label"><Radio size={17} /><span>TRANSPORT</span></div>
        <label className="rack-control port-control"><span>PORT</span>
          <select value={portName} onChange={(event) => setPortName(event.target.value)} disabled={runtimeAttached}>
            {!toolbox.ports.length && <option value="">No COM ports</option>}
            {toolbox.ports.map((port) => <option value={port.name} key={port.name}>{port.name}{port.product ? ` · ${port.product}` : ""}</option>)}
          </select>
        </label>
        <button className="icon-button" onClick={toolbox.refreshPorts} disabled={Boolean(toolbox.busy) || runtimeAttached} title="Refresh COM ports"><RefreshCw size={16} className={toolbox.busy === "ports" ? "spin" : ""} /></button>
        <label className="rack-control baud-control"><span>BAUD</span>
          <select value={baudRate} onChange={(event) => setBaudRate(Number(event.target.value))} disabled={runtimeAttached || sessionActive}>
            {[9600, 57600, 115200, 230400, 460800, 921600].map((baud) => <option key={baud} value={baud}>{baud.toLocaleString()}</option>)}
          </select>
        </label>
        <div className="serial-format"><span>8</span><i>N</i><span>1</span></div>
        <button className="secondary-button" onClick={() => toolbox.connectSerial(serialConfig)} disabled={Boolean(toolbox.busy) || runtimeAttached || !portName}><Cable size={16} /> Connect</button>
        <button className="secondary-button simulator-button" onClick={toolbox.connectSynthetic} disabled={Boolean(toolbox.busy) || runtimeAttached}><Activity size={16} /> Simulator</button>
        <button className="danger-button" onClick={toolbox.disconnect} disabled={Boolean(toolbox.busy) || !runtimeAttached}><CircleStop size={16} /> Disconnect</button>
        <div className="rack-spacer" />
        <div className="endpoint-readout"><span>{source?.transport?.toUpperCase() ?? "NO SOURCE"}</span><code>{source?.endpoint || "—"}</code></div>
        <button className={`record-button ${sessionActive ? "recording" : ""}`} onClick={toolbox.toggleRecording} disabled={Boolean(toolbox.busy) || (!connected && !sessionActive)}>
          <i /> {sessionActive ? "Stop recording" : "Record session"}
        </button>
      </section>

      <nav className="tab-strip" aria-label="Toolbox modules">
        {tabs.map(({ id, label, icon: Icon }) => (
          <button key={id} className={tab === id ? "active" : ""} onClick={() => setTab(id)}>
            <Icon size={17} strokeWidth={1.8} /><span>{label}</span>
            {id === "packets" && toolbox.packets.length > 0 && <small>{Math.min(toolbox.packets.length, 9999)}</small>}
          </button>
        ))}
        <div className="tab-spacer" />
        <div className="pipeline-indicator"><span>PIPELINE</span><b>FRAMER</b><i /><b>DECODER</b><i /><b>{channelCount} CH</b></div>
      </nav>

      <div className="app-workspace">
        {tab === "terminal" && <TerminalView entries={toolbox.terminal} onSend={(bytes) => toolbox.send(bytes, "terminal")} onClear={toolbox.clearTerminal} disabled={!connected || Boolean(toolbox.busy)} />}
        {tab === "plotter" && <PlotterView channels={snapshot.project.channels} samples={toolbox.samples} sampleVersion={toolbox.sampleVersion} latestValues={snapshot.latestChannelValues} />}
        {tab === "packets" && <PacketsView packets={toolbox.packets} onClear={toolbox.clearPackets} />}
        {tab === "pid" && <PidView profile={pidProfile} command={pidCommand} latestValues={snapshot.latestChannelValues} samples={toolbox.samples} sampleVersion={toolbox.sampleVersion} disabled={!connected || Boolean(toolbox.busy)} onSend={(bytes, minGap) => toolbox.send(bytes, "pidTuner", minGap)} />}
      </div>

      {toolbox.notice && (
        <aside className={`error-toast ${toolbox.notice.severity}`} role="alert">
          <AlertTriangle size={18} /><div><strong>{toolbox.notice.code}</strong><span>{toolbox.notice.messageKey}{toolbox.notice.cause ? ` · ${toolbox.notice.cause}` : ""}</span></div>
          <button onClick={toolbox.dismissNotice} title="Dismiss"><X size={15} /></button>
        </aside>
      )}

      <footer className="statusbar">
        <div><span className={`status-dot ${connected ? "online" : ""}`} />{source?.status.toUpperCase() ?? "NO SOURCE"}</div>
        <div><span>RX</span><strong>{formatBytes(source?.stats.rxBytes)}</strong></div>
        <div><span>TX</span><strong>{formatBytes(source?.stats.txBytes)}</strong></div>
        <div><span>FRAMES</span><strong>{source?.stats.parsedFrames.toLocaleString() ?? "0"}</strong></div>
        <div><span>DROPPED UI</span><strong>{source?.stats.uiDroppedBatches.toLocaleString() ?? "0"}</strong></div>
        <div className="statusbar-spacer" />
        <div><span>SESSION</span><strong className={sessionActive ? "recording-text" : ""}>{snapshot.session.status.toUpperCase()}</strong></div>
        <div><span>EPOCH</span><strong>{source?.sourceEpoch ?? "—"}</strong></div>
        <div><span>CURSOR</span><strong>{snapshot.eventCursor}</strong></div>
        <div><span>IPC Q</span><strong>{formatBytes(source?.queueDepths.ipcBytes)}</strong></div>
        <div className="native-badge">{toolbox.native ? "TAURI / RUST" : "BROWSER PREVIEW"}</div>
      </footer>
    </main>
  );
}

export default App;
