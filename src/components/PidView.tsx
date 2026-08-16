import { useMemo, useState } from "react";
import { ArrowRight, Gauge, Send, TimerReset } from "lucide-react";
import type { SampleStore } from "../buffers";
import type { CommandProfile, PidProfile } from "../types";

interface Props {
  profile?: PidProfile;
  command?: CommandProfile;
  latestValues: Record<string, number>;
  samples: SampleStore;
  sampleVersion: number;
  disabled: boolean;
  onSend: (bytes: Uint8Array, minGapMs: number) => void;
}

function Parameter({ label, value, min, max, step, onChange }: { label: string; value: number; min: number; max: number; step: number; onChange: (value: number) => void }) {
  return <label className="pid-parameter"><span>{label}</span><input type="number" value={value} min={min} max={max} step={step} onChange={(event) => onChange(Number(event.target.value))} /><small>{min} — {max}</small></label>;
}

export function PidView({ profile, command, latestValues, samples, sampleVersion, disabled, onSend }: Props) {
  const [kp, setKp] = useState(1.8);
  const [ki, setKi] = useState(0.12);
  const [kd, setKd] = useState(0.04);
  const [applied, setApplied] = useState({ kp, ki, kd });
  const metrics = useMemo(() => {
    if (!profile) return { error: 0, rms: 0, samples: 0 };
    const setpoint = latestValues[profile.setpointChannelId] ?? 0;
    const measured = latestValues[profile.measuredChannelId] ?? 0;
    const recent = samples[profile.measuredChannelId]?.last(250) ?? [];
    const rms = recent.length ? Math.sqrt(recent.reduce((sum, point) => sum + (setpoint - point.value) ** 2, 0) / recent.length) : 0;
    return { error: setpoint - measured, rms, samples: recent.length };
  }, [latestValues, profile, sampleVersion, samples]);

  if (!profile || !command) return <section className="workspace"><div className="empty-state"><span>PID</span> No PID profile in the active project</div></section>;
  const changed = kp !== applied.kp || ki !== applied.ki || kd !== applied.kd;
  const sendParameters = () => {
    const text = command.textTemplate.replace("{kp}", String(kp)).replace("{ki}", String(ki)).replace("{kd}", String(kd));
    onSend(new TextEncoder().encode(text), command.minGapMs);
    setApplied({ kp, ki, kd });
  };

  return (
    <section className="workspace pid-workspace">
      <div className="workspace-toolbar">
        <Gauge size={16} className="accent-icon" /><strong>{profile.name}</strong>
        <span className="memory-label">Manual closed-loop tuning · changes are staged locally</span>
        <div className="toolbar-spacer" />
        <span className={`staged-badge ${changed ? "changed" : ""}`}>{changed ? "UNSENT CHANGES" : "IN SYNC"}</span>
      </div>
      <div className="pid-grid">
        <article className="instrument-card parameters-card">
          <header><span>Controller gains</span><small>TX via scheduler</small></header>
          <div className="parameter-grid">
            <Parameter label="Kp" value={kp} min={profile.kpMin} max={profile.kpMax} step={0.01} onChange={setKp} />
            <Parameter label="Ki" value={ki} min={profile.kiMin} max={profile.kiMax} step={0.01} onChange={setKi} />
            <Parameter label="Kd" value={kd} min={profile.kdMin} max={profile.kdMax} step={0.01} onChange={setKd} />
          </div>
          <div className="command-preview"><span>COMMAND</span><code>{command.textTemplate.replace("{kp}", String(kp)).replace("{ki}", String(ki)).replace("{kd}", String(kd)).replace(/\r/g, "\\r").replace(/\n/g, "\\n")}</code></div>
          <button className="primary-button apply-button" onClick={sendParameters} disabled={disabled || !changed}><Send size={17} /> Apply parameters</button>
        </article>
        <article className="instrument-card telemetry-card">
          <header><span>Live loop state</span><small>{metrics.samples} sample window</small></header>
          <div className="loop-values">
            <div><span>SETPOINT</span><strong>{(latestValues[profile.setpointChannelId] ?? 0).toFixed(2)}</strong></div>
            <ArrowRight size={19} />
            <div><span>MEASURED</span><strong>{(latestValues[profile.measuredChannelId] ?? 0).toFixed(2)}</strong></div>
            <ArrowRight size={19} />
            <div><span>OUTPUT</span><strong>{(latestValues[profile.outputChannelId] ?? 0).toFixed(2)}</strong></div>
          </div>
          <div className="metric-grid">
            <div className="metric"><span>Instant error</span><strong>{metrics.error.toFixed(3)}</strong><small>%</small></div>
            <div className="metric"><span>RMS error</span><strong>{metrics.rms.toFixed(3)}</strong><small>last 5 s</small></div>
            <div className="metric"><span>TX min gap</span><strong>{command.minGapMs}</strong><small>ms</small></div>
            <div className="metric"><span>State</span><strong className="nominal"><TimerReset size={17} /> LIVE</strong><small>50 Hz input</small></div>
          </div>
        </article>
      </div>
    </section>
  );
}
