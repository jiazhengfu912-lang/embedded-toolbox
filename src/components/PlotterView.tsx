import { useEffect, useRef, useState } from "react";
import { Crosshair, Pause, Play, RotateCcw } from "lucide-react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import type { SampleStore } from "../buffers";
import type { ChannelSpec } from "../types";

interface Props {
  channels: ChannelSpec[];
  samples: SampleStore;
  sampleVersion: number;
  latestValues: Record<string, number>;
}

function buildData(channels: ChannelSpec[], samples: SampleStore, width: number, windowSeconds: number): uPlot.AlignedData {
  const newest = Math.max(0, ...channels.map((channel) => samples[channel.id]?.lastTime ?? 0));
  const since = Number.isFinite(windowSeconds) ? newest - windowSeconds : Number.NEGATIVE_INFINITY;
  const tables = channels.map((channel) => {
    const points = samples[channel.id]?.downsampleSince(since, width) ?? [];
    return [points.map((point) => point.timeSeconds), points.map((point) => point.value)] as uPlot.AlignedData;
  }).filter((table) => table[0].length > 0);
  return tables.length ? uPlot.join(tables) : [[], ...channels.map(() => [])] as uPlot.AlignedData;
}

export function PlotterView({ channels, samples, sampleVersion, latestValues }: Props) {
  const host = useRef<HTMLDivElement>(null);
  const chart = useRef<uPlot | null>(null);
  const [paused, setPaused] = useState(false);
  const [windowSeconds, setWindowSeconds] = useState(15);
  const [fps, setFps] = useState(30);
  const [resetKey, setResetKey] = useState(0);
  useEffect(() => {
    if (!host.current) return;
    const container = host.current;
    const options: uPlot.Options = {
      width: Math.max(320, container.clientWidth),
      height: Math.max(280, container.clientHeight),
      cursor: { drag: { x: true, y: false }, focus: { prox: 24 } },
      scales: { x: { time: false }, y: { auto: true } },
      axes: [
        { stroke: "#788896", grid: { stroke: "#24313a", width: 1 }, ticks: { stroke: "#34434d" }, label: "Time (s)", labelFont: "11px IBM Plex Mono" },
        { stroke: "#788896", grid: { stroke: "#24313a", width: 1 }, ticks: { stroke: "#34434d" }, size: 52 },
      ],
      legend: { show: false },
      series: [
        {},
        ...channels.map((channel) => ({ label: channel.name, stroke: channel.color, width: 1.5, points: { show: false }, spanGaps: true })),
      ],
    };
    const instance = new uPlot(options, buildData(channels, samples, container.clientWidth, windowSeconds), container);
    chart.current = instance;
    const observer = new ResizeObserver(([entry]) => {
      instance.setSize({ width: Math.max(320, Math.floor(entry.contentRect.width)), height: Math.max(280, Math.floor(entry.contentRect.height)) });
    });
    observer.observe(container);
    return () => { observer.disconnect(); instance.destroy(); chart.current = null; };
  }, [channels, resetKey]);

  useEffect(() => {
    if (!paused && chart.current) chart.current.setData(buildData(channels, samples, chart.current.width, windowSeconds));
  }, [channels, paused, samples, sampleVersion, windowSeconds, fps]);

  return (
    <section className="workspace plotter-workspace">
      <div className="workspace-toolbar">
        <button className={`tool-button ${paused ? "active warning" : ""}`} onClick={() => setPaused((value) => !value)}>
          {paused ? <Play size={15} /> : <Pause size={15} />} {paused ? "Resume" : "Pause"}
        </button>
        <div className="toolbar-divider" />
        <label className="compact-control">Window
          <select value={windowSeconds} onChange={(event) => setWindowSeconds(Number(event.target.value))}>
            <option value={5}>5 s</option><option value={15}>15 s</option><option value={60}>60 s</option><option value={Infinity}>All</option>
          </select>
        </label>
        <label className="compact-control">Refresh
          <select value={fps} onChange={(event) => setFps(Number(event.target.value))}>
            <option value={15}>15 FPS</option><option value={30}>30 FPS</option><option value={60}>60 FPS</option>
          </select>
        </label>
        <div className="toolbar-spacer" />
        <button className="tool-button" onClick={() => setResetKey((value) => value + 1)}><RotateCcw size={15} /> Reset view</button>
        <span className="memory-label"><Crosshair size={14} /> min/max pixel buckets</span>
      </div>
      <div className="plot-body">
        <div className="channel-legend">
          {channels.map((channel) => (
            <div className="legend-card" key={channel.id}>
              <span className="legend-color" style={{ backgroundColor: channel.color }} />
              <span className="legend-name">{channel.name}</span>
              <strong>{latestValues[channel.id]?.toFixed(2) ?? "—"}</strong>
              <span>{channel.unit}</span>
            </div>
          ))}
        </div>
        <div className="plot-canvas" ref={host} />
      </div>
    </section>
  );
}
