import { useEffect, useMemo, useRef, useState } from "react";
import { ArrowDownToLine, Eraser, Send, WrapText } from "lucide-react";
import type { TerminalEntry } from "../types";

interface Props {
  entries: TerminalEntry[];
  onSend: (bytes: Uint8Array) => void;
  onClear: () => void;
  disabled: boolean;
}

function asHex(bytes: Uint8Array): string {
  return Array.from(bytes, (value) => value.toString(16).padStart(2, "0").toUpperCase()).join(" ");
}

function parseHex(text: string): Uint8Array | null {
  const compact = text.replace(/0x/gi, "").replace(/[\s,;:_-]+/g, "");
  if (!compact || compact.length % 2 !== 0 || !/^[0-9a-f]+$/i.test(compact)) return null;
  return Uint8Array.from(compact.match(/.{2}/g)!.map((pair) => Number.parseInt(pair, 16)));
}

export function TerminalView({ entries, onSend, onClear, disabled }: Props) {
  const [mode, setMode] = useState<"text" | "hex">("text");
  const [txMode, setTxMode] = useState<"text" | "hex">("text");
  const [input, setInput] = useState("");
  const [appendCrLf, setAppendCrLf] = useState(true);
  const [autoscroll, setAutoscroll] = useState(true);
  const [wrap, setWrap] = useState(false);
  const [invalidHex, setInvalidHex] = useState(false);
  const viewport = useRef<HTMLDivElement>(null);
  const decoder = useMemo(() => new TextDecoder(undefined, { fatal: false }), []);
  const visible = useMemo(() => entries.slice(-2_000), [entries]);

  useEffect(() => {
    if (autoscroll && viewport.current) viewport.current.scrollTop = viewport.current.scrollHeight;
  }, [autoscroll, entries.length]);

  const submit = () => {
    let bytes: Uint8Array | null;
    if (txMode === "hex") bytes = parseHex(input);
    else bytes = new TextEncoder().encode(input + (appendCrLf ? "\r\n" : ""));
    if (!bytes?.length) {
      setInvalidHex(txMode === "hex");
      return;
    }
    setInvalidHex(false);
    onSend(bytes);
    setInput("");
  };

  return (
    <section className="workspace terminal-workspace" aria-label="Serial terminal">
      <div className="workspace-toolbar">
        <div className="segmented" aria-label="Display format">
          <button className={mode === "text" ? "active" : ""} onClick={() => setMode("text")}>Text</button>
          <button className={mode === "hex" ? "active" : ""} onClick={() => setMode("hex")}>Hex</button>
        </div>
        <button className={`tool-button ${autoscroll ? "active" : ""}`} onClick={() => setAutoscroll((value) => !value)} title="Auto scroll">
          <ArrowDownToLine size={15} /> Auto scroll
        </button>
        <button className={`tool-button ${wrap ? "active" : ""}`} onClick={() => setWrap((value) => !value)} title="Line wrap">
          <WrapText size={15} /> Wrap
        </button>
        <div className="toolbar-spacer" />
        <span className="memory-label">{entries.length.toLocaleString()} chunks · 16 MiB cap</span>
        <button className="tool-button" onClick={onClear}><Eraser size={15} /> Clear</button>
      </div>

      <div className={`terminal-output ${wrap ? "wrap" : ""}`} ref={viewport}>
        {visible.length === 0 ? (
          <div className="empty-state"><span>RX</span> Waiting for serial data</div>
        ) : visible.map((entry) => (
          <div className={`terminal-line ${entry.direction}`} key={entry.id}>
            <span className="terminal-time">{entry.timeSeconds.toFixed(3)}</span>
            <span className="direction-badge">{entry.direction.toUpperCase()}</span>
            <pre>{mode === "hex" ? asHex(entry.bytes) : decoder.decode(entry.bytes).replace(/\r?\n$/, "")}</pre>
          </div>
        ))}
      </div>

      <div className="terminal-compose">
        <select value={txMode} onChange={(event) => { setTxMode(event.target.value as "text" | "hex"); setInvalidHex(false); }} aria-label="Transmit format">
          <option value="text">TEXT</option>
          <option value="hex">HEX</option>
        </select>
        <input
          className={invalidHex ? "invalid" : ""}
          value={input}
          onChange={(event) => setInput(event.target.value)}
          onKeyDown={(event) => { if (event.key === "Enter") submit(); }}
          placeholder={txMode === "hex" ? "AA 55 01 00" : "Enter command…"}
          aria-label="Transmit data"
        />
        {txMode === "text" && (
          <label className="check-label"><input type="checkbox" checked={appendCrLf} onChange={(event) => setAppendCrLf(event.target.checked)} /> CRLF</label>
        )}
        <button className="primary-button send-button" onClick={submit} disabled={disabled || !input.trim()}><Send size={16} /> Send</button>
      </div>
    </section>
  );
}

