import { useMemo, useState } from "react";
import { CheckCircle2, Eraser, Search, ShieldAlert } from "lucide-react";
import type { PacketEntry } from "../types";

interface Props { packets: PacketEntry[]; onClear: () => void; }

function hex(bytes: number[]): string {
  return bytes.map((value) => value.toString(16).padStart(2, "0").toUpperCase()).join(" ");
}

export function PacketsView({ packets, onClear }: Props) {
  const [query, setQuery] = useState("");
  const [validity, setValidity] = useState<"all" | "valid" | "invalid">("all");
  const visible = useMemo(() => packets.filter((packet) => {
    if (validity === "valid" && !packet.valid) return false;
    if (validity === "invalid" && packet.valid) return false;
    return !query || hex(packet.bytes).includes(query.toUpperCase()) || packet.errorCode?.toUpperCase().includes(query.toUpperCase());
  }).slice(-2_000).reverse(), [packets, query, validity]);
  const invalidCount = useMemo(() => packets.filter((packet) => !packet.valid).length, [packets]);

  return (
    <section className="workspace packets-workspace">
      <div className="workspace-toolbar">
        <div className="search-box"><Search size={15} /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Filter bytes or error code" /></div>
        <select value={validity} onChange={(event) => setValidity(event.target.value as typeof validity)}>
          <option value="all">All packets</option><option value="valid">Valid only</option><option value="invalid">Invalid only</option>
        </select>
        <div className="toolbar-spacer" />
        <span className="packet-stat ok"><CheckCircle2 size={14} /> {(packets.length - invalidCount).toLocaleString()} valid</span>
        <span className={`packet-stat ${invalidCount ? "bad" : ""}`}><ShieldAlert size={14} /> {invalidCount.toLocaleString()} invalid</span>
        <span className="memory-label">{packets.length.toLocaleString()} / 50,000</span>
        <button className="tool-button" onClick={onClear}><Eraser size={15} /> Clear</button>
      </div>
      <div className="packet-table-wrap">
        <table className="packet-table">
          <thead><tr><th>#</th><th>TIME</th><th>EPOCH</th><th>DIR</th><th>LEN</th><th>STATUS</th><th>PAYLOAD</th></tr></thead>
          <tbody>
            {visible.map((packet) => (
              <tr key={packet.id} className={!packet.valid ? "invalid-row" : ""}>
                <td>{packet.sequence}</td><td>{(packet.monotonicOffsetNs / 1e9).toFixed(3)}</td><td>{packet.sourceEpoch}</td>
                <td><span className={`direction-badge ${packet.direction}`}>{packet.direction.toUpperCase()}</span></td>
                <td>{packet.bytes.length}</td>
                <td>{packet.valid ? <span className="status-valid">OK</span> : <span className="status-invalid">{packet.errorCode ?? "INVALID"}</span>}</td>
                <td><code>{hex(packet.bytes)}</code></td>
              </tr>
            ))}
          </tbody>
        </table>
        {visible.length === 0 && <div className="empty-state"><span>PKT</span> No matching packets</div>}
      </div>
    </section>
  );
}

