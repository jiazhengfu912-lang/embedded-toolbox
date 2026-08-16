import { describe, expect, it } from "vitest";
import { PayloadType, decodeEnvelope, isNextControlEvent } from "./ipc";

function envelope(payloadType: number, payload: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(56 + payload.length);
  bytes.set(new TextEncoder().encode("ETBX"), 0);
  const view = new DataView(bytes.buffer);
  view.setUint16(4, 1, true);
  view.setUint16(6, payloadType, true);
  view.setUint16(8, 1, true);
  bytes.set(Uint8Array.from([1, 152, 163, 232, 31, 72, 124, 17, 141, 184, 240, 65, 109, 71, 16, 2]), 12);
  view.setBigUint64(28, 3n, true);
  view.setBigUint64(36, 9n, true);
  view.setBigInt64(44, 2_500_000_000n, true);
  view.setUint32(52, payload.length, true);
  bytes.set(payload, 56);
  return bytes;
}

describe("versioned IPC envelope", () => {
  it("decodes raw direction, sequence, timestamp and bytes", () => {
    const payload = new Uint8Array(16);
    const view = new DataView(payload.buffer);
    payload[0] = 0;
    view.setBigUint64(1, 44n, true);
    view.setUint32(9, 3, true);
    payload.set([0xaa, 0x55, 0x01], 13);
    const decoded = decodeEnvelope(envelope(PayloadType.RawBatch, payload));
    expect(decoded.type).toBe(PayloadType.RawBatch);
    if (decoded.type !== PayloadType.RawBatch) return;
    expect(decoded.sourceEpoch).toBe(3);
    expect(decoded.terminal.sequence).toBe(44);
    expect(decoded.terminal.timeSeconds).toBe(2.5);
    expect(Array.from(decoded.terminal.bytes)).toEqual([0xaa, 0x55, 0x01]);
  });

  it("rejects unsupported envelope versions", () => {
    const bytes = envelope(PayloadType.SampleBatch, new TextEncoder().encode("[]"));
    new DataView(bytes.buffer).setUint16(4, 2, true);
    expect(() => decodeEnvelope(bytes)).toThrow("E_IPC_ENVELOPE_VERSION");
  });

  it("decodes JSON sample batches independently from the envelope", () => {
    const payload = new TextEncoder().encode(JSON.stringify([{ channelId: "ch", value: 1.5, monotonicOffsetNs: 9, frameSequence: 7 }]));
    const decoded = decodeEnvelope(envelope(PayloadType.SampleBatch, payload));
    expect(decoded.type).toBe(PayloadType.SampleBatch);
    if (decoded.type === PayloadType.SampleBatch) expect(decoded.samples[0].value).toBe(1.5);
  });
});

describe("EventCursor continuity", () => {
  const base = { runtimeInstanceId: "runtime-a", cursor: 11, eventType: "source.connected", payload: {} };

  it("accepts only the next cursor from the same process instance", () => {
    expect(isNextControlEvent("runtime-a", 10, base)).toBe(true);
    expect(isNextControlEvent("runtime-a", 9, base)).toBe(false);
    expect(isNextControlEvent("runtime-b", 10, base)).toBe(false);
  });
});
