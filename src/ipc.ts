import { Channel, invoke } from "@tauri-apps/api/core";
import type {
  AppEvent,
  AppSnapshot,
  EventSubscription,
  FaultPlan,
  FrameView,
  SampleView,
  SerialConfig,
  SerialPortDescriptor,
  SourceSnapshot,
  SyntheticConfig,
  TerminalEntry,
  ToolboxError,
  TxRequest,
  ToolboxProject,
} from "./types";

export const ENVELOPE_VERSION = 1;
export const PAYLOAD_VERSION = 1;
const ENVELOPE_HEADER_BYTES = 56;

export enum PayloadType {
  RawBatch = 1,
  PacketBatch = 2,
  SampleBatch = 3,
  DiagnosticBatch = 4,
}

export type DecodedEnvelope =
  | { type: PayloadType.RawBatch; sourceId: string; sourceEpoch: number; sequence: number; terminal: TerminalEntry }
  | { type: PayloadType.PacketBatch; sourceId: string; sourceEpoch: number; sequence: number; packets: FrameView[] }
  | { type: PayloadType.SampleBatch; sourceId: string; sourceEpoch: number; sequence: number; samples: SampleView[] }
  | { type: PayloadType.DiagnosticBatch; sourceId: string; sourceEpoch: number; sequence: number; diagnostic: Record<string, number>; timeSeconds: number };

function uuidFromBytes(bytes: Uint8Array): string {
  const hex = Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function toUint8Array(input: ArrayBuffer | Uint8Array): Uint8Array {
  return input instanceof Uint8Array ? input : new Uint8Array(input);
}

export function decodeEnvelope(input: ArrayBuffer | Uint8Array): DecodedEnvelope {
  const bytes = toUint8Array(input);
  if (bytes.byteLength < ENVELOPE_HEADER_BYTES || new TextDecoder().decode(bytes.subarray(0, 4)) !== "ETBX") {
    throw new Error("E_IPC_ENVELOPE_INVALID");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const envelopeVersion = view.getUint16(4, true);
  const payloadType = view.getUint16(6, true) as PayloadType;
  const payloadVersion = view.getUint16(8, true);
  if (envelopeVersion !== ENVELOPE_VERSION) throw new Error("E_IPC_ENVELOPE_VERSION");
  if (payloadVersion !== PAYLOAD_VERSION) throw new Error("E_IPC_PAYLOAD_VERSION");
  const sourceId = uuidFromBytes(bytes.subarray(12, 28));
  const sourceEpoch = Number(view.getBigUint64(28, true));
  const sequence = Number(view.getBigUint64(36, true));
  const monotonicOffsetNs = Number(view.getBigInt64(44, true));
  const payloadLength = view.getUint32(52, true);
  if (ENVELOPE_HEADER_BYTES + payloadLength !== bytes.byteLength) throw new Error("E_IPC_PAYLOAD_LENGTH");
  const payload = bytes.subarray(ENVELOPE_HEADER_BYTES);

  if (payloadType === PayloadType.RawBatch) {
    if (payload.byteLength < 13) throw new Error("E_IPC_RAW_PAYLOAD_INVALID");
    const payloadView = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
    const direction = payload[0] === 0 ? "rx" : "tx";
    const chunkSequence = Number(payloadView.getBigUint64(1, true));
    const length = payloadView.getUint32(9, true);
    if (13 + length !== payload.byteLength) throw new Error("E_IPC_RAW_PAYLOAD_LENGTH");
    return {
      type: PayloadType.RawBatch,
      sourceId,
      sourceEpoch,
      sequence,
      terminal: {
        id: `${sourceEpoch}:${chunkSequence}`,
        sequence: chunkSequence,
        timeSeconds: monotonicOffsetNs / 1e9,
        direction,
        bytes: payload.slice(13),
      },
    };
  }

  const parsed = JSON.parse(new TextDecoder().decode(payload));
  if (payloadType === PayloadType.PacketBatch) {
    return { type: payloadType, sourceId, sourceEpoch, sequence, packets: parsed as FrameView[] };
  }
  if (payloadType === PayloadType.SampleBatch) {
    return { type: payloadType, sourceId, sourceEpoch, sequence, samples: parsed as SampleView[] };
  }
  if (payloadType === PayloadType.DiagnosticBatch) {
    return { type: payloadType, sourceId, sourceEpoch, sequence, diagnostic: parsed as Record<string, number>, timeSeconds: monotonicOffsetNs / 1e9 };
  }
  throw new Error("E_IPC_PAYLOAD_TYPE");
}

export function isTauriRuntime(): boolean {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

export function isNextControlEvent(runtimeInstanceId: string, cursor: number, event: AppEvent): boolean {
  return event.runtimeInstanceId === runtimeInstanceId && event.cursor === cursor + 1;
}

export const api = {
  snapshot: () => invoke<AppSnapshot>("app_get_snapshot"),
  ports: () => invoke<SerialPortDescriptor[]>("device_list_ports"),
  connectSerial: (config: SerialConfig) => invoke<SourceSnapshot>("device_connect_serial", { config }),
  connectSynthetic: (config: SyntheticConfig) => invoke<SourceSnapshot>("device_connect_synthetic", { config }),
  disconnect: (sourceId: string) => invoke<SourceSnapshot>("device_disconnect", { sourceId }),
  send: (request: TxRequest) => invoke<{ jobId: string }>("device_send", { request }),
  sessionStart: () => invoke("session_start_default"),
  sessionStop: () => invoke("session_stop"),
  sessionExportCsv: (sessionPath: string, csvPath: string) => invoke<number>("session_export_csv", { sessionPath, csvPath }),
  projectLoad: (path: string) => invoke<ToolboxProject>("project_load", { path }),
  projectSave: (path: string) => invoke("project_save", { path }),
  replayStart: (path: string, speed = 1) => invoke("replay_start", { path, speed }),
  replaySeek: (sessionOffsetNs: number) => invoke("replay_seek", { sessionOffsetNs }),
  replayStop: () => invoke("replay_stop"),
};

export async function subscribeEvents(
  resume: { runtimeInstanceId: string; lastCursor: number } | null,
  onEvent: (event: AppEvent) => void,
): Promise<EventSubscription> {
  const events = new Channel<AppEvent>(onEvent);
  return invoke<EventSubscription>("app_subscribe_events", { resume, events });
}

export async function subscribeStream(onEnvelope: (envelope: DecodedEnvelope) => void, onError: (error: Error) => void): Promise<number> {
  const stream = new Channel<ArrayBuffer | Uint8Array>((bytes) => {
    try {
      onEnvelope(decodeEnvelope(bytes));
    } catch (error) {
      onError(error instanceof Error ? error : new Error(String(error)));
    }
  });
  return invoke<number>("stream_subscribe", { stream });
}

export function normalizeError(error: unknown): ToolboxError {
  if (typeof error === "object" && error !== null && "code" in error) return error as ToolboxError;
  return {
    code: "E_INTERNAL",
    severity: "error",
    recoverable: true,
    operation: "frontend",
    messageKey: "unexpected_error",
    context: {},
    cause: error instanceof Error ? error.message : String(error),
  };
}

export function defaultSyntheticConfig(faults: FaultPlan = {}): SyntheticConfig {
  return { rateHz: 50, seed: 24301, faults };
}
