export type SourceStatus = "disconnected" | "connecting" | "connected" | "faulted";
export type SessionStatus =
  | "idle"
  | "recording"
  | "suspended"
  | "finalizing"
  | "closed"
  | "interrupted"
  | "failed";

export interface ToolboxError {
  code: string;
  severity: "info" | "warning" | "error" | "fatal";
  recoverable: boolean;
  sourceId?: string;
  operation: string;
  messageKey: string;
  context: Record<string, unknown>;
  cause?: string;
}

export interface ChannelSpec {
  id: string;
  name: string;
  unit: string;
  color: string;
}

export interface CommandProfile {
  id: string;
  name: string;
  textTemplate: string;
  minGapMs: number;
}

export interface PidProfile {
  id: string;
  name: string;
  setpointChannelId: string;
  measuredChannelId: string;
  outputChannelId: string;
  commandProfileId: string;
  kpMin: number;
  kpMax: number;
  kiMin: number;
  kiMax: number;
  kdMin: number;
  kdMax: number;
}

export interface ToolboxProject {
  id: string;
  name: string;
  projectSchemaVersion: string;
  pipelineSemanticVersion: string;
  appVersion: string;
  source: {
    id: string;
    name: string;
    serial: SerialConfig;
  };
  channels: ChannelSpec[];
  commands: CommandProfile[];
  pidProfiles: PidProfile[];
  views: Array<{ id: string; name: string; kind: string; settings: Record<string, unknown> }>;
  [key: string]: unknown;
}

export interface QueueDepths {
  recorderBytes: number;
  parserBytes: number;
  terminalBytes: number;
  ipcBytes: number;
  txBytes: number;
}

export interface SourceStats {
  rxBytes: number;
  txBytes: number;
  parsedFrames: number;
  checksumFailures: number;
  parserGaps: number;
  uiDroppedBytes: number;
  uiDroppedBatches: number;
}

export interface SourceSnapshot {
  sourceId: string;
  name: string;
  status: SourceStatus;
  transport: string;
  endpoint: string;
  sourceEpoch: number;
  epochId?: string;
  clockAnchor?: {
    utcAnchorUnixNs: number;
    monotonicAnchorNs: number;
  };
  queueDepths: QueueDepths;
  stats: SourceStats;
}

export interface SessionSnapshot {
  status: SessionStatus;
  sessionId?: string;
  path?: string;
  epochOrdinal?: number;
  bytesWritten: number;
  checkpointPending: boolean;
  message?: string;
}

export interface AppSnapshot {
  runtimeInstanceId: string;
  appVersion: string;
  eventCursor: number;
  project: ToolboxProject;
  sources: SourceSnapshot[];
  session: SessionSnapshot;
  latestChannelValues: Record<string, number>;
  recentErrors: ToolboxError[];
}

export interface AppEvent {
  runtimeInstanceId: string;
  cursor: number;
  eventType: string;
  payload: unknown;
}

export interface EventSubscription {
  runtimeInstanceId: string;
  currentCursor: number;
  resyncRequired: boolean;
}

export interface SerialPortDescriptor {
  name: string;
  kind: string;
  vid?: number;
  pid?: number;
  manufacturer?: string;
  product?: string;
  serialNumber?: string;
}

export interface SerialConfig {
  id: string;
  portName: string;
  baudRate: number;
  dataBits: number;
  stopBits: number;
  parity: "none" | "odd" | "even";
  flowControl: "none" | "software" | "hardware";
  timeoutMs: number;
}

export interface FaultPlan {
  disconnectAfterFrames?: number;
  corruptEveryFrames?: number;
  dropEveryFrames?: number;
  duplicateEveryFrames?: number;
  stallEveryFrames?: number;
  burstEveryFrames?: number;
  fragmentMaxBytes?: number;
  failWriteEveryJobs?: number;
  partialWriteEveryJobs?: number;
}

export interface SyntheticConfig {
  rateHz: number;
  seed: number;
  faults: FaultPlan;
}

export interface TxRequest {
  sourceId: string;
  payload: number[];
  origin: string;
  notBeforeUnixMs?: number;
  deadlineUnixMs?: number;
  minGapMs?: number;
}

export interface FrameView {
  sequence: number;
  monotonicOffsetNs: number;
  direction: "rx" | "tx";
  bytes: number[];
  valid: boolean;
  errorCode?: string;
  fields: Record<string, number>;
}

export interface SampleView {
  channelId: string;
  value: number;
  monotonicOffsetNs: number;
  frameSequence: number;
}

export interface TerminalEntry {
  id: string;
  sequence: number;
  timeSeconds: number;
  direction: "rx" | "tx";
  bytes: Uint8Array;
}

export interface PacketEntry extends FrameView {
  id: string;
  sourceEpoch: number;
}

export interface PlotPoint {
  timeSeconds: number;
  value: number;
  sequence: number;
}

export interface DiagnosticEntry {
  id: string;
  timeSeconds: number;
  values: Record<string, number>;
  count?: number;
}

export type PlotSeries = Record<string, PlotPoint[]>;
