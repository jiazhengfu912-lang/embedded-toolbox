use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

pub const PROJECT_SCHEMA_VERSION: &str = "1.0.0";
pub const PIPELINE_SEMANTIC_VERSION: &str = "1.0.0";
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Rx,
    Tx,
}

#[derive(Debug, Clone)]
pub struct RawChunk {
    pub source_id: Uuid,
    pub source_epoch: u64,
    pub sequence: u64,
    pub monotonic_offset_ns: i64,
    pub direction: Direction,
    pub bytes: Arc<[u8]>,
    pub gap_before: bool,
    pub tx_job_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClockAnchor {
    pub utc_anchor_unix_ns: i64,
    pub monotonic_anchor_ns: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EpochIdentity {
    pub epoch_id: Uuid,
    pub runtime_instance_id: Uuid,
    pub source_id: Uuid,
    pub source_epoch: u64,
    pub session_epoch_ordinal: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerialConfig {
    pub id: Uuid,
    pub port_name: String,
    pub baud_rate: u32,
    pub data_bits: u8,
    pub stop_bits: u8,
    pub parity: Parity,
    pub flow_control: FlowControl,
    pub timeout_ms: u64,
}

impl Default for SerialConfig {
    fn default() -> Self {
        Self {
            id: Uuid::now_v7(),
            port_name: String::new(),
            baud_rate: 115_200,
            data_bits: 8,
            stop_bits: 1,
            parity: Parity::None,
            flow_control: FlowControl::None,
            timeout_ms: 20,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Parity {
    None,
    Odd,
    Even,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FlowControl {
    None,
    Software,
    Hardware,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerialPortDescriptor {
    pub name: String,
    pub kind: String,
    pub vid: Option<u16>,
    pub pid: Option<u16>,
    pub manufacturer: Option<String>,
    pub product: Option<String>,
    pub serial_number: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyntheticConfig {
    pub rate_hz: u32,
    pub seed: u64,
    #[serde(default)]
    pub faults: FaultPlan,
}

impl Default for SyntheticConfig {
    fn default() -> Self {
        Self {
            rate_hz: 50,
            seed: 0x5eed,
            faults: FaultPlan::default(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FaultPlan {
    pub disconnect_after_frames: Option<u64>,
    pub corrupt_every_frames: Option<u64>,
    pub drop_every_frames: Option<u64>,
    pub duplicate_every_frames: Option<u64>,
    pub stall_every_frames: Option<u64>,
    pub burst_every_frames: Option<u64>,
    pub fragment_max_bytes: Option<usize>,
    pub fail_write_every_jobs: Option<u64>,
    pub partial_write_every_jobs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "mode",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum FramerSpec {
    EndDelimiter {
        id: Uuid,
        delimiter: Vec<u8>,
        max_frame_bytes: usize,
    },
    StartEnd {
        id: Uuid,
        start: Vec<u8>,
        end: Vec<u8>,
        max_frame_bytes: usize,
    },
    FixedLength {
        id: Uuid,
        length: usize,
        max_frame_bytes: usize,
    },
    LengthField {
        id: Uuid,
        sync_prefix: Vec<u8>,
        length_offset: usize,
        length_width: usize,
        byte_order: ByteOrder,
        length_adjustment: i32,
        max_frame_bytes: usize,
    },
}

impl FramerSpec {
    pub fn max_frame_bytes(&self) -> usize {
        match self {
            Self::EndDelimiter {
                max_frame_bytes, ..
            }
            | Self::StartEnd {
                max_frame_bytes, ..
            }
            | Self::FixedLength {
                max_frame_bytes, ..
            }
            | Self::LengthField {
                max_frame_bytes, ..
            } => *max_frame_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ByteOrder {
    Little,
    Big,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChecksumSpec {
    pub id: Uuid,
    pub algorithm: ChecksumAlgorithm,
    pub start_offset: usize,
    pub end_offset_exclusive: usize,
    pub stored_offset: usize,
    pub stored_width: usize,
    pub byte_order: ByteOrder,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ChecksumAlgorithm {
    Xor8,
    Sum8,
    Crc8,
    Crc16Modbus,
    Crc16Ccitt,
    Crc32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum DecoderSpec {
    Text { id: Uuid },
    Csv { id: Uuid, delimiter: char },
    Json { id: Uuid },
    Binary { id: Uuid, fields: Vec<FieldSpec> },
}

impl DecoderSpec {
    pub fn id(&self) -> Uuid {
        match self {
            Self::Text { id }
            | Self::Csv { id, .. }
            | Self::Json { id }
            | Self::Binary { id, .. } => *id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldSpec {
    pub id: Uuid,
    pub name: String,
    pub offset: usize,
    pub field_type: FieldType,
    pub byte_order: ByteOrder,
    pub scale: f64,
    pub bias: f64,
    pub unit: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    F32,
    F64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ChannelSource {
    CsvIndex { index: usize },
    JsonPath { path: String },
    BinaryField { field_id: Uuid },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelSpec {
    pub id: Uuid,
    pub name: String,
    pub unit: String,
    pub color: String,
    pub source: ChannelSource,
    pub transforms: Vec<TransformSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum TransformSpec {
    ScaleBias { id: Uuid, scale: f64, bias: f64 },
    Ema { id: Uuid, alpha: f64 },
    MovingAverage { id: Uuid, window: usize },
    Derivative { id: Uuid },
}

impl TransformSpec {
    pub fn id(&self) -> Uuid {
        match self {
            Self::ScaleBias { id, .. }
            | Self::Ema { id, .. }
            | Self::MovingAverage { id, .. }
            | Self::Derivative { id } => *id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandProfile {
    pub id: Uuid,
    pub name: String,
    pub text_template: String,
    pub min_gap_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PidProfile {
    pub id: Uuid,
    pub name: String,
    pub setpoint_channel_id: Uuid,
    pub measured_channel_id: Uuid,
    pub output_channel_id: Uuid,
    pub command_profile_id: Uuid,
    pub kp_min: f64,
    pub kp_max: f64,
    pub ki_min: f64,
    pub ki_max: f64,
    pub kd_min: f64,
    pub kd_max: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceProfile {
    pub id: Uuid,
    pub name: String,
    pub serial: SerialConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewSpec {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub settings: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolboxProject {
    pub id: Uuid,
    pub name: String,
    pub project_schema_version: String,
    pub pipeline_semantic_version: String,
    pub app_version: String,
    pub source: SourceProfile,
    pub framer: FramerSpec,
    pub checksum: Option<ChecksumSpec>,
    pub decoder: DecoderSpec,
    pub channels: Vec<ChannelSpec>,
    pub commands: Vec<CommandProfile>,
    pub pid_profiles: Vec<PidProfile>,
    pub views: Vec<ViewSpec>,
    #[serde(default)]
    pub ui: BTreeMap<String, Value>,
}

impl ToolboxProject {
    pub fn demo() -> Self {
        let source_id = Uuid::now_v7();
        let setpoint = Uuid::now_v7();
        let measured = Uuid::now_v7();
        let output = Uuid::now_v7();
        let command = Uuid::now_v7();
        Self {
            id: Uuid::now_v7(),
            name: "PID Loop Demo".into(),
            project_schema_version: PROJECT_SCHEMA_VERSION.into(),
            pipeline_semantic_version: PIPELINE_SEMANTIC_VERSION.into(),
            app_version: APP_VERSION.into(),
            source: SourceProfile {
                id: source_id,
                name: "Primary device".into(),
                serial: SerialConfig::default(),
            },
            framer: FramerSpec::EndDelimiter {
                id: Uuid::now_v7(),
                delimiter: vec![b'\n'],
                max_frame_bytes: 64 * 1024,
            },
            checksum: None,
            decoder: DecoderSpec::Csv {
                id: Uuid::now_v7(),
                delimiter: ',',
            },
            channels: vec![
                ChannelSpec {
                    id: setpoint,
                    name: "Setpoint".into(),
                    unit: "%".into(),
                    color: "#ffb454".into(),
                    source: ChannelSource::CsvIndex { index: 0 },
                    transforms: vec![],
                },
                ChannelSpec {
                    id: measured,
                    name: "Measured".into(),
                    unit: "%".into(),
                    color: "#4de3c1".into(),
                    source: ChannelSource::CsvIndex { index: 1 },
                    transforms: vec![TransformSpec::Ema {
                        id: Uuid::now_v7(),
                        alpha: 0.18,
                    }],
                },
                ChannelSpec {
                    id: output,
                    name: "Output".into(),
                    unit: "%".into(),
                    color: "#7aa2ff".into(),
                    source: ChannelSource::CsvIndex { index: 2 },
                    transforms: vec![],
                },
            ],
            commands: vec![CommandProfile {
                id: command,
                name: "Set PID".into(),
                text_template: "PID,{kp},{ki},{kd}\r\n".into(),
                min_gap_ms: 20,
            }],
            pid_profiles: vec![PidProfile {
                id: Uuid::now_v7(),
                name: "Main loop".into(),
                setpoint_channel_id: setpoint,
                measured_channel_id: measured,
                output_channel_id: output,
                command_profile_id: command,
                kp_min: 0.0,
                kp_max: 100.0,
                ki_min: 0.0,
                ki_max: 100.0,
                kd_min: 0.0,
                kd_max: 100.0,
            }],
            views: ["Terminal", "Plotter", "Packets", "PID Tuner"]
                .into_iter()
                .map(|name| ViewSpec {
                    id: Uuid::now_v7(),
                    name: name.into(),
                    kind: name.replace(' ', "").to_lowercase(),
                    settings: BTreeMap::new(),
                })
                .collect(),
            ui: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SourceStatus {
    Disconnected,
    Connecting,
    Connected,
    Faulted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueDepths {
    pub recorder_bytes: usize,
    pub parser_bytes: usize,
    pub terminal_bytes: usize,
    pub ipc_bytes: usize,
    pub tx_bytes: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceStats {
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub parsed_frames: u64,
    pub checksum_failures: u64,
    pub parser_gaps: u64,
    pub ui_dropped_bytes: u64,
    pub ui_dropped_batches: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceSnapshot {
    pub source_id: Uuid,
    pub name: String,
    pub status: SourceStatus,
    pub transport: String,
    pub endpoint: String,
    pub source_epoch: u64,
    pub epoch_id: Option<Uuid>,
    pub clock_anchor: Option<ClockAnchor>,
    pub queue_depths: QueueDepths,
    pub stats: SourceStats,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SessionStatus {
    Idle,
    Recording,
    Suspended,
    Finalizing,
    Closed,
    Interrupted,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionSnapshot {
    pub status: SessionStatus,
    pub session_id: Option<Uuid>,
    pub path: Option<String>,
    pub epoch_ordinal: Option<u32>,
    pub bytes_written: u64,
    pub checkpoint_pending: bool,
    pub message: Option<String>,
}

impl Default for SessionSnapshot {
    fn default() -> Self {
        Self {
            status: SessionStatus::Idle,
            session_id: None,
            path: None,
            epoch_ordinal: None,
            bytes_written: 0,
            checkpoint_pending: false,
            message: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppSnapshot {
    pub runtime_instance_id: Uuid,
    pub app_version: String,
    pub event_cursor: u64,
    pub project: ToolboxProject,
    pub sources: Vec<SourceSnapshot>,
    pub session: SessionSnapshot,
    pub latest_channel_values: BTreeMap<Uuid, f64>,
    pub recent_errors: Vec<crate::core::error::ToolboxError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppEvent {
    pub runtime_instance_id: Uuid,
    pub cursor: u64,
    pub event_type: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventResume {
    pub runtime_instance_id: Uuid,
    pub last_cursor: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventSubscription {
    pub runtime_instance_id: Uuid,
    pub current_cursor: u64,
    pub resync_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TxRequest {
    pub source_id: Uuid,
    pub payload: Vec<u8>,
    pub origin: String,
    pub not_before_unix_ms: Option<i64>,
    pub deadline_unix_ms: Option<i64>,
    pub min_gap_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TxAccepted {
    pub job_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FrameView {
    pub sequence: u64,
    pub monotonic_offset_ns: i64,
    pub direction: Direction,
    pub bytes: Vec<u8>,
    pub valid: bool,
    pub error_code: Option<String>,
    pub fields: BTreeMap<Uuid, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SampleView {
    pub channel_id: Uuid,
    pub value: f64,
    pub monotonic_offset_ns: i64,
    pub frame_sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Public pipeline reset contract; V1 UI does not expose every trigger yet.
pub enum ResetReason {
    Connect,
    Disconnect,
    ConfigChanged,
    StreamGap,
    ReplayStart,
    ReplaySeek,
    EpochChanged,
    ManualReset,
}
