use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    DeviceNotFound,
    DeviceBusy,
    DeviceOpen,
    DeviceRead,
    DeviceWrite,
    DeviceDisconnected,
    ProductSourceLimit,
    TxQueueFull,
    TxTimeout,
    TxCancelled,
    TxPartialWrite,
    FrameOversize,
    FrameResync,
    ChecksumMismatch,
    ParseFailed,
    ParserBackpressure,
    RecordingBackpressure,
    ProjectSchemaInvalid,
    PipelineVersionUnsupported,
    IpcEnvelopeVersion,
    IpcPayloadVersion,
    EventCursorExpired,
    SessionState,
    SessionOpen,
    SessionWrite,
    SessionCheckpointBusy,
    SessionCheckpointFailed,
    SessionCorrupt,
    EpochIdentity,
    ReplayActiveSession,
    ReplayFailed,
    Internal,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DeviceNotFound => "E_DEVICE_NOT_FOUND",
            Self::DeviceBusy => "E_DEVICE_BUSY",
            Self::DeviceOpen => "E_DEVICE_OPEN",
            Self::DeviceRead => "E_DEVICE_READ",
            Self::DeviceWrite => "E_DEVICE_WRITE",
            Self::DeviceDisconnected => "E_DEVICE_DISCONNECTED",
            Self::ProductSourceLimit => "E_PRODUCT_SOURCE_LIMIT",
            Self::TxQueueFull => "E_TX_QUEUE_FULL",
            Self::TxTimeout => "E_TX_TIMEOUT",
            Self::TxCancelled => "E_TX_CANCELLED",
            Self::TxPartialWrite => "E_TX_PARTIAL_WRITE",
            Self::FrameOversize => "E_FRAME_OVERSIZE",
            Self::FrameResync => "E_FRAME_RESYNC",
            Self::ChecksumMismatch => "E_CHECKSUM_MISMATCH",
            Self::ParseFailed => "E_PARSE_FAILED",
            Self::ParserBackpressure => "E_PARSER_BACKPRESSURE",
            Self::RecordingBackpressure => "E_RECORDING_BACKPRESSURE",
            Self::ProjectSchemaInvalid => "E_PROJECT_SCHEMA_INVALID",
            Self::PipelineVersionUnsupported => "E_PIPELINE_VERSION_UNSUPPORTED",
            Self::IpcEnvelopeVersion => "E_IPC_ENVELOPE_VERSION",
            Self::IpcPayloadVersion => "E_IPC_PAYLOAD_VERSION",
            Self::EventCursorExpired => "E_EVENT_CURSOR_EXPIRED",
            Self::SessionState => "E_SESSION_STATE",
            Self::SessionOpen => "E_SESSION_OPEN",
            Self::SessionWrite => "E_SESSION_WRITE",
            Self::SessionCheckpointBusy => "E_SESSION_CHECKPOINT_BUSY",
            Self::SessionCheckpointFailed => "E_SESSION_CHECKPOINT_FAILED",
            Self::SessionCorrupt => "E_SESSION_CORRUPT",
            Self::EpochIdentity => "E_EPOCH_IDENTITY",
            Self::ReplayActiveSession => "E_REPLAY_ACTIVE_SESSION",
            Self::ReplayFailed => "E_REPLAY_FAILED",
            Self::Internal => "E_INTERNAL",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Error)]
#[error("{code}: {message_key}")]
#[serde(rename_all = "camelCase")]
pub struct ToolboxError {
    pub code: String,
    pub severity: ErrorSeverity,
    pub recoverable: bool,
    pub source_id: Option<Uuid>,
    pub operation: String,
    pub message_key: String,
    pub context: BTreeMap<String, Value>,
    pub cause: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ErrorSeverity {
    Info,
    Warning,
    Error,
    Fatal,
}

impl ToolboxError {
    pub fn new(
        code: ErrorCode,
        operation: impl Into<String>,
        message_key: impl Into<String>,
    ) -> Self {
        Self {
            code: code.as_str().to_string(),
            severity: ErrorSeverity::Error,
            recoverable: true,
            source_id: None,
            operation: operation.into(),
            message_key: message_key.into(),
            context: BTreeMap::new(),
            cause: None,
        }
    }

    pub fn source(mut self, source_id: Uuid) -> Self {
        self.source_id = Some(source_id);
        self
    }

    pub fn cause(mut self, cause: impl ToString) -> Self {
        self.cause = Some(cause.to_string());
        self
    }

    pub fn context(mut self, key: impl Into<String>, value: impl Serialize) -> Self {
        if let Ok(value) = serde_json::to_value(value) {
            self.context.insert(key.into(), value);
        }
        self
    }
}

pub type ToolboxResult<T> = Result<T, ToolboxError>;
