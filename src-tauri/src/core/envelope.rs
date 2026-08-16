use crate::core::model::Direction;
use serde::Serialize;
use tauri::ipc::InvokeResponseBody;
use uuid::Uuid;

pub const ENVELOPE_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy)]
#[repr(u16)]
pub enum PayloadType {
    RawBatch = 1,
    PacketBatch = 2,
    SampleBatch = 3,
    DiagnosticBatch = 4,
}

pub struct EnvelopeMeta {
    pub payload_type: PayloadType,
    pub payload_version: u16,
    pub source_id: Uuid,
    pub source_epoch: u64,
    pub sequence: u64,
    pub monotonic_offset_ns: i64,
}

pub fn encode(meta: EnvelopeMeta, payload: &[u8]) -> InvokeResponseBody {
    let mut bytes = Vec::with_capacity(56 + payload.len());
    bytes.extend_from_slice(b"ETBX");
    bytes.extend_from_slice(&ENVELOPE_VERSION.to_le_bytes());
    bytes.extend_from_slice(&(meta.payload_type as u16).to_le_bytes());
    bytes.extend_from_slice(&meta.payload_version.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(meta.source_id.as_bytes());
    bytes.extend_from_slice(&meta.source_epoch.to_le_bytes());
    bytes.extend_from_slice(&meta.sequence.to_le_bytes());
    bytes.extend_from_slice(&meta.monotonic_offset_ns.to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(payload);
    InvokeResponseBody::Raw(bytes)
}

pub fn encode_raw_payload(direction: Direction, chunk_sequence: u64, bytes: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(13 + bytes.len());
    payload.push(match direction {
        Direction::Rx => 0,
        Direction::Tx => 1,
    });
    payload.extend_from_slice(&chunk_sequence.to_le_bytes());
    payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    payload.extend_from_slice(bytes);
    payload
}

pub fn encode_json_payload<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).unwrap_or_else(|_| b"[]".to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_and_payload_versions_are_independent() {
        let body = encode(
            EnvelopeMeta {
                payload_type: PayloadType::RawBatch,
                payload_version: 7,
                source_id: Uuid::nil(),
                source_epoch: 2,
                sequence: 3,
                monotonic_offset_ns: 4,
            },
            b"abc",
        );
        let InvokeResponseBody::Raw(bytes) = body else {
            panic!("raw body expected")
        };
        assert_eq!(&bytes[..4], b"ETBX");
        assert_eq!(u16::from_le_bytes([bytes[4], bytes[5]]), ENVELOPE_VERSION);
        assert_eq!(u16::from_le_bytes([bytes[8], bytes[9]]), 7);
    }
}
