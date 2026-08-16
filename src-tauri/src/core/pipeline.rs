use crate::core::checksum::validate_checksum;
use crate::core::error::{ErrorCode, ToolboxError, ToolboxResult};
use crate::core::framer::Framer;
use crate::core::model::{
    ByteOrder, ChannelSource, DecoderSpec, FieldSpec, FieldType, FrameView, RawChunk, ResetReason,
    SampleView, ToolboxProject,
};
use crate::core::transform::TransformEngine;
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Default)]
pub struct PipelineOutput {
    pub frames: Vec<FrameView>,
    pub samples: Vec<SampleView>,
    pub oversize_frames: u64,
    pub resyncs: u64,
    pub checksum_failures: u64,
    pub parse_failures: u64,
}

pub struct Pipeline {
    project: ToolboxProject,
    framer: Framer,
    transforms: TransformEngine,
    next_frame_sequence: u64,
}

impl Pipeline {
    pub fn new(project: ToolboxProject) -> ToolboxResult<Self> {
        let framer = Framer::new(project.framer.clone()).map_err(|message| {
            ToolboxError::new(
                ErrorCode::ProjectSchemaInvalid,
                "pipeline.new",
                "framer_invalid",
            )
            .cause(message)
        })?;
        Ok(Self {
            project,
            framer,
            transforms: TransformEngine::default(),
            next_frame_sequence: 0,
        })
    }

    pub fn reset(&mut self, reason: ResetReason) {
        self.framer.reset();
        self.transforms.reset(reason);
    }

    pub fn process(&mut self, chunk: &RawChunk) -> PipelineOutput {
        let mut output = PipelineOutput::default();
        if chunk.gap_before {
            self.reset(ResetReason::StreamGap);
        }
        let framed = self.framer.push(&chunk.bytes);
        output.oversize_frames = framed.oversize_frames;
        output.resyncs = framed.resyncs;
        for bytes in framed.frames {
            self.next_frame_sequence = self.next_frame_sequence.saturating_add(1);
            let frame_sequence = self.next_frame_sequence;
            let mut frame = FrameView {
                sequence: frame_sequence,
                monotonic_offset_ns: chunk.monotonic_offset_ns,
                direction: chunk.direction,
                bytes: bytes.clone(),
                valid: true,
                error_code: None,
                fields: BTreeMap::new(),
            };
            if let Some(spec) = &self.project.checksum {
                if let Err(error) = validate_checksum(&bytes, spec) {
                    frame.valid = false;
                    frame.error_code = Some(error.code);
                    output.checksum_failures += 1;
                }
            }
            if frame.valid {
                match self.decode_frame(
                    &bytes,
                    chunk.source_id,
                    chunk.monotonic_offset_ns,
                    frame_sequence,
                ) {
                    Ok((fields, samples)) => {
                        frame.fields = fields;
                        output.samples.extend(samples);
                    }
                    Err(error) => {
                        frame.valid = false;
                        frame.error_code = Some(error.code);
                        output.parse_failures += 1;
                    }
                }
            }
            output.frames.push(frame);
        }
        output
    }

    fn decode_frame(
        &mut self,
        bytes: &[u8],
        source_id: Uuid,
        timestamp_ns: i64,
        frame_sequence: u64,
    ) -> ToolboxResult<(BTreeMap<Uuid, f64>, Vec<SampleView>)> {
        let mut fields = BTreeMap::new();
        let mut raw_channels = BTreeMap::new();
        match &self.project.decoder {
            DecoderSpec::Text { .. } => {}
            DecoderSpec::Csv { delimiter, .. } => {
                let text = std::str::from_utf8(bytes).map_err(|error| {
                    ToolboxError::new(ErrorCode::ParseFailed, "pipeline.csv", "utf8_invalid")
                        .cause(error)
                })?;
                let values: Vec<&str> = text.split(*delimiter).map(str::trim).collect();
                for channel in &self.project.channels {
                    if let ChannelSource::CsvIndex { index } = channel.source {
                        let value = values
                            .get(index)
                            .ok_or_else(|| {
                                ToolboxError::new(
                                    ErrorCode::ParseFailed,
                                    "pipeline.csv",
                                    "csv_index_missing",
                                )
                                .context("index", index)
                            })?
                            .parse::<f64>()
                            .map_err(|error| {
                                ToolboxError::new(
                                    ErrorCode::ParseFailed,
                                    "pipeline.csv",
                                    "csv_number_invalid",
                                )
                                .cause(error)
                            })?;
                        raw_channels.insert(channel.id, value);
                    }
                }
            }
            DecoderSpec::Json { .. } => {
                let value: Value = serde_json::from_slice(bytes).map_err(|error| {
                    ToolboxError::new(ErrorCode::ParseFailed, "pipeline.json", "json_invalid")
                        .cause(error)
                })?;
                for channel in &self.project.channels {
                    if let ChannelSource::JsonPath { path } = &channel.source {
                        let raw =
                            json_path(&value, path)
                                .and_then(Value::as_f64)
                                .ok_or_else(|| {
                                    ToolboxError::new(
                                        ErrorCode::ParseFailed,
                                        "pipeline.json",
                                        "json_path_missing",
                                    )
                                    .context("path", path)
                                })?;
                        raw_channels.insert(channel.id, raw);
                    }
                }
            }
            DecoderSpec::Binary { fields: specs, .. } => {
                for spec in specs {
                    fields.insert(spec.id, decode_field(bytes, spec)?);
                }
                for channel in &self.project.channels {
                    if let ChannelSource::BinaryField { field_id } = channel.source {
                        if let Some(value) = fields.get(&field_id) {
                            raw_channels.insert(channel.id, *value);
                        }
                    }
                }
            }
        }
        let mut samples = Vec::with_capacity(raw_channels.len());
        for channel in &self.project.channels {
            if let Some(value) = raw_channels.get(&channel.id) {
                let transformed = self.transforms.apply(
                    source_id,
                    channel.id,
                    &channel.transforms,
                    *value,
                    timestamp_ns,
                );
                if transformed.is_finite() {
                    samples.push(SampleView {
                        channel_id: channel.id,
                        value: transformed,
                        monotonic_offset_ns: timestamp_ns,
                        frame_sequence,
                    });
                }
            }
        }
        Ok((fields, samples))
    }
}

fn json_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    path.trim_matches('.')
        .split('.')
        .filter(|part| !part.is_empty())
        .try_fold(root, |value, key| value.get(key))
}

fn decode_field(bytes: &[u8], spec: &FieldSpec) -> ToolboxResult<f64> {
    let width = match spec.field_type {
        FieldType::U8 | FieldType::I8 => 1,
        FieldType::U16 | FieldType::I16 => 2,
        FieldType::U32 | FieldType::I32 | FieldType::F32 => 4,
        FieldType::F64 => 8,
    };
    let raw = bytes.get(spec.offset..spec.offset + width).ok_or_else(|| {
        ToolboxError::new(
            ErrorCode::ParseFailed,
            "pipeline.binary",
            "field_out_of_range",
        )
        .context("field", &spec.name)
    })?;
    let value = match spec.field_type {
        FieldType::U8 => raw[0] as f64,
        FieldType::I8 => (raw[0] as i8) as f64,
        FieldType::U16 => read_u16(raw, spec.byte_order) as f64,
        FieldType::I16 => read_u16(raw, spec.byte_order) as i16 as f64,
        FieldType::U32 => read_u32(raw, spec.byte_order) as f64,
        FieldType::I32 => read_u32(raw, spec.byte_order) as i32 as f64,
        FieldType::F32 => f32::from_bits(read_u32(raw, spec.byte_order)) as f64,
        FieldType::F64 => f64::from_bits(read_u64(raw, spec.byte_order)),
    };
    Ok(value * spec.scale + spec.bias)
}

fn read_u16(bytes: &[u8], order: ByteOrder) -> u16 {
    match order {
        ByteOrder::Little => u16::from_le_bytes([bytes[0], bytes[1]]),
        ByteOrder::Big => u16::from_be_bytes([bytes[0], bytes[1]]),
    }
}
fn read_u32(bytes: &[u8], order: ByteOrder) -> u32 {
    let array = [bytes[0], bytes[1], bytes[2], bytes[3]];
    match order {
        ByteOrder::Little => u32::from_le_bytes(array),
        ByteOrder::Big => u32::from_be_bytes(array),
    }
}
fn read_u64(bytes: &[u8], order: ByteOrder) -> u64 {
    let array = [
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ];
    match order {
        ByteOrder::Little => u64::from_le_bytes(array),
        ByteOrder::Big => u64::from_be_bytes(array),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::model::Direction;
    use std::sync::Arc;

    #[test]
    fn demo_csv_reaches_all_channels() {
        let project = ToolboxProject::demo();
        let mut pipeline = Pipeline::new(project).unwrap();
        let chunk = RawChunk {
            source_id: Uuid::nil(),
            source_epoch: 1,
            sequence: 1,
            monotonic_offset_ns: 100,
            direction: Direction::Rx,
            bytes: Arc::from(&b"50,48,20\n"[..]),
            gap_before: false,
            tx_job_id: None,
        };
        let output = pipeline.process(&chunk);
        assert_eq!(output.frames.len(), 1);
        assert_eq!(output.samples.len(), 3);
    }
}
