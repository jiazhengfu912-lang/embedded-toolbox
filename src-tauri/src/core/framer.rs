use crate::core::model::{ByteOrder, FramerSpec};

const HARD_MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Default)]
pub struct FramerOutput {
    pub frames: Vec<Vec<u8>>,
    pub oversize_frames: u64,
    pub resyncs: u64,
}

pub struct Framer {
    spec: FramerSpec,
    buffer: Vec<u8>,
    dropping_oversize: bool,
}

impl Framer {
    pub fn new(spec: FramerSpec) -> Result<Self, String> {
        let max = spec.max_frame_bytes();
        if max == 0 || max > HARD_MAX_FRAME_BYTES {
            return Err(format!(
                "maxFrameBytes must be between 1 and {HARD_MAX_FRAME_BYTES}"
            ));
        }
        match &spec {
            FramerSpec::EndDelimiter { delimiter, .. } if delimiter.is_empty() => {
                return Err("delimiter must not be empty".into());
            }
            FramerSpec::StartEnd { start, end, .. } if start.is_empty() || end.is_empty() => {
                return Err("start/end must not be empty".into());
            }
            FramerSpec::FixedLength {
                length,
                max_frame_bytes,
                ..
            } if *length == 0 || length > max_frame_bytes => {
                return Err("fixed length exceeds maxFrameBytes".into());
            }
            FramerSpec::LengthField {
                sync_prefix,
                length_width,
                ..
            } if sync_prefix.is_empty() || !matches!(*length_width, 1 | 2 | 4) => {
                return Err("length field requires sync prefix and width 1, 2, or 4".into());
            }
            _ => {}
        }
        Ok(Self {
            spec,
            buffer: Vec::new(),
            dropping_oversize: false,
        })
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
        self.dropping_oversize = false;
    }

    pub fn push(&mut self, bytes: &[u8]) -> FramerOutput {
        self.buffer.extend_from_slice(bytes);
        match self.spec.clone() {
            FramerSpec::EndDelimiter {
                delimiter,
                max_frame_bytes,
                ..
            } => self.drain_end_delimiter(&delimiter, max_frame_bytes),
            FramerSpec::StartEnd {
                start,
                end,
                max_frame_bytes,
                ..
            } => self.drain_start_end(&start, &end, max_frame_bytes),
            FramerSpec::FixedLength { length, .. } => self.drain_fixed(length),
            FramerSpec::LengthField {
                sync_prefix,
                length_offset,
                length_width,
                byte_order,
                length_adjustment,
                max_frame_bytes,
                ..
            } => self.drain_length(
                &sync_prefix,
                length_offset,
                length_width,
                byte_order,
                length_adjustment,
                max_frame_bytes,
            ),
        }
    }

    fn drain_end_delimiter(&mut self, delimiter: &[u8], max: usize) -> FramerOutput {
        let mut out = FramerOutput::default();
        loop {
            let Some(position) = find_subslice(&self.buffer, delimiter) else {
                if self.buffer.len() > max {
                    self.dropping_oversize = true;
                    out.oversize_frames += 1;
                    let keep = delimiter.len().saturating_sub(1);
                    if self.buffer.len() > keep {
                        self.buffer.drain(..self.buffer.len() - keep);
                    }
                }
                break;
            };
            if self.dropping_oversize || position > max {
                self.buffer.drain(..position + delimiter.len());
                self.dropping_oversize = false;
                out.resyncs += 1;
                continue;
            }
            out.frames.push(self.buffer[..position].to_vec());
            self.buffer.drain(..position + delimiter.len());
        }
        out
    }

    fn drain_start_end(&mut self, start: &[u8], end: &[u8], max: usize) -> FramerOutput {
        let mut out = FramerOutput::default();
        loop {
            let Some(start_pos) = find_subslice(&self.buffer, start) else {
                let keep = start.len().saturating_sub(1);
                if self.buffer.len() > keep {
                    self.buffer.drain(..self.buffer.len() - keep);
                }
                break;
            };
            if start_pos > 0 {
                self.buffer.drain(..start_pos);
                out.resyncs += 1;
            }
            let search_from = start.len();
            let Some(relative_end) = find_subslice(&self.buffer[search_from..], end) else {
                if self.buffer.len() > max + start.len() {
                    self.buffer.drain(..start.len());
                    out.oversize_frames += 1;
                    out.resyncs += 1;
                    continue;
                }
                break;
            };
            let end_pos = search_from + relative_end;
            if end_pos - start.len() > max {
                self.buffer.drain(..start.len());
                out.oversize_frames += 1;
                out.resyncs += 1;
                continue;
            }
            out.frames.push(self.buffer[start.len()..end_pos].to_vec());
            self.buffer.drain(..end_pos + end.len());
        }
        out
    }

    fn drain_fixed(&mut self, length: usize) -> FramerOutput {
        let mut out = FramerOutput::default();
        while self.buffer.len() >= length {
            out.frames.push(self.buffer.drain(..length).collect());
        }
        out
    }

    fn drain_length(
        &mut self,
        sync: &[u8],
        offset: usize,
        width: usize,
        order: ByteOrder,
        adjustment: i32,
        max: usize,
    ) -> FramerOutput {
        let mut out = FramerOutput::default();
        loop {
            let Some(sync_pos) = find_subslice(&self.buffer, sync) else {
                let keep = sync.len().saturating_sub(1);
                if self.buffer.len() > keep {
                    self.buffer.drain(..self.buffer.len() - keep);
                }
                break;
            };
            if sync_pos > 0 {
                self.buffer.drain(..sync_pos);
                out.resyncs += 1;
            }
            if self.buffer.len() < offset + width {
                break;
            }
            let declared = read_length(&self.buffer[offset..offset + width], order) as i64;
            let total = declared + adjustment as i64;
            if total <= 0 || total as usize > max || (total as usize) < offset + width {
                self.buffer.drain(..1);
                out.oversize_frames += u64::from(total as usize > max);
                out.resyncs += 1;
                continue;
            }
            let total = total as usize;
            if self.buffer.len() < total {
                break;
            }
            out.frames.push(self.buffer.drain(..total).collect());
        }
        out
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn read_length(bytes: &[u8], order: ByteOrder) -> u32 {
    match order {
        ByteOrder::Little => bytes.iter().enumerate().fold(0u32, |acc, (index, byte)| {
            acc | ((*byte as u32) << (index * 8))
        }),
        ByteOrder::Big => bytes
            .iter()
            .fold(0u32, |acc, byte| (acc << 8) | *byte as u32),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn delimiter_handles_fragmented_and_multiple_frames() {
        let mut framer = Framer::new(FramerSpec::EndDelimiter {
            id: Uuid::now_v7(),
            delimiter: vec![b'\n'],
            max_frame_bytes: 16,
        })
        .unwrap();
        assert!(framer.push(b"1,2").frames.is_empty());
        let out = framer.push(b",3\n4,5,6\n");
        assert_eq!(out.frames, vec![b"1,2,3".to_vec(), b"4,5,6".to_vec()]);
    }

    #[test]
    fn oversize_frame_resynchronizes_at_delimiter() {
        let mut framer = Framer::new(FramerSpec::EndDelimiter {
            id: Uuid::now_v7(),
            delimiter: vec![b'\n'],
            max_frame_bytes: 4,
        })
        .unwrap();
        let first = framer.push(b"123456");
        assert_eq!(first.oversize_frames, 1);
        let second = framer.push(b"\nok\n");
        assert_eq!(second.frames, vec![b"ok".to_vec()]);
        assert_eq!(second.resyncs, 1);
    }
}
