use crate::core::error::{ErrorCode, ToolboxError, ToolboxResult};
use crate::core::model::{ByteOrder, ChecksumAlgorithm, ChecksumSpec};

pub fn validate_checksum(frame: &[u8], spec: &ChecksumSpec) -> ToolboxResult<()> {
    if spec.start_offset > spec.end_offset_exclusive
        || spec.end_offset_exclusive > frame.len()
        || spec.stored_offset + spec.stored_width > frame.len()
    {
        return Err(ToolboxError::new(
            ErrorCode::ProjectSchemaInvalid,
            "checksum.validate",
            "checksum_range_invalid",
        ));
    }
    let data = &frame[spec.start_offset..spec.end_offset_exclusive];
    let calculated = calculate(spec.algorithm, data);
    let stored = read_unsigned(
        &frame[spec.stored_offset..spec.stored_offset + spec.stored_width],
        spec.byte_order,
    );
    let mask = match spec.stored_width {
        1 => 0xff,
        2 => 0xffff,
        4 => 0xffff_ffff,
        _ => {
            return Err(ToolboxError::new(
                ErrorCode::ProjectSchemaInvalid,
                "checksum.validate",
                "checksum_width_invalid",
            ));
        }
    };
    if calculated & mask == stored {
        Ok(())
    } else {
        Err(ToolboxError::new(
            ErrorCode::ChecksumMismatch,
            "checksum.validate",
            "checksum_mismatch",
        )
        .context("expected", stored)
        .context("actual", calculated & mask))
    }
}

pub fn calculate(algorithm: ChecksumAlgorithm, data: &[u8]) -> u64 {
    match algorithm {
        ChecksumAlgorithm::Xor8 => data.iter().fold(0u8, |acc, value| acc ^ value) as u64,
        ChecksumAlgorithm::Sum8 => {
            data.iter().fold(0u8, |acc, value| acc.wrapping_add(*value)) as u64
        }
        ChecksumAlgorithm::Crc8 => crc8(data) as u64,
        ChecksumAlgorithm::Crc16Modbus => crc16_modbus(data) as u64,
        ChecksumAlgorithm::Crc16Ccitt => crc16_ccitt(data) as u64,
        ChecksumAlgorithm::Crc32 => crc32(data) as u64,
    }
}

fn read_unsigned(bytes: &[u8], order: ByteOrder) -> u64 {
    match order {
        ByteOrder::Little => bytes.iter().enumerate().fold(0u64, |acc, (index, byte)| {
            acc | ((*byte as u64) << (index * 8))
        }),
        ByteOrder::Big => bytes
            .iter()
            .fold(0u64, |acc, byte| (acc << 8) | *byte as u64),
    }
}

fn crc8(data: &[u8]) -> u8 {
    let mut crc = 0u8;
    for byte in data {
        crc ^= *byte;
        for _ in 0..8 {
            crc = if crc & 0x80 != 0 {
                (crc << 1) ^ 0x07
            } else {
                crc << 1
            };
        }
    }
    crc
}

fn crc16_modbus(data: &[u8]) -> u16 {
    let mut crc = 0xffffu16;
    for byte in data {
        crc ^= *byte as u16;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xa001
            } else {
                crc >> 1
            };
        }
    }
    crc
}

fn crc16_ccitt(data: &[u8]) -> u16 {
    let mut crc = 0xffffu16;
    for byte in data {
        crc ^= (*byte as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in data {
        crc ^= *byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xedb8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_crc_vectors_match() {
        let bytes = b"123456789";
        assert_eq!(calculate(ChecksumAlgorithm::Crc8, bytes), 0xf4);
        assert_eq!(calculate(ChecksumAlgorithm::Crc16Modbus, bytes), 0x4b37);
        assert_eq!(calculate(ChecksumAlgorithm::Crc16Ccitt, bytes), 0x29b1);
        assert_eq!(calculate(ChecksumAlgorithm::Crc32, bytes), 0xcbf4_3926);
    }
}
