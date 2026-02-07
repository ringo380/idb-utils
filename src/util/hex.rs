//! Hex dump formatting utilities.
//!
//! Helpers for formatting byte offsets, hex values, and producing traditional
//! hex dump output with offset columns and ASCII sidebars.

/// Format a byte offset as "decimal (0xhex)".
pub fn format_offset(offset: u64) -> String {
    format!("{} (0x{:x})", offset, offset)
}

/// Format a u32 value as hex with 0x prefix.
pub fn format_hex32(value: u32) -> String {
    format!("0x{:08x}", value)
}

/// Format a u64 value as hex with 0x prefix.
pub fn format_hex64(value: u64) -> String {
    format!("0x{:016x}", value)
}

/// Format bytes as a compact hex string (e.g., "4a2f00ff").
pub fn format_bytes(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Produce a standard hex dump of `data` with the given `base_offset`.
///
/// Output format (16 bytes per line):
/// ```text
/// 00000000  xx xx xx xx xx xx xx xx  xx xx xx xx xx xx xx xx  |................|
/// ```
pub fn hex_dump(data: &[u8], base_offset: u64) -> String {
    let mut lines = Vec::new();

    for (i, chunk) in data.chunks(16).enumerate() {
        let offset = base_offset + (i * 16) as u64;

        // Offset column
        let mut line = format!("{:08x}  ", offset);

        // Hex columns (two groups of 8 bytes separated by extra space)
        for (j, byte) in chunk.iter().enumerate() {
            if j == 8 {
                line.push(' ');
            }
            line.push_str(&format!("{:02x} ", byte));
        }

        // Pad short last line
        if chunk.len() < 16 {
            let missing = 16 - chunk.len();
            for j in 0..missing {
                if chunk.len() + j == 8 {
                    line.push(' ');
                }
                line.push_str("   ");
            }
        }

        // ASCII column
        line.push(' ');
        line.push('|');
        for byte in chunk {
            if byte.is_ascii_graphic() || *byte == b' ' {
                line.push(*byte as char);
            } else {
                line.push('.');
            }
        }
        // Pad ASCII column for short last line
        for _ in chunk.len()..16 {
            line.push(' ');
        }
        line.push('|');

        lines.push(line);
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(&[0x4a, 0x2f, 0x00, 0xff]), "4a2f00ff");
        assert_eq!(format_bytes(&[]), "");
        assert_eq!(format_bytes(&[0x00]), "00");
    }

    #[test]
    fn test_hex_dump_full_line() {
        let data: Vec<u8> = (0..16).collect();
        let output = hex_dump(&data, 0);
        assert!(output.starts_with("00000000  "));
        assert!(output.contains("00 01 02 03 04 05 06 07  08 09 0a 0b 0c 0d 0e 0f"));
        assert!(output.contains('|'));
    }

    #[test]
    fn test_hex_dump_partial_line() {
        let data = vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]; // "Hello"
        let output = hex_dump(&data, 0x100);
        assert!(output.starts_with("00000100  "));
        assert!(output.contains("48 65 6c 6c 6f"));
        assert!(output.contains("|Hello"));
    }

    #[test]
    fn test_hex_dump_nonprintable() {
        let data = vec![0x00, 0x01, 0x7f, 0x80, 0xff];
        let output = hex_dump(&data, 0);
        assert!(output.contains("|....."));
    }
}
