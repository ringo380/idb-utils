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
