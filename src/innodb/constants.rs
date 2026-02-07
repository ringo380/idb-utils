//! InnoDB page and file structure constants.
//!
//! These values are derived from the MySQL/InnoDB source code headers:
//! - `fil0fil.h` (FIL header/trailer)
//! - `page0page.h` (page header)
//! - `fsp0fsp.h` (FSP header)

// ── Page sizes ──────────────────────────────────────────────────────

/// Default InnoDB page size (16 KiB).
pub const SIZE_PAGE_DEFAULT: u32 = 16384;
/// 4 KiB page size.
pub const SIZE_PAGE_4K: u32 = 4096;
/// 8 KiB page size.
pub const SIZE_PAGE_8K: u32 = 8192;
/// 16 KiB page size.
pub const SIZE_PAGE_16K: u32 = 16384;
/// 32 KiB page size.
pub const SIZE_PAGE_32K: u32 = 32768;
/// 64 KiB page size.
pub const SIZE_PAGE_64K: u32 = 65536;

// ── FIL Header (38 bytes total) ─────────────────────────────────────

/// Size of the FIL header in bytes.
pub const SIZE_FIL_HEAD: usize = 38;
/// Offset of the checksum (or space id in older formats). 4 bytes.
pub const FIL_PAGE_SPACE_OR_CHKSUM: usize = 0;
/// Offset of the page number within the tablespace. 4 bytes.
pub const FIL_PAGE_OFFSET: usize = 4;
/// Offset of the previous page pointer. 4 bytes.
pub const FIL_PAGE_PREV: usize = 8;
/// Offset of the next page pointer. 4 bytes.
pub const FIL_PAGE_NEXT: usize = 12;
/// Offset of the LSN of newest modification. 8 bytes.
pub const FIL_PAGE_LSN: usize = 16;
/// Offset of the page type field. 2 bytes.
pub const FIL_PAGE_TYPE: usize = 24;
/// Offset of the flush LSN (only page 0 of system tablespace). 8 bytes.
pub const FIL_PAGE_FILE_FLUSH_LSN: usize = 26;
/// Offset of the space ID. 4 bytes.
pub const FIL_PAGE_SPACE_ID: usize = 34;

// ── FIL Trailer (8 bytes total) ─────────────────────────────────────

/// Size of the FIL trailer in bytes.
pub const SIZE_FIL_TRAILER: usize = 8;
// Trailer is at: page_size - SIZE_FIL_TRAILER
// old-style checksum: offset 0 within trailer (4 bytes)
// low 32 bits of LSN: offset 4 within trailer (4 bytes)

/// Start of page data (immediately after FIL header).
pub const FIL_PAGE_DATA: usize = 38;

// ── FSP Header (112 bytes, starts at FIL_PAGE_DATA on page 0) ──────

/// Size of the FSP header in bytes.
pub const FSP_HEADER_SIZE: usize = 112;
/// Offset of the space ID within the FSP header. 4 bytes.
pub const FSP_SPACE_ID: usize = 0;
/// Unused field in the FSP header. 4 bytes.
pub const FSP_NOT_USED: usize = 4;
/// Offset of the tablespace size (in pages) within the FSP header. 4 bytes.
pub const FSP_SIZE: usize = 8;
/// Offset of the minimum page not yet initialized. 4 bytes.
pub const FSP_FREE_LIMIT: usize = 12;
/// Offset of the FSP flags field. 4 bytes.
pub const FSP_SPACE_FLAGS: usize = 16;
/// Offset of the used-page count in the FSP_FREE_FRAG list. 4 bytes.
pub const FSP_FRAG_N_USED: usize = 20;

// ── FSP flags bit positions for page size detection ─────────────────

/// Bit position of the page size field within FSP flags.
pub const FSP_FLAGS_POS_PAGE_SSIZE: u32 = 6;
/// Bitmask for the 4-bit page size field within FSP flags.
pub const FSP_FLAGS_MASK_PAGE_SSIZE: u32 = 0xF << FSP_FLAGS_POS_PAGE_SSIZE;

// ── Page Header (INDEX page specific, starts at FIL_PAGE_DATA) ──────

/// Offset of the directory slot count. 2 bytes.
pub const PAGE_N_DIR_SLOTS: usize = 0;
/// Offset of the record heap top pointer. 2 bytes.
pub const PAGE_HEAP_TOP: usize = 2;
/// Offset of the heap record count (bit 15 = compact flag). 2 bytes.
pub const PAGE_N_HEAP: usize = 4;
/// Offset of the free record list pointer. 2 bytes.
pub const PAGE_FREE: usize = 6;
/// Offset of the deleted-record byte count (garbage). 2 bytes.
pub const PAGE_GARBAGE: usize = 8;
/// Offset of the last-inserted record pointer. 2 bytes.
pub const PAGE_LAST_INSERT: usize = 10;
/// Offset of the last insert direction. 2 bytes.
pub const PAGE_DIRECTION: usize = 12;
/// Offset of the consecutive same-direction insert count. 2 bytes.
pub const PAGE_N_DIRECTION: usize = 14;
/// Offset of the user record count. 2 bytes.
pub const PAGE_N_RECS: usize = 16;
/// Offset of the maximum transaction ID (secondary indexes only). 8 bytes.
pub const PAGE_MAX_TRX_ID: usize = 18;
/// Offset of the B+Tree level (0 = leaf). 2 bytes.
pub const PAGE_LEVEL: usize = 26;
/// Offset of the index ID. 8 bytes.
pub const PAGE_INDEX_ID: usize = 28;
/// Offset of the leaf segment FSEG header. 10 bytes.
pub const PAGE_BTR_SEG_LEAF: usize = 36;
/// Offset of the non-leaf segment FSEG header. 10 bytes.
pub const PAGE_BTR_SEG_TOP: usize = 46;
/// Total INDEX page header size (before FSEG headers).
pub const PAGE_HEADER_SIZE: usize = 56;

// ── FSEG Header ─────────────────────────────────────────────────────

/// Size of an FSEG (file segment) header in bytes.
pub const FSEG_HEADER_SIZE: usize = 10;

// ── Record extra bytes ──────────────────────────────────────────────

/// Extra bytes preceding each record in old-style (redundant) format.
pub const REC_N_OLD_EXTRA_BYTES: usize = 6;
/// Extra bytes preceding each record in new-style (compact) format.
pub const REC_N_NEW_EXTRA_BYTES: usize = 5;

// ── System record offsets (compact pages) ───────────────────────────

/// Computed data offset (FIL_PAGE_DATA + PAGE_HEADER + 2 * FSEG_HEADER).
pub const PAGE_DATA: usize = FIL_PAGE_DATA + PAGE_HEADER_SIZE + 2 * FSEG_HEADER_SIZE;
// Note: the MySQL source uses PAGE_DATA = 38 + 36 + 20 = 94
/// Actual system record data offset matching MySQL source.
pub const PAGE_DATA_OFFSET: usize = 94;

/// Offset of the infimum record (compact format).
pub const PAGE_NEW_INFIMUM: usize = PAGE_DATA_OFFSET + REC_N_NEW_EXTRA_BYTES; // 99
/// Offset of the supremum record (compact format).
pub const PAGE_NEW_SUPREMUM: usize = PAGE_DATA_OFFSET + 2 * REC_N_NEW_EXTRA_BYTES + 8; // 112
/// Offset of the infimum record (redundant format).
pub const PAGE_OLD_INFIMUM: usize = PAGE_DATA_OFFSET + 1 + REC_N_OLD_EXTRA_BYTES; // 101
/// Offset of the supremum record (redundant format).
pub const PAGE_OLD_SUPREMUM: usize = PAGE_DATA_OFFSET + 2 + 2 * REC_N_OLD_EXTRA_BYTES + 8; // 112

// ── Special values ──────────────────────────────────────────────────

/// Null page reference (0xFFFFFFFF / 4294967295).
pub const FIL_NULL: u32 = 0xFFFFFFFF;

// ── Checksum constants ──────────────────────────────────────────────

/// First random mask used by `ut_fold_ulint_pair` in legacy InnoDB checksums.
pub const UT_HASH_RANDOM_MASK: u32 = 1463735687;
/// Second random mask used by `ut_fold_ulint_pair` in legacy InnoDB checksums.
pub const UT_HASH_RANDOM_MASK2: u32 = 1653893711;

// ── Insert direction values ─────────────────────────────────────────

/// Insert direction: left.
pub const PAGE_LEFT: u16 = 1;
/// Insert direction: right.
pub const PAGE_RIGHT: u16 = 2;
/// Insert direction: same record position.
pub const PAGE_SAME_REC: u16 = 3;
/// Insert direction: same page.
pub const PAGE_SAME_PAGE: u16 = 4;
/// Insert direction: no direction.
pub const PAGE_NO_DIRECTION: u16 = 5;
