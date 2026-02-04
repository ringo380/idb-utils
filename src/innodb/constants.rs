/// InnoDB page and file structure constants.
///
/// These values are derived from the MySQL/InnoDB source code headers:
/// - fil0fil.h (FIL header/trailer)
/// - page0page.h (page header)
/// - fsp0fsp.h (FSP header)
// Page sizes
pub const SIZE_PAGE_DEFAULT: u32 = 16384;
pub const SIZE_PAGE_4K: u32 = 4096;
pub const SIZE_PAGE_8K: u32 = 8192;
pub const SIZE_PAGE_16K: u32 = 16384;
pub const SIZE_PAGE_32K: u32 = 32768;
pub const SIZE_PAGE_64K: u32 = 65536;

// FIL Header (38 bytes total)
pub const SIZE_FIL_HEAD: usize = 38;
pub const FIL_PAGE_SPACE_OR_CHKSUM: usize = 0; // 4 bytes - checksum or space id
pub const FIL_PAGE_OFFSET: usize = 4; // 4 bytes - page number
pub const FIL_PAGE_PREV: usize = 8; // 4 bytes - previous page
pub const FIL_PAGE_NEXT: usize = 12; // 4 bytes - next page
pub const FIL_PAGE_LSN: usize = 16; // 8 bytes - LSN of newest modification
pub const FIL_PAGE_TYPE: usize = 24; // 2 bytes - page type
pub const FIL_PAGE_FILE_FLUSH_LSN: usize = 26; // 8 bytes - flush LSN (only page 0 of system tablespace)
pub const FIL_PAGE_SPACE_ID: usize = 34; // 4 bytes - space id

// FIL Trailer (8 bytes total)
pub const SIZE_FIL_TRAILER: usize = 8;
// Trailer is at: page_size - SIZE_FIL_TRAILER
// old-style checksum: offset 0 within trailer (4 bytes)
// low 32 bits of LSN: offset 4 within trailer (4 bytes)

// Start of page data (immediately after FIL header)
pub const FIL_PAGE_DATA: usize = 38;

// FSP Header (112 bytes, starts at FIL_PAGE_DATA on page 0)
pub const FSP_HEADER_SIZE: usize = 112;
pub const FSP_SPACE_ID: usize = 0; // 4 bytes - space id
pub const FSP_NOT_USED: usize = 4; // 4 bytes - unused
pub const FSP_SIZE: usize = 8; // 4 bytes - tablespace size in pages
pub const FSP_FREE_LIMIT: usize = 12; // 4 bytes - minimum page not yet initialized
pub const FSP_SPACE_FLAGS: usize = 16; // 4 bytes - flags
pub const FSP_FRAG_N_USED: usize = 20; // 4 bytes - number of used pages in FSP_FREE_FRAG list

// FSP flags bit positions for page size detection
pub const FSP_FLAGS_POS_PAGE_SSIZE: u32 = 6; // bit position of page size
pub const FSP_FLAGS_MASK_PAGE_SSIZE: u32 = 0xF << FSP_FLAGS_POS_PAGE_SSIZE; // 4 bits

// Page Header (INDEX page specific, starts at FIL_PAGE_DATA = offset 38)
pub const PAGE_N_DIR_SLOTS: usize = 0; // 2 bytes - number of directory slots
pub const PAGE_HEAP_TOP: usize = 2; // 2 bytes - pointer to record heap top
pub const PAGE_N_HEAP: usize = 4; // 2 bytes - number of records in heap (bit 15 = compact flag)
pub const PAGE_FREE: usize = 6; // 2 bytes - pointer to start of free record list
pub const PAGE_GARBAGE: usize = 8; // 2 bytes - bytes in deleted records
pub const PAGE_LAST_INSERT: usize = 10; // 2 bytes - pointer to last inserted record
pub const PAGE_DIRECTION: usize = 12; // 2 bytes - last insert direction
pub const PAGE_N_DIRECTION: usize = 14; // 2 bytes - consecutive inserts in same direction
pub const PAGE_N_RECS: usize = 16; // 2 bytes - number of user records
pub const PAGE_MAX_TRX_ID: usize = 18; // 8 bytes - max trx id (secondary indexes only)
pub const PAGE_LEVEL: usize = 26; // 2 bytes - level in B+tree (0 = leaf)
pub const PAGE_INDEX_ID: usize = 28; // 8 bytes - index id
pub const PAGE_BTR_SEG_LEAF: usize = 36; // 10 bytes - leaf segment header
pub const PAGE_BTR_SEG_TOP: usize = 46; // 10 bytes - non-leaf segment header
pub const PAGE_HEADER_SIZE: usize = 56; // total index page header size (before FSEG headers)

// FSEG Header size
pub const FSEG_HEADER_SIZE: usize = 10;

// Record extra bytes
pub const REC_N_OLD_EXTRA_BYTES: usize = 6;
pub const REC_N_NEW_EXTRA_BYTES: usize = 5;

// System record offsets (for new-style compact pages)
pub const PAGE_DATA: usize = FIL_PAGE_DATA + PAGE_HEADER_SIZE + 2 * FSEG_HEADER_SIZE; // = 38 + 56 + 20 = 114... but Perl uses 94
// Note: the Perl code uses PAGE_DATA = PAGE_HEADER(38) + 36 + 2*FSEG_HEADER_SIZE(10) = 94
// This matches the MySQL source: PAGE_DATA = PAGE_HEADER + PAGE_HEADER_PRIV_END(26) + FLST_BASE_NODE_SIZE(16)*?
// Actually from MySQL source: PAGE_DATA = PAGE_HEADER + 36 + 2 * FSEG_HEADER_SIZE = 38 + 36 + 20 = 94
pub const PAGE_DATA_OFFSET: usize = 94;

pub const PAGE_NEW_INFIMUM: usize = PAGE_DATA_OFFSET + REC_N_NEW_EXTRA_BYTES; // 99
pub const PAGE_NEW_SUPREMUM: usize = PAGE_DATA_OFFSET + 2 * REC_N_NEW_EXTRA_BYTES + 8; // 112 (Perl: 109, but that uses different calc)
pub const PAGE_OLD_INFIMUM: usize = PAGE_DATA_OFFSET + 1 + REC_N_OLD_EXTRA_BYTES; // 101
pub const PAGE_OLD_SUPREMUM: usize = PAGE_DATA_OFFSET + 2 + 2 * REC_N_OLD_EXTRA_BYTES + 8; // 112 (Perl: 112)

// Special page number values
pub const FIL_NULL: u32 = 0xFFFFFFFF; // "null" page reference (4294967295)

// Checksum constants
pub const UT_HASH_RANDOM_MASK: u32 = 1463735687;
pub const UT_HASH_RANDOM_MASK2: u32 = 1653893711;

// Insert direction values
pub const PAGE_LEFT: u16 = 1;
pub const PAGE_RIGHT: u16 = 2;
pub const PAGE_SAME_REC: u16 = 3;
pub const PAGE_SAME_PAGE: u16 = 4;
pub const PAGE_NO_DIRECTION: u16 = 5;
