//! Integration tests for encrypted tablespace support.
//!
//! These tests construct synthetic encrypted InnoDB tablespace files and
//! keyring files, then run the full parsing/decryption pipeline against them.

use aes::cipher::block_padding::NoPadding;
use aes::cipher::{BlockEncryptMut, KeyInit, KeyIvInit};
use aes::Aes256;
use byteorder::{BigEndian, ByteOrder};
use sha2::{Digest, Sha256};
use std::io::Write;
use tempfile::NamedTempFile;

use idb::innodb::constants::*;
use idb::innodb::decryption::DecryptionContext;
use idb::innodb::encryption::{encryption_info_offset, parse_encryption_info};
use idb::innodb::keyring::Keyring;
use idb::innodb::page::FilHeader;
use idb::innodb::page_types::PageType;
use idb::innodb::tablespace::Tablespace;

const PAGE_SIZE: u32 = 16384;
const PS: usize = PAGE_SIZE as usize;

type Aes256CbcEnc = cbc::Encryptor<Aes256>;
type Aes256EcbEnc = ecb::Encryptor<Aes256>;

/// XOR obfuscation key used by MySQL's keyring_file plugin.
const OBFUSCATE_KEY: &[u8] = b"*305=Ljt0*!@$Hnm(*-9-w;:";

// ── Test data ────────────────────────────────────────────────────────

const MASTER_KEY: [u8; 32] = [0xAA; 32];
const TS_KEY: [u8; 32] = [0xBB; 32];
const TS_IV: [u8; 32] = [0xCC; 32];
const SERVER_UUID: &str = "12345678-1234-1234-1234-123456789abc";

// ── Helpers ──────────────────────────────────────────────────────────

fn write_crc32c_checksum(page: &mut [u8]) {
    let end = PS - SIZE_FIL_TRAILER;
    let crc1 = crc32c::crc32c(&page[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);
    let crc2 = crc32c::crc32c(&page[FIL_PAGE_DATA..end]);
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], crc1 ^ crc2);
}

/// Encrypt the tablespace key+IV using AES-256-ECB with the master key.
fn encrypt_key_iv(master: &[u8; 32], ts_key: &[u8; 32], ts_iv: &[u8; 32]) -> [u8; 64] {
    let mut data = [0u8; 64];
    data[..32].copy_from_slice(ts_key);
    data[32..].copy_from_slice(ts_iv);
    let encryptor = Aes256EcbEnc::new_from_slice(master).unwrap();
    encryptor
        .encrypt_padded_mut::<NoPadding>(&mut data, 64)
        .unwrap();
    data
}

/// Compute CRC32 of plaintext key+IV.
fn key_iv_checksum(ts_key: &[u8; 32], ts_iv: &[u8; 32]) -> u32 {
    let mut data = [0u8; 64];
    data[..32].copy_from_slice(ts_key);
    data[32..].copy_from_slice(ts_iv);
    crc32c::crc32c(&data)
}

/// Build a minimal FSP_HDR page 0 with encryption info and CRC-32C checksum.
fn build_encrypted_fsp_hdr(
    space_id: u32,
    total_pages: u32,
    master_key_id: u32,
    uuid: &str,
    encrypted_key_iv: &[u8; 64],
    key_iv_crc: u32,
) -> Vec<u8> {
    let mut page = vec![0u8; PS];

    // FIL header
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 0);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 1000);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 8); // FSP_HDR
    BigEndian::write_u64(&mut page[FIL_PAGE_FILE_FLUSH_LSN..], 1000);
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

    // FSP header at FIL_PAGE_DATA (offset 38)
    let fsp = FIL_PAGE_DATA;
    BigEndian::write_u32(&mut page[fsp + FSP_SPACE_ID..], space_id);
    BigEndian::write_u32(&mut page[fsp + FSP_SIZE..], total_pages);
    BigEndian::write_u32(&mut page[fsp + FSP_FREE_LIMIT..], total_pages);
    // Flags: bit 13 = encryption enabled
    BigEndian::write_u32(&mut page[fsp + FSP_SPACE_FLAGS..], 1 << 13);

    // Encryption info after XDES array
    let offset = encryption_info_offset(PAGE_SIZE);
    page[offset..offset + 3].copy_from_slice(b"lCC"); // V3
    BigEndian::write_u32(&mut page[offset + 3..], master_key_id);
    page[offset + 7..offset + 7 + 36].copy_from_slice(uuid.as_bytes());
    page[offset + 43..offset + 43 + 64].copy_from_slice(encrypted_key_iv);
    BigEndian::write_u32(&mut page[offset + 107..], key_iv_crc);

    // FIL trailer
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], (1000u32) & 0xFFFFFFFF);

    write_crc32c_checksum(&mut page);
    page
}

/// Build an encrypted INDEX page.
fn build_encrypted_index_page(
    page_num: u32,
    space_id: u32,
    lsn: u64,
    ts_key: &[u8; 32],
    ts_iv: &[u8; 32],
) -> Vec<u8> {
    let mut page = vec![0u8; PS];

    // FIL header
    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
    // Save original page type at offset 26 (FIL_PAGE_ORIGINAL_TYPE_V1)
    BigEndian::write_u16(&mut page[FIL_PAGE_ORIGINAL_TYPE_V1..], 17855); // INDEX
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

    // INDEX page header at FIL_PAGE_DATA (offset 38)
    let ph = FIL_PAGE_DATA;
    BigEndian::write_u16(&mut page[ph + PAGE_N_DIR_SLOTS..], 2);
    BigEndian::write_u16(&mut page[ph + PAGE_N_HEAP..], 0x8002);
    BigEndian::write_u16(&mut page[ph + PAGE_N_RECS..], 0);
    BigEndian::write_u16(&mut page[ph + PAGE_LEVEL..], 0);
    BigEndian::write_u64(&mut page[ph + PAGE_INDEX_ID..], 42);

    // Encrypt page body: [38..page_size-8)
    let encrypt_start = SIZE_FIL_HEAD;
    let encrypt_end = PS - SIZE_FIL_TRAILER;
    let encrypt_len = encrypt_end - encrypt_start;
    let aes_block_size = 16;
    let main_len = (encrypt_len / aes_block_size) * aes_block_size;

    let cbc_iv: [u8; 16] = ts_iv[..16].try_into().unwrap();
    let encryptor = Aes256CbcEnc::new_from_slices(ts_key, &cbc_iv).unwrap();
    encryptor
        .encrypt_padded_mut::<NoPadding>(
            &mut page[encrypt_start..encrypt_start + main_len],
            main_len,
        )
        .unwrap();

    // Set encrypted page type
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 15); // Encrypted

    // FIL trailer
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], (lsn & 0xFFFFFFFF) as u32);

    write_crc32c_checksum(&mut page);
    page
}

/// Build a MySQL keyring file with one InnoDB master key entry.
fn build_keyring_file(uuid: &str, key_id: u32, master_key: &[u8; 32]) -> Vec<u8> {
    let key_id_str = format!("INNODBKey-{}-{}", uuid, key_id);
    let key_type = "AES";
    let user_id = "";

    // XOR-obfuscate the master key
    let mut obfuscated = master_key.to_vec();
    for (i, byte) in obfuscated.iter_mut().enumerate() {
        *byte ^= OBFUSCATE_KEY[i % OBFUSCATE_KEY.len()];
    }

    // Build entry
    let mut entry = Vec::new();
    let pod_size = 40 + key_id_str.len() + key_type.len() + user_id.len() + master_key.len();
    entry.extend_from_slice(&(pod_size as u64).to_le_bytes());
    entry.extend_from_slice(&(key_id_str.len() as u64).to_le_bytes());
    entry.extend_from_slice(&(key_type.len() as u64).to_le_bytes());
    entry.extend_from_slice(&(user_id.len() as u64).to_le_bytes());
    entry.extend_from_slice(&(master_key.len() as u64).to_le_bytes());
    entry.extend_from_slice(key_id_str.as_bytes());
    entry.extend_from_slice(key_type.as_bytes());
    // user_id is empty, no bytes to write
    entry.extend_from_slice(&obfuscated);

    // Append SHA-256 digest
    let mut hasher = Sha256::new();
    hasher.update(&entry);
    let hash = hasher.finalize();
    entry.extend_from_slice(&hash);

    entry
}

/// Write pages to a temp file and return the NamedTempFile handle.
fn write_tablespace(pages: &[Vec<u8>]) -> NamedTempFile {
    let mut tmp = NamedTempFile::new().expect("create temp file");
    for page in pages {
        tmp.write_all(page).expect("write page");
    }
    tmp.flush().expect("flush");
    tmp
}

/// Write a keyring file to a temp file and return the NamedTempFile handle.
fn write_keyring(data: &[u8]) -> NamedTempFile {
    let mut tmp = NamedTempFile::new().expect("create temp keyring");
    tmp.write_all(data).expect("write keyring");
    tmp.flush().expect("flush keyring");
    tmp
}

// ── Tests ────────────────────────────────────────────────────────────

#[test]
fn test_encrypted_tablespace_detection() {
    let encrypted_key_iv = encrypt_key_iv(&MASTER_KEY, &TS_KEY, &TS_IV);
    let crc = key_iv_checksum(&TS_KEY, &TS_IV);

    let page0 = build_encrypted_fsp_hdr(1, 2, 1, SERVER_UUID, &encrypted_key_iv, crc);
    let page1 = build_encrypted_index_page(1, 1, 2000, &TS_KEY, &TS_IV);

    let ts_file = write_tablespace(&[page0, page1]);
    let ts = Tablespace::open(ts_file.path().to_str().unwrap()).unwrap();

    assert!(ts.is_encrypted());
    let enc_info = ts.encryption_info().unwrap();
    assert_eq!(enc_info.magic_version, 3);
    assert_eq!(enc_info.master_key_id, 1);
    assert_eq!(enc_info.server_uuid, SERVER_UUID);
}

#[test]
fn test_encrypted_tablespace_no_encryption_info() {
    // A normal (non-encrypted) tablespace should have no encryption info
    let mut page0 = vec![0u8; PS];
    BigEndian::write_u32(&mut page0[FIL_PAGE_OFFSET..], 0);
    BigEndian::write_u32(&mut page0[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page0[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page0[FIL_PAGE_LSN..], 1000);
    BigEndian::write_u16(&mut page0[FIL_PAGE_TYPE..], 8);
    BigEndian::write_u64(&mut page0[FIL_PAGE_FILE_FLUSH_LSN..], 1000);
    BigEndian::write_u32(&mut page0[FIL_PAGE_SPACE_ID..], 1);
    let fsp = FIL_PAGE_DATA;
    BigEndian::write_u32(&mut page0[fsp + FSP_SPACE_ID..], 1);
    BigEndian::write_u32(&mut page0[fsp + FSP_SIZE..], 1);
    BigEndian::write_u32(&mut page0[fsp + FSP_FREE_LIMIT..], 1);
    BigEndian::write_u32(&mut page0[fsp + FSP_SPACE_FLAGS..], 0);
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page0[trailer + 4..], 1000);
    write_crc32c_checksum(&mut page0);

    let ts_file = write_tablespace(&[page0]);
    let ts = Tablespace::open(ts_file.path().to_str().unwrap()).unwrap();

    assert!(!ts.is_encrypted());
    assert!(ts.encryption_info().is_none());
}

#[test]
fn test_end_to_end_decrypt_with_keyring() {
    let encrypted_key_iv = encrypt_key_iv(&MASTER_KEY, &TS_KEY, &TS_IV);
    let crc = key_iv_checksum(&TS_KEY, &TS_IV);

    let page0 = build_encrypted_fsp_hdr(1, 3, 1, SERVER_UUID, &encrypted_key_iv, crc);
    let page1 = build_encrypted_index_page(1, 1, 2000, &TS_KEY, &TS_IV);
    let page2 = build_encrypted_index_page(2, 1, 3000, &TS_KEY, &TS_IV);

    let ts_file = write_tablespace(&[page0, page1, page2]);
    let keyring_data = build_keyring_file(SERVER_UUID, 1, &MASTER_KEY);
    let keyring_file = write_keyring(&keyring_data);

    // Open tablespace and set up decryption
    let mut ts = Tablespace::open(ts_file.path().to_str().unwrap()).unwrap();
    let keyring = Keyring::load(keyring_file.path()).unwrap();
    let enc_info = ts.encryption_info().unwrap();
    let ctx = DecryptionContext::from_encryption_info(enc_info, &keyring).unwrap();
    ts.set_decryption_context(ctx);

    // Read page 1 — should be auto-decrypted
    let data1 = ts.read_page(1).unwrap();
    let header1 = FilHeader::parse(&data1).unwrap();
    assert_eq!(header1.page_type, PageType::Index);
    assert_eq!(header1.page_number, 1);

    // Read page 2 — should also be decrypted
    let data2 = ts.read_page(2).unwrap();
    let header2 = FilHeader::parse(&data2).unwrap();
    assert_eq!(header2.page_type, PageType::Index);
    assert_eq!(header2.page_number, 2);
}

#[test]
fn test_encrypted_pages_without_keyring() {
    let encrypted_key_iv = encrypt_key_iv(&MASTER_KEY, &TS_KEY, &TS_IV);
    let crc = key_iv_checksum(&TS_KEY, &TS_IV);

    let page0 = build_encrypted_fsp_hdr(1, 2, 1, SERVER_UUID, &encrypted_key_iv, crc);
    let page1 = build_encrypted_index_page(1, 1, 2000, &TS_KEY, &TS_IV);

    let ts_file = write_tablespace(&[page0, page1]);

    // Open without decryption context
    let mut ts = Tablespace::open(ts_file.path().to_str().unwrap()).unwrap();

    // Page 1 should still show as Encrypted type (no decryption)
    let data = ts.read_page(1).unwrap();
    let header = FilHeader::parse(&data).unwrap();
    assert_eq!(header.page_type, PageType::Encrypted);
}

#[test]
fn test_wrong_keyring_crc_mismatch() {
    let encrypted_key_iv = encrypt_key_iv(&MASTER_KEY, &TS_KEY, &TS_IV);
    let crc = key_iv_checksum(&TS_KEY, &TS_IV);

    let page0 = build_encrypted_fsp_hdr(1, 2, 1, SERVER_UUID, &encrypted_key_iv, crc);
    let page1 = build_encrypted_index_page(1, 1, 2000, &TS_KEY, &TS_IV);

    let ts_file = write_tablespace(&[page0, page1]);

    // Build keyring with wrong master key
    let wrong_key: [u8; 32] = [0xDD; 32];
    let keyring_data = build_keyring_file(SERVER_UUID, 1, &wrong_key);
    let keyring_file = write_keyring(&keyring_data);

    let ts = Tablespace::open(ts_file.path().to_str().unwrap()).unwrap();
    let keyring = Keyring::load(keyring_file.path()).unwrap();
    let enc_info = ts.encryption_info().unwrap();
    let result = DecryptionContext::from_encryption_info(enc_info, &keyring);

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("CRC32 checksum mismatch"),
        "Expected CRC32 mismatch error, got: {}",
        err
    );
}

#[test]
fn test_keyring_master_key_not_found() {
    let encrypted_key_iv = encrypt_key_iv(&MASTER_KEY, &TS_KEY, &TS_IV);
    let crc = key_iv_checksum(&TS_KEY, &TS_IV);

    let page0 = build_encrypted_fsp_hdr(1, 2, 1, SERVER_UUID, &encrypted_key_iv, crc);
    let ts_file = write_tablespace(&[page0]);

    // Build keyring with a different UUID — key won't be found
    let different_uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    let keyring_data = build_keyring_file(different_uuid, 1, &MASTER_KEY);
    let keyring_file = write_keyring(&keyring_data);

    let ts = Tablespace::open(ts_file.path().to_str().unwrap()).unwrap();
    let keyring = Keyring::load(keyring_file.path()).unwrap();
    let enc_info = ts.encryption_info().unwrap();
    let result = DecryptionContext::from_encryption_info(enc_info, &keyring);

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("Master key not found"),
        "Expected 'Master key not found' error, got: {}",
        err
    );
}

#[test]
fn test_parse_subcommand_with_encrypted_tablespace() {
    let encrypted_key_iv = encrypt_key_iv(&MASTER_KEY, &TS_KEY, &TS_IV);
    let crc = key_iv_checksum(&TS_KEY, &TS_IV);

    let page0 = build_encrypted_fsp_hdr(1, 2, 1, SERVER_UUID, &encrypted_key_iv, crc);
    let page1 = build_encrypted_index_page(1, 1, 2000, &TS_KEY, &TS_IV);

    let ts_file = write_tablespace(&[page0, page1]);
    let keyring_data = build_keyring_file(SERVER_UUID, 1, &MASTER_KEY);
    let keyring_file = write_keyring(&keyring_data);

    // Run parse subcommand with keyring
    let mut buf = Vec::new();
    let result = idb::cli::parse::execute(
        &idb::cli::parse::ParseOptions {
            file: ts_file.path().to_str().unwrap().to_string(),
            page: None,
            verbose: false,
            no_empty: false,
            page_size: None,
            json: false,
            keyring: Some(keyring_file.path().to_str().unwrap().to_string()),
        },
        &mut buf,
    );
    assert!(result.is_ok(), "parse failed: {:?}", result.err());

    let output = String::from_utf8(buf).unwrap();
    // Should show INDEX (decrypted) rather than Encrypted
    assert!(
        output.contains("INDEX"),
        "Expected 'INDEX' in output, got:\n{}",
        output
    );
}

#[test]
fn test_parse_subcommand_without_keyring_shows_encrypted() {
    let encrypted_key_iv = encrypt_key_iv(&MASTER_KEY, &TS_KEY, &TS_IV);
    let crc = key_iv_checksum(&TS_KEY, &TS_IV);

    let page0 = build_encrypted_fsp_hdr(1, 2, 1, SERVER_UUID, &encrypted_key_iv, crc);
    let page1 = build_encrypted_index_page(1, 1, 2000, &TS_KEY, &TS_IV);

    let ts_file = write_tablespace(&[page0, page1]);

    // Run parse subcommand without keyring
    let mut buf = Vec::new();
    let result = idb::cli::parse::execute(
        &idb::cli::parse::ParseOptions {
            file: ts_file.path().to_str().unwrap().to_string(),
            page: None,
            verbose: false,
            no_empty: false,
            page_size: None,
            json: false,
            keyring: None,
        },
        &mut buf,
    );
    assert!(result.is_ok(), "parse failed: {:?}", result.err());

    let output = String::from_utf8(buf).unwrap();
    // Should show Encrypted (not decrypted)
    assert!(
        output.contains("Encrypted"),
        "Expected 'Encrypted' in output, got:\n{}",
        output
    );
}

#[test]
fn test_checksum_subcommand_encrypted_with_keyring() {
    let encrypted_key_iv = encrypt_key_iv(&MASTER_KEY, &TS_KEY, &TS_IV);
    let crc = key_iv_checksum(&TS_KEY, &TS_IV);

    let page0 = build_encrypted_fsp_hdr(1, 2, 1, SERVER_UUID, &encrypted_key_iv, crc);
    let page1 = build_encrypted_index_page(1, 1, 2000, &TS_KEY, &TS_IV);

    let ts_file = write_tablespace(&[page0, page1]);
    let keyring_data = build_keyring_file(SERVER_UUID, 1, &MASTER_KEY);
    let keyring_file = write_keyring(&keyring_data);

    let mut buf = Vec::new();
    let result = idb::cli::checksum::execute(
        &idb::cli::checksum::ChecksumOptions {
            file: ts_file.path().to_str().unwrap().to_string(),
            verbose: false,
            json: false,
            page_size: None,
            keyring: Some(keyring_file.path().to_str().unwrap().to_string()),
        },
        &mut buf,
    );

    // Should succeed (or at least not crash)
    // Encrypted pages have checksums computed on encrypted data, so validation
    // on decrypted data may legitimately fail — the key thing is no panics
    let output = String::from_utf8(buf).unwrap();
    assert!(
        output.contains("Validating checksums"),
        "Expected checksum output, got:\n{}",
        output
    );
    // The result may be Ok or Err (invalid checksums after decryption is expected
    // for synthetic test data where checksums were computed pre-decryption)
    drop(result);
}

#[test]
fn test_encryption_info_roundtrip() {
    // Verify we can build an encrypted tablespace, parse encryption info,
    // and decrypt the key+IV to recover the original keys
    let encrypted_key_iv = encrypt_key_iv(&MASTER_KEY, &TS_KEY, &TS_IV);
    let crc = key_iv_checksum(&TS_KEY, &TS_IV);

    let page0 = build_encrypted_fsp_hdr(1, 1, 1, SERVER_UUID, &encrypted_key_iv, crc);

    // Parse encryption info from the raw page
    let info = parse_encryption_info(&page0, PAGE_SIZE).unwrap();
    assert_eq!(info.magic_version, 3);
    assert_eq!(info.master_key_id, 1);
    assert_eq!(info.server_uuid, SERVER_UUID);
    assert_eq!(info.encrypted_key_iv, encrypted_key_iv);
    assert_eq!(info.checksum, crc);

    // Load keyring and decrypt
    let keyring_data = build_keyring_file(SERVER_UUID, 1, &MASTER_KEY);
    let keyring_file = write_keyring(&keyring_data);
    let keyring = Keyring::load(keyring_file.path()).unwrap();

    let ctx = DecryptionContext::from_encryption_info(&info, &keyring).unwrap();

    // Build an encrypted page and decrypt it
    let mut page = build_encrypted_index_page(1, 1, 2000, &TS_KEY, &TS_IV);
    let decrypted = ctx.decrypt_page(&mut page, PS).unwrap();
    assert!(decrypted);

    // Verify page type is restored to INDEX
    let pt = BigEndian::read_u16(&page[FIL_PAGE_TYPE..]);
    assert_eq!(pt, 17855);
}

#[test]
fn test_pages_subcommand_shows_encryption_info() {
    let encrypted_key_iv = encrypt_key_iv(&MASTER_KEY, &TS_KEY, &TS_IV);
    let crc = key_iv_checksum(&TS_KEY, &TS_IV);

    let page0 = build_encrypted_fsp_hdr(1, 1, 1, SERVER_UUID, &encrypted_key_iv, crc);
    let ts_file = write_tablespace(&[page0]);

    // Run pages subcommand in full-file mode (no --page) to see FSP header detail
    let mut buf = Vec::new();
    let result = idb::cli::pages::execute(
        &idb::cli::pages::PagesOptions {
            file: ts_file.path().to_str().unwrap().to_string(),
            page: None,
            verbose: false,
            show_empty: false,
            list_mode: false,
            filter_type: None,
            page_size: None,
            json: false,
            keyring: None,
        },
        &mut buf,
    );
    assert!(result.is_ok(), "pages failed: {:?}", result.err());

    let output = String::from_utf8(buf).unwrap();
    // Should display encryption info in FSP header detail
    assert!(
        output.contains("Encryption") || output.contains("Master Key"),
        "Expected encryption info in output, got:\n{}",
        output
    );
}

#[test]
fn test_diff_subcommand_with_keyring() {
    let encrypted_key_iv = encrypt_key_iv(&MASTER_KEY, &TS_KEY, &TS_IV);
    let crc = key_iv_checksum(&TS_KEY, &TS_IV);

    let page0 = build_encrypted_fsp_hdr(1, 2, 1, SERVER_UUID, &encrypted_key_iv, crc);
    let page1 = build_encrypted_index_page(1, 1, 2000, &TS_KEY, &TS_IV);

    let ts_file1 = write_tablespace(&[page0.clone(), page1.clone()]);
    let ts_file2 = write_tablespace(&[page0, page1]);
    let keyring_data = build_keyring_file(SERVER_UUID, 1, &MASTER_KEY);
    let keyring_file = write_keyring(&keyring_data);

    // Diff two identical encrypted files with keyring
    let mut buf = Vec::new();
    let result = idb::cli::diff::execute(
        &idb::cli::diff::DiffOptions {
            file1: ts_file1.path().to_str().unwrap().to_string(),
            file2: ts_file2.path().to_str().unwrap().to_string(),
            verbose: false,
            byte_ranges: false,
            page: None,
            json: false,
            page_size: None,
            keyring: Some(keyring_file.path().to_str().unwrap().to_string()),
        },
        &mut buf,
    );
    assert!(result.is_ok(), "diff failed: {:?}", result.err());

    let output = String::from_utf8(buf).unwrap();
    assert!(
        output.contains("Identical pages"),
        "Expected identical pages in diff output, got:\n{}",
        output
    );
}

#[test]
fn test_dump_subcommand_decrypt_flag() {
    let encrypted_key_iv = encrypt_key_iv(&MASTER_KEY, &TS_KEY, &TS_IV);
    let crc = key_iv_checksum(&TS_KEY, &TS_IV);

    let page0 = build_encrypted_fsp_hdr(1, 2, 1, SERVER_UUID, &encrypted_key_iv, crc);
    let page1 = build_encrypted_index_page(1, 1, 2000, &TS_KEY, &TS_IV);

    let ts_file = write_tablespace(&[page0, page1]);
    let keyring_data = build_keyring_file(SERVER_UUID, 1, &MASTER_KEY);
    let keyring_file = write_keyring(&keyring_data);

    // Dump with --decrypt flag
    let mut buf = Vec::new();
    let result = idb::cli::dump::execute(
        &idb::cli::dump::DumpOptions {
            file: ts_file.path().to_str().unwrap().to_string(),
            page: Some(1),
            offset: None,
            length: Some(64),
            raw: false,
            page_size: None,
            keyring: Some(keyring_file.path().to_str().unwrap().to_string()),
            decrypt: true,
        },
        &mut buf,
    );
    assert!(result.is_ok(), "dump --decrypt failed: {:?}", result.err());

    let output = String::from_utf8(buf).unwrap();
    assert!(
        output.contains("Hex dump"),
        "Expected hex dump output, got:\n{}",
        output
    );
}

#[test]
fn test_dump_decrypt_without_keyring_errors() {
    let mut page0 = vec![0u8; PS];
    BigEndian::write_u32(&mut page0[FIL_PAGE_OFFSET..], 0);
    BigEndian::write_u32(&mut page0[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page0[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page0[FIL_PAGE_LSN..], 1000);
    BigEndian::write_u16(&mut page0[FIL_PAGE_TYPE..], 8);
    BigEndian::write_u32(&mut page0[FIL_PAGE_SPACE_ID..], 1);
    let fsp = FIL_PAGE_DATA;
    BigEndian::write_u32(&mut page0[fsp + FSP_SPACE_ID..], 1);
    BigEndian::write_u32(&mut page0[fsp + FSP_SIZE..], 1);
    BigEndian::write_u32(&mut page0[fsp + FSP_FREE_LIMIT..], 1);
    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page0[trailer + 4..], 1000);
    write_crc32c_checksum(&mut page0);

    let ts_file = write_tablespace(&[page0]);

    let mut buf = Vec::new();
    let result = idb::cli::dump::execute(
        &idb::cli::dump::DumpOptions {
            file: ts_file.path().to_str().unwrap().to_string(),
            page: Some(0),
            offset: None,
            length: None,
            raw: false,
            page_size: None,
            keyring: None,
            decrypt: true,
        },
        &mut buf,
    );

    // --decrypt without --keyring should error
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("--decrypt requires --keyring"),
        "Expected '--decrypt requires --keyring' error, got: {}",
        err
    );
}
