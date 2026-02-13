//! Tablespace page decryption using AES-256-CBC.
//!
//! Provides [`DecryptionContext`] which holds the per-tablespace key and IV
//! derived from the keyring master key and the encryption info on page 0.
//! Pages with encrypted page types (15, 16, 17) are decrypted in-place
//! by [`DecryptionContext::decrypt_page`].

use aes::cipher::block_padding::NoPadding;
use aes::cipher::{BlockDecryptMut, KeyInit, KeyIvInit};
use aes::Aes256;
use byteorder::{BigEndian, ByteOrder};

use crate::innodb::constants::*;
use crate::innodb::encryption::EncryptionInfo;
use crate::innodb::keyring::Keyring;
use crate::innodb::page_types::PageType;
use crate::IdbError;

type Aes256CbcDec = cbc::Decryptor<Aes256>;
type Aes256EcbDec = ecb::Decryptor<Aes256>;

/// Holds the decrypted per-tablespace key and IV for page decryption.
#[derive(Debug)]
pub struct DecryptionContext {
    /// Decrypted 32-byte tablespace key for AES-256-CBC.
    tablespace_key: [u8; 32],
    /// Decrypted 32-byte IV (first 16 bytes used as AES-CBC IV).
    tablespace_iv: [u8; 32],
}

impl DecryptionContext {
    /// Build a decryption context from encryption info and a keyring.
    ///
    /// Looks up the master key in the keyring using the server UUID and
    /// master key ID from the encryption info, then decrypts the tablespace
    /// key+IV using AES-256-ECB, and verifies the CRC32 checksum.
    pub fn from_encryption_info(
        info: &EncryptionInfo,
        keyring: &Keyring,
    ) -> Result<Self, IdbError> {
        let master_key = keyring
            .find_innodb_master_key(&info.server_uuid, info.master_key_id)
            .ok_or_else(|| {
                IdbError::Parse(format!(
                    "Master key not found in keyring: INNODBKey-{}-{}",
                    info.server_uuid, info.master_key_id
                ))
            })?;

        if master_key.len() != 32 {
            return Err(IdbError::Parse(format!(
                "Master key has wrong length: expected 32, got {}",
                master_key.len()
            )));
        }

        // Decrypt the tablespace key+IV using AES-256-ECB with the master key
        let mut decrypted = info.encrypted_key_iv;
        let decryptor = Aes256EcbDec::new_from_slice(master_key)
            .map_err(|e| IdbError::Parse(format!("AES-256-ECB init failed: {}", e)))?;
        decryptor
            .decrypt_padded_mut::<NoPadding>(&mut decrypted)
            .map_err(|e| IdbError::Parse(format!("AES-256-ECB decrypt failed: {}", e)))?;

        // Verify CRC32 checksum of the decrypted key+IV
        let computed_crc = crc32c::crc32c(&decrypted);
        if computed_crc != info.checksum {
            return Err(IdbError::Parse(format!(
                "Failed to decrypt tablespace key: CRC32 checksum mismatch \
                 (computed=0x{:08X}, expected=0x{:08X}). Wrong keyring?",
                computed_crc, info.checksum
            )));
        }

        let mut tablespace_key = [0u8; 32];
        let mut tablespace_iv = [0u8; 32];
        tablespace_key.copy_from_slice(&decrypted[..32]);
        tablespace_iv.copy_from_slice(&decrypted[32..64]);

        Ok(DecryptionContext {
            tablespace_key,
            tablespace_iv,
        })
    }

    /// Decrypt an encrypted page in-place.
    ///
    /// Decrypts bytes [38..page_size-8) using AES-256-CBC, then restores
    /// the original page type from the FIL header byte 26 (where MySQL
    /// saves it before overwriting with the encrypted page type).
    ///
    /// Returns `Ok(true)` if the page was decrypted, `Ok(false)` if the
    /// page type is not an encrypted type and no decryption was needed.
    pub fn decrypt_page(&self, page_data: &mut [u8], page_size: usize) -> Result<bool, IdbError> {
        if page_data.len() < page_size {
            return Err(IdbError::Parse(
                "Page data too short for decryption".to_string(),
            ));
        }

        // Check if this page has an encrypted page type
        let page_type_raw = BigEndian::read_u16(&page_data[FIL_PAGE_TYPE..]);
        let page_type = PageType::from_u16(page_type_raw);

        if !matches!(
            page_type,
            PageType::Encrypted | PageType::CompressedEncrypted | PageType::EncryptedRtree
        ) {
            return Ok(false);
        }

        // Read the original page type stored at offset 26 (FIL_PAGE_FILE_FLUSH_LSN)
        // MySQL saves the original type here before encrypting
        let original_type = BigEndian::read_u16(&page_data[FIL_PAGE_ORIGINAL_TYPE_V1..]);

        // Encrypted range: [38..page_size-8)
        let encrypt_start = SIZE_FIL_HEAD;
        let encrypt_end = page_size - SIZE_FIL_TRAILER;
        let encrypt_len = encrypt_end - encrypt_start;

        // AES block size is 16 bytes; MySQL handles the tail specially
        let aes_block_size = 16;

        if encrypt_len < aes_block_size {
            return Err(IdbError::Parse(
                "Encrypted page body too small for AES decryption".to_string(),
            ));
        }

        // Use first 16 bytes of the 32-byte IV
        let iv: [u8; 16] = self.tablespace_iv[..16].try_into().unwrap();

        // Decrypt the block-aligned portion of the page body.
        // For standard 16K pages, the body is 16338 bytes (remainder of 2 bytes
        // after block alignment). The trailing non-aligned bytes are left as-is;
        // they are not significant for page structure parsing.
        let main_len = (encrypt_len / aes_block_size) * aes_block_size;

        if main_len > 0 {
            let main_end = encrypt_start + main_len;
            let decryptor = Aes256CbcDec::new_from_slices(&self.tablespace_key, &iv)
                .map_err(|e| IdbError::Parse(format!("AES-256-CBC init failed: {}", e)))?;
            decryptor
                .decrypt_padded_mut::<NoPadding>(&mut page_data[encrypt_start..main_end])
                .map_err(|e| IdbError::Parse(format!("AES-256-CBC decrypt failed: {}", e)))?;
        }

        // Restore the original page type
        BigEndian::write_u16(&mut page_data[FIL_PAGE_TYPE..], original_type);

        Ok(true)
    }

    /// Check if a page has an encrypted page type.
    pub fn is_encrypted_page(page_data: &[u8]) -> bool {
        if page_data.len() < SIZE_FIL_HEAD {
            return false;
        }
        let page_type = PageType::from_u16(BigEndian::read_u16(&page_data[FIL_PAGE_TYPE..]));
        matches!(
            page_type,
            PageType::Encrypted | PageType::CompressedEncrypted | PageType::EncryptedRtree
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes::cipher::BlockEncryptMut;

    type Aes256CbcEnc = cbc::Encryptor<Aes256>;
    type Aes256EcbEnc = ecb::Encryptor<Aes256>;

    /// Build a synthetic encrypted page for testing.
    fn build_encrypted_page(
        page_num: u32,
        space_id: u32,
        original_type: u16,
        key: &[u8; 32],
        iv: &[u8; 32],
        page_size: usize,
    ) -> Vec<u8> {
        let mut page = vec![0u8; page_size];

        // FIL header
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
        BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
        BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 5000);
        // Save original type at offset 26
        BigEndian::write_u16(&mut page[FIL_PAGE_ORIGINAL_TYPE_V1..], original_type);
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

        // Write some recognizable data in the page body
        for i in SIZE_FIL_HEAD..page_size - SIZE_FIL_TRAILER {
            page[i] = ((i * 7 + 13) & 0xFF) as u8;
        }

        // Encrypt body: [38..page_size-8)
        let encrypt_start = SIZE_FIL_HEAD;
        let encrypt_end = page_size - SIZE_FIL_TRAILER;
        let encrypt_len = encrypt_end - encrypt_start;
        let aes_block_size = 16;
        let main_len = (encrypt_len / aes_block_size) * aes_block_size;

        let cbc_iv: [u8; 16] = iv[..16].try_into().unwrap();
        let encryptor = Aes256CbcEnc::new_from_slices(key, &cbc_iv).unwrap();
        encryptor
            .encrypt_padded_mut::<NoPadding>(
                &mut page[encrypt_start..encrypt_start + main_len],
                main_len,
            )
            .unwrap();

        // Set encrypted page type
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 15); // Encrypted

        // Trailer
        let trailer = page_size - SIZE_FIL_TRAILER;
        BigEndian::write_u32(&mut page[trailer + 4..], (5000u64 & 0xFFFFFFFF) as u32);

        page
    }

    #[test]
    fn test_decrypt_page_roundtrip() {
        let key: [u8; 32] = [0x42; 32];
        let iv: [u8; 32] = [0x13; 32];
        let page_size = 16384;

        // Build a reference page with original content
        let mut reference = vec![0u8; page_size];
        for i in SIZE_FIL_HEAD..page_size - SIZE_FIL_TRAILER {
            reference[i] = ((i * 7 + 13) & 0xFF) as u8;
        }

        // Build encrypted version
        let mut encrypted = build_encrypted_page(1, 1, 17855, &key, &iv, page_size);

        // Verify it's marked encrypted
        let pt = BigEndian::read_u16(&encrypted[FIL_PAGE_TYPE..]);
        assert_eq!(pt, 15);

        // Decrypt
        let ctx = DecryptionContext {
            tablespace_key: key,
            tablespace_iv: iv,
        };
        let decrypted = ctx.decrypt_page(&mut encrypted, page_size).unwrap();
        assert!(decrypted);

        // Page type should be restored to INDEX (17855)
        let restored_type = BigEndian::read_u16(&encrypted[FIL_PAGE_TYPE..]);
        assert_eq!(restored_type, 17855);

        // Body content should match reference
        assert_eq!(
            &encrypted[SIZE_FIL_HEAD..page_size - SIZE_FIL_TRAILER],
            &reference[SIZE_FIL_HEAD..page_size - SIZE_FIL_TRAILER]
        );
    }

    #[test]
    fn test_decrypt_non_encrypted_page_is_noop() {
        let key: [u8; 32] = [0x42; 32];
        let iv: [u8; 32] = [0x13; 32];

        let mut page = vec![0u8; 16384];
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX, not encrypted

        let ctx = DecryptionContext {
            tablespace_key: key,
            tablespace_iv: iv,
        };
        let result = ctx.decrypt_page(&mut page, 16384).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_is_encrypted_page() {
        let mut page = vec![0u8; 38];
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 15);
        assert!(DecryptionContext::is_encrypted_page(&page));

        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 16);
        assert!(DecryptionContext::is_encrypted_page(&page));

        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17);
        assert!(DecryptionContext::is_encrypted_page(&page));

        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855);
        assert!(!DecryptionContext::is_encrypted_page(&page));
    }

    #[test]
    fn test_from_encryption_info() {
        use crate::innodb::keyring::Keyring;
        use sha2::{Digest, Sha256};

        // Generate known keys
        let master_key: [u8; 32] = [0xAA; 32];
        let ts_key: [u8; 32] = [0xBB; 32];
        let ts_iv: [u8; 32] = [0xCC; 32];

        // Encrypt ts_key+iv with AES-256-ECB using master key
        let mut key_iv_data = [0u8; 64];
        key_iv_data[..32].copy_from_slice(&ts_key);
        key_iv_data[32..].copy_from_slice(&ts_iv);

        let crc = crc32c::crc32c(&key_iv_data);

        let encryptor = Aes256EcbEnc::new_from_slice(&master_key).unwrap();
        let mut encrypted_key_iv = key_iv_data;
        encryptor
            .encrypt_padded_mut::<NoPadding>(&mut encrypted_key_iv, 64)
            .unwrap();

        let uuid = "12345678-1234-1234-1234-123456789abc";
        let info = EncryptionInfo {
            magic_version: 3,
            master_key_id: 1,
            server_uuid: uuid.to_string(),
            encrypted_key_iv,
            checksum: crc,
        };

        // Build a keyring file with the master key
        let obfuscate_key = b"*305=Ljt0*!@$Hnm(*-9-w;:";
        let key_id = format!("INNODBKey-{}-1", uuid);
        let mut obfuscated_master = master_key.to_vec();
        for (i, byte) in obfuscated_master.iter_mut().enumerate() {
            *byte ^= obfuscate_key[i % obfuscate_key.len()];
        }

        let mut entry = Vec::new();
        let pod_size = 40 + key_id.len() + 3 + 0 + 32;
        entry.extend_from_slice(&(pod_size as u64).to_le_bytes());
        entry.extend_from_slice(&(key_id.len() as u64).to_le_bytes());
        entry.extend_from_slice(&(3u64).to_le_bytes()); // "AES"
        entry.extend_from_slice(&(0u64).to_le_bytes()); // ""
        entry.extend_from_slice(&(32u64).to_le_bytes());
        entry.extend_from_slice(key_id.as_bytes());
        entry.extend_from_slice(b"AES");
        entry.extend_from_slice(&obfuscated_master);

        let mut file_data = entry;
        let mut hasher = Sha256::new();
        hasher.update(&file_data);
        let hash = hasher.finalize();
        file_data.extend_from_slice(&hash);

        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), &file_data).unwrap();

        let keyring = Keyring::load(tmp.path()).unwrap();
        let ctx = DecryptionContext::from_encryption_info(&info, &keyring).unwrap();

        assert_eq!(ctx.tablespace_key, ts_key);
        assert_eq!(ctx.tablespace_iv, ts_iv);
    }

    #[test]
    fn test_from_encryption_info_wrong_key() {
        use crate::innodb::keyring::Keyring;
        use sha2::{Digest, Sha256};

        let master_key: [u8; 32] = [0xAA; 32];
        let wrong_master: [u8; 32] = [0xDD; 32];
        let ts_key: [u8; 32] = [0xBB; 32];
        let ts_iv: [u8; 32] = [0xCC; 32];

        let mut key_iv_data = [0u8; 64];
        key_iv_data[..32].copy_from_slice(&ts_key);
        key_iv_data[32..].copy_from_slice(&ts_iv);
        let crc = crc32c::crc32c(&key_iv_data);

        // Encrypt with the correct master key
        let encryptor = Aes256EcbEnc::new_from_slice(&master_key).unwrap();
        let mut encrypted_key_iv = key_iv_data;
        encryptor
            .encrypt_padded_mut::<NoPadding>(&mut encrypted_key_iv, 64)
            .unwrap();

        let uuid = "12345678-1234-1234-1234-123456789abc";
        let info = EncryptionInfo {
            magic_version: 3,
            master_key_id: 1,
            server_uuid: uuid.to_string(),
            encrypted_key_iv,
            checksum: crc,
        };

        // Build keyring with WRONG master key
        let obfuscate_key = b"*305=Ljt0*!@$Hnm(*-9-w;:";
        let key_id = format!("INNODBKey-{}-1", uuid);
        let mut obfuscated = wrong_master.to_vec();
        for (i, byte) in obfuscated.iter_mut().enumerate() {
            *byte ^= obfuscate_key[i % obfuscate_key.len()];
        }

        let mut entry = Vec::new();
        let pod_size = 40 + key_id.len() + 3 + 0 + 32;
        entry.extend_from_slice(&(pod_size as u64).to_le_bytes());
        entry.extend_from_slice(&(key_id.len() as u64).to_le_bytes());
        entry.extend_from_slice(&(3u64).to_le_bytes());
        entry.extend_from_slice(&(0u64).to_le_bytes());
        entry.extend_from_slice(&(32u64).to_le_bytes());
        entry.extend_from_slice(key_id.as_bytes());
        entry.extend_from_slice(b"AES");
        entry.extend_from_slice(&obfuscated);

        let mut file_data = entry;
        let mut hasher = Sha256::new();
        hasher.update(&file_data);
        let hash = hasher.finalize();
        file_data.extend_from_slice(&hash);

        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), &file_data).unwrap();

        let keyring = Keyring::load(tmp.path()).unwrap();
        let result = DecryptionContext::from_encryption_info(&info, &keyring);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("CRC32 checksum mismatch"));
    }
}
