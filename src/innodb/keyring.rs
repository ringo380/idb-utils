//! MySQL `keyring_file` plugin binary format reader.
//!
//! Parses the legacy binary keyring file format used by the `keyring_file`
//! MySQL plugin (MySQL 5.7.11+). Each key entry is serialized with length
//! prefixes and the key data is XOR-obfuscated. The file ends with a
//! SHA-256 digest over all preceding bytes for integrity verification.

use std::path::Path;

use sha2::{Digest, Sha256};

use crate::IdbError;

/// XOR obfuscation key used by MySQL's `keyring_file` plugin.
const OBFUSCATE_KEY: &[u8] = b"*305=Ljt0*!@$Hnm(*-9-w;:";

/// A single entry from a MySQL keyring file.
#[derive(Debug, Clone)]
pub struct KeyringEntry {
    /// Key identifier (e.g., `INNODBKey-{uuid}-{id}`).
    pub key_id: String,
    /// Key type (e.g., `AES`).
    pub key_type: String,
    /// User ID associated with the key.
    pub user_id: String,
    /// De-obfuscated key data.
    pub key_data: Vec<u8>,
}

/// A parsed MySQL keyring file.
#[derive(Debug)]
pub struct Keyring {
    entries: Vec<KeyringEntry>,
}

impl Keyring {
    /// Load and parse a MySQL `keyring_file` from disk.
    ///
    /// Reads the binary file, verifies the trailing SHA-256 checksum,
    /// and parses all key entries with XOR de-obfuscation.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, IdbError> {
        let path = path.as_ref();
        let data = std::fs::read(path)
            .map_err(|e| IdbError::Io(format!("Cannot read keyring file {}: {}", path.display(), e)))?;

        if data.len() < 32 {
            return Err(IdbError::Parse(
                "Keyring file too small (must contain at least SHA-256 digest)".to_string(),
            ));
        }

        // Verify SHA-256 checksum (last 32 bytes)
        let content_len = data.len() - 32;
        let content = &data[..content_len];
        let stored_hash = &data[content_len..];

        let mut hasher = Sha256::new();
        hasher.update(content);
        let computed_hash = hasher.finalize();

        if computed_hash.as_slice() != stored_hash {
            return Err(IdbError::Parse(
                "Keyring file SHA-256 checksum mismatch (file may be corrupt)".to_string(),
            ));
        }

        // Parse entries from content
        let entries = parse_entries(content)?;

        Ok(Keyring { entries })
    }

    /// Find a key entry by its full key ID string.
    pub fn find_key(&self, key_id: &str) -> Option<&KeyringEntry> {
        self.entries.iter().find(|e| e.key_id == key_id)
    }

    /// Find the InnoDB master key for a given server UUID and key ID number.
    ///
    /// Constructs the key ID as `INNODBKey-{server_uuid}-{key_id}` and
    /// looks it up in the keyring.
    pub fn find_innodb_master_key(&self, server_uuid: &str, key_id: u32) -> Option<&[u8]> {
        let full_id = format!("INNODBKey-{}-{}", server_uuid, key_id);
        self.find_key(&full_id).map(|e| e.key_data.as_slice())
    }

    /// Returns the number of entries in the keyring.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the keyring contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// XOR de-obfuscate key data using MySQL's obfuscation key.
fn deobfuscate(data: &mut [u8]) {
    let key_len = OBFUSCATE_KEY.len();
    for (i, byte) in data.iter_mut().enumerate() {
        *byte ^= OBFUSCATE_KEY[i % key_len];
    }
}

/// Read a little-endian u64 from a byte slice.
fn read_le_u64(data: &[u8]) -> u64 {
    u64::from_le_bytes(data[..8].try_into().unwrap())
}

/// Parse all keyring entries from the content portion of the file.
fn parse_entries(mut data: &[u8]) -> Result<Vec<KeyringEntry>, IdbError> {
    let mut entries = Vec::new();

    while !data.is_empty() {
        if data.len() < 40 {
            // Need at least 5 * 8 bytes for the length headers
            break;
        }

        // Each entry: [pod_size(8)][key_id_len(8)][key_type_len(8)][user_id_len(8)][key_len(8)]
        //             [key_id][key_type][user_id][key_data]
        let pod_size = read_le_u64(&data[0..8]) as usize;
        let key_id_len = read_le_u64(&data[8..16]) as usize;
        let key_type_len = read_le_u64(&data[16..24]) as usize;
        let user_id_len = read_le_u64(&data[24..32]) as usize;
        let key_len = read_le_u64(&data[32..40]) as usize;

        let header_size = 40;
        let total_data = key_id_len + key_type_len + user_id_len + key_len;
        let entry_size = header_size + total_data;

        // Validate sizes
        if pod_size == 0 || entry_size > data.len() {
            break;
        }

        let mut offset = header_size;

        let key_id = String::from_utf8_lossy(&data[offset..offset + key_id_len]).to_string();
        offset += key_id_len;

        let key_type = String::from_utf8_lossy(&data[offset..offset + key_type_len]).to_string();
        offset += key_type_len;

        let user_id = String::from_utf8_lossy(&data[offset..offset + user_id_len]).to_string();
        offset += user_id_len;

        let mut key_data = data[offset..offset + key_len].to_vec();
        deobfuscate(&mut key_data);

        entries.push(KeyringEntry {
            key_id,
            key_type,
            user_id,
            key_data,
        });

        data = &data[entry_size..];
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deobfuscate_roundtrip() {
        let original = vec![0x41, 0x42, 0x43, 0x44];
        let mut data = original.clone();
        deobfuscate(&mut data);
        // After one XOR, should differ
        assert_ne!(data, original);
        // After second XOR, should be back to original
        deobfuscate(&mut data);
        assert_eq!(data, original);
    }

    #[test]
    fn test_deobfuscate_wraps_key() {
        // Data longer than OBFUSCATE_KEY should wrap
        let mut data = vec![0u8; OBFUSCATE_KEY.len() * 2 + 5];
        deobfuscate(&mut data);
        // First and (key_len+1)th bytes should use same XOR key byte
        assert_eq!(data[0], data[OBFUSCATE_KEY.len()]);
    }

    fn build_keyring_entry(key_id: &str, key_type: &str, user_id: &str, key_data: &[u8]) -> Vec<u8> {
        let mut obfuscated = key_data.to_vec();
        deobfuscate(&mut obfuscated);

        let pod_size = 40 + key_id.len() + key_type.len() + user_id.len() + key_data.len();
        let mut entry = Vec::new();
        entry.extend_from_slice(&(pod_size as u64).to_le_bytes());
        entry.extend_from_slice(&(key_id.len() as u64).to_le_bytes());
        entry.extend_from_slice(&(key_type.len() as u64).to_le_bytes());
        entry.extend_from_slice(&(user_id.len() as u64).to_le_bytes());
        entry.extend_from_slice(&(key_data.len() as u64).to_le_bytes());
        entry.extend_from_slice(key_id.as_bytes());
        entry.extend_from_slice(key_type.as_bytes());
        entry.extend_from_slice(user_id.as_bytes());
        entry.extend_from_slice(&obfuscated);
        entry
    }

    fn build_keyring_file(entries: &[Vec<u8>]) -> Vec<u8> {
        let mut data = Vec::new();
        for entry in entries {
            data.extend_from_slice(entry);
        }
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let hash = hasher.finalize();
        data.extend_from_slice(&hash);
        data
    }

    #[test]
    fn test_parse_single_entry() {
        let key_data = vec![0x01, 0x02, 0x03, 0x04];
        let entry = build_keyring_entry("test-key", "AES", "user1", &key_data);
        let file_data = build_keyring_file(&[entry]);

        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), &file_data).unwrap();

        let keyring = Keyring::load(tmp.path()).unwrap();
        assert_eq!(keyring.len(), 1);
        let e = keyring.find_key("test-key").unwrap();
        assert_eq!(e.key_type, "AES");
        assert_eq!(e.user_id, "user1");
        assert_eq!(e.key_data, key_data);
    }

    #[test]
    fn test_parse_multiple_entries() {
        let key1 = vec![0xAA; 32];
        let key2 = vec![0xBB; 32];
        let entry1 = build_keyring_entry("INNODBKey-uuid-1", "AES", "", &key1);
        let entry2 = build_keyring_entry("INNODBKey-uuid-2", "AES", "", &key2);
        let file_data = build_keyring_file(&[entry1, entry2]);

        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), &file_data).unwrap();

        let keyring = Keyring::load(tmp.path()).unwrap();
        assert_eq!(keyring.len(), 2);
        assert_eq!(keyring.find_key("INNODBKey-uuid-1").unwrap().key_data, key1);
        assert_eq!(keyring.find_key("INNODBKey-uuid-2").unwrap().key_data, key2);
    }

    #[test]
    fn test_find_innodb_master_key() {
        let key_data = vec![0xCC; 32];
        let entry = build_keyring_entry(
            "INNODBKey-12345678-1234-1234-1234-123456789abc-1",
            "AES",
            "",
            &key_data,
        );
        let file_data = build_keyring_file(&[entry]);

        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), &file_data).unwrap();

        let keyring = Keyring::load(tmp.path()).unwrap();
        let found = keyring
            .find_innodb_master_key("12345678-1234-1234-1234-123456789abc", 1)
            .unwrap();
        assert_eq!(found, &key_data[..]);
    }

    #[test]
    fn test_find_innodb_master_key_not_found() {
        let entry = build_keyring_entry("INNODBKey-uuid-1", "AES", "", &[0u8; 32]);
        let file_data = build_keyring_file(&[entry]);

        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), &file_data).unwrap();

        let keyring = Keyring::load(tmp.path()).unwrap();
        assert!(keyring.find_innodb_master_key("other-uuid", 1).is_none());
    }

    #[test]
    fn test_bad_checksum_rejected() {
        let entry = build_keyring_entry("key", "AES", "", &[0u8; 16]);
        let mut file_data = build_keyring_file(&[entry]);
        // Corrupt the SHA-256 digest
        let len = file_data.len();
        file_data[len - 1] ^= 0xFF;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), &file_data).unwrap();

        let result = Keyring::load(tmp.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("checksum mismatch"));
    }

    #[test]
    fn test_empty_keyring() {
        let file_data = build_keyring_file(&[]);

        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), &file_data).unwrap();

        let keyring = Keyring::load(tmp.path()).unwrap();
        assert!(keyring.is_empty());
        assert_eq!(keyring.len(), 0);
    }
}
