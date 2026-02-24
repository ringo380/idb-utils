//! Audit logging for write operations.
//!
//! Provides [`AuditLogger`] which writes NDJSON events to a log file for
//! compliance audit trails. Every write operation (repair, defrag, transplant,
//! corrupt) emits structured events recording what was changed, when, and by
//! which invocation.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Mutex;
use std::time::Instant;

use chrono::Local;
use fs2::FileExt;
use serde::Serialize;

use crate::IdbError;

/// A single audit log event, serialized as tagged NDJSON.
#[derive(Serialize)]
#[serde(tag = "event")]
pub enum AuditEvent {
    /// Emitted once at the start of a CLI invocation.
    #[serde(rename = "session_start")]
    SessionStart {
        timestamp: String,
        args: Vec<String>,
        version: String,
    },

    /// Emitted when a single page is written (repair, transplant, corrupt).
    #[serde(rename = "page_write")]
    PageWrite {
        timestamp: String,
        file: String,
        page_number: u64,
        operation: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        old_checksum: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        new_checksum: Option<u32>,
    },

    /// Emitted when a whole file is written (defrag).
    #[serde(rename = "file_write")]
    FileWrite {
        timestamp: String,
        file: String,
        operation: String,
        pages_written: u64,
    },

    /// Emitted when a backup file is created.
    #[serde(rename = "backup_created")]
    BackupCreated {
        timestamp: String,
        source: String,
        backup_path: String,
    },

    /// Emitted once at the end of a CLI invocation.
    #[serde(rename = "session_end")]
    SessionEnd {
        timestamp: String,
        duration_ms: u64,
        pages_written: u64,
        files_written: u64,
    },
}

struct AuditLoggerInner {
    file: File,
    pages_written: u64,
    files_written: u64,
}

/// Thread-safe audit logger that appends NDJSON events to a file.
///
/// File-level locking (via `fs2`) ensures safe concurrent access from
/// multiple processes.
pub struct AuditLogger {
    inner: Mutex<AuditLoggerInner>,
    start: Instant,
}

impl AuditLogger {
    /// Open (or create) the audit log file in append mode.
    pub fn open(path: &str) -> Result<Self, IdbError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| IdbError::Io(format!("Cannot open audit log {}: {}", path, e)))?;

        Ok(Self {
            inner: Mutex::new(AuditLoggerInner {
                file,
                pages_written: 0,
                files_written: 0,
            }),
            start: Instant::now(),
        })
    }

    /// Emit a single audit event as one NDJSON line.
    pub fn emit(&self, event: &AuditEvent) -> Result<(), IdbError> {
        let line = serde_json::to_string(event)
            .map_err(|e| IdbError::Parse(format!("Audit JSON error: {}", e)))?;

        let mut inner = self.inner.lock().unwrap();
        inner
            .file
            .lock_exclusive()
            .map_err(|e| IdbError::Io(format!("Audit log lock error: {}", e)))?;
        writeln!(inner.file, "{}", line)
            .map_err(|e| IdbError::Io(format!("Audit log write error: {}", e)))?;
        inner
            .file
            .flush()
            .map_err(|e| IdbError::Io(format!("Audit log flush error: {}", e)))?;
        inner
            .file
            .unlock()
            .map_err(|e| IdbError::Io(format!("Audit log unlock error: {}", e)))?;

        Ok(())
    }

    /// Emit a `session_start` event.
    pub fn start_session(&self, args: Vec<String>) -> Result<(), IdbError> {
        self.emit(&AuditEvent::SessionStart {
            timestamp: now(),
            args,
            version: env!("CARGO_PKG_VERSION").to_string(),
        })
    }

    /// Emit a `session_end` event with accumulated counters.
    pub fn end_session(&self) -> Result<(), IdbError> {
        let inner = self.inner.lock().unwrap();
        let duration_ms = self.start.elapsed().as_millis() as u64;
        let event = AuditEvent::SessionEnd {
            timestamp: now(),
            duration_ms,
            pages_written: inner.pages_written,
            files_written: inner.files_written,
        };
        drop(inner);
        self.emit(&event)
    }

    /// Log a page-level write operation.
    pub fn log_page_write(
        &self,
        file: &str,
        page_number: u64,
        operation: &str,
        old_checksum: Option<u32>,
        new_checksum: Option<u32>,
    ) -> Result<(), IdbError> {
        self.emit(&AuditEvent::PageWrite {
            timestamp: now(),
            file: file.to_string(),
            page_number,
            operation: operation.to_string(),
            old_checksum,
            new_checksum,
        })?;
        self.inner.lock().unwrap().pages_written += 1;
        Ok(())
    }

    /// Log a whole-file write operation (e.g., defrag output).
    pub fn log_file_write(
        &self,
        file: &str,
        operation: &str,
        pages_written: u64,
    ) -> Result<(), IdbError> {
        self.emit(&AuditEvent::FileWrite {
            timestamp: now(),
            file: file.to_string(),
            operation: operation.to_string(),
            pages_written,
        })?;
        self.inner.lock().unwrap().files_written += 1;
        Ok(())
    }

    /// Log a backup file creation.
    pub fn log_backup(&self, source: &str, backup_path: &str) -> Result<(), IdbError> {
        self.emit(&AuditEvent::BackupCreated {
            timestamp: now(),
            source: source.to_string(),
            backup_path: backup_path.to_string(),
        })
    }
}

fn now() -> String {
    Local::now().to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use tempfile::NamedTempFile;

    fn temp_logger() -> (AuditLogger, String) {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        // Keep the file handle alive via the path; logger opens independently
        drop(tmp);
        let logger = AuditLogger::open(&path).unwrap();
        (logger, path)
    }

    #[test]
    fn test_writes_ndjson_lines() {
        let (logger, path) = temp_logger();
        logger
            .start_session(vec!["inno".into(), "repair".into()])
            .unwrap();
        logger
            .log_page_write("test.ibd", 1, "repair", Some(0xDEAD), Some(0xBEEF))
            .unwrap();
        logger.end_session().unwrap();

        let file = File::open(&path).unwrap();
        let lines: Vec<String> = BufReader::new(file).lines().map(|l| l.unwrap()).collect();
        assert_eq!(lines.len(), 3);

        // Verify each line is valid JSON with expected event tag
        let v0: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(v0["event"], "session_start");

        let v1: serde_json::Value = serde_json::from_str(&lines[1]).unwrap();
        assert_eq!(v1["event"], "page_write");
        assert_eq!(v1["page_number"], 1);

        let v2: serde_json::Value = serde_json::from_str(&lines[2]).unwrap();
        assert_eq!(v2["event"], "session_end");
        assert_eq!(v2["pages_written"], 1);
        assert_eq!(v2["files_written"], 0);
    }

    #[test]
    fn test_append_mode() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        drop(tmp);

        // First logger writes some events
        {
            let logger = AuditLogger::open(&path).unwrap();
            logger.start_session(vec!["session1".into()]).unwrap();
        }

        // Second logger appends
        {
            let logger = AuditLogger::open(&path).unwrap();
            logger.start_session(vec!["session2".into()]).unwrap();
        }

        let file = File::open(&path).unwrap();
        let lines: Vec<String> = BufReader::new(file).lines().map(|l| l.unwrap()).collect();
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn test_session_counters() {
        let (logger, path) = temp_logger();
        logger.start_session(vec![]).unwrap();
        logger
            .log_page_write("a.ibd", 0, "repair", None, None)
            .unwrap();
        logger
            .log_page_write("a.ibd", 1, "repair", None, None)
            .unwrap();
        logger.log_file_write("out.ibd", "defrag", 10).unwrap();
        logger.end_session().unwrap();

        let file = File::open(&path).unwrap();
        let lines: Vec<String> = BufReader::new(file).lines().map(|l| l.unwrap()).collect();
        let last: serde_json::Value = serde_json::from_str(lines.last().unwrap()).unwrap();
        assert_eq!(last["pages_written"], 2);
        assert_eq!(last["files_written"], 1);
    }

    #[test]
    fn test_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let (logger, path) = temp_logger();
        let logger = Arc::new(logger);

        let mut handles = Vec::new();
        for i in 0..10 {
            let lg = Arc::clone(&logger);
            handles.push(thread::spawn(move || {
                lg.log_page_write("test.ibd", i, "repair", None, None)
                    .unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let file = File::open(&path).unwrap();
        let lines: Vec<String> = BufReader::new(file).lines().map(|l| l.unwrap()).collect();
        assert_eq!(lines.len(), 10);

        // Every line should be valid JSON
        for line in &lines {
            let _: serde_json::Value = serde_json::from_str(line).unwrap();
        }
    }

    #[test]
    fn test_backup_event() {
        let (logger, path) = temp_logger();
        logger.log_backup("test.ibd", "test.ibd.bak").unwrap();

        let file = File::open(&path).unwrap();
        let lines: Vec<String> = BufReader::new(file).lines().map(|l| l.unwrap()).collect();
        assert_eq!(lines.len(), 1);

        let v: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(v["event"], "backup_created");
        assert_eq!(v["source"], "test.ibd");
    }

    #[test]
    fn test_file_write_event() {
        let (logger, path) = temp_logger();
        logger.log_file_write("output.ibd", "defrag", 42).unwrap();

        let file = File::open(&path).unwrap();
        let lines: Vec<String> = BufReader::new(file).lines().map(|l| l.unwrap()).collect();
        assert_eq!(lines.len(), 1);

        let v: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(v["event"], "file_write");
        assert_eq!(v["pages_written"], 42);
    }
}
