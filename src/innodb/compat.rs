//! MySQL version compatibility checking for InnoDB tablespaces.
//!
//! Analyzes an InnoDB tablespace file and checks whether it is compatible
//! with a target MySQL version. Reports warnings and errors for features
//! that are deprecated, removed, or unsupported in the target version.
//!
//! # Usage
//!
//! ```rust,ignore
//! use idb::innodb::tablespace::Tablespace;
//! use idb::innodb::compat::{extract_tablespace_info, build_compat_report, MysqlVersion};
//!
//! let mut ts = Tablespace::open("table.ibd").unwrap();
//! let info = extract_tablespace_info(&mut ts).unwrap();
//! let target = MysqlVersion::parse("8.4.0").unwrap();
//! let report = build_compat_report(&info, &target, "table.ibd");
//! println!("Compatible: {}", report.compatible);
//! ```

use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;

use crate::innodb::constants::*;
use crate::innodb::page::FilHeader;
use crate::innodb::page_types::PageType;
use crate::innodb::vendor::{InnoDbVendor, VendorInfo};
use crate::IdbError;

/// A parsed MySQL version (major.minor.patch).
///
/// # Examples
///
/// ```
/// use idb::innodb::compat::MysqlVersion;
///
/// let v = MysqlVersion::parse("8.4.0").unwrap();
/// assert_eq!(v.major, 8);
/// assert_eq!(v.minor, 4);
/// assert_eq!(v.patch, 0);
/// assert_eq!(v.to_string(), "8.4.0");
/// assert_eq!(v.to_id(), 80400);
///
/// let v2 = MysqlVersion::from_id(90001);
/// assert_eq!(v2.major, 9);
/// assert_eq!(v2.minor, 0);
/// assert_eq!(v2.patch, 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MysqlVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl MysqlVersion {
    /// Parse from "8.0.32" format.
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::compat::MysqlVersion;
    ///
    /// let v = MysqlVersion::parse("5.7.44").unwrap();
    /// assert_eq!(v.major, 5);
    /// assert_eq!(v.minor, 7);
    /// assert_eq!(v.patch, 44);
    ///
    /// assert!(MysqlVersion::parse("8.0").is_err());
    /// assert!(MysqlVersion::parse("abc").is_err());
    /// ```
    pub fn parse(s: &str) -> Result<Self, IdbError> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 3 {
            return Err(IdbError::Argument(format!(
                "Invalid MySQL version '{}': expected format X.Y.Z",
                s
            )));
        }
        let major = parts[0]
            .parse::<u32>()
            .map_err(|_| IdbError::Argument(format!("Invalid major version in '{}'", s)))?;
        let minor = parts[1]
            .parse::<u32>()
            .map_err(|_| IdbError::Argument(format!("Invalid minor version in '{}'", s)))?;
        let patch = parts[2]
            .parse::<u32>()
            .map_err(|_| IdbError::Argument(format!("Invalid patch version in '{}'", s)))?;
        Ok(MysqlVersion {
            major,
            minor,
            patch,
        })
    }

    /// Create from MySQL version_id (e.g., 80032 -> 8.0.32).
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::compat::MysqlVersion;
    ///
    /// let v = MysqlVersion::from_id(80032);
    /// assert_eq!(v.to_string(), "8.0.32");
    /// ```
    pub fn from_id(version_id: u64) -> Self {
        MysqlVersion {
            major: (version_id / 10000) as u32,
            minor: ((version_id % 10000) / 100) as u32,
            patch: (version_id % 100) as u32,
        }
    }

    /// Convert to MySQL version_id format.
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::compat::MysqlVersion;
    ///
    /// let v = MysqlVersion::parse("8.0.32").unwrap();
    /// assert_eq!(v.to_id(), 80032);
    /// ```
    pub fn to_id(&self) -> u64 {
        (self.major as u64) * 10000 + (self.minor as u64) * 100 + self.patch as u64
    }

    /// Check if this version is >= another.
    ///
    /// # Examples
    ///
    /// ```
    /// use idb::innodb::compat::MysqlVersion;
    ///
    /// let v8 = MysqlVersion::parse("8.4.0").unwrap();
    /// let v9 = MysqlVersion::parse("9.0.0").unwrap();
    /// assert!(v9.is_at_least(&v8));
    /// assert!(!v8.is_at_least(&v9));
    /// assert!(v8.is_at_least(&v8));
    /// ```
    pub fn is_at_least(&self, other: &MysqlVersion) -> bool {
        (self.major, self.minor, self.patch) >= (other.major, other.minor, other.patch)
    }
}

impl std::fmt::Display for MysqlVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Severity of a compatibility finding.
///
/// # Examples
///
/// ```
/// use idb::innodb::compat::Severity;
///
/// assert_eq!(format!("{}", Severity::Error), "error");
/// assert_eq!(format!("{}", Severity::Warning), "warning");
/// assert_eq!(format!("{}", Severity::Info), "info");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Severity {
    /// Informational: no action required.
    Info,
    /// Warning: feature is deprecated or may cause issues.
    Warning,
    /// Error: tablespace cannot be used with the target version.
    Error,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Info => write!(f, "info"),
            Severity::Warning => write!(f, "warning"),
            Severity::Error => write!(f, "error"),
        }
    }
}

/// A single compatibility check result.
#[derive(Debug, Clone, Serialize)]
pub struct CompatCheck {
    /// Name of the check (e.g., "page_size", "row_format", "sdi").
    pub check: String,
    /// Human-readable description of the finding.
    pub message: String,
    /// Severity level of the finding.
    pub severity: Severity,
    /// Current value observed in the tablespace, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_value: Option<String>,
    /// Expected or recommended value, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<String>,
}

/// Information extracted from a tablespace for compatibility analysis.
#[derive(Debug, Clone, Serialize)]
pub struct TablespaceInfo {
    /// Detected page size in bytes.
    pub page_size: u32,
    /// Raw FSP flags from page 0.
    pub fsp_flags: u32,
    /// Space ID from the FSP header.
    pub space_id: u32,
    /// Row format name (e.g., "DYNAMIC", "COMPRESSED"), if available from SDI.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_format: Option<String>,
    /// Whether the tablespace contains SDI pages.
    pub has_sdi: bool,
    /// Whether the tablespace is encrypted.
    pub is_encrypted: bool,
    /// Detected vendor information.
    pub vendor: VendorInfo,
    /// MySQL version ID from SDI metadata, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mysql_version_id: Option<u64>,
    /// Whether the tablespace contains compressed pages (FIL_PAGE_COMPRESSED).
    pub has_compressed_pages: bool,
    /// Whether the tablespace uses instant ADD COLUMN (detected from SDI).
    pub has_instant_columns: bool,
}

/// Compatibility report for a tablespace.
#[derive(Debug, Clone, Serialize)]
pub struct CompatReport {
    /// Path to the analyzed file.
    pub file: String,
    /// Target MySQL version string.
    pub target_version: String,
    /// Source MySQL version string (from SDI metadata), if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_version: Option<String>,
    /// Whether the tablespace is compatible with the target version (no errors).
    pub compatible: bool,
    /// Individual check results.
    pub checks: Vec<CompatCheck>,
    /// Summary counts.
    pub summary: CompatSummary,
}

/// Summary counts for a compatibility report.
#[derive(Debug, Clone, Serialize)]
pub struct CompatSummary {
    /// Total number of checks performed.
    pub total_checks: usize,
    /// Number of error-level findings.
    pub errors: usize,
    /// Number of warning-level findings.
    pub warnings: usize,
    /// Number of info-level findings.
    pub info: usize,
}

/// Extract tablespace metadata for compatibility analysis.
///
/// Reads page 0 to get FSP flags, space ID, and vendor info.
/// Optionally extracts SDI metadata if available.
pub fn extract_tablespace_info(
    ts: &mut crate::innodb::tablespace::Tablespace,
) -> Result<TablespaceInfo, IdbError> {
    let page_size = ts.page_size();
    let page0 = ts.read_page(0)?;
    let vendor = ts.vendor_info().clone();
    let fsp_flags = if page0.len() >= (FIL_PAGE_DATA + FSP_SPACE_FLAGS + 4) {
        BigEndian::read_u32(&page0[FIL_PAGE_DATA + FSP_SPACE_FLAGS..])
    } else {
        0
    };
    let space_id = ts
        .fsp_header()
        .map(|h| h.space_id)
        .unwrap_or_else(|| FilHeader::parse(&page0).map(|h| h.space_id).unwrap_or(0));
    let is_encrypted = ts.encryption_info().is_some();

    // Check for SDI pages
    let sdi_pages = crate::innodb::sdi::find_sdi_pages(ts).unwrap_or_default();
    let has_sdi = !sdi_pages.is_empty();

    // Try to extract SDI for version info and row format
    let mut mysql_version_id = None;
    let mut row_format = None;
    let has_instant_columns = false;

    if has_sdi {
        if let Ok(records) = crate::innodb::sdi::extract_sdi_from_pages(ts, &sdi_pages) {
            for rec in &records {
                if rec.sdi_type == 1 {
                    if let Ok(envelope) =
                        serde_json::from_str::<crate::innodb::schema::SdiEnvelope>(&rec.data)
                    {
                        mysql_version_id = Some(envelope.mysqld_version_id);
                        let rf_code = envelope.dd_object.row_format;
                        row_format =
                            Some(crate::innodb::schema::row_format_name(rf_code).to_string());
                        // Note: reliable instant ADD COLUMN detection requires
                        // se_private_data from the DD, which is not exposed in SDI.
                    }
                }
            }
        }
    }

    // Check for compressed pages (FIL_PAGE_COMPRESSED type)
    let has_compressed_pages = {
        let page_count = ts.page_count();
        let mut found = false;
        // Check first 10 pages (or all if fewer) for compression indicator
        let check_count = page_count.min(10);
        for i in 0..check_count {
            if let Ok(page) = ts.read_page(i) {
                if let Some(hdr) = FilHeader::parse(&page) {
                    if hdr.page_type == PageType::Compressed {
                        found = true;
                        break;
                    }
                }
            }
        }
        found
    };

    Ok(TablespaceInfo {
        page_size,
        fsp_flags,
        space_id,
        row_format,
        has_sdi,
        is_encrypted,
        vendor,
        mysql_version_id,
        has_compressed_pages,
        has_instant_columns,
    })
}

/// Run all compatibility checks against a target MySQL version.
///
/// Returns a list of findings with severity levels. Error-level findings
/// indicate the tablespace cannot be used with the target version.
pub fn check_compatibility(info: &TablespaceInfo, target: &MysqlVersion) -> Vec<CompatCheck> {
    let mut checks = Vec::new();

    check_page_size(info, target, &mut checks);
    check_row_format(info, target, &mut checks);
    check_sdi_presence(info, target, &mut checks);
    check_encryption(info, target, &mut checks);
    check_vendor_compatibility(info, target, &mut checks);
    check_compression(info, target, &mut checks);

    checks
}

/// Build a full compatibility report.
///
/// Runs all checks and produces a structured report with summary counts
/// and an overall compatible/incompatible verdict.
pub fn build_compat_report(
    info: &TablespaceInfo,
    target: &MysqlVersion,
    file: &str,
) -> CompatReport {
    let checks = check_compatibility(info, target);

    let errors = checks
        .iter()
        .filter(|c| c.severity == Severity::Error)
        .count();
    let warnings = checks
        .iter()
        .filter(|c| c.severity == Severity::Warning)
        .count();
    let info_count = checks
        .iter()
        .filter(|c| c.severity == Severity::Info)
        .count();

    let source_version = info.mysql_version_id.map(|id| {
        let v = MysqlVersion::from_id(id);
        v.to_string()
    });

    CompatReport {
        file: file.to_string(),
        target_version: target.to_string(),
        source_version,
        compatible: errors == 0,
        checks,
        summary: CompatSummary {
            total_checks: errors + warnings + info_count,
            errors,
            warnings,
            info: info_count,
        },
    }
}

// --- Private check functions ---

fn check_page_size(info: &TablespaceInfo, target: &MysqlVersion, checks: &mut Vec<CompatCheck>) {
    // 4K/8K/32K/64K page sizes added in MySQL 5.7.6
    let non_default = info.page_size != SIZE_PAGE_DEFAULT;
    if non_default
        && !target.is_at_least(&MysqlVersion {
            major: 5,
            minor: 7,
            patch: 6,
        })
    {
        checks.push(CompatCheck {
            check: "page_size".to_string(),
            message: format!(
                "Non-default page size {} requires MySQL 5.7.6+",
                info.page_size
            ),
            severity: Severity::Error,
            current_value: Some(info.page_size.to_string()),
            expected: Some("16384".to_string()),
        });
    } else if non_default {
        checks.push(CompatCheck {
            check: "page_size".to_string(),
            message: format!("Non-default page size {} is supported", info.page_size),
            severity: Severity::Info,
            current_value: Some(info.page_size.to_string()),
            expected: None,
        });
    }
}

fn check_row_format(info: &TablespaceInfo, target: &MysqlVersion, checks: &mut Vec<CompatCheck>) {
    if let Some(ref rf) = info.row_format {
        let rf_upper = rf.to_uppercase();
        // COMPRESSED deprecated in 8.4+
        if rf_upper == "COMPRESSED"
            && target.is_at_least(&MysqlVersion {
                major: 8,
                minor: 4,
                patch: 0,
            })
        {
            checks.push(CompatCheck {
                check: "row_format".to_string(),
                message: "ROW_FORMAT=COMPRESSED is deprecated in MySQL 8.4+".to_string(),
                severity: Severity::Warning,
                current_value: Some(rf.clone()),
                expected: Some("DYNAMIC".to_string()),
            });
        }
        // REDUNDANT deprecated in 9.0+
        if rf_upper == "REDUNDANT"
            && target.is_at_least(&MysqlVersion {
                major: 9,
                minor: 0,
                patch: 0,
            })
        {
            checks.push(CompatCheck {
                check: "row_format".to_string(),
                message: "ROW_FORMAT=REDUNDANT is deprecated in MySQL 9.0+".to_string(),
                severity: Severity::Warning,
                current_value: Some(rf.clone()),
                expected: Some("DYNAMIC".to_string()),
            });
        }
    }
}

fn check_sdi_presence(info: &TablespaceInfo, target: &MysqlVersion, checks: &mut Vec<CompatCheck>) {
    // SDI required for MySQL 8.0+
    if target.is_at_least(&MysqlVersion {
        major: 8,
        minor: 0,
        patch: 0,
    }) && !info.has_sdi
    {
        checks.push(CompatCheck {
            check: "sdi".to_string(),
            message: "Tablespace lacks SDI metadata required by MySQL 8.0+".to_string(),
            severity: Severity::Error,
            current_value: Some("absent".to_string()),
            expected: Some("present".to_string()),
        });
    } else if info.has_sdi
        && !target.is_at_least(&MysqlVersion {
            major: 8,
            minor: 0,
            patch: 0,
        })
    {
        checks.push(CompatCheck {
            check: "sdi".to_string(),
            message: "Tablespace has SDI metadata not recognized by MySQL < 8.0".to_string(),
            severity: Severity::Warning,
            current_value: Some("present".to_string()),
            expected: Some("absent".to_string()),
        });
    }
}

fn check_encryption(info: &TablespaceInfo, target: &MysqlVersion, checks: &mut Vec<CompatCheck>) {
    // Tablespace-level encryption added in MySQL 5.7.11
    if info.is_encrypted
        && !target.is_at_least(&MysqlVersion {
            major: 5,
            minor: 7,
            patch: 11,
        })
    {
        checks.push(CompatCheck {
            check: "encryption".to_string(),
            message: "Tablespace encryption requires MySQL 5.7.11+".to_string(),
            severity: Severity::Error,
            current_value: Some("encrypted".to_string()),
            expected: Some("unencrypted".to_string()),
        });
    }
}

fn check_vendor_compatibility(
    info: &TablespaceInfo,
    target: &MysqlVersion,
    checks: &mut Vec<CompatCheck>,
) {
    // MariaDB -> MySQL = error (divergent formats)
    if info.vendor.vendor == InnoDbVendor::MariaDB {
        checks.push(CompatCheck {
            check: "vendor".to_string(),
            message: "MariaDB tablespace is not compatible with MySQL".to_string(),
            severity: Severity::Error,
            current_value: Some(info.vendor.to_string()),
            expected: Some("MySQL".to_string()),
        });
    }
    // Percona -> MySQL is fine (binary compatible)
    if info.vendor.vendor == InnoDbVendor::Percona {
        checks.push(CompatCheck {
            check: "vendor".to_string(),
            message: "Percona XtraDB tablespace is binary-compatible with MySQL".to_string(),
            severity: Severity::Info,
            current_value: Some(info.vendor.to_string()),
            expected: None,
        });
    }
    // Suppress the unused variable warning
    let _ = target;
}

fn check_compression(info: &TablespaceInfo, _target: &MysqlVersion, checks: &mut Vec<CompatCheck>) {
    if info.has_compressed_pages {
        checks.push(CompatCheck {
            check: "compression".to_string(),
            message: "Tablespace uses page compression".to_string(),
            severity: Severity::Info,
            current_value: Some("compressed".to_string()),
            expected: None,
        });
    }
}

/// Per-file result for directory scan mode.
#[derive(Debug, Clone, Serialize)]
pub struct ScanFileResult {
    /// Relative path within the data directory.
    pub file: String,
    /// Whether this file is compatible with the target version.
    pub compatible: bool,
    /// Error message if the file could not be analyzed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Individual check results (empty if error occurred).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub checks: Vec<CompatCheck>,
}

/// Directory scan compatibility report.
#[derive(Debug, Clone, Serialize)]
pub struct ScanCompatReport {
    /// Target MySQL version.
    pub target_version: String,
    /// Number of files scanned.
    pub files_scanned: usize,
    /// Number of compatible files.
    pub files_compatible: usize,
    /// Number of incompatible files.
    pub files_incompatible: usize,
    /// Number of files with errors.
    pub files_error: usize,
    /// Per-file results.
    pub results: Vec<ScanFileResult>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::innodb::vendor::MariaDbFormat;

    #[test]
    fn test_version_parse_valid() {
        let v = MysqlVersion::parse("8.0.32").unwrap();
        assert_eq!(v.major, 8);
        assert_eq!(v.minor, 0);
        assert_eq!(v.patch, 32);
    }

    #[test]
    fn test_version_parse_invalid_format() {
        assert!(MysqlVersion::parse("8.0").is_err());
        assert!(MysqlVersion::parse("8").is_err());
        assert!(MysqlVersion::parse("").is_err());
        assert!(MysqlVersion::parse("8.0.x").is_err());
    }

    #[test]
    fn test_version_from_id() {
        let v = MysqlVersion::from_id(80032);
        assert_eq!(v.major, 8);
        assert_eq!(v.minor, 0);
        assert_eq!(v.patch, 32);

        let v = MysqlVersion::from_id(90001);
        assert_eq!(v.major, 9);
        assert_eq!(v.minor, 0);
        assert_eq!(v.patch, 1);
    }

    #[test]
    fn test_version_to_id() {
        let v = MysqlVersion::parse("8.0.32").unwrap();
        assert_eq!(v.to_id(), 80032);

        let v = MysqlVersion::parse("9.0.1").unwrap();
        assert_eq!(v.to_id(), 90001);
    }

    #[test]
    fn test_version_display() {
        let v = MysqlVersion::parse("8.4.0").unwrap();
        assert_eq!(v.to_string(), "8.4.0");
    }

    #[test]
    fn test_version_is_at_least() {
        let v8 = MysqlVersion::parse("8.0.0").unwrap();
        let v84 = MysqlVersion::parse("8.4.0").unwrap();
        let v9 = MysqlVersion::parse("9.0.0").unwrap();

        assert!(v9.is_at_least(&v84));
        assert!(v9.is_at_least(&v8));
        assert!(v84.is_at_least(&v8));
        assert!(v8.is_at_least(&v8));
        assert!(!v8.is_at_least(&v84));
        assert!(!v84.is_at_least(&v9));
    }

    #[test]
    fn test_severity_display() {
        assert_eq!(Severity::Info.to_string(), "info");
        assert_eq!(Severity::Warning.to_string(), "warning");
        assert_eq!(Severity::Error.to_string(), "error");
    }

    #[test]
    fn test_check_page_size_default() {
        let info = TablespaceInfo {
            page_size: 16384,
            fsp_flags: 0,
            space_id: 1,
            row_format: None,
            has_sdi: true,
            is_encrypted: false,
            vendor: VendorInfo::mysql(),
            mysql_version_id: None,
            has_compressed_pages: false,
            has_instant_columns: false,
        };
        let target = MysqlVersion::parse("8.0.0").unwrap();
        let mut checks = Vec::new();
        check_page_size(&info, &target, &mut checks);
        // Default page size should produce no checks
        assert!(checks.is_empty());
    }

    #[test]
    fn test_check_page_size_non_default_old_mysql() {
        let info = TablespaceInfo {
            page_size: 8192,
            fsp_flags: 0,
            space_id: 1,
            row_format: None,
            has_sdi: false,
            is_encrypted: false,
            vendor: VendorInfo::mysql(),
            mysql_version_id: None,
            has_compressed_pages: false,
            has_instant_columns: false,
        };
        let target = MysqlVersion::parse("5.6.0").unwrap();
        let mut checks = Vec::new();
        check_page_size(&info, &target, &mut checks);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].severity, Severity::Error);
    }

    #[test]
    fn test_check_sdi_missing_for_8_0() {
        let info = TablespaceInfo {
            page_size: 16384,
            fsp_flags: 0,
            space_id: 1,
            row_format: None,
            has_sdi: false,
            is_encrypted: false,
            vendor: VendorInfo::mysql(),
            mysql_version_id: None,
            has_compressed_pages: false,
            has_instant_columns: false,
        };
        let target = MysqlVersion::parse("8.0.0").unwrap();
        let mut checks = Vec::new();
        check_sdi_presence(&info, &target, &mut checks);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].severity, Severity::Error);
        assert!(checks[0].message.contains("lacks SDI"));
    }

    #[test]
    fn test_check_sdi_present_for_pre_8() {
        let info = TablespaceInfo {
            page_size: 16384,
            fsp_flags: 0,
            space_id: 1,
            row_format: None,
            has_sdi: true,
            is_encrypted: false,
            vendor: VendorInfo::mysql(),
            mysql_version_id: None,
            has_compressed_pages: false,
            has_instant_columns: false,
        };
        let target = MysqlVersion::parse("5.7.44").unwrap();
        let mut checks = Vec::new();
        check_sdi_presence(&info, &target, &mut checks);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].severity, Severity::Warning);
    }

    #[test]
    fn test_check_vendor_mariadb() {
        let info = TablespaceInfo {
            page_size: 16384,
            fsp_flags: 0,
            space_id: 1,
            row_format: None,
            has_sdi: false,
            is_encrypted: false,
            vendor: VendorInfo::mariadb(MariaDbFormat::FullCrc32),
            mysql_version_id: None,
            has_compressed_pages: false,
            has_instant_columns: false,
        };
        let target = MysqlVersion::parse("8.4.0").unwrap();
        let mut checks = Vec::new();
        check_vendor_compatibility(&info, &target, &mut checks);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].severity, Severity::Error);
        assert!(checks[0].message.contains("MariaDB"));
    }

    #[test]
    fn test_check_vendor_percona() {
        let info = TablespaceInfo {
            page_size: 16384,
            fsp_flags: 0,
            space_id: 1,
            row_format: None,
            has_sdi: true,
            is_encrypted: false,
            vendor: VendorInfo::percona(),
            mysql_version_id: None,
            has_compressed_pages: false,
            has_instant_columns: false,
        };
        let target = MysqlVersion::parse("8.4.0").unwrap();
        let mut checks = Vec::new();
        check_vendor_compatibility(&info, &target, &mut checks);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].severity, Severity::Info);
    }

    #[test]
    fn test_check_row_format_compressed_84() {
        let info = TablespaceInfo {
            page_size: 16384,
            fsp_flags: 0,
            space_id: 1,
            row_format: Some("COMPRESSED".to_string()),
            has_sdi: true,
            is_encrypted: false,
            vendor: VendorInfo::mysql(),
            mysql_version_id: None,
            has_compressed_pages: false,
            has_instant_columns: false,
        };
        let target = MysqlVersion::parse("8.4.0").unwrap();
        let mut checks = Vec::new();
        check_row_format(&info, &target, &mut checks);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].severity, Severity::Warning);
        assert!(checks[0].message.contains("COMPRESSED"));
    }

    #[test]
    fn test_check_row_format_redundant_90() {
        let info = TablespaceInfo {
            page_size: 16384,
            fsp_flags: 0,
            space_id: 1,
            row_format: Some("REDUNDANT".to_string()),
            has_sdi: true,
            is_encrypted: false,
            vendor: VendorInfo::mysql(),
            mysql_version_id: None,
            has_compressed_pages: false,
            has_instant_columns: false,
        };
        let target = MysqlVersion::parse("9.0.0").unwrap();
        let mut checks = Vec::new();
        check_row_format(&info, &target, &mut checks);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].severity, Severity::Warning);
        assert!(checks[0].message.contains("REDUNDANT"));
    }

    #[test]
    fn test_check_encryption_old_mysql() {
        let info = TablespaceInfo {
            page_size: 16384,
            fsp_flags: 0,
            space_id: 1,
            row_format: None,
            has_sdi: false,
            is_encrypted: true,
            vendor: VendorInfo::mysql(),
            mysql_version_id: None,
            has_compressed_pages: false,
            has_instant_columns: false,
        };
        let target = MysqlVersion::parse("5.6.0").unwrap();
        let mut checks = Vec::new();
        check_encryption(&info, &target, &mut checks);
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].severity, Severity::Error);
    }

    #[test]
    fn test_build_compat_report_compatible() {
        let info = TablespaceInfo {
            page_size: 16384,
            fsp_flags: 0,
            space_id: 1,
            row_format: Some("DYNAMIC".to_string()),
            has_sdi: true,
            is_encrypted: false,
            vendor: VendorInfo::mysql(),
            mysql_version_id: Some(80032),
            has_compressed_pages: false,
            has_instant_columns: false,
        };
        let target = MysqlVersion::parse("8.4.0").unwrap();
        let report = build_compat_report(&info, &target, "test.ibd");
        assert!(report.compatible);
        assert_eq!(report.summary.errors, 0);
        assert_eq!(report.source_version, Some("8.0.32".to_string()));
    }

    #[test]
    fn test_build_compat_report_incompatible() {
        let info = TablespaceInfo {
            page_size: 16384,
            fsp_flags: 0,
            space_id: 1,
            row_format: None,
            has_sdi: false,
            is_encrypted: false,
            vendor: VendorInfo::mariadb(MariaDbFormat::FullCrc32),
            mysql_version_id: None,
            has_compressed_pages: false,
            has_instant_columns: false,
        };
        let target = MysqlVersion::parse("8.4.0").unwrap();
        let report = build_compat_report(&info, &target, "test.ibd");
        assert!(!report.compatible);
        assert!(report.summary.errors > 0);
    }
}
