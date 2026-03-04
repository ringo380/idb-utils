//! R-tree (spatial index) page parsing.
//!
//! InnoDB uses R-tree indexes for spatial data types (GEOMETRY, POINT, etc.).
//! R-tree pages share the same INDEX page header structure as B+Tree pages
//! but store Minimum Bounding Rectangles (MBRs) as keys instead of row data.
//!
//! Each MBR is 32 bytes: four 8-byte IEEE 754 doubles representing
//! `(min_x, min_y, max_x, max_y)` in BigEndian format.

use byteorder::{BigEndian, ByteOrder};
use serde::Serialize;

use crate::innodb::index::IndexHeader;

/// Size of a Minimum Bounding Rectangle in bytes (4 × f64).
const MBR_SIZE: usize = 32;

/// Parsed Minimum Bounding Rectangle from an R-tree record.
///
/// # Examples
///
/// ```
/// use idb::innodb::rtree::MinimumBoundingRectangle;
/// use byteorder::{BigEndian, ByteOrder};
///
/// let mut buf = vec![0u8; 32];
/// BigEndian::write_f64(&mut buf[0..], 1.0);
/// BigEndian::write_f64(&mut buf[8..], 2.0);
/// BigEndian::write_f64(&mut buf[16..], 3.0);
/// BigEndian::write_f64(&mut buf[24..], 4.0);
///
/// let mbr = MinimumBoundingRectangle::parse(&buf).unwrap();
/// assert_eq!(mbr.min_x, 1.0);
/// assert_eq!(mbr.min_y, 2.0);
/// assert_eq!(mbr.max_x, 3.0);
/// assert_eq!(mbr.max_y, 4.0);
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct MinimumBoundingRectangle {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

impl MinimumBoundingRectangle {
    /// Parse an MBR from a 32-byte buffer.
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < MBR_SIZE {
            return None;
        }

        Some(MinimumBoundingRectangle {
            min_x: BigEndian::read_f64(&data[0..]),
            min_y: BigEndian::read_f64(&data[8..]),
            max_x: BigEndian::read_f64(&data[16..]),
            max_y: BigEndian::read_f64(&data[24..]),
        })
    }

    /// Returns the area of this bounding rectangle.
    pub fn area(&self) -> f64 {
        (self.max_x - self.min_x) * (self.max_y - self.min_y)
    }

    /// Returns the overall MBR that encloses all given MBRs.
    pub fn enclosing(mbrs: &[MinimumBoundingRectangle]) -> Option<MinimumBoundingRectangle> {
        if mbrs.is_empty() {
            return None;
        }
        let mut min_x = f64::MAX;
        let mut min_y = f64::MAX;
        let mut max_x = f64::MIN;
        let mut max_y = f64::MIN;
        for m in mbrs {
            if m.min_x < min_x {
                min_x = m.min_x;
            }
            if m.min_y < min_y {
                min_y = m.min_y;
            }
            if m.max_x > max_x {
                max_x = m.max_x;
            }
            if m.max_y > max_y {
                max_y = m.max_y;
            }
        }
        Some(MinimumBoundingRectangle {
            min_x,
            min_y,
            max_x,
            max_y,
        })
    }
}

/// Parsed R-tree page information.
///
/// Reuses the standard INDEX page header for level and record count,
/// then extracts MBRs from the record data area.
#[derive(Debug, Clone, Serialize)]
pub struct RtreePageInfo {
    /// R-tree level (0 = leaf).
    pub level: u16,
    /// Number of user records on this page.
    pub record_count: u16,
    /// MBRs extracted from records on this page.
    pub mbrs: Vec<MinimumBoundingRectangle>,
    /// Enclosing MBR covering all records (if any).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enclosing_mbr: Option<MinimumBoundingRectangle>,
}

/// Parse an R-tree page and extract MBR data.
///
/// Uses the standard INDEX page header for level and record count. MBRs
/// are extracted by scanning for 32-byte aligned record data after the
/// page header area.
///
/// Returns `None` if the page is too small or the INDEX header can't be parsed.
///
/// # Examples
///
/// ```
/// use idb::innodb::rtree::parse_rtree_page;
/// use idb::innodb::constants::*;
/// use byteorder::{BigEndian, ByteOrder};
///
/// let mut page = vec![0u8; 256];
/// let base = FIL_PAGE_DATA;
///
/// // Set INDEX header fields
/// BigEndian::write_u16(&mut page[base + PAGE_LEVEL..], 0); // leaf
/// BigEndian::write_u16(&mut page[base + PAGE_N_RECS..], 1);
///
/// let info = parse_rtree_page(&page);
/// assert!(info.is_some());
/// let info = info.unwrap();
/// assert_eq!(info.level, 0);
/// assert_eq!(info.record_count, 1);
/// ```
pub fn parse_rtree_page(page_data: &[u8]) -> Option<RtreePageInfo> {
    let idx_hdr = IndexHeader::parse(page_data)?;

    // Records start after the INDEX header (36 bytes) + 2 FSEG headers (20 bytes)
    // + infimum (13 bytes) + supremum (13 bytes) = 82 bytes from FIL_PAGE_DATA
    // = offset 120 from page start
    let record_area_start = 120;

    let mut mbrs = Vec::new();
    let n_recs = idx_hdr.n_recs as usize;

    // Scan for MBRs in the record area
    // Each R-tree record starts with a 5-byte compact record header,
    // followed by the 32-byte MBR key, then a 4-byte child page pointer (non-leaf)
    // or row data (leaf).
    let mut offset = record_area_start;
    let record_header_size = 5; // compact format record header

    for _ in 0..n_recs {
        let mbr_start = offset + record_header_size;
        if mbr_start + MBR_SIZE > page_data.len() {
            break;
        }

        if let Some(mbr) = MinimumBoundingRectangle::parse(&page_data[mbr_start..]) {
            // Sanity check: skip obviously invalid MBRs (NaN or infinity)
            if mbr.min_x.is_finite()
                && mbr.min_y.is_finite()
                && mbr.max_x.is_finite()
                && mbr.max_y.is_finite()
            {
                mbrs.push(mbr);
            }
        }

        // Move to next record: header + MBR + child pointer (4 bytes for non-leaf)
        // For leaf: header + MBR + row data (variable)
        // Use a conservative estimate of 5 + 32 + 4 = 41 bytes per record
        offset += record_header_size + MBR_SIZE + 4;
    }

    let enclosing_mbr = MinimumBoundingRectangle::enclosing(&mbrs);

    Some(RtreePageInfo {
        level: idx_hdr.level,
        record_count: idx_hdr.n_recs,
        mbrs,
        enclosing_mbr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::innodb::constants::*;

    #[test]
    fn test_mbr_parse() {
        let mut buf = vec![0u8; 32];
        BigEndian::write_f64(&mut buf[0..], 1.0);
        BigEndian::write_f64(&mut buf[8..], 2.0);
        BigEndian::write_f64(&mut buf[16..], 3.0);
        BigEndian::write_f64(&mut buf[24..], 4.0);

        let mbr = MinimumBoundingRectangle::parse(&buf).unwrap();
        assert_eq!(mbr.min_x, 1.0);
        assert_eq!(mbr.min_y, 2.0);
        assert_eq!(mbr.max_x, 3.0);
        assert_eq!(mbr.max_y, 4.0);
    }

    #[test]
    fn test_mbr_too_short() {
        let buf = vec![0u8; 20];
        assert!(MinimumBoundingRectangle::parse(&buf).is_none());
    }

    #[test]
    fn test_mbr_area() {
        let mbr = MinimumBoundingRectangle {
            min_x: 0.0,
            min_y: 0.0,
            max_x: 10.0,
            max_y: 5.0,
        };
        assert!((mbr.area() - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_mbr_enclosing() {
        let mbrs = vec![
            MinimumBoundingRectangle {
                min_x: 1.0,
                min_y: 2.0,
                max_x: 5.0,
                max_y: 6.0,
            },
            MinimumBoundingRectangle {
                min_x: 3.0,
                min_y: 1.0,
                max_x: 10.0,
                max_y: 8.0,
            },
        ];

        let enc = MinimumBoundingRectangle::enclosing(&mbrs).unwrap();
        assert_eq!(enc.min_x, 1.0);
        assert_eq!(enc.min_y, 1.0);
        assert_eq!(enc.max_x, 10.0);
        assert_eq!(enc.max_y, 8.0);
    }

    #[test]
    fn test_mbr_enclosing_empty() {
        assert!(MinimumBoundingRectangle::enclosing(&[]).is_none());
    }

    #[test]
    fn test_parse_rtree_page_basic() {
        let mut page = vec![0u8; 256];
        let base = FIL_PAGE_DATA;

        // Set INDEX header: level=0, n_recs=1
        BigEndian::write_u16(&mut page[base + PAGE_LEVEL..], 0);
        BigEndian::write_u16(&mut page[base + PAGE_N_RECS..], 1);

        // Write an MBR at record area (offset 120 + 5 byte record header)
        let mbr_offset = 125;
        BigEndian::write_f64(&mut page[mbr_offset..], 10.0);
        BigEndian::write_f64(&mut page[mbr_offset + 8..], 20.0);
        BigEndian::write_f64(&mut page[mbr_offset + 16..], 30.0);
        BigEndian::write_f64(&mut page[mbr_offset + 24..], 40.0);

        let info = parse_rtree_page(&page).unwrap();
        assert_eq!(info.level, 0);
        assert_eq!(info.record_count, 1);
        assert_eq!(info.mbrs.len(), 1);
        assert_eq!(info.mbrs[0].min_x, 10.0);
        assert_eq!(info.mbrs[0].max_y, 40.0);
        assert!(info.enclosing_mbr.is_some());
    }

    #[test]
    fn test_parse_rtree_page_too_short() {
        let page = vec![0u8; 30];
        assert!(parse_rtree_page(&page).is_none());
    }
}
