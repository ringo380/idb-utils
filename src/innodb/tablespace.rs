//! InnoDB tablespace file I/O.
//!
//! Provides [`Tablespace`], the primary entry point for opening and reading
//! `.ibd` tablespace files. The page size is auto-detected from the FSP flags
//! on page 0 (supports 4K, 8K, 16K, 32K, and 64K pages). Individual pages
//! can be read by number, and the full file can be iterated page-by-page.
//!
//! The FSP header from page 0 is also parsed and cached, giving access to
//! the space ID, tablespace size, and feature flags (compression, encryption).

use std::io::{Cursor, Read, Seek, SeekFrom};

use crate::innodb::constants::*;
use crate::innodb::decryption::DecryptionContext;
use crate::innodb::encryption::{self, EncryptionInfo};
use crate::innodb::page::{FilHeader, FilTrailer, FspHeader};
use crate::innodb::vendor::{detect_vendor_from_flags, VendorInfo};
use crate::IdbError;

/// Supertrait combining `Read + Seek` for type-erased readers.
pub(crate) trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

/// A memory-mapped file reader implementing `Read` and `Seek`.
///
/// Wraps a `memmap2::Mmap` with a cursor position so it can be used as a
/// drop-in replacement for `File` or `Cursor<Vec<u8>>` via `Box<dyn ReadSeek>`.
/// Unlike `Cursor<Vec<u8>>`, the data is not copied — it remains backed by
/// the OS page cache and only faults in pages that are actually accessed.
#[cfg(feature = "cli")]
struct MmapReader {
    mmap: memmap2::Mmap,
    position: u64,
}

#[cfg(feature = "cli")]
impl MmapReader {
    fn new(mmap: memmap2::Mmap) -> Self {
        Self { mmap, position: 0 }
    }
}

#[cfg(feature = "cli")]
impl Read for MmapReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.mmap.len() as u64;
        if self.position >= len {
            return Ok(0);
        }
        let available = (len - self.position) as usize;
        let to_read = buf.len().min(available);
        let start = self.position as usize;
        buf[..to_read].copy_from_slice(&self.mmap[start..start + to_read]);
        self.position += to_read as u64;
        Ok(to_read)
    }
}

#[cfg(feature = "cli")]
impl Seek for MmapReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let len = self.mmap.len() as i64;
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::End(offset) => len + offset,
            SeekFrom::Current(offset) => self.position as i64 + offset,
        };
        if new_pos < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "seek to a negative position",
            ));
        }
        self.position = new_pos as u64;
        Ok(self.position)
    }
}

/// Represents an open InnoDB tablespace file (.ibd) or in-memory tablespace.
pub struct Tablespace {
    reader: Box<dyn ReadSeek>,
    file_size: u64,
    page_size: u32,
    page_count: u64,
    fsp_header: Option<FspHeader>,
    vendor_info: VendorInfo,
    encryption_info: Option<EncryptionInfo>,
    decryption_ctx: Option<DecryptionContext>,
}

impl Tablespace {
    /// Open an InnoDB tablespace file and auto-detect the page size.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self, IdbError> {
        let path = path.as_ref();
        let file = std::fs::File::open(path)
            .map_err(|e| IdbError::Io(format!("Cannot open {}: {}", path.display(), e)))?;

        let file_size = file
            .metadata()
            .map_err(|e| IdbError::Io(format!("Cannot stat {}: {}", path.display(), e)))?
            .len();

        Self::init(Box::new(file), file_size, None)
    }

    /// Open an InnoDB tablespace file using memory-mapped I/O.
    ///
    /// Maps the entire file into virtual memory using `mmap(2)`. The OS
    /// manages page faults and caching, which can be more memory-efficient
    /// than buffered I/O for large files since only accessed pages are loaded
    /// into physical RAM. This is particularly beneficial when combined with
    /// parallel processing (rayon) as it avoids seek contention.
    ///
    /// # Safety
    ///
    /// The underlying `mmap` call is marked `unsafe` because the mapped file
    /// must not be modified by another process while the mapping is active.
    /// For read-only analysis of `.ibd` files this is safe in practice (the
    /// file should not be actively written to by MySQL while being analyzed).
    #[cfg(all(not(target_arch = "wasm32"), feature = "cli"))]
    pub fn open_mmap<P: AsRef<std::path::Path>>(path: P) -> Result<Self, IdbError> {
        let path = path.as_ref();
        let file = std::fs::File::open(path)
            .map_err(|e| IdbError::Io(format!("Cannot open {}: {}", path.display(), e)))?;

        let file_size = file
            .metadata()
            .map_err(|e| IdbError::Io(format!("Cannot stat {}: {}", path.display(), e)))?
            .len();

        let mmap = unsafe {
            memmap2::Mmap::map(&file)
                .map_err(|e| IdbError::Io(format!("Cannot mmap {}: {}", path.display(), e)))?
        };

        let reader = MmapReader::new(mmap);
        Self::init(Box::new(reader), file_size, None)
    }

    /// Open with memory-mapped I/O and a specific page size (bypass auto-detection).
    #[cfg(all(not(target_arch = "wasm32"), feature = "cli"))]
    pub fn open_mmap_with_page_size<P: AsRef<std::path::Path>>(
        path: P,
        page_size: u32,
    ) -> Result<Self, IdbError> {
        let path = path.as_ref();
        let file = std::fs::File::open(path)
            .map_err(|e| IdbError::Io(format!("Cannot open {}: {}", path.display(), e)))?;

        let file_size = file
            .metadata()
            .map_err(|e| IdbError::Io(format!("Cannot stat {}: {}", path.display(), e)))?
            .len();

        let mmap = unsafe {
            memmap2::Mmap::map(&file)
                .map_err(|e| IdbError::Io(format!("Cannot mmap {}: {}", path.display(), e)))?
        };

        let reader = MmapReader::new(mmap);
        Self::init(Box::new(reader), file_size, Some(page_size))
    }

    /// Open with a specific page size (bypass auto-detection).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_with_page_size<P: AsRef<std::path::Path>>(
        path: P,
        page_size: u32,
    ) -> Result<Self, IdbError> {
        let path = path.as_ref();
        let file = std::fs::File::open(path)
            .map_err(|e| IdbError::Io(format!("Cannot open {}: {}", path.display(), e)))?;

        let file_size = file
            .metadata()
            .map_err(|e| IdbError::Io(format!("Cannot stat {}: {}", path.display(), e)))?
            .len();

        Self::init(Box::new(file), file_size, Some(page_size))
    }

    /// Create a tablespace from an in-memory byte buffer with auto-detected page size.
    ///
    /// The byte buffer must contain at least one valid page starting with
    /// a FIL header and FSP header on page 0 so that the page size can be
    /// auto-detected.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use idb::innodb::tablespace::Tablespace;
    ///
    /// // `data` must be a valid tablespace image (at least one 16 KiB page
    /// // with a properly formatted FSP header on page 0).
    /// let data: Vec<u8> = std::fs::read("table.ibd").unwrap();
    /// let ts = Tablespace::from_bytes(data).unwrap();
    /// assert!(ts.page_count() > 0);
    /// println!("Page size: {} bytes", ts.page_size());
    /// ```
    pub fn from_bytes(data: Vec<u8>) -> Result<Self, IdbError> {
        let file_size = data.len() as u64;
        Self::init(Box::new(Cursor::new(data)), file_size, None)
    }

    /// Create a tablespace from an in-memory byte buffer with a specific page size.
    pub fn from_bytes_with_page_size(data: Vec<u8>, page_size: u32) -> Result<Self, IdbError> {
        let file_size = data.len() as u64;
        Self::init(Box::new(Cursor::new(data)), file_size, Some(page_size))
    }

    /// Shared initialization: read page 0, detect page size/vendor/encryption.
    fn init(
        mut reader: Box<dyn ReadSeek>,
        file_size: u64,
        forced_page_size: Option<u32>,
    ) -> Result<Self, IdbError> {
        if file_size < SIZE_FIL_HEAD as u64 + FSP_HEADER_SIZE as u64 {
            return Err(IdbError::Parse(format!(
                "File too small to be a valid tablespace: {} bytes",
                file_size
            )));
        }

        // Read the first page (at least FIL header + FSP header area) to detect page size
        let read_size = match forced_page_size {
            Some(ps) => std::cmp::min(file_size, ps as u64) as usize,
            None => std::cmp::min(file_size, SIZE_PAGE_DEFAULT as u64) as usize,
        };
        let mut buf = vec![0u8; read_size];
        reader
            .read_exact(&mut buf)
            .map_err(|e| IdbError::Io(format!("Cannot read page 0: {}", e)))?;

        // Parse FSP header from page 0 to detect page size and vendor
        let fsp_header = FspHeader::parse(&buf);
        let vendor_info = match &fsp_header {
            Some(fsp) => detect_vendor_from_flags(fsp.flags),
            None => VendorInfo::mysql(),
        };
        let page_size = forced_page_size.unwrap_or_else(|| match &fsp_header {
            Some(fsp) => {
                let detected = fsp.page_size_from_flags_with_vendor(&vendor_info);
                if matches!(detected, 4096 | 8192 | 16384 | 32768 | 65536) {
                    detected
                } else {
                    SIZE_PAGE_DEFAULT
                }
            }
            None => SIZE_PAGE_DEFAULT,
        });

        let page_count = file_size / page_size as u64;
        let encryption_info = encryption::parse_encryption_info(&buf, page_size);

        // Seek back to start for future reads
        reader
            .seek(SeekFrom::Start(0))
            .map_err(|e| IdbError::Io(format!("Cannot seek to start: {}", e)))?;

        Ok(Tablespace {
            reader,
            file_size,
            page_size,
            page_count,
            fsp_header,
            vendor_info,
            encryption_info,
            decryption_ctx: None,
        })
    }

    /// Returns the detected or configured page size.
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Returns the total number of pages in the file.
    pub fn page_count(&self) -> u64 {
        self.page_count
    }

    /// Returns the file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Returns the FSP header from page 0, if available.
    pub fn fsp_header(&self) -> Option<&FspHeader> {
        self.fsp_header.as_ref()
    }

    /// Returns the detected vendor information for this tablespace.
    pub fn vendor_info(&self) -> &VendorInfo {
        &self.vendor_info
    }

    /// Returns the parsed encryption info from page 0, if present.
    pub fn encryption_info(&self) -> Option<&EncryptionInfo> {
        self.encryption_info.as_ref()
    }

    /// Returns true if the tablespace has encryption info on page 0.
    pub fn is_encrypted(&self) -> bool {
        self.encryption_info.is_some()
    }

    /// Set a decryption context for transparent page decryption.
    ///
    /// When set, [`read_page`](Self::read_page) and
    /// [`for_each_page`](Self::for_each_page) will automatically decrypt
    /// pages with encrypted page types (15, 16, 17) before returning them.
    pub fn set_decryption_context(&mut self, ctx: DecryptionContext) {
        self.decryption_ctx = Some(ctx);
    }

    /// Read a single page by page number into a newly allocated buffer.
    ///
    /// If a decryption context has been set and the page has an encrypted
    /// page type, the page is decrypted before being returned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use idb::innodb::tablespace::Tablespace;
    /// use idb::innodb::page::FilHeader;
    ///
    /// let mut ts = Tablespace::open("table.ibd").unwrap();
    ///
    /// // Read page 0 (the FSP_HDR page)
    /// let page_data = ts.read_page(0).unwrap();
    /// let header = FilHeader::parse(&page_data).unwrap();
    /// println!("Page 0 type: {}", header.page_type);
    /// println!("Space ID: {}", header.space_id);
    /// ```
    pub fn read_page(&mut self, page_num: u64) -> Result<Vec<u8>, IdbError> {
        if page_num >= self.page_count {
            return Err(IdbError::Parse(format!(
                "Page {} out of range (tablespace has {} pages)",
                page_num, self.page_count
            )));
        }

        let offset = page_num * self.page_size as u64;
        let mut buf = vec![0u8; self.page_size as usize];

        self.reader
            .seek(SeekFrom::Start(offset))
            .map_err(|e| IdbError::Io(format!("Cannot seek to page {}: {}", page_num, e)))?;

        self.reader
            .read_exact(&mut buf)
            .map_err(|e| IdbError::Io(format!("Cannot read page {}: {}", page_num, e)))?;

        // Decrypt if a decryption context is available
        if let Some(ref ctx) = self.decryption_ctx {
            let _ = ctx.decrypt_page(&mut buf, self.page_size as usize)?;
        }

        Ok(buf)
    }

    /// Parse the FIL header from a page buffer.
    pub fn parse_fil_header(page_data: &[u8]) -> Option<FilHeader> {
        FilHeader::parse(page_data)
    }

    /// Parse the FIL trailer from a page buffer.
    pub fn parse_fil_trailer(&self, page_data: &[u8]) -> Option<FilTrailer> {
        let ps = self.page_size as usize;
        if page_data.len() < ps {
            return None;
        }
        let trailer_offset = ps - SIZE_FIL_TRAILER;
        FilTrailer::parse(&page_data[trailer_offset..])
    }

    /// Read all pages into a contiguous in-memory buffer for parallel processing.
    ///
    /// Returns the entire tablespace contents as a single `Vec<u8>` where page N
    /// starts at offset `N * page_size`. This enables parallel page processing
    /// with libraries like `rayon` — since `Tablespace` holds a non-`Send` reader,
    /// it cannot be shared across threads directly, but the returned buffer can be
    /// sliced and processed in parallel.
    ///
    /// If a decryption context is set, each page is decrypted after reading.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use idb::innodb::tablespace::Tablespace;
    /// use idb::innodb::page::FilHeader;
    ///
    /// let mut ts = Tablespace::open("table.ibd").unwrap();
    /// let all_data = ts.read_all_pages().unwrap();
    /// let page_size = ts.page_size() as usize;
    ///
    /// for page_num in 0..ts.page_count() as usize {
    ///     let offset = page_num * page_size;
    ///     let page_data = &all_data[offset..offset + page_size];
    ///     if let Some(header) = FilHeader::parse(page_data) {
    ///         println!("Page {}: type={}", page_num, header.page_type);
    ///     }
    /// }
    /// ```
    pub fn read_all_pages(&mut self) -> Result<Vec<u8>, IdbError> {
        self.reader
            .seek(SeekFrom::Start(0))
            .map_err(|e| IdbError::Io(format!("Cannot seek to start: {}", e)))?;

        let mut data = vec![0u8; self.file_size as usize];
        self.reader
            .read_exact(&mut data)
            .map_err(|e| IdbError::Io(format!("Cannot read tablespace data: {}", e)))?;

        // Decrypt pages if a decryption context is available
        if let Some(ref ctx) = self.decryption_ctx {
            let ps = self.page_size as usize;
            for page_num in 0..self.page_count as usize {
                let offset = page_num * ps;
                let page_slice = &mut data[offset..offset + ps];
                let _ = ctx.decrypt_page(page_slice, ps)?;
            }
        }

        Ok(data)
    }

    /// Iterate over all pages, calling the callback with (page_number, page_data).
    ///
    /// If a decryption context has been set, encrypted pages are decrypted
    /// before being passed to the callback.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use idb::innodb::tablespace::Tablespace;
    /// use idb::innodb::page::FilHeader;
    ///
    /// let mut ts = Tablespace::open("table.ibd").unwrap();
    /// ts.for_each_page(|page_num, page_data| {
    ///     if let Some(header) = FilHeader::parse(page_data) {
    ///         println!("Page {}: type={}, LSN={}",
    ///             page_num, header.page_type, header.lsn);
    ///     }
    ///     Ok(())
    /// }).unwrap();
    /// ```
    pub fn for_each_page<F>(&mut self, mut callback: F) -> Result<(), IdbError>
    where
        F: FnMut(u64, &[u8]) -> Result<(), IdbError>,
    {
        self.reader
            .seek(SeekFrom::Start(0))
            .map_err(|e| IdbError::Io(format!("Cannot seek to start: {}", e)))?;

        let ps = self.page_size as usize;
        let mut buf = vec![0u8; ps];
        for page_num in 0..self.page_count {
            self.reader
                .read_exact(&mut buf)
                .map_err(|e| IdbError::Io(format!("Cannot read page {}: {}", page_num, e)))?;

            if let Some(ref ctx) = self.decryption_ctx {
                let _ = ctx.decrypt_page(&mut buf, ps)?;
            }

            callback(page_num, &buf)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{BigEndian, ByteOrder};
    use std::io::Write;
    use tempfile::NamedTempFile;

    const PS: usize = SIZE_PAGE_DEFAULT as usize;

    fn build_fsp_page(space_id: u32, total_pages: u32) -> Vec<u8> {
        let mut page = vec![0u8; PS];
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 0);
        BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
        BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 1000);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 8); // FSP_HDR
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);
        let fsp = FIL_PAGE_DATA;
        BigEndian::write_u32(&mut page[fsp + FSP_SPACE_ID..], space_id);
        BigEndian::write_u32(&mut page[fsp + FSP_SIZE..], total_pages);
        BigEndian::write_u32(&mut page[fsp + FSP_FREE_LIMIT..], total_pages);
        BigEndian::write_u32(&mut page[fsp + FSP_SPACE_FLAGS..], 0);
        let trailer = PS - SIZE_FIL_TRAILER;
        BigEndian::write_u32(&mut page[trailer + 4..], 1000 & 0xFFFFFFFF);
        let end = PS - SIZE_FIL_TRAILER;
        let crc1 = crc32c::crc32c(&page[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);
        let crc2 = crc32c::crc32c(&page[FIL_PAGE_DATA..end]);
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], crc1 ^ crc2);
        page
    }

    fn build_index_page(page_num: u32, space_id: u32, lsn: u64) -> Vec<u8> {
        let mut page = vec![0u8; PS];
        BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
        BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
        BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
        BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
        BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);
        let trailer = PS - SIZE_FIL_TRAILER;
        BigEndian::write_u32(&mut page[trailer + 4..], (lsn & 0xFFFFFFFF) as u32);
        let end = PS - SIZE_FIL_TRAILER;
        let crc1 = crc32c::crc32c(&page[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);
        let crc2 = crc32c::crc32c(&page[FIL_PAGE_DATA..end]);
        BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], crc1 ^ crc2);
        page
    }

    fn write_pages(pages: &[Vec<u8>]) -> NamedTempFile {
        let mut tmp = NamedTempFile::new().expect("create temp file");
        for page in pages {
            tmp.write_all(page).expect("write page");
        }
        tmp.flush().expect("flush");
        tmp
    }

    #[test]
    fn test_open_detects_default_page_size() {
        let tmp = write_pages(&[build_fsp_page(1, 2), build_index_page(1, 1, 2000)]);
        let ts = Tablespace::open(tmp.path()).unwrap();
        assert_eq!(ts.page_size(), SIZE_PAGE_DEFAULT);
        assert_eq!(ts.page_count(), 2);
    }

    #[test]
    fn test_open_with_page_size_override() {
        let tmp = write_pages(&[build_fsp_page(1, 2), build_index_page(1, 1, 2000)]);
        let ts = Tablespace::open_with_page_size(tmp.path(), SIZE_PAGE_DEFAULT).unwrap();
        assert_eq!(ts.page_size(), SIZE_PAGE_DEFAULT);
        assert_eq!(ts.page_count(), 2);
    }

    #[test]
    fn test_open_rejects_too_small_file() {
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(&[0u8; 10]).unwrap();
        tmp.flush().unwrap();
        let result = Tablespace::open(tmp.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_read_page_returns_correct_data() {
        let tmp = write_pages(&[build_fsp_page(5, 2), build_index_page(1, 5, 9999)]);
        let mut ts = Tablespace::open(tmp.path()).unwrap();
        let data = ts.read_page(1).unwrap();
        let hdr = FilHeader::parse(&data).unwrap();
        assert_eq!(hdr.page_number, 1);
        assert_eq!(hdr.space_id, 5);
        assert_eq!(hdr.lsn, 9999);
    }

    #[test]
    fn test_read_page_out_of_range() {
        let tmp = write_pages(&[build_fsp_page(1, 1)]);
        let mut ts = Tablespace::open(tmp.path()).unwrap();
        assert!(ts.read_page(99).is_err());
    }

    #[test]
    fn test_parse_fil_header_static() {
        let page = build_index_page(7, 3, 5000);
        let hdr = Tablespace::parse_fil_header(&page).unwrap();
        assert_eq!(hdr.page_number, 7);
        assert_eq!(hdr.space_id, 3);
    }

    #[test]
    fn test_parse_fil_trailer() {
        let tmp = write_pages(&[build_fsp_page(1, 1)]);
        let ts = Tablespace::open(tmp.path()).unwrap();
        let page = build_fsp_page(1, 1);
        let trailer = ts.parse_fil_trailer(&page).unwrap();
        assert_eq!(trailer.lsn_low32, 1000);
    }

    #[test]
    fn test_for_each_page_visits_all() {
        let tmp = write_pages(&[
            build_fsp_page(1, 3),
            build_index_page(1, 1, 2000),
            build_index_page(2, 1, 3000),
        ]);
        let mut ts = Tablespace::open(tmp.path()).unwrap();
        let mut visited = Vec::new();
        ts.for_each_page(|num, _data| {
            visited.push(num);
            Ok(())
        })
        .unwrap();
        assert_eq!(visited, vec![0, 1, 2]);
    }

    #[test]
    fn test_from_bytes_detects_page_size() {
        let mut data = build_fsp_page(1, 2);
        data.extend_from_slice(&build_index_page(1, 1, 2000));
        let ts = Tablespace::from_bytes(data).unwrap();
        assert_eq!(ts.page_size(), SIZE_PAGE_DEFAULT);
        assert_eq!(ts.page_count(), 2);
    }

    #[test]
    fn test_from_bytes_read_page() {
        let mut data = build_fsp_page(5, 2);
        data.extend_from_slice(&build_index_page(1, 5, 9999));
        let mut ts = Tablespace::from_bytes(data).unwrap();
        let page = ts.read_page(1).unwrap();
        let hdr = FilHeader::parse(&page).unwrap();
        assert_eq!(hdr.page_number, 1);
        assert_eq!(hdr.space_id, 5);
        assert_eq!(hdr.lsn, 9999);
    }

    #[test]
    fn test_from_bytes_rejects_too_small() {
        let result = Tablespace::from_bytes(vec![0u8; 10]);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_bytes_empty_file() {
        let result = Tablespace::from_bytes(vec![]);
        match result {
            Err(e) => assert!(
                e.to_string().contains("too small"),
                "Expected 'too small' in: {e}"
            ),
            Ok(_) => panic!("Expected error for empty input"),
        }
    }

    #[test]
    fn test_from_bytes_all_zeros() {
        let data = vec![0u8; PS];
        // All-zeros page should not panic — may succeed or return an error
        let _ = Tablespace::from_bytes(data);
    }

    #[test]
    fn test_from_bytes_for_each_page() {
        let mut data = build_fsp_page(1, 3);
        data.extend_from_slice(&build_index_page(1, 1, 2000));
        data.extend_from_slice(&build_index_page(2, 1, 3000));
        let mut ts = Tablespace::from_bytes(data).unwrap();
        let mut visited = Vec::new();
        ts.for_each_page(|num, _data| {
            visited.push(num);
            Ok(())
        })
        .unwrap();
        assert_eq!(visited, vec![0, 1, 2]);
    }

    #[test]
    fn test_read_all_pages_returns_correct_data() {
        let page0 = build_fsp_page(5, 3);
        let page1 = build_index_page(1, 5, 2000);
        let page2 = build_index_page(2, 5, 3000);
        let tmp = write_pages(&[page0.clone(), page1.clone(), page2.clone()]);
        let mut ts = Tablespace::open(tmp.path()).unwrap();

        let all_data = ts.read_all_pages().unwrap();
        assert_eq!(all_data.len(), PS * 3);

        // Each page slice should match the individual page data
        assert_eq!(&all_data[0..PS], &page0[..]);
        assert_eq!(&all_data[PS..PS * 2], &page1[..]);
        assert_eq!(&all_data[PS * 2..PS * 3], &page2[..]);
    }

    #[test]
    fn test_read_all_pages_matches_read_page() {
        let page0 = build_fsp_page(1, 3);
        let page1 = build_index_page(1, 1, 2000);
        let page2 = build_index_page(2, 1, 3000);
        let tmp = write_pages(&[page0, page1, page2]);
        let mut ts = Tablespace::open(tmp.path()).unwrap();

        let all_data = ts.read_all_pages().unwrap();

        // Verify each page matches read_page
        for page_num in 0..3u64 {
            let individual = ts.read_page(page_num).unwrap();
            let offset = page_num as usize * PS;
            assert_eq!(
                &all_data[offset..offset + PS],
                &individual[..],
                "Page {} data mismatch",
                page_num
            );
        }
    }

    #[test]
    fn test_read_all_pages_from_bytes() {
        let mut data = build_fsp_page(1, 2);
        data.extend_from_slice(&build_index_page(1, 1, 5000));
        let original = data.clone();

        let mut ts = Tablespace::from_bytes(data).unwrap();
        let all_data = ts.read_all_pages().unwrap();
        assert_eq!(all_data, original);
    }

    // ── Mmap tests ─────────────────────────────────────────────────

    #[test]
    fn test_open_mmap_detects_page_size() {
        let tmp = write_pages(&[build_fsp_page(1, 2), build_index_page(1, 1, 2000)]);
        let ts = Tablespace::open_mmap(tmp.path()).unwrap();
        assert_eq!(ts.page_size(), SIZE_PAGE_DEFAULT);
        assert_eq!(ts.page_count(), 2);
    }

    #[test]
    fn test_open_mmap_with_page_size_override() {
        let tmp = write_pages(&[build_fsp_page(1, 2), build_index_page(1, 1, 2000)]);
        let ts = Tablespace::open_mmap_with_page_size(tmp.path(), SIZE_PAGE_DEFAULT).unwrap();
        assert_eq!(ts.page_size(), SIZE_PAGE_DEFAULT);
        assert_eq!(ts.page_count(), 2);
    }

    #[test]
    fn test_open_mmap_read_page_matches_buffered() {
        let tmp = write_pages(&[build_fsp_page(5, 2), build_index_page(1, 5, 9999)]);

        let mut ts_buffered = Tablespace::open(tmp.path()).unwrap();
        let mut ts_mmap = Tablespace::open_mmap(tmp.path()).unwrap();

        for page_num in 0..2u64 {
            let buf_page = ts_buffered.read_page(page_num).unwrap();
            let mmap_page = ts_mmap.read_page(page_num).unwrap();
            assert_eq!(
                buf_page, mmap_page,
                "Page {} data mismatch between buffered and mmap",
                page_num
            );
        }
    }

    #[test]
    fn test_open_mmap_read_all_pages_matches_buffered() {
        let tmp = write_pages(&[
            build_fsp_page(1, 3),
            build_index_page(1, 1, 2000),
            build_index_page(2, 1, 3000),
        ]);

        let mut ts_buffered = Tablespace::open(tmp.path()).unwrap();
        let mut ts_mmap = Tablespace::open_mmap(tmp.path()).unwrap();

        let buf_data = ts_buffered.read_all_pages().unwrap();
        let mmap_data = ts_mmap.read_all_pages().unwrap();
        assert_eq!(buf_data, mmap_data);
    }

    #[test]
    fn test_open_mmap_for_each_page_visits_all() {
        let tmp = write_pages(&[
            build_fsp_page(1, 3),
            build_index_page(1, 1, 2000),
            build_index_page(2, 1, 3000),
        ]);
        let mut ts = Tablespace::open_mmap(tmp.path()).unwrap();
        let mut visited = Vec::new();
        ts.for_each_page(|num, _data| {
            visited.push(num);
            Ok(())
        })
        .unwrap();
        assert_eq!(visited, vec![0, 1, 2]);
    }

    #[test]
    fn test_open_mmap_rejects_too_small_file() {
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(&[0u8; 10]).unwrap();
        tmp.flush().unwrap();
        let result = Tablespace::open_mmap(tmp.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_open_mmap_fsp_header_matches_buffered() {
        let tmp = write_pages(&[build_fsp_page(42, 2), build_index_page(1, 42, 9000)]);

        let ts_buffered = Tablespace::open(tmp.path()).unwrap();
        let ts_mmap = Tablespace::open_mmap(tmp.path()).unwrap();

        let fsp_buf = ts_buffered.fsp_header().unwrap();
        let fsp_mmap = ts_mmap.fsp_header().unwrap();

        assert_eq!(fsp_buf.space_id, fsp_mmap.space_id);
        assert_eq!(fsp_buf.size, fsp_mmap.size);
        assert_eq!(fsp_buf.flags, fsp_mmap.flags);
    }

    #[test]
    fn test_open_mmap_page_out_of_range() {
        let tmp = write_pages(&[build_fsp_page(1, 1)]);
        let mut ts = Tablespace::open_mmap(tmp.path()).unwrap();
        assert!(ts.read_page(99).is_err());
    }
}
