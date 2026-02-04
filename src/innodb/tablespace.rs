use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::innodb::constants::*;
use crate::innodb::page::{FilHeader, FilTrailer, FspHeader};
use crate::IdbError;

/// Represents an open InnoDB tablespace file (.ibd).
pub struct Tablespace {
    file: File,
    file_size: u64,
    page_size: u32,
    page_count: u64,
    fsp_header: Option<FspHeader>,
}

impl Tablespace {
    /// Open an InnoDB tablespace file and auto-detect the page size.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, IdbError> {
        let path = path.as_ref();
        let mut file = File::open(path)
            .map_err(|e| IdbError::Io(format!("Cannot open {}: {}", path.display(), e)))?;

        let file_size = file
            .metadata()
            .map_err(|e| IdbError::Io(format!("Cannot stat {}: {}", path.display(), e)))?
            .len();

        if file_size < SIZE_FIL_HEAD as u64 + FSP_HEADER_SIZE as u64 {
            return Err(IdbError::Parse(format!(
                "File too small to be a valid tablespace: {} bytes",
                file_size
            )));
        }

        // Read the first page (at least FIL header + FSP header area) to detect page size
        // We read a full default-size page to be safe
        let initial_read_size = std::cmp::min(file_size, SIZE_PAGE_DEFAULT as u64) as usize;
        let mut buf = vec![0u8; initial_read_size];
        file.read_exact(&mut buf)
            .map_err(|e| IdbError::Io(format!("Cannot read page 0: {}", e)))?;

        // Parse FSP header from page 0 to detect page size
        let fsp_header = FspHeader::parse(&buf);
        let page_size = match &fsp_header {
            Some(fsp) => {
                let detected = fsp.page_size_from_flags();
                // Validate the detected page size
                if matches!(detected, 4096 | 8192 | 16384 | 32768 | 65536) {
                    detected
                } else {
                    SIZE_PAGE_DEFAULT
                }
            }
            None => SIZE_PAGE_DEFAULT,
        };

        let page_count = file_size / page_size as u64;

        Ok(Tablespace {
            file,
            file_size,
            page_size,
            page_count,
            fsp_header,
        })
    }

    /// Open with a specific page size (bypass auto-detection).
    pub fn open_with_page_size<P: AsRef<Path>>(
        path: P,
        page_size: u32,
    ) -> Result<Self, IdbError> {
        let path = path.as_ref();
        let mut file = File::open(path)
            .map_err(|e| IdbError::Io(format!("Cannot open {}: {}", path.display(), e)))?;

        let file_size = file
            .metadata()
            .map_err(|e| IdbError::Io(format!("Cannot stat {}: {}", path.display(), e)))?
            .len();

        // Read page 0 for FSP header
        let initial_read_size = std::cmp::min(file_size, page_size as u64) as usize;
        let mut buf = vec![0u8; initial_read_size];
        file.read_exact(&mut buf)
            .map_err(|e| IdbError::Io(format!("Cannot read page 0: {}", e)))?;

        let fsp_header = FspHeader::parse(&buf);
        let page_count = file_size / page_size as u64;

        Ok(Tablespace {
            file,
            file_size,
            page_size,
            page_count,
            fsp_header,
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

    /// Read a single page by page number into a newly allocated buffer.
    pub fn read_page(&mut self, page_num: u64) -> Result<Vec<u8>, IdbError> {
        if page_num >= self.page_count {
            return Err(IdbError::Parse(format!(
                "Page {} out of range (tablespace has {} pages)",
                page_num, self.page_count
            )));
        }

        let offset = page_num * self.page_size as u64;
        let mut buf = vec![0u8; self.page_size as usize];

        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| IdbError::Io(format!("Cannot seek to page {}: {}", page_num, e)))?;

        self.file
            .read_exact(&mut buf)
            .map_err(|e| IdbError::Io(format!("Cannot read page {}: {}", page_num, e)))?;

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

    /// Iterate over all pages, calling the callback with (page_number, page_data).
    pub fn for_each_page<F>(&mut self, mut callback: F) -> Result<(), IdbError>
    where
        F: FnMut(u64, &[u8]) -> Result<(), IdbError>,
    {
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|e| IdbError::Io(format!("Cannot seek to start: {}", e)))?;

        let mut buf = vec![0u8; self.page_size as usize];
        for page_num in 0..self.page_count {
            self.file
                .read_exact(&mut buf)
                .map_err(|e| IdbError::Io(format!("Cannot read page {}: {}", page_num, e)))?;
            callback(page_num, &buf)?;
        }
        Ok(())
    }
}
