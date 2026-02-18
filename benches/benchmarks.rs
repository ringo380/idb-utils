//! Criterion benchmarks for idb-utils core operations.
//!
//! Benchmarks cover:
//! - Page header parsing (FilHeader::parse)
//! - Checksum validation (CRC-32C, legacy InnoDB, MariaDB full_crc32)
//! - Full tablespace scan (Tablespace::from_bytes + for_each_page)
//! - Real fixture parsing (MySQL 9.0 multipage tablespace)
//! - SDI extraction (find_sdi_pages + extract_sdi_from_pages)

use byteorder::{BigEndian, ByteOrder};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use idb::innodb::checksum::validate_checksum;
use idb::innodb::constants::*;
use idb::innodb::page::FilHeader;
use idb::innodb::sdi::{extract_sdi_from_pages, find_sdi_pages};
use idb::innodb::tablespace::Tablespace;
use idb::innodb::vendor::{MariaDbFormat, VendorInfo};

const PAGE_SIZE: u32 = 16384;
const PS: usize = PAGE_SIZE as usize;

// ---------------------------------------------------------------------------
// Synthetic page builders (mirrors integration test helpers)
// ---------------------------------------------------------------------------

/// Build a minimal valid FSP_HDR page (page 0) with CRC-32C checksum.
fn build_fsp_hdr_page(space_id: u32, total_pages: u32) -> Vec<u8> {
    let mut page = vec![0u8; PS];

    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], 0);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 1000);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 8); // FSP_HDR
    BigEndian::write_u64(&mut page[FIL_PAGE_FILE_FLUSH_LSN..], 1000);
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

    let fsp = FIL_PAGE_DATA;
    BigEndian::write_u32(&mut page[fsp + FSP_SPACE_ID..], space_id);
    BigEndian::write_u32(&mut page[fsp + FSP_SIZE..], total_pages);
    BigEndian::write_u32(&mut page[fsp + FSP_FREE_LIMIT..], total_pages);
    BigEndian::write_u32(&mut page[fsp + FSP_SPACE_FLAGS..], 0);

    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], 1000);

    write_crc32c_checksum(&mut page);
    page
}

/// Build a minimal INDEX page with CRC-32C checksum.
fn build_index_page(page_num: u32, space_id: u32, lsn: u64) -> Vec<u8> {
    let mut page = vec![0u8; PS];

    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], lsn);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

    let ph = FIL_PAGE_DATA;
    BigEndian::write_u16(&mut page[ph + PAGE_N_DIR_SLOTS..], 2);
    BigEndian::write_u16(&mut page[ph + PAGE_N_HEAP..], 0x8002);
    BigEndian::write_u16(&mut page[ph + PAGE_N_RECS..], 0);
    BigEndian::write_u16(&mut page[ph + PAGE_LEVEL..], 0);
    BigEndian::write_u64(&mut page[ph + PAGE_INDEX_ID..], 42);

    let trailer = PS - SIZE_FIL_TRAILER;
    BigEndian::write_u32(&mut page[trailer + 4..], (lsn & 0xFFFFFFFF) as u32);

    write_crc32c_checksum(&mut page);
    page
}

/// Build a page with MariaDB full_crc32 checksum format.
fn build_mariadb_full_crc32_page(page_num: u32, space_id: u32) -> Vec<u8> {
    let mut page = vec![0xABu8; PS];

    BigEndian::write_u32(&mut page[FIL_PAGE_OFFSET..], page_num);
    BigEndian::write_u32(&mut page[FIL_PAGE_PREV..], FIL_NULL);
    BigEndian::write_u32(&mut page[FIL_PAGE_NEXT..], FIL_NULL);
    BigEndian::write_u64(&mut page[FIL_PAGE_LSN..], 5000);
    BigEndian::write_u16(&mut page[FIL_PAGE_TYPE..], 17855); // INDEX
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_ID..], space_id);

    // MariaDB full_crc32: checksum = CRC-32C over [0..page_size-4), stored in last 4 bytes
    let crc = crc32c::crc32c(&page[0..PS - 4]);
    BigEndian::write_u32(&mut page[PS - 4..], crc);

    page
}

/// Calculate and write CRC-32C into the checksum field (bytes 0-3).
fn write_crc32c_checksum(page: &mut [u8]) {
    let end = PS - SIZE_FIL_TRAILER;
    let crc1 = crc32c::crc32c(&page[FIL_PAGE_OFFSET..FIL_PAGE_FILE_FLUSH_LSN]);
    let crc2 = crc32c::crc32c(&page[FIL_PAGE_DATA..end]);
    BigEndian::write_u32(&mut page[FIL_PAGE_SPACE_OR_CHKSUM..], crc1 ^ crc2);
}

/// Build a synthetic tablespace with `n` pages (1 FSP_HDR + (n-1) INDEX pages).
fn build_synthetic_tablespace(num_pages: u32) -> Vec<u8> {
    let mut data = build_fsp_hdr_page(1, num_pages);
    for i in 1..num_pages {
        data.extend_from_slice(&build_index_page(i, 1, 1000 + i as u64));
    }
    data
}

// ---------------------------------------------------------------------------
// Benchmark: FilHeader::parse
// ---------------------------------------------------------------------------

fn bench_fil_header_parse(c: &mut Criterion) {
    let page = build_index_page(1, 1, 5000);

    c.bench_function("fil_header_parse_single_page", |b| {
        b.iter(|| {
            black_box(FilHeader::parse(black_box(&page)).unwrap());
        });
    });
}

fn bench_fil_header_parse_tablespace(c: &mut Criterion) {
    let mut group = c.benchmark_group("fil_header_parse_tablespace");

    for num_pages in [64u32, 640, 6400] {
        let data = build_synthetic_tablespace(num_pages);
        group.throughput(Throughput::Elements(num_pages as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_pages}_pages")),
            &data,
            |b, data| {
                b.iter(|| {
                    for i in 0..num_pages as usize {
                        let offset = i * PS;
                        let page_data = &data[offset..offset + PS];
                        black_box(FilHeader::parse(page_data));
                    }
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: Single-page checksum validation (all 3 algorithms)
// ---------------------------------------------------------------------------

fn bench_checksum_single_page(c: &mut Criterion) {
    let mut group = c.benchmark_group("checksum_single_page");

    // CRC-32C page
    let crc32c_page = build_index_page(1, 1, 5000);
    group.bench_function("crc32c", |b| {
        b.iter(|| {
            black_box(validate_checksum(
                black_box(&crc32c_page),
                PAGE_SIZE,
                None,
            ));
        });
    });

    // Legacy InnoDB page: build one where the stored checksum matches legacy
    // We use a CRC-32C page here and measure the validation path that tries
    // CRC-32C first (which is the common fast path).
    group.bench_function("crc32c_then_legacy_fallback", |b| {
        // Corrupt the CRC-32C so it falls through to legacy check
        let mut legacy_page = build_index_page(1, 1, 5000);
        // Store a value that won't match CRC-32C but also won't match legacy
        // This exercises the full validation path
        BigEndian::write_u32(&mut legacy_page[FIL_PAGE_SPACE_OR_CHKSUM..], 0x12345678);
        b.iter(|| {
            black_box(validate_checksum(
                black_box(&legacy_page),
                PAGE_SIZE,
                None,
            ));
        });
    });

    // MariaDB full_crc32 page
    let mariadb_page = build_mariadb_full_crc32_page(1, 1);
    let mariadb_vendor = VendorInfo::mariadb(MariaDbFormat::FullCrc32);
    group.bench_function("mariadb_full_crc32", |b| {
        b.iter(|| {
            black_box(validate_checksum(
                black_box(&mariadb_page),
                PAGE_SIZE,
                Some(&mariadb_vendor),
            ));
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: Checksum validation across tablespace
// ---------------------------------------------------------------------------

fn bench_checksum_tablespace(c: &mut Criterion) {
    let mut group = c.benchmark_group("checksum_tablespace");

    for num_pages in [64u32, 640, 6400] {
        let data = build_synthetic_tablespace(num_pages);
        group.throughput(Throughput::Elements(num_pages as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_pages}_pages")),
            &data,
            |b, data| {
                b.iter(|| {
                    for i in 0..num_pages as usize {
                        let offset = i * PS;
                        let page_data = &data[offset..offset + PS];
                        black_box(validate_checksum(page_data, PAGE_SIZE, None));
                    }
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: Full tablespace scan (Tablespace::from_bytes + for_each_page)
// ---------------------------------------------------------------------------

fn bench_tablespace_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("tablespace_scan");

    for num_pages in [64u32, 640, 6400] {
        let data = build_synthetic_tablespace(num_pages);
        let data_size = data.len() as u64;
        group.throughput(Throughput::Bytes(data_size));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_pages}_pages")),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut ts = Tablespace::from_bytes(data.clone()).unwrap();
                    ts.for_each_page(|_num, page_data| {
                        black_box(FilHeader::parse(page_data));
                        Ok(())
                    })
                    .unwrap();
                });
            },
        );
    }
    group.finish();
}

fn bench_tablespace_scan_with_checksum(c: &mut Criterion) {
    let mut group = c.benchmark_group("tablespace_scan_with_checksum");

    for num_pages in [64u32, 640, 6400] {
        let data = build_synthetic_tablespace(num_pages);
        let data_size = data.len() as u64;
        group.throughput(Throughput::Bytes(data_size));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_pages}_pages")),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut ts = Tablespace::from_bytes(data.clone()).unwrap();
                    ts.for_each_page(|_num, page_data| {
                        black_box(FilHeader::parse(page_data));
                        black_box(validate_checksum(page_data, PAGE_SIZE, None));
                        Ok(())
                    })
                    .unwrap();
                });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: Real MySQL 9.0 fixture
// ---------------------------------------------------------------------------

fn bench_real_fixture_parse(c: &mut Criterion) {
    let fixture_path = "tests/fixtures/mysql9/mysql90_multipage.ibd";
    let data = match std::fs::read(fixture_path) {
        Ok(d) => d,
        Err(e) => {
            eprintln!(
                "Skipping real fixture benchmark: cannot read {}: {}",
                fixture_path, e
            );
            return;
        }
    };

    let page_count = data.len() / PS;
    let mut group = c.benchmark_group("real_fixture");
    group.throughput(Throughput::Bytes(data.len() as u64));

    group.bench_function(
        format!("parse_{page_count}_page_tablespace"),
        |b| {
            b.iter(|| {
                let mut ts = Tablespace::from_bytes(data.clone()).unwrap();
                ts.for_each_page(|_num, page_data| {
                    black_box(FilHeader::parse(page_data));
                    Ok(())
                })
                .unwrap();
            });
        },
    );

    group.bench_function(
        format!("checksum_{page_count}_page_tablespace"),
        |b| {
            b.iter(|| {
                let mut ts = Tablespace::from_bytes(data.clone()).unwrap();
                ts.for_each_page(|_num, page_data| {
                    black_box(validate_checksum(page_data, PAGE_SIZE, None));
                    Ok(())
                })
                .unwrap();
            });
        },
    );

    group.bench_function(
        format!("full_analysis_{page_count}_page_tablespace"),
        |b| {
            b.iter(|| {
                let mut ts = Tablespace::from_bytes(data.clone()).unwrap();
                ts.for_each_page(|_num, page_data| {
                    black_box(FilHeader::parse(page_data));
                    black_box(validate_checksum(page_data, PAGE_SIZE, None));
                    Ok(())
                })
                .unwrap();
            });
        },
    );

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark: SDI extraction (find_sdi_pages + extract_sdi_from_pages)
// ---------------------------------------------------------------------------

fn bench_sdi_extraction(c: &mut Criterion) {
    let fixture_path = "tests/fixtures/mysql9/mysql90_standard.ibd";
    let data = match std::fs::read(fixture_path) {
        Ok(d) => d,
        Err(e) => {
            eprintln!(
                "Skipping SDI extraction benchmark: cannot read {}: {}",
                fixture_path, e
            );
            return;
        }
    };

    let mut group = c.benchmark_group("sdi_extraction");
    group.throughput(Throughput::Bytes(data.len() as u64));

    group.bench_function("find_sdi_pages", |b| {
        b.iter(|| {
            let mut ts = Tablespace::from_bytes(data.clone()).unwrap();
            black_box(find_sdi_pages(&mut ts).unwrap());
        });
    });

    group.bench_function("find_and_extract_sdi", |b| {
        b.iter(|| {
            let mut ts = Tablespace::from_bytes(data.clone()).unwrap();
            let sdi_pages = find_sdi_pages(&mut ts).unwrap();
            black_box(extract_sdi_from_pages(&mut ts, &sdi_pages).unwrap());
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Group and main
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_fil_header_parse,
    bench_fil_header_parse_tablespace,
    bench_checksum_single_page,
    bench_checksum_tablespace,
    bench_tablespace_scan,
    bench_tablespace_scan_with_checksum,
    bench_real_fixture_parse,
    bench_sdi_extraction,
);
criterion_main!(benches);
