use anyreader::test::{bz2_data, gzip_data, xz_data, zstd_data};
use anyreader::AnyReader;
use anyreader::{AnyFormat, FormatKind};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

const TEST_DATA_TAR_ARCHIVE: &[u8] = include_bytes!("../../../test_data/gping/archive.tar");
const TEST_DATA_ZIP_ARCHIVE: &[u8] = include_bytes!("../../../test_data/gping/archive.zip");
const TEST_DATA_FILE: &[u8] = include_bytes!("bench_detection.rs");

fn make_compression() -> Vec<(FormatKind, Vec<u8>)> {
    vec![
        (FormatKind::Gzip, gzip_data(TEST_DATA_FILE)),
        (FormatKind::Zstd, zstd_data(TEST_DATA_FILE)),
        (FormatKind::Bzip2, bz2_data(TEST_DATA_FILE)),
        (FormatKind::Xz, xz_data(TEST_DATA_FILE)),
        (FormatKind::Unknown, TEST_DATA_FILE.to_vec()),
    ]
}

fn make_archive() -> Vec<(FormatKind, &'static str, Vec<u8>)> {
    vec![
        (FormatKind::Zip, "zip", TEST_DATA_ZIP_ARCHIVE.to_vec()),
        (FormatKind::Tar, "tar", TEST_DATA_TAR_ARCHIVE.to_vec()),
        (FormatKind::Tar, "tar.gz", gzip_data(TEST_DATA_TAR_ARCHIVE)),
        (FormatKind::Tar, "tar.zst", zstd_data(TEST_DATA_TAR_ARCHIVE)),
        (FormatKind::Tar, "tar.bz2", bz2_data(TEST_DATA_TAR_ARCHIVE)),
        (FormatKind::Tar, "tar.xz", xz_data(TEST_DATA_TAR_ARCHIVE)),
    ]
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("anyformat/detect");
    group.throughput(Throughput::Elements(1));
    for (format, data) in make_compression() {
        group.bench_function(format.to_string(), |b| {
            b.iter(|| {
                let res = AnyFormat::from_reader(black_box(data.as_slice())).unwrap();
                assert_eq!(res.kind, format);
            })
        });
    }

    for (format, display, data) in make_archive() {
        group.bench_function(display, |b| {
            b.iter(|| {
                let res = AnyFormat::from_reader(black_box(data.as_slice())).unwrap();
                assert_eq!(res.kind, format);
            })
        });
    }
    group.finish();

    let mut group = c.benchmark_group("compression/detect");
    group.throughput(Throughput::Elements(1));
    for (format, data) in make_compression() {
        group.bench_function(format.to_string(), |b| {
            b.iter(|| {
                let res = AnyReader::from_reader(black_box(data.as_slice())).unwrap();
                assert_eq!(format, (&res).into());
            })
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
