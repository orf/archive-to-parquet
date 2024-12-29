use anyreader::test::{bz2_data, gzip_data, xz_data, zstd_data};
use anyreader::AnyReader;
use anyreader::{AnyFormat, FormatKind};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

fn make_compression() -> Vec<(FormatKind, Vec<u8>)> {
    let test_data_file = include_bytes!("bench_detection.rs");
    vec![
        (FormatKind::Gzip, gzip_data(test_data_file)),
        (FormatKind::Zstd, zstd_data(test_data_file)),
        (FormatKind::Bzip2, bz2_data(test_data_file)),
        (FormatKind::Xz, xz_data(test_data_file)),
        (FormatKind::Unknown, test_data_file.to_vec()),
    ]
}

fn make_archive() -> Vec<(FormatKind, &'static str, Vec<u8>)> {
    let test_data_zip_archive = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../test_data/gping/archive.zip"
    ))
    .unwrap();
    let test_data_tar_archive = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../test_data/gping/archive.tar"
    ))
    .unwrap();
    vec![
        (FormatKind::Tar, "tar.gz", gzip_data(&test_data_tar_archive)),
        (
            FormatKind::Tar,
            "tar.zst",
            zstd_data(&test_data_tar_archive),
        ),
        (FormatKind::Tar, "tar.bz2", bz2_data(&test_data_tar_archive)),
        (FormatKind::Tar, "tar.xz", xz_data(&test_data_tar_archive)),
        (FormatKind::Zip, "zip", test_data_zip_archive),
        (FormatKind::Tar, "tar", test_data_tar_archive),
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
