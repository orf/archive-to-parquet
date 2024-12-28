use anyreader::test::{bz2_data, gzip_data, tar_archive, xz_data, zip_archive, zstd_data};
use anyreader::AnyReader;
use anyreader::{AnyFormat, FormatKind};
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use rand::Rng;
use std::io::sink;

fn make_test_data(size: usize) -> Vec<u8> {
    let mut vec: Vec<_> = (0..size).map(|_| 0).collect();
    rand::thread_rng().fill(&mut vec[..]);
    vec
}

fn make_compression(size: usize) -> Vec<(FormatKind, Vec<u8>)> {
    let test_data = make_test_data(size);
    vec![
        (FormatKind::Gzip, gzip_data(&test_data)),
        (FormatKind::Zstd, zstd_data(&test_data)),
        (FormatKind::Bzip2, bz2_data(&test_data)),
        (FormatKind::Xz, xz_data(&test_data)),
        (FormatKind::Unknown, test_data.clone()),
    ]
}

fn make_archive(size: usize) -> Vec<(FormatKind, &'static str, Vec<u8>)> {
    let test_data = make_test_data(size);
    let tar_archive = tar_archive([("foo", &test_data)]);
    vec![
        (FormatKind::Zip, "zip", zip_archive([("foo", &test_data)])),
        (FormatKind::Tar, "tar", tar_archive.clone()),
        (FormatKind::Tar, "tar.gz", gzip_data(tar_archive.clone())),
        (FormatKind::Tar, "tar.zst", zstd_data(tar_archive.clone())),
        (FormatKind::Tar, "tar.bz2", bz2_data(tar_archive.clone())),
        (FormatKind::Tar, "tar.xz", xz_data(tar_archive.clone())),
    ]
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("anyformat/detect");
    for size in [1024] {
        group.throughput(Throughput::Elements(1));
        for (format, data) in make_compression(size) {
            group.bench_function(BenchmarkId::new(format!("{format:?}"), size), |b| {
                b.iter(|| {
                    let res = AnyFormat::from_reader(black_box(data.as_slice())).unwrap();
                    assert_eq!(res.kind, format);
                })
            });
        }

        for (format, display, data) in make_archive(size) {
            group.bench_function(BenchmarkId::new(display, size), |b| {
                b.iter(|| {
                    let res = AnyFormat::from_reader(black_box(data.as_slice())).unwrap();
                    assert_eq!(res.kind, format);
                })
            });
        }
    }
    group.finish();

    let mut group = c.benchmark_group("compression/detect");
    for size in [1024] {
        group.throughput(Throughput::Elements(1));
        for (format, data) in make_compression(size) {
            group.bench_function(BenchmarkId::new(format!("{format:?}"), size), |b| {
                b.iter(|| {
                    let res = AnyReader::from_reader(black_box(data.as_slice())).unwrap();
                    assert_eq!(format, (&res).into());
                })
            });
        }
    }
    group.finish();

    let mut group = c.benchmark_group("compression/read");
    for size in [64, 1024, 4096, 16384, 65536] {
        group.throughput(Throughput::BytesDecimal(size as u64));
        for (format, data) in make_compression(size) {
            group.bench_function(BenchmarkId::new(format!("{format:?}"), size), |b| {
                b.iter_batched(
                    || AnyFormat::from_reader(black_box(data.as_slice())).unwrap(),
                    |mut r| {
                        assert_eq!(std::io::copy(&mut r, &mut sink()).unwrap(), size as u64);
                    },
                    BatchSize::PerIteration,
                )
            });
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
