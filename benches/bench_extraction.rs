extern crate core;

use archive_to_parquet::{
    ArchiveFormat, ExtractionOptions, Extractor, OutputSink, ParquetCompression,
};
use bytes::Bytes;
use criterion::measurement::WallTime;
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkGroup, BenchmarkId, Criterion, Throughput,
};
use itertools::Itertools;
use std::env;
use std::io::sink;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn make_opts() -> ExtractionOptions {
    ExtractionOptions {
        min_file_size: 1u32.into(),
        max_file_size: None,
        max_depth: None,
        only_text: false,
        threads: NonZeroUsize::new(1).unwrap(),
        unique: false,
        ignore_unsupported: false,
        compression: ParquetCompression::UNCOMPRESSED,
    }
}

fn run_test<T: OutputSink>(
    group: &mut BenchmarkGroup<WallTime>,
    name: impl AsRef<str>,
    format: ArchiveFormat,
    data: &Bytes,
    opts: ExtractionOptions,
    extractor_fn: impl Fn(&str, ArchiveFormat, ExtractionOptions) -> Extractor<T>,
) {
    group.bench_function(BenchmarkId::new(name.as_ref(), format.to_string()), |b| {
        b.iter_custom(|iters| {
            let mut duration = Duration::from_secs(0);
            for _i in 0..iters {
                let mut extractor = black_box(extractor_fn(name.as_ref(), format, opts));
                extractor
                    .add_buffer(PathBuf::from("linux"), black_box(data.clone()))
                    .unwrap();
                assert!(extractor.has_input_files());
                let start = Instant::now();
                let counts = extractor.extract().unwrap();
                extractor.finish().unwrap();
                duration += start.elapsed();
                assert!(
                    counts.read > 0 && counts.written > 0,
                    "counts: {:?}",
                    counts
                );
            }
            duration
        });
    });
}

fn run_matrix<T: OutputSink>(
    c: &mut Criterion,
    group: impl AsRef<str>,
    dir: &Path,
    matrix: &[(String, ExtractionOptions)],
    extractor_fn: impl Fn(&str, ArchiveFormat, ExtractionOptions) -> Extractor<T>,
) {
    let mut group = c.benchmark_group(group.as_ref());
    for (format, path) in [
        (ArchiveFormat::TarGz, dir.join("archive.tar.gz")),
        (ArchiveFormat::Tar, dir.join("archive.tar")),
        (ArchiveFormat::Zip, dir.join("archive.zip")),
    ] {
        let data = Bytes::from(std::fs::read(path).unwrap());
        group.throughput(Throughput::Bytes(data.len() as u64));
        // group.sample_size(20);
        // group.nresamples(20);

        for (name, options) in matrix {
            run_test(
                &mut group,
                name, // format!("unique={unique}/only_text={only_text}"),
                format,
                &data,
                *options,
                &extractor_fn,
            );
        }
    }
    group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    let vec_initial_size = 1024 * 1024 * 6;
    let data_dir = Path::new("test_data/gping");

    let unique = [true, false];
    let only_text = [true, false];
    let matrix = unique
        .into_iter()
        .cartesian_product(only_text)
        .map(|(unique, only_text)| {
            (
                format!("unique={}/only_text={}", unique, only_text),
                ExtractionOptions {
                    unique,
                    only_text,
                    ..make_opts()
                },
            )
        })
        .collect_vec();

    let dir = env::temp_dir()
        .join("archive_to_parquet_bench")
        .join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir_all(&dir).unwrap();

    eprintln!("Temp directory: {:?}", dir);

    run_matrix(c, "file", data_dir, &matrix, |name, format, opts| {
        Extractor::with_path(dir.join(format!("{name}-{format}").replace("/", "-")), opts).unwrap()
    });

    run_matrix(c, "sink", data_dir, &matrix, |_, _, opts| {
        Extractor::with_writer(sink(), opts).unwrap()
    });

    run_matrix(c, "vec", data_dir, &matrix, |_, _, opts| {
        Extractor::with_writer(Vec::with_capacity(vec_initial_size), opts).unwrap()
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
