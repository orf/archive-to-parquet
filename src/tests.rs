use crate::extraction::{make_schema, HASH_WIDTH};
use crate::formats::{ArchiveFormat, Counts};
use crate::{ExtractionOptions, Extractor, OutputSink, ParquetCompression};
use arrow::array::{
    BinaryViewBuilder, FixedSizeBinaryBuilder, PrimitiveBuilder, StringViewBuilder,
};
use arrow::compute::SortColumn;
use arrow::datatypes::UInt64Type;
use arrow::record_batch::RecordBatch;
use arrow_schema::{SchemaRef, SortOptions};
use arrow_select::concat::concat_batches;
use arrow_select::take::take;
use bytes::{Buf, Bytes};
use flate2::write::GzEncoder;
use flate2::Compression as GzCompression;
use itertools::Itertools;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use tar::{Builder, Header};
use tracing::trace;
use tracing_test::traced_test;
use zip::write::SimpleFileOptions;

type ArchiveContents<'a> = (ArchiveFormat, &'a Path, Vec<(&'a Path, Bytes)>);

fn default_opts() -> ExtractionOptions {
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

fn make_zip_file(contents: &[(&Path, impl AsRef<[u8]>)]) -> Bytes {
    let options = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let mut a = zip::write::ZipWriter::new(std::io::Cursor::new(vec![]));
    for (name, data) in contents.iter() {
        trace!("adding zip file {:?}", name);
        a.start_file(name.to_string_lossy(), options).unwrap();
        a.write_all(data.as_ref()).unwrap();
    }
    Bytes::from(a.finish().unwrap().into_inner())
}

fn make_tar_file(contents: &[(&Path, impl AsRef<[u8]>)]) -> Bytes {
    let mut a = Builder::new(vec![]);
    for (name, data) in contents.iter() {
        trace!("adding tar file {:?}", name);
        let data = data.as_ref();
        let mut header = Header::new_gnu();
        header.set_size(data.len() as u64);
        a.append_data(&mut header, name, data).unwrap();
    }
    Bytes::from(a.into_inner().unwrap())
}

fn make_extractor(opts: ExtractionOptions, archives: Vec<ArchiveContents>) -> Extractor<Vec<u8>> {
    let mut extractor = Extractor::with_writer(vec![], opts).unwrap();
    for (format, name, files) in archives.iter() {
        let data = match format {
            ArchiveFormat::Tar => make_tar_file(files),
            ArchiveFormat::TarGz => {
                let mut e = GzEncoder::new(Vec::new(), GzCompression::fast());
                e.write_all(make_tar_file(files).as_ref()).unwrap();
                Bytes::from(e.finish().unwrap())
            }
            ArchiveFormat::Zip => make_zip_file(files),
        };
        extractor.add_reader(name.into(), Box::new(data.clone().reader()), *format);
    }
    extractor
}

fn assert_extraction<T: OutputSink>(
    mut extractor: Extractor<T>,
    read: usize,
    skipped: usize,
    deduplicated: usize,
    written: usize,
) -> T {
    let count = extractor.extract().unwrap();
    assert_eq!(
        count,
        Counts {
            read,
            skipped,
            written,
            deduplicated
        }
    );
    extractor.finish().unwrap()
}

fn sort_batch(batch: RecordBatch) -> RecordBatch {
    let indices = arrow::compute::lexsort_to_indices(
        &batch
            .columns()
            .iter()
            .map(|values| SortColumn {
                values: values.clone(),
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
            })
            .collect_vec(),
        None,
    )
    .unwrap();
    let columns = batch
        .columns()
        .iter()
        .map(|c| take(c, &indices, None).unwrap())
        .collect();
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

fn compare_batches(expected: RecordBatch, actual: RecordBatch) {
    assert_eq!(expected.schema(), actual.schema());
    assert_eq!(expected.num_columns(), actual.num_columns());

    let expected = sort_batch(expected);
    let actual = sort_batch(actual);

    for (i, (expected_array, actual_array)) in
        expected.columns().iter().zip(actual.columns()).enumerate()
    {
        assert_eq!(
            expected_array, actual_array,
            "column {}. Diff:\n\nExpected: {:#?}\n\nActual: {:#?}\n\n",
            i, expected, actual
        );
    }
    assert_eq!(expected.num_rows(), actual.num_rows());
}

fn assert_contents(data: Vec<u8>, contents: Vec<ArchiveContents>) {
    let expected_schema = make_schema(false);
    let expected_batch = concat_batches(
        &expected_schema.clone(),
        &contents
            .into_iter()
            .map(|(_, name, contents)| make_batch(name, contents, expected_schema.clone()))
            .collect_vec(),
    )
    .unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data)).unwrap();
    let schema = builder.schema().clone();
    let reader = builder.build().unwrap();
    let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
    let batch = concat_batches(&schema, &batches).unwrap();
    compare_batches(batch, expected_batch)
}

fn make_batch(name: &Path, files: Vec<(&Path, Bytes)>, schema: SchemaRef) -> RecordBatch {
    let mut sources = StringViewBuilder::with_capacity(files.len());
    let mut paths = StringViewBuilder::with_capacity(files.len());
    let mut sizes: PrimitiveBuilder<UInt64Type> = PrimitiveBuilder::with_capacity(files.len());
    let mut hashes = FixedSizeBinaryBuilder::with_capacity(files.len(), HASH_WIDTH);
    let mut contents = BinaryViewBuilder::with_capacity(files.len());

    for (file_name, data) in files.iter() {
        sources.append_value(name.to_string_lossy());
        paths.append_value(file_name.to_string_lossy());
        sizes.append_value(data.as_ref().len() as u64);
        hashes.append_value(Sha256::digest(data.as_ref())).unwrap();
        contents.append_value(data.as_ref());
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(sources.finish()),
            Arc::new(paths.finish()),
            Arc::new(sizes.finish()),
            Arc::new(hashes.finish()),
            Arc::new(contents.finish()),
        ],
    )
    .unwrap()
}

fn b(data: impl AsRef<[u8]>) -> Bytes {
    Bytes::copy_from_slice(data.as_ref())
}

fn p(data: &str) -> &Path {
    Path::new(data)
}

fn test_simple(format: ArchiveFormat, name: &Path) {
    let archives = vec![(format, name, vec![(p("hello-world.txt"), b("hello world"))])];
    let extractor = make_extractor(default_opts(), archives.clone());
    assert_eq!(extractor.input_file_count(), 1);
    let contents = assert_extraction(extractor, 1, 0, 0, 1);
    assert_contents(contents, archives);

    let archives = vec![(format, name, vec![(p("hello-world.txt"), b("hello world"))])];
    let extractor = make_extractor(
        ExtractionOptions {
            min_file_size: 1000u32.into(),
            ..default_opts()
        },
        archives.clone(),
    );
    assert_eq!(extractor.input_file_count(), 1);
    let contents = assert_extraction(extractor, 1, 1, 0, 0);
    assert_contents(contents, vec![]);
}

#[test]
#[traced_test]
fn test_simple_tar() {
    test_simple(ArchiveFormat::Tar, p("test.tar"));
}

#[test]
#[traced_test]
fn test_simple_tar_gz() {
    test_simple(ArchiveFormat::TarGz, p("test.tar.gz"));
}

#[test]
#[traced_test]
fn test_simple_tar_zip() {
    test_simple(ArchiveFormat::Zip, p("test.zip"));
}

#[test]
#[traced_test]
fn test_nested() {
    let inner_contents = vec![
        (
            p("test.zip"),
            make_zip_file(&[(p("hello-world.txt"), b("hello world 1"))]),
        ),
        (
            p("test.tar"),
            make_tar_file(&[(p("hello-world.txt"), b("hello world 2"))]),
        ),
    ];

    for kind in [ArchiveFormat::Tar, ArchiveFormat::TarGz, ArchiveFormat::Zip] {
        let archives = vec![(kind, p("archive"), inner_contents.clone())];
        let extractor = make_extractor(
            ExtractionOptions {
                max_depth: NonZeroUsize::new(1),
                unique: false,
                ..default_opts()
            },
            archives,
        );
        assert_eq!(extractor.input_file_count(), 1);
        let contents = assert_extraction(extractor, 2, 0, 0, 2);
        assert_contents(
            contents,
            vec![
                (
                    kind,
                    &p("archive").join("test.zip"),
                    vec![(p("hello-world.txt"), b("hello world 1"))],
                ),
                (
                    kind,
                    &p("archive").join("test.tar"),
                    vec![(p("hello-world.txt"), b("hello world 2"))],
                ),
            ],
        );
    }
}

#[test]
#[traced_test]
fn test_removes_duplicates() {
    let contents = vec![
        (p("hello.txt"), b("hello world")),
        (p("hello2.txt"), b("hello world")),
    ];
    let extractor = make_extractor(
        ExtractionOptions {
            unique: true,
            ..default_opts()
        },
        vec![
            (ArchiveFormat::Tar, p("test1.tar"), contents.clone()),
            (ArchiveFormat::Tar, p("test2.tar"), contents.clone()),
        ],
    );
    assert_eq!(extractor.input_file_count(), 2);
    let contents = assert_extraction(extractor, 4, 0, 3, 1);
    assert_contents(
        contents,
        vec![(
            ArchiveFormat::Tar,
            p("test1.tar"),
            vec![(p("hello.txt"), b("hello world"))],
        )],
    );
}
