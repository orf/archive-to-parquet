use crate::extraction::{ExtractError, HASH_WIDTH};
use crate::formats::Counts;
use crate::ExtractionOptions;
use arrow::array::{Array, BooleanArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow_select::filter::filter_record_batch;
use foldhash::HashSet;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::errors::ParquetError;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::{debug, info};

const BLOOM_FILTER_FIELDS: &[&str] = &["source", "path", "hash"];
const STATISTICS_FIELDS: &[&str] = &["source", "path", "size", "hash"];
const DICTIONARY_FIELDS: &[&str] = &["source", "path"];

pub(crate) fn make_schema(only_text: bool) -> Arc<Schema> {
    let content_type = if only_text {
        DataType::Utf8View
    } else {
        DataType::BinaryView
    };
    let schema = Schema::new([
        Arc::new(Field::new("source", DataType::Utf8View, false)),
        Arc::new(Field::new("path", DataType::Utf8View, false)),
        Arc::new(Field::new("size", DataType::UInt64, false)),
        Arc::new(Field::new(
            "hash",
            DataType::FixedSizeBinary(HASH_WIDTH),
            false,
        )),
        Arc::new(Field::new("content", content_type, false)),
    ]);
    Arc::new(schema)
}

#[derive(Debug)]
struct OutputFileMutable<T: OutputSink> {
    writer: ArrowWriter<BufWriter<T>>,
    counts: Counts,
    seen_hashes: HashSet<[u8; HASH_WIDTH as usize]>,
}

pub trait OutputSink: Write + Send + Sync + Debug {}
impl<T> OutputSink for T where T: Write + Send + Sync + Debug {}

#[derive(Debug)]
pub struct OutputWriter<T: OutputSink> {
    schema: Arc<Schema>,
    mutable: Mutex<Option<OutputFileMutable<T>>>,
    unique: bool,
    path: Option<PathBuf>,
}

impl OutputWriter<File> {
    pub fn from_path(path: PathBuf, opts: ExtractionOptions) -> anyhow::Result<OutputWriter<File>> {
        let file = File::create(&path)?;
        Self::new(file, Some(path), opts)
    }
}

impl<T: OutputSink> OutputWriter<T> {
    pub fn from_writer(writer: T, opts: ExtractionOptions) -> anyhow::Result<OutputWriter<T>> {
        Self::new(writer, None, opts)
    }

    pub fn from_writer_with_path(
        writer: T,
        opts: ExtractionOptions,
        path: PathBuf,
    ) -> anyhow::Result<OutputWriter<T>> {
        Self::new(writer, Some(path), opts)
    }

    fn new(
        output: T,
        path: Option<PathBuf>,
        opts: ExtractionOptions,
    ) -> anyhow::Result<OutputWriter<T>> {
        let schema = make_schema(opts.only_text);

        let mut props = WriterProperties::builder()
            .set_compression(opts.compression)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_dictionary_enabled(false)
            .set_write_batch_size(1024 * 1024);

        for field in BLOOM_FILTER_FIELDS {
            props = props.set_column_bloom_filter_enabled((*field).into(), true);
        }
        for field in STATISTICS_FIELDS {
            props = props.set_column_statistics_enabled((*field).into(), EnabledStatistics::Page);
        }
        for field in DICTIONARY_FIELDS {
            props = props.set_column_dictionary_enabled((*field).into(), true);
        }

        let output = BufWriter::new(output);
        let writer = ArrowWriter::try_new(output, schema.clone(), Some(props.build()))?;
        let mutable = OutputFileMutable {
            writer,
            counts: Counts::default(),
            seen_hashes: Default::default(),
        };
        Ok(Self {
            schema,
            mutable: Mutex::new(Some(mutable)),
            unique: opts.unique,
            path,
        })
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn exclude_duplicates(
        batch: RecordBatch,
        mutable: &mut OutputFileMutable<T>,
    ) -> Result<RecordBatch, ArrowError> {
        let hash_column = batch
            .column_by_name("hash")
            .expect("hash column not found")
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .expect("hash column not FixedSizeBinaryArray");
        let mut filter_vec = vec![true; hash_column.len()];
        for (idx, row) in hash_column.iter().enumerate() {
            let hash = match row {
                None => {
                    return Err(ArrowError::CastError(format!(
                        "Hash at row {idx} has an empty value"
                    )))
                }
                Some(slice) => slice.try_into().map_err(|e| {
                    ArrowError::CastError(format!("Hash at row {idx} has an invalid value: {e}"))
                })?,
            };
            // If we've seen the hash before, set the row mask to false in order to
            // exclude it from the output
            if mutable.seen_hashes.contains(&hash) {
                filter_vec[idx] = false;
            } else {
                mutable.seen_hashes.insert(hash);
            }
        }
        let filter_array = BooleanArray::from(filter_vec);
        let filtered_batch = filter_record_batch(&batch, &filter_array)?;
        let excluded = batch.num_rows() - filtered_batch.num_rows();
        mutable.counts.deduplicated += excluded;
        info!(
            "Removed {} duplicate rows out of {}, leaving {}",
            excluded,
            batch.num_rows(),
            filtered_batch.num_rows()
        );
        Ok(filtered_batch)
    }

    pub fn write_items(&self, mut batch: RecordBatch) -> Result<usize, ParquetError> {
        debug!(rows = batch.num_rows(), "writing");
        let mut mutable = self.mutable.lock().expect("lock poisoned");
        let Some(mutable) = mutable.deref_mut() else {
            return Err(ParquetError::General("OutputWriter is closed".to_string()));
        };

        if self.unique {
            batch = Self::exclude_duplicates(batch, mutable)?;
        }
        let total_rows = batch.num_rows();
        mutable.counts.written += total_rows;

        mutable.writer.write(&batch)?;
        debug!(rows = total_rows, "written");
        Ok(total_rows)
    }

    pub fn counts(&self) -> Counts {
        let mutable = self.mutable.lock().expect("lock poisoned");
        let mutable = mutable.as_ref().expect("OutputWriter is closed");
        mutable.counts
    }

    pub fn finish(&self) -> Result<T, ExtractError> {
        let mut mutable = self.mutable.lock().expect("lock poisoned");
        let Some(mutable) = mutable.take() else {
            return Err(ParquetError::General("OutputWriter is closed".to_string()).into());
        };

        let sink = mutable.writer.into_inner()?;
        let writer = sink.into_inner().map_err(|e| e.into_error())?;
        Ok(writer)
    }
}

impl<T: OutputSink> Display for OutputWriter<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.path {
            None => write!(f, "OutputFile(impl Write)"),
            Some(p) => write!(f, "OutputFile({:?})", p),
        }
    }
}

impl<T: OutputSink> Drop for OutputWriter<T> {
    fn drop(&mut self) {
        let mut mutable = self.mutable.lock().expect("lock poisoned");
        // Already closed
        let Some(mut mutable) = mutable.take() else {
            return;
        };
        mutable.writer.finish().expect("failed to finish writing");
    }
}
