use crate::items::HASH_WIDTH;
use arrow::array::{Array, BooleanArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_select::filter::filter_record_batch;
use foldhash::HashSet;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::BufWriter;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::{debug, info};

const BLOOM_FILTER_FIELDS: &[&str] = &["source", "path", "hash"];
const STATISTICS_FIELDS: &[&str] = &["source", "path", "size", "hash"];
const DICTIONARY_FIELDS: &[&str] = &["source", "path"];

fn make_schema() -> Arc<Schema> {
    let schema = Schema::new([
        Arc::new(Field::new("source", DataType::Utf8View, false)),
        Arc::new(Field::new("path", DataType::Utf8View, false)),
        Arc::new(Field::new("size", DataType::UInt64, false)),
        Arc::new(Field::new("content", DataType::BinaryView, false)),
        Arc::new(Field::new(
            "hash",
            DataType::FixedSizeBinary(HASH_WIDTH),
            false,
        )),
    ]);
    Arc::new(schema)
}

pub struct OutputFileMutable {
    writer: ArrowWriter<BufWriter<File>>,
    total_written: usize,
    seen_hashes: HashSet<[u8; HASH_WIDTH as usize]>,
}

pub struct OutputFile {
    path: PathBuf,
    pub schema: Arc<Schema>,
    mutable: Mutex<OutputFileMutable>,
    unique: bool,
}

impl OutputFile {
    pub fn new(path: PathBuf, unique: bool) -> anyhow::Result<Self> {
        let writer = BufWriter::new(File::create(&path)?);
        let schema = make_schema();

        let mut props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_write_batch_size(1024 * 1024);

        for field in BLOOM_FILTER_FIELDS {
            props = props.set_column_bloom_filter_enabled((*field).into(), true);
        }
        for field in STATISTICS_FIELDS {
            props = props.set_column_statistics_enabled((*field).into(), EnabledStatistics::Chunk);
        }
        for field in DICTIONARY_FIELDS {
            props = props.set_column_dictionary_enabled((*field).into(), true);
        }

        let writer = ArrowWriter::try_new(writer, schema.clone(), Some(props.build()))?;
        let mutable = OutputFileMutable {
            writer,
            total_written: 0,
            seen_hashes: Default::default(),
        };
        Ok(Self {
            path,
            schema,
            mutable: Mutex::new(mutable),
            unique,
        })
    }

    fn exclude_duplicates(batch: RecordBatch, mutable: &mut OutputFileMutable) -> RecordBatch {
        let hash_column = batch
            .column_by_name("hash")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();
        let mut filter_vec = vec![true; hash_column.len()];
        for (idx, row) in hash_column.iter().enumerate() {
            let hash = row.unwrap();
            // If we've seen the hash before, set the row mask to false in order to
            // exclude it from the output
            if mutable.seen_hashes.contains(hash) {
                filter_vec[idx] = false;
            } else {
                mutable.seen_hashes.insert(hash.try_into().unwrap());
            }
        }
        let filter_array = BooleanArray::from(filter_vec);
        let filtered_batch = filter_record_batch(&batch, &filter_array).unwrap();
        let excluded = batch.num_rows() - filtered_batch.num_rows();
        info!(
            "Removed {} duplicate rows out of {}, leaving {}",
            excluded,
            batch.num_rows(),
            filtered_batch.num_rows()
        );
        filtered_batch
    }

    pub fn write_items(&self, mut batch: RecordBatch) -> Result<usize, ParquetError> {
        debug!(rows = batch.num_rows(), "writing");
        let mut mutable = self.mutable.lock().expect("lock poisoned");

        if self.unique {
            batch = Self::exclude_duplicates(batch, mutable.deref_mut());
        }
        let total_rows = batch.num_rows();
        mutable.total_written += total_rows;

        mutable.writer.write(&batch)?;
        debug!(rows = total_rows, "written");
        Ok(total_rows)
    }

    pub fn total_rows_written(&self) -> usize {
        let mutable = self.mutable.lock().expect("lock poisoned");
        mutable.total_written
    }
}

impl Display for OutputFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path.display())
    }
}

impl Drop for OutputFile {
    fn drop(&mut self) {
        self.mutable
            .lock()
            .expect("lock poisoned")
            .writer
            .finish()
            .expect("failed to finish writing");
    }
}
