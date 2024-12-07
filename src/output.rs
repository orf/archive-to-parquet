use crate::items::HASH_WIDTH;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::debug;

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

pub struct OutputFile {
    path: PathBuf,
    pub schema: Arc<Schema>,
    writer: Mutex<ArrowWriter<BufWriter<File>>>,
}

impl OutputFile {
    pub fn new(path: PathBuf) -> anyhow::Result<Self> {
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
        Ok(Self {
            path,
            schema,
            writer: Mutex::new(writer),
        })
    }

    pub fn write_items(&self, batch: RecordBatch) -> Result<(), ParquetError> {
        debug!(rows = batch.num_rows(), "writing");
        let mut writer = self.writer.lock().expect("lock poisoned");
        writer.write(&batch)?;
        debug!(rows = batch.num_rows(), "written");
        Ok(())
    }
}

impl Display for OutputFile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path.display())
    }
}

impl Drop for OutputFile {
    fn drop(&mut self) {
        self.writer
            .lock()
            .expect("lock poisoned")
            .finish()
            .expect("failed to finish writing");
    }
}
