use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::info;

fn make_schema() -> Arc<Schema> {
    let schema = Schema::new([
        Arc::new(Field::new("source", DataType::Utf8View, false)),
        Arc::new(Field::new("path", DataType::Utf8View, false)),
        Arc::new(Field::new("size", DataType::UInt64, false)),
        Arc::new(Field::new("content", DataType::BinaryView, false)),
        Arc::new(Field::new("hash", DataType::FixedSizeBinary(32), false)),
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
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(7)?))
            .set_statistics_enabled(Default::default())
            .set_statistics_truncate_length(Some(1024))
            .set_column_bloom_filter_enabled("hash".into(), true);

        for column in ["source", "path"] {
            props = props
                .set_column_bloom_filter_enabled(column.into(), true)
                .set_column_dictionary_enabled(column.into(), true)
        }

        let writer = ArrowWriter::try_new(writer, schema.clone(), Some(props.build()))?;
        Ok(Self {
            path,
            schema,
            writer: Mutex::new(writer),
        })
    }

    #[tracing::instrument(skip_all, fields(self=%self))]
    pub fn write_items(&mut self, batch: RecordBatch) -> Result<(), ParquetError> {
        info!(rows = batch.num_rows(), "writing");
        let mut writer = self.writer.lock().expect("lock poisoned");
        writer.write(&batch)?;
        info!(rows = batch.num_rows(), "written");
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
