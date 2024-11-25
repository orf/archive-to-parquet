use crate::items::Items;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

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
    schema: Arc<Schema>,
    writer: Mutex<ArrowWriter<BufWriter<File>>>,
}

impl OutputFile {
    pub fn new(path: PathBuf) -> anyhow::Result<Self> {
        let writer = BufWriter::new(File::create(path)?);
        let schema = make_schema();

        let mut props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(10)?))
            .set_statistics_enabled(Default::default())
            .set_statistics_truncate_length(Some(1024));
        for column in ["source", "path", "hash"] {
            props = props
                .set_column_bloom_filter_enabled(column.into(), true)
                .set_column_dictionary_enabled(column.into(), true)
        }

        let writer = ArrowWriter::try_new(writer, schema.clone(), Some(props.build()))?;
        Ok(Self {
            schema,
            writer: Mutex::new(writer),
        })
    }

    pub fn write_items(&mut self, items: Items) -> anyhow::Result<()> {
        let batch = items.into_record_batch(self.schema.clone())?;
        let mut writer = self.writer.lock().expect("lock poisoned");
        writer.write(&batch)?;
        Ok(())
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
