use crate::hasher::{HashedWriter, HASH_SIZE};
use anyreader_walker::FileEntry;
use arrow::array::{
    ArrayBuilder, FixedSizeBinaryBuilder, LargeBinaryBuilder, PrimitiveBuilder, StringViewBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt64Type};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use byte_unit::Byte;
use std::fmt::{Display, Formatter};
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, LazyLock};
use tracing::{debug, trace};

static ARROW_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let schema = Schema::new([
        Arc::new(Field::new("source", DataType::Utf8View, false)),
        Arc::new(Field::new("path", DataType::Utf8View, false)),
        Arc::new(Field::new("size", DataType::UInt64, false)),
        Arc::new(Field::new(
            "hash",
            DataType::FixedSizeBinary(HASH_SIZE as i32),
            false,
        )),
        Arc::new(Field::new("content", DataType::LargeBinary, false)),
    ]);
    Arc::new(schema)
});

pub fn arrow_schema() -> Arc<Schema> {
    (*ARROW_SCHEMA).clone()
}

#[derive(Debug)]
pub struct OutputBatch {
    capacity: usize,
    schema: SchemaRef,
    sources: StringViewBuilder,
    paths: StringViewBuilder,
    sizes: PrimitiveBuilder<UInt64Type>,
    content: LargeBinaryBuilder,
    hashes: FixedSizeBinaryBuilder,
    target_content_size: Byte,
    total_content_size: Byte,
}

impl OutputBatch {
    pub fn new_with_target_size(target_size: Byte) -> Self {
        let capacity = 1024;
        Self {
            capacity,
            schema: arrow_schema(),
            sources: StringViewBuilder::with_capacity(capacity).with_deduplicate_strings(),
            paths: StringViewBuilder::with_capacity(capacity),
            sizes: PrimitiveBuilder::with_capacity(capacity),
            content: LargeBinaryBuilder::with_capacity(capacity, capacity * 1024),
            hashes: FixedSizeBinaryBuilder::with_capacity(capacity, HASH_SIZE as i32),
            total_content_size: 0u64.into(),
            target_content_size: target_size,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    pub fn should_flush(&self) -> bool {
        self.sources.len() >= self.capacity || self.total_content_size >= self.target_content_size
    }

    pub fn add_record(
        &mut self,
        input_path: &Path,
        source: &Path,
        entry: &mut FileEntry<impl Read>,
    ) -> std::io::Result<u64> {
        trace!(path=?entry.path(), size=?entry.size(), "add_record");
        self.sources.append_value(input_path.to_string_lossy());
        self.paths
            .append_value(source.join(entry.path()).to_string_lossy());
        // Copy the data into the buffer, and finish it with appending an empty value.
        let mut hashed_writer = HashedWriter::new(&mut self.content);
        let bytes_written = std::io::copy(entry, &mut hashed_writer)?;
        let digest = hashed_writer.into_digest();
        self.content.append_value("");
        self.hashes
            .append_value(digest.as_ref())
            .expect("Error appending hash");
        self.sizes.append_value(bytes_written);
        self.total_content_size = (self.total_content_size.as_u64() + bytes_written).into();
        trace!(path=?entry.path(), bytes_written=bytes_written, "record_added");
        Ok(bytes_written)
    }

    pub fn create_record_batch_and_reset(&mut self) -> Result<RecordBatch, ArrowError> {
        debug!(total_content_size=?self.total_content_size, "create_record_batch_and_reset");
        self.total_content_size = 0u64.into();
        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.sources.finish()),
                Arc::new(self.paths.finish()),
                Arc::new(self.sizes.finish()),
                Arc::new(self.hashes.finish()),
                Arc::new(self.content.finish()),
            ],
        )
    }
}

impl Display for OutputBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Items (buf: {}/{})",
            self.sources.len(),
            self.capacity,
        ))
    }
}
