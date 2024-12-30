use crate::hasher::{HashedWriter, HASH_SIZE};
use crate::{ConvertionOptions, IncludeType};
use anyreader_walker::FileEntry;
use arrow::array::{
    Array, ArrayBuilder, AsArray, BooleanArray, FixedSizeBinaryBuilder, LargeBinaryBuilder,
    PrimitiveBuilder, StringViewBuilder,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt64Type};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use byte_unit::Byte;
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};
use std::ops::Range;
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

#[inline(always)]
fn infallable_copy(reader: &mut impl Read, writer: &mut impl Write) -> u64 {
    const BUFFER_SIZE: usize = 1024 * 8; // 8KB
    let mut buffer = [0u8; BUFFER_SIZE];
    let mut total_bytes = 0;
    loop {
        let Ok(bytes_read) = reader.read(&mut buffer) else {
            trace!("Incomplete read: {total_bytes} read");
            return total_bytes;
        };
        if bytes_read == 0 {
            break;
        }
        writer
            .write_all(&buffer[..bytes_read])
            .expect("Error writing to buffer");
        total_bytes += bytes_read as u64;
    }
    total_bytes
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
    options: ConvertionOptions,
    // target_content_size: Byte,
    total_content_size: Byte,
}

impl OutputBatch {
    pub fn new_with_options(options: ConvertionOptions) -> Self {
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
            options,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    pub fn should_flush(&self) -> bool {
        self.sources.len() >= self.capacity || self.total_content_size >= self.options.batch_size
    }

    pub fn add_record(
        &mut self,
        input_path: &Path,
        source: &Path,
        entry: &mut FileEntry<impl Read>,
    ) -> u64 {
        trace!(path=?entry.path(), size=?entry.size(), "add_record");
        self.sources.append_value(input_path.to_string_lossy());
        self.paths
            .append_value(source.join(entry.path()).to_string_lossy());
        // Copy the data into the buffer, and finish it with appending an empty value.
        let mut hashed_writer = HashedWriter::new(&mut self.content);
        let bytes_written = infallable_copy(entry, &mut hashed_writer);
        let digest = hashed_writer.into_digest();
        self.content.append_value("");
        self.hashes
            .append_value(digest.as_ref())
            .expect("Error appending hash");
        self.sizes.append_value(bytes_written);
        self.total_content_size = (self.total_content_size.as_u64() + bytes_written).into();
        trace!(path=?entry.path(), bytes_written=bytes_written, "record_added");
        bytes_written
    }

    pub fn create_record_batch_and_reset(&mut self) -> Result<RecordBatch, ArrowError> {
        debug!(total_content_size=?self.total_content_size, "create_record_batch_and_reset");
        self.total_content_size = 0u64.into();
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.sources.finish()),
                Arc::new(self.paths.finish()),
                Arc::new(self.sizes.finish()),
                Arc::new(self.hashes.finish()),
                Arc::new(self.content.finish()),
            ],
        )?;
        let batch = match self.options.include {
            IncludeType::All => batch,
            _ => Self::filter_types(self.options.include, batch)?,
        };
        let batch = match &self.options.get_size_range() {
            None => batch,
            Some(size_range) => Self::filter_size(size_range, batch)?,
        };
        Ok(batch)
    }

    #[inline(always)]
    fn is_utf8(v: &[u8]) -> bool {
        simdutf8::basic::from_utf8(v).is_ok()
    }

    fn filter_types(
        include: IncludeType,
        batch: RecordBatch,
    ) -> parquet::errors::Result<RecordBatch> {
        let column = batch.column_by_name("content").unwrap().as_binary::<i64>();
        assert!(!column.is_nullable(), "Content column is nullable");
        let filter_array = match include {
            IncludeType::All => return Ok(batch),
            IncludeType::Text => BooleanArray::from_iter(
                column.iter().map(|path| Some(Self::is_utf8(path.unwrap()))),
            ),
            IncludeType::Binary => BooleanArray::from_iter(
                column
                    .iter()
                    .map(|path| Some(!Self::is_utf8(path.unwrap()))),
            ),
        };
        Ok(filter_record_batch(&batch, &filter_array)?)
    }

    fn filter_size(
        size_range: &Range<Byte>,
        batch: RecordBatch,
    ) -> parquet::errors::Result<RecordBatch> {
        let sizes = batch
            .column_by_name("size")
            .unwrap()
            .as_primitive::<UInt64Type>();
        assert!(!sizes.is_nullable(), "Size column is nullable");
        let filter_array = BooleanArray::from_iter(
            sizes
                .iter()
                .map(|size| Some(size_range.contains(&Byte::from(size.unwrap())))),
        );
        Ok(filter_record_batch(&batch, &filter_array)?)
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
