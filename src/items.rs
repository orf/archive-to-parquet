use crate::output::OutputFile;
use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryViewBuilder, FixedSizeBinaryBuilder, PrimitiveBuilder,
    RecordBatch, StringViewBuilder,
};
use arrow::datatypes::UInt64Type;
use arrow::error::ArrowError;
use parquet::errors::ParquetError;
use sha2::{digest::FixedOutputReset, Digest, Sha256};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

pub const HASH_WIDTH: i32 = 32;

#[derive(Debug, thiserror::Error)]
pub enum ItemsError {
    #[error(transparent)]
    Batch(#[from] ArrowError),
    #[error(transparent)]
    Write(#[from] ParquetError),
    #[error(transparent)]
    IO(#[from] std::io::Error),
}

pub struct Items<'a> {
    output_file: &'a OutputFile,
    capacity: usize,
    pub total_written: usize,
    sources: StringViewBuilder,
    paths: StringViewBuilder,
    sizes: PrimitiveBuilder<UInt64Type>,
    data: BinaryViewBuilder,
    text_data: StringViewBuilder,
    hashes: FixedSizeBinaryBuilder,
    only_text: bool,
}

impl<'a> Items<'a> {
    pub fn new_with_capacity(
        output_file: &'a OutputFile,
        capacity: usize,
        only_text: bool,
    ) -> Self {
        Self {
            output_file,
            capacity,
            total_written: 0,
            sources: StringViewBuilder::with_capacity(capacity).with_deduplicate_strings(),
            paths: StringViewBuilder::with_capacity(capacity),
            sizes: PrimitiveBuilder::with_capacity(capacity),
            data: BinaryViewBuilder::with_capacity(capacity),
            text_data: StringViewBuilder::with_capacity(capacity),
            hashes: FixedSizeBinaryBuilder::with_capacity(capacity, HASH_WIDTH),
            only_text,
        }
    }

    pub fn add_record(
        &mut self,
        source: impl AsRef<str>,
        paths: impl AsRef<str>,
        size: u64,
        data: &[u8],
    ) -> Result<(), ItemsError> {
        assert!(!self.only_text, "add_record called when only_text is true");

        self.sources.append_value(source.as_ref());
        self.paths.append_value(paths.as_ref());
        self.sizes.append_value(size);
        self.data.append_value(data);
        if self.sources.len() >= self.capacity {
            self.flush()?;
        }
        Ok(())
    }

    pub fn add_text_record(
        &mut self,
        source: impl AsRef<str>,
        paths: impl AsRef<str>,
        size: u64,
        data: &str,
    ) -> Result<(), ItemsError> {
        assert!(
            self.only_text,
            "add_text_record called when only_text is false"
        );

        self.sources.append_value(source.as_ref());
        self.paths.append_value(paths.as_ref());
        self.sizes.append_value(size);
        self.text_data.append_value(data);
        if self.sources.len() >= self.capacity {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), ItemsError> {
        if self.sources.len() == 0 {
            return Ok(());
        }
        let batch = self.create_record_batch_and_reset()?;
        self.total_written += batch.num_rows();
        self.output_file.write_items(batch)?;
        Ok(())
    }

    fn hash_iterator(&mut self, data: impl Iterator<Item = Option<impl AsRef<[u8]>>>) {
        let mut hasher = Sha256::new();
        for data in data {
            if let Some(data) = data {
                hasher.update(data);
            }
            let hashed = hasher.finalize_fixed_reset();
            self.hashes
                .append_value(hashed)
                .expect("Error appending hash");
        }
    }

    fn create_record_batch_and_reset(&mut self) -> Result<RecordBatch, ArrowError> {
        let schema = self.output_file.schema.clone();
        let data: ArrayRef = if !self.only_text {
            let binary_data = self.data.finish();
            self.hash_iterator(binary_data.iter());
            Arc::new(binary_data)
        } else {
            let text_data = self.text_data.finish();
            self.hash_iterator(text_data.iter());
            Arc::new(text_data)
        };

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.sources.finish()),
                Arc::new(self.paths.finish()),
                Arc::new(self.sizes.finish()),
                Arc::new(self.hashes.finish()),
                Arc::new(data),
            ],
        )
    }
}

impl Display for Items<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Items (buf: {}/{}, written: {})",
            self.sources.len(),
            self.capacity,
            self.total_written
        ))
    }
}
