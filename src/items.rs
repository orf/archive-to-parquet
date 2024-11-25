use arrow::array::{
    Array, BinaryViewBuilder, FixedSizeBinaryBuilder, PrimitiveBuilder, RecordBatch,
    StringViewBuilder,
};
use arrow::datatypes::{SchemaRef, UInt64Type};
use arrow::error::ArrowError;
use sha2::digest::FixedOutputReset;
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub struct Items {
    pub source: String,
    pub paths: StringViewBuilder,
    pub sizes: PrimitiveBuilder<UInt64Type>,
    pub data: BinaryViewBuilder,
    hashes: FixedSizeBinaryBuilder,
}

impl Items {
    pub fn new_with_capacity(source: String, capacity: usize) -> Self {
        Self {
            source,
            paths: StringViewBuilder::with_capacity(capacity),
            sizes: PrimitiveBuilder::with_capacity(capacity),
            data: BinaryViewBuilder::with_capacity(capacity),
            hashes: FixedSizeBinaryBuilder::with_capacity(capacity, 32),
        }
    }
    pub fn into_record_batch(mut self, schema: SchemaRef) -> Result<RecordBatch, ArrowError> {
        let data = self.data.finish();

        let mut hasher = Sha256::new();
        for data in data.iter() {
            if let Some(data) = data {
                hasher.update(data);
            }
            let hashed = hasher.finalize_fixed_reset();
            self.hashes
                .append_value(hashed)
                .expect("Error appending hash");
        }

        // Maybe a better way to do this
        let mut sources = StringViewBuilder::new();
        sources.extend(std::iter::repeat(Some(self.source)).take(data.len()));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(sources.finish()),
                Arc::new(self.paths.finish()),
                Arc::new(self.sizes.finish()),
                Arc::new(data),
                Arc::new(self.hashes.finish()),
            ],
        )
    }
}
