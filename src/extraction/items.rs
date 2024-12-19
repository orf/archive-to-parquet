use crate::formats::Counts;
use crate::{ExtractionOptions, OutputSink, OutputWriter};
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BinaryViewBuilder, FixedSizeBinaryBuilder, PrimitiveBuilder,
    RecordBatch, StringViewBuilder,
};
use arrow::datatypes::UInt64Type;
use arrow::error::ArrowError;
use arrow_select::filter::filter;
use parquet::errors::ParquetError;
use ring::digest;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::sync::Arc;
use tracing::trace;

pub(crate) const HASH_WIDTH: i32 = 32;

#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    #[error(transparent)]
    Batch(#[from] ArrowError),
    #[error(transparent)]
    Write(#[from] ParquetError),
    #[error(transparent)]
    IO(#[from] std::io::Error),
}

#[derive(Debug)]
pub struct Items<T: OutputSink> {
    output_file: Arc<OutputWriter<T>>,
    capacity: usize,
    counts: Counts,
    sources: StringViewBuilder,
    paths: StringViewBuilder,
    sizes: PrimitiveBuilder<UInt64Type>,
    data: BinaryViewBuilder,
    text_data: StringViewBuilder,
    hashes: FixedSizeBinaryBuilder,
    options: ExtractionOptions,
}

impl<T: OutputSink> Items<T> {
    pub fn new_with_capacity(
        output_file: Arc<OutputWriter<T>>,
        capacity: usize,
        options: ExtractionOptions,
    ) -> Self {
        Self {
            output_file,
            capacity,
            counts: Counts::default(),
            sources: StringViewBuilder::with_capacity(capacity).with_deduplicate_strings(),
            paths: StringViewBuilder::with_capacity(capacity),
            sizes: PrimitiveBuilder::with_capacity(capacity),
            data: BinaryViewBuilder::with_capacity(capacity),
            text_data: StringViewBuilder::with_capacity(capacity),
            hashes: FixedSizeBinaryBuilder::with_capacity(capacity, HASH_WIDTH),
            options,
        }
    }

    #[inline(always)]
    pub fn add_record(
        &mut self,
        source: &Path,
        paths: impl AsRef<str>,
        size: u64,
        data: &[u8],
    ) -> Result<(), ExtractError> {
        trace!(?source, paths = paths.as_ref(), size, "add_record");
        assert!(
            !self.options.only_text,
            "add_record called when only_text is true"
        );

        self.sources.append_value(source.to_string_lossy());
        self.paths.append_value(paths.as_ref());
        self.sizes.append_value(size);
        self.data.append_value(data);
        if self.sources.len() >= self.capacity {
            self.flush()?;
        }
        Ok(())
    }

    #[inline(always)]
    pub fn add_text_record(
        &mut self,
        source: &Path,
        paths: impl AsRef<str>,
        size: u64,
        data: &str,
    ) -> Result<(), ExtractError> {
        trace!(?source, paths = paths.as_ref(), size, "add_text_record");
        assert!(
            self.options.only_text,
            "add_text_record called when only_text is false"
        );

        self.sources.append_value(source.to_string_lossy());
        self.paths.append_value(paths.as_ref());
        self.sizes.append_value(size);
        self.text_data.append_value(data);
        if self.sources.len() >= self.capacity {
            self.flush()?;
        }
        Ok(())
    }

    #[inline(always)]
    pub fn flush(&mut self) -> Result<(), ExtractError> {
        if self.sources.len() == 0 {
            return Ok(());
        }
        let batch = self.create_record_batch_and_reset()?;
        self.output_file.write_items(batch)?;
        Ok(())
    }

    #[inline(always)]
    fn hash_iterator(
        &mut self,
        data: impl Iterator<Item = Option<impl AsRef<[u8]>>>,
        deduplicate: bool,
    ) -> Result<(), ParquetError> {
        let mut hash_set: HashSet<[u8; HASH_WIDTH as usize]> = HashSet::new();
        for data in data {
            let hashed = digest::digest(
                &digest::SHA256,
                data.expect("Empty hash value passed").as_ref(),
            );
            let digest: [u8; HASH_WIDTH as usize] = hashed.as_ref().try_into().unwrap();
            if !deduplicate {
                self.hashes.append_value(digest)?;
                continue;
            }
            if hash_set.contains(&digest) {
                self.hashes.append_null();
            } else {
                hash_set.insert(digest);
                self.hashes.append_value(digest)?;
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn create_record_batch_and_reset(&mut self) -> Result<RecordBatch, ArrowError> {
        let schema = self.output_file.schema();
        let data: ArrayRef = if !self.options.only_text {
            let binary_data = self.data.finish();
            self.hash_iterator(binary_data.iter(), self.options.unique)?;
            Arc::new(binary_data)
        } else {
            let text_data = self.text_data.finish();
            self.hash_iterator(text_data.iter(), self.options.unique)?;
            Arc::new(text_data)
        };

        let sources = self.sources.finish();
        let paths = self.paths.finish();
        let sizes = self.sizes.finish();
        let hashes = self.hashes.finish();

        if !self.options.unique {
            return RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(sources),
                    Arc::new(paths),
                    Arc::new(sizes),
                    Arc::new(hashes),
                    Arc::new(data),
                ],
            );
        }
        let null_filter_mask = arrow::compute::is_not_null(&hashes)?;
        let filtered_batch = RecordBatch::try_new(
            schema,
            vec![
                filter(&sources, &null_filter_mask)?,
                filter(&paths, &null_filter_mask)?,
                filter(&sizes, &null_filter_mask)?,
                filter(&hashes, &null_filter_mask)?,
                filter(&data, &null_filter_mask)?,
            ],
        )?;
        let filtered_count = hashes.len() - filtered_batch.num_rows();
        trace!("Removed {} duplicate rows", filtered_count);
        self.counts.deduplicated += filtered_count;
        Ok(filtered_batch)
    }

    pub fn counts(&self) -> Counts {
        self.counts
    }
}

impl<T: OutputSink> Display for Items<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Items (buf: {}/{}, counts: {})",
            self.sources.len(),
            self.capacity,
            self.counts
        ))
    }
}
