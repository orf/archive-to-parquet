use byte_unit::{Byte, Unit};
use indicatif::DecimalBytes;
pub use parquet::basic::Compression;
use std::fmt::Display;

mod batch;
mod channel;
mod converter;
mod hasher;
mod progress;
mod sink;
mod visitor;

pub use anyreader_walker::{AnyWalker, ArchiveStack, EntryDetails, FileEntry, FormatKind};
pub use channel::{new_record_batch_channel, RecordBatchChannel};
pub use converter::{Converter, ProgressBarConverter, StandardConverter};
pub use sink::{new_parquet_writer, IncludeType, ParquetSink};
pub use visitor::*;

#[derive(Debug, derive_new::new)]
pub struct ConvertionOptions {
    pub include: IncludeType,
    pub unique: bool,
    pub compression: Compression,
    pub min_size: Option<Byte>,
    pub max_size: Option<Byte>,
    pub batch_count: usize,
    pub batch_size: Byte,
}

impl ConvertionOptions {
    pub const fn const_default() -> Self {
        Self {
            include: IncludeType::All,
            unique: false,
            compression: Compression::SNAPPY,
            min_size: None,
            max_size: None,
            batch_count: 14,
            // Also needs changing in the Args struct inside main.rs
            batch_size: Byte::from_u64_with_unit(100, Unit::MB).unwrap(),
        }
    }
}

impl Display for ConvertionOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConvertionOptions(include={:?}, unique={}, compression={:?}",
            self.include, self.unique, self.compression
        )?;
        if let Some(min_size) = &self.min_size {
            write!(f, ", min_size={}", DecimalBytes(min_size.as_u64()))?;
        } else {
            write!(f, ", min_size=None")?;
        }

        if let Some(max_size) = &self.max_size {
            write!(f, ", max_size={}", DecimalBytes(max_size.as_u64()))?;
        } else {
            write!(f, ", size_range=None")?;
        }
        write!(
            f,
            ", batch_count={}, batch_size={:#.1})",
            self.batch_count,
            DecimalBytes(self.batch_size.as_u64())
        )
    }
}
