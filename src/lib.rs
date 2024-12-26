extern crate core;

use byte_unit::Byte;
use std::fmt::{Display, Formatter};
use std::num::{NonZero, NonZeroUsize};

mod extraction;
mod formats;

#[cfg(feature = "python")]
mod python;

#[cfg(test)]
mod tests;
mod output;

pub use extraction::{Extractor};
pub use formats::{Counts, ArchiveFormat, DetectionError, DetectedFile};
pub use parquet::basic::Compression as ParquetCompression;
pub use output::*;
pub use extraction::*;

pub fn default_threads() -> NonZeroUsize {
    std::thread::available_parallelism().unwrap_or(NonZero::new(1).unwrap())
}

#[derive(Debug, Copy, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::prelude::pyclass(str, module = "archive_to_parquet")
)]
pub struct ExtractionOptions {
    pub min_file_size: Byte,
    pub max_file_size: Option<Byte>,
    pub max_depth: Option<NonZeroUsize>,
    pub only_text: bool,
    pub threads: NonZeroUsize,
    pub unique: bool,
    pub ignore_unsupported: bool,
    pub compression: ParquetCompression,
}

impl Display for ExtractionOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "ExtractionOptions(min_file_size=\"{:#.1}\",",
            self.min_file_size
        ))?;
        if let Some(max_file_size) = self.max_file_size {
            f.write_fmt(format_args!(" max_file_size=\"{:#.1}\",", max_file_size))?;
        } else {
            f.write_str(" max_file_size=None,")?;
        }
        f.write_fmt(format_args!(
            " max_depth={:?}, only_text={}, threads={}, unique={}, compression={})",
            self.max_depth, self.only_text, self.threads, self.unique, self.compression
        ))
    }
}

impl ExtractionOptions {
    #[inline(always)]
    pub fn decrement_max_depth(self) -> Self {
        let mut cloned = self;
        let max_depth = cloned
            .max_depth
            .expect("decrement_max_depth called with None");
        cloned.max_depth = NonZeroUsize::new(max_depth.get() - 1);
        cloned
    }
}

impl ExtractionOptions {
    #[inline(always)]
    pub fn check_file_size(&self, size: u64) -> bool {
        if size < self.min_file_size || size == 0 {
            return false;
        }
        if let Some(max_file_size) = self.max_file_size {
            if size > max_file_size {
                return false;
            }
        }

        true
    }
}
