use crate::{ExtractError, ExtractionOptions, Items, OutputSink};
use flate2::read::GzDecoder;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::Read;
use std::iter::Sum;
use std::ops::{Add, AddAssign};
use std::path::{Path, PathBuf};
use tracing::{debug, error, trace};

mod common;
mod tar;
mod zip;

// Tar requires ~260 bytes to detect, gz and zip requires a lot less
const BUF_SIZE: usize = 280;

#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Empty file")]
    Empty,
    #[error("Unsupported format")]
    UnsupportedFormat,
}

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq)]
#[cfg_attr(
    feature = "python",
    pyo3::prelude::pyclass(str, frozen, get_all, module = "archive_to_parquet")
)]
pub struct Counts {
    pub read: usize,
    pub skipped: usize,
    pub deduplicated: usize,
    pub written: usize,
}

impl Counts {
    pub fn new_processed() -> Self {
        Self {
            read: 1,
            written: 1,
            ..Default::default()
        }
    }

    pub fn new_skipped() -> Self {
        Self {
            read: 1,
            skipped: 1,
            ..Default::default()
        }
    }

    pub fn new_deduplicated(deduplicated: usize) -> Self {
        Self {
            deduplicated,
            ..Default::default()
        }
    }

    pub fn skipped(&mut self) {
        self.read += 1;
        self.skipped += 1;
    }
}

impl Display for Counts {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} read, {} written, {} skipped, {} deduplicated",
            self.read, self.written, self.skipped, self.deduplicated
        )
    }
}

impl Add for Counts {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            read: self.read + rhs.read,
            skipped: self.skipped + rhs.skipped,
            deduplicated: self.deduplicated + rhs.deduplicated,
            written: self.written + rhs.written,
        }
    }
}

impl Sum for Counts {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), Add::add)
    }
}

impl AddAssign for Counts {
    fn add_assign(&mut self, rhs: Self) {
        self.read += rhs.read;
        self.written += rhs.written;
        self.skipped += rhs.skipped;
        self.deduplicated += rhs.deduplicated;
    }
}

#[cfg_attr(
    feature = "python",
    pyo3::prelude::pyclass(str, eq, frozen, module = "archive_to_parquet")
)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ArchiveFormat {
    Tar,
    TarGz,
    Zip,
}

impl Display for ArchiveFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ArchiveFormat::Tar => write!(f, "tar"),
            ArchiveFormat::TarGz => write!(f, "tar.gz"),
            ArchiveFormat::Zip => write!(f, "zip"),
        }
    }
}

impl ArchiveFormat {
    #[tracing::instrument(skip_all, fields(%self, ?source))]
    pub fn extract<T: OutputSink>(
        &self,
        source: &PathBuf,
        reader: impl Read,
        items: &mut Items<T>,
        options: ExtractionOptions,
    ) -> Result<Counts, ExtractError> {
        let file_count = match self {
            ArchiveFormat::Tar => tar::extract(source, reader, items, options),
            ArchiveFormat::TarGz => {
                let decoder = GzDecoder::new(reader);
                tar::extract(source, decoder, items, options)
            }
            ArchiveFormat::Zip => zip::extract(source, reader, items, options),
        }
        .map_err(|e| {
            error!("Extraction error: {e}");
            e
        })?;

        debug!("Output {file_count} records from {source:?}");
        Ok(file_count)
    }

    #[inline(always)]
    pub fn detect_type(
        mut reader: impl Read,
        options: ExtractionOptions,
    ) -> Result<Self, FormatError> {
        let mut buf = [0u8; BUF_SIZE];
        let buf = Self::read_slice(&mut reader, &mut buf, options)?;
        if Self::is_tar(buf) {
            debug!("Detected tar format");
            return Ok(Self::Tar);
        } else if Self::is_zip(buf) {
            debug!("Detected zip format");
            return Ok(Self::Zip);
        } else if Self::is_tar_gz(buf, options)? {
            debug!("Detected tar.gz format");
            return Ok(Self::TarGz);
        }
        trace!("Could not detect format");
        Err(FormatError::UnsupportedFormat)
    }

    #[inline(always)]
    fn is_zip(slice: &[u8]) -> bool {
        infer::archive::is_zip(slice)
    }

    #[inline(always)]
    fn is_tar(slice: &[u8]) -> bool {
        infer::archive::is_tar(slice)
    }

    #[inline(always)]
    fn is_tar_gz(slice: &[u8], options: ExtractionOptions) -> Result<bool, FormatError> {
        if !infer::archive::is_gz(slice) {
            return Ok(false);
        }
        let mut decoder = GzDecoder::new(slice);
        let buf = &mut [0u8; BUF_SIZE];
        let slice = Self::read_slice(&mut decoder, buf, options)?;
        Ok(Self::is_tar(slice))
    }

    #[inline(always)]
    fn read_slice(
        mut reader: impl Read,
        buf: &mut [u8; BUF_SIZE],
        options: ExtractionOptions,
    ) -> Result<&[u8], FormatError> {
        let read = reader.read(buf)?;
        if read == 0 {
            return Err(FormatError::Empty);
        } else if (read as u64) < options.min_file_size {
            return Err(FormatError::UnsupportedFormat);
        }
        Ok(&buf[0..read])
    }

    #[tracing::instrument(name = "from_path", skip_all, fields(path=%path.display()))]
    pub fn try_from_path(path: &Path, options: ExtractionOptions) -> Result<Self, FormatError> {
        let reader = File::open(path)?;
        Self::detect_type(reader, options)
    }
}
