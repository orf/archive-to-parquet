use crate::items::{Items, ItemsError};
use crate::Limits;
use flate2::read::GzDecoder;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tracing::{debug, info};

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

#[derive(Debug, derive_more::Display)]
pub enum Format {
    #[display("tar")]
    Tar,
    #[display("tar.gz")]
    TarGz,
    #[display("zip")]
    Zip,
}

impl Format {
    #[tracing::instrument(skip(self, source, reader, items), fields(%self, %source, %limits))]
    pub fn extract(
        &self,
        source: &str,
        reader: impl Read,
        items: &mut Items,
        limits: Limits,
    ) -> Result<usize, ItemsError> {
        let file_count = match self {
            Format::Tar => tar::extract(source, reader, items, limits),
            Format::TarGz => {
                let decoder = GzDecoder::new(reader);
                tar::extract(source, decoder, items, limits)
            }
            Format::Zip => zip::extract(source, reader, items, limits),
        }?;
        info!("Output {file_count} records from {source}");
        Ok(file_count)
    }

    #[inline(always)]
    fn detect_type(mut reader: impl Read, limits: Limits) -> Result<Self, FormatError> {
        let mut buf = [0u8; BUF_SIZE];
        let buf = Self::read_slice(&mut reader, &mut buf, limits)?;
        if Self::is_tar(buf) {
            debug!("Detected tar format");
            return Ok(Self::Tar);
        } else if Self::is_zip(buf) {
            debug!("Detected zip format");
            return Ok(Self::Zip);
        } else if Self::is_tar_gz(buf, limits)? {
            debug!("Detected tar.gz format");
            return Ok(Self::TarGz);
        }
        Err(FormatError::UnsupportedFormat)
    }

    fn is_zip(slice: &[u8]) -> bool {
        infer::archive::is_zip(slice)
    }

    fn is_tar(slice: &[u8]) -> bool {
        infer::archive::is_tar(slice)
    }

    fn is_tar_gz(slice: &[u8], limits: Limits) -> Result<bool, FormatError> {
        if !infer::archive::is_gz(slice) {
            return Ok(false);
        }
        let mut decoder = GzDecoder::new(slice);
        let buf = &mut [0u8; BUF_SIZE];
        let slice = Self::read_slice(&mut decoder, buf, limits)?;
        Ok(Self::is_tar(slice))
    }

    fn read_slice(
        mut reader: impl Read,
        buf: &mut [u8; BUF_SIZE],
        limits: Limits,
    ) -> Result<&[u8], FormatError> {
        let read = reader.read(buf).expect("Error reading");
        if read == 0 {
            return Err(FormatError::Empty);
        } else if (read as u64) < limits.min_file_size {
            return Err(FormatError::UnsupportedFormat);
        }
        Ok(&buf[0..read])
    }

    #[tracing::instrument(name = "from_path", fields(limits=%limits))]
    pub fn try_from_path(path: &Path, limits: Limits) -> Result<Self, FormatError> {
        let reader = File::open(path)?;
        Self::detect_type(reader, limits)
    }
}
