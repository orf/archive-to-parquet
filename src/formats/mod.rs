use crate::items::{Items, ItemsError};
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
const MIN_FILE_SIZE: usize = BUF_SIZE;

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
    #[tracing::instrument(skip(self, source, reader, items), fields(%self, %source))]
    pub fn extract(
        &self,
        source: &str,
        reader: impl Read,
        items: &mut Items,
        depth: usize,
    ) -> Result<usize, ItemsError> {
        let file_count = match self {
            Format::Tar => tar::extract(source, reader, items, depth),
            Format::TarGz => {
                let decoder = GzDecoder::new(reader);
                tar::extract(source, decoder, items, depth)
            }
            Format::Zip => zip::extract(source, reader, items, depth),
        }?;
        info!("Output {file_count} records from {source}");
        Ok(file_count)
    }

    #[inline(always)]
    fn detect_type(mut reader: impl Read) -> Result<Self, FormatError> {
        let mut buf = [0u8; BUF_SIZE];
        let buf = Self::read_slice(&mut reader, &mut buf)?;
        if Self::is_tar(buf) {
            debug!("Detected tar format");
            return Ok(Self::Tar);
        } else if Self::is_zip(buf) {
            debug!("Detected zip format");
            return Ok(Self::Zip);
        } else if Self::is_tar_gz(buf)? {
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

    fn is_tar_gz(slice: &[u8]) -> Result<bool, FormatError> {
        if !infer::archive::is_gz(slice) {
            return Ok(false);
        }
        let mut decoder = GzDecoder::new(slice);
        let buf = &mut [0u8; BUF_SIZE];
        let slice = Self::read_slice(&mut decoder, buf)?;
        Ok(Self::is_tar(slice))
    }

    fn read_slice(mut reader: impl Read, buf: &mut [u8; BUF_SIZE]) -> Result<&[u8], FormatError> {
        let read = reader.read(buf).expect("Error reading");
        if read == 0 {
            return Err(FormatError::Empty);
        } else if read < MIN_FILE_SIZE {
            return Err(FormatError::UnsupportedFormat);
        }
        Ok(&buf[0..read])
    }
}

impl TryFrom<&Path> for Format {
    type Error = FormatError;

    #[tracing::instrument(name = "from_path")]
    fn try_from(value: &Path) -> Result<Self, Self::Error> {
        let reader = File::open(value)?;
        Self::detect_type(reader)
    }
}
