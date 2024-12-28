use crate::AnyReader;
use peekable::Peekable;
use std::fmt::{Debug, Formatter};
use std::io;
use std::io::Read;
use tracing::trace;

/// A reader that contains a detected file format.
///
/// ## Read from compressed formats
/// ```
/// use anyreader::AnyFormat;
/// let compressed_data = zstd::encode_all("hello compressed world".as_bytes(), 1).unwrap();
/// let mut reader = AnyFormat::from_reader(compressed_data.as_slice()).unwrap();
/// assert!(reader.kind.is_zstd());
/// assert_eq!(std::io::read_to_string(reader).unwrap(), "hello compressed world");
/// ```
///
/// ## Detect and read from compressed archive formats
/// ```
/// # fn make_tar_zst_archive(data: &str) -> Vec<u8> {
/// #     let mut builder = tar::Builder::new(Vec::new());
/// #     let mut header = tar::Header::new_gnu();
/// #     header.set_size(data.len() as u64);
/// #     builder.append_data(&mut header, "file-name", data.as_bytes()).unwrap();
/// #     let tar_file = builder.into_inner().unwrap();
/// #     zstd::encode_all(&tar_file[..], 1).unwrap()
/// # }
/// use anyreader::AnyFormat;
/// let tar_gz = make_tar_zst_archive("hello tar world");
/// let mut reader = AnyFormat::from_reader(tar_gz.as_slice()).unwrap();
/// assert!(reader.kind.is_tar());
/// let mut archive = tar::Archive::new(reader);
/// let mut entry = archive.entries().unwrap().next().unwrap().unwrap();
/// assert_eq!(std::io::read_to_string(entry).unwrap(), "hello tar world");
/// ```
pub struct AnyFormat<T: Read> {
    pub kind: FormatKind,
    reader: Peekable<AnyReader<T>>,
}

impl<T: Read> AnyFormat<T> {
    pub fn from_reader(reader: T) -> io::Result<AnyFormat<T>> {
        const MAX_PEEK_BUFFER_SIZE: usize = 262;

        let compression_reader = AnyReader::from_reader(reader)?;
        let format: FormatKind = (&compression_reader).into();
        let mut reader = Peekable::with_capacity(compression_reader, MAX_PEEK_BUFFER_SIZE);
        reader.fill_peek_buf()?;
        let buf = crate::peek_upto::<MAX_PEEK_BUFFER_SIZE>(&mut reader)?;
        trace!("peeked {} bytes", buf.len());

        let format: FormatKind = if infer::archive::is_tar(buf) {
            FormatKind::Tar
        } else if infer::archive::is_zip(buf) {
            FormatKind::Zip
        } else {
            format
        };

        trace!("format detected: {format:?}");

        Ok(AnyFormat {
            kind: format,
            reader,
        })
    }

    pub fn get_ref(&self) -> &T {
        self.reader.get_ref().1.get_ref()
    }
}

impl<T: Read> Debug for AnyFormat<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnyFormat")
            .field("kind", &self.kind)
            .finish()
    }
}

impl<T: Read> Read for AnyFormat<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }
}

/// Supported file/compression formats.
#[derive(
    Debug,
    Copy,
    Clone,
    Eq,
    PartialEq,
    Hash,
    Default,
    strum::EnumString,
    strum::Display,
    strum::EnumIs,
)]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
pub enum FormatKind {
    /// Gzip compression
    Gzip,
    /// ZStandard compression
    Zstd,
    /// Bzip2 compression
    Bzip2,
    /// XZ compression
    Xz,
    /// Zip archive
    Zip,
    /// Tar archive. Note: this may be compressed with any of the
    /// previous compression formats (i.e. tar.gz, tar.zst, ...)
    Tar,
    /// Unknown format. This is the fallback when the format is not recognized, and
    /// the associated [AnyFormat] will read the data as-is.
    #[default]
    Unknown,
}

impl<T: Read> From<&AnyReader<T>> for FormatKind {
    /// Convert a `CompressionReader` into a `FormatKind`.
    fn from(reader: &AnyReader<T>) -> Self {
        match reader {
            AnyReader::Gzip(_) => FormatKind::Gzip,
            AnyReader::Zst(_) => FormatKind::Zstd,
            AnyReader::Bzip2(_) => FormatKind::Bzip2,
            AnyReader::Xz(_) => FormatKind::Xz,
            AnyReader::Unknown(_) => FormatKind::Unknown,
        }
    }
}

impl<T: Read> From<AnyReader<T>> for FormatKind {
    fn from(reader: AnyReader<T>) -> Self {
        (&reader).into()
    }
}
