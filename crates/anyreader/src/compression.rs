use crate::peek_upto;
use flate2::read::GzDecoder;
use peekable::Peekable;
use std::io::{BufReader, Read, Result};

/// A reader that can read from different compression formats
/// ```
/// use anyreader::AnyReader;
///
/// let mut reader = AnyReader::from_reader(b"hello world".as_slice()).unwrap();
/// assert!(reader.is_unknown());
/// assert_eq!(std::io::read_to_string(reader).unwrap(), "hello world");
///
/// let gzip_data = zstd::encode_all("hello compressed world".as_bytes(), 1).unwrap();
/// let mut reader = AnyReader::from_reader(gzip_data.as_slice()).unwrap();
/// assert!(reader.is_zst());
/// assert_eq!(std::io::read_to_string(reader).unwrap(), "hello compressed world");
/// ```
#[derive(strum::EnumIs)]
pub enum AnyReader<T: Read> {
    /// Gzip compressed data
    Gzip(GzDecoder<Peekable<T>>),
    /// Zstandard compressed data
    Zst(zstd::Decoder<'static, BufReader<Peekable<T>>>),
    /// Bzip2 compressed data
    Bzip2(bzip2::read::BzDecoder<Peekable<T>>),
    /// Xz compressed data
    Xz(liblzma::read::XzDecoder<Peekable<T>>),
    /// Unknown: This is the fallback reader when the format is not recognized, and
    /// it will just read the data as is.
    Unknown(Peekable<T>),
}

impl<T: Read> Read for AnyReader<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            AnyReader::Gzip(r) => r.read(buf),
            AnyReader::Zst(r) => r.read(buf),
            AnyReader::Bzip2(r) => r.read(buf),
            AnyReader::Xz(r) => r.read(buf),
            AnyReader::Unknown(r) => r.read(buf),
        }
    }
}

impl<T: Read> AnyReader<T> {
    /// Detect the compression format and create a [AnyReader] for it.
    pub fn from_reader(reader: T) -> Result<AnyReader<T>> {
        const MAX_PEEK_BUFFER_SIZE: usize = 6;

        let mut reader = Peekable::with_capacity(reader, MAX_PEEK_BUFFER_SIZE);
        reader.fill_peek_buf()?;
        let buf = peek_upto::<MAX_PEEK_BUFFER_SIZE>(&mut reader)?;
        tracing::trace!("peeked {} bytes", buf.len());
        if infer::archive::is_gz(buf) {
            tracing::trace!("gz detected");
            let decoder = GzDecoder::new(reader);
            Ok(Self::Gzip(decoder))
        } else if is_zstd(buf) {
            tracing::trace!("zstd detected");
            let decoder = zstd::Decoder::new(reader)?;
            Ok(Self::Zst(decoder))
        } else if infer::archive::is_bz2(buf) {
            tracing::trace!("bz2 detected");
            let decoder = bzip2::read::BzDecoder::new(reader);
            Ok(Self::Bzip2(decoder))
        } else if infer::archive::is_xz(buf) {
            tracing::trace!("xz detected");
            let decoder = liblzma::read::XzDecoder::new_multi_decoder(reader);
            Ok(Self::Xz(decoder))
        } else {
            tracing::trace!("unknown compression");
            Ok(Self::Unknown(reader))
        }
    }

    pub fn get_ref(&self) -> &T {
        let peekable = match self {
            AnyReader::Gzip(r) => r.get_ref(),
            AnyReader::Zst(r) => r.get_ref().get_ref(),
            AnyReader::Bzip2(r) => r.get_ref(),
            AnyReader::Xz(r) => r.get_ref(),
            AnyReader::Unknown(r) => r,
        };
        peekable.get_ref().1
    }
}

fn is_zstd(buffer: &[u8]) -> bool {
    // https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md#zstandard-frames
    // 4 Bytes, little-endian format. Value : 0xFD2FB528

    const SKIPPABLE_FRAME_BASE: u32 = 0x184D2A50;
    const SKIPPABLE_FRAME_MASK: u32 = 0xFFFFFFF0;
    const ZSTD_MAGIC_NUMBER: u32 = 0xFD2FB528;

    if buffer.len() < 4 {
        return false;
    }

    let magic_from_buffer = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
    magic_from_buffer == ZSTD_MAGIC_NUMBER
        || (magic_from_buffer & SKIPPABLE_FRAME_MASK) == SKIPPABLE_FRAME_BASE
}

#[cfg(test)]
mod tests {
    use crate::test::{assert_data_equal, bz2_data, gzip_data, read_vec, xz_data, zstd_data};
    use crate::AnyReader;
    pub const TEST_DATA: &[u8] = b"hello world";

    #[test]
    #[allow(clippy::type_complexity)]
    fn test_compression_reader() {
        let test_cases: &[(Vec<u8>, fn(&AnyReader<&[u8]>) -> bool)] = &[
            (gzip_data(TEST_DATA), |c| c.is_gzip()),
            (zstd_data(TEST_DATA), |c| c.is_zst()),
            (bz2_data(TEST_DATA), |c| c.is_bzip_2()),
            (xz_data(TEST_DATA), |c| c.is_xz()),
            (TEST_DATA.to_vec(), |c| c.is_unknown()),
        ];
        for (data, func) in test_cases {
            let res = AnyReader::from_reader(data.as_slice()).unwrap();
            assert!(func(&res));
            assert_data_equal(read_vec(res), TEST_DATA);
        }
    }
}
