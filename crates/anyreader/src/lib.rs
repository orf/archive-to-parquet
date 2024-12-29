#![allow(clippy::needless_doctest_main)]
#![doc = include_str!("../README.md")]

///
/// There are two main types in this library:
/// - [AnyReader] - a reader that can read compressed and uncompressed data
/// - [AnyFormat] - a reader with a specific [FormatKind]
mod compression;
mod format;
#[cfg(any(test, feature = "test-utils"))]
pub mod test;

pub use crate::compression::AnyReader;
pub use crate::format::{AnyFormat, FormatKind};
use peekable::Peekable;
use std::io::{Read, Result};

#[inline(always)]
pub(crate) fn peek_upto<const N: usize>(reader: &mut Peekable<impl Read>) -> Result<&[u8]> {
    let buf = reader.get_ref().0;
    let end = N.min(buf.len());
    Ok(&buf[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::{
        assert_data_equal, assert_data_equal_with_msg, bz2_data, gzip_data, read_vec, tar_archive,
        tar_read_entries, xz_data, zip_archive, zstd_data,
    };
    use assert_matches::assert_matches;

    use crate::format::AnyFormat;
    use infer::archive::{is_bz2, is_gz, is_tar, is_xz, is_zip, is_zst};

    use std::io::Cursor;

    pub const TEST_DATA: &[u8] = b"hello world";

    #[test]
    #[allow(clippy::type_complexity)]
    fn test_formats() {
        let test_cases: &[(FormatKind, Vec<u8>, fn(&[u8]) -> bool)] = &[
            (FormatKind::Gzip, gzip_data(TEST_DATA), is_gz),
            (FormatKind::Zstd, zstd_data(TEST_DATA), is_zst),
            (FormatKind::Bzip2, bz2_data(TEST_DATA), is_bz2),
            (FormatKind::Xz, xz_data(TEST_DATA), is_xz),
            (FormatKind::Unknown, TEST_DATA.to_vec(), |_| true),
        ];
        for (expected, data, func) in test_cases {
            assert!(
                func(data.as_slice()),
                "Test data is not recognized as {:?}",
                expected
            );
            let res = AnyFormat::from_reader(data.as_slice()).unwrap();
            let kind = res.kind;
            assert_eq!(kind, *expected);
            assert_data_equal_with_msg(TEST_DATA, read_vec(res), format!("Format: {}", kind));
        }
    }

    #[test]
    fn test_zip() {
        let data = zip_archive(vec![("test", TEST_DATA)]);
        assert!(
            is_zip(data.as_slice()),
            "Test zip data is not recognized as zip"
        );
        let res = AnyFormat::from_reader(data.as_slice()).unwrap();
        assert_matches!(res.kind, FormatKind::Zip);
        let mut archive = zip::ZipArchive::new(Cursor::new(read_vec(res))).unwrap();
        assert_eq!(archive.len(), 1);
        assert_data_equal(TEST_DATA, read_vec(archive.by_name("test").unwrap()));
    }

    #[test]
    fn test_tar() {
        let tar_data = tar_archive(vec![("test", TEST_DATA)]);
        assert!(
            is_tar(tar_data.as_slice()),
            "Test tar data is not recognized as tar"
        );
        for (name, data) in [
            ("tar_data", tar_data.clone()),
            ("gzip_data", gzip_data(&tar_data)),
            ("zstd_data", zstd_data(&tar_data)),
            ("bz2_data", bz2_data(&tar_data)),
            ("xz_data", xz_data(&tar_data)),
        ] {
            let res = AnyFormat::from_reader(data.as_slice()).unwrap();
            assert_matches!(res.kind, FormatKind::Tar);
            let entries = tar_read_entries(res);
            assert_eq!(entries.len(), 1);
            assert_data_equal_with_msg(TEST_DATA, &entries[0], format!("Format: {}", name));
        }
    }

    #[test]
    fn test_nested_tar() {
        let tar_data = tar_archive(vec![("test", tar_archive(vec![("test", TEST_DATA)]))]);
        assert!(
            is_tar(tar_data.as_slice()),
            "Test tar data is not recognized as tar"
        );
        for (name, data) in [
            ("tar_data", tar_data.clone()),
            ("gzip_data", gzip_data(&tar_data)),
            ("zstd_data", zstd_data(&tar_data)),
            ("bz2_data", bz2_data(&tar_data)),
            ("xz_data", xz_data(&tar_data)),
        ] {
            let res = AnyFormat::from_reader(data.as_slice()).unwrap();
            assert_matches!(res.kind, FormatKind::Tar);
            let entries = tar_read_entries(res);
            assert_eq!(entries.len(), 1);
            let inner_archive = tar_read_entries(&mut entries[0].as_slice());
            assert_eq!(inner_archive.len(), 1);
            assert_data_equal_with_msg(TEST_DATA, &inner_archive[0], format!("Format: {}", name));
        }
    }

    #[test]
    fn test_not_unknown() {
        let tar_archive = tar_archive([("foo", TEST_DATA)]);
        let zip_contents = zip_archive([("foo", TEST_DATA)]);

        for (format, data, expected) in [
            ("gzip", gzip_data(TEST_DATA), TEST_DATA),
            ("zstd", zstd_data(TEST_DATA), TEST_DATA),
            ("bz2", bz2_data(TEST_DATA), TEST_DATA),
            ("xz", xz_data(TEST_DATA), TEST_DATA),
            ("tar", tar_archive.clone(), tar_archive.as_slice()),
            (
                "tar.gz",
                gzip_data(tar_archive.clone()),
                tar_archive.as_slice(),
            ),
            (
                "tar.zstd",
                zstd_data(tar_archive.clone()),
                tar_archive.as_slice(),
            ),
            (
                "tar.bz2",
                bz2_data(tar_archive.clone()),
                tar_archive.as_slice(),
            ),
            (
                "tar.xz",
                xz_data(tar_archive.clone()),
                tar_archive.as_slice(),
            ),
            (
                "zip",
                zip_archive([("foo", TEST_DATA)]),
                zip_contents.as_slice(),
            ),
        ] {
            let res = AnyFormat::from_reader(data.as_slice()).unwrap();
            assert_ne!(
                res.kind,
                FormatKind::Unknown,
                "expected {format}, got unknown"
            );
            let res = read_vec(res);
            assert_data_equal_with_msg(res.as_slice(), expected, format!("Format {format}"));
        }
    }
}
