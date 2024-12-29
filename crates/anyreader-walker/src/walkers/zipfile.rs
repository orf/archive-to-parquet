use crate::entry::FileEntry;
use crate::stack::AnyWalker;
use crate::walkers::ArchiveVisitor;
use std::io::Read;
use std::path::PathBuf;
use zip::read::ZipFile;

pub struct ZipWalker<T: Read> {
    archive: T,
}

impl<T: Read> ZipWalker<T> {
    pub fn new(reader: T) -> Self {
        Self { archive: reader }
    }
}

impl<'a, T: Read> ArchiveVisitor<'a> for ZipWalker<T> {
    type Item = ZipFile<'a>;

    fn visit<V: AnyWalker>(mut self, visitor: &mut V) -> std::io::Result<()> {
        while let Ok(Some(entry)) = zip::read::read_zipfile_from_stream(&mut self.archive) {
            if !entry.is_file() || entry.size() == 0 {
                continue;
            }
            let path = PathBuf::from(entry.name());
            let size = entry.size();
            let entry = FileEntry::from_reader(path, size, entry)?;
            visitor.walk(entry)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::{assert_visitor_equal, TestVisitor};

    use crate::entry::FileEntry;
    use crate::walkers::ArchiveVisitor;
    use anyreader::test::zip_archive;
    use anyreader::FormatKind;
    use std::path::PathBuf;

    pub const TEST_DATA: &[u8] = b"hello world";

    #[test]
    fn test_read_zip() {
        let data = zip_archive(vec![("test", TEST_DATA.to_vec())]);
        let entry = FileEntry::from_bytes(PathBuf::from("test"), data).unwrap();
        let mut visitor = TestVisitor::default();

        entry.visit(&mut visitor).unwrap();

        let found = visitor.into_data();
        assert_eq!(
            found,
            vec![(
                FormatKind::Unknown,
                PathBuf::from("test"),
                TEST_DATA.to_vec()
            )]
        )
    }

    #[test]
    fn test_read_zip_nested() {
        let data = zip_archive(vec![
            ("file", TEST_DATA.to_vec()),
            ("nested", zip_archive(vec![("test", TEST_DATA)])),
        ]);
        let entry = FileEntry::from_bytes(PathBuf::from("test"), data).unwrap();
        let mut visitor = TestVisitor::default();
        entry.visit(&mut visitor).unwrap();
        let found = visitor.into_data();

        assert_visitor_equal(
            found,
            vec![
                (
                    FormatKind::Unknown,
                    PathBuf::from("file"),
                    TEST_DATA.to_vec(),
                ),
                (
                    FormatKind::Unknown,
                    PathBuf::from("test"),
                    TEST_DATA.to_vec(),
                ),
            ],
        )
    }
}
