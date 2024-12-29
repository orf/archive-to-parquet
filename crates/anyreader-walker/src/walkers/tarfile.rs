use crate::entry::FileEntry;
use crate::stack::AnyWalker;
use crate::walkers::ArchiveVisitor;
use std::io::Read;

pub struct TarWalker<T: Read> {
    archive: tar::Archive<T>,
}

impl<T: Read> TarWalker<T> {
    pub fn new(reader: T) -> Self {
        Self {
            archive: tar::Archive::new(reader),
        }
    }
}

impl<'a, T: Read + 'a> ArchiveVisitor<'a> for TarWalker<T> {
    type Item = tar::Entry<'a, T>;

    fn visit<V: AnyWalker>(mut self, visitor: &mut V) -> std::io::Result<()> {
        let mut entries = self.archive.entries()?;
        while let Some(Ok(entry)) = entries.next() {
            if entry.header().entry_type() != tar::EntryType::Regular || entry.size() == 0 {
                continue;
            }
            let size = entry.size();
            let path = entry.path()?.to_path_buf();
            let entry = FileEntry::from_reader(path, size, entry)?;
            visitor.walk(entry)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::{assert_visitor_equal, TestVisitor, TEST_DATA};

    use crate::entry::FileEntry;
    use crate::walkers::ArchiveVisitor;
    use anyreader::test::tar_archive;
    use anyreader::FormatKind;
    use std::path::PathBuf;

    #[test]
    fn test_read_tar() {
        let data = tar_archive([("test", TEST_DATA.to_vec())]);
        let entry = FileEntry::from_bytes(PathBuf::from("test"), data).unwrap();
        let mut visitor = TestVisitor::default();

        entry.visit(&mut visitor).unwrap();

        let found = visitor.into_data();
        assert_visitor_equal(
            found,
            vec![(
                FormatKind::Unknown,
                PathBuf::from("test"),
                TEST_DATA.to_vec(),
            )],
        )
    }

    #[test]
    fn test_read_tar_nested() {
        let data = tar_archive([
            ("file", TEST_DATA.to_vec()),
            ("nested", tar_archive(vec![("test", TEST_DATA)])),
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
