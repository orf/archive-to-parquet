use crate::entry::FileEntry;
use crate::stack::AnyWalker;
use crate::EntryDetails;
use anyreader::test::{assert_data_equal_with_msg, read_vec};
use anyreader::FormatKind;
use std::io::Read;
use std::path::PathBuf;

pub const TEST_DATA: &[u8] = b"hello world";

#[derive(Debug, Default)]
pub struct TestVisitor {
    data: Vec<(FormatKind, PathBuf, Vec<u8>)>,
}

impl TestVisitor {
    pub fn into_data(self) -> Vec<(FormatKind, PathBuf, Vec<u8>)> {
        self.data
    }
}

impl AnyWalker for TestVisitor {
    fn visit_file_entry(&mut self, entry: &mut FileEntry<impl Read>) -> std::io::Result<()> {
        let kind = entry.format();
        let path = entry.path().to_path_buf();
        let data = read_vec(entry);
        self.data.push((kind, path, data));
        Ok(())
    }

    fn begin_visit_archive(
        &mut self,
        _details: &EntryDetails,
        _format: FormatKind,
    ) -> std::io::Result<bool> {
        Ok(true)
    }
}

pub fn assert_visitor_equal(
    expected: Vec<(FormatKind, PathBuf, Vec<u8>)>,
    found: Vec<(FormatKind, PathBuf, Vec<u8>)>,
) {
    assert_eq!(expected.len(), found.len());
    for (idx, (e_kind, e_path, e_data)) in expected.iter().enumerate() {
        let (f_kind, f_path, f_data) = &found[idx];
        assert_eq!((e_kind, e_path), (f_kind, f_path), "Index {idx}");
        assert_data_equal_with_msg(e_data, f_data, format!("Index {idx}"));
    }
}
