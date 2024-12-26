use anyreader::FormatKind;
use anyreader_walker::{AnyWalker, EntryDetails, FileEntry};
use std::io::Read;

struct Visitor;

impl AnyWalker for Visitor {
    fn visit_file_entry(&mut self, entry: &mut FileEntry<impl Read>) -> std::io::Result<()> {
        eprintln!(
            "Found file with format {}: {}",
            entry.format(),
            entry.path().display()
        );
        Ok(())
    }

    fn begin_visit_archive(
        &mut self,
        details: &EntryDetails,
        format: FormatKind,
    ) -> std::io::Result<bool> {
        eprintln!(
            "Found archive with format {}: {}",
            format,
            details.path.display()
        );
        Ok(true)
    }
}

fn main() {
    let entry = FileEntry::from_path("file.tar.gz").unwrap();
    let mut visitor = Visitor {};
    visitor.walk(entry).unwrap();
}
