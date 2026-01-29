use crate::anyreader_walker::entry::FileEntry;
use crate::anyreader_walker::stack::AnyWalker;
use crate::anyreader_walker::walkers::ArchiveVisitor;
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
