use crate::anyreader_walker::entry::FileEntry;
use crate::anyreader_walker::stack::AnyWalker;
use crate::anyreader_walker::walkers::ArchiveVisitor;
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

impl<'a, T: Read + 'a> ArchiveVisitor<'a> for ZipWalker<T> {
    type Item = ZipFile<'a, T>;

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
