use crate::anyreader_walker::entry::FileEntry;
use crate::anyreader_walker::stack::AnyWalker;
use crate::anyreader_walker::walkers::ArchiveVisitor;
use std::io::Read;

pub struct FileWalker<T: Read> {
    entry: FileEntry<T>,
}

impl<T: Read> FileWalker<T> {
    pub fn new(entry: FileEntry<T>) -> Self {
        Self { entry }
    }
}

impl<'a, T: Read + 'a> ArchiveVisitor<'a> for FileWalker<T> {
    type Item = T;

    fn visit<V: AnyWalker>(self, visitor: &mut V) -> std::io::Result<()> {
        visitor.walk(self.entry)
    }
}
