use crate::entry::{EntryDetails, FileEntry};
use crate::walkers::ArchiveVisitor;
use anyreader::FormatKind;
use std::io::Read;

#[allow(unused_variables)]
pub trait AnyWalker: Sized {
    fn visit_file_entry(&mut self, entry: &mut FileEntry<impl Read>) -> std::io::Result<()>;

    fn begin_visit_archive(
        &mut self,
        details: &EntryDetails,
        format: FormatKind,
    ) -> std::io::Result<bool> {
        Ok(true)
    }

    fn end_visit_archive(
        &mut self,
        details: EntryDetails,
        format: FormatKind,
    ) -> std::io::Result<()> {
        Ok(())
    }

    fn walk(&mut self, mut entry: FileEntry<impl Read>) -> std::io::Result<()> {
        if entry.supports_recursion()
            && self.begin_visit_archive(entry.details(), entry.format())?
        {
            let details = entry.details().clone();
            let format = entry.format();
            entry.visit(self)?;
            self.end_visit_archive(details, format)?;
            Ok(())
        } else {
            self.visit_file_entry(&mut entry)?;
            // std::io::copy(&mut entry, &mut std::io::sink())?;
            Ok(())
        }
    }
}
