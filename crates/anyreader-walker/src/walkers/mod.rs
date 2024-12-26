mod file;
mod tarfile;
mod zipfile;

use crate::stack::AnyWalker;
pub use file::FileWalker;
use std::io::Read;
pub use tarfile::TarWalker;
pub use zipfile::ZipWalker;

pub trait ArchiveVisitor<'a> {
    type Item: Read + 'a;

    fn visit<V: AnyWalker>(self, visitor: &mut V) -> std::io::Result<()>;
}
