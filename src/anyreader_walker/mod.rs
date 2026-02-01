mod entry;
mod stack;
#[cfg(test)]
pub(crate) mod tests;
mod utils;
mod walkers;

pub use crate::anyreader::FormatKind;
pub use entry::{EntryDetails, FileEntry};
pub use stack::AnyWalker;
pub use utils::ArchiveStack;
