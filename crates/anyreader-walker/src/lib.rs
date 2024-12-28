#![allow(clippy::needless_doctest_main)]
#![doc = include_str!("../README.md")]
mod entry;
mod stack;
#[cfg(test)]
mod tests;
mod utils;
mod walkers;

pub use anyreader::AnyFormat;
pub use anyreader::FormatKind;
pub use entry::{EntryDetails, FileEntry};
pub use stack::AnyWalker;
pub use utils::ArchiveStack;
