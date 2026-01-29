use crate::anyreader_walker::EntryDetails;
use std::path::{Path, PathBuf};

/// A utility struct to keep track of the current archive stack.
/// This is useful when processing nested archives - it supports
/// pushing and popping archives from the stack, and provides the
/// current nested path - including all previous nested paths.
#[derive(Debug, Default)]
pub struct ArchiveStack {
    stack: smallvec::SmallVec<[EntryDetails; 6]>,
}

impl ArchiveStack {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn last_entry(&self) -> Option<&EntryDetails> {
        self.stack.last()
    }

    pub fn push_details(&mut self, details: EntryDetails) {
        self.stack.push(details);
    }

    pub fn pop_details(&mut self) -> Option<EntryDetails> {
        self.stack.pop()
    }

    pub fn current_depth(&self) -> usize {
        self.stack.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stack.is_empty()
    }

    pub fn full_path(&self) -> PathBuf {
        PathBuf::from_iter(self.stack.iter().map(|d| d.path.as_path()))
    }

    pub fn root_path(&self) -> &Path {
        self.stack
            .first()
            .map(|d| d.path.as_path())
            .unwrap_or(Path::new(""))
    }

    pub fn nested_path(&self) -> PathBuf {
        PathBuf::from_iter(self.nested_path_iter())
    }

    pub fn nested_path_iter(&self) -> impl Iterator<Item = &Path> {
        self.stack.iter().skip(1).map(|d| d.path.as_path())
    }
}
