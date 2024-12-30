use crate::EntryDetails;
use std::path::{Path, PathBuf};

/// A utility struct to keep track of the current archive stack.
/// This is useful when processing nested archives - it supports
/// pushing and popping archives from the stack, and provides the
/// current nested path - including all previous nested paths.
///
/// # Example
/// ```
/// # use std::path::Path;
/// # use anyreader_walker::{ArchiveStack, EntryDetails};
/// let mut stack = ArchiveStack::new();
/// stack.push_details(EntryDetails::new("first.tar", 5));
/// stack.push_details(EntryDetails::new("second.tar", 10));
/// assert_eq!(stack.nested_path(), Path::new("first.tar/second.tar"));
/// assert_eq!(stack.current_depth(), 2);
/// stack.pop_details();
/// assert_eq!(stack.nested_path(), Path::new("first.tar"));
/// ```
#[derive(Debug, Default)]
pub struct ArchiveStack {
    stack: smallvec::SmallVec<[EntryDetails; 6]>,
    nested_path: PathBuf,
}

impl ArchiveStack {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn last_entry(&self) -> Option<&EntryDetails> {
        self.stack.last()
    }

    pub fn push_details(&mut self, details: EntryDetails) -> &Path {
        self.nested_path.push(&details.path);
        self.stack.push(details);
        &self.nested_path
    }

    pub fn pop_details(&mut self) -> (&Path, Option<EntryDetails>) {
        let finished = self.stack.pop();
        self.nested_path = PathBuf::from_iter(self.stack.iter().map(|d| &d.path));
        (&self.nested_path, finished)
    }

    pub fn current_depth(&self) -> usize {
        self.stack.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stack.is_empty()
    }

    pub fn nested_path(&self) -> &Path {
        &self.nested_path
    }
}
