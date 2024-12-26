use std::path::{Path, PathBuf};

/// A utility struct to keep track of the current archive stack.
/// This is useful when processing nested archives - it supports
/// pushing and popping archives from the stack, and provides the
/// current nested path - including all previous nested paths.
///
/// # Example
/// ```
/// # use std::path::Path;
/// # use anyreader_walker::ArchiveStack;
/// let mut stack = ArchiveStack::new();
/// stack.push_archive("first.tar");
/// stack.push_archive("second.tar");
/// assert_eq!(stack.nested_path(), Path::new("first.tar/second.tar"));
/// assert_eq!(stack.current_depth(), 2);
/// stack.pop_archive();
/// assert_eq!(stack.nested_path(), Path::new("first.tar"));
/// ```
#[derive(Debug, Default)]
pub struct ArchiveStack {
    stack: smallvec::SmallVec<[PathBuf; 6]>,
    nested_path: PathBuf,
}

impl ArchiveStack {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_archive(&mut self, path: impl AsRef<Path>) -> &Path {
        let path = path.as_ref().to_path_buf();
        self.nested_path.push(&path);
        self.stack.push(path);
        &self.nested_path
    }

    pub fn pop_archive(&mut self) -> &Path {
        self.stack.pop();
        self.nested_path = PathBuf::from_iter(self.stack.iter());
        &self.nested_path
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
