use crate::stack::AnyWalker;
use crate::walkers::{ArchiveVisitor, FileWalker, TarWalker, ZipWalker};
use anyreader::AnyFormat;
use anyreader::FormatKind;
use bytes::buf::Reader;
use bytes::{Buf, Bytes};
use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

/// Represents the details of a [FileEntry], including its path and size.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EntryDetails {
    pub path: PathBuf,
    pub size: u64,
}

impl EntryDetails {
    pub fn new(path: impl Into<PathBuf>, size: u64) -> Self {
        Self {
            path: path.into(),
            size,
        }
    }
}

impl Display for EntryDetails {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({} bytes)", self.path.display(), self.size)
    }
}

/// A [FileEntry] represents a file in an archive, along with its format and size.
/// It can be used to read the file's contents, and can also be used to visit the contents of
/// an archive.
///
/// # Example
/// This walks a nested tar file
/// ```
/// # use std::io::Read;
/// use std::path::{PathBuf, Path};
/// # use anyreader::test::{tar_archive, zip_archive};
/// use anyreader_walker::{FileEntry, AnyWalker, EntryDetails, FormatKind, ArchiveStack};
/// // Create a tar archive containing a nested tar archive, containing a nested zip archive
/// let tar_archive = tar_archive([
///     ("test", b"Hello, world!".to_vec()),
///     ("nested.tar", tar_archive([
///         ("nested", b"Hello, nested!".to_vec()),
///         ("nested2", b"Hello, nested2!".to_vec()),
///         ("nested_zip", zip_archive([("nested3", "Hello, nested zip!")]))
///     ])),
/// ]);
/// let path = Path::new("archive.tar.gz");
/// let entry = FileEntry::from_bytes(&path, tar_archive).unwrap();
///
/// #[derive(Default)]
/// struct Visitor {
///    names: Vec<PathBuf>,
///    stack: ArchiveStack
/// };
///
/// impl AnyWalker for Visitor {
///     fn visit_file_entry(&mut self, entry: &mut FileEntry<impl Read>) -> std::io::Result<()> {
///         self.names.push(self.stack.nested_path().join(&entry.path()));
///         Ok(())
///     }
///
///     fn begin_visit_archive(&mut self, details: &EntryDetails, format: FormatKind) -> std::io::Result<bool> {
///         self.stack.push_archive(&details.path);
///         Ok(true)
///     }
///     fn end_visit_archive(&mut self, details: EntryDetails, format: FormatKind) -> std::io::Result<()> {
///        self.stack.pop_archive();
///        Ok(())
///    }
/// }
///
/// let mut visitor = Visitor::default();
/// visitor.walk(entry).unwrap();
///
/// assert_eq!(visitor.names, [
///     path.join("test"),
///     path.join("nested.tar").join("nested"),
///     path.join("nested.tar").join("nested2"),
///     path.join("nested.tar").join("nested_zip").join("nested3"),
/// ]);
/// ```
pub struct FileEntry<T: Read> {
    details: EntryDetails,
    inner: AnyFormat<T>,
}

impl FileEntry<BufReader<File>> {
    pub fn from_path(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = File::open(&path)?;
        let size = file.metadata()?.len();
        let format = AnyFormat::from_reader(BufReader::new(file))?;
        Ok(Self::new(path.as_ref().to_path_buf(), size, format))
    }
}

impl FileEntry<Reader<Bytes>> {
    pub fn from_bytes(
        path: impl AsRef<Path>,
        data: impl Into<Bytes>,
    ) -> std::io::Result<FileEntry<Reader<Bytes>>> {
        let data = data.into();
        let size = data.len() as u64;
        let inner = AnyFormat::from_reader(data.reader())?;
        Ok(FileEntry {
            details: EntryDetails::new(path.as_ref(), size),
            inner,
        })
    }
}

impl<T: Read> FileEntry<T> {
    pub fn new(path: PathBuf, size: u64, format: AnyFormat<T>) -> Self {
        Self {
            details: EntryDetails::new(path, size),
            inner: format,
        }
    }

    pub fn from_reader(
        path: impl Into<PathBuf>,
        size: u64,
        reader: T,
    ) -> std::io::Result<FileEntry<T>> {
        let inner = AnyFormat::from_reader(reader)?;
        Ok(FileEntry {
            details: EntryDetails::new(path, size),
            inner,
        })
    }

    pub fn into_components(self) -> (EntryDetails, AnyFormat<T>) {
        (self.details, self.inner)
    }

    pub fn details(&self) -> &EntryDetails {
        &self.details
    }

    pub fn path(&self) -> &Path {
        &self.details.path
    }

    pub fn size(&self) -> u64 {
        self.details.size
    }

    pub fn supports_recursion(&self) -> bool {
        matches!(self.inner.kind, FormatKind::Tar | FormatKind::Zip)
    }

    pub fn format(&self) -> FormatKind {
        self.inner.kind
    }

    pub fn get_ref(&self) -> &T {
        self.inner.get_ref()
    }
}

impl<'a, T: Read + 'a> ArchiveVisitor<'a> for FileEntry<T> {
    type Item = T;

    #[inline(always)]
    fn visit<V: AnyWalker>(mut self, visitor: &mut V) -> std::io::Result<()> {
        match self.format() {
            FormatKind::Tar => TarWalker::new(&mut self as &mut dyn Read).visit(visitor),
            FormatKind::Zip => ZipWalker::new(&mut self as &mut dyn Read).visit(visitor),
            _ => FileWalker::new(self).visit(visitor),
        }
    }
}

impl<T: Read> Debug for FileEntry<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchiveEntry")
            .field("path", &self.details.path)
            .field("size", &self.details.size)
            .field("format", &self.inner)
            .finish()
    }
}

impl<T: Read> Read for FileEntry<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}
