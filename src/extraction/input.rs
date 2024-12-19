use crate::extraction::{ExtractError, Items};
use crate::formats::{ArchiveFormat, Counts, FormatError};
use crate::{ExtractionOptions, InputError, OutputSink, OutputWriter};
use bytes::buf::Reader;
use bytes::{Buf, Bytes};
use rayon::prelude::*;
use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::trace;
use walkdir::WalkDir;

#[derive(Debug)]
pub struct InputContents {
    contents: Vec<ContentKind>,
}

impl Display for InputContents {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "InputFiles(contents={})", self.contents.len())
    }
}

impl InputContents {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            contents: Vec::new(),
        })
    }

    pub fn from_contents(contents: Vec<ContentKind>) -> Self {
        Self { contents }
    }

    pub fn len(&self) -> usize {
        self.contents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.contents.is_empty()
    }

    pub fn input_path_exists(&self, path: &Path) -> bool {
        self.contents.iter().any(|f| f.path() == path)
    }

    pub fn add_directory(&mut self, path: &Path, options: ExtractionOptions) -> Vec<InputError> {
        let mut errors = vec![];
        for entry in WalkDir::new(path).into_iter() {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    let path = err.path().unwrap_or(path).to_path_buf();
                    errors.push(InputError::new(path, FormatError::Io(err.into())));
                    continue;
                }
            };
            if entry.file_type().is_file() {
                let path = entry.path().to_path_buf();
                if let Err(e) = self.add_path(path.clone(), options) {
                    errors.push(e);
                }
            }
        }
        errors
    }

    pub fn add_path(
        &mut self,
        path: PathBuf,
        options: ExtractionOptions,
    ) -> Result<(), InputError> {
        if self.input_path_exists(&path) {
            return Ok(());
        }

        let format = match ArchiveFormat::try_from_path(&path, options) {
            Ok(format) => format,
            Err(FormatError::UnsupportedFormat) if options.ignore_unsupported => return Ok(()),
            Err(e) => return Err(InputError::new(path.clone(), e)),
        };

        let fd = File::open(&path).map_err(|e| InputError::new(path.clone(), e.into()))?;
        let reader = BufReader::with_capacity(1024 * 1024, fd);
        self.contents
            .push(ContentKind::new_filesystem(path, format, reader));
        Ok(())
    }

    pub fn add_buffer(
        &mut self,
        path: PathBuf,
        buffer: Bytes,
        options: ExtractionOptions,
    ) -> Result<(), InputError> {
        if self.input_path_exists(&path) {
            return Ok(());
        }

        let format = match ArchiveFormat::detect_type(buffer.as_ref(), options) {
            Ok(format) => format,
            Err(FormatError::UnsupportedFormat) if options.ignore_unsupported => return Ok(()),
            Err(e) => return Err(InputError::new(path.clone(), e)),
        };

        self.contents
            .push(ContentKind::new_buffer(path, format, buffer));
        Ok(())
    }

    pub fn add_reader(
        &mut self,
        path: PathBuf,
        reader: Box<dyn Read + Send + Sync + 'static>,
        format: ArchiveFormat,
    ) {
        self.contents.push(ContentKind::Boxed(Content {
            path,
            format,
            reader,
        }));
    }

    pub fn contents_sequential(&mut self) -> impl Iterator<Item = ContentKind> + '_ {
        self.contents.sort_by(|a, b| a.path().cmp(b.path()));
        self.contents.drain(0..)
    }

    pub fn contents_parallel(&mut self) -> impl ParallelIterator<Item = ContentKind> + '_ {
        self.contents.par_drain(0..)
    }
}

pub enum ContentKind {
    Filesystem(Content<BufReader<File>>),
    Buffer(Content<Reader<Bytes>>),
    Boxed(Content<Box<dyn Read + Send + Sync + 'static>>),
}

impl Debug for ContentKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ContentKind::Filesystem(r) => {
                write!(f, "ContentKind(FileSystem, path={:?})", r.path())
            }
            ContentKind::Buffer(r) => {
                write!(f, "ContentKind(Buffer, path={:?})", r.path())
            }
            ContentKind::Boxed(r) => {
                write!(f, "ContentKind(Boxed, path={:?})", r.path())
            }
        }
    }
}

impl ContentKind {
    pub fn new_filesystem(path: PathBuf, format: ArchiveFormat, reader: BufReader<File>) -> Self {
        ContentKind::Filesystem(Content {
            path,
            format,
            reader,
        })
    }

    pub fn new_buffer(path: PathBuf, format: ArchiveFormat, buf: Bytes) -> Self {
        ContentKind::Buffer(Content {
            path,
            format,
            reader: buf.reader(),
        })
    }

    pub fn path(&self) -> &Path {
        match self {
            ContentKind::Filesystem(f) => f.path(),
            ContentKind::Buffer(b) => b.path(),
            ContentKind::Boxed(b) => b.path(),
        }
    }

    pub fn extract<O: OutputSink>(
        self,
        output_file: Arc<OutputWriter<O>>,
        options: ExtractionOptions,
    ) -> Result<Counts, ExtractError> {
        match self {
            ContentKind::Filesystem(f) => f.extract(output_file, options),
            ContentKind::Buffer(b) => b.extract(output_file, options),
            ContentKind::Boxed(b) => b.extract(output_file, options),
        }
    }
}

pub struct Content<T: Read + Send + Sync> {
    path: PathBuf,
    format: ArchiveFormat,
    reader: T,
}

impl<T: Read + Send + Sync> Content<T> {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn extract<O: OutputSink>(
        self,
        output_file: Arc<OutputWriter<O>>,
        options: ExtractionOptions,
    ) -> Result<Counts, ExtractError> {
        trace!(?self.path, %self.format, "extracting");
        let mut items = Items::new_with_capacity(output_file, 1024, options);
        let count = self
            .format
            .extract(&self.path, self.reader, &mut items, options)?;
        items.flush()?;
        trace!(
            "extract_count={count} item_count={} path={:?}",
            items.counts(),
            self.path
        );
        Ok(count + items.counts())
    }
}
