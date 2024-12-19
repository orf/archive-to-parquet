use crate::formats::{ArchiveFormat, Counts, FormatError};
use crate::{ExtractionOptions, InputContents, OutputSink, OutputWriter};
use bytes::Bytes;
use rayon::ThreadPoolBuilder;
use std::fmt::{Debug, Display, Formatter};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::trace;

use crate::extraction::ExtractError;
#[cfg(not(test))]
use rayon::iter::ParallelIterator;

#[derive(Debug, thiserror::Error)]
#[error("Error reading input file {path}: {error}")]
#[cfg_attr(
    feature = "python",
    pyo3::prelude::pyclass(frozen, module = "archive_to_parquet")
)]
pub struct InputError {
    pub error: FormatError,
    pub path: PathBuf,
}

impl InputError {
    pub fn new(path: PathBuf, error: FormatError) -> Self {
        Self { error, path }
    }
}

#[derive(Debug)]
pub struct Extractor<T: OutputSink> {
    pool: rayon::ThreadPool,
    options: ExtractionOptions,
    output: Arc<OutputWriter<T>>,
    input: InputContents,
}

impl<T: OutputSink> Display for Extractor<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Extractor(input={}, output={}, options={})",
            self.input, self.output, self.options
        )
    }
}

impl Extractor<File> {
    pub fn with_path(path: PathBuf, options: ExtractionOptions) -> anyhow::Result<Extractor<File>> {
        let output = OutputWriter::from_path(path, options)?;
        Self::new(output, options)
    }
}

impl<T: OutputSink> Extractor<T> {
    pub fn with_writer(writer: T, options: ExtractionOptions) -> anyhow::Result<Self> {
        let output = OutputWriter::from_writer(writer, options)?;
        Self::new(output, options)
    }

    pub fn new(output: OutputWriter<T>, options: ExtractionOptions) -> anyhow::Result<Self> {
        let output = Arc::new(output);
        let pool = ThreadPoolBuilder::new()
            .num_threads(options.threads.into())
            .build()?;
        let input = InputContents::new()?;
        Ok(Self {
            pool,
            output,
            input,
            options,
        })
    }

    pub fn set_input_contents(&mut self, files: InputContents) {
        self.input = files;
    }

    pub fn input_contents_mut(&mut self) -> &mut InputContents {
        &mut self.input
    }

    pub fn input_file_count(&self) -> usize {
        self.input.len()
    }

    pub fn has_input_files(&self) -> bool {
        !self.input.is_empty()
    }

    pub fn add_path(&mut self, path: PathBuf) -> Result<(), InputError> {
        self.input.add_path(path, self.options)
    }

    pub fn add_directory(&mut self, path: &Path) -> Vec<InputError> {
        self.input.add_directory(path, self.options)
    }

    pub fn add_buffer(&mut self, path: PathBuf, buffer: Bytes) -> Result<(), InputError> {
        self.input.add_buffer(path, buffer, self.options)
    }

    pub fn add_reader(
        &mut self,
        path: PathBuf,
        reader: Box<dyn Read + Send + Sync + 'static>,
        format: ArchiveFormat,
    ) {
        self.input.add_reader(path, reader, format);
    }

    pub fn extract_with_callback(
        &mut self,
        callback: impl Fn(PathBuf, &Result<Counts, ExtractError>) + Send + Sync,
    ) -> Result<Counts, ExtractError> {
        self.do_extract(callback)
    }

    pub fn extract(&mut self) -> Result<Counts, ExtractError> {
        self.do_extract(|_, _| {})
    }

    fn do_extract(
        &mut self,
        callback: impl Fn(PathBuf, &Result<Counts, ExtractError>) + Send + Sync,
    ) -> Result<Counts, ExtractError> {
        trace!("Extracting with options {:?}", self.options);
        self.pool.in_place_scope(|_| {
            #[cfg(test)]
            let iterator = self.input.contents_sequential();
            #[cfg(not(test))]
            let iterator = self.input.contents_parallel();
            let items: Vec<_> = iterator
                .map(|file| {
                    trace!("Extracting file {}", file.path().display());
                    let path = file.path().to_path_buf();
                    let result = file.extract(self.output.clone(), self.options);
                    trace!("Result for file {}: {:?}", path.display(), result);
                    callback(path, &result);
                    result
                })
                .collect();

            let summed_count: Counts = items.into_iter().flatten().sum();
            let output_count = self.output.counts();
            trace!("Summed count: {:?}", summed_count);
            let counts = Counts {
                read: summed_count.read,
                skipped: summed_count.skipped,
                deduplicated: summed_count.deduplicated + output_count.deduplicated,
                written: output_count.written,
            };
            Ok(counts)
        })
    }

    // #[cfg(feature = "python")]
    // pub(crate) fn finish_mut(&self) -> Result<T, ExtractError> {
    //     self.output.finish()
    // }

    pub fn finish(self) -> Result<T, ExtractError> {
        self.output.finish()
    }
}
