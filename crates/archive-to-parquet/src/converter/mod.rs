mod base;
mod progress;

use crate::channel::{ConversionCounter, RecordBatchChannel};
use crate::{ConvertionOptions, Visitor};
use anyreader_walker::{EntryDetails, FormatKind};
pub use base::StandardConverter;
pub use progress::ProgressBarConverter;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;

pub trait Converter<T: Read + Send>: Sized {
    fn new(options: ConvertionOptions) -> Self;

    fn entry_details(&self) -> impl Iterator<Item = (FormatKind, &EntryDetails)>;

    fn options(&self) -> &ConvertionOptions;

    fn add_paths(
        &mut self,
        paths: impl IntoIterator<Item = PathBuf>,
        channel: &RecordBatchChannel,
    ) -> std::io::Result<()>
    where
        Self: Converter<BufReader<File>>,
    {
        let mut readers = vec![];
        for path in paths.into_iter() {
            let reader = File::open(&path)?;
            let size = reader.metadata()?.len();
            readers.push((path, size, BufReader::new(reader)));
        }
        self.add_readers(readers, channel)
    }

    fn add_readers(
        &mut self,
        readers: impl IntoIterator<Item = (PathBuf, u64, T)>,
        channel: &RecordBatchChannel,
    ) -> std::io::Result<()> {
        let batch_size = self.options().batch_size;

        for (path, size, reader) in readers.into_iter() {
            let visitor = Visitor::new(path.clone(), channel.sender.clone(), batch_size);
            self.add_visitor(visitor, path, size, reader)?
        }
        Ok(())
    }

    fn add_visitor(
        &mut self,
        visitor: Visitor,
        path: PathBuf,
        size: u64,
        reader: T,
    ) -> std::io::Result<()>;

    fn convert(
        self,
        writer: impl Write + Send,
        channel: RecordBatchChannel,
    ) -> parquet::errors::Result<ConversionCounter>;
}
