use crate::channel::ConversionCounter;
use crate::converter::Converter;
use crate::progress::OutputCounter;
use crate::{ConvertionOptions, RecordBatchChannel, Visitor};
use anyreader_walker::{EntryDetails, FileEntry, FormatKind};
use std::io::{Read, Write};
use std::path::PathBuf;

#[derive(Debug)]
pub struct StandardConverter<T: Read + Send> {
    pub(super) visitors: Vec<(Visitor, FileEntry<T>)>,
    pub(super) options: ConvertionOptions,
}

impl<T: Read + Send> Converter<T> for StandardConverter<T> {
    fn new(options: ConvertionOptions) -> Self {
        Self {
            visitors: vec![],
            options,
        }
    }

    fn entry_details(&self) -> impl Iterator<Item = (FormatKind, &EntryDetails)> {
        self.visitors
            .iter()
            .map(|(_, entry)| (entry.format(), entry.details()))
    }

    fn options(&self) -> &ConvertionOptions {
        &self.options
    }

    fn add_visitor(
        &mut self,
        visitor: Visitor,
        path: PathBuf,
        size: u64,
        reader: T,
    ) -> std::io::Result<()> {
        let entry = FileEntry::from_reader(path, size, reader)?;
        self.visitors.push((visitor, entry));
        Ok(())
    }

    fn convert(
        self,
        writer: impl Write + Send,
        channel: RecordBatchChannel,
    ) -> parquet::errors::Result<ConversionCounter> {
        let counters: OutputCounter = Default::default();

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.options.threads.into())
            .build()
            .unwrap();
        pool.in_place_scope(|scope| {
            for (mut visitor, entry) in self.visitors {
                scope.spawn(move |_| {
                    visitor.start_walking(entry);
                });
            }
            let counters = channel.sink_batches(counters, writer, self.options)?;
            Ok(counters)
        })
    }
}
