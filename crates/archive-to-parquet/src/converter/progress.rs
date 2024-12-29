use crate::channel::ConversionCounter;
use crate::progress::{Counters, OutputCounter};
use crate::{Converter, ConvertionOptions, RecordBatchChannel, StandardConverter, Visitor};
use anyreader_walker::{EntryDetails, FormatKind};
use indicatif::{MultiProgress, ProgressBar, ProgressBarIter};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug)]
pub struct ProgressBarConverter<T: Read + Send> {
    converter: StandardConverter<ProgressReader<T>>,
    progress: MultiProgress,
}

impl<T: Read + Send> ProgressBarConverter<T> {
    pub fn progress(&self) -> &MultiProgress {
        &self.progress
    }
}

impl<T: Read + Send> Converter<T> for ProgressBarConverter<T> {
    fn new(options: ConvertionOptions) -> Self {
        Self {
            converter: StandardConverter::new(options),
            progress: Default::default(),
        }
    }

    fn entry_details(&self) -> impl Iterator<Item = (FormatKind, &EntryDetails)> {
        self.converter.entry_details()
    }

    fn options(&self) -> &ConvertionOptions {
        self.converter.options()
    }

    fn add_visitor(
        &mut self,
        visitor: Visitor,
        path: PathBuf,
        size: u64,
        reader: T,
    ) -> std::io::Result<()> {
        let counters = visitor.counters().clone();
        let reader = ProgressReader::new(size, counters, reader);
        self.converter.add_visitor(visitor, path, size, reader)?;
        Ok(())
    }

    fn convert(
        self,
        writer: impl Write + Send,
        channel: RecordBatchChannel,
    ) -> parquet::errors::Result<ConversionCounter> {
        let counters: OutputCounter = Default::default();
        let progress_bar = self.progress.insert(
            0,
            ProgressBar::new(0).with_style(
                indicatif::ProgressStyle::with_template(
                    "{spinner:.green} Writing [{elapsed}] {decimal_bytes} ({decimal_bytes_per_sec}) {status}",
                )
                    .unwrap()
                    .with_key("status", counters.clone()),
            ),
        );
        progress_bar.enable_steady_tick(Duration::from_millis(250));
        let writer = progress_bar.wrap_write(writer);

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.options().threads.into())
            .build()
            .unwrap();
        pool.in_place_scope(|scope| {
            for (mut visitor, entry) in self.converter.visitors {
                let progress = &self.progress;
                scope.spawn(move |_| {
                    entry.get_ref().start_progress_bar(progress);
                    visitor.start_walking(entry);
                });
            }
            let counter = channel.sink_batches(counters, writer, self.converter.options)?;
            Ok(counter)
        })
    }
}

#[derive(Debug)]
struct ProgressReader<T: Read> {
    progress_bar: ProgressBar,
    reader: ProgressBarIter<T>,
}

impl<T: Read> ProgressReader<T> {
    pub fn new(size: u64, counters: Counters, reader: T) -> ProgressReader<T> {
        let progress_bar = ProgressBar::hidden().with_style(
            indicatif::ProgressStyle::with_template(
                "{spinner:.green} Reading [{elapsed}] [{bar:20.cyan/blue}] {decimal_bytes}/{decimal_total_bytes} ({decimal_bytes_per_sec}) {counters}",
            )
                .unwrap()
                .with_key("counters", counters.clone()),
        );
        progress_bar.set_length(size);
        let reader = progress_bar.wrap_read(reader);

        Self {
            progress_bar,
            reader,
        }
    }

    pub fn start_progress_bar(&self, multi_progress: &MultiProgress) {
        multi_progress
            .add(self.progress_bar.clone())
            .enable_steady_tick(Duration::from_millis(500));
    }
}

impl<T: Read> Read for ProgressReader<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.reader.read(buf)
    }
}
