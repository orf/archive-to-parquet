use crate::ConvertionOptions;
use crate::anyreader_walker::{AnyWalker, ArchiveStack, EntryDetails, FileEntry, FormatKind};
use crate::batch::OutputBatch;
use crate::channel::RecordBatchSender;
use crate::progress::Counters;
use std::io::Read;
use std::path::PathBuf;
use tracing::{debug, error, trace};

#[derive(Debug)]
pub struct Visitor {
    input_path: PathBuf,
    batch: OutputBatch,
    channel: RecordBatchSender,
    stack: ArchiveStack,
    counters: Counters,
}

impl Visitor {
    pub(crate) fn new(
        path: impl Into<PathBuf>,
        channel: RecordBatchSender,
        options: ConvertionOptions,
    ) -> Self {
        Self {
            input_path: path.into(),
            channel,
            batch: OutputBatch::new_with_options(options),
            stack: ArchiveStack::default(),
            counters: Counters::default(),
        }
    }
}

impl Visitor {
    pub fn counters(&self) -> &Counters {
        &self.counters
    }

    fn send_batch(&mut self) -> std::io::Result<()> {
        let batch = self
            .batch
            .create_record_batch_and_reset()
            .map_err(std::io::Error::other)?;
        trace!("Sending batch with {} rows", batch.num_rows());
        self.counters.sent_batch();
        self.channel.send_batch(Ok(batch))?;
        Ok(())
    }

    fn try_walk(&mut self, entry: FileEntry<impl Read>) -> std::io::Result<()> {
        self.walk(entry)?;
        if !self.batch.is_empty() {
            self.send_batch()?;
        }
        Ok(())
    }

    pub fn start_walking(&mut self, entry: FileEntry<impl Read>) {
        debug!("Starting to walk: {}", entry.details());
        if let Err(e) = self.try_walk(entry) {
            error!("Error while walking {:?}: {}", self.stack.nested_path(), e);
            self.channel.send_batch(Err(e)).ok(); // Channel disconnected, ignore
        }
    }
}

impl AnyWalker for Visitor {
    fn visit_file_entry(&mut self, entry: &mut FileEntry<impl Read>) -> std::io::Result<()> {
        trace!(
            "Processing file: {}. Current source: {}",
            entry.details(),
            self.stack.nested_path().display()
        );

        let entry_size = self
            .batch
            .add_record(&self.input_path, self.stack.nested_path(), entry);

        self.counters.read_entry(entry_size);

        if self.batch.should_flush() {
            self.send_batch()?;
        }
        Ok(())
    }

    fn begin_visit_archive(
        &mut self,
        details: &EntryDetails,
        format: FormatKind,
    ) -> std::io::Result<bool> {
        // Detect quine zip files
        if format.is_zip() && Some(details) == self.stack.last_entry() {
            debug!(
                "Skipping archive: quine zip. details: {details}. Current source: {:?}",
                self.stack.nested_path()
            );
            return Ok(false);
        }
        self.stack.push_details(details.clone());
        debug!(
            "Processing archive: {details} - {format}. Current source: {:?}",
            self.stack.nested_path()
        );
        Ok(true)
    }

    fn end_visit_archive(
        &mut self,
        _details: EntryDetails,
        _format: FormatKind,
    ) -> std::io::Result<()> {
        self.counters.read_archive();

        let finished = self.stack.pop_details();
        debug!(
            "Finished processing archive: {finished:?}. Current source: {:?}",
            self.stack.nested_path()
        );
        Ok(())
    }
}
