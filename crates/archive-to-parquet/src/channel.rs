use crate::progress::OutputCounter;
use crate::{new_parquet_writer, ConvertionOptions, ParquetSink};
use arrow::record_batch::RecordBatch;
use crossbeam_channel::{Receiver, Sender};
use indicatif::{DecimalBytes, HumanCount, HumanDuration};
use std::fmt::{Debug, Display, Formatter};
use std::io::Write;
use tracing::{error, info};

pub enum RecordBatchResult {
    Batch(RecordBatch),
    Errored(std::io::Error),
}

impl Debug for RecordBatchResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordBatchResult::Batch(batch) => {
                write!(f, "BatchResult(Batch, {} rows)", batch.num_rows())
            }
            RecordBatchResult::Errored(e) => {
                write!(f, "BatchResult(Errored, {e})")
            }
        }
    }
}

#[derive(Debug)]
pub struct ConversionCounter {
    pub total_batches: u64,
    pub total_entries: u64,
    pub total_entries_bytes: u64,
    pub output_rows: u64,
    pub output_bytes: u64,
}

impl Display for ConversionCounter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "entries: in={} out={} bytes: in={} out={}, batches={}",
            HumanCount(self.total_entries),
            HumanCount(self.output_rows),
            DecimalBytes(self.total_entries_bytes),
            DecimalBytes(self.output_bytes),
            HumanCount(self.total_batches),
        )
    }
}

#[derive(Debug, derive_new::new)]
pub struct RecordBatchChannel {
    pub(crate) sender: RecordBatchSender,
    pub(crate) receiver: RecordBatchReceiver,
}

impl RecordBatchChannel {
    fn into_receiver(self) -> RecordBatchReceiver {
        self.receiver
    }

    pub fn sink_batches(
        self,
        counters: OutputCounter,
        writer: impl Write + Send,
        options: ConvertionOptions,
    ) -> parquet::errors::Result<ConversionCounter> {
        let start = std::time::Instant::now();
        let mut writer = new_parquet_writer(writer, options.compression)?;
        let mut sink = ParquetSink::new(&mut writer, options);

        let mut total_rows: u64 = 0;
        let rows_before_flush = 10_000;
        let receiver = self.into_receiver();

        for msg in receiver.inner.iter() {
            match msg {
                RecordBatchResult::Batch(batch) => {
                    counters.batch_received(&batch);
                    total_rows += batch.num_rows() as u64;
                    let res = sink.write_batch(batch)?;
                    counters.batch_handled(res, receiver.inner.len() as u64);
                    if total_rows > rows_before_flush {
                        sink.flush()?;
                        total_rows = 0;
                    }
                }
                RecordBatchResult::Errored(e) => {
                    error!("Error processing: {e:?}");
                    return Err(parquet::errors::ParquetError::from(e));
                }
            }
        }
        writer.flush()?;
        let metadata = writer.finish()?;
        let total_output_bytes: i64 = metadata
            .row_groups
            .iter()
            .map(|rg| rg.total_compressed_size.unwrap_or_default())
            .sum();
        let duration = start.elapsed();
        let conversion_counter: ConversionCounter = counters.into();
        info!(
            "File written in {}. size={}, {conversion_counter}",
            HumanDuration(duration),
            DecimalBytes(total_output_bytes as u64),
        );
        Ok(conversion_counter)
    }
}

pub fn new_record_batch_channel(size: usize) -> RecordBatchChannel {
    let (batch_tx, batch_rx) = crossbeam_channel::bounded(size);
    RecordBatchChannel::new(
        RecordBatchSender::new(batch_tx),
        RecordBatchReceiver::new(batch_rx),
    )
}

#[derive(Clone, Debug, derive_new::new)]
pub(crate) struct RecordBatchSender {
    inner: Sender<RecordBatchResult>,
}

impl RecordBatchSender {
    pub fn send_batch(&self, result: std::io::Result<RecordBatch>) {
        match result {
            Ok(batch) => {
                self.inner.send(RecordBatchResult::Batch(batch)).unwrap();
            }
            Err(error) => {
                self.inner.send(RecordBatchResult::Errored(error)).unwrap();
            }
        }
    }
}

#[derive(Debug, derive_new::new)]
pub(crate) struct RecordBatchReceiver {
    inner: Receiver<RecordBatchResult>,
}
