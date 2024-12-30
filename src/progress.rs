use crate::channel::ConversionCounter;
use crate::sink::WriteBatchOutput;
use arrow::array::RecordBatch;
use indicatif::style::ProgressTracker;
use indicatif::{DecimalBytes, HumanCount, ProgressState};
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Default)]
struct CountersInner {
    entries_read: AtomicU64,
    entries_read_bytes: AtomicU64,
    archives_read: AtomicU64,
    batches_sent: AtomicU64,
}

#[derive(Clone, Debug, Default)]
pub struct Counters {
    counters: Arc<CountersInner>,
}

impl ProgressTracker for Counters {
    fn clone_box(&self) -> Box<dyn ProgressTracker> {
        Box::new(self.clone())
    }

    fn tick(&mut self, _state: &ProgressState, _now: Instant) {}

    fn reset(&mut self, _state: &ProgressState, _now: Instant) {}

    fn write(&self, _state: &ProgressState, w: &mut dyn Write) {
        w.write_fmt(format_args!(
            "Entries: {}, Entries Bytes: {}, Archives: {}, Batches: {}",
            HumanCount(self.counters.entries_read.load(Ordering::Acquire)),
            DecimalBytes(self.counters.entries_read_bytes.load(Ordering::Acquire)),
            HumanCount(self.counters.archives_read.load(Ordering::Acquire)),
            HumanCount(self.counters.batches_sent.load(Ordering::Acquire)),
        ))
        .unwrap();
    }
}

impl Counters {
    pub fn read_archive(&self) {
        self.counters.archives_read.fetch_add(1, Ordering::Relaxed);
    }

    pub fn sent_batch(&self) {
        self.counters.batches_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn read_entry(&self, size: u64) {
        self.counters.entries_read.fetch_add(1, Ordering::Relaxed);
        self.counters
            .entries_read_bytes
            .fetch_add(size, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
struct OutputCounterInner {
    total_batches: AtomicU64,
    total_entries: AtomicU64,
    total_entries_bytes: AtomicU64,

    output_rows: AtomicU64,
    output_bytes: AtomicU64,

    batches_pending: AtomicU64,
}

#[derive(Clone, Debug, Default, derive_new::new)]
pub struct OutputCounter {
    #[new(into)]
    counters: Arc<OutputCounterInner>,
}

impl From<OutputCounter> for ConversionCounter {
    fn from(val: OutputCounter) -> Self {
        val.as_conversion_counter(Ordering::SeqCst)
    }
}

impl OutputCounter {
    pub fn as_conversion_counter(&self, ordering: Ordering) -> ConversionCounter {
        ConversionCounter {
            total_batches: self.counters.total_batches.load(ordering),
            total_entries: self.counters.total_entries.load(ordering),
            total_entries_bytes: self.counters.total_entries_bytes.load(ordering),
            output_rows: self.counters.output_rows.load(ordering),
            output_bytes: self.counters.output_bytes.load(ordering),
        }
    }

    pub fn batch_received(&self, batch: &RecordBatch) {
        self.counters.total_batches.fetch_add(1, Ordering::Relaxed);
        self.counters
            .total_entries
            .fetch_add(batch.num_rows() as u64, Ordering::Acquire);
        self.counters
            .total_entries_bytes
            .fetch_add(batch.get_array_memory_size() as u64, Ordering::Acquire);
    }
    pub fn batch_handled(&self, output: WriteBatchOutput, pending_batches: u64) {
        self.counters
            .output_rows
            .fetch_add(output.num_rows, Ordering::Acquire);
        self.counters
            .output_bytes
            .fetch_add(output.bytes, Ordering::Acquire);
        self.counters
            .batches_pending
            .store(pending_batches, Ordering::Relaxed);
    }
}

impl ProgressTracker for OutputCounter {
    fn clone_box(&self) -> Box<dyn ProgressTracker> {
        Box::new(self.clone())
    }

    fn tick(&mut self, _state: &ProgressState, _now: Instant) {}

    fn reset(&mut self, _state: &ProgressState, _now: Instant) {}

    fn write(&self, _state: &ProgressState, w: &mut dyn std::fmt::Write) {
        let pending_batches = self.counters.batches_pending.load(Ordering::Acquire);
        let counter = self.as_conversion_counter(Ordering::Relaxed);
        write!(w, "{counter} ({pending_batches} pending)").unwrap();
    }
}
