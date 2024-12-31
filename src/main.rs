use anyhow::{bail, Context};
use archive_to_parquet::{
    new_record_batch_channel, ConversionCounter, ConvertionOptions, IncludeType,
    ProgressBarConverter, StandardConverter,
};
use archive_to_parquet::{Converter, RecordBatchChannel};
use byte_unit::Byte;
use clap::Parser;
use indicatif::MultiProgress;
pub use parquet::basic::Compression as ParquetCompression;
use std::fs::File;
use std::io::{stderr, BufRead, BufReader, BufWriter, Stderr, Write};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use tracing::{error, info, Level};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub fn default_threads() -> NonZeroUsize {
    std::thread::available_parallelism().unwrap_or(NonZeroUsize::new(1).unwrap())
}

const DEFAULT_OPTS: ConvertionOptions = ConvertionOptions::const_default();

#[derive(Debug, Clone, Parser)]
struct Args {
    /// Output Parquet file to create
    output: PathBuf,

    /// Input paths to read. Pass "-" to read paths from stdin
    #[clap(required = true)]
    paths: Vec<PathBuf>,

    /// Min file size to output.
    /// Files below this size are skipped
    #[clap(long)]
    min_size: Option<Byte>,

    /// Max file size to output.
    /// Files above this size are skipped.
    #[clap(long)]
    max_size: Option<Byte>,

    /// Only output unique files by hash
    #[clap(long)]
    unique: bool,

    /// Only output text files, skipping binary files
    #[clap(long, value_enum, default_value_t=DEFAULT_OPTS.include)]
    include: IncludeType,

    /// Number of threads to use when extracting.
    /// Defaults to number of CPU cores
    #[clap(long, default_value_t = default_threads())]
    threads: NonZeroUsize,

    /// Compression to use
    #[clap(long, default_value_t = DEFAULT_OPTS.compression)]
    compression: ParquetCompression,

    /// Number of batches to buffer in memory at once.
    #[clap(long, default_value_t = DEFAULT_OPTS.batch_count)]
    batch_count: usize,

    /// Maximum size of each batch in memory.
    #[clap(long, default_value = "100MB")]
    batch_size: Byte,

    /// Log file to write messages to
    #[clap(long)]
    log_file: Option<PathBuf>,

    /// Disable progress bars
    #[clap(long)]
    no_progress: bool,
}
fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    do_main(args)?;
    Ok(())
}
fn do_main(args: Args) -> anyhow::Result<()> {
    let options = ConvertionOptions::new(
        args.threads,
        args.include,
        args.unique,
        args.compression,
        args.min_size,
        args.max_size,
        args.batch_count,
        args.batch_size,
    );

    let channel = new_record_batch_channel(options.batch_count);

    let counts = if args.no_progress {
        let converter: StandardConverter<BufReader<File>> = StandardConverter::new(options);
        let _guard = setup_tracing_output(args.log_file, None)?;
        run_converter(converter, channel, args.paths, args.output)?
    } else {
        let converter: ProgressBarConverter<BufReader<File>> = ProgressBarConverter::new(options);
        let _guard = setup_tracing_output(args.log_file, Some(converter.progress().clone()))?;
        run_converter(converter, channel, args.paths, args.output)?
    };

    if counts.output_rows == 0 {
        error!("No rows written to output file. Raw stats: {counts:#?}");
        bail!("No rows written to output file");
    }

    Ok(())
}

fn run_converter(
    mut converter: impl Converter<BufReader<File>>,
    channel: RecordBatchChannel,
    paths: Vec<PathBuf>,
    output_file: PathBuf,
) -> anyhow::Result<ConversionCounter> {
    let paths = if paths.len() == 1 && paths[0].to_string_lossy() == "-" {
        info!("Reading paths from stdin");
        std::io::stdin()
            .lock()
            .lines()
            .map(|line| line.map(PathBuf::from))
            .collect::<Result<Vec<_>, _>>()
            .context("Reading paths from stdin")?
    } else {
        paths
    };

    let limit = rlimit::increase_nofile_limit((paths.len() * 100) as u64)?;
    info!("Increased open file limit to {}", limit);
    info!("Converting {} files to Parquet", paths.len());
    info!("Options: {}", converter.options());
    for path in paths {
        converter
            .add_paths([&path], &channel)
            .with_context(|| format!("Adding path {path:?}"))?;
    }

    let output_file =
        File::create(&output_file).with_context(|| format!("Creating file {:?}", output_file))?;
    converter
        .convert(output_file, channel)
        .context("Converting")
}

fn setup_tracing_output(
    log_file: Option<PathBuf>,
    progress: Option<MultiProgress>,
) -> anyhow::Result<WorkerGuard> {
    let (writer, guard) = match (log_file, progress) {
        (Some(log_file), _) => {
            let file = File::create(&log_file)
                .with_context(|| format!("Creating log file {:?}", log_file))?;
            tracing_appender::non_blocking(BufWriter::new(file))
        }
        (_, Some(progress)) => {
            let writer = TracingProgressWriter::new(progress, stderr());
            tracing_appender::non_blocking(writer)
        }
        (None, None) => tracing_appender::non_blocking(stderr()),
    };
    setup_tracing(writer)?;
    Ok(guard)
}

fn setup_tracing(writer: impl Write + Sync + Send + Clone + 'static) -> anyhow::Result<()> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(Level::INFO.into())
        .from_env()
        .context("Setting up tracing environment filter")?;
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .compact()
                .with_file(false)
                .with_thread_ids(true)
                .with_writer(move || writer.clone()),
        )
        .with(env_filter)
        .init();
    Ok(())
}

// Utils for making tracing and indicatif work together
#[derive(derive_new::new)]
struct TracingProgressWriter {
    progress: MultiProgress,
    writer: Stderr,
}

impl Clone for TracingProgressWriter {
    fn clone(&self) -> Self {
        Self {
            progress: self.progress.clone(),
            writer: stderr(),
        }
    }
}

impl MakeWriter<'_> for TracingProgressWriter {
    type Writer = TracingProgressWriter;

    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}

impl Write for TracingProgressWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.progress.suspend(|| self.writer.write(buf))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.progress.suspend(|| self.writer.flush())
    }
}
