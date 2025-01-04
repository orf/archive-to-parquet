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
use std::io::{stderr, BufRead, BufReader, BufWriter, Read, Stderr, Write};
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
    paths: Vec<String>,

    /// Treat input paths as URLs
    #[clap(long)]
    urls: bool,

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

    /// Extract strings from executables
    #[clap(long)]
    extract_executable_strings: bool,
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
        args.extract_executable_strings,
    );

    let channel = new_record_batch_channel(options.batch_count);

    let paths = get_paths(args.paths)?;

    let counts = if args.no_progress {
        run_progress_converter(channel, paths, args.log_file, args.output, args.urls, options)?
    } else {
        run_no_progress_converter(channel, paths, args.log_file, args.output, args.urls, options)?
    };

    if counts.output_rows == 0 {
        error!("No rows written to output file. Raw stats: {counts:#?}");
        bail!("No rows written to output file");
    }

    Ok(())
}

fn get_paths(paths: Vec<String>) -> anyhow::Result<Vec<String>> {
    let paths = if paths.len() == 1 && paths[0] == "-" {
        info!("Reading paths from stdin");
        std::io::stdin()
            .lock()
            .lines()
            .collect::<Result<Vec<_>, _>>()
            .context("Reading paths from stdin")?
    } else {
        paths
    };

    let limit = rlimit::increase_nofile_limit((paths.len() * 100) as u64)?;
    info!("Increased open file limit to {}", limit);
    info!("Converting {} files to Parquet", paths.len());
    Ok(paths)
}

fn run_no_progress_converter(channel: RecordBatchChannel, paths: Vec<String>, log_file: Option<PathBuf>, output: PathBuf, urls: bool, options: ConvertionOptions) -> anyhow::Result<ConversionCounter> {
    if urls {
        let mut converter: StandardConverter<reqwest::blocking::Response> = StandardConverter::new(options);
        let _guard = setup_tracing_output(log_file, None)?;
        add_urls_to_converter(paths, &channel, &mut converter)?;
        Ok(run_converter(converter, channel, output)?)
    } else {
        let mut converter: StandardConverter<BufReader<File>> = StandardConverter::new(options);
        let _guard = setup_tracing_output(log_file, None)?;
        add_files_to_converter(paths, &channel, &mut converter)?;
        Ok(run_converter(converter, channel, output)?)
    }
}

fn run_progress_converter(channel: RecordBatchChannel, paths: Vec<String>, log_file: Option<PathBuf>, output: PathBuf, urls: bool, options: ConvertionOptions) -> anyhow::Result<ConversionCounter> {
    if urls {
        let mut converter: ProgressBarConverter<reqwest::blocking::Response> = ProgressBarConverter::new(options);
        let _guard = setup_tracing_output(log_file, Some(converter.progress().clone()))?;
        add_urls_to_converter(paths, &channel, &mut converter)?;
        Ok(run_converter(converter, channel, output)?)
    } else {
        let mut converter: ProgressBarConverter<BufReader<File>> = ProgressBarConverter::new(options);
        let _guard = setup_tracing_output(log_file, Some(converter.progress().clone()))?;
        add_files_to_converter(paths, &channel, &mut converter)?;
        Ok(run_converter(converter, channel, output)?)
    }
}

fn add_urls_to_converter(urls: Vec<String>, channel: &RecordBatchChannel, converter: &mut impl Converter<reqwest::blocking::Response>) -> anyhow::Result<()> {
    let session = reqwest::blocking::Client::new();
    for url in urls {
        let response = session.get(&url).send().with_context(|| format!("Fetching URL {url:?}"))?;
        let content_length = response.content_length().unwrap_or_default();
        let reader = response.error_for_status().with_context(|| format!("Fetching URL {url:?}"))?;
        converter
            .add_readers([(&url, content_length, reader)], channel)
            .with_context(|| format!("Adding URL {url:?}"))?;
    }
    Ok(())
}

fn add_files_to_converter(paths: Vec<String>, channel: &RecordBatchChannel, converter: &mut impl Converter<BufReader<File>>) -> anyhow::Result<()> {
    for path in paths {
        converter
            .add_paths([&path], channel)
            .with_context(|| format!("Adding path {path:?}"))?;
    }
    Ok(())
}

fn run_converter<T: Read + Send>(
    converter: impl Converter<T>,
    channel: RecordBatchChannel,
    output_file: PathBuf,
) -> anyhow::Result<ConversionCounter> {
    info!("Options: {}", converter.options());

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
