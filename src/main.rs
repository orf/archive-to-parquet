use anyhow::bail;
use byte_unit::{Byte, Unit};
use bytes::Bytes;
use clap::Parser;
use std::io::{sink, stderr, stdout};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use tracing::{error, info, warn, Level};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

use archive_to_parquet::{
    default_threads, ExtractionOptions, Extractor, OutputSink, ParquetCompression,
};

#[derive(Debug, Clone, Parser)]
struct Args {
    /// Output Parquet file to create
    output: PathBuf,
    /// Input paths to read
    #[clap(required = true)]
    paths: Vec<PathBuf>,
    /// Recursion depth
    /// How many times to recurse into nested archives
    #[clap(short, long)]
    max_depth: Option<NonZeroUsize>,
    /// Min file size to output.
    /// Files below this size are skipped
    #[clap(long, default_value = "300b")]
    min_size: Byte,
    /// Max file size to output.
    /// Files above this size are skipped.
    #[clap(long)]
    max_size: Option<Byte>,

    /// Only output unique files by hash
    #[clap(long)]
    unique: bool,

    /// Only output text files, skipping binary files
    #[clap(long)]
    only_text: bool,

    /// Only output text files, skipping binary files
    #[clap(long, action)]
    ignore_unsupported: bool,

    /// Number of threads to use when extracting.
    /// Defaults to number of CPU cores
    #[clap(long, default_value_t = default_threads())]
    threads: NonZeroUsize,

    /// Compression to use
    #[clap(long, default_value = "zstd(3)")]
    compression: ParquetCompression,
}

fn main() -> anyhow::Result<()> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(Level::INFO.into())
        .from_env()?;
    tracing_subscriber::registry()
        .with(fmt::layer().compact().with_file(false).with_writer(stderr))
        .with(env_filter)
        .init();

    let args = Args::parse();

    let opts = ExtractionOptions {
        min_file_size: args.min_size,
        max_file_size: args.max_size,
        max_depth: args.max_depth,
        only_text: args.only_text,
        threads: args.threads,
        unique: args.unique,
        ignore_unsupported: args.ignore_unsupported,
        compression: args.compression,
    };

    if args.output.to_string_lossy() == "-" {
        let extractor = Extractor::with_writer(stdout(), opts)?;
        do_extraction(extractor, args.paths)?;
    } else if args.output.to_string_lossy() == "/dev/null" {
        let extractor = Extractor::with_writer(sink(), opts)?;
        do_extraction(extractor, args.paths)?;
    } else {
        let extractor = Extractor::with_path(args.output, opts)?;
        do_extraction(extractor, args.paths)?;
    }

    Ok(())
}

fn do_extraction<T: OutputSink>(
    mut extractor: Extractor<T>,
    paths: Vec<PathBuf>,
) -> anyhow::Result<()> {
    let mut errors = vec![];

    // if cfg!(feature = "bench") {
    //     let cloned = extractor.input_contents_mut().cloned_buffers()?;
    //     extractor.set_input_contents(cloned);
    // }

    match paths.as_slice() {
        [first] if first.as_os_str() == "-" => {
            info!("Reading from stdin..");
            // Read from stdin
            let mut buffer = Vec::with_capacity(1024 * 1024);
            std::io::copy(&mut std::io::stdin().lock(), &mut buffer)?;
            let read_bytes = byte_unit::Byte::from(buffer.len()).get_adjusted_unit(Unit::MB);
            info!("Read {:#.1} from stdin", read_bytes);
            extractor.add_buffer(Path::new("stdin").to_path_buf(), Bytes::from(buffer))?;
        }
        paths => {
            for path in paths {
                if path.is_dir() {
                    let dir_errors = extractor.add_directory(path);
                    errors.extend(dir_errors);
                } else if path.is_file() {
                    if let Err(e) = extractor.add_path(path.clone()) {
                        errors.push(e);
                    }
                } else {
                    warn!("Path {:?} is neither a file or a directory, skipping", path);
                }
            }
        }
    }

    for err in errors {
        warn!("{}", err);
    }

    if !extractor.has_input_files() {
        bail!("No input files found")
    }

    info!(
        "Extracting from {} input files",
        extractor.input_file_count()
    );
    let counts = extractor.extract_with_callback(|path, result| match result {
        Ok(count) => {
            info!("{path:?} - {count}");
        }
        Err(e) => {
            error!("{path:?} - error: {e}");
        }
    })?;
    info!("All done: {counts}");
    Ok(())
}
