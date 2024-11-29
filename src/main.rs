mod formats;
mod items;
mod output;

use crate::formats::Format;
use crate::items::{Items, ItemsError};
use byte_unit::Byte;
use clap::Parser;
use std::io::BufReader;
use std::path::PathBuf;
use tracing::{info, warn, Level};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

use rayon::prelude::*;

#[derive(Debug, Clone, Parser)]
struct Args {
    /// Output Parquet file to create
    output: PathBuf,
    /// Input paths to read
    paths: Vec<PathBuf>,
    /// Recursion depth
    /// How many times to recurse into nested archives
    #[clap(short, long)]
    depth: Option<usize>,
    /// Min file size to output.
    /// Files below this size are skipped
    #[clap(long, default_value = "300b")]
    min_size: Byte,
    /// Max file size to output.
    /// Files above this size are skipped.
    #[clap(long)]
    max_size: Option<Byte>,
}

#[derive(Debug, Copy, Clone)]
pub struct Limits {
    min_file_size: Byte,
    max_file_size: Option<Byte>,
    depth: usize,
}

impl Limits {
    pub fn check_file_size(&self, size: u64) -> bool {
        if size < self.min_file_size {
            return false;
        }
        if let Some(max_file_size) = self.max_file_size {
            if size > max_file_size {
                return false;
            }
        }

        true
    }
}

fn main() -> anyhow::Result<()> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(Level::INFO.into())
        .from_env()?;
    tracing_subscriber::registry()
        .with(fmt::layer().compact().with_file(false))
        .with(env_filter)
        .init();

    let args = Args::parse();

    let limits = Limits {
        min_file_size: args.min_size,
        max_file_size: args.max_size,
        depth: args.depth.unwrap_or(0),
    };

    let output = output::OutputFile::new(args.output)?;
    let items: Vec<_> = args
        .paths
        .into_par_iter()
        .filter(|p| p.is_file())
        .map(|path| {
            let mut items = Items::new_with_capacity(&output, 1024 * 80);
            match Format::try_from_path(path.as_path(), limits) {
                Ok(format) => {
                    info!("Reading from {path:?}");
                    let reader = BufReader::with_capacity(1024 * 1024, std::fs::File::open(&path)?);
                    format.extract(&path.to_string_lossy(), reader, &mut items, limits)?;
                    items.flush()?;
                }
                Err(e) => {
                    warn!("Failed to detect format for {}: {}", path.display(), e);
                }
            }
            Ok::<_, ItemsError>(items.total_written)
        })
        .collect();
    let total_written: usize = items.into_iter().map(|r| r.unwrap_or(0)).sum();
    info!("All done. Wrote {} rows", total_written);
    Ok(())
}
