mod formats;
mod items;
mod output;

use crate::formats::Format;
use crate::items::{Items, ItemsError};
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
    #[clap(short, long, default_value_t = 0)]
    depth: usize,
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
    let output = output::OutputFile::new(args.output)?;
    let items: Vec<_> = args.paths.into_par_iter().filter(|p| p.is_file()).map(|path| {
        let mut items = Items::new_with_capacity(&output, 1024 * 80);
        match Format::try_from(path.as_path()) {
            Ok(format) => {
                info!("Reading from {path:?}");
                let reader = BufReader::with_capacity(1024 * 1024, std::fs::File::open(&path)?);
                format.extract(&path.to_string_lossy(), reader, &mut items, args.depth)?;
                items.flush()?;
            }
            Err(e) => {
                warn!("Failed to detect format for {}: {}", path.display(), e);
            }
        }
        Ok::<_, ItemsError>(items.total_written)
    }).collect();
    let total_written: usize = items.into_iter().map(|r| r.unwrap_or(0)).sum();
    info!("All done. Wrote {} rows", total_written);
    Ok(())
}
