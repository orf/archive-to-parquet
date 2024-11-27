mod formats;
mod items;
mod output;

use crate::formats::Format;
use crate::items::Items;
use clap::Parser;
use std::io::BufReader;
use std::path::PathBuf;
use tracing::{info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Debug, Clone, Parser)]
struct Args {
    output: PathBuf,
    paths: Vec<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let output = output::OutputFile::new(args.output)?;
    let mut items = Items::new_with_capacity(output, 1024 * 8);
    for path in args.paths {
        if !path.is_file() {
            continue;
        }
        match Format::try_from(path.as_path()) {
            Ok(format) => {
                let reader = BufReader::with_capacity(1024 * 1024, std::fs::File::open(&path)?);
                format.extract(path.to_string_lossy(), reader, &mut items, true)?;
            }
            Err(e) => {
                warn!("Failed to detect format for {}: {}", path.display(), e);
                continue;
            }
        }
    }
    items.flush()?;
    info!("All done. Wrote {} rows", items.total_written);
    Ok(())
}
