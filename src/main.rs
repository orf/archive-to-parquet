mod formats;
mod items;
mod output;

use crate::formats::read_tar;
use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Clone, Parser)]
struct Args {
    output: PathBuf,
    paths: Vec<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut output = output::OutputFile::new(args.output)?;
    for path in args.paths {
        let items = read_tar(path)?;
        output.write_items(items)?;
    }
    Ok(())
}
