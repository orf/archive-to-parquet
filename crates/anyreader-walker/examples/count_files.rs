use anyreader::FormatKind;
use anyreader_walker::EntryDetails;
use anyreader_walker::{AnyWalker, FileEntry};
use std::collections::HashMap;
use std::env::args;
use std::io::Read;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Default)]
struct Visitor {
    count: usize,
    size: u64,
    format_counts: HashMap<FormatKind, usize>,
}

impl AnyWalker for Visitor {
    fn visit_file_entry(&mut self, entry: &mut FileEntry<impl Read>) -> std::io::Result<()> {
        self.count += 1;
        self.size += entry.size();
        self.format_counts
            .entry(entry.format())
            .and_modify(|c| *c += 1)
            .or_insert(1);
        Ok(())
    }

    fn begin_visit_archive(
        &mut self,
        details: &EntryDetails,
        format: FormatKind,
    ) -> std::io::Result<bool> {
        eprintln!("Recursing into {:?} ({:?})", details.path, format);
        Ok(true)
    }

    fn end_visit_archive(
        &mut self,
        _details: EntryDetails,
        _format: FormatKind,
    ) -> std::io::Result<()> {
        eprintln!("Finished recursing");
        Ok(())
    }
}

fn main() -> std::io::Result<()> {
    let path = PathBuf::from(args().nth(1).unwrap());
    println!("{path:?}");

    let mut elapsed = Duration::from_secs(0);

    let mut visitor = Visitor::default();

    for item in std::fs::read_dir(&path)? {
        let item = item?;
        let path = item.path();
        if path.is_dir() {
            continue;
        }
        let start = Instant::now();
        let entry = FileEntry::from_path(path.clone())?;
        visitor.walk(entry)?;
        elapsed += start.elapsed();
    }
    println!("Total files: {}", visitor.count);
    println!("Total size: {}", visitor.size);

    let elapsed = elapsed.as_secs_f64();
    println!("Elapsed: {:.3}s", elapsed);
    println!("Throughput: {:.3} files/s", visitor.count as f64 / elapsed);
    println!(
        "Throughput: {:.3} MB/s",
        visitor.size as f64 / elapsed / 1024.0 / 1024.0
    );

    let mut counts: Vec<_> = visitor.format_counts.into_iter().collect();
    counts.sort_by_key(|(_, count)| *count);

    for (format, count) in counts {
        println!("{:?}: {}", format, count);
    }

    Ok(())
}
