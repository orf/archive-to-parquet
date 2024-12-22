use crate::formats::common::add_archive_entry;
use crate::formats::Counts;
use crate::{ExtractError, ExtractionOptions, Items, OutputSink};
use std::io::Read;
use std::path::Path;
use tar::Archive;
use tracing::trace;

pub fn extract<T: OutputSink>(
    source: &Path,
    reader: impl Read,
    items: &mut Items<T>,
    options: ExtractionOptions,
) -> Result<Counts, ExtractError> {
    let mut archive = Archive::new(reader);
    let mut buffer = vec![];
    let mut counts = Counts::default();
    for entry in archive.entries()? {
        let entry = entry?;
        if entry.header().entry_type() != tar::EntryType::Regular {
            counts.skipped();
            continue;
        }
        let Ok(path) = entry.path() else {
            counts.skipped();
            continue;
        };
        let path = path.into_owned();
        let size = entry.header().size()?;
        trace!(?path, size, ?counts, "read path");

        counts += add_archive_entry(source, items, options, size, entry, &path, &mut buffer)?;
    }

    Ok(counts)
}
