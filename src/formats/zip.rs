use crate::formats::common::add_archive_entry;
use crate::formats::Counts;
use crate::{ExtractError, ExtractionOptions, Items, OutputSink};
use std::io::Read;
use std::path::Path;
use tracing::trace;

pub fn extract<T: OutputSink>(
    source: &Path,
    mut reader: impl Read,
    items: &mut Items<T>,
    options: ExtractionOptions,
) -> Result<Counts, ExtractError> {
    let mut counts = Counts::default();
    let mut buffer = vec![];

    loop {
        let Ok(Some(entry)) = zip::read::read_zipfile_from_stream(&mut reader) else {
            break;
        };

        if !entry.is_file() {
            counts.skipped();
            continue;
        }

        let path = entry.name().to_string();
        let size = entry.size();
        trace!(?path, size, "read path");

        counts += add_archive_entry(source, items, options, size, entry, path, &mut buffer)?;
    }
    Ok(counts)
}
