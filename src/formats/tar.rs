use crate::formats::common::add_archive_entry;
use crate::items::{Items, ItemsError};
use crate::Limits;
use std::io::Read;
use tar::Archive;
use tracing::trace;

pub fn extract(
    source: &str,
    reader: impl Read,
    items: &mut Items,
    limits: Limits,
) -> Result<usize, ItemsError> {
    let mut archive = Archive::new(reader);
    let mut buffer = vec![];
    let mut count = 0;
    for entry in archive.entries()? {
        let entry = entry?;
        if entry.header().entry_type() != tar::EntryType::Regular {
            continue;
        }
        let Ok(path) = entry.path() else { continue };
        let Some(path) = path.to_str() else { continue };
        let size = entry.header().size()?;
        let path = path.to_string();
        trace!(%path, size, "read path");

        count += add_archive_entry(source, items, limits, size, entry, path, &mut buffer)?;
    }

    Ok(count)
}
