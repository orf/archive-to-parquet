use crate::formats::common::add_archive_entry;
use std::io::Read;
use tracing::trace;
// use crate::formats::common::fill_buffer;
use crate::items::{Items, ItemsError};
use crate::Limits;

pub fn extract(
    source: &str,
    mut reader: impl Read,
    items: &mut Items,
    limits: Limits,
) -> Result<usize, ItemsError> {
    let mut count = 0;
    let mut buffer = vec![];

    loop {
        let Ok(Some(entry)) = zip::read::read_zipfile_from_stream(&mut reader) else {
            break;
        };

        if !entry.is_file() {
            continue;
        }
        let path = entry.name().to_string();
        let size = entry.size();
        trace!(?path, size, "read path");

        count += add_archive_entry(source, items, limits, size, entry, path, &mut buffer)?;
    }
    Ok(count)
}
