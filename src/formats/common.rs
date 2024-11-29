use crate::formats::Format;
use crate::items::{Items, ItemsError};
use crate::Limits;
use std::io;
use std::io::Read;
use tracing::debug;

fn fill_buffer(
    mut reader: impl Read,
    size: u64,
    limits: Limits,
    buffer: &mut Vec<u8>,
) -> io::Result<Option<&[u8]>> {
    buffer.clear();
    if !limits.check_file_size(size) {
        Ok(None)
    } else {
        buffer.reserve(size as usize);
        reader.read_to_end(buffer)?;
        Ok(Some(buffer.as_slice()))
    }
}

pub fn add_archive_entry(
    source: &str,
    items: &mut Items,
    mut limits: Limits,
    size: u64,
    entry: impl Read,
    path: String,
    buffer: &mut Vec<u8>,
) -> Result<usize, ItemsError> {
    let Some(data) = fill_buffer(entry, size, limits, buffer)? else {
        return Ok(0);
    };

    if limits.depth > 0 {
        if let Ok(format) = Format::detect_type(data, limits) {
            debug!(%path, %format, "detected format");
            limits.depth -= 1;
            let count = format.extract(&format!("{}/{}", source, path), data, items, limits)?;
            return Ok(count);
        }
    }

    items.add_record(source, path, size, data)?;

    Ok(1)
}
