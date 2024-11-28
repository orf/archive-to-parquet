use crate::formats::{Format, MIN_FILE_SIZE};
use crate::items::{Items, ItemsError};
use std::io;
use std::io::Read;
use tracing::debug;

fn fill_buffer(
    mut reader: impl Read,
    size: u64,
    buffer: &mut Vec<u8>,
) -> io::Result<Option<&[u8]>> {
    buffer.clear();
    if size < MIN_FILE_SIZE as u64 {
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
    depth: usize,
    size: u64,
    entry: impl Read,
    path: String,
    buffer: &mut Vec<u8>,
) -> Result<usize, ItemsError> {
    let Some(data) = fill_buffer(entry, size, buffer)? else {
        return Ok(0);
    };

    if depth > 0 {
        if let Ok(format) = Format::detect_type(data) {
            debug!(%path, %format, "detected format");
            let count = format.extract(&format!("{}/{}", source, path), data, items, depth - 1)?;
            return Ok(count);
        }
    }

    items.add_record(source, path, size, data)?;

    Ok(1)
}
