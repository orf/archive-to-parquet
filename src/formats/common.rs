use crate::formats::{ArchiveFormat, Counts};
use crate::{ExtractError, ExtractionOptions, Items, OutputSink};
use simdutf8::basic::Utf8Error;
use std::io;
use std::io::Read;
use std::path::Path;
use tracing::trace;

#[inline(always)]
fn fill_buffer(
    mut reader: impl Read,
    size: u64,
    options: ExtractionOptions,
    buffer: &mut Vec<u8>,
) -> io::Result<Option<&[u8]>> {
    buffer.clear();
    if !options.check_file_size(size) {
        Ok(None)
    } else {
        buffer.reserve(size as usize);
        reader.read_to_end(buffer)?;
        Ok(Some(buffer.as_slice()))
    }
}

#[inline(always)]
fn decode_text(data: &[u8]) -> Result<&str, Utf8Error> {
    simdutf8::basic::from_utf8(data)
}

#[inline(always)]
pub fn add_archive_entry<T: OutputSink>(
    source: &Path,
    items: &mut Items<T>,
    options: ExtractionOptions,
    size: u64,
    entry: impl Read,
    path: String,
    buffer: &mut Vec<u8>,
) -> Result<Counts, ExtractError> {
    let Some(data) = fill_buffer(entry, size, options, buffer)? else {
        trace!("Skipping file {path} due to size limit: {size}");
        return Ok(Counts::new_skipped());
    };

    trace!(
        "Adding archive entry source={source:?} path={path:?} size={size} depth={:?}",
        options.max_depth
    );
    if let Some(previous_max_depth) = options.max_depth {
        if let Ok(format) = ArchiveFormat::detect_type(data, options) {
            trace!(%path, %format, "detected format");
            let new_options = options.decrement_max_depth();
            let path = source.join(path);
            trace!(depth=?new_options.max_depth, old_depth=previous_max_depth, "recursing");
            let count = format.extract(&path, data, items, new_options)?;
            return Ok(count);
        }
    }

    if options.only_text {
        let decoded = decode_text(data);
        match decoded {
            Ok(str) => {
                items.add_text_record(source, path, size, str)?;
            }
            Err(_) => {
                trace!("Skipping non-text file {path}");
                return Ok(Counts::new_skipped());
            }
        }
    } else {
        items.add_record(source, path, size, data)?;
    }
    Ok(Counts::new_processed())
}
