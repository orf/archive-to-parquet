use crate::formats::{Format, MIN_FILE_SIZE};
use crate::items::{Items, ItemsError};
use std::fmt::Display;
use std::io::Read;
use tar::Archive;
use tracing::{debug, info, trace};

#[tracing::instrument(skip(source, reader, items), fields(%source))]
pub fn extract(
    source: impl AsRef<str> + Display,
    reader: impl Read,
    items: &mut Items,
    recursive: bool,
) -> Result<(), ItemsError> {
    info!("Extracting tar archive from {}", source.as_ref());
    let mut archive = Archive::new(reader);
    let mut buffer = vec![];
    for entry in archive.entries()? {
        buffer.clear();

        let mut entry = entry?;
        if entry.header().entry_type() != tar::EntryType::Regular {
            continue;
        }
        let Ok(path) = entry.path() else { continue };
        let Some(path) = path.to_str() else { continue };
        let size = entry.header().size()?;
        let path = path.to_string();
        trace!(%path, size, "read path");

        let data = if size < MIN_FILE_SIZE as u64 {
            continue;
        } else {
            buffer.reserve(size as usize);
            entry.read_to_end(&mut buffer)?;
            buffer.as_slice()
        };

        if recursive {
            if let Ok(format) = Format::detect_type(buffer.as_slice()) {
                debug!(%path, %format, "detected format");
                format.extract(
                    format!("{}/{path}", source.as_ref()),
                    buffer.as_slice(),
                    items,
                    false,
                )?;
                continue;
            }
        }
        items.add_record(&source, path.as_str(), size, data)?;
    }

    Ok(())
}
