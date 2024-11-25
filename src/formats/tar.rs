use crate::items::Items;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;

pub fn read_tar(source: PathBuf) -> anyhow::Result<Items> {
    let reader = BufReader::new(File::open(&source)?);
    let mut archive = tar::Archive::new(reader);
    let mut items = Items::new_with_capacity(source.to_string_lossy().to_string(), 100);
    let mut buffer = vec![];
    for entry in archive.entries()? {
        let mut entry = entry?;
        if entry.header().entry_type() != tar::EntryType::Regular {
            continue;
        }
        let Ok(path) = entry.path() else { continue };
        let Some(path) = path.to_str() else { continue };
        let size = entry.header().size()?;

        items.paths.append_value(path);
        items.sizes.append_value(size);

        if size == 0 {
            items.data.append_value([]);
            continue;
        }

        buffer.reserve(size as usize);
        entry.read_to_end(&mut buffer)?;

        items.data.append_value(&buffer);

        buffer.clear();
    }
    Ok(items)
}
