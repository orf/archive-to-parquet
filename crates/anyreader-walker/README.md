# anyreader-walker

A library for reading streams of compressed and uncompressed data without knowing the format in advance.

# Example:

```rust
use anyreader_walker::{AnyWalker, FileEntry, EntryDetails, FormatKind};
use std::io::Read;

#[derive(Default)]
struct Visitor {
    files: usize
}

impl AnyWalker for Visitor {
    fn visit_file_entry(&mut self, entry: &mut FileEntry<impl Read>) -> std::io::Result<()> {
        eprintln!("Found file: {}", entry.path().display());
        self.files += 1;
        Ok(())
    }

    fn begin_visit_archive(
        &mut self,
        details: &EntryDetails,
        format: FormatKind,
    ) -> std::io::Result<bool> {
        use anyreader_walker::EntryDetails;
        eprintln!("Found archive: {}", details.path.display());
        Ok(true)
    }
}

fn main() {
    let data = make_tar_zst_archive("hello world");
    let mut entry = FileEntry::from_reader("input.tar.zst", data.len() as u64, data.as_slice()).unwrap();
    // Or a file:
    // let mut entry = FileEntry::from_path("file.tar.zst").unwrap();
    let mut visitor = Visitor::default();
    visitor.walk(entry).unwrap();
    assert_eq!(visitor.files, 1);
}

fn make_tar_zst_archive(data: &str) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_size(data.len() as u64);
    builder.append_data(&mut header, "file-name", data.as_bytes()).unwrap();
    let tar_file = builder.into_inner().unwrap();
    zstd::encode_all(&tar_file[..], 1).unwrap()
}
```
