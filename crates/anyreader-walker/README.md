# anyreader-walker

This crate provides a simple way to recursively walk through compressed archives/data.

## Example

```rust
use anyreader_walker::{AnyWalker, FileEntry};

struct Visitor;

impl AnyWalker for Visitor {
    fn visit_file_entry(&mut self, entry: &mut FileEntry<impl Read>) -> std::io::Result<()> {
        eprintln!("Found file: {}", entry.path().display());
        Ok(())
    }

    fn begin_visit_archive(
        &mut self,
        details: &EntryDetails,
        format: FormatKind,
    ) -> std::io::Result<bool> {
        eprintln!("Found archive: {}", details.path.display());
        Ok(true)
    }
}

fn main() {
    let mut entry = FileEntry::from_path("file.tar.gz").unwrap();
    let mut visitor = Visitor;
    walker.walk(&mut visitor).unwrap();
}
```
