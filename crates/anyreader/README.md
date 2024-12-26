# anyreader

This crate provides a simple way to detect and read compressed streams of data in a
transparent way. It supports the following compression formats:

- Gzip
- Zstd
- Bzip2
- Xz

And it can detect the following archive formats:

- Tar
- Zip

## Example: Reading compressed streams

```rust
use anyreader::AnyReader;
use std::fs::File;

fn main() {
    // Supports compressed files
    let file = File::open("file.zstd").unwrap();
    let mut reader = AnyReader::from_reader(file).unwrap();
    assert!(reader.is_zst());
    // Read the data
    assert_eq!(std::io::read_to_string(&mut reader).unwrap(), "hello world");
}
```


## Example: Detecting archive types

```rust
use anyreader::{AnyFormat, FormatKind};
use std::fs::File;
use tar::Archive;

fn main() {
    let file = File::open("file.tar.gz").unwrap();
    let reader = AnyFormat::from_reader(file).unwrap();
    assert_eq!(reader.kind, FormatKind::Tar);
    let archive = tar::Archive::new(reader);
    // Process the archive
    for entry in archive.entries().unwrap() {
        println!("{:?}", entry.unwrap().path());
    }
}
```
