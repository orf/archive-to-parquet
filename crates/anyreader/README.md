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
    // Supports compressed files/data
    let data = zstd::encode_all("hello world".as_bytes(), 1).unwrap();
    // Or a file:
    // let data = File::open("file.zstd").unwrap();
    let mut reader = AnyReader::from_reader(data.as_slice()).unwrap();
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

fn make_tar_zst_archive(data: &str) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_size(data.len() as u64);
    builder.append_data(&mut header, "file-name", data.as_bytes()).unwrap();
    let tar_file = builder.into_inner().unwrap();
    zstd::encode_all(&tar_file[..], 1).unwrap()
}

fn main() {
    let data = make_tar_zst_archive("hello world");
    // Or a file:
    // let data = File::open("file.tar.zst").unwrap();
    let reader = AnyFormat::from_reader(data.as_slice()).unwrap();
    assert_eq!(reader.kind, FormatKind::Tar);
    let mut archive = tar::Archive::new(reader);
    // Process the archive
    for entry in archive.entries().unwrap() {
        println!("{:?}", entry.unwrap().path());
    }
}
```
