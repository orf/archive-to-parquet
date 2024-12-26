use anyreader::{AnyFormat, FormatKind};
use std::fs::File;

fn main() {
    // Supports compressed files
    let file = File::open("file.zstd").unwrap();
    let mut reader = AnyFormat::from_reader(file).unwrap();
    assert_eq!(reader.kind, FormatKind::Zstd);
    // Read the data
    std::io::copy(&mut reader, &mut std::io::stdout()).unwrap();

    // Supports archives
    let file = File::open("file.tar.gz").unwrap();
    let reader = AnyFormat::from_reader(file).unwrap();
    assert_eq!(reader.kind, FormatKind::Tar);
    let mut archive = tar::Archive::new(reader);
    for entry in archive.entries().unwrap() {
        println!("{:?}", entry.unwrap().path());
    }
}
