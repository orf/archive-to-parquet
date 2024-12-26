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
