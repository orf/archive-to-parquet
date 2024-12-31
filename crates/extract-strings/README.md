# Extract strings

A small utility to extract ascii strings from binary content.

## Usage

```rust
use std::io::{BufReader, Write};
use extract_strings::AsciiStrings;

fn main() {
    let path = std::env::args().nth(1).expect("no path provided");
    let file = std::fs::File::open(path).expect("failed to open file");
    let reader = BufReader::new(file);

    let mut total = 0;
    for string in reader.iter_ascii_strings(10) {
        println!(out, "{}", string).unwrap();
        total += 1;
    }
    println!(out, "Total strings: {total}").unwrap();
}
```
