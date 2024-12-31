use extract_strings::AsciiStrings;
use std::io::{BufReader, Write};

fn main() {
    let path = std::env::args().nth(1).expect("no path provided");
    let file = std::fs::File::open(path).expect("failed to open file");
    let reader = BufReader::new(file);

    let mut out = std::io::stdout().lock();

    let mut total = 0;
    for string in reader.iter_ascii_strings(10) {
        writeln!(out, "{}", string).unwrap();
        total += 1;
    }
    writeln!(out, "Total strings: {total}").unwrap();
}
