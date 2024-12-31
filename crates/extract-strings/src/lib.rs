mod ascii;
mod string_result;

// pub use crate::string_result::StringResult;
use std::io::Read;

pub trait AsciiStrings {
    fn iter_ascii_strings(self, min_length: usize) -> impl Iterator<Item = String>;
}

impl<T> AsciiStrings for T
where
    T: Read, // for<'a> &'a T: Read,
{
    fn iter_ascii_strings(self, min_length: usize) -> impl Iterator<Item = String> {
        ascii::AsciiIterator::new(self, min_length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_ascii_strings() {
        let data = b"hello\0world";
        let reader = Cursor::new(data);
        let strings: Vec<String> = reader.iter_ascii_strings(4).collect();
        assert_eq!(strings, vec!["hello", "world"]);
    }
}
