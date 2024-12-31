use std::fmt::{Debug, Formatter};
use std::io::Read;
use std::ops::Range;

#[inline(always)]
fn is_text(c: u8) -> bool {
    c != 0 && c.is_ascii() && !c.is_ascii_control()
}

macro_rules! debug_log {
    ($fmt:expr) => {
        if cfg!(test) {
            eprintln!($fmt);
        }
    };
    ($fmt:expr, $($args:tt)*) => {
        if cfg!(test) {
            eprintln!($fmt, $($args)*);
        }
    };
}

pub(crate) struct AsciiIterator<T, const N: usize> {
    slice_range: Range<usize>,
    current_run: Option<Range<usize>>,
    reader: T,
    slice: [u8; N],
    min_length: usize,
    partial_string: Vec<u8>,
}

impl<T> AsciiIterator<T, 1024> {
    pub fn new(reader: T, min_length: usize) -> Self {
        Self::new_with_buf_size(reader, min_length)
    }
}

impl<T, const N: usize> AsciiIterator<T, N> {
    pub(crate) fn new_with_buf_size(reader: T, min_length: usize) -> Self {
        Self {
            slice_range: 0..0,
            current_run: None,
            reader,
            slice: [0; N],
            min_length,
            partial_string: Vec::new(),
        }
    }
}

impl<T, const N: usize> Debug for AsciiIterator<T, N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsciiIterator")
            .field("slice_range", &self.slice_range)
            .field("current_run", &self.current_run)
            .finish()
    }
}

impl<T: Read, const N: usize> Iterator for AsciiIterator<T, N> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            debug_log!("Started iteration: {:?}", self);
            if self.slice_range.is_empty() {
                let read = self.reader.read(&mut self.slice).unwrap();
                debug_log!("Read {} bytes", read);
                if read == 0 {
                    if self.partial_string.len() >= self.min_length {
                        let res = std::str::from_utf8(&self.partial_string)
                            .unwrap()
                            .to_string();
                        self.partial_string.clear();
                        return Some(res);
                    }
                    return None;
                }
                self.slice_range.end = read;
            }

            for (byte, idx) in self.slice[self.slice_range.clone()]
                .iter()
                .zip(self.slice_range.clone())
            {
                debug_log!("Loop: self={self:?}, idx={idx}, byte={byte}");
                match (is_text(*byte), &mut self.current_run) {
                    (false, None) => continue,
                    (false, Some(range)) => {
                        self.partial_string
                            .extend_from_slice(&self.slice[range.start..idx]);

                        self.current_run = None;
                        self.slice_range = idx..self.slice_range.end;

                        if self.partial_string.len() >= self.min_length {
                            let res = std::str::from_utf8(&self.partial_string)
                                .unwrap()
                                .to_string();
                            self.partial_string.clear();
                            debug_log!("Found string: {:?} - (self={self:?})", res);
                            return Some(res);
                        }
                    }
                    (true, None) => {
                        self.current_run = Some(idx..idx + 1);
                    }
                    (true, Some(range)) => {
                        range.end = idx + 1;
                    }
                }
            }

            self.slice_range = 0..0;
            debug_log!("Finished iteration");

            if let Some(range) = &self.current_run {
                self.partial_string
                    .extend(&self.slice[range.start..range.end]);
                self.current_run = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INPUT: &[u8] =
        b"\0binary\0data\0\xFF\xFEHello, \xF0\x9F\x8C\x8E World!\0more binary".as_slice();
    const EXPECTED: &[&str] = &["binary", "data", "Hello, ", " World!", "more binary"];

    #[test]
    fn test_find_strings_reader() {
        let mut reader = INPUT;
        let reader = AsciiIterator::new(&mut reader, 1);
        let found: Vec<_> = reader.into_iter().collect();
        assert_eq!(found, EXPECTED);
    }

    // #[test]
    // fn test_find_strings() {
    //     let mut values = vec![];
    //
    //     let mut rest = INPUT;
    //     for expect in EXPECTED {
    //         let (r, found) = find_string(rest).expect(&format!("Failed to find {:?}", expect));
    //         values.push(found.unwrap());
    //         rest = r;
    //     }
    //     assert_eq!(values, EXPECTED);
    // }
}
