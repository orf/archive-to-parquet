mod compression;
mod format;

pub use crate::anyreader::compression::AnyReader;
pub use crate::anyreader::format::{AnyFormat, FormatKind};
use peekable::Peekable;
use std::io::Read;

#[inline(always)]
pub(crate) fn peek_upto<const N: usize>(reader: &mut Peekable<impl Read>) -> &[u8] {
    let buf = reader.get_ref().0;
    let end = N.min(buf.len());
    &buf[..end]
}
