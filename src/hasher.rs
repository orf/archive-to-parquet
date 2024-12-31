use ring::digest::{Digest, SHA256_OUTPUT_LEN};
use std::io::Write;
pub const HASH_SIZE: usize = SHA256_OUTPUT_LEN;

pub struct HashedWriter<T: Write> {
    inner: T,
    hasher: ring::digest::Context,
    written: usize,
}

impl<T: Write> HashedWriter<T> {
    pub fn new(writer: T) -> Self {
        Self {
            inner: writer,
            hasher: ring::digest::Context::new(&ring::digest::SHA256),
            written: 0,
        }
    }

    pub fn into_inner(self) -> (Digest, u64) {
        let finished = self.hasher.finish();
        (finished, self.written as u64)
    }
}

impl<T: Write> Write for HashedWriter<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        let written = self.inner.write(buf)?;
        self.written += written;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
