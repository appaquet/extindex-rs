use std::io::{Error, Write};

pub struct CountedWrite<W: Write> {
    inner: W,
    written_count: u64,
}

impl<W: Write> CountedWrite<W> {
    pub fn new(write: W) -> CountedWrite<W> {
        CountedWrite {
            inner: write,
            written_count: 0,
        }
    }

    pub fn written_count(&self) -> u64 {
        self.written_count
    }
}

impl<W: Write> Write for CountedWrite<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let count = self.inner.write(buf)?;
        self.written_count += count as u64;
        Ok(count)
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_count_write() {
        let mut buf: Vec<u8> = Vec::new();

        let mut counted = CountedWrite::new(&mut buf);
        assert_eq!(counted.written_count(), 0);
        counted.write(&[0, 1, 2]);
        assert_eq!(counted.written_count(), 3);
    }

    #[test]
    fn test_not_completely_written() {
        struct MockWrite;
        impl Write for MockWrite {
            fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
                Ok(1)
            }

            fn flush(&mut self) -> Result<(), Error> {
                unimplemented!()
            }
        }

        let mut counted = CountedWrite::new(MockWrite);
        assert_eq!(counted.written_count(), 0);
        counted.write(&[0, 1, 2]);
        assert_eq!(counted.written_count(), 1);
    }
}
