// Copyright 2018 Andre-Philippe Paquet
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    fn simple_count_write() {
        let mut buf: Vec<u8> = Vec::new();

        let mut counted = CountedWrite::new(&mut buf);
        assert_eq!(counted.written_count(), 0);
        counted.write_all(&[0, 1, 2]).unwrap();
        assert_eq!(counted.written_count(), 3);
    }

    #[test]
    fn not_completely_written() {
        struct MockWrite;
        impl Write for MockWrite {
            fn write(&mut self, _buf: &[u8]) -> Result<usize, Error> {
                Ok(1)
            }

            fn flush(&mut self) -> Result<(), Error> {
                unimplemented!()
            }
        }

        let mut counted = CountedWrite::new(MockWrite);
        assert_eq!(counted.written_count(), 0);
        let count = counted.write(&[0, 1, 2]).unwrap();
        assert_eq!(count, 1);
        assert_eq!(counted.written_count(), 1);
    }
}
