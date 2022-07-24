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

//! Immutable persisted index (on disk) that can be built in one pass using a
//! sorted iterator, or can use [extsort](https://crates.io/crates/extsort) to externally sort the iterator first, and
//! then build the index from it.
//!
//! The index allows random lookups and sorted scans. An indexed entry consists
//! of a key and a value. The key needs to implement `Eq` and `Ord`, and both
//! the key and values need to implement a `Encodable` trait for serialization
//! to and from disk.
//!
//! The index is built using a skip list like data structure, but in which
//! lookups are starting from the end of the index instead of from the
//! beginning. This allow building the index in a single pass on a sorted
//! iterator, since starting from the beginning would require knowing
//! checkpoints/nodes ahead in the file.
//!
//! # Examples
//! ```rust
//! extern crate extindex;
//!
//! use std::io::{Read, Write};
//! use extindex::{Builder, Encodable, Entry, Reader};
//!
//! #[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
//! struct TestString(String);
//!
//! impl Encodable for TestString {
//!     fn encoded_size(&self) -> Option<usize> {
//!         Some(self.0.as_bytes().len())
//!     }
//!
//!     fn encode<W: Write>(&self, write: &mut W) -> Result<(), std::io::Error> {
//!         write.write_all(self.0.as_bytes()).map(|_| ())
//!     }
//!
//!     fn decode<R: Read>(data: &mut R, size: usize) -> Result<TestString, std::io::Error> {
//!         let mut bytes = vec![0u8; size];
//!         data.read_exact(&mut bytes)?;
//!         Ok(TestString(String::from_utf8_lossy(&bytes).to_string()))
//!     }
//! }
//!
//! let index_file = tempfile::NamedTempFile::new().unwrap();
//!
//! let builder = Builder::new(index_file.path());
//! let entries = vec![
//!    Entry::new(TestString("mykey".to_string()), TestString("my value".to_string()))
//! ];
//! builder.build(entries.into_iter()).unwrap();
//!
//! let reader = Reader::<TestString, TestString>::open(index_file).unwrap();
//! assert!(reader.find(&TestString("mykey".to_string())).unwrap().is_some());
//! assert!(reader.find(&TestString("notfound".to_string())).unwrap().is_none());
//! ```

#[macro_use]
extern crate log;

pub use crate::{
    builder::{Builder, BuilderError},
    encodable::Encodable,
    entry::Entry,
    reader::{Reader, ReaderError},
};

pub mod builder;
pub mod encodable;
pub mod entry;
pub mod reader;

mod seri;
mod utils;

#[cfg(test)]
pub mod tests {
    use std::io::{Read, Write};

    #[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
    pub struct TestString(pub String);

    impl super::Encodable for TestString {
        fn encoded_size(&self) -> Option<usize> {
            Some(self.0.as_bytes().len())
        }

        fn encode<W: Write>(&self, write: &mut W) -> Result<(), std::io::Error> {
            write.write_all(self.0.as_bytes()).map(|_| ())
        }

        fn decode<R: Read>(data: &mut R, size: usize) -> Result<TestString, std::io::Error> {
            let mut bytes = vec![0u8; size];
            data.read_exact(&mut bytes)?;
            Ok(TestString(String::from_utf8_lossy(&bytes).to_string()))
        }
    }
}
