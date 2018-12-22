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

//! Immutable persisted index (on disk) that can be built in one pass using a sorted iterator, or
//! uses [extsort](https://crates.io/crates/extsort) to externally sort the iterator first, and
//! then build the index. The index allow random lookups and sorted scans. An indexed entry consists
//! of a key and a value. The key needs to implement `Eq` and `Ord`, and both the key and values
//! need to implement a `Encodable` trait for serialization to and from disk.
//!
//! # Examples
//! ```rust
//! extern crate extindex;
//! extern crate tempdir;
//!
//! use std::io::{Read, Write};
//! use extindex::{Builder, Encodable, Entry, Reader};
//!
//! #[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
//! struct TestString(String);
//!
//! impl Encodable<TestString> for TestString {
//!     fn encode_size(item: &TestString) -> usize {
//!         item.0.as_bytes().len()
//!     }
//!
//!     fn encode(item: &TestString, write: &mut Write) -> Result<(), std::io::Error> {
//!         write.write(item.0.as_bytes()).map(|_| ())
//!     }
//!
//!     fn decode(data: &mut Read, size: usize) -> Result<TestString, std::io::Error> {
//!         let mut bytes = vec![0u8; size];
//!         data.read_exact(&mut bytes)?;
//!         Ok(TestString(String::from_utf8_lossy(&bytes).to_string()))
//!     }
//! }
//!
//! let tempdir = tempdir::TempDir::new("extindex").unwrap();
//! let index_file = tempdir.path().join("index.idx");
//!
//! let builder = Builder::new(index_file.clone());
//! let entries = vec![
//!    Entry::new(TestString("mykey".to_string()), TestString("my value".to_string()))
//! ];
//! builder.build(entries.into_iter()).unwrap();
//!
//! let reader = Reader::<TestString, TestString>::open(&index_file).unwrap();
//! assert!(reader.find(&TestString("mykey".to_string())).unwrap().is_some());
//! assert!(reader.find(&TestString("notfound".to_string())).unwrap().is_none());
//! ```

#[macro_use]
extern crate log;

use std::cmp::Ordering;
use std::io::{Read, Write};

use extsort::Sortable;

pub use crate::builder::Builder;
pub use crate::reader::Reader;

pub mod builder;
pub mod reader;

mod seri;
mod utils;

pub trait Encodable<T> {
    fn encode_size(item: &T) -> usize;
    fn encode(item: &T, write: &mut Write) -> Result<(), std::io::Error>;
    fn decode(data: &mut Read, size: usize) -> Result<T, std::io::Error>;
}

pub struct Entry<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    key: K,
    value: V,
}

impl<K, V> Entry<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    pub fn new(key: K, value: V) -> Entry<K, V> {
        Entry { key, value }
    }

    pub fn key(&self) -> &K {
        &self.key
    }

    pub fn value(&self) -> &V {
        &self.value
    }
}

impl<K, V> Sortable<Entry<K, V>> for Entry<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    fn encode(entry: Entry<K, V>, output: &mut Write) {
        let seri_entry = seri::Entry { entry };
        let _ = seri_entry.write(output);
    }

    fn decode(read: &mut Read) -> Option<Entry<K, V>> {
        let (entry, _read_size) = seri::Entry::read(read).ok()?;
        Some(entry.entry)
    }
}

impl<K, V> Ord for Entry<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<K, V> PartialOrd for Entry<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl<K, V> PartialEq for Entry<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<K, V> Eq for Entry<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
    pub struct TestString(pub String);

    impl super::Encodable<TestString> for TestString {
        fn encode_size(item: &TestString) -> usize {
            item.0.as_bytes().len()
        }

        fn encode(item: &TestString, write: &mut std::io::Write) -> Result<(), std::io::Error> {
            write.write(item.0.as_bytes()).map(|_| ())
        }

        fn decode(data: &mut Read, size: usize) -> Result<TestString, std::io::Error> {
            let mut bytes = vec![0u8; size];
            data.read_exact(&mut bytes)?;
            Ok(TestString(String::from_utf8_lossy(&bytes).to_string()))
        }
    }
}
