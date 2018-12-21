#[macro_use]
extern crate log;

use std::cmp::Ordering;
use std::io::{Read, Write};

use extsort::Sortable;

pub mod builder;
pub mod reader;
pub use crate::builder::Builder;
pub use crate::reader::Reader;

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
