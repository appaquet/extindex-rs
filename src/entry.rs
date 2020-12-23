use std::{
    cmp::Ordering,
    io::{Read, Write},
};

use crate::seri;
use extsort::Sortable;

/// Trait representing a structure that can be encoded / serialized to a Writer
/// and decoded / deserialized from a Reader.
pub trait Encodable: Send + Sized {
    /// Exact size that the encoded item will have, if known. If none is
    /// returned, the encoding will be buffered in memory.
    fn encoded_size(&self) -> Option<usize>;

    /// Encodes the given item to the writer
    fn encode<W: Write>(&self, write: &mut W) -> Result<(), std::io::Error>;

    /// Decodes the given from the reader
    fn decode<R: Read>(data: &mut R, size: usize) -> Result<Self, std::io::Error>;
}

/// An entry to be indexed, or retrieved from the index
pub struct Entry<K, V>
where
    K: Ord + Encodable,
    V: Encodable,
{
    pub(crate) key: K,
    pub(crate) value: V,
}

impl<K, V> Entry<K, V>
where
    K: Ord + Encodable,
    V: Encodable,
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

impl<K, V> Sortable for Entry<K, V>
where
    K: Ord + Encodable,
    V: Encodable,
{
    fn encode<W: Write>(&self, output: &mut W) {
        let _ = seri::Entry::write(self, output);
    }

    fn decode<R: Read>(read: &mut R) -> Option<Entry<K, V>> {
        let (entry, _read_size) = seri::Entry::read(read).ok()?;
        Some(entry.entry)
    }
}

impl<K, V> Ord for Entry<K, V>
where
    K: Ord + Encodable,
    V: Encodable,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<K, V> PartialOrd for Entry<K, V>
where
    K: Ord + Encodable,
    V: Encodable,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl<K, V> PartialEq for Entry<K, V>
where
    K: Ord + Encodable,
    V: Encodable,
{
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<K, V> Eq for Entry<K, V>
where
    K: Ord + Encodable,
    V: Encodable,
{
}
