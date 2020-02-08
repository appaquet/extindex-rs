use std::cmp::Ordering;
use std::io::{Read, Write};

use crate::seri;
use extsort::Sortable;

///
/// Trait representing a structure that can be encoded / serialized to a Writer and decoded / deserialized
/// from a Reader.
///
pub trait Encodable<T> {
    ///
    /// Exact size that the encoded item will have, if known. If none is returned, the encoding
    /// will be buffered in memory.
    ///
    fn encode_size(item: &T) -> Option<usize>;

    ///
    /// Encode the given item to the writer
    ///
    fn encode<W: Write>(item: &T, write: &mut W) -> Result<(), std::io::Error>;

    /// Decode the given from the reader
    ///
    fn decode<R: Read>(data: &mut R, size: usize) -> Result<T, std::io::Error>;
}

///
/// An entry to be indexed, or retrieved from the index
///
pub struct Entry<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    pub(crate) key: K,
    pub(crate) value: V,
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
    fn encode<W: Write>(entry: Entry<K, V>, output: &mut W) {
        let seri_entry = seri::Entry { entry };
        let _ = seri_entry.write(output);
    }

    fn decode<R: Read>(read: &mut R) -> Option<Entry<K, V>> {
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
