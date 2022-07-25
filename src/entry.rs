use std::{
    cmp::Ordering,
    io::{Read, Write},
};

use crate::{seri, Encodable};
use extsort::Sortable;

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
