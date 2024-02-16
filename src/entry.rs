use std::{
    cmp::Ordering,
    io::{Read, Write},
};

use crate::{data, Serializable};
use extsort::Sortable;

/// An entry to be indexed, or retrieved from the index
pub struct Entry<K, V>
where
    K: Ord + Serializable,
    V: Serializable,
{
    pub(crate) key: K,
    pub(crate) value: V,
}

impl<K, V> Entry<K, V>
where
    K: Ord + Serializable,
    V: Serializable,
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

    pub fn into_inner(self) -> (K, V) {
        (self.key, self.value)
    }
}

impl<K, V> Sortable for Entry<K, V>
where
    K: Ord + Serializable,
    V: Serializable,
{
    fn encode<W: Write>(&self, output: &mut W) {
        let _ = data::Entry::write(self, output);
    }

    fn decode<R: Read>(read: &mut R) -> Option<Entry<K, V>> {
        let (entry, _read_size) = data::Entry::read(read).ok()?;
        Some(entry.entry)
    }
}

impl<K, V> Ord for Entry<K, V>
where
    K: Ord + Serializable,
    V: Serializable,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<K, V> PartialOrd for Entry<K, V>
where
    K: Ord + Serializable,
    V: Serializable,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, V> PartialEq for Entry<K, V>
where
    K: Ord + Serializable,
    V: Serializable,
{
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<K, V> Eq for Entry<K, V>
where
    K: Ord + Serializable,
    V: Serializable,
{
}

impl<K: std::fmt::Debug, V: std::fmt::Debug> std::fmt::Debug for Entry<K, V>
where
    K: Ord + Serializable,
    V: Serializable,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("key", &self.key)
            .field("value", &self.value)
            .finish()
    }
}
