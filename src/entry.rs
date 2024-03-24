use std::{
    cmp::Ordering,
    io::{Read, Write},
};

use crate::{data, size::DataSize, Serializable};
use extsort::Sortable;

/// An entry to be indexed, or retrieved from the index
pub struct Entry<K, V, KS = u16, VS = u16>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    pub(crate) key: K,
    pub(crate) value: V,
    pub(crate) _phantom: std::marker::PhantomData<(KS, VS)>,
}

impl<K, V> Entry<K, V, u16, u16>
where
    K: Ord + Serializable,
    V: Serializable,
{
    pub fn new(key: K, value: V) -> Entry<K, V, u16, u16> {
        Self::new_sized(key, value)
    }
}

impl<K, V, KS, VS> Entry<K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    pub fn new_sized(key: K, value: V) -> Entry<K, V, KS, VS> {
        Entry {
            key,
            value,
            _phantom: std::marker::PhantomData,
        }
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

impl<K, V, KS, VS> Sortable for Entry<K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    fn encode<W: Write>(&self, output: &mut W) -> std::io::Result<()> {
        data::Entry::write(self, output).map_err(|err| err.into_io())?;
        Ok(())
    }

    fn decode<R: Read>(read: &mut R) -> std::io::Result<Entry<K, V, KS, VS>> {
        let (entry, _read_size) = data::Entry::read(read).map_err(|err| err.into_io())?;
        Ok(entry.entry)
    }
}

impl<K, V, KS, VS> Ord for Entry<K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<K, V, KS, VS> PartialOrd for Entry<K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, V, KS, VS> PartialEq for Entry<K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<K, V, KS, VS> Eq for Entry<K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
}

impl<K: std::fmt::Debug, V: std::fmt::Debug, KS, VS> std::fmt::Debug for Entry<K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("key", &self.key)
            .field("value", &self.value)
            .finish()
    }
}
