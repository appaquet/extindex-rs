use std::{
    cmp::Ordering,
    io::{Read, Write},
};

use crate::{data, size::DataSize, Serializable};
use extsort::Sortable;

/// An entry to be indexed, or retrieved from the index.
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
        Self::new_sized(key, value)
    }
}

impl<K, V> Entry<K, V>
where
    K: Ord + Serializable,
    V: Serializable,
{
    pub fn new_sized(key: K, value: V) -> Entry<K, V> {
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

/// Entry wrapper used for sorting. This is used so that we can write entries
/// using `extsort` external sorter.
pub(crate) struct SortableEntry<K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
{
    pub(crate) entry: Entry<K, V>,
    _ks: std::marker::PhantomData<KS>,
    _vs: std::marker::PhantomData<VS>,
}

impl<K, V, KS, VS> SortableEntry<K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
{
    pub fn new(entry: Entry<K, V>) -> Self {
        Self {
            entry,
            _ks: std::marker::PhantomData,
            _vs: std::marker::PhantomData,
        }
    }

    pub fn into_inner(self) -> Entry<K, V> {
        self.entry
    }
}

impl<K, V, KS, VS> Sortable for SortableEntry<K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    fn encode<W: Write>(&self, output: &mut W) -> std::io::Result<()> {
        data::Entry::<K, V, KS, VS>::write(&self.entry, output).map_err(|err| err.into_io())?;
        Ok(())
    }

    fn decode<R: Read>(read: &mut R) -> std::io::Result<SortableEntry<K, V, KS, VS>> {
        let (entry, _read_size) =
            data::Entry::<K, V, KS, VS>::read(read).map_err(|err| err.into_io())?;
        Ok(SortableEntry {
            entry: entry.entry,
            _ks: std::marker::PhantomData,
            _vs: std::marker::PhantomData,
        })
    }
}

impl<K, V, KS, VS> Ord for SortableEntry<K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.entry.cmp(&other.entry)
    }
}

impl<K, V, KS, VS> PartialOrd for SortableEntry<K, V, KS, VS>
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

impl<K, V, KS, VS> PartialEq for SortableEntry<K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    fn eq(&self, other: &Self) -> bool {
        self.entry.eq(&other.entry)
    }
}

impl<K, V, KS, VS> Eq for SortableEntry<K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
}
