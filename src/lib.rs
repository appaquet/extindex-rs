
#[macro_use]
extern crate log;

use extsort::{ExternalSorter, Sortable};
use std::cmp::Ordering;
use std::io::{Read, Write};

const INDEX_FILE_MAGIC_HEADER: [u8; 2] = [40, 12];
const INDEX_FILE_VERSION: u8 = 0;

pub mod builder;
pub mod index;
mod utils;

pub trait Encodable<T> {
    fn encode_size(item: &T) -> usize;
    fn encode(item: &T, write: &mut Write) -> Result<(), std::io::Error>;
    fn decode(data: &[u8]) -> Result<T, std::io::Error>;
}


pub struct Entry<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    key: K,
    value: V,
}

impl <K,V> Entry<K,V>
    where
        K: Ord + Encodable<K>,
        V: Encodable<V>,
{
    pub fn new(key: K, value: V) -> Entry<K, V> {
        Entry {
            key, value
        }
    }
}

impl<K, V> Sortable<Entry<K, V>> for Entry<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    fn encode(item: &Entry<K, V>, write: &mut Write) {
        unimplemented!()
    }

    fn decode(read: &mut Read) -> Option<Entry<K, V>> {
        unimplemented!()
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
