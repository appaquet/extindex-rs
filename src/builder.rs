use std;
use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use extsort::{ExternalSorter, Sortable};

use crate::utils::CountedWrite;
use crate::{Encodable, Entry};

const MAX_LEVELS: usize = 256;
const MAX_KEY_SIZE_BYTES: usize = 1024;
const MAX_VALUE_SIZE_BYTES: usize = 65000;

pub struct Builder<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    path: PathBuf,
    log_base: f64,
    phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Builder<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    pub fn new(path: PathBuf) -> Builder<K, V> {
        Builder {
            path,
            log_base: 5.0,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn build<I>(self, iter: I) -> Result<(), Error>
    where
        I: Iterator<Item = Entry<K, V>>,
    {
        let sort_dir = self.path.with_extension("tmp_sort");
        std::fs::create_dir_all(&sort_dir)?;

        let mut sorter = ExternalSorter::new();
        sorter.set_sort_dir(sort_dir);

        let sorted_iter = sorter.sort(iter)?;
        let sorted_count = sorted_iter.sorted_count();

        self.build_from_sorted(sorted_iter, sorted_count)
    }

    pub fn build_from_sorted<I>(self, iter: I, nb_items: u64) -> Result<(), Error>
    where
        I: Iterator<Item = Entry<K, V>>,
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(false)
            .write(true)
            .open(&self.path)?;
        let mut buffered_output = BufWriter::new(file);
        let mut counted_output = CountedWrite::new(buffered_output);

        let mut levels = levels_for_items_count(nb_items, self.log_base);
        if levels.len() > MAX_LEVELS {
            return Err(Error::MaxSize);
        }

        self.write_header(&mut counted_output, &levels)?;

        // how often we write checkpoint is the expected items at last level
        let checkpoint_interval = levels.last().unwrap().expected_items;

        let mut entries_since_last_checkpoint = 0;
        let mut last_entry_position: u64 = 0;
        for entry in iter {
            last_entry_position = counted_output.written_count();
            self.write_entry(&mut counted_output, entry)?;
            entries_since_last_checkpoint += 1;

            if entries_since_last_checkpoint >= checkpoint_interval {
                for level in &mut levels {
                    level.current_items += entries_since_last_checkpoint;
                }

                let current_position = counted_output.written_count();
                self.write_levels_checkpoint(
                    &mut counted_output,
                    current_position,
                    last_entry_position,
                    &mut levels,
                    false,
                )?;
                entries_since_last_checkpoint = 0;
            }
        }

        // if we have written any items since last checkpoint
        if entries_since_last_checkpoint > 0 {
            let current_position = counted_output.written_count();
            self.write_levels_checkpoint(
                &mut counted_output,
                current_position,
                last_entry_position,
                &mut levels,
                true,
            )?;
        }

        Ok(())
    }

    fn write_header(&self, output: &mut Write, levels: &[Level]) -> Result<(), Error> {
        output.write(&crate::INDEX_FILE_MAGIC_HEADER)?;
        output.write(&[crate::INDEX_FILE_VERSION])?;
        output.write_u8(levels.len() as u8)?;

        Ok(())
    }

    fn write_entry(&self, output: &mut Write, entry: Entry<K, V>) -> Result<(), Error> {
        let key_size = <K as Encodable<K>>::encode_size(&entry.key);
        let value_size = <V as Encodable<V>>::encode_size(&entry.value);

        if key_size > MAX_KEY_SIZE_BYTES || value_size > MAX_VALUE_SIZE_BYTES {
            error!(
                "One item exceeds maximum encoded size: key {} > {} OR value {} > {}",
                key_size, MAX_KEY_SIZE_BYTES, value_size, MAX_VALUE_SIZE_BYTES
            );
            return Err(Error::InvalidItem);
        }

        output.write_u16::<LittleEndian>(key_size as u16)?;
        output.write_u16::<LittleEndian>(value_size as u16)?;
        <K as Encodable<K>>::encode(&entry.key, output)?;
        <V as Encodable<V>>::encode(&entry.value, output)?;

        Ok(())
    }

    fn write_levels_checkpoint(
        &self,
        output: &mut Write,
        current_position: u64,
        last_entry_position: u64,
        levels: &mut [Level],
        force_write: bool,
    ) -> Result<(), Error> {
        output.write_u64::<LittleEndian>(last_entry_position)?;

        for (idx, level) in levels.iter_mut().enumerate() {
            output.write_u64::<LittleEndian>(level.last_item.unwrap_or(0))?;

            const WRITE_UPCOMING_WITHIN_DISTANCE: u64 = 2;
            if force_write
                || (level.expected_items - level.current_items) <= WRITE_UPCOMING_WITHIN_DISTANCE
            {
                println!(
                    "Level {} Pos {} Item {} Last {:?} Force {}",
                    idx, current_position, last_entry_position, level.last_item, force_write
                );
                level.last_item = Some(current_position);
                level.current_items = 0;
            }
        }

        Ok(())
    }

    fn write_footer(&self, output: &mut Write, levels: &[Level]) -> Result<(), Error> {
        for level in levels {}
        Ok(())
    }
}

fn levels_for_items_count(nb_items: u64, log_base: f64) -> Vec<Level> {
    let nb_levels = (nb_items as f64).log(log_base).round().max(1.0) as u64;
    let log_base_u64 = log_base as u64;

    let mut levels = Vec::new();
    let mut max_items = (nb_items / log_base_u64).max(1);
    for idx in 0..nb_levels {
        if levels.len() != 0 && max_items < log_base_u64 {
            break;
        }

        levels.push(Level {
            expected_items: max_items,
            current_items: 0,
            last_item: None,
        });
        max_items /= log_base_u64;
    }

    levels
}

#[derive(Debug)]
struct Level {
    expected_items: u64,
    current_items: u64,
    last_item: Option<u64>,
}

#[derive(Debug)]
pub enum Error {
    MaxSize,
    InvalidItem,
    IO(std::io::Error),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IO(err)
    }
}

#[cfg(test)]
mod tests {
    use tempdir;

    use super::*;

    #[test]
    fn test_calculate_levels() {
        let levels = levels_for_items_count(0, 5.0);
        let levels_items = levels.iter().map(|l| l.expected_items).collect::<Vec<_>>();
        assert_eq!(levels_items, vec![1]); // 0 items would still have 1 item per level

        let levels = levels_for_items_count(1, 5.0);
        let levels_items = levels.iter().map(|l| l.expected_items).collect::<Vec<_>>();
        assert_eq!(levels_items, vec![1]);

        let levels = levels_for_items_count(20, 5.0);
        let levels_items = levels.iter().map(|l| l.expected_items).collect::<Vec<_>>();
        assert_eq!(levels_items, vec![4]);

        let levels = levels_for_items_count(1_000, 5.0);
        let levels_items = levels.iter().map(|l| l.expected_items).collect::<Vec<_>>();
        assert_eq!(levels_items, vec![200, 40, 8]);

        let levels = levels_for_items_count(1_000_000, 10.0);
        let levels_items = levels.iter().map(|l| l.expected_items).collect::<Vec<_>>();
        assert_eq!(levels_items, vec![100_000, 10_000, 1000, 100, 10]);
    }

    #[test]
    fn test_build_from_unsorted() {
        let mut data= Vec::new();

        for i in 0..100 {
            data.push(crate::Entry::new("ccc".to_string(), "ccc".to_string()));
        }

        let tempdir = tempdir::TempDir::new("extindex").unwrap();
        let index_path = tempdir.path().join("index.idx");
        let builder = Builder::<String, String>::new(index_path.clone());

        builder.build(data.into_iter()).unwrap();

        let file_meta = std::fs::metadata(&index_path).unwrap();
        assert!(file_meta.len() > 10);
    }

    impl Encodable<String> for String {
        fn encode_size(item: &String) -> usize {
            item.as_bytes().len()
        }

        fn encode(item: &String, write: &mut Write) -> Result<(), std::io::Error> {
            write.write(item.as_bytes())?;
            Ok(())
        }

        fn decode(data: &[u8]) -> Result<String, std::io::Error> {
            Ok(String::from_utf8_lossy(data).to_string())
        }
    }
}
