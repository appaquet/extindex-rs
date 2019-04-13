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

use std;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use extsort::ExternalSorter;

use crate::seri;
use crate::utils::CountedWrite;
use crate::{Encodable, Entry};

const CHECKPOINT_WRITE_UPCOMING_WITHIN_DISTANCE: u64 = 3;
const LEVELS_MINIMUM_ITEMS: u64 = 2;

///
/// Index builder that creates a file index from any iterator. If the given iterator is already sorted,
/// the `build_from_sorted` method can be used, while `build` can take any iterator. The `build` method
/// will sort the iterator first using external sorting exposed by the `extsort` crate.
///
/// As the index is being built, checkpoints / nodes are added to the file. These checkpoints / nodes
/// are similar to the ones in a skip list, except that they point to previous checkpoints instead of
/// pointing to next checkpoints.
///
pub struct Builder<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    path: PathBuf,
    log_base: f64,
    extsort_max_size: Option<usize>,
    phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Builder<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    ///
    /// Create an index builder that will write to the given file path.
    ///
    pub fn new(path: PathBuf) -> Builder<K, V> {
        Builder {
            path,
            log_base: 5.0,
            extsort_max_size: None,
            phantom: std::marker::PhantomData,
        }
    }

    ///
    /// Indicate approximately how many items we want per last-level checkpoint. A higher value
    /// means that once a checkpoint in which we know the item is after is found, we may need to
    /// iterate through up to `log_base` items. A lower value will prevent creating too many levels
    /// when the index gets bigger, but will require more scanning through more entries to find the
    /// right one.
    ///
    /// Default value: 5.0
    ///
    pub fn with_log_base(mut self, log_base: f64) -> Self {
        self.log_base = log_base;
        self
    }

    ///
    /// When using the `build` method from a non-sorted iterator, this value is passed to the
    /// `extsort` crate to define how many items will be buffered to memory before being dumped
    /// to disk.
    ///
    /// This number is the actual number of entries, not the sum of their size.
    ///
    pub fn with_extsort_max_size(mut self, max_size: usize) -> Self {
        self.extsort_max_size = Some(max_size);
        self
    }

    ///
    /// Build the index using a non-sorted iterator.
    ///
    pub fn build<I>(self, iter: I) -> Result<(), BuilderError>
    where
        I: Iterator<Item = Entry<K, V>>,
    {
        let sort_dir = self.path.with_extension("tmp_sort");
        std::fs::create_dir_all(&sort_dir)?;

        let mut sorter = ExternalSorter::new();
        sorter.set_sort_dir(sort_dir);

        if let Some(max_size) = self.extsort_max_size {
            sorter.set_max_size(max_size);
        }

        let sorted_iter = sorter.sort(iter)?;
        let sorted_count = sorted_iter.sorted_count();

        self.build_from_sorted(sorted_iter, sorted_count)
    }

    ///
    /// Build the index using a sorted iterator.
    ///
    pub fn build_from_sorted<I>(self, iter: I, nb_items: u64) -> Result<(), BuilderError>
    where
        I: Iterator<Item = Entry<K, V>>,
    {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(false)
            .write(true)
            .open(&self.path)?;
        let buffered_output = BufWriter::new(file);
        let mut counted_output = CountedWrite::new(buffered_output);

        let mut levels = levels_for_items_count(nb_items, self.log_base);
        if levels.len() > seri::MAX_LEVELS {
            return Err(BuilderError::MaxSize);
        }

        self.write_header(&mut counted_output, &levels)?;

        if !levels.is_empty() {
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
                    self.write_checkpoint(
                        &mut counted_output,
                        current_position,
                        last_entry_position,
                        &mut levels,
                        false,
                    )?;
                    entries_since_last_checkpoint = 0;
                }
            }

            if entries_since_last_checkpoint > 0 {
                let current_position = counted_output.written_count();
                self.write_checkpoint(
                    &mut counted_output,
                    current_position,
                    last_entry_position,
                    &mut levels,
                    true,
                )?;
            }
        }

        Ok(())
    }

    fn write_header(&self, output: &mut Write, levels: &[Level]) -> Result<(), BuilderError> {
        let seri_header = seri::Header {
            nb_levels: levels.len() as u8,
        };
        seri_header.write(output)?;
        Ok(())
    }

    fn write_entry(&self, output: &mut Write, entry: Entry<K, V>) -> Result<(), BuilderError> {
        let seri_entry = seri::Entry { entry };
        seri_entry.write(output)?;
        Ok(())
    }

    fn write_checkpoint(
        &self,
        output: &mut Write,
        current_position: u64,
        entry_position: u64,
        levels: &mut [Level],
        force_write: bool,
    ) -> Result<(), BuilderError> {
        let seri_levels = levels
            .iter()
            .map(|level| seri::CheckpointLevel {
                next_position: level.last_item.unwrap_or(0),
            })
            .collect();
        let seri_checkpoint = seri::Checkpoint {
            entry_position,
            levels: seri_levels,
        };
        seri_checkpoint.write(output)?;

        for level in levels.iter_mut() {
            if force_write
                || level.current_items > level.expected_items
                || (level.expected_items - level.current_items)
                    <= CHECKPOINT_WRITE_UPCOMING_WITHIN_DISTANCE
            {
                level.last_item = Some(current_position);
                level.current_items = 0;
            }
        }

        Ok(())
    }
}

fn levels_for_items_count(nb_items: u64, log_base: f64) -> Vec<Level> {
    if nb_items == 0 {
        return Vec::new();
    }

    let nb_levels = (nb_items as f64).log(log_base).round().max(1.0) as u64;
    let log_base_u64 = log_base as u64;

    let mut levels = Vec::new();
    let mut max_items = (nb_items / log_base_u64).max(1);
    for level_id in 0..nb_levels {
        // we don't want to create an extra level with only a few items per checkpoint
        if !levels.is_empty() && max_items < LEVELS_MINIMUM_ITEMS {
            break;
        }

        levels.push(Level {
            id: level_id as usize,
            expected_items: max_items,
            current_items: 0,
            last_item: None,
        });
        max_items /= log_base_u64;
    }

    levels
}

///
///
///
#[derive(Debug)]
struct Level {
    id: usize,
    expected_items: u64,
    current_items: u64,
    last_item: Option<u64>,
}

///
/// Index building related errors
///
#[derive(Debug)]
pub enum BuilderError {
    MaxSize,
    InvalidItem,
    Serialization(seri::SerializationError),
    IO(std::io::Error),
}

impl From<std::io::Error> for BuilderError {
    fn from(err: std::io::Error) -> Self {
        BuilderError::IO(err)
    }
}

impl From<seri::SerializationError> for BuilderError {
    fn from(err: seri::SerializationError) -> Self {
        BuilderError::Serialization(err)
    }
}

#[cfg(test)]
mod tests {
    use tempdir;

    use super::*;
    use crate::tests::TestString;

    #[test]
    fn calculate_levels() {
        let levels = levels_for_items_count(0, 5.0);
        let levels_items = levels.iter().map(|l| l.expected_items).collect::<Vec<_>>();
        assert_eq!(levels_items, vec![]);

        let levels = levels_for_items_count(1, 5.0);
        let levels_items = levels.iter().map(|l| l.expected_items).collect::<Vec<_>>();
        assert_eq!(levels_items, vec![1]);

        let levels = levels_for_items_count(20, 5.0);
        let levels_items = levels.iter().map(|l| l.expected_items).collect::<Vec<_>>();
        assert_eq!(levels_items, vec![4]);

        let levels = levels_for_items_count(100, 5.0);
        let levels_items = levels.iter().map(|l| l.expected_items).collect::<Vec<_>>();
        assert_eq!(levels_items, vec![20, 4]);

        let levels = levels_for_items_count(1_000, 5.0);
        let levels_items = levels.iter().map(|l| l.expected_items).collect::<Vec<_>>();
        assert_eq!(levels_items, vec![200, 40, 8]);

        let levels = levels_for_items_count(1_000_000, 10.0);
        let levels_items = levels.iter().map(|l| l.expected_items).collect::<Vec<_>>();
        assert_eq!(levels_items, vec![100_000, 10_000, 1000, 100, 10]);
    }

    #[test]
    fn build_from_unsorted() {
        let mut data = Vec::new();

        for _i in 0..100 {
            data.push(Entry::new(
                TestString("ccc".to_string()),
                TestString("ccc".to_string()),
            ));
        }

        let tempdir = tempdir::TempDir::new("extindex").unwrap();
        let index_path = tempdir.path().join("index.idx");
        let builder = Builder::<TestString, TestString>::new(index_path.clone());

        builder.build(data.into_iter()).unwrap();

        let file_meta = std::fs::metadata(&index_path).unwrap();
        assert!(file_meta.len() > 10);
    }
}
