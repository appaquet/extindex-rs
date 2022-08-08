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

use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::PathBuf,
};

use extsort::ExternalSorter;

use crate::{data, utils::CountedWrite, Entry, Serializable};

const CHECKPOINT_WRITE_UPCOMING_WITHIN_DISTANCE: u64 = 3;
const LEVELS_MINIMUM_ITEMS: u64 = 2;

/// Index builder that creates a file index from any iterator. If the given
/// iterator is already sorted, the `build_from_sorted` method can be used,
/// while `build` can take any iterator. The `build` method will sort the
/// iterator first using external sorting using the `extsort` crate.
///
/// As the index is being built, checkpoints / nodes are added to the file.
/// These checkpoints / nodes are similar to the ones in a skip list, except
/// that they point to previous checkpoints instead of pointing to next
/// checkpoints.
pub struct Builder<K, V>
where
    K: Ord + Serializable,
    V: Serializable,
{
    path: PathBuf,
    log_base: f64,
    extsort_segment_size: Option<usize>,
    phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Builder<K, V>
where
    K: Ord + Serializable,
    V: Serializable,
{
    /// Creates an index builder that will write to the given file path.
    pub fn new<P: Into<PathBuf>>(path: P) -> Builder<K, V> {
        Builder {
            path: path.into(),
            log_base: 5.0,
            extsort_segment_size: None,
            phantom: std::marker::PhantomData,
        }
    }

    /// Indicates approximate number of items we want per last-level checkpoint.
    ///
    /// A higher value means that once a checkpoint in which we know the
    /// item is after is found, we may need to iterate through up to
    /// `log_base` items. A lower value will prevent creating too many levels
    /// when the index gets bigger, but will require more scanning through more
    /// entries to find the right one.
    ///
    /// Default value: 5.0
    pub fn with_log_base(mut self, log_base: f64) -> Self {
        self.log_base = log_base;
        self
    }

    /// When using the `build` method from a non-sorted iterator, this value is
    /// passed to the `extsort` crate to define how many items will be buffered
    /// to memory before being flushed to disk.
    ///
    /// This number is the actual number of entries, not the sum of their size.
    pub fn with_extsort_segment_size(mut self, max_size: usize) -> Self {
        self.extsort_segment_size = Some(max_size);
        self
    }

    /// Builds the index using a non-sorted iterator.
    pub fn build<I>(self, iter: I) -> Result<(), BuilderError>
    where
        I: Iterator<Item = Entry<K, V>>,
    {
        let sort_dir = self.path.with_extension("tmp_sort");
        std::fs::create_dir_all(&sort_dir)?;

        let mut sorter = ExternalSorter::new().with_sort_dir(sort_dir);

        if let Some(segment_size) = self.extsort_segment_size {
            sorter = sorter.with_segment_size(segment_size);
        }

        let sorted_iter = sorter.sort(iter)?;
        let sorted_count = sorted_iter.sorted_count();

        self.build_from_sorted(sorted_iter, sorted_count)
    }

    /// Builds the index using a sorted iterator.
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
        if levels.len() > data::MAX_LEVELS {
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
                self.write_entry(&mut counted_output, &entry)?;
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

    fn write_header<W: Write>(&self, output: &mut W, levels: &[Level]) -> Result<(), BuilderError> {
        let seri_header = data::Header {
            nb_levels: levels.len() as u8,
        };
        seri_header.write(output)?;
        Ok(())
    }

    fn write_entry<W: Write>(
        &self,
        output: &mut W,
        entry: &Entry<K, V>,
    ) -> Result<(), BuilderError> {
        data::Entry::write(entry, output)?;
        Ok(())
    }

    fn write_checkpoint<W: Write>(
        &self,
        output: &mut W,
        current_position: u64,
        entry_position: u64,
        levels: &mut [Level],
        force_write: bool,
    ) -> Result<(), BuilderError> {
        let seri_levels = levels
            .iter()
            .map(|level| data::CheckpointLevel {
                next_position: level.last_item.unwrap_or(0),
            })
            .collect();
        let seri_checkpoint = data::Checkpoint {
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
    for _ in 0..nb_levels {
        // we don't want to create an extra level with only a few items per checkpoint
        if !levels.is_empty() && max_items < LEVELS_MINIMUM_ITEMS {
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

/// Represent a level of the skip list index.
#[derive(Debug)]
struct Level {
    expected_items: u64,
    current_items: u64,
    last_item: Option<u64>,
}

/// Index building related errors.
#[derive(Debug)]
pub enum BuilderError {
    MaxSize,
    InvalidItem,
    Serialization(data::SerializationError),
    IO(std::io::Error),
}

impl From<std::io::Error> for BuilderError {
    fn from(err: std::io::Error) -> Self {
        BuilderError::IO(err)
    }
}

impl From<data::SerializationError> for BuilderError {
    fn from(err: data::SerializationError) -> Self {
        BuilderError::Serialization(err)
    }
}

#[cfg(test)]
mod tests {
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

        let index_file = tempfile::NamedTempFile::new().unwrap();
        let index_file = index_file.path();
        let builder = Builder::<TestString, TestString>::new(index_file);

        builder.build(data.into_iter()).unwrap();

        let file_meta = std::fs::metadata(index_file).unwrap();
        assert!(file_meta.len() > 10);
    }
}
