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

use std::{borrow::Borrow, fs::File, path::Path};

use crate::{data, size::DataSize, Entry, Serializable};

/// Index reader
pub struct Reader<K, V, KS = u16, VS = u16>
where
    K: Ord + PartialEq + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    _file: File,
    data: memmap2::Mmap,
    nb_levels: usize,
    last_checkpoint_position: Option<usize>, // if none, index is empty
    phantom: std::marker::PhantomData<(K, V, KS, VS)>,
}

impl<K, V> Reader<K, V, u16, u16>
where
    K: Ord + PartialEq + Serializable,
    V: Serializable,
{
    /// Opens an index file built using the Builder at the given path with the
    /// default key and value size of u16.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Reader<K, V, u16, u16>, ReaderError> {
        Reader::<K, V, u16, u16>::open_sized(path)
    }
}

impl<K, V, KS, VS> Reader<K, V, KS, VS>
where
    K: Ord + PartialEq + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    /// Opens an index file built using the Builder at the given path with the
    /// given key and value size.
    pub fn open_sized<P: AsRef<Path>>(path: P) -> Result<Reader<K, V, KS, VS>, ReaderError> {
        let file_meta = std::fs::metadata(path.as_ref())?;
        if file_meta.len() > std::usize::MAX as u64 {
            error!(
                "Tried to open an index file bigger than usize ({} > {})",
                file_meta.len(),
                std::usize::MAX
            );
            return Err(ReaderError::TooBig);
        }

        if file_meta.len() < data::Header::size() as u64 {
            error!("Tried to open an index file that is too small to be valid");
            return Err(ReaderError::InvalidFormat);
        }

        let file = File::open(path.as_ref())?;
        let file_mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };

        let (seri_header, _header_size) = data::Header::read_slice(&file_mmap[..])?;
        let nb_levels = seri_header.nb_levels as usize;

        let last_checkpoint_position = if nb_levels > 0 {
            Some(file_mmap.len() - data::Checkpoint::size(nb_levels))
        } else {
            None
        };

        Ok(Reader {
            _file: file,
            data: file_mmap,
            nb_levels,
            last_checkpoint_position,
            phantom: std::marker::PhantomData,
        })
    }

    /// Finds any entry matching the given needle. This means that there is no
    /// guarantee on which entry is returned first if the key is present
    /// multiple times.
    pub fn find<Q: ?Sized>(&self, needle: &Q) -> Result<Option<Entry<K, V>>, ReaderError>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        Ok(self
            .find_entry_position(needle, false)?
            .map(|file_entry| file_entry.entry))
    }

    /// Finds the first entry matching the given needle. This is guarantee to
    /// return the first entry that the iterator had given at build time.
    ///
    /// Warning: Make sure that you sort by key + value and use stable sorting
    /// if you care in which order values are returned.
    pub fn find_first<Q: ?Sized>(&self, needle: &Q) -> Result<Option<Entry<K, V>>, ReaderError>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        Ok(self
            .find_entry_position(needle, true)?
            .map(|file_entry| file_entry.entry))
    }

    /// Returns an iterator that iterates from the beginning of the index to the
    /// index of the index.
    pub fn iter(&self) -> impl Iterator<Item = Entry<K, V>> + '_ {
        self.iterate_entries_from_position(None)
            .map(|file_entry| file_entry.entry)
    }

    /// Returns an iterator that iterates from the end of the index to the
    /// beginning of the index.
    pub fn iter_reverse(&self) -> impl Iterator<Item = Entry<K, V>> + '_ {
        self.reverse_iterate_entries_from_position(None)
            .map(|file_entry| file_entry.entry)
    }

    /// Returns an iterator that iterates from the given needle.
    ///
    /// Warning: Make sure that you sort by key + value and use stable sorting
    /// if you care in which order values are returned.
    pub fn iter_from<'a, Q: ?Sized>(
        &'a self,
        needle: &Q,
    ) -> Result<impl Iterator<Item = Entry<K, V>> + 'a, ReaderError>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        let first_entry = self.find_entry_position(needle, true)?;
        let from_position = match first_entry {
            Some(entry) => entry.position,
            None => self.data.len(),
        };

        let iter = self
            .iterate_entries_from_position(Some(from_position))
            .map(|file_entry| file_entry.entry);

        Ok(iter)
    }

    fn read_checkpoint_and_key(
        &self,
        checkpoint_position: usize,
    ) -> Result<Checkpoint<K>, ReaderError> {
        let (seri_checkpoint, _read_size) =
            data::Checkpoint::read_slice(&self.data[checkpoint_position..], self.nb_levels)?;
        let next_checkpoints = seri_checkpoint
            .levels
            .iter()
            .map(|checkpoint| checkpoint.next_position as usize)
            .collect();

        let entry_file_position = seri_checkpoint.entry_position as usize;
        let entry_key = data::Entry::<K, V, KS, VS>::read_key(&self.data[entry_file_position..])?;

        Ok(Checkpoint {
            entry_key,
            entry_file_position,
            next_checkpoints,
        })
    }

    fn read_entry(&self, entry_position: usize) -> Result<FileEntry<K, V>, ReaderError> {
        let (seri_entry, _read_size) =
            data::Entry::<K, V, KS, VS>::read_slice(&self.data[entry_position..])?;
        Ok(FileEntry {
            entry: seri_entry.entry,
            position: entry_position,
        })
    }

    fn find_next_checkpoint_from_position(
        &self,
        from_position: usize,
    ) -> Result<Option<Checkpoint<K>>, ReaderError> {
        let Some(last_checkpoint_position) = self.last_checkpoint_position else {
            return Ok(None);
        };

        let mut from_position = from_position;

        loop {
            let (objects, read_size) =
                data::Object::<K, V, KS, VS>::read(&self.data[from_position..], self.nb_levels)?;

            match objects {
                data::Object::Checkpoint(_) => {
                    return self.read_checkpoint_and_key(from_position).map(Some);
                }
                data::Object::Entry(_) => {
                    from_position += read_size;
                }
            }

            if from_position >= last_checkpoint_position {
                return Ok(None);
            }
        }
    }

    fn find_entry_position<Q: ?Sized>(
        &self,
        needle: &Q,
        find_first_match: bool,
    ) -> Result<Option<FileEntry<K, V>>, ReaderError>
    where
        K: Borrow<Q>,
        Q: Ord + PartialEq + Eq,
    {
        let Some(last_checkpoint_position) = self.last_checkpoint_position else {
            return Ok(None);
        };

        let last_checkpoint = self.read_checkpoint_and_key(last_checkpoint_position)?;

        if needle > last_checkpoint.entry_key.borrow() {
            // needle is after last checkpoint, so we can't find it
            return Ok(None);
        }

        let last_checkpoint_find = FindCheckpoint {
            level: 0,
            checkpoint: last_checkpoint,
        };

        let mut stack = std::collections::LinkedList::new();
        stack.push_front(last_checkpoint_find);

        while let Some(mut current) = stack.pop_front() {
            if current.checkpoint.entry_key.borrow() == needle && !find_first_match {
                return Ok(Some(
                    self.read_entry(current.checkpoint.entry_file_position)?,
                ));
            } else if needle <= current.checkpoint.entry_key.borrow() {
                let next_checkpoint_position = current.checkpoint.next_checkpoints[current.level];
                if next_checkpoint_position != 0 {
                    // go to next checkpoint
                    let next_checkpoint = self.read_checkpoint_and_key(next_checkpoint_position)?;
                    let next_checkpoint_find = FindCheckpoint {
                        level: current.level,
                        checkpoint: next_checkpoint,
                    };
                    stack.push_front(current);
                    stack.push_front(next_checkpoint_find);
                } else if current.level == self.nb_levels - 1 {
                    // we are at last level, and next checkpoint is 0, we sequentially find entry
                    return Ok(self.sequential_find_entry(None, needle));
                } else {
                    // next checkpoint is 0, but not a last level, so we just into a deeper level
                    current.level += 1;
                    stack.push_front(current);
                }
            } else if needle > current.checkpoint.entry_key.borrow() {
                if current.level == self.nb_levels - 1 {
                    // we reached last level, we need to iterate to entry
                    return Ok(self.sequential_find_entry(
                        Some(current.checkpoint.entry_file_position),
                        needle,
                    ));
                } else if let Some(previous_find) = stack.front_mut() {
                    // we go a level deeper into previous checkpoint
                    previous_find.level += 1;
                }
            }
        }

        Ok(None)
    }

    fn iterate_entries_from_position(
        &self,
        from_position: Option<usize>,
    ) -> FileEntryIterator<K, V, KS, VS> {
        let from_position = from_position.unwrap_or_else(data::Header::size);
        FileEntryIterator {
            reader: self,
            current_position: from_position,
        }
    }

    fn reverse_iterate_entries_from_position(
        &self,
        from_position: Option<usize>,
    ) -> ReverseFileEntryIterator<K, V, KS, VS> {
        let (next_checkpoint, from_position) =
            if let Some(last_checkpoint_position) = self.last_checkpoint_position {
                let from_position = from_position.unwrap_or(last_checkpoint_position);
                let next_checkpoint = self
                    .find_next_checkpoint_from_position(from_position)
                    .ok()
                    .flatten();

                (next_checkpoint, Some(from_position))
            } else {
                (None, None)
            };

        let mut iter = ReverseFileEntryIterator::new(self, next_checkpoint);

        if let Some(from_position) = from_position {
            iter.pop_to_before_position(from_position);
        }

        iter
    }

    fn sequential_find_entry<Q: ?Sized>(
        &self,
        from_position: Option<usize>,
        needle: &Q,
    ) -> Option<FileEntry<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + PartialEq + Eq,
    {
        self.iterate_entries_from_position(from_position)
            .take_while(|read_entry| read_entry.entry.key.borrow() <= needle)
            .find(|read_entry| read_entry.entry.key.borrow() == needle)
    }
}

/// Iterator over entries of the index.
struct FileEntryIterator<'reader, K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    reader: &'reader Reader<K, V, KS, VS>,
    current_position: usize,
}

impl<'reader, K, V, KS, VS> Iterator for FileEntryIterator<'reader, K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    type Item = FileEntry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_position >= self.reader.data.len() {
            return None;
        }

        loop {
            let position = self.current_position;
            let (read_object, read_size) = data::Object::<K, V, KS, VS>::read(
                &self.reader.data[position..],
                self.reader.nb_levels,
            )
            .ok()?;
            self.current_position += read_size;
            match read_object {
                data::Object::Checkpoint(_) => continue,
                data::Object::Entry(entry) => {
                    return Some(FileEntry {
                        entry: entry.entry,
                        position,
                    });
                }
            };
        }
    }
}

/// Reverse iterator over entries of the index.
///
/// It works by loading all entries prior to a checkpoint, and then returning
/// them in reverse order. Then when they have all been yielded, it loads the
/// next checkpoint (which is at an earlier position in the file), and repeats
/// the process.
struct ReverseFileEntryIterator<'reader, K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    reader: &'reader Reader<K, V, KS, VS>,
    checkpoint: Option<Checkpoint<K>>, // next checkpoint
    entries: Vec<FileEntry<K, V>>,     // entries between prev and next checkpoint
    _phantom: std::marker::PhantomData<(KS, VS)>,
}

impl<'reader, K, V, KS, VS> ReverseFileEntryIterator<'reader, K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    fn new(reader: &'reader Reader<K, V, KS, VS>, checkpoint: Option<Checkpoint<K>>) -> Self {
        let mut iter = ReverseFileEntryIterator {
            reader,
            checkpoint,
            entries: Vec::new(),
            _phantom: std::marker::PhantomData,
        };

        iter.load_entries();

        iter
    }

    fn load_entries(&mut self) {
        let Some(next_checkpoint) = self.checkpoint.take() else {
            return;
        };

        let prev_checkpoint = next_checkpoint
            .next_checkpoints
            .last()
            .copied()
            .unwrap_or_default();
        if prev_checkpoint == 0 {
            // we are now at the beginning of the file, we are done
            return;
        }

        for entry in self
            .reader
            .iterate_entries_from_position(Some(prev_checkpoint))
        {
            if entry.position > next_checkpoint.entry_file_position {
                break;
            }

            self.entries.push(entry);
        }

        self.checkpoint = self.reader.read_checkpoint_and_key(prev_checkpoint).ok();
    }

    // Pop any entries that are after the given position.
    //
    // This is used when we are skipping from a specified position.
    fn pop_to_before_position(&mut self, position: usize) {
        while let Some(entry) = self.entries.last() {
            if entry.position <= position {
                break;
            }

            self.entries.pop();
        }
    }
}

impl<'reader, K, V, KS, VS> Iterator for ReverseFileEntryIterator<'reader, K, V, KS, VS>
where
    K: Ord + Serializable,
    V: Serializable,
    KS: DataSize,
    VS: DataSize,
{
    type Item = FileEntry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        let _ = self.checkpoint.as_ref()?;

        if self.entries.is_empty() {
            self.load_entries();
            if self.entries.is_empty() {
                return None;
            }
        }

        self.entries.pop()
    }
}

/// Entry found at a specific position of the index
struct FileEntry<K, V>
where
    K: Ord + Serializable,
    V: Serializable,
{
    entry: Entry<K, V>,
    position: usize,
}

/// Index reading error
#[derive(Debug)]
pub enum ReaderError {
    TooBig,
    InvalidItem,
    InvalidFormat,
    Serialization(data::SerializationError),
    IO(std::io::Error),
}

impl From<data::SerializationError> for ReaderError {
    fn from(err: data::SerializationError) -> Self {
        ReaderError::Serialization(err)
    }
}

impl From<std::io::Error> for ReaderError {
    fn from(err: std::io::Error) -> Self {
        ReaderError::IO(err)
    }
}

/// Represents a checkpoint in the index file.
struct Checkpoint<K>
where
    K: Ord + Serializable,
{
    entry_key: K,
    entry_file_position: usize,
    next_checkpoints: Vec<usize>,
}

/// Checkpoint struct used when finding an entry in the index.
struct FindCheckpoint<K>
where
    K: Ord + Serializable,
{
    checkpoint: Checkpoint<K>,
    level: usize,
}
