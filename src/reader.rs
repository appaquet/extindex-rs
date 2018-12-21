use std;
use std::fs::File;
use std::path::Path;

use crate::seri;
use crate::{Encodable, Entry};

pub struct Reader<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    _file: File,
    data: memmap::Mmap,
    nb_levels: usize,
    phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Reader<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Reader<K, V>, Error> {
        let file_meta = std::fs::metadata(path.as_ref())?;
        if file_meta.len() > std::usize::MAX as u64 {
            error!(
                "Tried to open an index file that is bigger than usize ({} > {})",
                file_meta.len(),
                std::usize::MAX
            );
            return Err(Error::TooBig);
        }

        if file_meta.len() < seri::Header::size() as u64 {
            error!("Tried to open an index file that is too small to be valid");
            return Err(Error::InvalidFormat);
        }

        let file = File::open(path.as_ref())?;
        let file_mmap = unsafe { memmap::MmapOptions::new().map(&file)? };

        let (seri_header, _header_size) = seri::Header::read(&file_mmap[..])?;
        let nb_levels = seri_header.nb_levels as usize;
        if nb_levels == 0 {
            return Err(Error::Empty);
        }

        Ok(Reader {
            _file: file,
            data: file_mmap,
            nb_levels,
            phantom: std::marker::PhantomData,
        })
    }

    pub fn find(&self, needle: K) -> Result<Option<Entry<K, V>>, Error> {
        Ok(self
            .find_entry_position(needle)?
            .map(|file_entry| file_entry.entry))
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = Entry<K, V>> + 'a {
        self.iterate_entries_from_position(None)
            .map(|file_entry| file_entry.entry)
    }

    fn read_checkpoint_and_key(&self, checkpoint_position: usize) -> Result<Checkpoint<K>, Error> {
        let (seri_checkpoint, _read_size) =
            seri::Checkpoint::read(&self.data[checkpoint_position..], self.nb_levels)?;
        let previous_checkpoints = seri_checkpoint
            .levels
            .iter()
            .map(|checkpoint| checkpoint.previous_position as usize)
            .collect();

        let entry_file_position = seri_checkpoint.entry_position as usize;
        let entry_key = seri::Entry::<K, V>::read_key(&self.data[entry_file_position..])?;

        Ok(Checkpoint {
            entry_key,
            entry_file_position,
            previous_checkpoints,
        })
    }

    fn read_entry(&self, entry_position: usize) -> Result<FileEntry<K, V>, Error> {
        let (seri_entry, _read_size) = seri::Entry::read(&self.data[entry_position..])?;
        Ok(FileEntry {
            entry: seri_entry.entry,
        })
    }

    fn find_entry_position(&self, needle: K) -> Result<Option<FileEntry<K, V>>, Error> {
        let mut stack = std::collections::LinkedList::new();

        let nb_levels = self.nb_levels;
        let last_checkpoint_position = self.last_checkpoint_position();
        let last_checkpoint = self.read_checkpoint_and_key(last_checkpoint_position)?;
        let last_checkpoint_find = FindCheckpoint {
            level: 0,
            checkpoint: last_checkpoint,
        };

        stack.push_front(last_checkpoint_find);

        while let Some(mut current) = stack.pop_front() {
            if current.checkpoint.entry_key == needle {
                return Ok(Some(
                    self.read_entry(current.checkpoint.entry_file_position)?,
                ));
            } else if needle < current.checkpoint.entry_key {
                let previous_checkpoint_position =
                    current.checkpoint.previous_checkpoints[current.level];
                if previous_checkpoint_position != 0 {
                    let prev_checkpoint =
                        self.read_checkpoint_and_key(previous_checkpoint_position)?;
                    let previous_checkpoint_find = FindCheckpoint {
                        level: current.level,
                        checkpoint: prev_checkpoint,
                    };
                    stack.push_front(current);
                    stack.push_front(previous_checkpoint_find);
                } else if current.level == nb_levels - 1 {
                    // we are at last level, and previous checkpoint is 0, we sequentially find entry
                    return Ok(self.sequential_find_entry(None, &needle));
                } else {
                    // previous checkpoint is 0, but not a last level, so we just into a deeper level
                    current.level += 1;
                    stack.push_front(current);
                }
            } else if needle > current.checkpoint.entry_key {
                if current.level == nb_levels - 1 {
                    return Ok(self.sequential_find_entry(
                        Some(current.checkpoint.entry_file_position),
                        &needle,
                    ));
                } else if let Some(previous_find) = stack.front_mut() {
                    previous_find.level += 1;
                }
            }
        }

        Ok(None)
    }

    fn iterate_entries_from_position(
        &self,
        from_position: Option<usize>,
    ) -> FileEntryIterator<K, V> {
        let from_position = from_position.unwrap_or_else(seri::Header::size);
        FileEntryIterator {
            reader: self,
            current_position: from_position,
        }
    }

    fn sequential_find_entry(
        &self,
        from_position: Option<usize>,
        needle: &K,
    ) -> Option<FileEntry<K, V>> {
        self.iterate_entries_from_position(from_position)
            .skip_while(|read_entry| read_entry.entry.key < *needle)
            .next()
            .filter(|read_entry| read_entry.entry.key == *needle)
    }

    fn last_checkpoint_position(&self) -> usize {
        self.data.len() - seri::Checkpoint::size(self.nb_levels)
    }
}

struct FileEntryIterator<'reader, K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    reader: &'reader Reader<K, V>,
    current_position: usize,
}

impl<'reader, K, V> Iterator for FileEntryIterator<'reader, K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    type Item = FileEntry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_position >= self.reader.data.len() {
            return None;
        }

        loop {
            let (read_object, read_size) = seri::Object::read(
                &self.reader.data[self.current_position..],
                self.reader.nb_levels,
            )
            .ok()?;
            self.current_position += read_size;
            match read_object {
                seri::Object::Checkpoint(_) => continue,
                seri::Object::Entry(entry) => return Some(FileEntry { entry: entry.entry }),
            };
        }
    }
}

struct FileEntry<K, V>
where
    K: Ord + Encodable<K>,
    V: Encodable<V>,
{
    entry: Entry<K, V>,
}

#[derive(Debug)]
pub enum Error {
    TooBig,
    InvalidItem,
    InvalidFormat,
    Empty,
    Serialization(seri::Error),
    IO(std::io::Error),
}

impl From<seri::Error> for Error {
    fn from(err: seri::Error) -> Self {
        Error::Serialization(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IO(err)
    }
}

struct Checkpoint<K>
where
    K: Ord + Encodable<K>,
{
    entry_key: K,
    entry_file_position: usize,
    previous_checkpoints: Vec<usize>,
}

struct FindCheckpoint<K>
where
    K: Ord + Encodable<K>,
{
    checkpoint: Checkpoint<K>,
    level: usize,
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_blabla() {}
}
