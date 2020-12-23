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

use std::io::{Cursor, Read, Write};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{Encodable, Entry as CrateEntry};

const INDEX_FILE_MAGIC_HEADER_SIZE: usize = 2;
const INDEX_FILE_MAGIC_HEADER: [u8; INDEX_FILE_MAGIC_HEADER_SIZE] = [40, 12];
const INDEX_FILE_VERSION: u8 = 0;

pub const OBJECT_ID_ENTRY: u8 = 0;
pub const OBJECT_ID_CHECKPOINT: u8 = 1;

pub const MAX_LEVELS: usize = 256;
pub const MAX_KEY_SIZE_BYTES: usize = 1024;
pub const MAX_VALUE_SIZE_BYTES: usize = 65000;

pub struct Header {
    pub nb_levels: u8,
}

impl Header {
    pub fn write<W: Write>(&self, output: &mut W) -> Result<(), std::io::Error> {
        output.write_all(&INDEX_FILE_MAGIC_HEADER)?;
        output.write_all(&[INDEX_FILE_VERSION])?;
        output.write_u8(self.nb_levels)?;
        Ok(())
    }

    pub fn read_slice(data: &[u8]) -> Result<(Header, usize), SerializationError> {
        let mut cursor = Cursor::new(data);

        let mut magic_buf = [0u8; INDEX_FILE_MAGIC_HEADER_SIZE];
        cursor.read_exact(&mut magic_buf)?;

        if magic_buf != INDEX_FILE_MAGIC_HEADER {
            return Err(SerializationError::InvalidFormat);
        }

        let version = cursor.read_u8()?;
        if version != INDEX_FILE_VERSION {
            return Err(SerializationError::InvalidVersion);
        }

        let nb_levels = cursor.read_u8()?;
        Ok((Header { nb_levels }, Self::size()))
    }

    pub const fn size() -> usize {
        2 + /* magic */
            1 + /* version */
            1 /* nb_levels */
    }
}

pub struct Checkpoint {
    pub entry_position: u64,
    pub levels: Vec<CheckpointLevel>,
}

pub struct CheckpointLevel {
    pub next_position: u64,
}

impl Checkpoint {
    pub fn write<W: Write>(&self, output: &mut W) -> Result<(), SerializationError> {
        output.write_u8(OBJECT_ID_CHECKPOINT)?;
        output.write_u64::<LittleEndian>(self.entry_position)?;

        for level in &self.levels {
            output.write_u64::<LittleEndian>(level.next_position)?;
        }

        Ok(())
    }

    pub fn read_slice(
        data: &[u8],
        nb_levels: usize,
    ) -> Result<(Checkpoint, usize), SerializationError> {
        let mut checkpoint_cursor = Cursor::new(data);
        let item_id = checkpoint_cursor.read_u8()?;
        if item_id != OBJECT_ID_CHECKPOINT {
            return Err(SerializationError::InvalidObjectType);
        }

        // TODO: Test performance of this heap alloc vs hard coding max size vec on
        // stack
        let entry_position = checkpoint_cursor.read_u64::<LittleEndian>()?;
        let mut levels = Vec::with_capacity(nb_levels);
        for _i in 0..nb_levels {
            let previous_checkpoint_position = checkpoint_cursor.read_u64::<LittleEndian>()?;
            levels.push(CheckpointLevel {
                next_position: previous_checkpoint_position,
            })
        }

        Ok((
            Checkpoint {
                entry_position,
                levels,
            },
            Self::size(nb_levels),
        ))
    }

    pub fn size(nb_levels: usize) -> usize {
        1 + /*id*/
            8 + /*entry_pos*/
            (nb_levels * 8)
    }
}

pub struct Entry<K, V>
where
    K: Ord + Encodable,
    V: Encodable,
{
    pub entry: CrateEntry<K, V>,
}

impl<K, V> Entry<K, V>
where
    K: Ord + Encodable,
    V: Encodable,
{
    pub fn write<W: Write>(
        entry: &CrateEntry<K, V>,
        output: &mut W,
    ) -> Result<(), SerializationError> {
        let (key_size, key_data) = match entry.key.encoded_size() {
            Some(size) => (size, None),
            None => {
                let mut buffer = Vec::new();
                entry.key.encode(&mut buffer)?;
                (buffer.len(), Some(buffer))
            }
        };

        let (value_size, value_data) = match entry.value.encoded_size() {
            Some(size) => (size, None),
            None => {
                let mut buffer = Vec::new();
                entry.value.encode(&mut buffer)?;
                (buffer.len(), Some(buffer))
            }
        };

        if key_size > MAX_KEY_SIZE_BYTES || value_size > MAX_VALUE_SIZE_BYTES {
            return Err(SerializationError::OutOfBound);
        }

        output.write_u8(OBJECT_ID_ENTRY)?;
        output.write_u16::<LittleEndian>(key_size as u16)?;
        output.write_u16::<LittleEndian>(value_size as u16)?;

        if let Some(key_data) = key_data.as_ref() {
            output.write_all(&key_data)?;
        } else {
            entry.key.encode(output)?;
        }

        if let Some(value_data) = value_data.as_ref() {
            output.write_all(&value_data)?;
        } else {
            entry.value.encode(output)?;
        }

        Ok(())
    }

    pub fn read_slice(data: &[u8]) -> Result<(Entry<K, V>, usize), SerializationError> {
        let mut data_cursor = Cursor::new(data);
        Self::read(&mut data_cursor)
    }

    pub fn read<R: Read>(data_cursor: &mut R) -> Result<(Entry<K, V>, usize), SerializationError> {
        let item_id = data_cursor.read_u8()?;
        if item_id != OBJECT_ID_ENTRY {
            return Err(SerializationError::InvalidObjectType);
        }

        let key_size = data_cursor.read_u16::<LittleEndian>()? as usize;
        let data_size = data_cursor.read_u16::<LittleEndian>()? as usize;
        let key = <K as Encodable>::decode(data_cursor, key_size)?;
        let value = <V as Encodable>::decode(data_cursor, data_size)?;

        let entry_file_size = 1 + 2 + 2 + key_size + data_size;
        let entry = CrateEntry { key, value };
        Ok((Entry { entry }, entry_file_size))
    }

    pub fn read_key(data: &[u8]) -> Result<K, SerializationError> {
        let mut data_cursor = Cursor::new(data);

        let item_id = data_cursor.read_u8()?;
        if item_id != OBJECT_ID_ENTRY {
            return Err(SerializationError::InvalidObjectType);
        }

        let key_size = data_cursor.read_u16::<LittleEndian>()? as usize;
        let _data_size = data_cursor.read_u16::<LittleEndian>()? as usize;

        let key = <K as Encodable>::decode(&mut data_cursor, key_size)?;
        Ok(key)
    }
}

pub enum Object<K, V>
where
    K: Ord + Encodable,
    V: Encodable,
{
    Entry(Entry<K, V>),
    Checkpoint(Checkpoint),
}

impl<K, V> Object<K, V>
where
    K: Ord + Encodable,
    V: Encodable,
{
    pub fn read(
        data: &[u8],
        nb_levels: usize,
    ) -> Result<(Object<K, V>, usize), SerializationError> {
        if data.is_empty() {
            return Err(SerializationError::OutOfBound);
        }

        if data[0] == OBJECT_ID_CHECKPOINT {
            let (chk, size) = Checkpoint::read_slice(data, nb_levels)?;
            Ok((Object::Checkpoint(chk), size))
        } else if data[0] == OBJECT_ID_ENTRY {
            let (entry, size) = Entry::read_slice(data)?;
            Ok((Object::Entry(entry), size))
        } else {
            Err(SerializationError::InvalidObjectType)
        }
    }
}

#[derive(Debug)]
pub enum SerializationError {
    OutOfBound,
    InvalidFormat,
    InvalidVersion,
    InvalidObjectType,
    IO(std::io::Error),
}

impl From<std::io::Error> for SerializationError {
    fn from(err: std::io::Error) -> Self {
        SerializationError::IO(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::TestString;

    #[test]
    fn head_write_read() {
        let mut data = Vec::new();

        let header = Header { nb_levels: 123 };
        header.write(&mut data).ok().unwrap();

        let (read_header, size) = Header::read_slice(&data).ok().unwrap();
        assert_eq!(read_header.nb_levels, 123);
        assert_eq!(Header::size(), data.len());
        assert_eq!(data.len(), size);
    }

    #[test]
    fn checkpoint_write_read() {
        let mut data = Vec::new();

        let checkpoint = Checkpoint {
            entry_position: 1234,
            levels: vec![CheckpointLevel {
                next_position: 1000,
            }],
        };
        checkpoint.write(&mut data).ok().unwrap();

        let (read_checkpoint, size) = Checkpoint::read_slice(&data, 1).ok().unwrap();
        assert_eq!(read_checkpoint.entry_position, 1234);
        assert_eq!(read_checkpoint.levels.len(), 1);
        assert_eq!(read_checkpoint.levels[0].next_position, 1000);
        assert_eq!(Checkpoint::size(1), data.len());
        assert_eq!(size, data.len());
    }

    #[test]
    fn known_size_entry_write_read() {
        let mut data = Vec::new();

        let entry = Entry {
            entry: CrateEntry {
                key: TestString("key".to_string()),
                value: TestString("value".to_string()),
            },
        };
        Entry::write(&entry.entry, &mut data).unwrap();

        let (read_entry, size) = Entry::<TestString, TestString>::read_slice(&data)
            .ok()
            .unwrap();
        assert_eq!(read_entry.entry.key, TestString("key".to_string()));
        assert_eq!(read_entry.entry.value, TestString("value".to_string()));
        assert_eq!(size, data.len());

        let read_entry_key = Entry::<TestString, TestString>::read_key(&data)
            .ok()
            .unwrap();
        assert_eq!(read_entry_key, TestString("key".to_string()));
    }

    #[test]
    fn unknown_size_entry_write_read() {
        let mut data = Vec::new();

        let entry = Entry {
            entry: CrateEntry {
                key: UnsizedString("key".to_string()),
                value: UnsizedString("value".to_string()),
            },
        };
        Entry::write(&entry.entry, &mut data).unwrap();

        let (read_entry, size) = Entry::<UnsizedString, UnsizedString>::read_slice(&data)
            .ok()
            .unwrap();
        assert_eq!(read_entry.entry.key, UnsizedString("key".to_string()));
        assert_eq!(read_entry.entry.value, UnsizedString("value".to_string()));
        assert_eq!(size, data.len());

        let read_entry_key = Entry::<UnsizedString, UnsizedString>::read_key(&data)
            .ok()
            .unwrap();
        assert_eq!(read_entry_key, UnsizedString("key".to_string()));
    }

    #[test]
    fn object_write_read() {
        let mut data = Vec::new();
        let mut cursor = Cursor::new(&mut data);

        let checkpoint = Checkpoint {
            entry_position: 1234,
            levels: vec![CheckpointLevel {
                next_position: 1000,
            }],
        };
        checkpoint.write(&mut cursor).ok().unwrap();

        let entry = Entry {
            entry: CrateEntry {
                key: TestString("key".to_string()),
                value: TestString("value".to_string()),
            },
        };
        Entry::write(&entry.entry, &mut cursor).unwrap();

        let chk_size = match Object::<TestString, TestString>::read(&data, 1)
            .ok()
            .unwrap()
        {
            (Object::Checkpoint(c), size) => {
                assert_eq!(c.entry_position, 1234);
                size
            }
            _ => {
                panic!("Got an invalid object type");
            }
        };

        match Object::<TestString, TestString>::read(&data[chk_size..], 1)
            .ok()
            .unwrap()
        {
            (Object::Entry(e), _size) => {
                assert_eq!(e.entry.key, TestString("key".to_string()));
            }
            _ => {
                panic!("Got an invalid object type");
            }
        }

        assert!(Object::<TestString, TestString>::read(&data[chk_size + 1..], 1).is_err());
        assert!(Object::<TestString, TestString>::read(&data[0..0], 1).is_err());
    }

    #[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
    pub struct UnsizedString(pub String);

    impl super::Encodable for UnsizedString {
        fn encoded_size(&self) -> Option<usize> {
            None
        }

        fn encode<W: Write>(&self, write: &mut W) -> Result<(), std::io::Error> {
            write.write_all(self.0.as_bytes()).map(|_| ())
        }

        fn decode<R: Read>(data: &mut R, size: usize) -> Result<UnsizedString, std::io::Error> {
            let mut bytes = vec![0u8; size];
            data.read_exact(&mut bytes)?;
            Ok(UnsizedString(String::from_utf8_lossy(&bytes).to_string()))
        }
    }
}
