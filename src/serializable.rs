use std::io::{Read, Write};

/// Trait representing a structure that can be serialized to a Writer
/// and deserialized from a Reader.
pub trait Serializable: Send + Sized {
    /// Exact size that the serialized item will have, if known.
    /// If none is returned, serialization will be buffered in memory.
    fn size(&self) -> Option<usize>;

    /// Serializes the given item to the writer
    fn serialize<W: Write>(&self, write: &mut W) -> Result<(), std::io::Error>;

    /// Deserializes the given from the reader
    fn deserialize<R: Read>(data: &mut R, size: usize) -> Result<Self, std::io::Error>;
}

impl Serializable for String {
    fn size(&self) -> Option<usize> {
        Some(self.as_bytes().len())
    }

    fn serialize<W: Write>(&self, write: &mut W) -> Result<(), std::io::Error> {
        write.write_all(self.as_bytes()).map(|_| ())
    }

    fn deserialize<R: Read>(data: &mut R, size: usize) -> Result<String, std::io::Error> {
        let mut bytes = vec![0u8; size];
        data.read_exact(&mut bytes)?;
        Ok(String::from_utf8_lossy(&bytes).to_string())
    }
}

#[cfg(feature = "serde")]
pub struct SerdeWrapper<T>(pub T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send;

#[cfg(feature = "serde")]
impl<T> Serializable for SerdeWrapper<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send,
{
    fn size(&self) -> Option<usize> {
        None
    }

    fn serialize<W: Write>(&self, write: &mut W) -> Result<(), std::io::Error> {
        bincode::encode_into_std_write(
            bincode::serde::Compat(&self.0),
            write,
            bincode::config::standard(),
        )
        .map(|_| ())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    fn deserialize<R: Read>(data: &mut R, _size: usize) -> Result<Self, std::io::Error> {
        bincode::decode_from_std_read::<bincode::serde::Compat<T>, _, _>(
            data,
            bincode::config::standard(),
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .map(|compat| SerdeWrapper(compat.0))
    }
}
