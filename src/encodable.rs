use std::io::{Read, Write};

/// Trait representing a structure that can be encoded / serialized to a Writer
/// and decoded / deserialized from a Reader.
pub trait Encodable: Send + Sized {
    /// Exact size that the encoded item will have, if known. If none is
    /// returned, the encoding will be buffered in memory.
    fn encoded_size(&self) -> Option<usize>;

    /// Encodes the given item to the writer
    fn encode<W: Write>(&self, write: &mut W) -> Result<(), std::io::Error>;

    /// Decodes the given from the reader
    fn decode<R: Read>(data: &mut R, size: usize) -> Result<Self, std::io::Error>;
}

#[cfg(not(feature = "serde"))]
impl Encodable for String {
    fn encoded_size(&self) -> Option<usize> {
        Some(self.as_bytes().len())
    }

    fn encode<W: Write>(&self, write: &mut W) -> Result<(), std::io::Error> {
        write.write_all(self.as_bytes()).map(|_| ())
    }

    fn decode<R: Read>(data: &mut R, size: usize) -> Result<String, std::io::Error> {
        let mut bytes = vec![0u8; size];
        data.read_exact(&mut bytes)?;
        Ok(String::from_utf8_lossy(&bytes).to_string())
    }
}

#[cfg(feature = "serde")]
impl<T> Encodable for T
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send,
{
    fn encoded_size(&self) -> Option<usize> {
        None
    }

    fn encode<W: Write>(&self, write: &mut W) -> Result<(), std::io::Error> {
        bincode::encode_into_std_write(
            bincode::serde::Compat(self),
            write,
            bincode::config::standard(),
        )
        .map(|_| ())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    fn decode<R: Read>(data: &mut R, _size: usize) -> Result<Self, std::io::Error> {
        bincode::decode_from_std_read::<bincode::serde::Compat<T>, _, _>(
            data,
            bincode::config::standard(),
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .map(|compat| compat.0)
    }
}
