use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use std::io::{Read, Write};

/// Trait for key and value size. This is used to specify the size of the key
/// and value in the builder and reader.
pub trait DataSize: Sized + Send + Sync + Clone + Copy {
    fn size() -> usize;

    fn max_value() -> usize;

    fn write<W: Write>(output: &mut W, value: usize) -> Result<(), std::io::Error>;

    fn read<R: Read>(input: &mut R) -> Result<usize, std::io::Error>;
}

impl DataSize for u8 {
    fn size() -> usize {
        1
    }

    fn max_value() -> usize {
        u8::MAX as usize
    }

    fn write<W: Write>(output: &mut W, value: usize) -> Result<(), std::io::Error> {
        output.write_u8(value as u8)
    }

    fn read<R: Read>(input: &mut R) -> Result<usize, std::io::Error> {
        input.read_u8().map(|v| v as usize)
    }
}

impl DataSize for u16 {
    fn size() -> usize {
        2
    }

    fn max_value() -> usize {
        u16::MAX as usize
    }

    fn write<W: Write>(output: &mut W, value: usize) -> Result<(), std::io::Error> {
        output.write_u16::<LittleEndian>(value as u16)
    }

    fn read<R: Read>(input: &mut R) -> Result<usize, std::io::Error> {
        input.read_u16::<LittleEndian>().map(|v| v as usize)
    }
}

#[derive(Clone, Copy)]
pub struct U24;

impl DataSize for U24 {
    fn size() -> usize {
        3
    }

    fn max_value() -> usize {
        16777215
    }

    fn write<W: Write>(output: &mut W, value: usize) -> Result<(), std::io::Error> {
        output.write_u24::<LittleEndian>(value as u32)
    }

    fn read<R: Read>(input: &mut R) -> Result<usize, std::io::Error> {
        input.read_u24::<LittleEndian>().map(|v| v as usize)
    }
}

impl DataSize for u32 {
    fn size() -> usize {
        4
    }

    fn max_value() -> usize {
        u32::MAX as usize
    }

    fn write<W: Write>(output: &mut W, value: usize) -> Result<(), std::io::Error> {
        output.write_u32::<LittleEndian>(value as u32)
    }

    fn read<R: Read>(input: &mut R) -> Result<usize, std::io::Error> {
        input.read_u32::<LittleEndian>().map(|v| v as usize)
    }
}

impl DataSize for u64 {
    fn size() -> usize {
        8
    }

    fn max_value() -> usize {
        u64::MAX as usize
    }

    fn write<W: Write>(output: &mut W, value: usize) -> Result<(), std::io::Error> {
        output.write_u64::<LittleEndian>(value as u64)
    }

    fn read<R: Read>(input: &mut R) -> Result<usize, std::io::Error> {
        input.read_u64::<LittleEndian>().map(|v| v as usize)
    }
}
