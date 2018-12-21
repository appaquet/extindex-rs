#![feature(test)]
extern crate test;

use std::io::{Read, Write};

use byteorder::{ReadBytesExt, WriteBytesExt};

use extsort::*;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct MyStruct(u32);

impl Sortable<MyStruct> for MyStruct {
    fn encode(item: &MyStruct, write: &mut Write) {
        write.write_u32::<byteorder::LittleEndian>(item.0).unwrap();
    }

    fn decode(read: &mut Read) -> Option<MyStruct> {
        read.read_u32::<byteorder::LittleEndian>()
            .ok()
            .map(MyStruct)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    #[bench]
    fn bench_random_access_1_million(b: &mut Bencher) {
        b.iter(|| {})
    }
}
