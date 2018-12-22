extindex [![crates.io](https://img.shields.io/crates/v/extindex.svg)](https://crates.io/crates/extindex)
=========

Immutable persisted index (on disk) that can be built in one pass using a sorted iterator, or uses [extsort](https://crates.io/crates/extsort)
to externally sort the iterator first, and then build the index. The index allow random lookups and sorted scans. 
An indexed entry consists of a key and a value. The key needs to implement `Eq` and `Ord`, and both the key and values
need to implement a `Encodable` trait for serialization to and from disk.

# Example
```rust
 extern crate extindex;
 extern crate tempdir;

 use std::io::{Read, Write};
 use extindex::{Builder, Encodable, Entry, Reader};

 #[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
 struct TestString(String);

 impl Encodable<TestString> for TestString {
     fn encode_size(item: &TestString) -> usize {
         item.0.as_bytes().len()
     }

     fn encode(item: &TestString, write: &mut Write) -> Result<(), std::io::Error> {
         write.write(item.0.as_bytes()).map(|_| ())
     }

     fn decode(data: &mut Read, size: usize) -> Result<TestString, std::io::Error> {
         let mut bytes = vec![0u8; size];
         data.read_exact(&mut bytes)?;
         Ok(TestString(String::from_utf8_lossy(&bytes).to_string()))
     }
 }

 fn main() {
    let tempdir = tempdir::TempDir::new("extindex").unwrap();
    let index_file = tempdir.path().join("index.idx");

    let builder = Builder::new(index_file.clone());
    let entries = vec![
        Entry::new(TestString("mykey".to_string()), TestString("my value".to_string()))
    ];
    builder.build(entries.into_iter()).unwrap();

    let reader = Reader::<TestString, TestString>::open(&index_file).unwrap();
    assert!(reader.find(&TestString("mykey".to_string())).unwrap().is_some());
    assert!(reader.find(&TestString("notfound".to_string())).unwrap().is_none());
 }
```

# TODO
- [ ] Add support for scan from key
- [ ] Possibility to use Bloom filter to prevent hitting the disk when index doesn't have a key
- [ ] Write an example using Serde serialization

