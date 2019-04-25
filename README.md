extindex [![crates.io](https://img.shields.io/crates/v/extindex.svg)](https://crates.io/crates/extindex)
=========

Immutable persisted index (on disk) that can be built in one pass using a sorted iterator, or can
use [extsort](https://crates.io/crates/extsort) to externally sort the iterator first, and
then build the index from it.

The index allows random lookups and sorted scans. An indexed entry consists of a key and a value.
The key needs to implement `Eq` and `Ord`, and both the key and values need to implement a
`Encodable` trait for serialization to and from disk.

The index is built using a skip list like data structure, but in which lookups are starting from
the end of the index instead of from the beginning. This allow building the index in a single 
pass on a sorted iterator, since starting from the beginning would require knowing
checkpoints/nodes ahead in the file.

# Example
```rust
 extern crate extindex;
 extern crate tempdir;

 use std::io::{Read, Write};
 use extindex::{Builder, Encodable, Entry, Reader};

 #[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
 struct TestString(String);

 impl Encodable<TestString> for TestString {
     fn encode_size(item: &TestString) -> Option<usize> {
         Some(item.0.as_bytes().len())
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
- [ ] Possibility to use Bloom filter to prevent hitting the disk when index doesn't have a key
- [ ] Write an example using Serde serialization

