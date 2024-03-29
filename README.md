# extindex

[![crates.io](https://img.shields.io/crates/v/extindex.svg)](https://crates.io/crates/extindex)

Immutable persisted index (on disk) that can be built in one pass using a sorted
iterator, or can use [extsort](https://crates.io/crates/extsort) to externally
sort the iterator first, and then build the index from it.

The index allows random lookups and sorted scans. An indexed entry consists of a
key and a value. The key needs to implement `Eq` and `Ord`, and both the key
and values need to implement a `Serializable` trait for serialization to and
from disk. It is possible to rely on the [`serde`](https://crates.io/crates/serde)
library to implement this trait for most types.

The index is built using a skip list-like data structure, but lookups start from
the end of the index instead of the beginning. This allows building the index in
a single pass on a sorted iterator, as starting from the beginning would require
knowing checkpoints/nodes ahead in the file.

## Example

```rust
extern crate extindex;
extern crate serde;

use extindex::{Builder, Entry, Reader, SerdeWrapper};

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, serde::Serialize, serde::Deserialize)]
struct SomeStruct {
    a: u32,
    b: String,
}

fn main() {
    let index_file = tempfile::NamedTempFile::new().unwrap();

    let builder = Builder::new(index_file.path());
    let entries = vec![Entry::new(
        "my_key".to_string(),
        SerdeWrapper(SomeStruct {
            a: 123,
            b: "my value".to_string(),
        }),
    )];
    builder.build(entries.into_iter()).unwrap();

    let reader = Reader::<String, SerdeWrapper<SomeStruct>>::open(index_file).unwrap();
    assert!(reader.find(&"my_key".to_string()).unwrap().is_some());
    assert!(reader.find(&"notfound".to_string()).unwrap().is_none());
}
```

## Roadmap

- Possibility to use a Bloom filter to avoid disk access when the index does not contain a key.
