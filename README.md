extindex
[![crates.io](https://img.shields.io/crates/v/extindex.svg)](https://crates.io/crates/extindex)
[![dependency status](https://deps.rs/repo/github/appaquet/extindex-rs/status.svg)](https://deps.rs/repo/github/appaquet/extindex-rs)
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

# Example <!-- keep in sync with serde_struct.rs  -->

```rust
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

# TODO

- [ ] Possibility to use Bloom filter to prevent hitting the disk when index doesn't have a key
