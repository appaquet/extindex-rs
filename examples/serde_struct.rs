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

use extindex::{Builder, Entry, Reader};

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
        SomeStruct {
            a: 123,
            b: "my value".to_string(),
        },
    )];
    builder.build(entries.into_iter()).unwrap();

    let reader = Reader::<String, SomeStruct>::open(index_file).unwrap();
    assert!(reader.find(&"my_key".to_string()).unwrap().is_some());
    assert!(reader.find(&"notfound".to_string()).unwrap().is_none());
}
