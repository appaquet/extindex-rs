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

#![feature(test)]
extern crate test;

use std::io::{Read, Write};
use tempdir;
use test::Bencher;

use extindex::{Builder, Encodable, Entry, Reader};

#[bench]
fn bench_random_access_1_million(b: &mut Bencher) {
    let tempdir = tempdir::TempDir::new("extindex").unwrap();
    let index_file = tempdir.path().join("index.idx");

    let builder = Builder::new(index_file.clone()).with_extsort_max_size(200_000);
    builder.build(create_entries(1_000_000)).unwrap();

    let index = Reader::<TestString, TestString>::open(&index_file).unwrap();
    let lookup_keys = vec![
        TestString("aaaa".to_string()),
        TestString("key:0".to_string()),
        TestString("key:10000".to_string()),
        TestString("key:999999".to_string()),
        TestString("zzzz".to_string()),
    ];

    b.iter(|| {
        for key in &lookup_keys {
            test::black_box(index.find(&key).unwrap());
        }
    })
}

fn create_entries(nb_entries: usize) -> impl Iterator<Item = Entry<TestString, TestString>> {
    (0..nb_entries).map(|idx| {
        Entry::new(
            TestString(format!("key:{}", idx)),
            TestString(format!("val:{}", idx)),
        )
    })
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
struct TestString(String);

impl Encodable<TestString> for TestString {
    fn encode_size(item: &TestString) -> usize {
        item.0.as_bytes().len()
    }

    fn encode(item: &TestString, write: &mut Write) -> Result<(), std::io::Error> {
        write.write_all(item.0.as_bytes()).map(|_| ())
    }

    fn decode(data: &mut Read, size: usize) -> Result<TestString, std::io::Error> {
        let mut bytes = vec![0u8; size];
        data.read_exact(&mut bytes)?;
        Ok(TestString(String::from_utf8_lossy(&bytes).to_string()))
    }
}
