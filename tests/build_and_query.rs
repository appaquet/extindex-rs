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

use extindex::{Builder, Encodable, Entry, Reader};
use std::io::{Read, Write};
use tempdir;

#[test]
fn simple_build_and_read() {
    let tempdir = tempdir::TempDir::new("extindex").unwrap();
    let index_file = tempdir.path().join("index.idx");

    let builder = Builder::new(index_file.clone());
    builder.build(create_entries(1_000)).unwrap();

    let reader = Reader::<TestString, TestString>::open(&index_file).unwrap();
    assert_eq!(find_key(&reader, "aaaa"), false);
    assert_eq!(find_key(&reader, "key:-1"), false);
    assert_eq!(find_key(&reader, "key:0"), true);
    assert_eq!(find_key(&reader, "key:1"), true);
    assert_eq!(find_key(&reader, "key:499"), true);
    assert_eq!(find_key(&reader, "key:500"), true);
    assert_eq!(find_key(&reader, "key:501"), true);
    assert_eq!(find_key(&reader, "key:999"), true);
    assert_eq!(find_key(&reader, "key:1000"), false);
    assert_eq!(find_key(&reader, "key:1001"), false);
}

#[test]
fn empty_index() {
    let tempdir = tempdir::TempDir::new("extindex").unwrap();
    let index_file = tempdir.path().join("index.idx");

    let builder = Builder::new(index_file.clone());
    builder.build(create_entries(0)).unwrap();

    match Reader::<TestString, TestString>::open(&index_file).err() {
        Some(extindex::reader::Error::Empty) => {}
        _ => panic!("Unexpected return"),
    }
}

#[test]
fn fuzz_build_read_1_to_650_items() {
    let tempdir = tempdir::TempDir::new("extindex").unwrap();
    let index_file = tempdir.path().join("index.idx");

    // Test up to 4 levels (log5(650) = 4)
    for nb_entries in (1..=650).step_by(13) {
        let builder = Builder::new(index_file.clone());
        builder.build(create_entries(nb_entries)).unwrap();

        let index = Reader::<TestString, TestString>::open(&index_file).unwrap();
        for i in 0..nb_entries {
            index
                .find(&TestString(format!("key:{}", i)))
                .unwrap()
                .unwrap();
        }
    }
}

#[test]
fn extsort_build_and_read() {
    let tempdir = tempdir::TempDir::new("extindex").unwrap();
    let index_file = tempdir.path().join("index.idx");

    let builder = Builder::new(index_file.clone()).with_extsort_max_size(50_000);
    builder.build(create_entries(100_000)).unwrap();

    let reader = Reader::<TestString, TestString>::open(&index_file).unwrap();
    assert_eq!(find_key(&reader, "aaaa"), false);
    assert_eq!(find_key(&reader, "key:0"), true);
    assert_eq!(find_key(&reader, "key:1"), true);
    assert_eq!(find_key(&reader, "key:50000"), true);
    assert_eq!(find_key(&reader, "key:50000"), true);
    assert_eq!(find_key(&reader, "key:99999"), true);
    assert_eq!(find_key(&reader, "key:100000"), false);
}

#[test]
fn reader_iter() {
    let tempdir = tempdir::TempDir::new("extindex").unwrap();
    let index_file = tempdir.path().join("index.idx");
    let builder = Builder::new(index_file.clone());
    builder.build(create_entries(1_000)).unwrap();

    // extract keys and sort in alphanumerical order
    let mut expected_keys: Vec<String> = create_entries(1_000)
        .map(|entry| entry.key().0.clone())
        .collect();
    expected_keys.sort();

    let reader = Reader::<TestString, TestString>::open(&index_file).unwrap();
    for (found_item, expected_key) in reader.iter().zip(expected_keys) {
        assert_eq!(found_item.key().0, expected_key);
    }
}

fn create_entries(nb_entries: usize) -> impl Iterator<Item = Entry<TestString, TestString>> {
    (0..nb_entries).map(|idx| {
        Entry::new(
            TestString(format!("key:{}", idx)),
            TestString(format!("val:{}", idx)),
        )
    })
}

fn find_key(reader: &Reader<TestString, TestString>, key: &str) -> bool {
    reader.find(&TestString(key.to_string())).unwrap().is_some()
}

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
