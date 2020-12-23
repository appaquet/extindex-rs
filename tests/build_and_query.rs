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

#[test]
fn build_and_read_unique_key() {
    let index_file = tempfile::NamedTempFile::new().unwrap();

    let builder = Builder::new(index_file.path());
    builder.build(create_entries(1_000, "")).unwrap();

    let reader = Reader::<TestString, TestString>::open(index_file.path()).unwrap();
    assert!(find_entry(&reader, "aaaa").is_none());
    assert!(find_entry(&reader, "key:-1").is_none());
    assert!(find_entry(&reader, "key:0").is_some());
    assert!(find_entry(&reader, "key:1").is_some());
    assert!(find_entry(&reader, "key:499").is_some());
    assert!(find_entry(&reader, "key:500").is_some());
    assert!(find_entry(&reader, "key:501").is_some());
    assert!(find_entry(&reader, "key:999").is_some());
    assert!(find_entry(&reader, "key:1000").is_none());
    assert!(find_entry(&reader, "key:1001").is_none());
}

#[test]
fn build_and_read_duplicate_key() {
    let index_file = tempfile::NamedTempFile::new().unwrap();

    let builder = Builder::new(index_file.path());
    let entries = vec![
        build_entry_ref("key:0", "val:0_0"),
        build_entry_ref("key:0", "val:0_1"),
        build_entry_ref("key:1", "val:1_0"),
        build_entry_ref("key:1", "val:1_1"),
        build_entry_ref("key:1", "val:1_2"),
        build_entry_ref("key:1", "val:1_3"),
        build_entry_ref("key:1", "val:1_4"),
        build_entry_ref("key:2", "val:2_0"),
        build_entry_ref("key:2", "val:2_1"),
    ];
    builder.build(entries.into_iter()).unwrap();

    let reader = Reader::<TestString, TestString>::open(index_file.path()).unwrap();

    assert_eq!(
        find_first_entry(&reader, "key:0"),
        Some("val:0_0".to_string())
    );
    assert_eq!(
        find_first_entry(&reader, "key:1"),
        Some("val:1_0".to_string())
    );
    assert_eq!(
        find_first_entry(&reader, "key:2"),
        Some("val:2_0".to_string())
    );

    assert_eq!(
        collect_entries_from(&reader, "key:0"),
        vec![
            "val:0_0", "val:0_1", "val:1_0", "val:1_1", "val:1_2", "val:1_3", "val:1_4", "val:2_0",
            "val:2_1",
        ]
    );

    assert_eq!(
        collect_entries_from(&reader, "key:1"),
        vec!["val:1_0", "val:1_1", "val:1_2", "val:1_3", "val:1_4", "val:2_0", "val:2_1",]
    );

    assert_eq!(
        collect_entries_from(&reader, "key:2"),
        vec!["val:2_0", "val:2_1",]
    );

    assert_eq!(
        collect_entries_from(&reader, "not_found"),
        Vec::<String>::new(),
    );
}

#[test]
fn empty_index() {
    let index_file = tempfile::NamedTempFile::new().unwrap();

    let builder = Builder::new(index_file.path());
    builder.build(create_entries(0, "")).unwrap();

    match Reader::<TestString, TestString>::open(index_file.path()).err() {
        Some(extindex::reader::ReaderError::Empty) => {}
        _ => panic!("Unexpected return"),
    }
}

#[test]
fn fuzz_build_read_1_to_650_items() {
    let index_file = tempfile::NamedTempFile::new().unwrap();

    // Test up to 4 levels (log5(650) = 4)
    for nb_entries in (1..=650).step_by(13) {
        let builder = Builder::new(index_file.path());
        builder.build(create_entries(nb_entries, "")).unwrap();

        let index = Reader::<TestString, TestString>::open(index_file.path()).unwrap();
        for i in 0..nb_entries {
            index
                .find(&TestString(format!("key:{}", i)))
                .unwrap()
                .unwrap();

            index
                .find_first(&TestString(format!("key:{}", i)))
                .unwrap()
                .unwrap();
        }
    }
}

#[test]
fn extsort_build_and_read() {
    let index_file = tempfile::NamedTempFile::new().unwrap();

    let builder = Builder::new(index_file.path()).with_extsort_segment_size(50_000);
    builder.build(create_entries(100_000, "")).unwrap();

    let reader = Reader::<TestString, TestString>::open(index_file.path()).unwrap();
    assert!(find_entry(&reader, "aaaa").is_none());
    assert!(find_entry(&reader, "key:0").is_some());
    assert!(find_entry(&reader, "key:1").is_some());
    assert!(find_entry(&reader, "key:50000").is_some());
    assert!(find_entry(&reader, "key:50000").is_some());
    assert!(find_entry(&reader, "key:99999").is_some());
    assert!(find_entry(&reader, "key:100000").is_none());
}

#[test]
fn reader_iter_unique_key() {
    let index_file = tempfile::NamedTempFile::new().unwrap();

    let builder = Builder::new(index_file.path());
    builder.build(create_entries(1_000, "")).unwrap();

    // extract keys and sort in alphanumerical order
    let mut expected_keys: Vec<String> = create_entries(1_000, "")
        .map(|entry| entry.key().0.clone())
        .collect();
    expected_keys.sort();

    let reader = Reader::<TestString, TestString>::open(index_file.path()).unwrap();
    for (found_item, expected_key) in reader.iter().zip(expected_keys) {
        assert_eq!(found_item.key().0, expected_key);
    }
}

fn create_entries(
    nb_entries: usize,
    extra: &'static str,
) -> impl Iterator<Item = Entry<TestString, TestString>> + 'static {
    (0..nb_entries)
        .map(move |idx| build_entry(format!("key:{}", idx), format!("val:{}:{}", extra, idx)))
}

fn build_entry(key: String, value: String) -> Entry<TestString, TestString> {
    Entry::new(TestString(key), TestString(value))
}

fn build_entry_ref(key: &str, value: &str) -> Entry<TestString, TestString> {
    Entry::new(key_ref(key), key_ref(value))
}

fn find_entry(reader: &Reader<TestString, TestString>, key: &str) -> Option<String> {
    reader
        .find(&key_ref(key))
        .unwrap()
        .map(|entry| entry.value().0.clone())
}

fn find_first_entry(reader: &Reader<TestString, TestString>, key: &str) -> Option<String> {
    reader
        .find_first(&key_ref(key))
        .unwrap()
        .map(|entry| entry.value().0.clone())
}

fn collect_entries_from(reader: &Reader<TestString, TestString>, from: &str) -> Vec<String> {
    reader
        .iter_from(&key_ref(from))
        .unwrap()
        .map(|entry| entry.value().0.clone())
        .collect()
}

fn key_ref(key: &str) -> TestString {
    TestString(key.to_string())
}

///
/// String wrapper to be encodable
///
#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
struct TestString(String);

impl Encodable for TestString {
    fn encode_size(&self) -> Option<usize> {
        Some(self.0.as_bytes().len())
    }

    fn encode<W: Write>(&self, write: &mut W) -> Result<(), std::io::Error> {
        write.write(self.0.as_bytes()).map(|_| ())
    }

    fn decode<R: Read>(data: &mut R, size: usize) -> Result<TestString, std::io::Error> {
        let mut bytes = vec![0u8; size];
        data.read_exact(&mut bytes)?;
        Ok(TestString(String::from_utf8_lossy(&bytes).to_string()))
    }
}
