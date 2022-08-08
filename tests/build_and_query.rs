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

use extindex::{Builder, Entry, Reader, SerdeWrapper};

#[test]
fn build_and_read_unique_key() {
    let index_file = tempfile::NamedTempFile::new().unwrap();

    let builder = Builder::new(index_file.path());
    builder.build(create_entries(1_000, "")).unwrap();

    let reader = Reader::<String, String>::open(index_file.path()).unwrap();
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

    let reader = Reader::<String, String>::open(index_file.path()).unwrap();

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

    match Reader::<String, String>::open(index_file.path()).err() {
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

        let index = Reader::<String, String>::open(index_file.path()).unwrap();
        for i in 0..nb_entries {
            index.find(&format!("key:{}", i)).unwrap().unwrap();

            index.find_first(&format!("key:{}", i)).unwrap().unwrap();
        }
    }
}

#[test]
fn extsort_build_and_read() {
    let index_file = tempfile::NamedTempFile::new().unwrap();

    let builder = Builder::new(index_file.path()).with_extsort_segment_size(50_000);
    builder.build(create_entries(100_000, "")).unwrap();

    let reader = Reader::<String, String>::open(index_file.path()).unwrap();
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
        .map(|entry| entry.key().clone())
        .collect();
    expected_keys.sort();

    let reader = Reader::<String, String>::open(index_file.path()).unwrap();
    for (found_item, expected_key) in reader.iter().zip(expected_keys) {
        assert_eq!(found_item.key(), &expected_key);
    }
}

#[test]
fn test_serde_struct() {
    #[derive(Ord, PartialOrd, Eq, PartialEq, Debug, serde::Serialize, serde::Deserialize)]
    struct SerdeStruct {
        a: u32,
        b: String,
    }

    let index_file = tempfile::NamedTempFile::new().unwrap();

    let builder = Builder::new(index_file.path());
    let entries = vec![Entry::new(
        "my_key".to_string(),
        SerdeWrapper(SerdeStruct {
            a: 123,
            b: "my value".to_string(),
        }),
    )];
    builder.build(entries.into_iter()).unwrap();

    let reader = Reader::<String, SerdeWrapper<SerdeStruct>>::open(index_file).unwrap();
    assert!(reader.find(&"my_key".to_string()).unwrap().is_some());
    assert!(reader.find(&"notfound".to_string()).unwrap().is_none());
}

fn create_entries(
    nb_entries: usize,
    extra: &'static str,
) -> impl Iterator<Item = Entry<String, String>> + 'static {
    (0..nb_entries)
        .map(move |idx| build_entry(format!("key:{}", idx), format!("val:{}:{}", extra, idx)))
}

fn build_entry(key: String, value: String) -> Entry<String, String> {
    Entry::new(key, value)
}

fn build_entry_ref(key: &str, value: &str) -> Entry<String, String> {
    Entry::new(key.to_string(), value.to_string())
}

fn find_entry(reader: &Reader<String, String>, key: &str) -> Option<String> {
    reader.find(key).unwrap().map(|entry| entry.value().clone())
}

fn find_first_entry(reader: &Reader<String, String>, key: &str) -> Option<String> {
    reader
        .find_first(key)
        .unwrap()
        .map(|entry| entry.value().clone())
}

fn collect_entries_from(reader: &Reader<String, String>, from: &str) -> Vec<String> {
    reader
        .iter_from(from)
        .unwrap()
        .map(|entry| entry.value().clone())
        .collect()
}
