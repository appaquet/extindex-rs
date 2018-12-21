use extindex::{Builder, Encodable, Entry, Reader};
use std::io::Write;
use tempdir;

#[test]
fn simple_build_and_read() {
    let tempdir = tempdir::TempDir::new("extindex").unwrap();
    let index_file = tempdir.path().join("index.idx");

    let builder = Builder::new(index_file.clone());
    builder.build(create_entries(1_000)).unwrap();

    let reader = Reader::<MyString, MyString>::open(&index_file).unwrap();
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

    match Reader::<MyString, MyString>::open(&index_file).err() {
        Some(extindex::reader::Error::Empty) => {}
        _ => panic!("Unexpected return"),
    }
}

#[test]
fn fuzz_build_read_0_to_1000_items() {
    let tempdir = tempdir::TempDir::new("extindex").unwrap();
    let index_file = tempdir.path().join("index.idx");

    for nb_entries in (1..=1300).step_by(13) {
        println!("Testing with {} entries", nb_entries);

        let builder = Builder::new(index_file.clone());
        builder.build(create_entries(nb_entries)).unwrap();

        let index = Reader::<MyString, MyString>::open(&index_file).unwrap();
        for i in 0..nb_entries {
            index.find(MyString(format!("key:{}", i))).unwrap().unwrap();
        }
    }
}

#[test]
fn extsort_build_and_read() {
    let tempdir = tempdir::TempDir::new("extindex").unwrap();
    let index_file = tempdir.path().join("index.idx");

    let builder = Builder::new(index_file.clone()).with_extsort_max_size(50_000);
    builder.build(create_entries(100_000)).unwrap();

    let reader = Reader::<MyString, MyString>::open(&index_file).unwrap();
    assert_eq!(find_key(&reader, "aaaa"), false);
    assert_eq!(find_key(&reader, "key:0"), true);
    assert_eq!(find_key(&reader, "key:1"), true);
    assert_eq!(find_key(&reader, "key:50000"), true);
    assert_eq!(find_key(&reader, "key:50000"), true);
    assert_eq!(find_key(&reader, "key:99999"), true);
    assert_eq!(find_key(&reader, "key:100000"), false);
}

fn create_entries(nb_entries: usize) -> impl Iterator<Item = Entry<MyString, MyString>> {
    (0..nb_entries).map(|idx| {
        Entry::new(
            MyString(format!("key:{}", idx)),
            MyString(format!("val:{}", idx)),
        )
    })
}

fn find_key(reader: &Reader<MyString, MyString>, key: &str) -> bool {
    reader.find(MyString(key.to_string())).unwrap().is_some()
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
struct MyString(String);

impl Encodable<MyString> for MyString {
    fn encode_size(item: &MyString) -> usize {
        item.0.as_bytes().len()
    }

    fn encode(item: &MyString, write: &mut Write) -> Result<(), std::io::Error> {
        write.write(item.0.as_bytes()).map(|_| ())
    }

    fn decode(data: &[u8]) -> Result<MyString, std::io::Error> {
        Ok(MyString(String::from_utf8_lossy(data).to_string()))
    }
}
