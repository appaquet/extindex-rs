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
fn bench_build_100_000_known_size(b: &mut Bencher) {
    b.iter(|| {
        let tempdir = tempdir::TempDir::new("extindex").unwrap();
        let index_file = tempdir.path().join("index.idx");

        let builder = Builder::new(index_file.clone());
        builder.build(create_known_size_entries(100_000)).unwrap();
    })
}

#[bench]
fn bench_random_access_1_million_known_size(b: &mut Bencher) {
    let tempdir = tempdir::TempDir::new("extindex").unwrap();
    let index_file = tempdir.path().join("index.idx");

    let builder = Builder::new(index_file.clone()).with_extsort_max_size(200_000);
    builder.build(create_known_size_entries(1_000_000)).unwrap();

    let index = Reader::<SizedString, SizedString>::open(&index_file).unwrap();
    let lookup_keys = vec![
        SizedString("aaaa".to_string()),
        SizedString("key:0".to_string()),
        SizedString("key:10000".to_string()),
        SizedString("key:999999".to_string()),
        SizedString("zzzz".to_string()),
    ];

    b.iter(|| {
        for key in &lookup_keys {
            test::black_box(index.find(&key).unwrap());
        }
    })
}

fn create_known_size_entries(
    nb_entries: usize,
) -> impl Iterator<Item = Entry<SizedString, SizedString>> {
    (0..nb_entries).map(|idx| {
        Entry::new(
            SizedString(format!("key:{}", idx)),
            SizedString(format!("val:{}", idx)),
        )
    })
}

#[bench]
fn bench_build_100_000_unknown_size(b: &mut Bencher) {
    b.iter(|| {
        let tempdir = tempdir::TempDir::new("extindex").unwrap();
        let index_file = tempdir.path().join("index.idx");

        let builder = Builder::new(index_file.clone());
        builder.build(create_unknown_size_entries(100_000)).unwrap();
    })
}

#[bench]
fn bench_random_access_1_million_unknown_size(b: &mut Bencher) {
    let tempdir = tempdir::TempDir::new("extindex").unwrap();
    let index_file = tempdir.path().join("index.idx");

    let builder = Builder::new(index_file.clone()).with_extsort_max_size(200_000);
    builder
        .build(create_unknown_size_entries(1_000_000))
        .unwrap();

    let index = Reader::<UnsizedString, UnsizedString>::open(&index_file).unwrap();
    let lookup_keys = vec![
        UnsizedString("aaaa".to_string()),
        UnsizedString("key:0".to_string()),
        UnsizedString("key:10000".to_string()),
        UnsizedString("key:999999".to_string()),
        UnsizedString("zzzz".to_string()),
    ];

    b.iter(|| {
        for key in &lookup_keys {
            test::black_box(index.find(&key).unwrap());
        }
    })
}

fn create_unknown_size_entries(
    nb_entries: usize,
) -> impl Iterator<Item = Entry<UnsizedString, UnsizedString>> {
    (0..nb_entries).map(|idx| {
        Entry::new(
            UnsizedString(format!("key:{}", idx)),
            UnsizedString(format!("val:{}", idx)),
        )
    })
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
struct SizedString(String);

impl Encodable<SizedString> for SizedString {
    fn encode_size(item: &SizedString) -> Option<usize> {
        Some(item.0.as_bytes().len())
    }

    fn encode(item: &SizedString, write: &mut Write) -> Result<(), std::io::Error> {
        write.write_all(item.0.as_bytes()).map(|_| ())
    }

    fn decode(data: &mut Read, size: usize) -> Result<SizedString, std::io::Error> {
        let mut bytes = vec![0u8; size];
        data.read_exact(&mut bytes)?;
        Ok(SizedString(String::from_utf8_lossy(&bytes).to_string()))
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct UnsizedString(pub String);

impl Encodable<UnsizedString> for UnsizedString {
    fn encode_size(_item: &UnsizedString) -> Option<usize> {
        None
    }

    fn encode(item: &UnsizedString, write: &mut std::io::Write) -> Result<(), std::io::Error> {
        write.write_all(item.0.as_bytes()).map(|_| ())
    }

    fn decode(data: &mut Read, size: usize) -> Result<UnsizedString, std::io::Error> {
        let mut bytes = vec![0u8; size];
        data.read_exact(&mut bytes)?;
        Ok(UnsizedString(String::from_utf8_lossy(&bytes).to_string()))
    }
}
