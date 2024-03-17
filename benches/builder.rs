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

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use std::{
    io::{Read, Write},
    time::Duration,
};

use extindex::{Builder, Entry, Serializable};

fn bench_builder(c: &mut Criterion) {
    let mut group = c.benchmark_group("builder");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(9));
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.warm_up_time(Duration::from_millis(100));

    let sizes = [10_000, 100_000, 1_000_000];
    for size in sizes {
        group.bench_with_input(BenchmarkId::new("known size", size), &size, |b, size| {
            b.iter(|| {
                let index_file = tempfile::NamedTempFile::new().unwrap();
                let index_file = index_file.path();

                let builder = Builder::new(index_file);
                builder.build(create_known_size_entries(*size)).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("unknown size", size), &size, |b, size| {
            b.iter(|| {
                let index_file = tempfile::NamedTempFile::new().unwrap();
                let index_file = index_file.path();

                let builder = Builder::new(index_file);
                builder.build(create_unknown_size_entries(*size)).unwrap();
            });
        });
    }
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

impl Serializable for SizedString {
    fn size(&self) -> Option<usize> {
        Some(self.0.as_bytes().len())
    }

    fn serialize<W: Write>(&self, write: &mut W) -> Result<(), std::io::Error> {
        write.write_all(self.0.as_bytes()).map(|_| ())
    }

    fn deserialize<R: Read>(data: &mut R, size: usize) -> Result<SizedString, std::io::Error> {
        let mut bytes = vec![0u8; size];
        data.read_exact(&mut bytes)?;
        Ok(SizedString(String::from_utf8_lossy(&bytes).to_string()))
    }
}

impl std::fmt::Display for SizedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct UnsizedString(pub String);

impl Serializable for UnsizedString {
    fn size(&self) -> Option<usize> {
        None
    }

    fn serialize<W: Write>(&self, write: &mut W) -> Result<(), std::io::Error> {
        write.write_all(self.0.as_bytes()).map(|_| ())
    }

    fn deserialize<R: Read>(data: &mut R, size: usize) -> Result<UnsizedString, std::io::Error> {
        let mut bytes = vec![0u8; size];
        data.read_exact(&mut bytes)?;
        Ok(UnsizedString(String::from_utf8_lossy(&bytes).to_string()))
    }
}

impl std::fmt::Display for UnsizedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

criterion_group!(benches, bench_builder,);
criterion_main!(benches);
