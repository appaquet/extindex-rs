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

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use std::{
    io::{Read, Write},
    time::Duration,
};

use extindex::{Builder, Entry, Reader, Serializable};

fn bench_random_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_access");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(7));
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.warm_up_time(Duration::from_millis(100));

    let index_file = tempfile::NamedTempFile::new().unwrap();
    let index_file = index_file.path();

    let builder = Builder::new(index_file).with_extsort_segment_size(200_000);
    builder
        .build(create_unknown_size_entries(1_000_000))
        .unwrap();

    let index = Reader::<UnsizedString, UnsizedString>::open(index_file).unwrap();
    let keys = vec![
        UnsizedString("aaaa".to_string()),
        UnsizedString("key:0".to_string()),
        UnsizedString("key:10000".to_string()),
        UnsizedString("key:999999".to_string()),
        UnsizedString("zzzz".to_string()),
    ];

    for key in keys {
        group.bench_with_input(BenchmarkId::new("key", &key), &key, |b, key| {
            b.iter(|| {
                black_box(index.find(key).unwrap());
            });
        });
    }
}

fn bench_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("iter_1million");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(7));
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.warm_up_time(Duration::from_millis(100));

    let index_file = tempfile::NamedTempFile::new().unwrap();
    let index_file = index_file.path();

    let builder = Builder::new(index_file).with_extsort_segment_size(200_000);
    builder
        .build(create_unknown_size_entries(1_000_000))
        .unwrap();

    let index = Reader::<UnsizedString, UnsizedString>::open(index_file).unwrap();

    group.bench_function("iter first entry", |b| {
        b.iter(|| {
            black_box(index.iter().next().unwrap());
        });
    });

    group.bench_function("iter 100 entries", |b| {
        b.iter(|| {
            black_box(index.iter().take(100).count());
        });
    });

    group.bench_function("iter all", |b| {
        b.iter(|| {
            black_box(index.iter().count());
        });
    });

    group.bench_function("revert iter, first entry", |b| {
        b.iter(|| {
            black_box(index.iter_reverse().next().unwrap());
        });
    });

    group.bench_function("reverse iter, 100 entries", |b| {
        b.iter(|| {
            black_box(index.iter_reverse().take(100).count());
        });
    });

    group.bench_function("reverse iter, all", |b| {
        b.iter(|| {
            black_box(index.iter_reverse().count());
        });
    });
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

criterion_group!(benches, bench_random_access, bench_iter);
criterion_main!(benches);
