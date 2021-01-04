// mod crate::bloom_filter;
use criterion::BatchSize::NumBatches;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use crossbeam_skiplist::SkipMap;
use rand::{seq::SliceRandom, Rng, SeedableRng}; // 0.6.5
use rand_chacha::ChaChaRng;
use rust_kv::memory_map::Map;
use rust_kv::run::Run;
use rust_kv::Config;
use std::time::Duration;

const KB: u64 = 1024;
const MB: u64 = KB * 1024;

// #[derive(Clone, Debug)]
// struct params {
//     skipmap: SkipMap<i32, Vec<u8>>,
// }

// fn insert_one_thread<T: Map<i32>>(params: Vec<params>, m: &T) -> u64 {
//     for i in 0..params.len() {
//         m.put(params[i].key, params[i].value.clone());
//     }
//     return 0;
// }

// generates a random number of random bytes
fn gen_rand_bytes<T: Rng>(mut rng: &mut T) -> Vec<u8> {
    let num_bytes = rng.gen_range(1..256);
    let mut ret_vec = vec![0u8; num_bytes];
    rng.fill_bytes(&mut ret_vec);
    return ret_vec;
}

// Size in bytes approx
fn create_skipmap(size: u64) -> SkipMap<i32, Vec<u8>> {
    let mut curr_size: u64 = 0;
    let map = SkipMap::new();
    let seed = [42; 32];
    let mut rng = ChaChaRng::from_seed(seed);

    while curr_size < size {
        let rand_key: i32 = rng.gen();
        let rand_val = gen_rand_bytes(&mut rng);
        curr_size += 4 + rand_val.len() as u64;
        map.insert(rand_key, rand_val);
    }

    return map;
}

fn run_create_from_skiplist(c: &mut Criterion) {
    let mut group = c.benchmark_group("run from skiplist (MB)");
    group.measurement_time(Duration::new(100, 0));
    group.sample_size(10);
    for n in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 4096].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, &n| {
            // let map = skipmap.clone();
            let mut config = Config::default();
            let dir = tempfile::tempdir().unwrap();
            config.set_directory(dir.path());

            b.iter_batched(
                || create_skipmap(n * MB as u64),
                |skipmap| Run::new(skipmap, &config),
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn run_create_from_32MB_skiplist_by_block_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("run from 32MB skiplist  (pagesize KB))");
    group.measurement_time(Duration::new(100, 0));
    group.sample_size(10);
    for n in [1, 2, 4, 8, 16, 32, 64].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, &n| {
            // let map = skipmap.clone();
            let mut config = Config::default();
            let dir = tempfile::tempdir().unwrap();
            config.set_directory(dir.path());
            config.set_block_size(n * KB);

            b.iter_batched(
                || create_skipmap(32 * MB as u64),
                |skipmap| Run::new(skipmap, &config),
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(
    run_benches,
    run_create_from_32MB_skiplist_by_block_size,
    run_create_from_skiplist
);
criterion_main!(run_benches);
