// mod crate::bloom_filter;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use crossbeam_skiplist::SkipMap;
use rand::{seq::SliceRandom, SeedableRng}; // 0.6.5
use rand_chacha::ChaChaRng;
use rust_kv::memory_map::Map;
use std::time::Duration;

#[derive(Clone, Debug)]
struct params {
    key: i32,
    value: Vec<u8>,
}

fn insert_one_thread<T: Map<i32>>(params: Vec<params>, m: &T) -> u64 {
    for i in 0..params.len() {
        m.put(params[i].key, params[i].value.clone()).unwrap();
    }
    return 0;
}

fn bench_skiplist_insert_10000_one_thread(c: &mut Criterion) {
    // we shuffle the keys, but with a fixed RNG/shuffle to have less variability
    let mmap: SkipMap<i32, Vec<u8>> = SkipMap::new();
    let num_items: i32 = 10000;
    let mut keys: Vec<i32> = (0..num_items).collect();
    let seed = [42; 32];
    let mut rng = ChaChaRng::from_seed(seed);
    keys.shuffle(&mut rng);
    let one_val: Vec<u8> = (0..8).collect();
    let vals = vec![one_val; num_items as usize];

    let mut params = vec![];
    for i in 0..(num_items as usize) {
        params.push(params {
            key: keys[i],
            value: vals[i].clone(),
        })
    }
    c.bench_function("skiplist 10,000 insert one thread", |b| {
        b.iter(|| insert_one_thread(params.clone(), &mmap))
    });
}

fn bench_skiplist_insert_100000_one_thread(c: &mut Criterion) {
    let mmap: SkipMap<i32, Vec<u8>> = SkipMap::new();
    let num_items: i32 = 100000;
    let mut keys: Vec<i32> = (0..num_items).collect();
    let seed = [42; 32];
    let mut rng = ChaChaRng::from_seed(seed);
    keys.shuffle(&mut rng);
    let one_val: Vec<u8> = (0..8).collect();
    let vals = vec![one_val; num_items as usize];

    let mut params = vec![];
    for i in 0..num_items as usize {
        params.push(params {
            key: keys[i],
            value: vals[i].clone(),
        })
    }
    c.bench_function("skiplist 100,000 insert one thread", |b| {
        b.iter(|| insert_one_thread(params.clone(), &mmap))
    });
}
use crossbeam_utils::thread::scope;
fn insert_n_threads_k_values(n: usize, param_vecs: Vec<Vec<params>>) {
    let m: SkipMap<i32, Vec<u8>> = SkipMap::new();
    scope(|s| {
        for param_vec in param_vecs.into_iter() {
            s.spawn(|s| {
                for param in param_vec {
                    m.put(param.key, param.value.clone()).unwrap();
                }
            });
        }
    })
    .unwrap();
}

fn bench_skiplist_insert_100000_n_threads(c: &mut Criterion) {
    let mut group = c.benchmark_group("skiplist multithreaded 100,000 inserts");
    group.measurement_time(Duration::new(10, 0));
    for n in [1, 2, 4, 8, 16].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, &n| {
            let seed = [42; 32];
            let mut rng = ChaChaRng::from_seed(seed);
            let keys_per_thread = 100000 / n;
            let mut param_vecs = vec![];

            for i in 0..(n as i32) {
                let mut keys: Vec<i32> =
                    ((i * keys_per_thread as i32)..((i + 1) * keys_per_thread as i32)).collect();
                keys.shuffle(&mut rng);

                let one_val: Vec<u8> = (0..8).collect();
                let mut params = vec![];
                for i in 0..keys.len() as usize {
                    params.push(params {
                        key: keys[i],
                        value: one_val.clone(),
                    })
                }
                param_vecs.push(params);
            }
            b.iter(|| insert_n_threads_k_values(n, param_vecs.clone()));
        });
    }
    group.finish();
}

criterion_group!(
    skiplist_benches,
    bench_skiplist_insert_10000_one_thread,
    bench_skiplist_insert_100000_one_thread,
    bench_skiplist_insert_100000_n_threads
);
criterion_main!(skiplist_benches);
