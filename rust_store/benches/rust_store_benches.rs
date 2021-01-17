// mod crate::bloom_filter;
use criterion::BatchSize::NumBatches;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use rust_kv::workload_generator::{RequestType, WorkloadParameters};
use rust_kv::{Config, RustStore};
use std::time::Duration;

fn printerfunc(c: &mut Criterion) {
    let params = WorkloadParameters::default();
    println!("{}", params);
    c.bench_function("contains fpr 0.05", |b| b.iter(|| 10 + 2));
}

fn single_threaded_runner(store: RustStore, workload: WorkloadParameters) {
    for op in workload.into_iter() {
        match op.request_type {
            RequestType::Get => {
                store.get(&op.key);
            }
            RequestType::Put => {
                store.put(op.key, op.value.unwrap());
            }
            RequestType::Delete => {
                store.delete(&op.key);
            }
        }
    }
}

fn bench_single_threaded_puts(c: &mut Criterion) {
    env_logger::try_init();
    let mut group = c.benchmark_group("Single threaded inserts)");
    // group.measurement_time(Duration::new(100, 0));
    group.sample_size(10);
    for n in [10000, 100000, 10000000, 100000000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, &n| {
            b.iter_batched(
                || {
                    let mut config = Config::default();
                    let dir = tempfile::tempdir().unwrap();
                    config.set_directory(dir.path());
                    RustStore::new(Some(config))
                },
                |rust_store| {
                    let workload = WorkloadParameters::new(n, 0, 0, 0, 0.0, 0.0, [42; 32]).unwrap();
                    single_threaded_runner(rust_store, workload);
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(rust_store_benches, bench_single_threaded_puts);
criterion_main!(rust_store_benches);
