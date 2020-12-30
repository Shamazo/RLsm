// mod crate::bloom_filter;
use bloom_filter::BloomFilter;
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_insert_normal_prob(c: &mut Criterion) {
    let mut bloom = BloomFilter::new_with_rate(0.05, 1000);
    c.bench_function("insert fpr 0.05", |b| b.iter(|| bloom.insert(&10)));
}

fn bench_contains_normal_prob(c: &mut Criterion) {
    let mut bloom = BloomFilter::new_with_rate(0.05, 1000);
    for i in 0..1000 {
        bloom.insert(&i);
    }
    c.bench_function("contains fpr 0.05", |b| b.iter(|| bloom.insert(&10)));
}

fn bench_insert_small_prob(c: &mut Criterion) {
    let mut bloom = BloomFilter::new_with_rate(0.0005, 1000);
    c.bench_function("insert fpr 0.0005 ", |b| b.iter(|| bloom.insert(&10)));
}

fn bench_contains_small_prob(c: &mut Criterion) {
    let mut bloom = BloomFilter::new_with_rate(0.0005, 1000);
    for i in 0..1000 {
        bloom.insert(&i);
    }
    c.bench_function("contains fpr 0.0005", |b| b.iter(|| bloom.contains(&10)));
}

criterion_group!(
    benches,
    bench_insert_normal_prob,
    bench_contains_normal_prob,
    bench_insert_small_prob,
    bench_contains_small_prob
);
criterion_main!(benches);
