extern crate bit_vec;
extern crate core;
extern crate rand;

use bit_vec::BitVec;
use rand::Rng;
use seahash::SeaHasher;
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::hash::{Hash, Hasher};

#[derive(Serialize, Deserialize, Debug)]
struct Seeds {
    k1: u64,
    k2: u64,
    k3: u64,
    k4: u64,
}

impl Seeds {
    pub fn new() -> Seeds {
        let mut rng = rand::thread_rng();
        return Seeds {
            k1: rng.gen::<u64>(),
            k2: rng.gen::<u64>(),
            k3: rng.gen::<u64>(),
            k4: rng.gen::<u64>(),
        };
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BloomFilter {
    // The bits
    bits: BitVec,
    num_hashes: usize,
    seeds: Vec<Seeds>, // vector of length num_hashes
}

impl BloomFilter {
    /// Create a new Bloom filter
    ///
    /// # Arguments
    ///
    /// * `num_bits` - Length of the bit vector used
    /// * `num_hashes` - The number of hash functions to use
    /// * `seed` - An optional pseudo random seed used for the hash functions
    pub fn new_with_size(num_bits: usize, num_hashes: usize) -> BloomFilter {
        debug_assert!(num_bits >= 1);
        debug_assert!(num_hashes >= 1);
        let mut field = BitVec::with_capacity(num_bits);
        for _ in 0..num_bits {
            field.push(false);
        }

        let mut seed_vec = vec![];
        for _ in 0..num_hashes {
            seed_vec.push(Seeds::new());
        }

        field.shrink_to_fit();
        BloomFilter {
            bits: field,
            num_hashes: num_hashes,
            seeds: seed_vec,
        }
    }

    pub fn new_with_rate(fpr: f32, expected_num_items: usize) -> BloomFilter {
        let bits = needed_bits(fpr, expected_num_items);
        return BloomFilter::new_with_size(bits, optimal_num_hashes(bits, expected_num_items));
    }

    fn index_iterator<'a, T: Hash>(&'a self, item: &'a T) -> Vec<usize> {
        // collapse this into a vec to avoid references into the bloom filter after
        return (0..self.num_hashes)
            .map(move |i| {
                let s = &self.seeds[i];
                let mut hasher =
                    SeaHasher::with_seeds(s.k1.clone(), s.k2.clone(), s.k3.clone(), s.k4.clone());
                item.hash(&mut hasher);
                let hash = hasher.finish();
                hasher.write_u64(hash);
                hash as usize % self.bits.len()
            })
            .collect();
    }

    pub fn insert<T: Hash>(&mut self, item: &T) {
        for index in self.index_iterator(item) {
            self.bits.set(index, true)
        }
    }

    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        self.index_iterator(item)
            .iter()
            .all(|index| self.bits.get(*index).unwrap())
    }

    /// Clear the bloom filter.
    pub fn clear(&mut self) {
        self.bits.clear();
    }
}

/// Return the number of bits needed to satisfy the specified false
/// positive rate, if the filter will hold `num_items` items.
fn needed_bits(false_pos_rate: f32, num_items: usize) -> usize {
    let ln22 = core::f32::consts::LN_2 * core::f32::consts::LN_2;
    (num_items as f32 * ((1.0 / false_pos_rate).ln() / ln22)).round() as usize
}

/// Return the optimal number of hashes to use for the given number of
/// bits and items in a filter
fn optimal_num_hashes(num_bits: usize, num_items: usize) -> usize {
    min(
        max(
            (num_bits as f32 / num_items as f32 * core::f32::consts::LN_2).round() as usize,
            2,
        ),
        200,
    )
}

#[test]
fn can_insert_bloom_rate() {
    let mut b = BloomFilter::new_with_rate(0.01, 100);
    let item = 20;
    b.insert(&item);
    assert!(b.contains(&item))
}

#[test]
fn can_insert_bloom_size_one_hash() {
    let mut b = BloomFilter::new_with_size(100, 1);
    let item = 20;
    b.insert(&item);
    assert!(b.contains(&item))
}

#[test]
fn can_insert_bloom_size_two_hashes() {
    let mut b = BloomFilter::new_with_size(100, 2);
    let item = 20;
    b.insert(&item);
    assert!(b.contains(&item))
}

#[test]
fn can_insert_string_bloom() {
    let mut b = BloomFilter::new_with_rate(0.01, 100);
    let item: String = "hello world".to_owned();
    b.insert(&item);
    assert!(b.contains(&item))
}

#[test]
fn can_clear_bloom() {
    let mut b = BloomFilter::new_with_rate(0.01, 100);
    let item: String = "hello world".to_owned();
    b.insert(&item);
    assert!(b.contains(&item));
    b.clear();
    assert!(!b.contains(&item));
}

#[test]
fn does_not_contain() {
    let mut b = BloomFilter::new_with_rate(0.01, 100);
    let upper = 100;
    for i in (0..upper).step_by(2) {
        b.insert(&i);
        assert_eq!(b.contains(&i), true);
    }
    for i in (1..upper).step_by(2) {
        assert_eq!(b.contains(&i), false);
    }
}

#[test]
fn can_insert_lots() {
    let mut b = BloomFilter::new_with_rate(0.01, 1024);
    for i in 0..1024 {
        b.insert(&i);
        assert!(b.contains(&i))
    }
}

#[test]
fn test_refs() {
    let item = String::from("Hello World");
    let mut b = BloomFilter::new_with_rate(0.01, 100);
    b.insert(&item);
    assert!(b.contains(&item));
}

#[test]
fn bloom_filter_can_serialize_deserialize() {
    let mut b = BloomFilter::new_with_rate(0.01, 1024);
    let upper = 100;
    for i in (0..upper).step_by(2) {
        b.insert(&i);
        assert_eq!(b.contains(&i), true);
    }
    for i in (1..upper).step_by(2) {
        assert_eq!(b.contains(&i), false);
    }

    let s = serde_json::to_string(&b).unwrap();
    let serde_b: BloomFilter = serde_json::from_str(&s).unwrap();

    for i in (0..upper).step_by(2) {
        assert_eq!(serde_b.contains(&i), true);
    }
    for i in (1..upper).step_by(2) {
        assert_eq!(serde_b.contains(&i), false);
    }
}
