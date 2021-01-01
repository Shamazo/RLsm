use crossbeam_skiplist::map::{Iter, Range};
use crossbeam_skiplist::SkipMap;
use std::ops::RangeBounds;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MemoryMapError {
    #[error("Unknown")]
    Unknown,
}

pub trait Map<K: Ord + Send + 'static> {
    // type RangeType;
    // Static method signature; `Self` refers to the implementor type.
    fn new() -> Self;

    fn put(&self, key: K, value: Vec<u8>) -> Result<(), MemoryMapError>;
    fn get(&self, key: K) -> Option<Vec<u8>>;
    // fn range(&self, range: RangeBounds<i32>) -> Self::RangeType;
    fn len(&self) -> usize;
}

impl<K: Ord + Send + 'static> Map<K> for SkipMap<K, Vec<u8>> {
    fn new() -> Self {
        return SkipMap::new();
    }

    fn put(&self, key: K, value: Vec<u8>) -> Result<(), MemoryMapError> {
        self.insert(key, value);
        return Ok(());
    }

    fn get(&self, key: K) -> Option<Vec<u8>> {
        let res = self.get(&key);
        if res.is_none() {
            return None;
        } else {
            return Some(res.unwrap().value().clone()); // I hope I am cloning the value and not the ref here
        }
    }

    fn len(&self) -> usize {
        self.len()
    }
}

// ///
// /// Putting this behind a layer of abstraction so I can easily swap out Map
// /// implementations.
// pub struct MemoryMap<T> {
//     memory_map: T,
//     approx_mem_usage: usize, // very approx TODO make this approx
// }
// unsafe impl Send for MemoryMap<SkipMap<i32, Vec<u8>>> {}
// unsafe impl Sync for MemoryMap<SkipMap<i32, Vec<u8>>> {}
//
// impl MemoryMap<SkipMap<i32, Vec<u8>>> {
//     pub fn new() -> MemoryMap<SkipMap<i32, Vec<u8>>> {
//         return MemoryMap {
//             memory_map: SkipMap::new(),
//             approx_mem_usage: 0,
//         };
//     }
//
//     pub fn get(self: &Self, key: i32) -> Option<Vec<u8>> {
//         let res = self.memory_map.get(&key);
//         return if res.is_none() {
//             None
//         } else {
//             Some(res.unwrap().value().to_vec())
//         };
//     }
//
//     pub fn put(self: &mut Self, key: i32, val: Vec<u8>) -> () {
//         self.approx_mem_usage += 4 + val.len();
//         self.memory_map.insert(key, val);
//         return ();
//     }
//
//     // iter and range are already implemented for SkipMap
//
//     // pub fn iter(self: &Self) -> Iter<i32, Vec<u8>>
//
//     // pub fn range<R>(self: &Self, range: R) -> Range<i32, R, i32, Vec<u8>>
//     // where
//     //     R: RangeBounds<i32>,
//
//     pub fn len(&self) -> usize {
//         self.memory_map.len()
//     }
//
//     pub fn approx_memory_usage(&self) -> usize {
//         return self.approx_mem_usage;
//     }
// }
