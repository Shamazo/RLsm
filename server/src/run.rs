// use crate::bloom_filter::bloom_filter::BloomFilter;
use crate::fence_pointer::fence_pointer::FencePointer;
use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};
// use anyhow::Result;
use crate::bloom_filter::BloomFilter;
use std::mem;
use thiserror::Error;
use tokio::io;

#[derive(Error, Debug)]
pub enum RunError {
    #[error("IO Error")]
    IoError(#[from] io::Error),
    #[error("Not yet implemented")]
    NotImplemented,
}

// K -> key type
// TODO serialize this as it will need to be saved to file
#[derive(Serialize, Deserialize)]
pub struct Run<K: Ord> {
    num_pages: usize,
    level: usize,
    bloom_filter: BloomFilter,
    fence_pointers: Vec<FencePointer<K>>,
}

impl<K: Ord> Run<K> {
    // given a (full) SkipMap construct a run from it
    // on disk each run has metadata, and two files. One of values and one of keys.
    #[allow(dead_code)]
    pub fn new(memory_map: SkipMap<K, Vec<u8>>, level: usize) -> Result<Run<K>, RunError> {
        let fpr = 0.1; // TODO calculate the intended fpr using level
        let mut filter = BloomFilter::new_with_rate(fpr, memory_map.len());
        let fence_pointers = vec![];

        let keys_per_page = mem::size_of::<K>();

        // iterate over the SkipMap
        return Ok(Run {
            num_pages: 42,
            level: 1,
            bloom_filter: filter,
            fence_pointers: fence_pointers,
        });
    }
    #[allow(dead_code)]
    pub fn new_from_merge(left: Run<K>, right: Run<K>) -> Result<Run<K>, RunError> {
        return Err(RunError::NotImplemented);
    }

    // Returns value if it exists in the run
    #[allow(dead_code)]
    pub fn get_from_run(key: K) -> Option<Vec<u8>> {
        // check bloom filter
        // if in bloom filter iterate over page fence pointers
        // load the specified page and search.

        return None;
    }
}
