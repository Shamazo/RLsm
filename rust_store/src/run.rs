// use crate::bloom_filter::bloom_filter::BloomFilter;
use crate::fence_pointer::fence_pointer::FencePointer;
use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};
use serde_cbor::Deserializer;
// use anyhow::Result;
use crate::bloom_filter::BloomFilter;
use serde::de::DeserializeOwned;
use std::fs::File;
use std::hash::Hash;
use std::io::prelude::*;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{io, mem};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RunError {
    #[error("IO Error")]
    IoError(#[from] io::Error),
    #[error("Error Deserializing")]
    DeserializeError(#[from] serde_cbor::Error),
    #[error("Creating run from memory map")]
    RunCreationError,
    #[error("Not yet implemented")]
    NotImplemented,
}

pub struct Level<K: Ord + Serialize + DeserializeOwned> {
    pub num_runs: usize,
    pub runs: Vec<Run<K>>,
}

impl<K: Ord + Serialize + DeserializeOwned + Copy + Hash> Level<K> {
    pub fn read_from_disk() -> Level<K> {
        unimplemented!();
    }

    pub fn get_from_level(&self, key: &K) -> Option<Vec<u8>> {
        if self.runs.len() == 0 {
            return None;
        };
        for run in &self.runs {
            let maybe_res = run.get_from_run(key);
            if maybe_res.is_some() {
                return maybe_res;
            }
        }
        return None;
    }
}

#[derive(Serialize, Deserialize)]
struct Item<K: Ord> {
    key: K,
    value: Vec<u8>,
}

//this struct lives at the end of the on disk file
// ie in the last sizeof(RunError) bytes
// K -> key type
#[derive(Serialize, Deserialize)]
pub struct Run<K: Ord> {
    num_pages: usize,
    level: usize,
    bloom_filter: BloomFilter,
    fence_pointers: Vec<FencePointer<K>>,
    file_name: PathBuf,
}

impl<'de, K: Ord + Serialize + Deserialize<'de> + Copy + Hash> Run<K> {
    // given a (full) SkipMap construct a run from it
    // on disk each run has metadata, and two files. One of values and one of keys.
    #[allow(dead_code)]
    pub fn new(memory_map: SkipMap<K, Vec<u8>>) -> Result<Run<K>, RunError> {
        if memory_map.len() == 0 {
            return Err(RunError::RunCreationError);
        }
        let fpr = 0.1; // TODO calculate the intended fpr using level
        let mut filter = BloomFilter::new_with_rate(fpr, memory_map.len());
        let mut fence_pointers = vec![];

        let mut min_val: K = *memory_map.front().unwrap().key(); // min val on the page
        let mut max_val: K = *memory_map.back().unwrap().key(); // max val on the page

        let file_name = "TODO this path calculation ";
        let mut writer = BufWriter::new(File::open(file_name)?);

        // This is an index into a page
        let mut idx: usize = 0;
        let mut num_pages: usize = 1;

        for item in memory_map {
            filter.insert(&item.0);
            let ser_item = serde_cbor::to_vec(&item)?;
            // page finished, pad it out
            if (idx + ser_item.len() > 4096) {
                // finish the page
                let zeros = vec![0u8; 4096 - idx];
                idx += writer.write(&zeros)?;
                debug_assert!(idx == 4096);
                idx = 0;
                fence_pointers.push(FencePointer::new(min_val, max_val));

                // start the next page.
                // if there is a bug related to min/max values if there is one value on a page, this is why
                num_pages += 1;
                min_val = item.0;
                idx += writer.write(&ser_item)?;
            } else {
                idx += writer.write(&ser_item)?;
                max_val = item.0;
            }
        }

        debug_assert!(fence_pointers.len() == num_pages);
        let run = Run {
            num_pages: fence_pointers.len(),
            level: 1,
            bloom_filter: filter,
            fence_pointers: fence_pointers,
            file_name: file_name.parse().unwrap(),
        };

        let ser_meta = serde_cbor::to_vec(&run)?;
        let ser_meta_length = writer.write(&ser_meta)?;
        let ser_ser_meta_length = serde_cbor::to_vec(&ser_meta_length)?;
        writer.write(&ser_ser_meta_length)?;
        writer.flush();
        return Ok(run);
    }

    pub async fn load(path: String) -> Result<Run<K>, RunError> {
        // // TODO figure out config and root file directory stuff
        // let mut f = File::open(path).await?;
        // let _ = f.seek(SeekFrom::End(-8)).await?;
        // let meta_size = f.read_u64().await?;
        //
        // let mut meta_buf = Box::new(vec![0u8; meta_size as usize]);
        // f.read_exact(&mut **meta_buf);
        // let mut deserializer = Deserializer::from_slice(&meta_buf);
        // let run = serde::de::Deserialize::deserialize(&mut deserializer)?;
        // return Ok(run);
        unimplemented!();
    }

    #[allow(dead_code)]
    pub fn new_from_merge(left: Run<K>, right: Run<K>) -> Result<Run<K>, RunError> {
        return Err(RunError::NotImplemented);
    }

    // Returns value if it exists in the run
    #[allow(dead_code)]
    pub fn get_from_run(self: &Self, key: &K) -> Option<Vec<u8>> {
        // check bloom filter
        // if in bloom filter iterate over page fence pointers
        // load the specified page and search.

        return None;
    }
}
