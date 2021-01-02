use crate::lsm::{Lsm, LsmError};
use crate::memory_map;
use crate::run;
use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

const KB: usize = 1024;
const MB: usize = KB * 1024;

#[derive(Error, Debug)]
pub enum RustStoreError {
    #[error("Error in a run")]
    RunError(#[from] run::RunError),
    #[error("Error in the LSM")]
    LsmError(#[from] LsmError),
    #[error("Not yet implemented")]
    NotImplemented,
}

pub struct RustStore {
    lsm: Arc<Lsm<SkipMap<i32, Vec<u8>>>>,
}

#[derive(Serialize, Deserialize)]
pub struct Config {
    /// Max size in bytes for the in memory map
    pub memory_map_budget: usize,
    /// Max size in bytes for the in memory bloom filters
    pub bloom_filter_budget: usize,
    /// Size ratio between levels
    pub T: usize,
    /// Maximum number of runs in each level other than the largest
    pub K: usize,
    /// Max number of rans at the largest level.
    pub Z: usize,
    pub directory: Option<PathBuf>,
}

impl Config {
    pub fn default() -> Config {
        return Config {
            memory_map_budget: 100 * MB,
            bloom_filter_budget: 10 * MB,
            T: 10,
            K: 10,
            Z: 10,
            directory: None,
        };
    }
}

impl RustStore {
    pub fn new(config: Option<Config>) -> RustStore {
        return RustStore {
            lsm: Lsm::new(config),
        };
    }

    #[allow(dead_code)]
    pub fn get(self: &Self, key: &i32) -> Option<Vec<u8>> {
        return self.lsm.get(key);
    }

    #[allow(dead_code)]
    pub fn put(self: &Self, key: i32, val: Vec<u8>) -> Result<(), RustStoreError> {
        let res = self.lsm.put(key, val)?;
        return Ok(res);
    }
}
