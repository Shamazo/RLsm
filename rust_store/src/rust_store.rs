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
    #[error("Invalid option")]
    OptionParsingError(),
    #[error("Not yet implemented")]
    NotImplemented,
}

pub struct RustStore {
    lsm: Arc<Lsm<SkipMap<i32, Vec<u8>>>>,
}

/// The options struct for rust_store.
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
    /// # Arguments
    ///
    /// * `name` - A string slice that holds the name of the person
    ///
    /// # Examples
    /// ```
    /// use rust_kv::Config;
    /// let config = Config::default();
    /// ```
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
    /// Sets the directory for data
    ///
    /// # Arguments
    ///
    /// * `dir` - A string containing the path to the directory where rust_store will store data
    ///             e.g /home/steve/data
    pub fn set_directory(self: &mut Self, dir: &str) {
        self.directory = Some(dir.parse().unwrap());
    }

    /// Sets the memory budget for the in memory store
    /// # Arguments
    ///
    /// * `budget` - The maximum number of bytes to be used by the in memory store.
    ///              Currently this is not a hard limit, but a threshold which will trigger the creation
    ///              of a new run. If there is a high write rate then the budget may be exceeded while
    ///              creating the new run.
    pub fn set_memory_map_budget(self: &mut Self, budget: usize) {
        self.memory_map_budget = budget;
    }

    /// Sets the memory budget for the in memory store
    /// # Arguments
    ///
    /// * `budget` - The maximum number of bytes to be used by bloom filters in each run.
    ///              Bloom filters are used for each run during reads to check if a value exists
    ///              to avoid costly IO if it does not. A larger budget will reduce the number
    ///              of false positives during get operations
    pub fn set_bloom_filter_budget(self: &mut Self, budget: usize) {
        self.bloom_filter_budget = budget;
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
