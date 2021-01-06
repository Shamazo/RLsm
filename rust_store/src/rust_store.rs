use crate::lsm::{Lsm, LsmError};
use crate::run;
use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

const KB: u64 = 1024;
const MB: u64 = KB * 1024;

#[derive(Error, Debug)]
pub enum RustStoreError {
    #[error("Error in a run")]
    RunError(#[from] run::RunError),
    #[error("Error in the LSM")]
    LsmError(#[from] LsmError),
    #[error("Invalid option: '{0}'")]
    OptionParsingError(String),
    #[error("Not yet implemented")]
    NotImplemented,
}

pub struct RustStore {
    lsm: Arc<Lsm>,
}

#[derive(Serialize, Deserialize)]
pub enum MemoryMapType {
    IntSkipList,
}

//TODO make these fields private and also accessible from the rest of the crate.
/// The options struct for rust_store.
#[derive(Serialize, Deserialize)]
pub struct Config {
    /// Max size in bytes for the in memory map
    pub memory_map_budget: u64,
    /// Max size in bytes for the in memory bloom filters
    pub bloom_filter_budget: u64,
    /// Size ratio between levels
    pub T: u64,
    /// Maximum number of runs in each level other than the largest
    pub K: u64,
    /// Max number of runs at the largest level.
    pub Z: u64,
    /// Directory for RustStore to use
    pub directory: Option<PathBuf>,
    /// Block size for runs.
    pub block_size: u64,
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
            block_size: 4 * KB,
        };
    }
    /// Sets the directory for data
    ///
    /// # Arguments
    ///
    /// * `dir` - A string containing the path to the directory where rust_store will store data
    ///             e.g /home/steve/data
    pub fn set_directory(self: &mut Self, dir: &Path) {
        //TODO maybe check path exists?
        self.directory = Some(PathBuf::from(dir));
    }

    /// Sets the memory budget for the in memory store
    /// # Arguments
    ///
    /// * `budget` - The maximum number of bytes to be used by the in memory store.
    ///              Currently this is not a hard limit, but a threshold which will trigger the creation
    ///              of a new run. If there is a high write rate then the budget may be exceeded while
    ///              creating the new run.
    pub fn set_memory_map_budget(self: &mut Self, budget: u64) -> Result<(), RustStoreError> {
        if budget == 0 {
            return Err(RustStoreError::OptionParsingError(
                "memory map budget cannot be 0".parse().unwrap(),
            ));
        }
        self.memory_map_budget = budget;
        return Ok(());
    }

    /// Sets the memory budget for the in memory store
    /// # Arguments
    ///
    /// * `budget` - The maximum number of bytes to be used by bloom filters in each run.
    ///              Bloom filters are used for each run during reads to check if a value exists
    ///              to avoid costly IO if it does not. A larger budget will reduce the number
    ///              of false positives during get operations
    pub fn set_bloom_filter_budget(self: &mut Self, budget: u64) {
        self.bloom_filter_budget = budget;
    }

    /// Sets the block size for each run.
    ///
    /// Blocks are the atomic IO unit of runs. Each block has associated fencepointers so when we
    /// need to get a value from a run we can identify which block contains it. Blocks are also
    /// compressed individually. A higher block size should reduce the same it takes to construct a run,
    /// but may slow read / lookup times.
    pub fn set_block_size(self: &mut Self, size: u64) -> Result<(), RustStoreError> {
        if size == 0 {
            return Err(RustStoreError::OptionParsingError(
                "block size cannot be 0".parse().unwrap(),
            ));
        }
        self.block_size = size;
        return Ok(());
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
