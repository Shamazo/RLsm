use crate::run::{Level, Run, RunError};
use serde::{Deserialize, Serialize};

use crate::memory_map::Map;
// use crate::run_manager::run_manager;
use crate::rust_store;
use crate::rust_store::Config;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use thiserror::Error;
use tokio::time::Duration;

#[derive(Error, Debug)]
pub enum LsmError {
    #[error("Error Deserializing")]
    RunError(#[from] RunError),
    #[error("Not yet implemented")]
    NotImplemented,
}

pub struct Lsm<T: Map<i32> + IntoIterator + Send + Sync> {
    #[allow(dead_code)]
    primary_memory_map: RwLock<T>,
    #[allow(dead_code)]
    secondary_memory_map: RwLock<T>,
    #[allow(dead_code)]
    use_primary_map: AtomicBool,
    primary_memory_map_memory_use: AtomicUsize,
    secondary_memory_map_use: AtomicUsize,
    config: rust_store::Config,
    levels: Vec<RwLock<Level<i32>>>,
    /// Signal to threads that we are shutting down
    time_to_shutdown: AtomicBool,
}

impl<T: 'static + Map<i32> + IntoIterator + Send + Sync> Lsm<T> {
    pub fn new(config: Option<Config>) -> Arc<Lsm<T>> {
        let config_to_use = match config {
            Some(config) => config,
            None => Config::default(),
        };

        if config_to_use.directory.is_some() {
            //TODO check and load from disk
            unimplemented!();
        }

        let mut lsm = Arc::new(Lsm {
            primary_memory_map: RwLock::new(T::new()),
            secondary_memory_map: RwLock::new(T::new()),
            use_primary_map: Default::default(),
            primary_memory_map_memory_use: AtomicUsize::new(0),
            secondary_memory_map_use: AtomicUsize::new(0),
            config: config_to_use,
            levels: vec![],
            time_to_shutdown: AtomicBool::new(false),
        });

        let manager_lsm = Arc::clone(&lsm);
        thread::spawn({ move || manager_lsm.run_manager() });

        return lsm;
    }

    #[allow(dead_code)]
    pub fn get(self: &Self, key: &i32) -> Option<Vec<u8>> {
        // TODO verify locking
        // If use_primary_map is true that means newer data is in the primary map, so we should
        // check it first.
        // If the key is in neither of the memory maps then we need to check each level
        let use_primary_map = self.use_primary_map.load(Ordering::SeqCst);
        if use_primary_map {
            {
                let primary_map = self.primary_memory_map.read();
                let maybe_res = primary_map.get(key);
                if maybe_res.is_some() {
                    return maybe_res;
                }
            }
            {
                let secondary_map = self.secondary_memory_map.read();
                let maybe_res = secondary_map.get(key);
                if maybe_res.is_some() {
                    return maybe_res;
                }
            }
        } else {
            {
                let secondary_map = self.secondary_memory_map.read();
                let maybe_res = secondary_map.get(key);
                if maybe_res.is_some() {
                    return maybe_res;
                }
            }
            {
                let primary_map = self.primary_memory_map.read();
                let maybe_res = primary_map.get(key);
                if maybe_res.is_some() {
                    return maybe_res;
                }
            }
        }

        // for i in 0..self.levels.len(){
        //     let level = self.levels[i].read();
        //     let maybe_res = level.get(key)?;
        //     if maybe_res.
        //
        // }
        return None;
    }
    #[allow(dead_code)]
    pub fn put(self: &Self, key: i32, val: Vec<u8>) -> Result<(), LsmError> {
        if self.use_primary_map.load(Ordering::SeqCst) {
            let m = self.primary_memory_map.read();
            m.put(key, val);
        } else {
            let m = self.secondary_memory_map.read();
            m.put(key, val);
        }
        return Ok(());
    }

    fn time_to_merge_memmap(self: &Self) -> bool {
        // if self.mem
        // if memory map approx size > config.budget retun true else false
        unimplemented!()
    }

    pub fn run_manager(self: &Self) {
        loop {
            if self.time_to_shutdown.load(Ordering::Relaxed) {
                // cleanup and save necessary stuff to disk
                // probably just putting writing the memory maps to new runs
            }

            if self.time_to_merge_memmap() {
                // make a new run.
            }

            std::thread::sleep(Duration::new(1, 0))
        }
    }
}
