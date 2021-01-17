use crate::run::{Level, Run, RunError};

// use crate::run_manager::run_manager;
use crate::rust_store;
use crate::rust_store::Config;
use crossbeam_skiplist::SkipMap;
use log::{debug, error, info, trace, warn};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::borrow::BorrowMut;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum LsmError {
    #[error("Error Deserializing")]
    RunError(#[from] RunError),
    #[error("Not yet implemented")]
    NotImplemented,
}

#[derive(Serialize, Deserialize)]
pub struct Catalogue {
    // each outer vector is a level ordered 1 -> n
    // each inner vector is a run, earlier runs are older
    levels: Vec<Vec<PathBuf>>,
}

impl Catalogue {
    pub fn get_levels(self: Self) -> Vec<RwLock<Level>> {
        // for each path
        unimplemented!();
    }
}

pub struct Lsm {
    primary_memory_map: Arc<SkipMap<i32, Option<Vec<u8>>>>,
    secondary_memory_map: Arc<SkipMap<i32, Option<Vec<u8>>>>,
    use_primary_map: AtomicBool,
    primary_memory_map_memory_use: AtomicU64,
    secondary_memory_map_memory_use: AtomicU64,
    config: rust_store::Config,
    levels: RwLock<Vec<RwLock<Level>>>,
    /// Signal to threads that we are shutting down
    time_to_shutdown: AtomicBool,
}

impl Lsm {
    pub fn new(config: Option<Config>) -> Arc<Lsm> {
        info!("Creating new LSM");
        let config_to_use = match config {
            Some(config) => config,
            None => Config::default(),
        };

        if config_to_use.directory.is_some() {
            //TODO check and load from disk
            // check for .catalogue file
            // unimplemented!();
        }

        let lsm = Arc::new(Lsm {
            primary_memory_map: Arc::new(SkipMap::new()),
            secondary_memory_map: Arc::new(SkipMap::new()),
            use_primary_map: AtomicBool::new(true),
            primary_memory_map_memory_use: AtomicU64::new(0),
            secondary_memory_map_memory_use: AtomicU64::new(0),
            config: config_to_use,
            levels: RwLock::new(vec![]),
            time_to_shutdown: AtomicBool::new(false),
        });

        let manager_lsm = lsm.clone();
        info!("Spawning manager thread");
        thread::spawn(move || manager_lsm.run_manager());

        return lsm;
    }

    pub fn get(self: &Self, key: &i32) -> Option<Vec<u8>> {
        // TODO verify locking
        // If use_primary_map is true that means newer data is in the primary map, so we should
        // check it first.
        // If the key is in neither of the memory maps then we need to check each level
        let use_primary_map = self.use_primary_map.load(Ordering::SeqCst);
        if use_primary_map {
            trace!("Getting value for key {} trying primary map first", key);
            // let primary_map = self.primary_memory_map.read();
            let maybe_res = self.primary_memory_map.get(key);
            if maybe_res.is_some() {
                return maybe_res.unwrap().value().clone();
            }

            // let secondary_map = self.secondary_memory_map.read();
            let maybe_res = self.secondary_memory_map.get(key);
            if maybe_res.is_some() {
                return maybe_res.unwrap().value().clone();
            }
        } else {
            trace!("Getting value for key {} trying secondary map first", key);

            let maybe_res = self.secondary_memory_map.get(key);
            if maybe_res.is_some() {
                return maybe_res.unwrap().value().clone();
            }

            let maybe_res = self.primary_memory_map.get(key);
            if maybe_res.is_some() {
                return maybe_res.unwrap().value().clone();
            }
        }

        trace!(
            "Value for key {} not found in memory, searching levels",
            key
        );
        let read_levels = self.levels.read();
        for i in 0..read_levels.len() {
            let level = read_levels[i].read();
            let maybe_res = level.get_from_level(key);
            if maybe_res.is_some() {
                return maybe_res;
            }
        }

        trace!("No value found for key {} ", key);

        return None;
    }

    // Internally Values are stored as Option<Vec<u8>> and None is reserved to mean key deleted
    pub fn delete(self: &Self, key: &i32) -> () {
        if self.use_primary_map.load(Ordering::SeqCst) {
            self.primary_memory_map.insert(key.clone(), None);
        } else {
            self.secondary_memory_map.insert(key.clone(), None);
        }
    }

    pub fn put(self: &Self, key: i32, val: Vec<u8>) -> () {
        if self.use_primary_map.load(Ordering::SeqCst) {
            trace!("Putting key {} into primary memmap", &key);
            let memuse =
                self.primary_memory_map_memory_use.load(Ordering::SeqCst) + 4 + val.len() as u64;
            self.primary_memory_map.insert(key, Some(val));
            self.primary_memory_map_memory_use
                .store(memuse, Ordering::SeqCst);
        } else {
            trace!("Putting key {} into secondary memmap", &key);
            let memuse =
                self.secondary_memory_map_memory_use.load(Ordering::SeqCst) + 4 + val.len() as u64;
            self.secondary_memory_map.insert(key, Some(val));
            self.secondary_memory_map_memory_use
                .store(memuse, Ordering::SeqCst);
        }
    }

    fn time_to_merge_primary_memmap(self: &Self) -> bool {
        let primary_mem_use = self.primary_memory_map_memory_use.load(Ordering::Relaxed);
        debug!(
            "time_to_merge primary: primary mem use: {} primary mem budget {}",
            primary_mem_use, self.config.memory_map_budget
        );
        if primary_mem_use > self.config.memory_map_budget {
            return true;
        }
        return false;
    }

    fn time_to_merge_secondary_memmap(self: &Self) -> bool {
        let secondary_mem_use = self.secondary_memory_map_memory_use.load(Ordering::Relaxed);
        debug!(
            "time_to_merge secondary: secondary mem use: {} secondary mem budget {}",
            secondary_mem_use, self.config.memory_map_budget
        );
        // if self.use_primary_map.load(Ordering::SeqCst) == false {
        if secondary_mem_use > self.config.memory_map_budget {
            return true;
        }
        // }
        return false;
    }

    // levels on disk are 1 indexed, level 0 is the in memory map
    pub fn add_run_to_level(self: &Self, run: Run, level: usize) {
        //insert into existing level
        let mut levels = self.levels.write();
        if levels.len() < level {
            info!("creating a new level {}", &level);
            debug_assert!(levels.len() == level - 1);
            levels.push(RwLock::new(Level {
                num_runs: 1,
                runs: vec![Arc::new(run)],
            }))
        } else {
            info!("inserting into existing level {}", &level);
            debug_assert!(levels.len() > level - 1);
            let mut level = &mut *levels[level - 1].write();
            level.runs.push(Arc::new(run));
            level.num_runs += 1;
        }
        info!("Completed adding run to level {}", &level);
    }

    pub fn run_manager(self: &Self) {
        info!("Starting up run_manager");
        loop {
            if self.time_to_shutdown.load(Ordering::Relaxed) {
                return;
            }
            std::thread::sleep(Duration::new(1, 0));
            if self.time_to_shutdown.load(Ordering::Relaxed) {
                info!("run_manager shutting down");
                // cleanup and save necessary stuff to disk
                // probably just putting writing the memory maps to new runs
            }

            if self.time_to_merge_primary_memmap() {
                info!("About to write primary mmap to disk");
                self.use_primary_map.store(false, Ordering::SeqCst);
                // let primary_map = self.primary_memory_map.write();
                let new_run = Run::new_from_skipmap(self.primary_memory_map.clone(), &self.config);
                if new_run.is_err() {
                    error!("Error creating run from memory map: {:?}", new_run.err());
                    continue;
                } else {
                    self.add_run_to_level(new_run.unwrap(), 1);
                    self.primary_memory_map_memory_use
                        .store(0, Ordering::SeqCst);
                }
            } else if self.time_to_merge_secondary_memmap() {
                info!("About to write secondary mmap to disk");
                self.use_primary_map.store(true, Ordering::SeqCst);
                // let primary_map = self.primary_memory_map.write();
                let new_run =
                    Run::new_from_skipmap(self.secondary_memory_map.clone(), &self.config);
                if new_run.is_err() {
                    error!("Error creating run from memory map: {:?}", new_run.err());
                    continue;
                } else {
                    self.add_run_to_level(new_run.unwrap(), 1);
                    self.secondary_memory_map_memory_use
                        .store(0, Ordering::SeqCst);
                }
            }

            for i in 0..self.levels.read().len() {
                if self.levels.read()[i].read().runs.len() >= self.config.t as usize {
                    info!("Compacting runs at level {}", i);
                    let new_run = Run::new_from_merge(
                        &self.levels.read()[i].read().runs,
                        &self.config,
                        i + 1,
                    )
                    .unwrap();
                    let old_runs = self.levels.read()[i].read().runs.clone();
                    {
                        let levels = self.levels.read();
                        let mut this_level = levels[i].write();
                        this_level.num_runs = 0;
                        this_level.runs.clear();
                        let mut next_level = levels[i + 1].write();
                        next_level.runs.push(Arc::new(new_run));
                        next_level.num_runs += 1;
                    }
                    for run in old_runs {
                        info!("Cleaning up runs after merging");
                        Arc::try_unwrap(run).ok().unwrap().delete().unwrap();
                    }
                }
            }
        }
    }
}

impl Drop for Lsm {
    fn drop(&mut self) {
        info!("Shutting down run manager");
        self.time_to_shutdown.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod test_run {
    use crate::run::Run;
    use crate::Config;
    use crossbeam_skiplist::SkipMap;
    use log::info;
    use rand::Rng;
    use rand_chacha::rand_core::SeedableRng;
    use rand_chacha::ChaChaRng;
    use std::fs;
    use std::thread::sleep;
    use tempfile::tempdir;
    // use test_case::test_case;
    use crate::lsm::Lsm;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;
    use test_env_log::test;

    #[test]
    fn lsm_small_one_disk_run() {
        // env_logger::init();
        info!("Running small_lsm");
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_memory_map_budget(1000).unwrap();
        config.set_directory(dir.path());
        let lsm = Lsm::new(Some(config));
        lsm.put(42, vec![042u8]);
        insert_vals(lsm.clone(), 1500);
        assert_eq!(lsm.get(&42).unwrap(), vec![042u8]);

        sleep(Duration::new(5, 0));

        let db_files = fs::read_dir(dir.path()).unwrap();

        info!("DB files created in temp directory");
        let mut num_files = 0;
        for path in db_files {
            info!("Name: {}", path.unwrap().path().display());
            num_files = num_files + 1;
        }
        assert_eq!(num_files, 1);
    }

    #[test]
    fn test_lsm_delete() {
        env_logger::try_init();
        info!("Running small_lsm");
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_memory_map_budget(1000).unwrap();
        config.set_directory(dir.path());
        let lsm = Lsm::new(Some(config));
        lsm.put(42, vec![042u8]);
        insert_vals(lsm.clone(), 1500);
        assert_eq!(lsm.get(&42).unwrap(), vec![042u8]);
        for i in 0..10 {
            insert_vals(lsm.clone(), 1100);
            sleep(Duration::new(3, 0));
        }
        lsm.delete(&42);

        assert_eq!(lsm.get(&42), None)
    }

    #[test]
    fn lsm_small_ten_disk_run10() {
        // env_logger::init();
        info!("Running small_lsm");
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_memory_map_budget(1000).unwrap();
        config.set_directory(dir.path());
        let lsm = Lsm::new(Some(config));
        lsm.put(42, vec![042u8]);
        for i in 0..10 {
            insert_vals(lsm.clone(), 1100);
            sleep(Duration::new(3, 0));
        }
        lsm.put(41, vec![041u8]);
        assert_eq!(lsm.get(&42).unwrap(), vec![042u8]);
        assert_eq!(lsm.get(&41).unwrap(), vec![041u8]);

        sleep(Duration::new(2, 0));

        let val = lsm.get(&42);
        assert_eq!(val, Some(vec![042u8]));
        let val = lsm.get(&41);
        assert_eq!(val, Some(vec![041u8]));

        let db_files = fs::read_dir(dir.path()).unwrap();

        info!("DB files created in temp directory");
        let mut num_files = 0;
        for path in db_files {
            info!("Name: {}", path.unwrap().path().display());
            num_files = num_files + 1;
        }
        assert_eq!(num_files, 10);
    }

    fn insert_vals(map: Arc<Lsm>, size: u64) {
        let mut curr_size: u64 = 0;
        let seed = [42; 32];
        let mut rng = ChaChaRng::from_seed(seed);
        let mut num_gen_items = 0;

        while curr_size < size {
            let rand_key: i32 = rng.gen_range(-50000..50000);
            let rand_val = gen_rand_bytes(&mut rng);
            curr_size += 4 + rand_val.len() as u64;
            map.put(rand_key, rand_val);
            num_gen_items += 1;
        }
        info!("Generated {} key/value pairs", num_gen_items);
    }

    // generates a random number of random bytes
    fn gen_rand_bytes<T: Rng>(mut rng: &mut T) -> Vec<u8> {
        let num_bytes = rng.gen_range(1..32);
        let mut ret_vec = vec![0u8; num_bytes];
        rng.fill_bytes(&mut ret_vec);
        return ret_vec;
    }
}
