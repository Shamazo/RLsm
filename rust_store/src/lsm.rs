use crate::run::{Level, Run, RunError};
use serde::{Deserialize, Serialize};

// use crate::run_manager::run_manager;
use crate::rust_store;
use crate::rust_store::Config;
use crossbeam_skiplist::SkipMap;
use log::{debug, error, info, warn};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

pub struct Lsm {
    #[allow(dead_code)]
    primary_memory_map: Arc<SkipMap<i32, Vec<u8>>>,
    #[allow(dead_code)]
    secondary_memory_map: Arc<SkipMap<i32, Vec<u8>>>,
    #[allow(dead_code)]
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
        let config_to_use = match config {
            Some(config) => config,
            None => Config::default(),
        };

        if config_to_use.directory.is_some() {
            //TODO check and load from disk
            // unimplemented!();
        }

        let mut lsm = Arc::new(Lsm {
            primary_memory_map: Arc::new(SkipMap::new()),
            secondary_memory_map: Arc::new(SkipMap::new()),
            use_primary_map: Default::default(),
            primary_memory_map_memory_use: AtomicU64::new(0),
            secondary_memory_map_memory_use: AtomicU64::new(0),
            config: config_to_use,
            levels: RwLock::new(vec![]),
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
                // let primary_map = self.primary_memory_map.read();
                let maybe_res = self.primary_memory_map.get(key);
                if maybe_res.is_some() {
                    return Some(maybe_res.unwrap().value().clone());
                }
            }
            {
                // let secondary_map = self.secondary_memory_map.read();
                let maybe_res = self.secondary_memory_map.get(key);
                if maybe_res.is_some() {
                    return Some(maybe_res.unwrap().value().clone());
                }
            }
        } else {
            {
                // let secondary_map = self.secondary_memory_map.read();
                let maybe_res = self.secondary_memory_map.get(key);
                if maybe_res.is_some() {
                    return Some(maybe_res.unwrap().value().clone());
                }
            }
            {
                // let primary_map = self.primary_memory_map.read();
                let maybe_res = self.primary_memory_map.get(key);
                if maybe_res.is_some() {
                    return Some(maybe_res.unwrap().value().clone());
                }
            }
        }
        let read_levels = self.levels.read();
        for i in 0..read_levels.len() {
            let level = read_levels[i].read();
            let maybe_res = level.get_from_level(key);
            if maybe_res.is_some() {
                return maybe_res;
            }
        }
        return None;
    }
    #[allow(dead_code)]
    pub fn put(self: &Self, key: i32, val: Vec<u8>) -> Result<(), LsmError> {
        if self.use_primary_map.load(Ordering::SeqCst) {
            // let m = self.primary_memory_map.read();
            let memuse =
                self.primary_memory_map_memory_use.load(Ordering::SeqCst) + 4 + val.len() as u64;
            self.primary_memory_map.insert(key, val);
            self.primary_memory_map_memory_use
                .store(memuse, Ordering::SeqCst);
        } else {
            // let m = self.secondary_memory_map.read();
            let memuse =
                self.secondary_memory_map_memory_use.load(Ordering::SeqCst) + 4 + val.len() as u64;
            self.secondary_memory_map.insert(key, val);
            self.secondary_memory_map_memory_use
                .store(memuse, Ordering::SeqCst);
        }
        return Ok(());
    }

    fn time_to_merge_primary_memmap(self: &Self) -> bool {
        let primary_mem_use = self.primary_memory_map_memory_use.load(Ordering::Relaxed);
        debug!(
            "time_to_merge primary: primary mem use: {} primary mem budget {}",
            primary_mem_use, self.config.memory_map_budget
        );
        // if self.use_primary_map.load(Ordering::SeqCst) == false {
        if primary_mem_use > self.config.memory_map_budget {
            return true;
        }
        // }
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

    pub fn add_run_to_level(self: &Self, run: Run) {
        if self.levels.read().len() >= 1 {
            //scope for write lock on levels.
            let mut levels = self.levels.write();
            let mut level = &mut *levels[0].write();
            level.runs.push(run);
            level.num_runs += 1;
        } else {
            let mut levels = self.levels.write();
            levels.push(RwLock::new(Level {
                num_runs: 1,
                runs: vec![run],
            }))
        }
    }

    pub fn run_manager(self: &Self) {
        info!("Starting up run_manager");
        loop {
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
                    self.add_run_to_level(new_run.unwrap());
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
                    self.add_run_to_level(new_run.unwrap());
                    self.secondary_memory_map_memory_use
                        .store(0, Ordering::SeqCst);
                }
            }
        }
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
    // use test_env_log::test;
    use crate::lsm::Lsm;
    use std::sync::Arc;
    use tokio::time::Duration;

    #[test]
    fn lsm_small_one_disk_run() {
        env_logger::init();
        info!("Running small_lsm");
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_memory_map_budget(1000).unwrap();
        config.set_directory(dir.path());
        let lsm = Lsm::new(Some(config));
        lsm.put(42, vec![042u8]).unwrap();
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
    fn lsm_small_ten_disk_run10() {
        env_logger::init();
        info!("Running small_lsm");
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_memory_map_budget(1000).unwrap();
        config.set_directory(dir.path());
        let lsm = Lsm::new(Some(config));
        lsm.put(42, vec![042u8]).unwrap();
        for i in 0..10 {
            insert_vals(lsm.clone(), 1100);
            sleep(Duration::new(2, 0));
        }
        lsm.put(41, vec![041u8]).unwrap();
        assert_eq!(lsm.get(&42).unwrap(), vec![042u8]);
        assert_eq!(lsm.get(&41).unwrap(), vec![041u8]);

        sleep(Duration::new(2, 0));

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
            map.put(rand_key, rand_val).unwrap();
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

    // #[test_case( 512 ; "0.125 KB")]
    // #[test_case(1024 ; "0.25 KB")]
    // #[test_case(2048 ; "0.5 KB")]
    // #[test_case(4096 ; "1 KB")]
    // #[test_case(8192 ; "2 KB")]
    // #[test_case(16384 ; "4 KB")]
    // #[test_case(32768 ; "8 KB")]
    // #[test_case(65536 ; "16 KB")]
    // #[test_case(131072 ; "32 KB")]
    // fn construct_1MB_run_varying_block_size(page_size: u64) {
    //     // env_logger::init();
    //     info!("Running construct_large_run_random_vals");
    //     let mut config = Config::default();
    //     let dir = tempdir().unwrap();
    //     config.set_directory(dir.path());
    //     let map = create_skipmap(4 * 1024 * 1024);
    //     let run = Run::new_from_skipmap(map, &config).unwrap();
    //
    //     run.delete().unwrap();
    // }
}
