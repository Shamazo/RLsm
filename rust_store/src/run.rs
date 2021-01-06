// use crate::bloom_filter::bloom_filter::BloomFilter;
use crate::fence_pointer::fence_pointer::FencePointer;
use bincode::Serializer;
use crossbeam_skiplist::SkipMap;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
// use anyhow::Result;
use crate::bloom_filter::BloomFilter;
use crate::rust_store;
use bincode::Options;
use flate2::read::DeflateDecoder;
use log::{debug, error, info, warn};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::fs::{remove_file, File};
use std::hash::Hash;
use std::io::prelude::*;
use std::io::{BufWriter, Cursor, SeekFrom};
use std::io::{Read, Write};
use std::path::Component::CurDir;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{io, mem};
use thiserror::Error;
// use tokio::io::AsyncWriteExt;
// use tokio::io::AsyncWriteExt;

#[derive(Error, Debug)]
pub enum RunError {
    #[error("IO Error")]
    IoError(#[from] io::Error),
    #[error("Error Deserializing")]
    DeserializeError(#[from] bincode::Error),
    #[error("Creating run from memory map")]
    RunCreationError,
    #[error("Not yet implemented")]
    NotImplemented,
}

pub struct Level {
    pub num_runs: usize, // doesn't need to be atomic because Levels are wrapped in RwLocks
    pub runs: Vec<Run>,
}

impl Level {
    pub fn read_from_disk() -> Level {
        unimplemented!();
    }

    pub fn get_from_level(&self, key: &i32) -> Option<Vec<u8>> {
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
struct Item<K: Ord + Copy> {
    key: K,
    value: Vec<u8>,
}
impl<K: Ord + Copy> Item<K> {
    pub fn new(key: K, value: Vec<u8>) -> Item<K> {
        return Item {
            key: key,
            value: value,
        };
    }
}

//this struct lives at the end of the on disk file
// ie in the last sizeof(RunError) bytes
// K -> key type
#[derive(Serialize, Deserialize)]
pub struct Run {
    pub num_pages: usize,
    pub level: usize,
    bloom_filter: BloomFilter,
    fence_pointers: Vec<FencePointer<i32>>,
    pub file_name: PathBuf,
    block_size: u64,
}

impl Run {
    // given a (full) SkipMap construct a run from it
    // on disk each run has metadata, and two files. One of values and one of keys.
    #[allow(dead_code)]
    pub fn new_from_skipmap(
        memory_map: Arc<SkipMap<i32, Vec<u8>>>,
        config: &rust_store::Config,
    ) -> Result<Run, RunError> {
        if memory_map.len() == 0 {
            return Err(RunError::RunCreationError);
        }
        let fpr = 0.1; // TODO calculate the intended fpr using level
        info!(
            "Creating a new run from a memory map with fpr {} with {} elements",
            fpr,
            memory_map.len()
        );
        let mut filter = BloomFilter::new_with_rate(fpr, memory_map.len());
        let mut fence_pointers = vec![];

        let mut min_val: i32 = 0;
        let mut max_val: i32 = 0;

        let now = SystemTime::now();
        let epoch_time = now
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();
        let file_name = format!("1_{}.run", epoch_time);
        let mut path = match &config.directory {
            None => "".parse().unwrap(),
            Some(pb) => pb.clone(),
        };
        path.push(file_name);
        let file = File::create(&path)?;
        info!("Run file name {}", &file.metadata().unwrap().len());
        let mut writer = BufWriter::new(file);

        // This is an index into a page
        let mut idx: u64 = 0;
        let mut num_pages: usize = 1;

        // data goes from memory map -> serialization -> compressor -> file

        // We use the default options because we want varint encoding on our lengths
        let bincode_opts = bincode::DefaultOptions::new();
        let mut encoder = DeflateEncoder::new(writer, Compression::default());
        // we compress each page individually

        for x in memory_map.iter() {
            encoder.flush()?;
            idx = encoder.total_out();
            // would prefer to move the value as we consume the memorymap, but to benchmark it I need to pass a reference.
            let item: Item<i32> = Item::new(x.key().clone(), x.value().clone());
            filter.insert(&item.key);
            // we use ser_length because I don't know how to get compressed length
            let ser_length = bincode_opts.serialized_size(&item)?;
            // finish out the page
            if idx + ser_length >= config.block_size - 20 {
                // finish out the encoder for each page so each page can be independently decoded.
                let mut writer = encoder.finish()?;
                // The wirter writes the bytes 0x3 and 0x0 to mark the end of the stream
                idx += 2;
                if idx > config.block_size - 1 {
                    error!("Wrote more than a page of bytes {}", idx);
                    debug_assert!(idx <= config.block_size);
                }

                // pad out the rest of the page
                let zeros = vec![4u8; (config.block_size - idx) as usize];
                let padding_byte_len = writer.write(&zeros)? as u64;

                debug!(
                    "Writing page with {} bytes with {} bytes of padding. with min {:?} max {:?}",
                    idx,
                    4096 - idx,
                    &min_val,
                    &max_val
                );
                idx = idx + padding_byte_len;

                if idx != 4096 {
                    warn!("idx is not page size {}", idx);
                    debug_assert!(idx == 4096);
                }
                // open ip a new encoder for the next page
                writer.flush()?;
                encoder = DeflateEncoder::new(writer, Compression::default());
                idx = 0;
                fence_pointers.push(FencePointer::new(min_val, max_val));

                // start the next page.
                // if there is a bug related to min/max values if there is one value on a page, this is why
                num_pages += 1;
                min_val = item.key;
                bincode_opts.serialize_into(&mut encoder, &item)?;
            } else {
                bincode_opts.serialize_into(&mut encoder, &item)?;
                max_val = item.key;
            }
            if !(idx < config.block_size as u64) {
                error!(
                    "idx too big! idx: {}, encoder total: {} ",
                    idx,
                    encoder.total_out()
                );
                debug_assert!(idx < 4096);
            }
        }

        //close out the last page
        // finish the page
        let mut writer = encoder.finish()?;
        idx += 2;
        let zeros = vec![4u8; (config.block_size - idx) as usize];
        let padding_byte_len = writer.write(&zeros)? as u64;
        debug!(
            "Writing page with {} bytes with {} bytes of padding. with min {:?} max {:?}",
            idx,
            4096 - idx,
            min_val,
            max_val
        );
        idx = idx + padding_byte_len;
        debug_assert!(idx == config.block_size as u64);
        fence_pointers.push(FencePointer::new(min_val, max_val));

        // The metadata struct is not compressed
        if fence_pointers.len() != num_pages {
            error!(
                "num fence pointers ({}) !- num pages ({})",
                fence_pointers.len(),
                num_pages
            );
            debug_assert!(fence_pointers.len() == num_pages);
        }
        info!("Creating run {:?} metadata with {} pages", path, num_pages);
        let run = Run {
            num_pages: fence_pointers.len(),
            level: 1,
            bloom_filter: filter,
            fence_pointers: fence_pointers,
            file_name: path.clone(),
            block_size: config.block_size,
        };

        let ser_meta = bincode::serialize(&run)?;
        let ser_meta_length = writer.write(&ser_meta)?;
        let ser_ser_meta_length = bincode::serialize(&ser_meta_length)?;
        writer.write(&ser_ser_meta_length)?;
        writer.flush()?;
        return Ok(run);
    }

    pub async fn load(path: String) -> Result<Run, RunError> {
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
    pub fn new_from_merge(left: Run, right: Run) -> Result<Run, RunError> {
        return Err(RunError::NotImplemented);
    }

    // Returns value if it exists in the run
    #[allow(dead_code)]
    pub fn get_from_run(self: &Self, key: &i32) -> Option<Vec<u8>> {
        if !self.bloom_filter.contains(key) {
            debug!("key {:?} not in bloom filter", key);
            return None;
        }

        let page_idx = self.get_page_index(key);
        if page_idx.is_none() {
            debug! {"key {:?} not in fence pointers", key};
            return None;
        }

        let maybe_val = self.get_from_page(page_idx.unwrap(), key);
        return maybe_val.unwrap();
        // return match maybe_val {
        //     Ok(None) => None,
        //     Ok(Some(val)) => Some(val),
        //     Err(RunError::DeserializeError(_)) => {
        //         // need to figure out handling of deserializing a page and what happens when we run out of data.
        //         warn!("Failed to get val from run Deserialize error");
        //         None
        //     }
        //     Err(E) => {
        //         warn!("Failed to get val from run {}", E);
        //         None
        //     }
        // };
    }

    fn get_from_page(self: &Self, page: u64, key: &i32) -> Result<Option<Vec<u8>>, RunError> {
        debug!("get_from_page: opening file {:?}", &self.file_name);
        let mut f = File::open(&self.file_name)?;
        let _ = f.seek(SeekFrom::Start(page * self.block_size))?;
        let mut page_buf = vec![0u8; self.block_size as usize];
        let _ = f.read_exact(&mut page_buf)?;

        let bincode_opts = bincode::DefaultOptions::new();
        bincode_opts.allow_trailing_bytes();

        let mut deflater = DeflateDecoder::new(Cursor::new(&page_buf));

        let mut decompressed: Vec<u8> = Vec::new();
        let decompressed_size = deflater.read_to_end(&mut decompressed)?;
        debug!(
            "Decompressed page {} into {} bytes",
            page, decompressed_size
        );
        let mut decompressed_cursor = Cursor::new(decompressed);

        loop {
            // let item_length: u64 = bincode_opts.deserialize(&mut decompress_bytes)?;
            let item: Item<i32> = match bincode_opts.deserialize_from(&mut decompressed_cursor) {
                Ok(x) => x,
                Err(E) => return Err(RunError::DeserializeError(E)),
            };
            if item.key == *key {
                return Ok(Some(item.value));
            }
        }

        // return Ok(None);
    }

    /// Given a key, find which page the value is on
    fn get_page_index(self: &Self, key: &i32) -> Option<u64> {
        let mut idx = 0;
        for fp in &self.fence_pointers {
            if fp.in_range(key) {
                return Some(idx);
            }
            idx += 1;
        }
        return None;
    }

    /// consume the run object and delete the file
    pub fn delete(self: Self) -> Result<(), RunError> {
        remove_file(self.file_name)?;
        return Ok(());
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
    use std::thread::sleep;
    use tempfile::tempdir;
    use test_case::test_case;
    // use test_env_log::test;
    use std::sync::Arc;
    use tokio::time::Duration;

    #[test]
    fn small_run_from_memory_map() {
        // env_logger::init();
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_directory(dir.path());
        info!("Running small_run_from_memory_map");
        let map = create_skipmap(1000);
        let run = Run::new_from_skipmap(map, &config).unwrap();
        info!("Num pages {}", run.num_pages);
        run.delete().unwrap();
    }

    #[test]
    fn small_run_get_page_index() {
        // env_logger::init();
        sleep(Duration::new(0, 2000000));
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_directory(dir.path());
        info!("Running small_run_from_memory_map");
        let map = create_skipmap(1000);
        for i in 0..500 {
            map.insert(i, vec![0u8, 20u8, 3u8]);
        }
        let run = Run::new_from_skipmap(map, &config).unwrap();
        let page_index = run.get_page_index(&0).unwrap();
        assert_eq!(page_index, 0);
        let page_index_last = run.get_page_index(&499).unwrap();
        assert_eq!(page_index_last as usize, run.fence_pointers.len() - 1);
        run.delete().unwrap();
    }

    #[test]
    fn small_run_get() {
        // env_logger::init();
        info!("Running small_run_get");
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_directory(dir.path());
        let map = create_skipmap(1000);
        for i in 0..250 {
            map.insert(i, vec![i as u8]);
        }
        let run = Run::new_from_skipmap(map, &config).unwrap();
        let val = run.get_from_run(&91);
        for i in 0..250 {
            let val = run.get_from_run(&i);
            assert_eq!(val.unwrap()[0], i as u8);
        }
        run.delete().unwrap();
    }

    #[test_case( 512 ; "0.125 KB")]
    #[test_case(1024 ; "0.25 KB")]
    #[test_case(2048 ; "0.5 KB")]
    #[test_case(4096 ; "1 KB")]
    #[test_case(8192 ; "2 KB")]
    #[test_case(16384 ; "4 KB")]
    #[test_case(32768 ; "8 KB")]
    #[test_case(65536 ; "16 KB")]
    #[test_case(131072 ; "32 KB")]
    fn construct_1MB_run_varying_block_size(page_size: u64) {
        // env_logger::init();
        info!("Running construct_large_run_random_vals");
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_directory(dir.path());
        let map = create_skipmap(4 * 1024 * 1024);
        let run = Run::new_from_skipmap(map, &config).unwrap();

        run.delete().unwrap();
    }

    // generates a random number of random bytes
    fn gen_rand_bytes<T: Rng>(mut rng: &mut T) -> Vec<u8> {
        let num_bytes = rng.gen_range(1..256);
        let mut ret_vec = vec![0u8; num_bytes];
        rng.fill_bytes(&mut ret_vec);
        return ret_vec;
    }

    // Size in bytes approx
    fn create_skipmap(size: u64) -> Arc<SkipMap<i32, Vec<u8>>> {
        let mut curr_size: u64 = 0;
        let map = Arc::new(SkipMap::new());
        let seed = [42; 32];
        let mut rng = ChaChaRng::from_seed(seed);

        while curr_size < size {
            let rand_key: i32 = rng.gen_range(-50000..50000);
            let rand_val = gen_rand_bytes(&mut rng);
            curr_size += 4 + rand_val.len() as u64;
            map.insert(rand_key, rand_val);
        }
        return map;
    }
}
