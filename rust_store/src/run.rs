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

pub struct Level<K: Ord + Serialize + DeserializeOwned> {
    pub num_runs: usize,
    pub runs: Vec<Run<K>>,
}

impl<K: Ord + Serialize + DeserializeOwned + Copy + Hash + Debug> Level<K> {
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
impl<K: Ord> Item<K> {
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
pub struct Run<K: Ord> {
    pub num_pages: usize,
    pub level: usize,
    bloom_filter: BloomFilter,
    fence_pointers: Vec<FencePointer<K>>,
    pub file_name: PathBuf,
}

impl<K: Ord + Serialize + DeserializeOwned + Copy + Hash + Debug> Run<K> {
    // given a (full) SkipMap construct a run from it
    // on disk each run has metadata, and two files. One of values and one of keys.
    #[allow(dead_code)]
    pub fn new(
        memory_map: SkipMap<K, Vec<u8>>,
        config: &rust_store::Config,
    ) -> Result<Run<K>, RunError> {
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

        let mut min_val: K = *memory_map.front().unwrap().key(); // min val on the page
        let mut max_val: K = *memory_map.back().unwrap().key(); // max val on the page

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

        let mut prev_enc_out = 0;

        for x in memory_map {
            encoder.flush()?;
            idx = encoder.total_out();
            let item = Item::new(x.0, x.1);
            filter.insert(&item.key);
            // we use ser_length because I don't know how to get compressed length
            let ser_length = bincode_opts.serialized_size(&item)?;
            // finish out the page
            if (idx + ser_length >= 4092) {
                // finish out the encoder for each page so each page can be indendently decoded.
                let mut writer = encoder.finish()?;
                // The wirter writes the bytes 0x3 and 0x0 to mark the end of the stream
                idx += 2;
                if idx > 4095 {
                    error!("Wrote more than a page of bytes {}", idx);
                    debug_assert!(idx <= 4095);
                }

                // pad out the rest of the page
                let zeros = vec![4u8; (4096 - idx) as usize];
                let padding_byte_len = writer.write(&zeros)? as u64;

                debug!(
                    "Writing page with {} bytes with {} bytes of padding. with min {:?} max {:?}",
                    idx,
                    4096 - idx,
                    min_val,
                    max_val
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
            if !(idx < 4096) {
                error!(
                    "idx too big! idx: {}, prev_enc_out: {} encoder total: {} ",
                    idx,
                    prev_enc_out,
                    encoder.total_out()
                );
                debug_assert!(idx < 4096);
            }
        }

        //close out the last page
        // finish the page
        let mut writer = encoder.finish()?;
        idx += 2;
        let zeros = vec![0u8; (4096 - idx) as usize];
        let padding_byte_len = writer.write(&zeros)? as u64;
        debug!(
            "Writing page with {} bytes with {} bytes of padding. with min {:?} max {:?}",
            idx,
            4096 - idx,
            min_val,
            max_val
        );
        idx = idx + padding_byte_len;
        debug_assert!(idx == 4096);
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
        };

        let ser_meta = bincode::serialize(&run)?;
        let ser_meta_length = writer.write(&ser_meta)?;
        let ser_ser_meta_length = bincode::serialize(&ser_meta_length)?;
        writer.write(&ser_ser_meta_length)?;
        writer.flush()?;
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

    fn get_from_page(self: &Self, page: usize, key: &K) -> Result<Option<Vec<u8>>, RunError> {
        debug!("get_from_page: opening file {:?}", &self.file_name);
        let mut f = File::open(&self.file_name)?;
        let _ = f.seek(SeekFrom::Start((page * 4096) as u64))?;
        let mut page_buf = vec![0u8; 4096];
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

        // let mut decompress_bytes = decompressed.as_bytes();
        // let mut idx: u64 = 0;
        loop {
            // let item_length: u64 = bincode_opts.deserialize(&mut decompress_bytes)?;
            let item: Item<K> = bincode_opts.deserialize_from(&mut decompressed_cursor)?;
            if item.key == *key {
                return Ok(Some(item.value));
            }
            // do I need to know the length of the bytes I am deserializing.
            // deser_item = bincode::deserialize(&page_buf)?;
        }

        // let meta_size = f.read_u64().await?;
        //
        // let mut meta_buf = Box::new(vec![0u8; meta_size as usize]);
        // f.read_exact(&mut **meta_buf);
        // let mut deserializer = Deserializer::from_slice(&meta_buf);
        // let run = serde::de::Deserialize::deserialize(&mut deserializer)?;
        return Ok(None);
    }

    /// Given a key, find which page the value is on
    fn get_page_index(self: &Self, key: &K) -> Option<usize> {
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
    use crate::memory_map::Map;
    use crate::run::Run;
    use crate::Config;
    use crossbeam_skiplist::SkipMap;
    use log::info;
    use std::thread::sleep;
    use test_env_log::test;
    use tokio::time::Duration;
    // TODO a better temporary file solution.

    #[test]
    fn small_run_from_memory_map() {
        // env_logger::init();
        sleep(Duration::new(0, 2000000));
        let mut config = Config::default();
        config.set_directory("/tmp");
        info!("Running small_run_from_memory_map");
        let map = SkipMap::new();
        for i in 0..500 {
            map.insert(i, vec![0u8, 20u8, 3u8]);
        }
        let run = Run::new(map, &config).unwrap();
        info!("Num pages {}", run.num_pages);
        run.delete().unwrap();
    }

    #[test]
    fn small_run_get_page_index() {
        // env_logger::init();
        sleep(Duration::new(0, 2000000));
        let mut config = Config::default();
        config.set_directory("/tmp");
        info!("Running small_run_from_memory_map");
        let map = SkipMap::new();
        for i in 0..500 {
            map.insert(i, vec![0u8, 20u8, 3u8]);
        }
        let run = Run::new(map, &config).unwrap();
        let page_index = run.get_page_index(&0).unwrap();
        assert_eq!(page_index, 0);
        let page_index_last = run.get_page_index(&499).unwrap();
        assert_eq!(page_index_last, run.fence_pointers.len() - 1);
        run.delete().unwrap();
    }

    #[test]
    fn small_run_get() {
        sleep(Duration::new(0, 2000000));
        // env_logger::init();
        info!("Running small_run_get");
        let mut config = Config::default();
        config.set_directory("/tmp");
        let map = SkipMap::new();
        for i in 0..500 {
            map.insert(i, vec![0u8, 20u8, 3u8]);
        }
        let run = Run::new(map, &config).unwrap();
        let val = run.get_from_run(&91);
        assert_eq!(val.unwrap(), vec![0u8, 20u8, 3u8]);
        run.delete().unwrap();
    }

    #[test]
    fn larger_run_from_memory_map() {
        info!("Running larger_run_from_memory_map");
        let mut config = Config::default();
        config.set_directory("/tmp");
        let map = SkipMap::new();
        for i in 0..50000 {
            map.insert(i, vec![0u8, 20u8, 3u8]);
        }
        let run = Run::new(map, &config).unwrap();
        let page_index = run.get_page_index(&0).unwrap();
        assert_eq!(page_index, 0);
        let page_index_last = run.get_page_index(&(50000 - 1)).unwrap();
        assert_eq!(page_index_last, run.fence_pointers.len() - 1);
        let val = run.get_from_run(&1000);
        assert_eq!(val.unwrap(), vec![0u8, 20u8, 3u8]);
        let val = run.get_from_run(&40000);
        assert_eq!(val.unwrap(), vec![0u8, 20u8, 3u8]);
        let val = run.get_from_run(&23000);
        assert_eq!(val.unwrap(), vec![0u8, 20u8, 3u8]);
        run.delete().unwrap();
    }

    // #[test]
    // fn fp_u64() {
    //     let lower: u64 = 1;
    //     let upper: u64 = 5;
    //     let fp = FencePointer::new(lower, upper);
    //     assert!(fp.in_range(&2));
    //     assert!(fp.in_range(&1));
    //     assert!(fp.in_range(&5));
    //     assert!(!fp.in_range(&7));
    // }
    //
    // #[test]
    // fn fp_can_serialize_deserialize() {
    //     let lower: u64 = 1;
    //     let upper: u64 = 5;
    //     let fp = FencePointer::new(lower, upper);
    //     assert!(fp.in_range(&2));
    //     assert!(fp.in_range(&1));
    //     assert!(fp.in_range(&5));
    //     assert!(!fp.in_range(&7));
    //
    //     let s = serde_json::to_string(&fp).unwrap();
    //     let serde_fp: FencePointer<u64> = serde_json::from_str(&s).unwrap();
    //     assert!(serde_fp.in_range(&2));
    //     assert!(serde_fp.in_range(&1));
    //     assert!(serde_fp.in_range(&5));
    //     assert!(!serde_fp.in_range(&7));
    // }
}
