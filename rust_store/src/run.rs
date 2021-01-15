// use crate::bloom_filter::bloom_filter::BloomFilter;
use crate::fence_pointer::fence_pointer::FencePointer;
use bincode::{ErrorKind, Serializer};
use crossbeam_skiplist::SkipMap;
use flate2::write::DeflateEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
// use anyhow::Result;
use crate::bloom_filter::BloomFilter;
use crate::rust_store;
use bincode::Options;
use flate2::read::DeflateDecoder;
use itertools::EitherOrBoth::{Both, Left, Right};
use itertools::Itertools;
use log::{debug, error, info, trace, warn};
use std::fmt::Debug;
use std::fs::{remove_file, File};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, Cursor, SeekFrom};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

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
    pub runs: Vec<Arc<Run>>,
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
pub struct Item<K: Ord + Copy> {
    key: K,
    value: Option<Vec<u8>>,
}

impl<K: Ord + Copy> Item<K> {
    pub fn new(key: K, value: Option<Vec<u8>>) -> Item<K> {
        return Item {
            key: key,
            value: value,
        };
    }

    pub fn key(self) -> K {
        return self.key;
    }

    pub fn value(self) -> Option<Vec<u8>> {
        return self.value;
    }
}

//this struct lives at the end of the on disk file
// the last 8 bytes are a u64 describing the length of the serialized representation
#[derive(Serialize, Deserialize)]
pub struct Run {
    pub num_blocks: usize,
    pub level: usize,
    bloom_filter: BloomFilter,
    fence_pointers: Vec<FencePointer<i32>>,
    pub file_name: PathBuf,
    block_size: u64,
    size_in_bytes: usize,
    num_elements: usize,
}

impl Run {
    /// Get disk usage in bytes
    pub fn size_in_bytes(self: &Self) -> usize {
        return self.size_in_bytes;
    }

    pub fn get_memory_use_in_bytes(self: &Self) -> usize {
        return self.bloom_filter.size_in_bytes()
            + self.num_blocks * self.fence_pointers[0].size_in_bytes();
    }

    // given a (full) SkipMap construct a run from it
    // on disk each run has metadata, and two files. One of values and one of keys.
    // this can be refactored to use run_from_iterator once I implement my own Skiplist, or
    // wrap the cross bream skiplist to return Items
    pub fn new_from_skipmap(
        memory_map: Arc<SkipMap<i32, Option<Vec<u8>>>>,
        config: &rust_store::Config,
    ) -> Result<Run, RunError> {
        let num_elements = memory_map.len();
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
        let writer = BufWriter::new(file);

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
                    config.block_size - idx,
                    &min_val,
                    &max_val
                );
                idx = idx + padding_byte_len;

                if idx != config.block_size {
                    warn!("idx is not page size {}", idx);
                    debug_assert!(idx == config.block_size);
                }
                // open up a new encoder for the next page
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
                debug_assert!(idx < config.block_size);
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
            config.block_size - idx,
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

        let run_bytes = fence_pointers.len() * config.block_size as usize
            + filter.size_in_bytes()
            + fence_pointers.len() * fence_pointers[0].size_in_bytes();

        info!(
            "Creating run {:?} metadata with {} pages total size in bytes {}",
            path, num_pages, run_bytes
        );
        let run = Run {
            num_blocks: fence_pointers.len(),
            level: 1,
            bloom_filter: filter,
            fence_pointers: fence_pointers,
            file_name: path.clone(),
            block_size: config.block_size,
            size_in_bytes: run_bytes,
            num_elements: num_elements,
        };

        let ser_meta = bincode::serialize(&run)?;
        let ser_meta_length = writer.write(&ser_meta)?;
        let ser_ser_meta_length = bincode::serialize(&ser_meta_length)?;
        writer.write(&ser_ser_meta_length)?;
        writer.flush()?;
        return Ok(run);
    }

    /// given an iterator that produces Items create a new run at the designated level
    fn run_from_iterator<K>(
        it: K,
        config: &rust_store::Config,
        level: usize,
        num_elements: usize,
    ) -> Result<Run, RunError>
    where
        K: Iterator<Item = Item<i32>>,
    {
        let fpr = 0.1; // TODO calculate the intended fpr using level

        let mut filter = BloomFilter::new_with_rate(fpr, num_elements);
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
        let writer = BufWriter::new(file);

        // This is an index into a page
        let mut idx: u64 = 0;
        let mut num_pages: usize = 1;

        // data goes from memory map -> serialization -> compressor -> file

        // We use the default options because we want varint encoding on our lengths
        let bincode_opts = bincode::DefaultOptions::new();
        let mut encoder = DeflateEncoder::new(writer, Compression::default());
        // we compress each page individually

        for item in it {
            encoder.flush()?;
            idx = encoder.total_out();
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
                    config.block_size - idx,
                    &min_val,
                    &max_val
                );
                idx = idx + padding_byte_len;

                if idx != config.block_size {
                    warn!("idx is not page size {}", idx);
                    debug_assert!(idx == config.block_size);
                }
                // open up a new encoder for the next page
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
                debug_assert!(idx < config.block_size);
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
            config.block_size - idx,
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

        let run_bytes = fence_pointers.len() * config.block_size as usize
            + filter.size_in_bytes()
            + fence_pointers.len() * fence_pointers[0].size_in_bytes();

        info!(
            "Creating run {:?} metadata with {} pages total size in bytes {}",
            path, num_pages, run_bytes
        );
        let run = Run {
            num_blocks: fence_pointers.len(),
            level: level,
            bloom_filter: filter,
            fence_pointers: fence_pointers,
            file_name: path.clone(),
            block_size: config.block_size,
            size_in_bytes: run_bytes,
            num_elements: num_elements,
        };

        let ser_meta = bincode::serialize(&run)?;
        let ser_meta_length = writer.write(&ser_meta)?;
        let ser_ser_meta_length = bincode::serialize(&ser_meta_length)?;
        writer.write(&ser_ser_meta_length)?;
        writer.flush()?;
        return Ok(run);
    }

    /// Given two runs, return an iterator over their merged items
    /// If keys exist in both left and right, then the key is disgarded from the right iterator
    /// i.e left MUST be the newer run and right must be the older run.
    pub fn runs_to_iterator(left: Arc<Run>, right: Arc<Run>) -> impl Iterator<Item = Item<i32>> {
        return left
            .into_iter()
            .merge_join_by(right.into_iter(), |i, j| i.key.cmp(&j.key))
            .map(|either| match either {
                Left(x) => x,
                Right(x) => x,
                Both(x, _) => x,
            });
    }

    /// Merge a memory map into an existing level 1 run
    /// without consuming the memory map, since we want to keep reading from it while doing this operation
    pub fn merge_memory_map_into_run(
        map: Arc<SkipMap<i32, Option<Vec<u8>>>>,
        run: Arc<Run>,
        config: &rust_store::Config,
    ) -> Result<Run, RunError> {
        debug_assert!(run.level == 1);
        let it = map
            .iter()
            .merge_join_by(run.into_iter(), |i, j| {
                let jkey = j.key;
                i.key().cmp(&jkey)
            })
            .map(|either| match either {
                Left(x) => Item::new(x.key().clone(), x.value().clone()),
                Right(x) => x,
                Both(x, _) => Item::new(x.key().clone(), x.value().clone()),
            });
        info!("Constructed iterator");
        // TODO figure out how to get a better estimate of elements
        // Since this implemented as an iterator, we would need to consume it to get the count,
        // which defeats the point of having an iterator.
        let num_elements = map.len() + run.num_elements;
        return Run::run_from_iterator(it, config, 1, num_elements);
    }

    pub fn load(path: String) -> Result<Run, RunError> {
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

    pub fn new_from_merge(
        runs: &Vec<Arc<Run>>,
        config: &rust_store::Config,
        level: usize,
    ) -> Result<Run, RunError> {
        info!(
            "Merging {} runs into a new level {} run",
            runs.len(),
            &level
        );
        // right now dealing with returning the run and changing the level is a pain
        // The caller should check that there are > 1 runs
        debug_assert!(runs.len() > 1);
        // approx and overestimated as we may double count
        let mut num_elements =
            runs[runs.len() - 1].num_elements + runs[runs.len() - 2].num_elements;

        let mut iterators: Vec<Box<dyn Iterator<Item = Item<i32>>>> = vec![Box::new(
            Run::runs_to_iterator(runs[runs.len() - 1].clone(), runs[runs.len() - 2].clone()),
        )];
        for run in runs[0..runs.len() - 3].into_iter().rev() {
            let next_iter = Box::new(
                iterators
                    .pop()
                    .unwrap()
                    .merge_join_by(run.into_iter(), |i, j| i.key.cmp(&j.key))
                    .map(|either| match either {
                        Left(x) => x,
                        Right(x) => x,
                        Both(x, _) => x,
                    }),
            );
            iterators.push(next_iter);
            num_elements += run.num_elements;
        }
        return Run::run_from_iterator(iterators.pop().unwrap(), config, level, num_elements);
    }

    // Returns value if it exists in the run
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

        let maybe_val = self.get_from_block(page_idx.unwrap(), key);
        return match maybe_val {
            Ok(None) => None,
            Ok(Some(val)) => Some(val),
            Err(RunError::DeserializeError(_)) => {
                // need to figure out handling of deserializing a page and what happens when we run out of data.
                warn!("Failed to get val from run Deserialize error");
                None
            }
            Err(E) => {
                warn!("Failed to get val from run {}", E);
                None
            }
        };
    }

    fn get_from_block(self: &Self, page: u64, key: &i32) -> Result<Option<Vec<u8>>, RunError> {
        debug!("get_from_block: opening file {:?}", &self.file_name);
        let mut f = File::open(&self.file_name)?;
        let _ = f.seek(SeekFrom::Start(page * self.block_size))?;
        let mut page_buf = vec![0u8; self.block_size as usize];
        let num_bytes_read = f.read(&mut page_buf)?;
        trace!("Read {} bytes off disk for page {}", num_bytes_read, page);

        let bincode_opts = bincode::DefaultOptions::new();
        bincode_opts.allow_trailing_bytes();

        let mut deflater = DeflateDecoder::new(Cursor::new(&page_buf));

        let mut decompressed: Vec<u8> = Vec::new();
        let decompressed_size = deflater.read_to_end(&mut decompressed)?;
        trace!(
            "Decompressed page {} from {} bytes",
            page,
            decompressed_size
        );
        let mut decompressed_cursor = Cursor::new(decompressed);

        loop {
            // let item_length: u64 = bincode_opts.deserialize(&mut decompress_bytes)?;
            let item: Item<i32> = match bincode_opts.deserialize_from(&mut decompressed_cursor) {
                Ok(x) => x,
                Err(e) => {
                    return match *e {
                        ErrorKind::Io(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                            Ok(None)
                        }
                        _ => Err(RunError::DeserializeError(e)),
                    }
                }
            };
            if item.key == *key {
                return Ok(item.value);
            }
        }
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

/// We only need a reference because we are not iterating over memory in the Run struct
/// but over the file associated with the run.
impl IntoIterator for &Run {
    type Item = Item<i32>;
    type IntoIter = RunIterator;

    fn into_iter(self) -> Self::IntoIter {
        let f = File::open(&self.file_name);
        debug!("Into iter opening {:?}", &self.file_name);

        if f.is_err() {
            let err = f.err();
            warn!(
                "IO Error for {:?} during run into_iter {:?}",
                &self.file_name, err
            );
            return RunIterator {
                error: Some(RunError::IoError(err.unwrap())),
                reader: None,
                block_size: self.block_size,
                /// only need this cursor since now it owns the block
                decompressed_block_cursor: Cursor::new(vec![0u8]),
                deserializer: bincode::DefaultOptions::new(),
            };
        } else {
            let mut reader = BufReader::new(f.unwrap());

            let mut block_buf = vec![0u8; self.block_size as usize];
            let bincode_opts = bincode::DefaultOptions::new();
            bincode_opts.allow_trailing_bytes();

            let mut decompressed: Vec<u8> = Vec::new();

            let read_res = reader.read_exact(&mut block_buf);

            let mut deflater = DeflateDecoder::new(Cursor::new(block_buf));
            let _ = deflater.read_to_end(&mut decompressed).unwrap();
            let decompressed_cursor = Cursor::new(decompressed);

            if read_res.is_err() {
                return RunIterator {
                    error: Some(RunError::IoError(read_res.err().unwrap())),
                    reader: Some(reader),
                    block_size: self.block_size,
                    /// only need this cursor since now it owns the block
                    decompressed_block_cursor: decompressed_cursor,
                    deserializer: bincode_opts,
                };
            }

            return RunIterator {
                error: None,
                reader: Some(reader),
                block_size: self.block_size,
                /// only need this cursor since now it owns the block
                decompressed_block_cursor: decompressed_cursor,
                deserializer: bincode_opts,
            };
        }
    }
}

/// As we iterate over the run, we load a block at a time off of disk
pub struct RunIterator {
    error: Option<RunError>,
    reader: Option<BufReader<File>>,
    decompressed_block_cursor: Cursor<Vec<u8>>,
    block_size: u64,
    deserializer: bincode::DefaultOptions,
}

impl Iterator for RunIterator {
    type Item = Item<i32>;

    fn next(&mut self) -> Option<Self::Item> {
        debug_assert!(self.reader.is_some());
        if self.error.is_some() {
            warn!(
                "Tried to get item from RUnIterator in broken state: {}",
                self.error.as_ref().unwrap()
            );
            return None;
        }

        let item_res: Result<Item<i32>, bincode::Error> = self
            .deserializer
            .deserialize_from(&mut self.decompressed_block_cursor);

        // First error is expected, we are out of stuff to deserialize,
        // a second error is a legitimate error.
        if item_res.is_err() {
            return match item_res {
                Ok(x) => Some(x),
                Err(e) => {
                    return match *e {
                        ErrorKind::Io(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                            info!("EOF on block in run iterator, opening next block");
                            let mut block_buf = vec![0u8; self.block_size as usize];
                            let reader = self.reader.as_mut();
                            let num_read_bytes = reader.unwrap().read(&mut block_buf);
                            trace!(
                                "Read {:?} bytes on next block of RunIterator",
                                num_read_bytes
                            );

                            let mut deflater = DeflateDecoder::new(Cursor::new(block_buf));
                            let mut decompressed: Vec<u8> = Vec::new();
                            let deflate_res = deflater.read_to_end(&mut decompressed);

                            match deflate_res {
                                Ok(num_bytes) => {
                                    trace!("run iter new block read {} bytes", num_bytes);
                                }
                                Err(e) => {
                                    info!("Got error {} on first read of page. (expected for the last page)", e);
                                    self.error = Some(RunError::IoError(e));
                                    return None;
                                }
                            }

                            let decompressed_cursor = Cursor::new(decompressed);
                            self.decompressed_block_cursor = decompressed_cursor;

                            let maybe_item = self
                                .deserializer
                                .deserialize_from(&mut self.decompressed_block_cursor);
                            match maybe_item {
                                Ok(x) => {
                                    trace!("Got first item from new block");
                                    Some(x)
                                }
                                Err(e) => {
                                    error!("Got error {} on first read of page", e);
                                    self.error = Some(RunError::DeserializeError(e));
                                    None
                                }
                            }
                        }
                        _ => {
                            error!("Error in run iterator: {}", e);
                            None
                        }
                    }
                }
            };
        } else {
            let item = item_res.unwrap();
            return Some(item);
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
    use std::thread::sleep;
    use tempfile::tempdir;
    use test_case::test_case;
    // use test_env_log::test;
    use rand::prelude::SliceRandom;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn small_run_from_memory_map() {
        let _ = env_logger::try_init();
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_directory(dir.path());
        let seed = [42; 32];
        let mut rng = ChaChaRng::from_seed(seed);

        info!("Running small_run_from_memory_map");
        let map = create_skipmap(1000, &mut rng);
        let run = Run::new_from_skipmap(map, &config).unwrap();
        info!("Num pages {}", run.num_blocks);
        run.delete().unwrap();
    }

    #[test]
    fn small_run_get_page_index() {
        let _ = env_logger::try_init();
        sleep(Duration::new(0, 2000000));
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_directory(dir.path());
        let seed = [42; 32];
        let mut rng = ChaChaRng::from_seed(seed);

        info!("Running small_run_from_memory_map");
        let map = create_skipmap(1000, &mut rng);
        for i in 0..500 {
            map.insert(i, Some(vec![0u8, 20u8, 3u8]));
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
        let _ = env_logger::try_init();
        info!("Running small_run_get");
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_directory(dir.path());
        let seed = [42; 32];
        let mut rng = ChaChaRng::from_seed(seed);

        let map = create_skipmap(1000, &mut rng);
        for i in 0..250 {
            map.insert(i, Some(vec![i as u8]));
        }
        let run = Run::new_from_skipmap(map, &config).unwrap();
        for i in 0..250 {
            let val = run.get_from_run(&i);
            assert_eq!(val.unwrap()[0], i as u8);
        }
        run.delete().unwrap();
    }

    #[test]
    fn small_run_get_no_result() {
        let _ = env_logger::try_init();
        info!("Running small_run_get");
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_directory(dir.path());
        let map = Arc::new(SkipMap::new());
        for i in 0..250 {
            map.insert(i, Some(vec![i as u8]));
        }
        let run = Run::new_from_skipmap(map, &config).unwrap();
        let val = run.get_from_run(&42000);
        assert_eq!(val, None);
        run.delete().unwrap();
    }

    #[test]
    fn run_get_from_block() {
        let _ = env_logger::try_init();
        info!("Running small_run_get");
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_directory(dir.path());
        let map = Arc::new(SkipMap::new());
        for i in 0..250 {
            map.insert(i, Some(vec![i as u8]));
        }
        let run = Run::new_from_skipmap(map, &config).unwrap();
        let val = run.get_from_block(0, &42000);
        assert_eq!(val.unwrap(), None);

        for i in 0..250 {
            let page_index = run.get_page_index(&i).unwrap();
            let val = run.get_from_block(page_index, &i);
            assert_eq!(val.unwrap(), Some(vec![i as u8]));
        }

        run.delete().unwrap();
    }

    #[test_case( 50, 50; "small map into small run")]
    #[test_case( 50, 5000; "small map into large run")]
    #[test_case( 5000, 50; "large map into small run")]
    #[test_case( 5000, 50000; "large map into large run")]
    fn merge_memorymap_into_run(memmap_size: i32, run_size: i32) {
        let _ = env_logger::try_init();
        info!("Running small_run_get");
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_directory(dir.path());
        let seed = [42; 32];
        let mut rng = ChaChaRng::from_seed(seed);

        let map = create_skipmap(2000, &mut rng);
        for i in 0..run_size {
            map.insert(i, Some(vec![i as u8]));
        }
        let run = Arc::new(Run::new_from_skipmap(map, &config).unwrap());

        let new_map = Arc::new(SkipMap::new());

        let mut new_keys = Vec::new();
        let mut new_vals = Vec::new();
        let seed = [42; 32];
        let mut rng = ChaChaRng::from_seed(seed);

        // we want some overlap in keys to test that we take the new values in the merge
        for i in run_size / 2..memmap_size + run_size / 2 {
            new_keys.push(i);
            new_vals.push(Some(vec![(i % 127 * 2) as u8]));
        }
        new_keys.shuffle(&mut rng);

        for i in 0..new_keys.len() {
            new_map.insert(new_keys[i], new_vals[i].clone());
        }

        let new_run =
            Arc::new(Run::merge_memory_map_into_run(new_map, run.clone(), &config).unwrap());
        for i in (0..new_keys.len()).step_by(5) {
            let val = new_run.get_from_run(&new_keys[i]).unwrap();
            assert!(&val == new_vals[i].as_ref().unwrap());
        }

        // TODO is this the correct way to unwrap a Arc?
        // seems like it works if you know for sure that no other references exist
        Arc::try_unwrap(run).ok().unwrap().delete().unwrap();
        Arc::try_unwrap(new_run).ok().unwrap().delete().unwrap();
    }

    #[allow(non_snake_case)]
    #[test_case( 512 ; "0.125 KB")]
    #[test_case(1024 ; "0.25 KB")]
    #[test_case(2048 ; "0.5 KB")]
    #[test_case(4096 ; "1 KB")]
    #[test_case(8192 ; "2 KB")]
    #[test_case(16384 ; "4 KB")]
    #[test_case(32768 ; "8 KB")]
    #[test_case(65536 ; "16 KB")]
    #[test_case(131072 ; "32 KB")]
    fn construct_1MB_run_varying_block_size(block_size: u64) {
        let _ = env_logger::try_init();
        info!("Running construct_large_run_random_vals");
        let mut config = Config::default();
        config.set_block_size(block_size).unwrap();
        let dir = tempdir().unwrap();
        config.set_directory(dir.path());
        let seed = [42; 32];
        let mut rng = ChaChaRng::from_seed(seed);

        let map = create_skipmap(4 * 1024 * 1024, &mut rng);
        let run = Run::new_from_skipmap(map, &config).unwrap();

        run.delete().unwrap();
    }

    // generates a random number of random bytes
    fn gen_rand_bytes<T: Rng>(rng: &mut T) -> Vec<u8> {
        let num_bytes = rng.gen_range(8..32);
        let mut ret_vec = vec![0u8; num_bytes];
        rng.fill_bytes(&mut ret_vec);
        return ret_vec;
    }

    // Size in bytes approx
    fn create_skipmap<T: Rng>(size: u64, mut rng: &mut T) -> Arc<SkipMap<i32, Option<Vec<u8>>>> {
        let mut curr_size: u64 = 0;
        let map = Arc::new(SkipMap::new());

        while curr_size < size {
            let rand_key: i32 = rng.gen_range(-50000..50000);
            let rand_val = gen_rand_bytes(&mut rng);
            curr_size += 4 + rand_val.len() as u64;
            map.insert(rand_key, Some(rand_val));
        }
        return map;
    }

    #[test_case( 5 ; "one block")]
    #[test_case(3000 ; "several blocks")]
    #[test_case(10000 ; "many blocks")]
    fn test_run_iter(num_items: i32) {
        let _ = env_logger::try_init();
        info!("Running small_run_get");
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_directory(dir.path());
        let map = Arc::new(SkipMap::new());
        for i in 0..num_items {
            map.insert(i, Some(vec![i as u8]));
        }
        for x in map.clone().iter() {
            println!("{:?} {:?}", x.key(), x.value());
        }
        let run = Run::new_from_skipmap(map, &config).unwrap();
        let mut i = 0;
        for key_val in &run {
            assert_eq!(key_val.value.unwrap()[0], i as u8);
            assert_eq!(key_val.key, i);
            i += 1;
        }
        run.delete().unwrap();
    }

    #[test_case( 3, 5 ; "left 3 elements, right 5 elements")]
    #[test_case( 500, 700 ; "left 500 elements, right 700 elements")]
    #[test_case( 5000, 4000 ; "left 5000 elements, right 4000 elements")]
    fn test_runs_to_iter_disjoint(left_size: i32, right_size: i32) {
        let _ = env_logger::try_init();
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_directory(dir.path());

        let left_map = Arc::new(SkipMap::new());
        let right_map = Arc::new(SkipMap::new());

        for i in 0..left_size {
            left_map.insert(i, Some(vec![(i % 255) as u8]));
        }
        for i in left_size..(left_size + right_size) {
            right_map.insert(i, Some(vec![(i % 255) as u8]));
        }

        let left_run = Arc::new(Run::new_from_skipmap(left_map, &config).unwrap());
        let right_run = Arc::new(Run::new_from_skipmap(right_map, &config).unwrap());

        let merge_iter = Run::runs_to_iterator(left_run, right_run);

        let mut merge_iter_size = 0;
        let mut prev_key = -1;
        for item in merge_iter {
            merge_iter_size += 1;
            let this_key = item.key;
            assert!(
                &this_key > &prev_key,
                "this_key {} > prev_key {}",
                this_key,
                prev_key
            );
            prev_key = this_key;
        }

        assert_eq!(
            merge_iter_size,
            left_size + right_size,
            "merge iter size {} should equal left_size {} + right size {} = {}",
            merge_iter_size,
            left_size,
            right_size,
            left_size + right_size
        );
    }

    #[test_case( 3, 5 ; "3 5 runs")]
    #[test_case( 10, 100 ; "10 100 runs")]
    #[test_case(10, 1000 ; "10 1000 runs")]
    #[test_case(10, 1000000 ; "10 1 mil runs")]
    fn test_new_from_merge(num_runs: i32, map_size: u64) {
        let _ = env_logger::try_init();
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        config.set_directory(dir.path());
        let seed = [42; 32];
        let mut rng = ChaChaRng::from_seed(seed);

        let mut runs = Vec::new();
        let mut total_num_pre_merge = 0;
        for i in 0..num_runs {
            let map = create_skipmap(map_size, &mut rng);
            map.insert(42, Some(vec![i as u8]));
            runs.push(Arc::new(Run::new_from_skipmap(map, &config).unwrap()));
            for item in runs.last().clone().into_iter() {
                total_num_pre_merge += 1;
            }
        }

        let merged_run = Run::new_from_merge(&runs, &config, 2).unwrap();
        let mut num_els = 0;
        for i in merged_run.into_iter() {
            num_els += 1;
        }
        info!("Merged run has {} elements", num_els);
        info!(
            "pre merge num iters run has {} elements",
            total_num_pre_merge
        );
        assert_eq!(
            merged_run.get_from_run(&42).unwrap(),
            vec![(num_runs - 1) as u8; 1]
        );
    }
}
