use crate::run;

use crate::memory_map::Map;
// use anyhow::Result;
use std::sync::atomic::AtomicBool;

pub struct Lsm<T: Map<i32> + IntoIterator> {
    #[allow(dead_code)]
    mutable_memory_map: T,
    #[allow(dead_code)]
    immutable_memory_map: T,
    #[allow(dead_code)]
    use_primary_map: AtomicBool,
}

impl<T: Map<i32> + IntoIterator> Lsm<T> {
    pub fn new() -> Lsm<T> {
        return Lsm {
            mutable_memory_map: T::new(),
            immutable_memory_map: T::new(),
            use_primary_map: Default::default(),
        };
    }
    #[allow(dead_code)]
    pub fn get(self: &Self, key: i32) -> Option<Vec<u8>> {
        return self.mutable_memory_map.get(key);
    }
    #[allow(dead_code)]
    pub fn set(self: &Self, key: i32, val: Vec<u8>) -> anyhow::Result<()> {
        self.mutable_memory_map.put(key, val);
        return Ok(());
    }
}
