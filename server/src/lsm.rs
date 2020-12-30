use crate::run;

pub mod lsm {
    use crossbeam_skiplist::SkipMap;
    use anyhow::Result;
    use std::sync::atomic::AtomicBool;

    pub struct lsm {
        primary_memory_map: SkipMap<i32, Vec<u8>>,
        secondary_memory_map: SkipMap<i32, Vec<u8>>,
        use_primary_map: AtomicBool,
    }

    impl lsm {
        pub fn new() -> lsm {
            return lsm{
                primary_memory_map: SkipMap::new(),
                secondary_memory_map: SkipMap::new(),
                use_primary_map: AtomicBool::new(true)
            }
        }

        pub fn get(self: &Self, key: i32) -> Option<Vec<u8>> {
            let res = self.primary_memory_map.get(&key);
            return if res.is_none() {
                None
            } else {
                Some(res.unwrap().value().to_vec())
            }

        }

        pub fn set(self: &Self, key: i32, val: Vec<u8>) -> anyhow::Result<()> {
            self.primary_memory_map.insert(key, val);
            return Ok(());
        }
    }

}