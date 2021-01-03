pub mod fence_pointer {
    use serde::{Deserialize, Serialize};
    use std::cmp::Ord;
    // T is the Key type
    #[derive(Serialize, Deserialize, Debug)]
    pub struct FencePointer<T: Ord> {
        low: T,
        high: T,
    }

    impl<T: Ord> FencePointer<T> {
        pub fn in_range(&self, x: &T) -> bool {
            return *x >= self.low && *x <= self.high;
        }
        pub fn new(l: T, h: T) -> FencePointer<T> {
            return FencePointer { low: l, high: h };
        }
    }
}

#[cfg(test)]
mod test_fp {
    use crate::fence_pointer::fence_pointer::FencePointer;
    #[test]
    fn fp_int() {
        let fp = FencePointer::new(1, 5);
        assert!(fp.in_range(&2));
        assert!(fp.in_range(&1));
        assert!(fp.in_range(&5));
        assert!(!fp.in_range(&7));
    }

    #[test]
    fn fp_u64() {
        let lower: u64 = 1;
        let upper: u64 = 5;
        let fp = FencePointer::new(lower, upper);
        assert!(fp.in_range(&2));
        assert!(fp.in_range(&1));
        assert!(fp.in_range(&5));
        assert!(!fp.in_range(&7));
    }

    #[test]
    fn fp_can_serialize_deserialize() {
        let lower: u64 = 1;
        let upper: u64 = 5;
        let fp = FencePointer::new(lower, upper);
        assert!(fp.in_range(&2));
        assert!(fp.in_range(&1));
        assert!(fp.in_range(&5));
        assert!(!fp.in_range(&7));

        let s = serde_json::to_string(&fp).unwrap();
        let serde_fp: FencePointer<u64> = serde_json::from_str(&s).unwrap();
        assert!(serde_fp.in_range(&2));
        assert!(serde_fp.in_range(&1));
        assert!(serde_fp.in_range(&5));
        assert!(!serde_fp.in_range(&7));
    }
}
