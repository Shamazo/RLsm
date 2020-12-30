pub mod fence_pointer {
    use std::cmp::Ord;
    use serde::{Serialize, Deserialize};
    // T is the Key type
    #[derive(Serialize, Deserialize, Debug)]
    pub struct FencePointer<T:Ord>{
        low: T,
        high: T
    }

    impl<T:Ord> FencePointer<T>{
        pub fn in_range(&self, x:T) -> bool {
            return x >= self.low && x <= self.high
        }
    }

}