[![codecov](https://codecov.io/gh/Shamazo/RLsm/branch/master/graph/badge.svg?token=yc5E3qaij9)](https://codecov.io/gh/Shamazo/RLsm)
![example workflow name](https://github.com/Shamazo/RLsm/workflows/CI/badge.svg)
# RLsm
A (WIP) LSM based key-value store written in rust.

A very basic example:
```
use rust_kv::{Config, new, get};
let config = Config::default();

    let db = new(config);
    let key = 42;
    let put_val = vec![43_u8, 44_u8];
    let get_val = db.get(&42);
    // get_val == put_val
```


to do
- design/implement more extensive benchmarks 
- design doc 
- compaction 