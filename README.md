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
- github actions to build and run tests
- code coverage
- design/implement more extensive benchmarks 
- design doc 