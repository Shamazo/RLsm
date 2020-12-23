# RLsm
A (WIP) LSM based key-value store written in rust

### Flatbuffers
To have decent Rust support with flatbuffers we need to use the latest master. To make things easier this repo has a flatbuffers submodule.  To get started clone this repo with the `--recurse-submodules` option. Then run `build_flatbuffers.py` in the root of the repo to generate flatbuffer code for the python and rust clients. 

to do 
- github actions to build and run tests
- design/implement benchmarks 
- design doc 