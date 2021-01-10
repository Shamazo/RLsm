use rand::seq::SliceRandom;
use rand::Rng;
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::{ChaCha20Rng, ChaChaRng};
use rand_distr::num_traits::clamp;
use rand_distr::{Distribution, Normal, Standard, Uniform, WeightedAliasIndex};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WorkloadGeneratorError {
    #[error("IO Error")]
    IoError(#[from] std::io::Error),
    #[error("Invalid parameter: '{0}'")]
    InvalidParameter(String),
    #[error("Not yet implemented")]
    NotImplemented,
}

// TODO generate range queries
#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq)]
pub enum RequestType {
    Get,
    Put,
    Delete,
}

// We want to be able to get a random request type
// This allows us to use:
// 'let op_type: RequstType =rng.gen();'
// to generate random values, but we also can use WeightedAliasIndex to get random values
// with a weighting
impl Distribution<RequestType> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> RequestType {
        match rng.gen_range(0..=2) {
            0 => RequestType::Get,
            1 => RequestType::Put,
            _ => RequestType::Delete,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Request {
    pub key: i32,
    pub value: Option<Vec<u8>>,
    pub request_type: RequestType,
}

#[derive(Default, Serialize, Deserialize)]
pub struct WorkloadParameters {
    num_puts: u32,
    num_gets: u32,
    gets_skew: f32,
    gets_miss_ratio: f32,
    num_deletes: u32,
    num_ranges: u32,
    seed: [u8; 32],
}

impl WorkloadParameters {
    /// Consume the WorkloadParamaters object and write a workload to the specified path.
    pub fn write_to_file(self: Self, file: PathBuf) {
        // write self to file and then a the requests
        unimplemented!();
    }
    pub fn new(
        num_puts: u32,
        num_gets: u32,
        num_deletes: u32,
        num_ranges: u32,
        gets_skew: f32,
        gets_miss_ratio: f32,
        seed: [u8; 32],
    ) -> Result<WorkloadParameters, WorkloadGeneratorError> {
        if num_puts == 0 && (num_gets > 0 || num_deletes > 0) {
            return Err(WorkloadGeneratorError::InvalidParameter(
                "Must have > 0 puts".parse().unwrap(),
            ));
        }

        return Ok(WorkloadParameters {
            num_puts,
            num_gets,
            gets_skew,
            gets_miss_ratio,
            num_deletes,
            num_ranges,
            seed,
        });
    }
}

impl fmt::Display for WorkloadParameters {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "+----------------------------+\n\
            |        WORKLOAD INFO       |\n\
            +----------------------------+\n\
            | Puts: {}                    |\n\
            | Gets: {}                    |\n\
            | Gets-Skewness: {}           |\n\
            | Deletes: {}                 |\n\
            | Ranges: {}                  |\n\
            | Get-Misses-Ratio: {}        |\n\
            | Seed : {:?}                 |\n\
            +----------------------------+",
            self.num_puts,
            self.num_gets,
            self.gets_skew,
            self.num_deletes,
            self.num_ranges,
            self.gets_miss_ratio,
            self.seed
        )
    }
}

impl IntoIterator for WorkloadParameters {
    type Item = Request;
    type IntoIter = WorkloadGenerator<ChaCha20Rng>;

    fn into_iter(self) -> Self::IntoIter {
        // we generate random ops using a weighted distribution on the number of each kind
        let choices = vec![RequestType::Get, RequestType::Put, RequestType::Delete];
        let weights = vec![self.num_gets, self.num_puts, self.num_deletes];
        let op_dist = WeightedAliasIndex::new(weights).unwrap();
        let rng = ChaChaRng::from_seed(self.seed);

        return WorkloadGenerator {
            params: self,
            current_gets: 0,
            current_puts: 0,
            current_deletes: 0,
            current_ranges: 0,
            old_puts: vec![],
            old_gets: vec![],
            old_puts_max_size: 0,
            old_gets_max_size: 0,
            rng: rng,
            op_dist: op_dist,
            ops: choices,
        };
    }
}

pub struct WorkloadGenerator<T: Rng> {
    params: WorkloadParameters,
    current_gets: u32,
    current_puts: u32,
    current_deletes: u32,
    current_ranges: u32,
    old_puts: Vec<i32>,
    old_gets: Vec<i32>,
    old_puts_max_size: usize,
    old_gets_max_size: usize,
    rng: T,
    op_dist: WeightedAliasIndex<u32>,
    ops: Vec<RequestType>,
}

impl<T: Rng> WorkloadGenerator<T> {
    pub fn sample_op(self: &mut Self) -> RequestType {
        return self.ops[self.op_dist.sample(&mut self.rng)];
    }
}

impl<T: Rng> Iterator for WorkloadGenerator<T> {
    type Item = Request;

    fn next(&mut self) -> Option<Self::Item> {
        // weighted sample
        let mut op = self.sample_op();

        // No more operations can be generated
        // The right side of the || is an invalid workflow, gets with no puts
        // such a workflow will infinite loop
        if (self.current_gets >= self.params.num_gets
            && self.current_puts >= self.params.num_puts
            && self.current_deletes >= self.params.num_deletes)
            || (self.params.num_puts == 0 && self.params.num_gets > 0)
        {
            return None;
        }

        loop {
            match op {
                RequestType::Get => {
                    if self.current_gets >= self.params.num_gets || self.current_puts == 0 {
                        op = self.sample_op();
                    } else {
                        break;
                    }
                }
                RequestType::Put => {
                    if self.current_puts >= self.params.num_puts {
                        op = self.sample_op();
                    } else {
                        break;
                    }
                }
                RequestType::Delete => {
                    // can't do deletes without puts
                    if self.current_deletes >= self.params.num_deletes || self.current_puts == 0 {
                        op = self.sample_op();
                    } else {
                        break;
                    }
                }
            }
        }

        return match op {
            RequestType::Get => {
                let key: i32;
                // With a probability use a new key
                if self.rng.gen_range(0.0..1.0) > self.params.gets_skew || self.old_gets.len() == 0
                {
                    // with a probability use a key that has been previously put
                    if self.rng.gen_range(0.0..1.0) > self.params.gets_miss_ratio {
                        key = self.old_puts.choose(&mut self.rng).unwrap().clone();
                    } else {
                        // or a random key that is likely a miss
                        key = generate_normal_int_key(&mut self.rng);
                    }
                    // save the selected key
                    if self.old_gets_max_size > self.old_gets.len() {
                        let idx = self.rng.gen_range(0..self.old_gets.len());
                        self.old_gets[idx] = key;
                    } else {
                        self.old_gets.push(key);
                    }
                }
                // or use an existing key
                else {
                    key = self.old_gets.choose(&mut self.rng).unwrap().clone();
                }

                self.current_gets += 1;
                return Some(Request {
                    key: key,
                    value: None,
                    request_type: RequestType::Get,
                });
            }
            RequestType::Put => {
                let key = generate_normal_int_key(&mut self.rng);
                let value = generate_normal_bytes(&mut self.rng, 16);
                self.current_puts += 1;

                if self.old_puts_max_size > self.old_puts.len() {
                    let idx = self.rng.gen_range(0..self.old_puts.len());
                    self.old_puts[idx] = key;
                } else {
                    self.old_puts.push(key);
                }
                Some(Request {
                    key: key,
                    value: Some(value),
                    request_type: RequestType::Put,
                })
            }
            RequestType::Delete => {
                let key = self.old_puts.choose(&mut self.rng).unwrap().clone();
                self.current_deletes += 1;
                Some(Request {
                    key: key,
                    value: None,
                    request_type: RequestType::Delete,
                })
            }
        };
    }
}

fn generate_normal_int_key<T: Rng>(mut rng: &mut T) -> i32 {
    let normal = Normal::new(0.0, (i32::MAX / 3) as f32).unwrap();
    let v = clamp(normal.sample(&mut rng) as i32, i32::MIN, i32::MAX);
    return v;
}

fn generate_uniform_int_key<T: Rng>(mut rng: &mut T) -> i32 {
    let u = Uniform::new(i32::MIN, i32::MAX);
    let v = u.sample(&mut rng);
    return v;
}

fn generate_normal_bytes<T: Rng>(mut rng: &mut T, num_bytes: usize) -> Vec<u8> {
    let mut ret_vec = vec![0u8; num_bytes];
    let normal = Normal::new(0.0, (u8::MAX / 3) as f32).unwrap();
    for i in 0..ret_vec.len() {
        ret_vec[i] = clamp(normal.sample(&mut rng) as u8, u8::MIN, u8::MAX);
    }
    return ret_vec;
}

#[cfg(test)]
mod test_run {
    use crate::workload_generator::{RequestType, WorkloadParameters};
    use log::info;
    use std::collections::HashSet;
    use std::panic;
    use test_case::test_case;

    macro_rules! assert_delta {
        ($x:expr, $y:expr, $d:expr) => {
            if !($x - $y < $d || $y - $x < $d) {
                panic!();
            }
        };
    }

    #[test]
    fn test_workload_equality() {
        let params_a = WorkloadParameters::new(1000, 2000, 7, 0, 0.3, 0.4, [42; 32]).unwrap();
        let params_b = WorkloadParameters::new(1000, 2000, 7, 0, 0.3, 0.4, [42; 32]).unwrap();

        itertools::assert_equal(params_a.into_iter(), params_b.into_iter());
    }

    #[test]
    fn test_workload_inequality() {
        let params_a = WorkloadParameters::new(1000, 2000, 7, 0, 0.3, 0.4, [42; 32]).unwrap();
        let params_b = WorkloadParameters::new(1000, 2000, 7, 0, 0.3, 0.4, [41; 32]).unwrap();

        let miss_skew_res = panic::catch_unwind(|| {
            itertools::assert_equal(params_a.into_iter(), params_b.into_iter());
        });

        assert!(miss_skew_res.is_err());
    }

    #[test_case( 10, 10, 0, 0.5, 0.5;  "10 puts, 10 gets, 0 deletes, 0.5 gets skew, 0.5 gets miss rate")]
    #[test_case( 100, 0, 0, 0.0, 0.0;  "100 puts, 0 gets, 0 deletes, 0.0 gets skew, 0.0 gets miss rate")]
    #[test_case( 100, 100, 100, 0.0, 0.0;  "100 puts, 100 gets, 100 deletes, 0.0 gets skew, 0.0 gets miss rate")]
    #[test_case( 1000, 10, 50, 0.0, 0.0;  "1000 puts, 10 gets, 50 deletes, 0.0 gets skew, 0.0 gets miss rate")]
    #[test_case( 1000, 1000, 50, 0.2, 0.0;  "1000 puts, 10 gets, 50 deletes, 0.2 gets skew, 0.0 gets miss rate")]
    #[test_case( 1000, 1000, 50, 0.0, 0.2;  "1000 puts, 10 gets, 50 deletes, 0.0 gets skew, 0.2 gets miss rate")]
    #[test_case( 1000, 1000, 50, 0.2, 0.2;  "1000 puts, 10 gets, 50 deletes, 0.2 gets skew, 0.2 gets miss rate")]
    fn test_workload_generate_iter(
        num_puts: u32,
        num_gets: u32,
        num_deletes: u32,
        gets_skew: f32,
        gets_miss_ratio: f32,
    ) {
        let params = WorkloadParameters::new(
            num_puts,
            num_gets,
            num_deletes,
            0,
            gets_skew,
            gets_miss_ratio,
            [42; 32],
        )
        .unwrap();

        info!("Testing generator: \n {}", params);

        let mut actual_gets = 0;
        let mut actual_puts = 0;
        let mut actual_deletes = 0;
        let mut get_miss_count = 0;
        let mut get_skew_count = 0;
        let mut get_keys = HashSet::new();
        let mut put_keys = HashSet::new();

        for op in params.into_iter() {
            match op.request_type {
                RequestType::Get => {
                    if !get_keys.contains(&op.key) {
                        get_skew_count += 1;
                    }
                    if !put_keys.contains(&op.key) {
                        get_miss_count += 1;
                    }
                    get_keys.insert(op.key);
                    actual_gets += 1;
                }
                RequestType::Put => {
                    put_keys.insert(op.key);
                    actual_puts += 1;
                }
                RequestType::Delete => {
                    actual_deletes += 1;
                }
            }
        }

        assert_eq!(actual_gets, num_gets);
        assert_eq!(actual_puts, num_puts);
        assert_eq!(actual_deletes, num_deletes);

        let miss_skew_res = panic::catch_unwind(|| {
            assert_delta!(
                get_miss_count as f32 / num_gets as f32,
                gets_miss_ratio,
                0.001
            );
        });

        if num_gets > 0 {
            if miss_skew_res.is_err() {
                println!(
                    "Get miss count {}, num gets {}, ratio {}, expected ratio {}",
                    get_miss_count,
                    num_gets,
                    get_miss_count as f32 / num_gets as f32,
                    gets_miss_ratio
                );
                panic!();
            }

            assert_delta!(
                get_skew_count as f32 / num_gets as f32,
                gets_miss_ratio,
                0.001
            );
        }
    }
}
