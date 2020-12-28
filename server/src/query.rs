extern crate flatbuffers;
use libmessages::api_generated::api;
use thiserror::Error;
use self::flatbuffers::InvalidFlatbuffer;

#[derive(Error, Debug)]
pub enum QueryParseError {
    /// Represents a failure to read from input.
    #[error("Read error")]
    ReadError { source: std::io::Error },
    #[error("Unimplemented error")]
    UnimplementedError,
    #[error("Flatbuffer None Error")]
    FlatBufferNoneError,
    #[error("InvalidFlatBuffer Error")]
    FlatBufferError(#[from] InvalidFlatbuffer),

    /// Represents all other cases of `std::io::Error`.
    #[error(transparent)]
    IOError(#[from] std::io::Error),

}

enum QueryType{
    Put,
    Get,
    Range,
    Delete
}

struct Query {
    query_type: QueryType,
    key: i32,
    value: i32,
}

impl Query{
    pub fn new(buf: &[u8]) -> Result<Query, QueryParseError>{
        let request = api::root_as_message(buf)?;
        return match request.payload_type() {
            api::Payload::PutRequest => {
                info!("Parsing PutRequest");

                let put_request = request.payload_as_put_request().ok_or(QueryParseError::FlatBufferNoneError)?;
                let (key, value) =
                //How do I handle multiple types here?
                    match (put_request.keys_type(), put_request.values_type()) {
                        (api::Keys::IntData, api::Values::IntData) => {
                            (put_request.keys_as_int_data().ok_or(QueryParseError::FlatBufferNoneError)?.data().ok_or(QueryParseError::FlatBufferNoneError)?.get(0), put_request.keys_as_int_data().ok_or(QueryParseError::FlatBufferNoneError)?.data().ok_or(QueryParseError::FlatBufferNoneError)?.get(0))
                        }
                        _ => {
                            return Err(QueryParseError::UnimplementedError)
                        }
                    };
                Ok(Query {
                    query_type: QueryType::Put,
                    key,
                    value
                })
            }

            api::Payload::GetRequest => {
                info!("Parsing GetRequest");

                let get_request = request.payload_as_get_request().ok_or(QueryParseError::FlatBufferNoneError)?;
                let (key) =
                    //How do I handle multiple types here?
                    match (get_request.keys_type()) {
                        api::Keys::IntData => {
                            get_request.keys_as_int_data().ok_or(QueryParseError::FlatBufferNoneError)?.data().ok_or(QueryParseError::FlatBufferNoneError)?.get(0)
                        }
                        _ => {
                            return Err(QueryParseError::UnimplementedError)
                        }
                    };
                Ok(Query {
                    query_type: QueryType::Get,
                    key,
                    value: 0
                })
            }
            _ => { Err(QueryParseError::UnimplementedError) }
        }
    }
}

// fn get_put_request_key_val(put_request: api::PutRequest) -> Result<(impl Key, impl Value), QueryParseError> {
//     // returns key, val pairs.
//     return match (put_request.keys_type(), put_request.values_type()) {
//         (api::Keys::IntData, api::Values::IntData) => {
//             Ok((put_request.keys_as_int_data().ok_or(std::io::Error)?.data().ok_or(std::io::Error)?.get(0), put_request.keys_as_int_data().ok_or(std::io::Error)?.data().ok_or(std::io::Error)?.get(0)))
//         }
//         (api::Keys::IntData, api::Values::UintData) => {
//             Ok((put_request.keys_as_int_data().ok_or(std::io::Error)?.data().ok_or(std::io::Error)?.get(0), put_request.keys_as_uint_data().ok_or(std::io::Error)?.data().ok_or(std::io::Error)?.get(0)))
//         }
//         (api::Keys::UintData, api::Values::UintData) => {
//             Ok((put_request.keys_as_uint_data().ok_or(std::io::Error)?.data().ok_or(std::io::Error)?.get(0), put_request.keys_as_uint_data().ok_or(std::io::Error)?.data().ok_or(std::io::Error)?.get(0)))
//
//         }
//         (_) => {
//             Err(QueryParseError::UnimplementedError)
//         }
//     }
// }