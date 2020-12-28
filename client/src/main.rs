pub mod pb {
    tonic::include_proto!("rust_kv");
}

use pb::{kv_client::KvClient, GetRequest, PutRequest, StatusCode};

use tonic;
// use tonic::transport::Endpoint;
use std::env;

use anyhow::{Result, bail, anyhow};


use std::io;
use std::io::Write;
use std::convert::TryInto;

enum KvMessage {
    GetRequest(GetRequest),
    PutRequest(PutRequest)
}


fn construct_message(input: &str) -> Result<KvMessage> {
    let args: Vec<&str> = input.split_whitespace().collect();
    let message: Option<KvMessage>;
    match input {
        _ if input.starts_with("put") => {
            if args.len() != 3 {
                bail!("Invalid put usage. Correct usage: put 42 67");
            }
            let key = args[1].parse::<i32>()?;
            let val = args[2].parse::<i32>()?;
            message = Some(KvMessage::PutRequest(pb::PutRequest {
                // this is overly complicated because of generated code from rust_kv.rs
                key: Some(pb::Key { key: Some(pb::key::Key::IntKey(key)) }),
                value: val.to_le_bytes().to_vec()
            }));
        },
        _ if input.starts_with("get") => {
            if args.len() != 2 {
                bail!("Invalid get usage. Correct usage: get 42");
            }
            let key = args[1].parse::<i32>()?;
            message = Some(KvMessage::GetRequest(pb::GetRequest {
                // this is overly complicated because of generated code from rust_kv.rs
                key: Some(pb::Key{key: Some(pb::key::Key::IntKey(key))}),
            }));
        },
        _ => {
            bail!("not a get or a put")
        }

    };
    return Ok(message.unwrap());
}

fn byte_vec_to_4_byte_arr(x: Vec<u8>) -> Result<[u8; 4]>{
    let y = x.try_into();
    if y.is_err(){
        //TODO real errors
        return Err(anyhow!("Failed to convert protobuf byte vector to 4byte array"))
    }
    return Ok(y.unwrap());
}

fn byte_vec_to_int(x: Vec<u8>) -> Result<i32> {
    return Ok(i32::from_le_bytes(byte_vec_to_4_byte_arr(x)?));
}

async fn handle_request(input: &str, client: &mut KvClient<tonic::transport::Channel> ) -> Result<()>{
    let message = construct_message(&input)?;
    match message {
        KvMessage::GetRequest(message) => {
            let request: tonic::Request<GetRequest> = tonic::Request::new(message);
            let response = client.get(request).await?.into_inner();
            if response.status_code != StatusCode::Success as i32 {
                println!("Failed to get")
            } else {
                println!("{}", byte_vec_to_int(response.value)?);
            }
        },
        KvMessage::PutRequest(message) => {
            let request: tonic::Request<PutRequest> = tonic::Request::new(message);
            let response = client.put(request).await?.into_inner();
            if response.status_code != StatusCode::Success as i32 {
                println!("Failed to Put")
            }
        }
    };
    return Ok(());
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        bail!("Proper usage: ./client 127.0.0.1:4242");
    }
    // TODO not hardcode the endpoint
    // https://github.com/hyperium/tonic/issues/450
    // may need to use sonic at master
    // let ip_port = &args[0].parse::<SocketAddrV4>()?
    // let endpoint = tonic::transport::Endpoint::try_from(&args[0]);
    let mut client :KvClient<tonic::transport::Channel> = KvClient::connect("http://127.0.0.1:4242").await?;

    loop{
        print!(">  ");
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        let maybe_error = handle_request(&input,  &mut client).await;
        if maybe_error.is_err() {
            println!("Failed request: {:?}", maybe_error.err())
        }
    }
}