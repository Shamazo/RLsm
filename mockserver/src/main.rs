// import generated rust code
pub mod pb {
    tonic::include_proto!("rust_kv");
}

use pb::{kv_server::KvServer, kv_server::Kv, GetRequest, PutRequest, GetReply, PutReply, StatusCode, key::Key};

use tonic::{transport::Server, Request, Response, Status};
use tonic;
use anyhow::{Result, bail, anyhow};

use std::env;
use simplelog::{ConfigBuilder, WriteLogger, CombinedLogger, LevelFilter};
use std::fs::File;

extern crate simplelog;
#[macro_use] extern crate log;

#[derive(Default)]
pub struct RequestHandler {}

#[tonic::async_trait]
impl Kv for RequestHandler {
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetReply>, Status> {
        info!("Got a get request from {:?}", request.remote_addr());

        let key = request.into_inner().key;
        let status_code =  match key {
            Some(pb::Key{key : Some(Key::IntKey(42))}) => StatusCode::Success as i32,
            Some(pb::Key{key : Some(Key::UintKey(42))}) => StatusCode::Success as i32,
            Some(pb::Key{key : Some(Key::IntKey(x))}) => if x == 7 {StatusCode::NoValue as i32} else {StatusCode::InternalError as i32},
            Some(pb::Key{key : Some(Key::UintKey(x))}) => if x == 7 {StatusCode::NoValue as i32} else {StatusCode::InternalError as i32},
            Some(pb::Key{key : None}) => return Err(Status::aborted("Malformed request")),
            None => return Err(Status::invalid_argument("Tried to use an unimplemented key type"))
        };

        info!("Sending status code {} for key {:?}", status_code, key);
        let reply = pb::GetReply {
            status_code: status_code as i32,
            value: 42_i32.to_le_bytes().to_vec()
        };
        Ok(Response::new(reply))
    }

    async fn put(
        &self,
        request: Request<PutRequest>,
    ) -> Result<Response<PutReply>, Status> {
        info!("Got a put request from {:?}", request.remote_addr());

        let status_code =  match request.into_inner().key {
            Some(pb::Key{key : Some(Key::IntKey(x))}) => if x == 42 {StatusCode::Success as i32} else {StatusCode::InternalError as i32},
            Some(pb::Key{key : Some(Key::UintKey(x))}) => if x == 42 {StatusCode::Success as i32} else {StatusCode::InternalError as i32},
            Some(pb::Key{key : None}) => return Err(Status::aborted("Malformed request")),
            None => return Err(Status::invalid_argument("Tried to use an unimplemented key type"))
        };

        let reply = pb::PutReply {
            status_code: status_code as i32,
        };
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut log_config_builder = ConfigBuilder::new();
    log_config_builder.set_location_level(LevelFilter::Error);
    CombinedLogger::init(
        vec![
            // TermLogger::new(LevelFilter::Debug, log_config_builder.build(), TerminalMode::Mixed).unwrap(),
            WriteLogger::new(LevelFilter::Debug, log_config_builder.build(), File::create("mockserver.log").unwrap()),
        ]
    ).unwrap();
    let addr = "127.0.0.1:4242".parse().unwrap();
    let request_handler = RequestHandler::default();

    println!("RequestHandler listening on {}", addr);

    Server::builder()
        .add_service(KvServer::new(request_handler))
        .serve(addr)
        .await?;

    Ok(())
}