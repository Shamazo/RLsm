// TODO use rust store crate and have this compile.

// import generated rust code
pub mod pb {
    tonic::include_proto!("rust_kv");
}

use pb::{
    key::Key, kv_server::Kv, kv_server::KvServer, GetReply, GetRequest, PutReply, PutRequest,
    StatusCode,
};

use anyhow::{anyhow, bail, Result};
use tonic;
use tonic::{transport::Server, Request, Response, Status};

use simplelog::{CombinedLogger, ConfigBuilder, LevelFilter, WriteLogger};
use std::fs::File;

use rayon;

extern crate config;

extern crate simplelog;
#[macro_use]
extern crate log;

pub struct RequestHandler {
    lsm_tree: lsm::Lsm<SkipMap<i32, Vec<u8>>>,
}

#[tonic::async_trait]
impl Kv for RequestHandler {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        info!("Got a get request from {:?}", request.remote_addr());

        let key = request.into_inner().key;
        let value = match key {
            Some(pb::Key {
                key: Some(Key::IntKey(x)),
            }) => self.lsm_tree.get(x),
            // Some(pb::Key{key : Some(Key::UintKey(x))}) => self.lsm_tree.get(x), # just dealing with int keys for now
            Some(pb::Key { key: None }) => return Err(Status::aborted("Malformed request")),
            _ => {
                return Err(Status::invalid_argument(
                    "Tried to use an unimplemented key type",
                ))
            }
        };

        let reply = pb::GetReply {
            status_code: StatusCode::Success as i32,
            value: value.unwrap(),
        };
        Ok(Response::new(reply))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutReply>, Status> {
        info!("Got a put request from {:?}", request.remote_addr());

        let req_pb = request.into_inner();
        let maybe_err = match req_pb.key {
            Some(pb::Key {
                key: Some(Key::IntKey(x)),
            }) => self.lsm_tree.set(x, req_pb.value),
            // Some(pb::Key{key : Some(Key::UintKey(x))}) => self.lsm_tree.set(x, req_pb.value), # just dealing with int keys for now
            Some(pb::Key { key: None }) => return Err(Status::aborted("Malformed request")),
            _ => {
                return Err(Status::invalid_argument(
                    "Tried to use an unimplemented key type",
                ))
            }
        };

        let status_code = match maybe_err {
            Err(_) => StatusCode::InternalError,
            _ => StatusCode::Success,
        };

        let reply = pb::PutReply {
            status_code: status_code as i32,
        };
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut settings = config::Config::default();
    settings.merge(config::File::with_name("settings")).unwrap();

    // Print out our settings (as a HashMap)
    println!(
        "{:?}",
        settings.try_into::<HashMap<String, String>>().unwrap()
    );

    let mut log_config_builder = ConfigBuilder::new();
    log_config_builder.set_location_level(LevelFilter::Error);
    CombinedLogger::init(vec![
        // TermLogger::new(LevelFilter::Debug, log_config_builder.build(), TerminalMode::Mixed).unwrap(),
        WriteLogger::new(
            LevelFilter::Debug,
            log_config_builder.build(),
            File::create("mockserver.log").unwrap(),
        ),
    ])
    .unwrap();
    let addr = "127.0.0.1:4242".parse().unwrap();
    // let request_handler = RequestHandler::default();
    println!("RequestHandler listening on {}", addr);

    // instantiate LSM tree
    let lsm = lsm::Lsm::new();

    let operator_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .unwrap();

    Server::builder()
        .add_service(KvServer::new(RequestHandler { lsm_tree: lsm }))
        .serve(addr)
        .await?;
    Ok(())
}
