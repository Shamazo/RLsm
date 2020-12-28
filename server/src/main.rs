mod query;

extern crate simplelog;
use std::io::{Write, Read};
use std::net::{TcpStream, TcpListener};
use std::fs::File;
#[macro_use] extern crate log;
use simplelog::*;


fn main() {
    // init logging
    let mut log_config_builder = ConfigBuilder::new();
    log_config_builder.set_location_level(LevelFilter::Error);
    CombinedLogger::init(
        vec![
            WriteLogger::new(LevelFilter::Debug, log_config_builder.build(), File::create("server.log").unwrap()),
        ]
    ).unwrap();

    // load from disk here


    //
}