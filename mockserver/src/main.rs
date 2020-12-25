extern crate flatbuffers;
extern crate simplelog;
use std::io::{Write, Read};
use std::net::{TcpStream, TcpListener};
use std::fs::File;
#[macro_use] extern crate log;
use simplelog::*;

#[allow(dead_code, unused_imports)]
use libmessages::api_generated::api;

fn main() {
    let mut log_config_builder = ConfigBuilder::new();
    log_config_builder.set_location_level(LevelFilter::Error);
    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Debug, log_config_builder.build(), TerminalMode::Mixed).unwrap(),
            WriteLogger::new(LevelFilter::Debug, log_config_builder.build(), File::create("mockserver.log").unwrap()),
        ]
    ).unwrap();

    let listener = TcpListener::bind("127.0.0.1:4242").unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        info!("Connection established!");
        handle_connection(stream);

    }
}

fn handle_connection(mut stream: TcpStream) {
    let mut prefix_count_buffer :[u8; 4] = [0,0,0,0];

    loop {
        if stream.read_exact(&mut prefix_count_buffer).is_err(){
            return;
        }
        let buffer_length = u32::from_le_bytes(prefix_count_buffer);

        debug!("Request length: {}", buffer_length);
        let mut flat_buf = vec![0 as u8; buffer_length as usize];
        let read_res = stream.read_exact(&mut flat_buf);
        if read_res.is_err(){
            error!("Failed to read flatbuffer from socket: {:?}", read_res.err())
        }

        let request = api::root_as_message(&*flat_buf).unwrap();
        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);

        match request.payload_type() {
            api::Payload::PutRequest => {
                info!("Got PutRequest");
                // if we put a key with 42 we return a success, else an internal error
                let key =  request.payload_as_put_request().unwrap().keys_as_int_data().unwrap().data().unwrap().get(0);

                let put_response = api::PutResponse::create(&mut builder, &api::PutResponseArgs{});
                let message = api::Message::create(&mut builder, &api::MessageArgs{
                    result: if key == 42 {api::ResultType::Success} else {api::ResultType::InternalFailure},
                    payload_type: api::Payload::PutResponse,
                    payload : Some(put_response.as_union_value())
                });
                builder.finish_size_prefixed(message, None);
                let res = stream.write(builder.finished_data());
                if res.is_err(){
                    error!("Error sending get response: {:?}", res.err())
                }
            }

            api::Payload::GetRequest => {
                info!("Got GetRequest");
                let val_data_args = &api::IntDataArgs{
                    data : Some(builder.create_vector(&[42]))};
                let key =  request.payload_as_get_request().unwrap().keys_as_int_data().unwrap().data().unwrap().get(0);

                let val_vec = api::IntData::create(&mut builder, val_data_args);

                let get_response = api::GetResponse::create(&mut builder, &api::GetResponseArgs{
                    length: 1,
                    values_type: api::Values::IntData,
                    values: Some(val_vec.as_union_value())
                });
                let message = api::Message::create(&mut builder, &api::MessageArgs{
                    result: if key == 42 {api::ResultType::Success} else {api::ResultType::InternalFailure},
                    payload_type: api::Payload::GetResponse,
                    payload : Some(get_response.as_union_value())
                });
                builder.finish_size_prefixed(message, None);
                let res = stream.write(builder.finished_data());
                if res.is_err(){
                    error!("Error sending get response: {:?}", res.err())
                }

            }
            _ => {unimplemented!("We currently only mock gets and puts!");}
        }

    }


}