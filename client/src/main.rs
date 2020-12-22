extern crate flatbuffers;
#[allow(dead_code, unused_imports)]
use libmessages::api_generated::api;
use std::io;
use std::io::{Write, BufRead};
use std::net::{SocketAddrV4, TcpStream};

// use std::io::Read;

mod handle_requests {
    use crate::Connection;
    use std::net::SocketAddrV4;
    use libmessages::api_generated::api;

    pub fn handle_put_request(input: String, conn: &mut Connection) {
        let args: Vec<&str> = input.split_whitespace().collect();
        if args.len() != 3 {
            syntax_error("Incorrect number of arguments for a put\n");
            return;
        }
        if !conn.is_connected {
            print!("Not connected to a server! \n");
            return;
        }
        let key_parse = args[1].parse::<i32>();
        let val_parse = args[2].parse::<i32>();

        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);

        if key_parse.is_err(){
            println!("Failed to parse key: {}", key_parse.err().unwrap());
            return;
        }
        if val_parse.is_err(){
            println!("Failed to parse key: {}", val_parse.err().unwrap());
            return;
        }

        let key_data_args = &api::IntDataArgs{
            data : Some(builder.create_vector(&[key_parse.unwrap()]))};
        let key_vec = api::IntData::create(&mut builder, key_data_args);

        let val_data_args = &api::IntDataArgs{
            data : Some(builder.create_vector(&[val_parse.unwrap()]))};
        let val_vec = api::IntData::create(&mut builder, val_data_args);

        let put_request = api::PutRequest::create(&mut builder, &api::PutRequestArgs{
            length: 1,
            keys_type: api::Keys::IntData,
            keys: Some(key_vec.as_union_value()),
            values_type: api::Values::IntData,
            values: Some(val_vec.as_union_value())
        });

        let message = api::Message::create(&mut builder, &api::MessageArgs{
            result: api::ResultType::Success, // not used in requests
            payload_type: api::Payload::PutRequest,
            payload : Some(put_request.as_union_value())
        });

        builder.finish(message, None);
        let send_result = conn.send_message(builder.finished_data());
        if send_result.is_err(){
            println!("Failed to send message: {}", send_result.err().unwrap());
            return;
        }

        // we block until we get a response
        let return_buffer_response = conn.receive_message();
        if return_buffer_response.is_err(){
            println!("Failed to receive message: {}", return_buffer_response.err().unwrap());
            return;
        }
        let return_buffer = return_buffer_response.unwrap();
        let possible_err = handle_response(return_buffer, api::Payload::PutResponse);
        if possible_err.is_err(){
            println!("flatbuffer error");
        }

    }

    pub fn handle_get_request(input: String, conn: &mut Connection) {
        let args: Vec<&str> = input.split_whitespace().collect();
        if args.len() != 2 {
            syntax_error("Incorrect number of arguments for a get\n");
            return;
        }
        if !conn.is_connected {
            print!("Not connected to a server! \n");
            return;
        }
        let key_parse = args[1].parse::<i32>();

        let mut builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);

        if key_parse.is_err(){
            println!("Failed to parse key: {}", key_parse.err().unwrap());
            return;
        }

        let key_data_args = &api::IntDataArgs{
            data : Some(builder.create_vector(&[key_parse.unwrap()]))};
        let key_vec = api::IntData::create(&mut builder, key_data_args);

        let get_request = api::GetRequest::create(&mut builder, &api::GetRequestArgs{
            length: 1,
            keys_type: api::Keys::IntData,
            keys: Some(key_vec.as_union_value()),
        });

        let message = api::Message::create(&mut builder, &api::MessageArgs{
            result: api::ResultType::Success, //unused in requests
            payload_type: api::Payload::GetRequest,
            payload : Some(get_request.as_union_value())
        });

        builder.finish(message, None);
        let send_result = conn.send_message(builder.finished_data());
        if send_result.is_err(){
            println!("Failed to send message: {}", send_result.err().unwrap());
            return;
        }

        // we block until we get a response
        let return_buffer_response = conn.receive_message();
        if return_buffer_response.is_err(){
            println!("Failed to receive message: {}", return_buffer_response.err().unwrap());
            return;
        }
        let return_buffer = return_buffer_response.unwrap();
        let possible_err = handle_response(return_buffer, api::Payload::GetResponse);
        if possible_err.is_err(){
            println!("flatbuffer error");
        }

        return;
    }

    pub fn handle_connect_request(input: String, conn: &mut Connection){
        let args: Vec<&str> = input.split_whitespace().collect();
        if args.len() != 2 {
            syntax_error("Incorrect number of arguments for connect\n");
            return;
        }
        let ip_socket = args[1].parse::<SocketAddrV4>();
        match ip_socket {
            Ok(ip_socket) => {
                conn.connect(ip_socket);
                println!("Connected to {:?}", ip_socket);
                return;
            },
            Err(e) => {
                println!("Failed to parse ip and port: {}", e);
                return;
            }
        }
    }

    fn handle_response(buf: Vec<u8>, expected_type: api::Payload) -> Result<(), std::io::Error> {
        let return_message_result = api::root_as_message(&*buf);
        match return_message_result {
            Ok(message) => {
                if message.result() == api::ResultType::Success{
                    if message.payload_type() == expected_type {
                        if message.payload_type() == api::Payload::PutResponse {
                            println!("Success")
                        } else if message.payload_type() == api::Payload::GetResponse {
                            match message.payload_as_get_response().ok_or(std::io::ErrorKind::InvalidData)?.values_type() {
                                api::Values::IntData => {
                                    println!("value {}", message.payload_as_get_response().ok_or(std::io::ErrorKind::InvalidData)?.values_as_int_data().ok_or(std::io::ErrorKind::InvalidData)?.data().ok_or(std::io::ErrorKind::InvalidData)?.get(0));
                                }
                                api::Values::UintData => {
                                    println!("value {}", message.payload_as_get_response().ok_or(std::io::ErrorKind::InvalidData)?.values_as_uint_data().ok_or(std::io::ErrorKind::InvalidData)?.data().ok_or(std::io::ErrorKind::InvalidData)?.get(0));
                                }
                                _ => {} //TODO
                            }
                        }

                    } else {
                        println!("Unexpected message received");
                    }
                } else if  message.result() == api::ResultType::InternalFailure{
                    println!("Internal failure");
                    return Ok(());
                } else {
                    println!("Invalid result type");
                    return Ok(());
                }
            },
            Err(e) => {
                println!("Failed to parse response: {:?}", e);
            }
        }
    return Ok(());
    }

    fn syntax_error(error: &str){
        print!("There was a syntax error: {}", error)
    }
}

pub struct Connection {
    is_connected: bool,
    stream: Option<TcpStream>
}

impl Connection{
    fn connect(&mut self, ip_socket: SocketAddrV4){
        self.is_connected = true;
        if self.is_connected {
            println!("Already connected to server!");
            return;
        }
        let stream = TcpStream::connect(ip_socket);
        match stream {
            Ok(stream) => {
                self.stream = Some(stream);
                self.is_connected = true;
            },
            Err(e) => {
                println!("Failed to connect: {}", e);
                self.is_connected = false;
            }
        }
    }

    fn send_message(&mut self, buf: &[u8]) -> io::Result<()> {
        let bytes_written = self.stream.as_ref().unwrap().write(buf)?;

        if bytes_written < buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                format!("Sent {}/{} bytes", bytes_written, buf.len()),
            ));
        }

        self.stream.as_ref().unwrap().flush()?;
        Ok(())
    }

    fn receive_message(&mut self) -> Result<Vec<u8>, std::io::Error> {
        // Wrap the stream in a BufReader, so we can use the BufRead methods
        let stream = self.stream.as_ref().unwrap();
        let mut reader = io::BufReader::new( stream);

        // Read current current data in the TcpStream
        let received: Vec<u8> = reader.fill_buf()?.to_vec();


        // Mark the bytes read as consumed so the buffer will not return them in a subsequent read
        reader.consume(received.len());


        return Ok(received);
    }
}

fn main() {
    let mut conn = Connection{is_connected: false, stream: None};

    loop{
        print!(">  ");
        io::stdout().flush().expect("Could not flush stdout");
        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Error reading input.");
        if input.starts_with("put") {handle_requests::handle_put_request(input, &mut conn)}
        else if input.starts_with("get") {handle_requests::handle_get_request(input, &mut conn)}
        else if input.starts_with("connect") {handle_requests::handle_connect_request(input,  &mut conn)}
        else {print!("Not a Put or a Get!!{}", input);
        }
    }
}
