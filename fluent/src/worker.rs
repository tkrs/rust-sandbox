use base64;
use rmp::encode;
use rmps;
use rmps::encode::StructMapWriter;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use tmc::DurationOpt;
use uuid::Uuid;
use event_time::TimeConverter;

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Options {
    chunk: String,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
struct Response {
    ack: String,
}

enum State {
    Continue,
    Break,
}

pub struct Worker {
    #[allow(dead_code)]
    pub id: usize,
    pub thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        let t = thread::spawn(move || {
            let addr = "127.0.0.1:24224";

            let mut stream = Worker::connect(addr).expect("Couldn't connect to the server...");

            let mut entry_queues: HashMap<String, RefCell<Vec<(SystemTime, Vec<u8>)>>> = HashMap::new();

            loop {
                let receiver = receiver.lock().expect("Receiver couldn't be locked.");

                let msg = receiver.recv().expect("Couldn't receive a message.");

                match msg {
                    Message::Terminate => {
                        for (k, q) in entry_queues.iter() {
                            let mut q = q.borrow_mut();
                            Worker::terminate(&mut stream, k.to_string(), &mut q);
                        }
                        break;
                    }
                    Message::Incoming(tag, tm, v) => {
                        let q = entry_queues
                            .entry(tag)
                            .or_insert_with(|| RefCell::new(Vec::new()));
                        let mut q = q.borrow_mut();
                        q.push((tm, v));
                    }
                    Message::Flush => {
                        for (k, q) in entry_queues.iter() {
                            let mut q = q.borrow_mut();
                            match Worker::flush(&mut stream, k.to_string(), &mut q, Some(100)) {
                                State::Continue => continue,
                                State::Break => break,
                            }
                        }
                    }
                };
            }
        });
        Worker {
            id,
            thread: Some(t),
        }
    }

    fn connect<A: ToSocketAddrs + Clone>(addr: A) -> io::Result<TcpStream> {
        let mut r = TcpStream::connect(addr.clone()).map(|s| {
            s.set_nodelay(true).unwrap();
            s.set_read_timeout(Some(3.seconds())).unwrap();
            s.set_write_timeout(Some(3.seconds())).unwrap();
            s
        });

        loop {
            match r {
                Ok(_) => break,
                Err(e) => {
                    println!("An error occurred {:?}", e);
                    thread::sleep(500.millis());
                    r = TcpStream::connect(addr.clone()).map(|s| {
                        s.set_nodelay(true).unwrap();
                        s.set_read_timeout(Some(3.seconds())).unwrap();
                        s.set_write_timeout(Some(3.seconds())).unwrap();
                        s
                    });
                }
            }
        }

        r
    }

    fn write<'a>(stream: &mut TcpStream, buf: &'a [u8], ack_id: String) {
        'outer: loop {
            let mut _stream = stream.try_clone().expect("Failed to clone stream");

            let rec = &buf[..];

            match _stream.write_all(rec) {
                Ok(_) => {
                    _stream.flush().unwrap();
                    for _ in 0..10 {
                        let mut resp_buf = [0u8; 64];
                        match _stream.read(&mut resp_buf) {
                            Ok(sz) => {
                                let mut de = Deserializer::new(&resp_buf[0..sz]);
                                let resp: Response = Deserialize::deserialize(&mut de).unwrap();
                                if resp.ack == ack_id {
                                    break 'outer;
                                }
                            }
                            Err(e) => {
                                println!("Failed to read response {:?}", e);
                            }
                        };
                    }
                }
                Err(e) => {
                    println!("Failed to write record. {:?}", e);
                    // let _ = _stream.shutdown(Shutdown::Both);
                    let addr = stream.local_addr().unwrap();
                    *stream = Worker::connect(addr).expect("Couldn't connect to the server...");
                    thread::sleep(500.millis());
                    continue 'outer;
                }
            }
        }
    }

    fn make_buffer(
        buf: &mut Vec<u8>,
        entries: Vec<(SystemTime, Vec<u8>)>,
        tag: String,
        chunk: String,
    ) -> Result<(), rmps::encode::Error> {
        buf.push(0x93u8);
        encode::write_str(buf, tag.as_str())?;
        encode::write_array_len(buf, entries.len() as u32)?;
        for (t, entry) in entries {
            encode::write_array_len(buf, 2)?;
            t.event_time(buf)?;
            for elem in entry {
                buf.push(elem);
            }
        }
        let options = Some(Options { chunk });
        options.serialize(&mut Serializer::with(buf, StructMapWriter))
    }

    fn flush(
        stream: &mut TcpStream,
        tag: String,
        entry_queue: &mut Vec<(SystemTime, Vec<u8>)>,
        sz: Option<usize>,
    ) -> State {
        if entry_queue.is_empty() {
            return State::Continue;
        }

        let mut entries = Vec::new();

        let sz = match sz {
            Some(v) => v,
            None => entry_queue.len(),
        };
        'acc: for _ in 0..sz {
            if entry_queue.is_empty() {
                break 'acc;
            }
            entries.push(entry_queue.remove(0));
        }

        let chunk = base64::encode(&Uuid::new_v4().to_string());
        let mut buf = Vec::new();
        match Worker::make_buffer(&mut buf, entries, tag, chunk.clone()) {
            Ok(_) => {
                // println!("{:?}", buf);
                Worker::write(stream, &buf[..], chunk)
            },
            Err(e) => {
                println!("Unexpected error occurred: {:?}.", e);
                return State::Break;
            }
        }
        State::Continue
    }

    fn terminate(stream: &mut TcpStream, tag: String, entry_queue: &mut Vec<(SystemTime, Vec<u8>)>) {
        if !entry_queue.is_empty() {
            // println!("Worker {} has {} entries left.", id, entry_queue.len());
            Worker::flush(stream, tag, entry_queue, None);
        }
    }
}

pub enum Message {
    Incoming(String, SystemTime, Vec<u8>),
    Flush,
    Terminate,
}
