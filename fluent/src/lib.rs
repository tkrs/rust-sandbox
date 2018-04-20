// Referred to https://doc.rust-lang.org/book/second-edition/ch20-05-sending-requests-via-channels.html

extern crate base64;
extern crate serde;
extern crate uuid;
#[macro_use]
extern crate serde_derive;
extern crate rmp;
extern crate rmp_serde as rmps;
extern crate tmc;

use std::io;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use tmc::DurationOpt;

use rmp::encode;
use rmps::encode::StructMapWriter;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Options {
    chunk: String,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
struct Response {
    ack: String,
}

enum Message {
    Incoming(u32, Vec<u8>),
    Flush,
    Terminate,
}

pub struct Client {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
}

impl Client {
    pub fn new(size: usize) -> Client {
        assert!(size > 0);

        let mut workers = Vec::with_capacity(size);

        let (tx, rx) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(rx));

        for id in 0..size {
            let worker = Worker::new(id, Arc::clone(&receiver));
            workers.push(worker);
        }

        {
            let sender = tx.clone();
            thread::spawn(move || loop {
                thread::sleep(20.millis());
                sender.send(Message::Flush).unwrap();
            });
        }

        Client {
            workers,
            sender: tx,
        }
    }
}

impl Client {
    pub fn send<A>(&self, a: A, timestamp: u32)
    where
        A: Serialize + Send + 'static,
    {
        let mut buf = Vec::new();
        a.serialize(&mut Serializer::with(&mut buf, StructMapWriter))
            .unwrap();
        self.sender.send(Message::Incoming(timestamp, buf)).unwrap();
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        println!("Sending terminate message to all workers.");

        for _ in &mut self.workers {
            let sender = self.sender.clone();
            sender.send(Message::Terminate).unwrap();
        }

        println!("Shutting down all workers.");

        for worker in &mut self.workers {
            println!("Shutting down worker {}", worker.id);

            if let Some(w) = worker.thread.take() {
                w.join().unwrap();
            }
        }
    }
}

struct Worker {
    #[allow(dead_code)]
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

enum State {
    Continue,
    Break,
}

impl Worker {
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
        entries: Vec<(u32, Vec<u8>)>,
        chunk: String,
    ) -> Result<(), rmps::encode::Error> {
        buf.push(0x93u8);
        let tag = "test.human";
        encode::write_str(buf, tag)?;
        encode::write_array_len(buf, entries.len() as u32)?;
        for (t, entry) in entries {
            encode::write_array_len(buf, 2)?;
            encode::write_u32(buf, t)?;
            for elem in entry {
                buf.push(elem);
            }
        }
        let options = Some(Options { chunk });
        options.serialize(&mut Serializer::with(buf, StructMapWriter))
    }

    fn flush(
        stream: &mut TcpStream,
        entry_queue: &mut Vec<(u32, Vec<u8>)>,
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
        match Worker::make_buffer(&mut buf, entries, chunk.clone()) {
            Ok(_) => Worker::write(stream, &buf[..], chunk),
            Err(e) => {
                println!("Unexpected error occurred: {:?}.", e);
                return State::Break;
            }
        }
        State::Continue
    }

    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        let t = thread::spawn(move || {
            let addr = "127.0.0.1:24224";

            let mut stream = Worker::connect(addr).expect("Couldn't connect to the server...");

            let mut entry_queue: Vec<(u32, Vec<u8>)> = Vec::new();

            loop {
                let receiver = receiver.lock().expect("Receiver couldn't be locked.");

                let msg = receiver.recv().expect("Couldn't receive a message.");

                match msg {
                    Message::Terminate => {
                        if !entry_queue.is_empty() {
                            println!("Worker {} has {} entries left.", id, entry_queue.len());
                            Worker::flush(&mut stream, &mut entry_queue, None);
                        }
                        break;
                    }
                    Message::Incoming(t, v) => {
                        entry_queue.push((t, v));
                    }
                    Message::Flush => {
                        match Worker::flush(&mut stream, &mut entry_queue, Some(50)) {
                            State::Continue => continue,
                            State::Break => break,
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
}
