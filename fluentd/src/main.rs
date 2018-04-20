// Referred to https://doc.rust-lang.org/book/second-edition/ch20-05-sending-requests-via-channels.html

extern crate base64;
extern crate uuid;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate rmp;
extern crate rmp_serde as rmps;
extern crate rand;

use rand::Rng;
use std::io;
use std::io::{Read, Write};
use std::net::{ToSocketAddrs, TcpStream};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use uuid::Uuid;
use serde::{Deserialize, Serialize};
use rmp::encode;
use rmps::{Deserializer, Serializer};
use rmps::encode::StructMapWriter;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Options {
    chunk: String
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
struct Response {
    ack: String
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Human {
    age: u32,
    name: String
}

trait DurationOpt {
    fn seconds(&self) -> Duration;
    fn millis(&self) -> Duration;
    // fn nanos(&self) -> Duration;
}

impl DurationOpt for i32 {
    fn seconds(&self) -> Duration { Duration::from_secs(*self as u64) }
    fn millis(&self) -> Duration { Duration::from_millis(*self as u64) }
    // fn nanos(&self) -> Duration { Duration::from_nanos(*self as u64) }
}
impl DurationOpt for u32 {
    fn seconds(&self) -> Duration { Duration::from_secs(*self as u64) }
    fn millis(&self) -> Duration { Duration::from_millis(*self as u64) }
    // fn nanos(&self) -> Duration { Duration::from_nanos(*self as u64) }
}

struct Client {
    workers: Vec<Worker>,
    sender: Arc<Mutex<mpsc::Sender<Message>>>
}

impl Client {
    fn new(size: usize) -> Client {
        assert!(size > 0);

        let mut workers = Vec::with_capacity(size);

        let (tx, rx) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(rx));
        let sender = Arc::new(Mutex::new(tx));

        for id in 0..size {
            let worker = Worker::new(id, Arc::clone(&receiver));
            workers.push(worker);
        }

        {
            let sender = Arc::clone(&sender);
            thread::spawn(move || {
                thread::sleep(1.seconds());
                loop {
                    let sender = sender.lock().unwrap();
                    sender.send(Message::Flush).unwrap();
                    thread::sleep(1.seconds());
//                    thread::sleep(50.millis());
                }
            });
        }

        Client {
            workers,
            sender
        }
    }
}

impl Client {
    pub fn send<A>(&self, a: A, timestamp: u32) where A: Serialize + Send + 'static {
        let mut buf = Vec::new();
        a.serialize(&mut Serializer::with(&mut buf, StructMapWriter)).unwrap();
        let sender = self.sender.lock().expect("Woo!!");
        sender.send(Message::Incoming(timestamp, buf)).unwrap();
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        println!("Sending terminate message to all workers.");

        for _ in &mut self.workers {
            let sender = self.sender.lock().expect("Woo!!");
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

enum Message {
    Incoming(u32, Vec<u8>),
    Flush,
    Terminate
}

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>
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

    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        let t = thread::spawn(move || {
            let addr = "127.0.0.1:24224";

            let mut stream = Worker::connect(addr)
                .expect("Couldn't connect to the server...");

            let mut entry_queue: Vec<(u32, Vec<u8>)> = Vec::new();

            'outer: loop {
                let receiver = receiver.lock()
                    .expect("Failed to lock receiver");

                let msg = receiver.recv()
                    .expect("Failed to receive job");

                match msg {
                    Message::Terminate => {
                        println!("Worker {} was told to terminate.", id);
                        break
                    },
                    Message::Incoming(t, v) => {
                        println!("Worker {} got a record: ({}, {:?}).", id, t, v);
                        entry_queue.push((t, v));
                    }
                    Message::Flush => {
                        println!("Worker {} was told to flush entries: {}; executing.", entry_queue.len(), id);

                        if entry_queue.is_empty() {
                            continue 'outer;
                        }

                        let mut entries = Vec::new();
                        'acc: for _ in 0..50 {
                            if entry_queue.is_empty() {
                                break 'acc;
                            }
                            entries.push(entry_queue.remove(0));
                        }
                        // let entries = entries.iter().map(|e| (e.0, &e.1[..])).collect();

                        // println!("{:?}", entries);
                        let mut buf = Vec::new();

                        buf.push(0x93u8);

                        let tag = "test.human";
                        encode::write_str(&mut buf, tag).unwrap();

                        encode::write_array_len(&mut buf, entries.len() as u32).unwrap();
                        for (t, entry) in entries {
                            encode::write_array_len(&mut buf, 2).unwrap();
                            encode::write_u32(&mut buf, t).unwrap();
                            for elem in entry {
                                buf.push(elem);
                            }
                        }

                        let chunk = base64::encode(&Uuid::new_v4().to_string());
                        let ack = chunk.clone();
                        let options = Some(Options { chunk });
                        options.serialize(&mut Serializer::with(&mut buf, StructMapWriter)).unwrap();

                        println!("{:?}", buf);

                        'inner: loop {
                            let mut _stream = stream.try_clone()
                                .expect("Failed to clone stream");

                            let rec = &buf[..];

                            match _stream.write_all(rec) {
                                Ok(_) => {
                                    _stream.flush().unwrap();
                                    for _ in 0..10  {
                                        let mut resp_buf = [0u8; 64];
                                        match _stream.read(&mut resp_buf) {
                                            Ok(sz) => {
                                                let mut de = Deserializer::new(&resp_buf[0..sz]);
                                                let resp: Response = Deserialize::deserialize(&mut de).unwrap();
                                                if resp.ack == ack {
                                                    break 'inner;
                                                }
                                            },
                                            Err(e) => {
                                                println!("Failed to read response {:?}", e);
                                            }
                                        };
                                    }

                                },
                                Err(e) => {
                                    // let _ = _stream.shutdown(Shutdown::Both);
                                    stream = Worker::connect(addr)
                                        .expect("Couldn't connect to the server...");
                                    println!("Failed to write record. {:?}", e);
                                    thread::sleep(500.millis());
                                    continue 'inner;
                                }
                            }
                        }
                        println!("Worker {} was done flushing entries.", id);
                    }
                };
            }

            if ! entry_queue.is_empty() {
                println!("Worker {} has {} entries left.", id, entry_queue.len());
            }
        });
        Worker { id, thread: Some(t) }
    }
}

fn main() {

    let pool = Arc::new(Mutex::new(Client::new(1)));

    let mut calls = Vec::new();

    for i in 0..5 {
        let pool = Arc::clone(&pool);
        let t = thread::spawn(move || {
            for _ in 0..200 {
                let mut age: u32 = i;
                let name = String::from("tkrs");
                let mut rng = rand::thread_rng();
                if rng.gen() {
                    age = rng.gen_range(0, 100);
                }
                thread::sleep(5.millis());

                let human = Human { age, name };

                let pool = pool.lock().unwrap();
                pool.send(human, 1500000000 + i);
            }
        });
        calls.push(t);
    }

    for c in calls {
        c.join().unwrap();
    }

    thread::sleep(10.seconds());
}
