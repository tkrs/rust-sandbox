// See https://doc.rust-lang.org/book/second-edition/ch20-05-sending-requests-via-channels.html

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
use std::net::{ToSocketAddrs, TcpStream, Shutdown};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use uuid::Uuid;
use serde::{Deserialize, Serialize};
use rmps::Serializer;
use rmps::encode::StructMapWriter;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Options {
    chunk: String
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Record<A>(String, u32, A, Option<Options>) where A: Serialize + Clone, A: Send + 'static;
impl <A> Record<A> where A: Serialize + Clone, A: Send + 'static {
    pub fn new(tag: String, time: u32, a: A, options: Options) -> Record<A>{
        Record(tag, time, a, Some(options))
    }
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
    sender: mpsc::Sender<Message>
}

impl Client {
    fn new(size: usize) -> Client {
        assert!(size > 0);

        let mut workers = Vec::with_capacity(size);

        let (tx, rx) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(rx));

        for id in 0..size {
            let worker = Worker::new(id, Arc::clone(&receiver));
            workers.push(worker);
        }

        Client {
            workers,
            sender: tx
        }
    }
}

impl Client {
    pub fn send(&self, r: Vec<u8>) {
        self.sender.send(Message::Incoming(r)).unwrap();
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        println!("Sending terminate message to all workers.");

        for _ in &mut self.workers {
            self.sender.send(Message::Terminate).unwrap();
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
    Incoming(Vec<u8>),
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
            s.set_read_timeout(Some(3.seconds()));
            s.set_write_timeout(Some(3.seconds()));
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
                        s.set_read_timeout(Some(3.seconds()));
                        s.set_write_timeout(Some(3.seconds()));
                        s
                    });
                }
            }
        }

        r

//        match r {
//            Ok(s) => {
//                match s.connect_complete() {
//                    Err(e) => Err(e),
//                    Ok(_) => Ok(s)
//                }
//            },
//            Err(e) => Err(e)
//        }
    }

    fn new(id: usize, reciever: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        let t = thread::spawn(move || {
            let addr = "127.0.0.1:24224";

            let mut stream = Worker::connect(addr)
                .expect("Couldn't connect to the server...");

            loop {
                let receiver = reciever.lock()
                    .expect("Failed to lock receiver");

                let msg = receiver.recv()
                    .expect("Failed to receive job");

                match msg {
                    Message::Terminate => {
                        println!("Worker {} was told to terminate.", id);

                        break
                    },
                    Message::Incoming(record) => {
                        println!("Worker {} got a record: {:?}.", id, record);

                        'inner: loop {
                            let mut _stream = stream.try_clone()
                                .expect("Failed to clone stream");

                            let rec = &record[..];

                            match stream.write_all(rec) {
                                Ok(_) => {
                                    // TODO: Read ACK ID
                                    let mut response = String::new();
                                    stream.read_to_string(&mut response).unwrap();

                                    println!("Response {:?}", response);
                                    break 'inner;
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
                    }
                };
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
                let mut rng = rand::thread_rng();
                if rng.gen() {
                    age = rng.gen_range(0, 100);
                }
                thread::sleep(age.millis());
                let chunk = base64::encode(&Uuid::new_v4().to_string());
                let msg = Record::new(
                    "test.human".to_string(),
                    1500000000 + i,
                    Human { age, name: "tkrs".to_string() },
                    Options { chunk }
                );
                let mut buf = Vec::new();
                msg.serialize(&mut Serializer::with(&mut buf, StructMapWriter)).unwrap();
                let pool = pool.lock().unwrap();
                pool.send(buf);
            }
        });
        calls.push(t);
    }

    for c in calls {
        c.join().unwrap();
    }

    thread::sleep(10.seconds());
}
