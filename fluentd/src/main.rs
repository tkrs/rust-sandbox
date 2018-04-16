extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate rmp;
extern crate rmp_serde as rmps;
extern crate rand;

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::collections::VecDeque;
use std::io::Write;
use std::net::TcpStream;
use rand::Rng;

use serde::Serialize;
use rmps::Serializer;
use rmps::encode::StructMapWriter;

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Human {
    age: u32,
    name: String
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Message<A>(String, u32, A) where A: Serialize + Clone, A: Send + 'static;
impl <A> Message<A> where A: Serialize + Clone, A: Send + 'static {
    pub fn new(tag: String, time: u32, a: A) -> Message<A>{
        Message(tag, time, a)
    }
}

fn consume(stream: TcpStream, msgs: &Arc<Mutex<VecDeque<Vec<u8>>>>) {
    let msgs = Arc::clone(msgs);
    thread::spawn(move || {
        let mut millis = 5;
        let mut count = 10;
        while count > 0 {
            let mut stream = stream.try_clone().unwrap();
            let mut q = msgs.lock().unwrap();
            if let Some(msg) = q.remove(0) {
                // println!("{:?}", msg);
                stream.write(&msg[..]).unwrap();
                millis = 50;
                count = 10;
            } else {
                millis *= 2;
                count -= 1;
                thread::sleep(Duration::from_millis(millis))
            }
        }
    });
}

fn emit(msgs: &Arc<Mutex<VecDeque<Vec<u8>>>>) {
    for i in 0..100 {
        let msgs = Arc::clone(msgs);
        let tname = format!("thread{}", i);
        thread::Builder::new().name(tname.clone()).spawn(move || {
            let mut rng = rand::thread_rng();
            if rng.gen() {
                let n = rng.gen::<u8>();
                thread::sleep(Duration::from_millis(n as u64));
            }
            let mut q = msgs.lock().unwrap();
            let name = thread::current().name().unwrap().to_string();
            let msg = Message("test".to_string(), 1500000000, Human { age: i, name });
            let mut buf = Vec::new();
            msg.serialize(&mut Serializer::with(&mut buf, StructMapWriter)).unwrap();
            q.push_back(buf);
        }).expect(&format!("Thread '{}' is not started!", tname));
    }

}

fn main() {

    let stream = TcpStream::connect("127.0.0.1:24224")
            .expect("Couldn't connect to the server...");
    let mut msgs: Arc<Mutex<VecDeque<Vec<u8>>>> = Arc::new(Mutex::new(VecDeque::new()));

    consume(stream, &mut msgs);
    emit(&mut msgs);

    thread::sleep(Duration::from_secs(3));
}
