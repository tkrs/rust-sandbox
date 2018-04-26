extern crate poston;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate tmc;

use rand::Rng;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Instant, SystemTime};
use tmc::DurationOpt;

use poston::client::{Client, WorkerPool, Settings};

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Human {
    age: u32,
    name: String,
}

fn main() {
    let addr = "127.0.0.1:24224".to_string();
    // let pool = WorkerPool::new(&"127.0.0.1:24224".to_string()).expect("Couldn't create the worker pool.");
    let settins = Settings {
        workers: 4,
        flush_period: 64.millis(),
        max_flush_entries: 2048,
        ..Default::default()
    };
    let pool = WorkerPool::with_settings(&addr, &settins)
        .expect("Couldn't create the worker pool.");
    let pool = Arc::new(Mutex::new(pool));

    let mut calls = Vec::new();

    let start = Instant::now();

    for i in 0..4 {
        let pool = Arc::clone(&pool);
        let t = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            for _ in 0..25000 {
                let name = String::from("tkrs");
                let age: u32 =
                    if rng.gen() { rng.gen_range(0, 100) } else { i };

                let tag = format!("test.human.{}", i);
                let a = Human { age, name };
                let timestamp = SystemTime::now();

                // thread::sleep(10.millis());
                let pool = pool.lock().expect("Client couldn't be locked.");
                pool.send(tag, a, timestamp).unwrap();
            }
        });
        calls.push(t);
    }

    for c in calls {
        c.join().expect("Couldn't join on the associated thread");
    }

    drop(pool);

    println!("{}", start.elapsed().subsec_nanos());
}
