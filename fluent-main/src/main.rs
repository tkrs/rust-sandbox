extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate fluent;
extern crate rand;
extern crate tmc;

use rand::Rng;
use std::time::SystemTime;
use std::sync::{Arc, Mutex};
use std::thread;
use tmc::DurationOpt;

use fluent::client::{Client, WorkerPool};

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Human {
    age: u32,
    name: String,
}

fn main() {
    let pool = Arc::new(Mutex::new(WorkerPool::new(3)));

    let mut calls = Vec::new();

    for i in 0..5 {
        let pool = Arc::clone(&pool);
        let t = thread::spawn(move || {
            let mut rng = rand::thread_rng();
            for _ in 0..200 {
                let name = String::from("tkrs");
                let age: u32 = if rng.gen() {
                    rng.gen_range(0, 100)
                } else {
                    i
                };

                let tag = format!("test.human.{}", i);
                let a = Human { age, name };
                let timestamp = SystemTime::now();

                thread::sleep(10.millis());
                let pool = pool.lock().expect("Client couldn't be locked.");
                pool.send(tag, a, timestamp);
            }
        });
        calls.push(t);
    }

    for c in calls {
        c.join().expect("Couldn't join on the associated thread");
    }
}
