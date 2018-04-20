extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate rand;
extern crate fluent;
extern crate tmc;

use rand::Rng;
use std::sync::{Arc, Mutex};
use std::thread;
use tmc::DurationOpt;

use fluent::Client;

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Human {
    age: u32,
    name: String
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

                let human = Human { age, name };

                thread::sleep(10.millis());
                let pool = pool.lock().unwrap();
                pool.send(human, 1500000000 + i);
            }
        });
        calls.push(t);
    }

    for c in calls {
        c.join().unwrap();
    }
}
