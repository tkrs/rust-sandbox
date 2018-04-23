use rmps::encode::StructMapWriter;
use rmps::Serializer;
use serde::Serialize;
use std::time::SystemTime;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use tmc::DurationOpt;
use worker;

pub trait Client {

    fn send<A>(&self, tag: String, a: A, timestamp: SystemTime)
        where
            A: Serialize + Send + 'static;
}

pub struct WorkerPool {
    workers: Vec<worker::Worker>,
    sender: mpsc::Sender<worker::Message>,
}

impl WorkerPool {
    pub fn new(size: usize) -> WorkerPool {
        assert!(size > 0);

        let mut workers = Vec::with_capacity(size);

        let (tx, rx) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(rx));

        for id in 0..size {
            let wkr = worker::Worker::new(id, Arc::clone(&receiver));
            workers.push(wkr);
        }

        {
            let sender = tx.clone();
            thread::spawn(move || loop {
                thread::sleep(20.millis());
                sender.send(worker::Message::Flush).unwrap();
            });
        }

        WorkerPool {
            workers,
            sender: tx,
        }
    }
}

impl Client for WorkerPool {
    fn send<A>(&self, tag: String, a: A, timestamp: SystemTime)
    where
        A: Serialize + Send + 'static,
    {
        let mut buf = Vec::new();
        a.serialize(&mut Serializer::with(&mut buf, StructMapWriter))
            .unwrap();
        self.sender
            .send(worker::Message::Incoming(tag, timestamp, buf))
            .unwrap();
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        println!("Sending terminate message to all workers.");

        for _ in &mut self.workers {
            let sender = self.sender.clone();
            sender.send(worker::Message::Terminate).unwrap();
        }

        println!("Shutting down all workers.");

        for wkr in &mut self.workers {
            println!("Shutting down worker {}", wkr.id);

            if let Some(w) = wkr.thread.take() {
                w.join().unwrap();
            }
        }
    }
}
