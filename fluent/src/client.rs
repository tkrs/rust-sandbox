use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use tmc::DurationOpt;
use rmps::encode::StructMapWriter;
use rmps::Serializer;
use serde::Serialize;
use worker;
use message;

pub struct Client {
    workers: Vec<worker::Worker>,
    sender: mpsc::Sender<message::Message>,
}

impl Client {
    pub fn new(size: usize) -> Client {
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
                sender.send(message::Message::Flush).unwrap();
            });
        }

        Client {
            workers,
            sender: tx,
        }
    }
}

impl Client {
    pub fn send<A>(&self, tag: String, a: A, timestamp: u32)
    where
        A: Serialize + Send + 'static,
    {
        let mut buf = Vec::new();
        a.serialize(&mut Serializer::with(&mut buf, StructMapWriter))
            .unwrap();
        self.sender
            .send(message::Message::Incoming(tag, timestamp, buf))
            .unwrap();
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        println!("Sending terminate message to all workers.");

        for _ in &mut self.workers {
            let sender = self.sender.clone();
            sender.send(message::Message::Terminate).unwrap();
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
