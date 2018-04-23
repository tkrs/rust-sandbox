use rmps::encode::StructMapWriter;
use rmps::Serializer;
use serde::Serialize;
use std::io;
use std::net::ToSocketAddrs;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
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
    pub fn new<A>(addr: A) -> io::Result<WorkerPool>
    where
        A: ToSocketAddrs + Clone,
        A: Send + 'static,
    {
        WorkerPool::with_settings(addr, Default::default())
    }
    pub fn with_settings<A>(addr: A, settings: Settings) -> io::Result<WorkerPool>
    where
        A: ToSocketAddrs + Clone,
        A: Send + 'static,
    {
        assert!(settings.workers > 0);

        let mut workers = Vec::with_capacity(settings.workers);

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));

        for id in 0..settings.workers {
            let settings = worker::Settings {
                connection_retry_initial_delay: settings.connection_retry_initial_delay,
                connection_retry_max_delay: settings.connection_retry_max_delay,
                connection_retry_timeout: settings.connection_retry_timeout,
                write_timeout: settings.write_timeout,
                emit_retry_initial_delay: settings.emit_retry_initial_delay,
                emit_retry_max_delay: settings.emit_retry_max_delay,
                emit_retry_timeout: settings.emit_retry_timeout,
                read_timeout: settings.read_timeout,
            };
            let wkr = worker::Worker::new(id, addr.clone(), settings, Arc::clone(&receiver));
            workers.push(wkr);
        }

        {
            // TODO: Consider a messaging way to all workers.
            let sender = sender.clone();
            let builder = thread::Builder::new().name(format!("fluent-client-flush-sender"));
            builder.spawn(move || loop {
                thread::sleep(settings.flush_period);
                sender
                    .send(worker::Message::Flush(settings.max_flush_entries))
                    .unwrap();
            })?;
        }

        Ok(WorkerPool { workers, sender })
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
            .send(worker::Message::Queuing(tag, timestamp, buf))
            .unwrap();
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        debug!("Sending terminate message to all workers.");

        for _ in &mut self.workers {
            let sender = self.sender.clone();
            sender.send(worker::Message::Terminate).unwrap();
        }

        debug!("Shutting down all workers.");

        for wkr in &mut self.workers {
            debug!("Shutting down worker {}", wkr.id);

            if let Some(w) = wkr.handler.take() {
                w.join().unwrap();
            }
        }
    }
}

#[derive(Clone)]
pub struct Settings {
    pub workers: usize,
    pub flush_period: Duration,
    pub max_flush_entries: usize,
    pub connection_retry_initial_delay: Duration,
    pub connection_retry_max_delay: Duration,
    pub connection_retry_timeout: Duration,
    pub write_timeout: Duration,
    pub emit_retry_initial_delay: Duration,
    pub emit_retry_max_delay: Duration,
    pub emit_retry_timeout: Duration,
    pub read_timeout: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            workers: 1,
            flush_period: 500.millis(),
            max_flush_entries: 500,
            connection_retry_initial_delay: 50.millis(),
            connection_retry_max_delay: 5.seconds(),
            connection_retry_timeout: 60.seconds(),
            emit_retry_initial_delay: 5.millis(),
            emit_retry_max_delay: 5.seconds(),
            emit_retry_timeout: 10.seconds(),
            write_timeout: 1.seconds(),
            read_timeout: 1.seconds(),
        }
    }
}
