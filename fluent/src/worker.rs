use emitter::Emitter;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Read;
use std::net::ToSocketAddrs;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::SystemTime;
use stream::{Stream, ReconnectableWriter, ConnectionSettings};

pub struct Worker {
    pub id: usize,
    pub handler: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub fn new<A>(
        id: usize,
        addr: A,
        conn_settings: ConnectionSettings,
        receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
    ) -> Worker
    where
        A: ToSocketAddrs + Clone,
        A: Send + 'static,
    {
        let mut stream = Stream::connect(addr, conn_settings).expect(&format!("Worker {} couldn't connect to server.", id));

        let emitters = RefCell::new(HashMap::new());
        let wh = WorkerHandler { emitters };

        let builder = thread::Builder::new().name(format!("fluent-worker-pool-{}", id));
        let handler = builder
            .spawn(move || loop {
                let receiver = receiver.lock().expect("Receiver couldn't be locked.");
                let msg = receiver.recv().expect("Couldn't receive a message.");
                match msg {
                    Message::Queuing(tag, tm, msg) => wh.push(tag, tm, msg),
                    Message::Flush(sz) => {
                        wh.flush(&mut stream, Some(sz));
                    }
                    Message::Terminate => {
                        wh.flush(&mut stream, None);
                        break;
                    }
                };
            })
            .ok();

        Worker { id, handler }
    }
}

struct WorkerHandler {
    emitters: RefCell<HashMap<String, Emitter>>,
}

impl WorkerHandler {
    fn push(&self, tag: String, tm: SystemTime, msg: Vec<u8>) {
        let mut emitters = self.emitters.borrow_mut();
        let emitter = emitters
            .entry(tag.clone())
            .or_insert_with(|| Emitter::new(tag));
        emitter.push((tm, msg));
    }

    fn flush<RW>(&self, w: &mut RW, size: Option<usize>)
    where
        RW: ReconnectableWriter + Read,
    {
        for (tag, emitter) in self.emitters.borrow().iter() {
            if let Err(e) = emitter.emit(w, size) {
                error!("A tag: {} unexpected error occurred during emitting message; cause: {:?}.", tag, e);
            }
        }
    }
}

pub enum Message {
    Queuing(String, SystemTime, Vec<u8>),
    Flush(usize),
    Terminate,
}
