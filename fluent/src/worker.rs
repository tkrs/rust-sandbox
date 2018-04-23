use base64;
use event_time::TimeConverter;
use rmp::encode;
use rmps;
use rmps::encode::StructMapWriter;
use rmps::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};
use uuid::Uuid;

pub struct Worker {
    pub id: usize,
    pub handler: Option<thread::JoinHandle<()>>,
}

impl Worker {
    pub fn new<A>(
        id: usize,
        addr: A,
        settings: Settings,
        receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
    ) -> Worker
    where
        A: ToSocketAddrs + Clone,
        A: Send + 'static,
    {
        let emitter = Emitter::new(id, addr, settings)
            .expect(&format!("Worker {} couldn't create emitter", id));

        let builder = thread::Builder::new().name(format!("fluent-worker-{}", id));
        let handler = builder
            .spawn(move || loop {
                let receiver = receiver.lock().expect("Receiver couldn't be locked.");

                let msg = receiver.recv().expect("Couldn't receive a message.");

                match msg {
                    Message::Queuing(tag, tm, v) => {
                        emitter.enqueue(tag, tm, v);
                    }
                    Message::Flush(sz) => {
                        emitter.flush(Some(sz));
                    }
                    Message::Terminate => {
                        emitter.flush(None);
                        break;
                    }
                };
            })
            .ok();

        Worker { id, handler }
    }
}

trait Flusher {
    fn emit<'a>(&self, buf: &'a [u8], ack_id: String) -> io::Result<()>;
    fn flush(&self, sz: Option<usize>) -> io::Result<()>;
}

struct Emitter {
    worker_id: usize,
    stream: RefCell<TcpStream>,
    queues: RefCell<HashMap<String, RefCell<Vec<(SystemTime, Vec<u8>)>>>>,
    settings: Settings,
}

impl Emitter {
    fn new<A>(worker_id: usize, addr: A, settings: Settings) -> io::Result<Emitter>
    where
        A: ToSocketAddrs + Clone,
        A: Send + 'static,
    {
        connect(worker_id, addr, settings).map(|stream| {
            let stream = RefCell::new(stream);
            let queues = RefCell::new(HashMap::new());
            Emitter {
                worker_id,
                stream,
                queues,
                settings,
            }
        })
    }

    fn enqueue(&self, tag: String, tm: SystemTime, v: Vec<u8>) {
        let mut queues = self.queues.borrow_mut();
        let mut queue = queues
            .entry(tag)
            .or_insert_with(|| RefCell::new(Vec::new()))
            .borrow_mut();
        queue.push((tm, v));
    }
}

impl Flusher for Emitter {
    fn flush(&self, sz: Option<usize>) -> io::Result<()> {
        for (tag, queue) in self.queues.borrow().iter() {
            let mut queue = queue.borrow_mut();
            if queue.is_empty() {
                continue;
            }
            let sz = sz.unwrap_or_else(|| queue.len());
            let mut entries = Vec::with_capacity(sz);
            queue.take(&mut entries);
            let chunk = base64::encode(&Uuid::new_v4().to_string());
            let mut buf = Vec::new();
            if let Err(e) = make_buffer(&mut buf, entries, tag.to_string(), chunk.clone()) {
                error!(
                    "Worker {} unexpected error occurred during making msgpack: {:?}.",
                    self.worker_id, e
                );
                continue;
            };
            debug!("Worker {} made the buffer: {:?}", self.worker_id, buf);
            let now = Instant::now();
            let mut write_retry_delay = self.settings.emit_retry_initial_delay;
            loop {
                if let Err(e) = self.emit(&buf[..], chunk.clone()) {
                    let mut stream = self.stream.borrow_mut();
                    let addr = stream.local_addr()?;
                    *stream = connect(self.worker_id, addr, self.settings)?;
                    error!(
                        "Worker {} unexpected error occurred during emitting buffer: {:?}.",
                        self.worker_id, e
                    );
                } else {
                    break;
                };
                if now.duration_since(Instant::now()) >= self.settings.emit_retry_timeout {
                    error!("???");
                    break;
                }
                thread::sleep(write_retry_delay);
                write_retry_delay = write_retry_delay + write_retry_delay;
                if write_retry_delay >= self.settings.emit_retry_max_delay {
                    write_retry_delay = self.settings.emit_retry_max_delay
                }
            }
        }
        Ok(())
    }

    fn emit<'a>(&self, buf: &'a [u8], ack_id: String) -> io::Result<()> {
        let mut stream = self.stream.borrow_mut();
        let mut stream = stream.try_clone()?;
        let rec = &buf[..];
        let _ = stream.write_all(rec)?;
        let mut resp_buf = [0u8; 64];
        let sz = stream.read(&mut resp_buf)?;
        let mut de = Deserializer::new(&resp_buf[0..sz]);
        let r: Result<Response, rmps::decode::Error> = Deserialize::deserialize(&mut de);
        match r {
            Ok(resp) => {
                if resp.ack != ack_id {
                    error!("Unmatched");
                }
            }
            Err(e) => {
                warn!(
                    "Worker {} failed to deserialize response: {:?}.",
                    self.worker_id, e
                );
            }
        }
        Ok(())
    }
}

fn connect<A>(id: usize, addr: A, settings: Settings) -> io::Result<TcpStream>
where
    A: ToSocketAddrs + Clone,
{
    let now = Instant::now();
    let mut retry_delay = settings.connection_retry_initial_delay;
    loop {
        let r = TcpStream::connect(addr.clone());
        match r {
            Ok(s) => {
                s.set_nodelay(true).unwrap();
                s.set_read_timeout(Some(settings.read_timeout)).unwrap();
                s.set_write_timeout(Some(settings.write_timeout)).unwrap();
                return Ok(s);
            }
            e => {
                if now.duration_since(Instant::now()) >= settings.connection_retry_timeout {
                    return e;
                }
                warn!("Worker {}, an error occurred {:?}", id, e);
                thread::sleep(retry_delay);
                if retry_delay >= settings.emit_retry_max_delay {
                    retry_delay = settings.emit_retry_max_delay;
                } else {
                    retry_delay = retry_delay + retry_delay;
                }
            }
        }
    }
}

fn make_buffer(
    buf: &mut Vec<u8>,
    entries: Vec<(SystemTime, Vec<u8>)>,
    tag: String,
    chunk: String,
) -> Result<(), rmps::encode::Error> {
    buf.push(0x93);
    encode::write_str(buf, tag.as_str())?;
    encode::write_array_len(buf, entries.len() as u32)?;
    for (t, entry) in entries {
        encode::write_array_len(buf, 2)?;
        t.event_time(buf)?;
        for elem in entry {
            buf.push(elem);
        }
    }
    let options = Some(Options { chunk });
    options.serialize(&mut Serializer::with(buf, StructMapWriter))
}

#[derive(Clone, Copy)]
pub struct Settings {
    pub connection_retry_initial_delay: Duration,
    pub connection_retry_max_delay: Duration,
    pub connection_retry_timeout: Duration,
    pub emit_retry_initial_delay: Duration,
    pub emit_retry_max_delay: Duration,
    pub emit_retry_timeout: Duration,
    pub write_timeout: Duration,
    pub read_timeout: Duration,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Options {
    chunk: String,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
struct Response {
    ack: String,
}

pub enum Message {
    Queuing(String, SystemTime, Vec<u8>),
    Flush(usize),
    Terminate,
}

trait Take<T> {
    fn take(&mut self, buf: &mut Vec<T>);
}

impl<T> Take<T> for Vec<T> {
    fn take(&mut self, buf: &mut Vec<T>) {
        let sz = buf.capacity();
        for _ in 0..sz {
            if self.is_empty() {
                return;
            }
            buf.push(self.remove(0));
        }
    }
}
