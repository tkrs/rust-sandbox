use std::cell::RefCell;
use std::io;
use std::io::ErrorKind;
use std::io::Write;
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

fn main() {
    let mut writer = ReconnectableWriter::connect("127.0.0.1:24224".to_string())
        .expect("Failed to connect to server");
    for _ in 0..10000 {
        let msg = [
            146, 168, 116, 97, 103, 46, 110, 97, 109, 101, 147, 146, 206, 85, 236, 230, 248, 129,
            167, 109, 101, 115, 115, 97, 103, 101, 163, 102, 111, 111, 146, 206, 85, 236, 230, 249,
            129, 167, 109, 101, 115, 115, 97, 103, 101, 163, 98, 97, 114, 146, 206, 85, 236, 230,
            250, 129, 167, 109, 101, 115, 115, 97, 103, 101, 163, 98, 97, 122,
        ];
        writer.write(&msg).unwrap();
        thread::sleep(Duration::from_millis(1));
    }
}

struct ReconnectableWriter<W: Write + Reconnect> {
    writer: W,
}

impl ReconnectableWriter<Stream> {
    fn connect(addr: String) -> io::Result<ReconnectableWriter<Stream>> {
        let tcp_stream = TcpStream::connect(addr.clone())?;
        let stream = RefCell::new(tcp_stream);
        let writer = Stream { addr, stream };
        Ok(ReconnectableWriter { writer })
    }
}

impl<W> ReconnectableWriter<W>
where
    W: Write + Reconnect,
{
    fn write<'a>(&mut self, msg: &'a [u8]) -> io::Result<()> {
        loop {
            match self.writer.write_all(&msg) {
                Ok(_) => break Ok(()),
                Err(e) => {
                    println!("Write error found {:?}.", e);
                    match e.kind() {
                        ErrorKind::BrokenPipe
                        | ErrorKind::ConnectionRefused
                        | ErrorKind::ConnectionAborted => loop {
                            println!("Try reconnect.");
                            match self.writer.reconnect() {
                                Ok(_) => {
                                    println!("Reconnected!");
                                    break;
                                }
                                Err(e) => {
                                    println!("Reconnect error found {:?}.", e);
                                }
                            }
                            thread::sleep(Duration::from_millis(50));
                        },
                        ErrorKind::TimedOut => {
                            thread::sleep(Duration::from_millis(20));
                            continue;
                        }
                        k => println!("unexpected error caused. {:?}", k),
                    }
                }
            }
        }
    }
}

struct Stream {
    addr: String,
    stream: RefCell<TcpStream>,
}

trait Reconnect {
    fn reconnect(&mut self) -> io::Result<()>;
}

impl Reconnect for Stream {
    fn reconnect(&mut self) -> io::Result<()> {
        let stream = TcpStream::connect(self.addr.clone())?;
        println!("connect");
        *self.stream.borrow_mut() = stream;
        Ok(())
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.borrow_mut().flush()
    }
}
