pub enum Message {
    Incoming(String, u32, Vec<u8>),
    Flush,
    Terminate,
}

