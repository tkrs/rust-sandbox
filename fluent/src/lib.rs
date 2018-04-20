// Referred to https://doc.rust-lang.org/book/second-edition/ch20-05-sending-requests-via-channels.html

extern crate base64;
extern crate serde;
extern crate uuid;
#[macro_use]
extern crate serde_derive;
extern crate rmp;
extern crate rmp_serde as rmps;
extern crate tmc;

pub mod client;

mod message;
mod worker;
