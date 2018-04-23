// Referred to https://doc.rust-lang.org/book/second-edition/ch20-05-sending-requests-via-channels.html

extern crate base64;
#[macro_use]
extern crate log;
extern crate rmp;
extern crate rmp_serde as rmps;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate tmc;
extern crate uuid;

pub mod client;

mod event_time;
mod worker;
