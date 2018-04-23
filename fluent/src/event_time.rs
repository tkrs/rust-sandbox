use rmp::encode;
use rmp::encode::ValueWriteError;
use std::time::{SystemTime, UNIX_EPOCH};

pub trait TimeConverter {
    fn event_time(&self, buf: &mut Vec<u8>) -> Result<(), ValueWriteError>;
}

impl TimeConverter for SystemTime {
    fn event_time(&self, buf: &mut Vec<u8>) -> Result<(), ValueWriteError> {
        encode::write_ext_meta(buf, 8, 0x00)?;
        let d = self.duration_since(UNIX_EPOCH).unwrap();
        encode::write_u32(buf, d.as_secs() as u32)?;
        encode::write_u32(buf, d.subsec_nanos())
    }
}
