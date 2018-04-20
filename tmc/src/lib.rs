use std::time::Duration;

pub trait DurationOpt {
    fn seconds(&self) -> Duration;
    fn millis(&self) -> Duration;
    // fn nanos(&self) -> Duration;
}

impl DurationOpt for i32 {
    fn seconds(&self) -> Duration {
        Duration::from_secs(*self as u64)
    }
    fn millis(&self) -> Duration {
        Duration::from_millis(*self as u64)
    }
    // fn nanos(&self) -> Duration { Duration::from_nanos(*self as u64) }
}
impl DurationOpt for u32 {
    fn seconds(&self) -> Duration {
        Duration::from_secs(*self as u64)
    }
    fn millis(&self) -> Duration {
        Duration::from_millis(*self as u64)
    }
    // fn nanos(&self) -> Duration { Duration::from_nanos(*self as u64) }
}
