mod job;

use crate::array::DataChunk;

pub use crate::stream::job::Job;
pub use crate::stream::job::StreamRunningJob;

#[derive(Debug, PartialEq)]
pub enum Message {
    Chunk(DataChunk),
    Barrier(Barrier),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Barrier {
    pub epoch: u64,
    pub timestamp: u64,
}