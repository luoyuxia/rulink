use std::sync::{Arc, Mutex};
use futures_async_stream::try_stream;
use crate::checkpoint::BarrierManager;
use crate::executor::BoxedExecutor;
use crate::stream::{Barrier, Message};
use crate::executor::ExecuteError;
use crate::array::DataChunk;

pub struct WrapExecutor {
    executor: BoxedExecutor,
    actor_id: u32,
    barrier_manager: Arc<Mutex<BarrierManager>>,
}

impl WrapExecutor {

    pub fn new(executor: BoxedExecutor, actor_id: u32, barrier_manager: Arc<Mutex<BarrierManager>>) -> Self {
         WrapExecutor {
            executor,
            actor_id,
            barrier_manager
         }
    }

    #[try_stream(boxed, ok = Message, error = ExecuteError)]
    pub async fn execute(self) {
        #[for_await]
        for batch in self.executor {
            let batch = batch?;
            match batch {
                Message::Barrier(barrier) => {
                    // // notify the barrier manager
                    self.barrier_manager.lock().unwrap()
                        .notify_barrier_complete(barrier.epoch, self.actor_id);
                    yield Message::Barrier(barrier);
                },
                Message::Chunk(_chunk) => {
                    yield Message::Chunk(_chunk);
                }
            }
        }
    }
}

pub trait StreamExecutor {

    fn process_data_chunk(&self, data_chunk: DataChunk) -> DataChunk;

    fn process_barrier(&self, barrier: Barrier);
}