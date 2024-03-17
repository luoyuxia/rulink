use futures_async_stream::try_stream;
use crate::connector::{StreamConnector};
use crate::executor::BoxedExecutor;
use crate::stream::Message;
use crate::executor::ExecuteError;

pub struct TableInsertExecutor {
    pub sink_connector: Box<dyn StreamConnector + Sync + Send>,
    pub child: BoxedExecutor,
}


impl TableInsertExecutor {

    #[try_stream(boxed, ok = Message, error = ExecuteError)]
    pub async fn execute(mut self) {
        #[for_await]
        for batch in self.child {
            match batch? {
                Message::Barrier(barrier) => {
                    self.sink_connector.on_receive_barrier(barrier.clone());
                    yield Message::Barrier(barrier.clone())
                },
                Message::Chunk(chunk) => {
                    self.sink_connector.write(chunk);
                }
            }
        }
    }
}