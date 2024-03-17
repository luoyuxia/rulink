use futures::future::Either;
use futures::stream::{PollNext, select_with_strategy, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use tokio::sync::mpsc::UnboundedReceiver;
use crate::executor::ExecuteError;
use crate::stream::{Barrier, Message};
use crate::connector::StreamConnector;
use futures::TryStreamExt;

pub struct TableScanExecutor {
    pub data_source: Box<dyn StreamConnector + Sync + Send>,
    pub rx: Option<UnboundedReceiver<Barrier>>
}

impl TableScanExecutor {
    #[try_stream(boxed, ok = Message, error = ExecuteError)]
    pub async fn execute(mut self) {
        let control_message_stream = Self::barrier_to_message_stream(self.rx.take().unwrap())
            .map_ok(Either::Left).boxed();
        let data_message_stream = self.data_source.read().map_ok(Either::Right).boxed();
        let strategy = |_: &mut PollNext| PollNext::Left;
        let mut stream = select_with_strategy(control_message_stream, data_message_stream, strategy)
            .boxed();

        while let Some(msg) = stream.next().await {
            match msg? {
                Either::Left(msg) => {
                    match msg {
                        Message::Barrier(_barrier) => {
                            yield Message::Barrier(_barrier);
                        }
                        _ => {}
                    }
                },
                Either::Right(msg) => {
                    yield msg;
                }
            }
        }
    }


    /// Receive barriers from barrier manager with the channel, error on channel close.
    #[try_stream(ok = Message, error = ExecuteError)]
    pub async fn barrier_to_message_stream(mut rx: UnboundedReceiver<Barrier>) {
        while let Some(barrier) = rx.recv().await {
            yield Message::Barrier(barrier);
        }
    }
}