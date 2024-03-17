use futures_async_stream::try_stream;
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::connector::StreamConnector;
use crate::executor::PROCESSING_WINDOW_SIZE;
use crate::planner::RecExpr;
use crate::stream::{Barrier, Message};
use crate::types::DataType;
use crate::executor::evaluator::Evaluator;
use crate::connector::ExecuteError;

pub struct ValueConnector {
    pub column_types: Vec<DataType>,
    /// Each row is composed of multiple values,
    /// each value is represented by an expression.
    pub values: Vec<Vec<RecExpr>>,
}


impl StreamConnector for ValueConnector {
    #[try_stream(boxed, ok = Message, error = ExecuteError)]
    async fn read(&self) {
        let dummy = DataChunk::single(0);
        for chunk in self.values.chunks(PROCESSING_WINDOW_SIZE) {
            let chunk1 =
                self.column_types.iter().enumerate().map(
                    |(col_idx, col_type)| {
                        let mut builder = ArrayBuilderImpl::with_capacity(chunk.len(), col_type);
                        for row in chunk {
                            let value = Evaluator::new(row.get(col_idx).unwrap()).eval(&dummy).unwrap().get(0);
                            builder.push(&value);
                        }
                        builder
                    }
                ).map(|builder| builder.finish())
                    .collect::<DataChunk>();
            yield Message::Chunk(chunk1);
        }
    }


    fn write(&mut self, chunk: DataChunk) {
        todo!()
    }

    fn on_receive_barrier(&mut self, barrier: Barrier) {
        todo!()
    }
}