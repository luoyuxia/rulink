use futures_async_stream::try_stream;
use crate::executor::BoxedExecutor;
use crate::stream::{Message};
use crate::executor::ExecuteError;
use crate::executor::evaluator::Evaluator;
use crate::planner::RecExpr;

/// The executor of project operation.
pub struct ProjectionExecutor {
    pub exprs: RecExpr,
    pub child: BoxedExecutor,
}

impl ProjectionExecutor {

    #[try_stream(boxed, ok = Message, error = ExecuteError)]
    pub async fn execute(self) {
        #[for_await]
        for batch in self.child {
            let batch = batch?;
            match batch {
                Message::Barrier(barrier) => {
                    yield Message::Barrier(barrier)
                },
                Message::Chunk(_chunk) => {
                    let chunk = Evaluator::new(&self.exprs)
                        .eval_list(&_chunk)?;
                    yield Message::Chunk(chunk)
                }
            }
        }
    }
}
