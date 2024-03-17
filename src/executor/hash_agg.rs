use std::collections::HashMap;
use futures_async_stream::try_stream;
use smallvec::SmallVec;
use crate::executor::BoxedExecutor;
use crate::planner::RecExpr;
use crate::types::{DataType, DataValue};
use crate::array::DataChunk;
use crate::stream::Message;
use crate::executor::ExecuteError;
use crate::executor::evaluator::Evaluator;
use crate::array::DataChunkBuilder;

const PROCESSING_WINDOW_SIZE: usize = 1024;

pub struct HashAggExecutor {
    pub child: BoxedExecutor,
    inner: ExecutorInner
}

pub struct ExecutorInner {
    pub aggs: RecExpr,
    pub group_keys: RecExpr,
    pub types: Vec<DataType>,
    pub state_entries: HashMap<GroupKeys, AggValue>,
}


pub type GroupKeys = SmallVec<[DataValue; 4]>;

pub type AggValue = SmallVec<[DataValue; 16]>;

impl ExecutorInner {

    pub fn new(aggs: RecExpr, group_keys: RecExpr, types: Vec<DataType>) -> Self{
        ExecutorInner {
            aggs,
            group_keys,
            types,
            state_entries: HashMap::new()
        }
    }

    #[try_stream(boxed, ok = Message, error = ExecuteError)]
    async fn execute_inner(&mut self, chunk: DataChunk) {
        let keys_chunk = Evaluator::new(&self.group_keys).eval_list(&chunk)?;
        let args_chunk = Evaluator::new(&self.aggs).eval_list(&chunk)?;

        let mut entries_to_flush = HashMap::new();

        for i in 0..chunk.cardinality() {
            let keys: GroupKeys = keys_chunk.row(i).values().collect();
            let states = self.state_entries
                .entry(keys.clone())
                .or_insert_with(|| Evaluator::new(&self.aggs).init_agg_states());
            Evaluator::new(&self.aggs).agg_list_append(states, args_chunk.row(i).values());
            entries_to_flush.insert(keys.clone(), states.clone());
        }


        let mut builder = DataChunkBuilder::new(&self.types, PROCESSING_WINDOW_SIZE);
        for (key, aggs) in entries_to_flush {
            let row = aggs.into_iter().chain(key.into_iter());
            if let Some(chunk) = builder.push_row(row) {
                yield Message::Chunk(chunk)
            }
        }
        if let Some(chunk) = builder.take() {
            yield Message::Chunk(chunk)
        }
    }
}

impl HashAggExecutor {

    pub fn new(aggs: RecExpr,
               group_keys: RecExpr,
               types: Vec<DataType>, child: BoxedExecutor) -> Self {
        HashAggExecutor {
            child,
            inner: ExecutorInner::new(
                aggs,
                group_keys,
                types)
        }
    }


    #[try_stream(boxed, ok = Message, error = ExecuteError)]
    pub async fn execute(self) {
        let HashAggExecutor {
            child,
            inner: mut this
        } = self;
        #[for_await]
        for chunk in child {
            let chunk = chunk?;
            match chunk {
                Message::Barrier(barrier) => {
                    yield Message::Barrier(barrier)
                },
                Message::Chunk(chunk) => {
                    #[for_await]
                    for chunk in this.execute_inner(chunk) {
                        yield chunk?
                    }
                }
            }
        }
    }
}