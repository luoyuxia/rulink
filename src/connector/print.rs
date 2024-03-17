use std::sync::Arc;
use futures_async_stream::try_stream;
use crate::array::DataChunk;
use crate::catalog::{ColumnId, TableCatalog};
use crate::connector::StreamConnector;
use crate::connector::ExecuteError;
use crate::stream::Barrier;
use crate::connector::Message;

pub struct Print {
    pub column_ids: Vec<ColumnId>,
    pub table: Arc<TableCatalog>,
}


impl StreamConnector for Print {
    #[try_stream(boxed, ok = Message, error = ExecuteError)]
    async fn read(&self) {
        unimplemented!()
    }

    fn write(&mut self, chunk: DataChunk) {
        println!("{}", chunk);
    }

    fn on_receive_barrier(&mut self, barrier: Barrier) {
        // do nothing
    }
}