use futures_async_stream::try_stream;
use crate::catalog::{CatalogRef};
use crate::stream::Message;
use crate::executor::ExecuteError;
use crate::array::DataChunk;
use crate::binder::{BoundDrop, Object};

pub struct DropExecutor {
    pub plan: BoundDrop,
    pub catalog: CatalogRef
}

impl DropExecutor {

    #[try_stream(boxed, ok = Message, error = ExecuteError)]
    pub async fn execute(self) {
        match self.plan.object {
            Object::Table(table_ref) => {
                self.catalog.drop_table(table_ref);
            }
        }
        yield Message::Chunk(DataChunk::no_column());
    }
}