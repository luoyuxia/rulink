use futures_async_stream::try_stream;
use crate::array::data_chunk::DataChunk;
use crate::binder::CreateTable;
use super::*;
use crate::stream::Message;

pub struct CreateTableExecutor {
    pub plan: CreateTable,
    pub catalog: CatalogRef
}

impl CreateTableExecutor {

    #[try_stream(boxed, ok = Message, error = ExecuteError)]
    pub async fn execute(self) {
        let schema = self.catalog.get_schema(self.plan.schema_id).unwrap();
        let table_id = schema.add_table(&self.plan.table_name).unwrap();
        let table = schema.get_table(table_id).unwrap();
        let mut column_descs = vec![];
        for (name, desc) in &self.plan.columns {
            table.add_column(name, desc.clone()).unwrap();
            column_descs.push(desc.clone());
        }
        table.add_options(self.plan.options);
        yield Message::Chunk(DataChunk::no_column());
    }
}


