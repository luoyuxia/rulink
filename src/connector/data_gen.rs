use std::sync::Arc;
use futures_async_stream::try_stream;
use crate::catalog::{ColumnId, TableCatalog};
use crate::connector::StreamConnector;
use crate::stream::{Barrier, Message};
use crate::executor::ExecuteError;
use std::time::Duration;
use crate::types::DataTypeKind;
use crate::array::ArrayBuilderImpl;
use crate::array::DataChunk;
use crate::types::DataValue;

pub struct DataGenSource {
    pub column_ids: Vec<ColumnId>,
    pub table: Arc<TableCatalog>,
}


impl StreamConnector for DataGenSource {

    #[try_stream(boxed, ok = Message, error = ExecuteError)]
    async fn read(&self) {
        let defaultSize = 10;
        for iter in 1..100 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let mut result = vec![];
            for col_id in self.column_ids.iter() {
                let col = self.table.get_column(*col_id);
                let data_type = col.unwrap().datatype();
                match data_type.kind() {
                    DataTypeKind::Int32 => {
                        let mut builder =
                            ArrayBuilderImpl::with_capacity(defaultSize, &data_type);
                        for i in 0..defaultSize {
                            let value :i32 = i.try_into().unwrap();
                            builder.push(&DataValue::Int32(value + (*col_id as i32)));
                        }
                        result.push(builder)
                    },
                    _ => todo!()
                }
            }
            yield Message::Chunk(result.into_iter().map(|builder| builder.finish())
                .collect::<DataChunk>());
        }
    }

    fn write(&mut self, chunk: DataChunk) {
        unimplemented!()
    }

    fn on_receive_barrier(&mut self, barrier: Barrier) {
        todo!()
    }
}