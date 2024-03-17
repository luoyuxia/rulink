use std::collections::HashMap;
use std::fs::File;
use std::io::{Write};
use std::sync::Arc;
use futures_async_stream::try_stream;
use crate::catalog::{ColumnId, TableCatalog};
use crate::connector::StreamConnector;
use crate::stream::{Barrier, Message};
use crate::connector::ExecuteError;
use prettytable::csv::Writer;
use std::io::BufRead;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::types::{DataType, DataTypeKind, DataValue};
use std::string::String;


pub struct FileSystemConnector {
    pub column_ids: Vec<ColumnId>,
    pub table: Arc<TableCatalog>,
    path: String,
    writer: Option<Writer<File>>,
}

impl FileSystemConnector {
    pub fn new_source(column_ids: Vec<ColumnId>,
               table: Arc<TableCatalog>, options: HashMap<String, String>) -> FileSystemConnector {
        FileSystemConnector {
            column_ids,
            table,
            path: options.get("path").unwrap().clone(),
            writer: None,
        }
    }

    pub fn new_sink(column_ids: Vec<ColumnId>,
                    table: Arc<TableCatalog>, options: HashMap<String, String>) -> FileSystemConnector {
        FileSystemConnector {
            column_ids,
            table,
            path: options.get("path").unwrap().clone(),
            writer: Some(Writer::from_path(options.get("path").unwrap().clone()).expect("s")),
        }
    }
}

impl FileSystemConnector {

    fn build_chunk_from_line(&self, line: Vec<String>) -> DataChunk {
        let mut result = vec![];
        // row1: [col1, col2, xxx]
        // row2: [col1, col2, xxx]
        let rows: Vec<_> = line.into_iter().map(|line| line.split_terminator(",")
            .map(|value| value.to_string())
            .into_iter().collect::<Vec<_>>())
            .collect();
        for (col_index, col_id) in self.column_ids.iter().enumerate() {
            let data_type = self.table.get_column(*col_id)
                .unwrap().datatype();
            result.push(FileSystemConnector::column_value_builder(data_type, col_index, &rows))
        }
        result.into_iter().map(|builder| builder.finish())
            .collect::<DataChunk>()
    }

    fn column_value_builder(data_type: DataType,
                            col_index: usize,
                            rows: &Vec<Vec<String>>) -> ArrayBuilderImpl {
        let rows_count = rows.len();
        let mut builder =
            ArrayBuilderImpl::with_capacity(rows_count, &data_type);
        match data_type.kind() {
            DataTypeKind::Int32 => {
                for row_index in 0 .. rows.len()  {
                    let item_value = rows.get(row_index)
                        .unwrap().get(col_index).unwrap();
                    builder.push(&DataValue::Int32(item_value.parse().expect("Failed to parse string to int32")))
                }
            },
            DataTypeKind::String => {
                let mut builder =
                    ArrayBuilderImpl::with_capacity(rows_count, &data_type);
                for row_index in 0 .. rows.len() {
                    let item_value = rows.get(row_index)
                        .unwrap().get(col_index).unwrap();
                    builder.push_str(item_value).expect("Fail to push build chunk");
                }
            }
            _ => todo!()
        }
        builder
    }
}


impl StreamConnector for FileSystemConnector {

    #[try_stream(boxed, ok = Message, error = ExecuteError)]
    async fn read(&self) {
        let chunk_size = 10;
        let file = tokio::fs::File::open(self.path.as_str()).await.expect("Fail to open file ");
        // create reader using file
        let reader = BufReader::new(file);
        // get iterator over lines
        let mut lines = reader.lines();
        let mut rows = vec![];
        loop {
            match lines.next_line().await.expect("Fail to get next line") {
                Some(line) => {
                    rows.push(line);
                    if rows.len() > chunk_size {
                        yield Message::Chunk(self.build_chunk_from_line(rows));
                        rows = vec![];
                    }
                }
                None => {
                    // No new line available; wait before trying again
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    fn write(&mut self, chunk: DataChunk) {
        if let Some(ref mut writer) = &mut self.writer {
            if let Some(ref mut writer) = &mut self.writer {
                for i in 0..chunk.cardinality() {
                    let row: Vec<_> = chunk.arrays.iter().map(|a| a.get(i).to_string()).collect();
                    writer.write_record(&row).expect("Fail to write record.");
                }
            }
        } else {
            panic!("Fail to write for writer is not initialized")
        }
    }

    fn on_receive_barrier(&mut self, barrier: Barrier) {
        if let Some(ref mut writer) = &mut self.writer {
            writer.flush().expect("Fail to flush.");
        }
    }
}


