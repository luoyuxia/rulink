use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::types::{DataType, DataValue};

pub struct DataChunkBuilder {
    array_builders: Vec<ArrayBuilderImpl>,
    size: usize,
    capacity: usize,
}


impl DataChunkBuilder {

    pub fn new<'a>(data_types: impl IntoIterator<Item = &'a DataType>,
                   capacity: usize) -> Self {
        let array_builders = data_types
            .into_iter()
            .map(|ty| ArrayBuilderImpl::with_capacity(capacity, ty))
            .collect();
        DataChunkBuilder {
            array_builders,
            size: 0,
            capacity,
        }
    }

    pub fn push_row(&mut self, row: impl IntoIterator<Item = DataValue>) -> Option<DataChunk> {
        self.array_builders
            .iter_mut()
            .zip(row)
            .for_each(|(builder, v)| builder.push(&v));
        self.size += 1;
        if self.size == self.capacity {
            self.take()
        } else {
            None
        }
    }

    pub fn take(&mut self) -> Option<DataChunk> {
        let size = std::mem::take(&mut self.size);
        let capacity = self.capacity;
        match size {
            0 => None,
            _ => Some(
                self.array_builders
                    .iter_mut()
                    .map(|builder| {
                        let chunk = builder.take();
                        builder.reserve(capacity);
                        chunk
                    })
                    .collect(),
            ),
        }
    }
}