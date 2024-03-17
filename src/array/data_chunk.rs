use std::fmt;
use std::fmt::{Formatter};
use std::sync::Arc;
use crate::array::{ArrayBuilder, ArrayBuilderImpl, ArrayImpl};
use crate::array::utf8_array::Utf8ArrayBuilder;
use crate::types::DataValue;

#[derive(PartialEq, Clone)]
pub struct DataChunk {
    pub arrays: Arc<[ArrayImpl]>,
}


pub type Row = Vec<DataValue>;

impl FromIterator<ArrayImpl> for DataChunk {

    fn from_iter<I: IntoIterator<Item=ArrayImpl>>(iter: I) -> Self {
        let arrays = iter.into_iter().collect::<Arc<[ArrayImpl]>>();
        assert!(!arrays.is_empty());
        let cardinality = arrays[0].len();
        assert!(
            arrays.iter().map(|a| a.len()).all(|l| l == cardinality),
            "all arrays must have the same length"
        );
        DataChunk { arrays }
    }
}

impl DataChunk {

    pub fn single(item: i32) -> Self {
        DataChunk {
            arrays: [ArrayImpl::Int32([item].into_iter().collect())]
                .into_iter().collect(),
        }
    }

    pub fn no_column() -> Self {
        DataChunk {
            arrays: Arc::new([]),
        }
    }

    pub fn single_str(item: &str) -> Self {
        let mut string_builder = Utf8ArrayBuilder::with_capacity(1);
        string_builder.push(Some(item));
        DataChunk::from_iter([ArrayBuilderImpl::from(ArrayBuilderImpl::Utf8(string_builder)).finish()])
    }

    pub fn from_strs(items: Vec<String>) -> Self {
        let mut string_builder = Utf8ArrayBuilder::with_capacity(items.len());
        for item in items {
            string_builder.push(Some(item.as_str()));
        }
        DataChunk::from_iter([ArrayBuilderImpl::from(ArrayBuilderImpl::Utf8(string_builder)).finish()])
    }

    pub fn from_array(array: ArrayImpl) -> Self {
        DataChunk {
            arrays: [array].into_iter().collect(),
        }
    }

    pub fn row(&self, idx: usize) -> RowRef<'_> {
        debug_assert!(idx < self.cardinality(), "index out of range");
        RowRef {
            chunk: self,
            row_idx: idx,
        }
    }

    pub fn array_at(&self, idx: usize) -> &ArrayImpl {
        &self.arrays[idx]
    }

    pub fn cardinality(&self) -> usize {
        if self.arrays.len() <= 0 {
            0
        } else {
            self.arrays[0].len()
        }
    }

    pub fn arrays(&self) -> &[ArrayImpl] {
        &self.arrays
    }
}

pub struct RowRef<'a> {
    chunk: &'a DataChunk,
    row_idx: usize,
}

impl RowRef<'_> {
    /// Get the value at given column index.
    pub fn get(&self, idx: usize) -> DataValue {
        self.chunk.array_at(idx).get(self.row_idx)
    }

    pub fn get_by_indexes(&self, indexes: &[usize]) -> Vec<DataValue> {
        indexes
            .iter()
            .map(|i| self.chunk.array_at(*i).get(self.row_idx))
            .collect()
    }

    /// Get an iterator over the values of the row.
    pub fn values(&self) -> impl Iterator<Item = DataValue> + '_ {
        self.chunk.arrays().iter().map(|a| a.get(self.row_idx))
    }

    pub fn to_owned(&self) -> Row {
        self.values().collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::array::DataChunk;

    #[test]
    fn test_collect() {
        let chunk = DataChunk::single_str("sd");
        println!("{}", chunk);
    }
}


impl fmt::Display for DataChunk {

    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use prettytable::{format, Table};
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
        for i in 0..self.cardinality() {
            let row = self.arrays.iter().map(|a| a.get(i).to_string()).collect();
            table.add_row(row);
        }
        write!(f, "{}", table)
    }
}


impl fmt::Debug for DataChunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}