use itertools::Itertools;
use sqlparser::ast::Ident;

use crate::binder::{Binder, BindError};
use crate::catalog::ColumnRefId;
use crate::types::DataType;
use crate::binder::Result;

#[derive(PartialEq, Eq, Clone)]
pub struct BoundColumnRef {
    pub column_ref_id: ColumnRefId,
    pub return_type: DataType,
}


impl std::fmt::Debug for BoundColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.column_ref_id)
    }
}

impl Binder {

    pub fn bind_colum_ref(&mut self, idents: &[Ident]) -> Result {
        let idents = idents
            .into_iter()
            .map(|ident| Ident::new(ident.value.to_lowercase()))
            .collect_vec();

        let (_schema_name, table_name, column_name) = match idents.as_slice() {
            [column] => (None, None, &column.value),
            [table, column] => (None, Some(&table.value), &column.value),
            [schema, table, column] => (Some(&schema.value), Some(&table.value), &column.value),
            _ => return Err(BindError::InvalidTableName(idents.into())),
        };

        let map = self
            .current_ctx()
            .aliases
            .get(column_name)
            .ok_or_else(|| BindError::ColumnNotFound(column_name.into()))?;
        if let Some(table_name) = table_name {
            map.get(table_name)
                .cloned()
                .ok_or_else(|| BindError::TableNotFound(table_name.clone()))
        } else if map.len() == 1 {
            Ok(*map.values().next().unwrap())
        } else {
            Err(BindError::AmbiguousColumnName(column_name.into()))
        }
    }
}
