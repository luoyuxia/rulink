
mod column;
mod table;
mod schema;
mod database;

use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;
pub use crate::catalog::database::DatabaseCatalog;
pub use self::column::*;
pub use self::table::*;

pub type SchemaId = u32;
pub type TableId = u32;
pub type ColumnId = u32;

pub type CatalogRef = Arc<DatabaseCatalog>;

/// The name of default schema: `postgres`.
pub const DEFAULT_SCHEMA_NAME: &str = "postgres";


#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Ord, PartialOrd)]
pub struct TableRefId {
    pub schema_id: SchemaId,
    pub table_id: TableId
}

impl Display for TableRefId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // TODO: now ignore database and schema
        write!(f, "${}", self.table_id)
    }
}

impl FromStr for TableRefId {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

impl TableRefId {
    pub const fn new(schema_id: SchemaId, table_id: TableId) -> Self {
        TableRefId {
            schema_id,
            table_id
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Ord, PartialOrd)]
pub struct ColumnRefId {
    pub schema_id: SchemaId,
    pub table_id: TableId,
    pub column_id: ColumnId
}

impl Display for ColumnRefId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl FromStr for ColumnRefId {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

impl ColumnRefId {
    pub const fn from_table(table: TableRefId, column_id: ColumnId) -> Self {
        ColumnRefId {
            schema_id: table.schema_id,
            table_id: table.table_id,
            column_id
        }
    }

    pub const fn new(schema_id: SchemaId, table_id: TableId, column_id: ColumnId) -> Self {
        ColumnRefId {
            schema_id,
            table_id,
            column_id
        }
    }
}



#[derive(thiserror::Error, Debug)]
pub enum CatalogError {

    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),

    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
}