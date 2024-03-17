mod expr;
mod create_table;
mod select;
mod insert;
mod table;
mod column_ref;
mod drop;

use std::collections::{HashMap, HashSet};
use egg::{Id};
use sqlparser::ast::{Ident, ObjectName, Statement, Value};
use crate::planner::{Expr as Node, RecExpr, TypeError, TypeSchemaAnalysis};
use crate::catalog::*;
use crate::types::DataValue;
pub use crate::binder::create_table::CreateTable;
pub use crate::binder::drop::*;


pub type Result<T = Id> = std::result::Result<T, BindError>;

impl From<&Value> for DataValue {

    fn from(value: &Value) -> Self {
        match value {
            Value::Number(n, _) => {
                if let Ok(int) = n.parse::<i32>() {
                    Self::Int32(int)
                }
                else {
                    panic!("invalid digit: {}", n);
                }
            }
            Value::SingleQuotedString(s) => Self::String(s.clone()),
            Value::DoubleQuotedString(s) => Self::String(s.clone()),
            Value::Boolean(b) => Self::Bool(*b),
            Value::Null => Self::Null,
            _ => todo!("parse value: {:?}", value),
        }
    }
}




/// The error type of bind operations.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum BindError {
    #[error("table must have at least one column")]
    EmptyColumns,
    #[error("schema not found: {0}")]
    SchemaNotFound(String),
    #[error("invalid table {0}")]
    InvalidTable(String),
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    #[error("duplicated table: {0}")]
    DuplicatedTable(String),
    #[error("duplicated column: {0}")]
    DuplicatedColumn(String),
    #[error("ambiguous column name: {0}")]
    AmbiguousColumnName(String),
    #[error("invalid expression: {0}")]
    InvalidExpression(String),
    #[error("type error: {0}")]
    TypeError(#[from] TypeError),
    #[error("invalid table name: {0:?}")]
    InvalidTableName(Vec<Ident>),
    #[error("not nullable column: {0}")]
    NotNullableColumn(String),
    #[error("tuple length mismatch: expected {expected} but got {actual}")]
    TupleLengthMismatch { expected: usize, actual: usize },
    #[error("value should not be null in column: {0}")]
    NullValueInColumn(String),
    #[error("duplicated alias: {0}")]
    DuplicatedAlias(String),

    #[error("column {0} must appear in the GROUP BY clause or be used in an aggregate function")]
    ColumnNotInAgg(String),
}

type TableName = String;

pub struct Binder {
    catalog: CatalogRef,
    contexts: Vec<BinderContext>,
    tables: HashMap<TableName, TableRefId>,
    egraph: egg::EGraph<Node, TypeSchemaAnalysis>,
}

#[derive(Debug, Default)]
pub struct BinderContext {
    /// Table names that can be accessed from the current query.
    table_aliases: HashSet<String>,

    /// Column names that can be accessed from the current query.
    /// column_name -> (table_name -> id)
    pub aliases: HashMap<String, HashMap<String, Id>>,

    /// Column names that can be accessed from the outside query.
    /// column_name -> id
    pub output_aliases: HashMap<String, Id>,
}


impl Binder {
    pub fn new(catalog: CatalogRef) -> Self {
        Binder {
            catalog: catalog.clone(),
            contexts: vec![BinderContext::default()],
            tables: HashMap::default(),
            egraph: egg::EGraph::new(TypeSchemaAnalysis {
                catalog: catalog.clone()
            })
        }
    }

    fn current_ctx(&self) -> &BinderContext {
        self.contexts.last().unwrap()
    }

    fn current_ctx_mut(&mut self) -> &mut BinderContext {
        self.contexts.last_mut().unwrap()
    }

    fn add_alias(&mut self, column_name: String, table_name: String, id: Id) {
        let context = self.contexts.last_mut().unwrap();
        context
            .aliases
            .entry(column_name)
            .or_default()
            .insert(table_name, id);
    }

    fn schema(&self, id: Id) -> Vec<Id> {
        self.egraph[id].data.schema.clone()
    }

    fn aggs(&self, id: Id) -> &[Node] {
        &self.egraph[id].data.aggs
    }

    fn node(&self, id: Id) -> &Node {
        &self.egraph[id].nodes[0]
    }


    pub fn bind(&mut self, stmt: Statement) -> Result<RecExpr> {
        let id = self.bind_stmt(stmt)?;
        let extractor = egg::Extractor::new(&self.egraph, egg::AstSize);
        let (_, best) = extractor.find_best(id);
        Ok(best)
    }

    fn push_context(&mut self) {
        todo!()
    }

    fn pop_context(&mut self) {
       todo!()
    }

    fn bind_stmt(&mut self, stmt: Statement) -> Result {
        match stmt {
            Statement::CreateTable {
                name, columns , with_options,
                ..
            } => self.bind_create_table(name, columns, with_options),
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                ..
            } => self.bind_drop(object_type, if_exists, names, cascade),
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => self.bind_insert(table_name, columns, source),
            Statement::Query(query) => self.bind_query(*query).map(|(id, _)| id),
            _ => todo!("bind statement: {:#?}", stmt),
        }
    }

    fn check_type(&self, id: Id) -> Result<crate::types::DataType> {
        Ok(self.egraph[id].data.type_.clone()?)
    }
}

fn split_name(name: &ObjectName) -> Result<(&str, &str)> {
    Ok(match name.0.as_slice() {
        [table] => (DEFAULT_SCHEMA_NAME, &table.value),
        [schema, table] => (&schema.value, &table.value),
        _ => return Err(BindError::InvalidTableName(name.0.clone())),
    })
}

fn lower_case_name(name: &ObjectName) -> ObjectName {
    ObjectName(
        name.0
            .iter()
            .map(|ident| Ident::new(ident.value.to_lowercase()))
            .collect::<Vec<_>>(),
    )
}


