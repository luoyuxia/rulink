use std::collections::{BTreeMap, HashSet};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::str::FromStr;
use sqlparser::ast::{ColumnDef, SqlOption, Value};
use super::*;

use std::result::Result as RawResult;
use pretty_xmlish::helper::delegate_fmt;
use pretty_xmlish::Pretty;

#[derive(Debug, PartialEq, Clone, Eq, PartialOrd, Ord, Hash)]
pub struct CreateTable {
    pub schema_id: SchemaId,
    pub table_name: String,
    pub columns: Vec<(String, ColumnDesc)>,
    pub options: BTreeMap<String, String>
}

impl Display for CreateTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let explainer = Pretty::childless_record("CreateTable", self.pretty_table());
        delegate_fmt(&explainer, f, String::with_capacity(1000))
    }
}

impl CreateTable {
    pub fn pretty_table<'a>(&self) -> Vec<(&'a str, Pretty<'a>)> {
        let cols = Pretty::Array(self.columns.iter().map(|(c, cd)| self.pretty_column(c,
        cd)).collect());
        vec![
            ("schema_id", Pretty::display(&self.schema_id)),
            ("name", Pretty::display(&self.table_name)),
            ("columns", cols),
        ]
    }

    fn pretty_column<'a>(&self, name: &String, column_desc: &ColumnDesc) -> Pretty<'a> {
        let mut fields = vec![
            ("name", Pretty::display(&name)),
            ("type", Pretty::display(&column_desc.datatype().kind)),
        ];
        if column_desc.is_primary() {
            fields.push(("primary", Pretty::display(&column_desc.is_primary())));
        }
        if column_desc.datatype().is_nullable() {
            fields.push(("nullable", Pretty::display(&column_desc.datatype().is_nullable())));
        }
        Pretty::childless_record("Column", fields)
    }
}

impl FromStr for CreateTable {
    type Err = ();

    fn from_str(_s: &str) -> RawResult<Self, Self::Err> {
        Err(())
    }
}

impl Binder {
    pub fn bind_create_table(
        &mut self,
        name: ObjectName,
        columns: Vec<ColumnDef>,
        with_options: Vec<SqlOption>) -> Result {
        // check empty columns
        if columns.is_empty() {
            return Err(BindError::EmptyColumns);
        }

        let (schema_name, table_name) = split_name(&name)?;
        let schema = self.catalog
            .get_schema_by_name(schema_name)
            .ok_or_else(|| BindError::SchemaNotFound(schema_name.into()))?;
        if schema.get_table_by_name(table_name).is_some() {
            return Err(BindError::DuplicatedTable(table_name.into()));
        }

        let mut set = HashSet::new();
        for column in columns.iter() {
            if !set.insert(column.name.value.clone()) {
                return Err(BindError::DuplicatedColumn(column.name.value.clone()));
            }
        }

        let columns = columns
            .iter()
            .map(|col| (col.name.value.clone(), ColumnDesc::from(col)))
            .collect();

        // extract options
        let mut options:BTreeMap<String, String> = BTreeMap::new();
        for option in with_options {
            if let Value::SingleQuotedString(val) = &option.value {
                options.insert(option.name.value.clone(),
                               val.to_string());
            }
        }

        let create = self.egraph.add(Node::CreateTable(
            CreateTable {
                schema_id: schema.id(),
                table_name: table_name.into(),
                columns,
                options,
            }
        ));
        Ok(create)
    }
}