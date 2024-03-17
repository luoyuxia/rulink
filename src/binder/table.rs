use sqlparser::ast::{Ident, ObjectName, TableAlias, TableFactor, TableWithJoins};
use crate::binder::Binder;
use super::*;

impl Binder {

    pub(in crate::binder) fn bind_table_factor(&mut self, table: TableFactor) -> Result {
        match table {
            TableFactor::Table { name, alias, .. } => {
                let table_id = self.bind_table_id(&name)?;
                let col_id = self.bind_table_name(&name, alias)?;
                let true_ = self.egraph.add(Node::null());
                Ok(self.egraph.add(Node::Scan([table_id, col_id, true_])))
            }
            _ => {
                unimplemented!()
            }
        }
    }

    pub(in crate::binder) fn bind_table_with_joins(&mut self, tables: TableWithJoins) -> Result {
        let node = self.bind_table_factor(tables.relation)?;
        for join in tables.joins {
            todo!("handle join")
        }
        Ok(node)
    }


    pub fn bind_table_columns(
        &mut self, table_name: &ObjectName, columns: &[Ident])
        -> Result {
        let (schema_name, table_name) = split_name(table_name)?;
        let schema = self.catalog
            .get_schema_by_name(schema_name)
            .ok_or_else(|| BindError::SchemaNotFound(schema_name.into()))?;
        let table = schema
            .get_table_by_name(table_name)
            .ok_or_else(|| BindError::TableNotFound(table_name.into()))?;
        let columns = if columns.is_empty() {
            table.all_columns().values().cloned().collect()
        } else {
            // Otherwise, we get columns info from the query.
            let mut column_catalogs = vec![];
            for col in columns.iter() {
                let col = table
                    .get_column_by_name(&col.value)
                    .ok_or_else(|| BindError::ColumnNotFound(col.value.clone()))?;
                column_catalogs.push(col);
            }
            column_catalogs
        };

        let ids = columns.into_iter().map(|col| {
            self.egraph.add(Node::Column(
                ColumnRefId::new(
                    schema.id(), table.id(), col.id())))
        }).collect();
        Ok(self.egraph.add(Node::List(ids)))
    }

    // Returns a [`Table`](Node::Table) node.
    ///
    /// # Example
    /// - `bind_table_id(t)` => `$1`
    pub(in crate::binder) fn bind_table_id(&mut self, table_name: &ObjectName) -> Result {
        let (schema_name, table_name) = split_name(table_name)?;

        let schema = self.catalog
            .get_schema_by_name(schema_name)
            .ok_or_else(|| BindError::SchemaNotFound(schema_name.into()))?;
        let table = schema
            .get_table_by_name(table_name)
            .ok_or_else(|| BindError::TableNotFound(table_name.into()))?;

        let id = self.egraph.add(Node::Table(
            TableRefId::new(schema.id(),
                            table.id())));
        Ok(id)
    }

    pub fn bind_table_name(&mut self, name: &ObjectName, alias: Option<TableAlias>) -> Result {
        let (schema_name, table_name) = split_name(&name)?;
        let ref_id = self
            .catalog
            .get_table_id_by_name(schema_name, table_name)
            .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;

        let table_alias = match &alias {
            Some(alias) => &alias.name.value,
            None => table_name,
        };

        if !self
            .current_ctx_mut()
            .table_aliases
            .insert(table_alias.into())
        {
            return Err(BindError::DuplicatedTable(table_alias.into()));
        }

        let table = self.catalog.get_table(ref_id).unwrap();

        let mut ids = vec![];
        for (cid, column) in table.all_columns() {
            let column_ref_id = ColumnRefId::from_table(ref_id, cid);
            let id = self.egraph.add(Node::Column(column_ref_id));

            // TODO: handle column aliases
            self.add_alias(column.name().into(), table_alias.into(), id);
            ids.push(id);
        }
        let id = self.egraph.add(Node::List(ids.into()));
        Ok(id)
    }
}