use sqlparser::ast::{Ident, ObjectName, Query};
use crate::binder::{Binder};
use crate::binder::{Result, Node};


impl Binder {


    pub fn bind_insert(&mut self,
                       table_name: ObjectName,
                       columns: Vec<Ident>, source: Box<Query>) -> Result {

        let table = self.bind_table_id(&table_name)?;
        let cols =
            self.bind_table_columns(&table_name, &columns)?;
        let source = self.bind_query(*source)?.0;
        let id = self.egraph.add(Node::Insert([table, cols, source]));
        Ok(id)
    }
}