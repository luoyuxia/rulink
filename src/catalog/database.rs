use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::catalog::schema::SchemaCatalog;
use crate::catalog::{CatalogError, ColumnCatalog, ColumnRefId, DEFAULT_SCHEMA_NAME, SchemaId, TableRefId};
use crate::catalog::table::TableCatalog;

pub struct DatabaseCatalog {
    inner: Mutex<Inner>
}

#[derive(Default)]
struct Inner {
    schema_idxs: HashMap<String, SchemaId>,
    schemas: HashMap<SchemaId, Arc<SchemaCatalog>>,
    next_schema_id: SchemaId
}

impl Default for DatabaseCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseCatalog {
    pub fn new() -> Self {
        let db_catalog = DatabaseCatalog {
            inner: Mutex::new(Inner::default())
        };
        db_catalog.add_schema(DEFAULT_SCHEMA_NAME).unwrap();
        db_catalog
    }

    pub fn add_schema(&self, name: &str) -> Result<SchemaId, CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.schema_idxs.contains_key(name) {
            return Err(CatalogError::Duplicated("schema", name.into()));
        }
        let id = inner.next_schema_id;
        inner.next_schema_id += 1;
        let schema_catalog = Arc::new(SchemaCatalog::new(id, name.into()));
        inner.schema_idxs.insert(name.into(), id);
        inner.schemas.insert(id, schema_catalog);
        Ok(id)
    }


    pub fn del_schema(&self, name: &str) -> Result<(), CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        let id = inner
            .schema_idxs
            .remove(name)
            .ok_or_else(|| CatalogError::NotFound("schema", name.into()))?;
        inner.schemas.remove(&id);
        Ok(())
    }

    pub fn all_schemas(&self) -> HashMap<SchemaId, Arc<SchemaCatalog>> {
        let inner = self.inner.lock().unwrap();
        inner.schemas.clone()
    }

    pub fn get_schema(&self, schema_id: SchemaId) -> Option<Arc<SchemaCatalog>> {
        let inner = self.inner.lock().unwrap();
        inner.schemas.get(&schema_id).cloned()
    }

    pub fn get_schema_by_name(&self, name: &str) -> Option<Arc<SchemaCatalog>> {
        let inner = self.inner.lock().unwrap();
        inner
            .schema_idxs
            .get(name)
            .and_then(|id| inner.schemas.get(id))
            .cloned()
    }

    pub fn get_table_id_by_name(&self, schema_name: &str, table_name: &str) -> Option<TableRefId> {
        let schema = self.get_schema_by_name(schema_name)?;
        let table = schema.get_table_by_name(table_name)?;

        Some(TableRefId {
            schema_id: schema.id(),
            table_id: table.id(),
        })
    }

    pub fn get_table(&self, table_ref_id: TableRefId) -> Option<Arc<TableCatalog>> {
        let schema = self.get_schema(table_ref_id.schema_id)?;
        schema.get_table(table_ref_id.table_id)
    }

    pub fn get_column(&self, column_ref_id: &ColumnRefId) -> Option<ColumnCatalog> {
        self.get_table(
            TableRefId::new(column_ref_id.schema_id,
                                       column_ref_id.table_id))?.get_column(column_ref_id.column_id)
    }

    pub fn drop_table(&self, table_ref_id: TableRefId)  {
        let schema = self.get_schema(table_ref_id.schema_id).unwrap();
        schema.del_table(table_ref_id.table_id);
    }
}