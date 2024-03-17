use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use crate::catalog::column::ColumnCatalog;
use crate::catalog::{CatalogError, ColumnDesc, ColumnId, TableId};

/// The catalog of a table.
pub struct TableCatalog {
    id: TableId,
    inner: Mutex<Inner>,
}

struct Inner {
    name: String,
    /// Mapping from column names to column ids
    column_idxs: HashMap<String, ColumnId>,
    columns: BTreeMap<ColumnId, ColumnCatalog>,
    next_column_id: ColumnId,
    options: HashMap<String, String>,
}

impl TableCatalog {
    pub(super) fn new(id: TableId, name: String) -> TableCatalog {
        TableCatalog {
            id,
            inner: Mutex::new(Inner {
                name,
                column_idxs: HashMap::new(),
                columns: BTreeMap::new(),
                next_column_id: 0,
                options: HashMap::new(),
            }),
        }
    }

    pub fn id(&self) -> TableId {
        self.id
    }

    pub fn name(&self) -> String {
        let inner = self.inner.lock().unwrap();
        inner.name.clone()
    }

    pub fn add_column(&self, name: &str, desc: ColumnDesc) -> Result<ColumnId, CatalogError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.column_idxs.contains_key(name) {
            return Err(CatalogError::Duplicated("column", name.into()));
        }
        let id = inner.next_column_id;
        inner.next_column_id += 1;
        inner.column_idxs.insert(name.into(), id);
        inner
            .columns
            .insert(id, ColumnCatalog::new(id, name.into(), desc));
        Ok(id)
    }

    pub fn add_options(&self, options: BTreeMap<String, String>) {
        let mut inner = self.inner.lock().unwrap();
        for (key, val) in options {
            inner.options.insert(key, val);
        }
    }

    pub fn contains_column(&self, name: &str) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.column_idxs.contains_key(name)
    }

    pub fn all_columns(&self) -> BTreeMap<ColumnId, ColumnCatalog> {
        let inner = self.inner.lock().unwrap();
        inner.columns.clone()
    }

    pub fn get_column(&self, id: ColumnId) -> Option<ColumnCatalog> {
        let inner = self.inner.lock().unwrap();
        inner.columns.get(&id).cloned()
    }

    pub fn get_option(&self, key: &str) -> Option<String> {
        let inner = self.inner.lock().unwrap();
        let v = inner.options.get(key);
        match v {
            None => {
                None
            }
            Some(tt) => {
                Some(tt.clone())
            }
        }
    }

    pub fn get_options(&self) -> HashMap<String, String> {
        return self.inner.lock().unwrap().options.clone();
    }

    pub fn get_column_by_name(&self, name: &str) -> Option<ColumnCatalog> {
        let inner = self.inner.lock().unwrap();
        inner
            .column_idxs
            .get(name)
            .and_then(|id| inner.columns.get(id))
            .cloned()
    }
}