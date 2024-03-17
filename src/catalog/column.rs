use sqlparser::ast::{ColumnDef, ColumnOption};
use crate::catalog::ColumnId;
use crate::types::DataType;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnDesc {
    datatype: DataType,
    is_primary: bool,
}

impl ColumnDesc {
    pub const fn new(datatype: DataType, is_primary: bool) -> Self {
        ColumnDesc {
            datatype,
            is_primary
        }
    }

    pub fn is_primary(&self) -> bool {
        self.is_primary.clone()
    }

    pub fn is_nullable(&self) -> bool {
        self.datatype.is_nullable()
    }

    pub fn datatype(&self) -> &DataType {
        &self.datatype
    }
}

impl From<&ColumnDef> for ColumnDesc {

    fn from(cdef: &ColumnDef) -> Self {
        let mut is_nullable = true;
        let mut is_primary = false;
        for opt in cdef.options.iter() {
            match opt.option {
                ColumnOption::Null => is_nullable = true,
                ColumnOption::NotNull => is_nullable = false,
                ColumnOption::Unique { is_primary: v } => is_primary = v,
                _ => todo!("column options"),
            }
        }
        ColumnDesc::new(DataType::new((&cdef.data_type).into(), is_nullable),
        is_primary)
    }
}


impl DataType {
    pub const fn to_column(self) -> ColumnDesc {
        ColumnDesc::new(self, false)
    }

    pub const fn to_column_primary_key(self) -> ColumnDesc {
        ColumnDesc::new(self, true)
    }
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnCatalog {
    id: ColumnId,
    name: String,
    desc: ColumnDesc
}

impl ColumnCatalog {
    pub fn new(id: ColumnId, name: String, desc: ColumnDesc) -> ColumnCatalog {
        ColumnCatalog { id, name, desc }
    }

    pub fn id(&self) -> ColumnId {
        self.id.clone()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn desc(&self) -> &ColumnDesc {
        &self.desc
    }

    pub fn datatype(&self) -> DataType {
        self.desc.datatype.clone()
    }

    pub fn is_primary(&self) -> bool {
        self.desc.is_primary()
    }

    pub fn is_nullable(&self) -> bool {
        self.desc.is_nullable()
    }
}








