pub mod native;


use std::fmt::{Display, Formatter};
use std::str::FromStr;

use parse_display::Display;
pub use self::native::*;
use sqlparser::ast::DataType::{Char, Int, Text, Varchar, Boolean};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DataTypeKind {
    // NOTE: order matters
    Null,
    Bool,
    Int16,
    Int32,
    Int64,
    Float64,
    Decimal(Option<u8>, Option<u8>),
    Date,
    Timestamp,
    TimestampTz,
    Interval,
    String,
    Blob,
    Struct(Vec<DataType>),
}
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DataType {
    pub kind: DataTypeKind,
    nullable: bool,
}

impl DataType {
    /// Returns the minimum compatible type of 2 types.
    pub fn union(&self, other: &Self) -> Option<Self> {
        Some(DataType {
            kind: self.kind.union(&other.kind)?,
            nullable: self.nullable || other.nullable,
        })
    }


}

impl Display for DataTypeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Int16 => write!(f, "SMALLINT"),
            Self::Int32 => write!(f, "INT"),
            Self::Int64 => write!(f, "BIGINT"),
            // Self::Float32 => write!(f, "REAL"),
            Self::Float64 => write!(f, "DOUBLE"),
            Self::String => write!(f, "STRING"),
            Self::Blob => write!(f, "BLOB"),
            Self::Bool => write!(f, "BOOLEAN"),
            Self::Decimal(p, s) => match (p, s) {
                (None, None) => write!(f, "DECIMAL"),
                (Some(p), None) => write!(f, "DECIMAL({p})"),
                (Some(p), Some(s)) => write!(f, "DECIMAL({p},{s})"),
                (None, Some(_)) => panic!("invalid decimal"),
            },
            Self::Date => write!(f, "DATE"),
            Self::Timestamp => write!(f, "TIMESTAMP"),
            Self::TimestampTz => write!(f, "TIMESTAMP WITH TIME ZONE"),
            Self::Interval => write!(f, "INTERVAL"),
            Self::Struct(types) => {
                write!(f, "STRUCT(")?;
                for t in types.iter().take(1) {
                    write!(f, "{}", t.kind())?;
                }
                for t in types.iter().skip(1) {
                    write!(f, ", {}", t.kind())?;
                }
                write!(f, ")")
            }
        }
    }
}

impl DataTypeKind {
    pub fn as_struct(&self) -> &[DataType] {
        let Self::Struct(types) = self else { panic!("not a struct: {self}") };
        types
    }

    pub const fn is_number(&self) -> bool {
        matches!(
            self,
            Self::Int16 | Self::Int32 | Self::Int64 | Self::Float64 | Self::Decimal(_, _)
        )
    }

    /// Returns the minimum compatible type of 2 types.
    pub fn union(&self, other: &Self) -> Option<Self> {
        use DataTypeKind::*;
        let (a, b) = if self <= other {
            (self, other)
        } else {
            (other, self)
        }; // a <= b
        match (a, b) {
            (Null, _) => Some(b.clone()),
            (Bool, Bool | Int32 | Int64 | Float64 | Decimal(_, _) | String) => Some(b.clone()),
            (Int32, Int32 | Int64 | Float64 | Decimal(_, _) | String) => Some(b.clone()),
            (Int64, Int64 | Float64 | Decimal(_, _) | String) => Some(b.clone()),
            (Float64, Float64 | Decimal(_, _) | String) => Some(b.clone()),
            (Decimal(_, _), Decimal(_, _) | String) => Some(b.clone()),
            (Date, Date | String) => Some(b.clone()),
            (Interval, Interval | String) => Some(b.clone()),
            (String, String | Blob) => Some(b.clone()),
            (Blob, Blob) => Some(b.clone()),
            (Struct(a), Struct(b)) => {
                if a.len() != b.len() {
                    return None;
                }
                let c = (a.iter().zip(b.iter()))
                    .map(|(a, b)| a.union(b))
                    .try_collect()?;
                Some(Struct(c))
            }
            _ => None,
        }
    }
}


impl From<&crate::parser::DataType> for DataTypeKind {

    fn from(kind: &sqlparser::ast::DataType) -> Self {
        match kind {
            Char(_) | Varchar(_) | crate::parser::DataType::String | Text => Self::String,
            Int(_) => Self::Int32,
            Boolean => Self::Bool,
            _ => todo!("not supported type: {:?}", kind)
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl FromStr for DataType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum ConvertError {
    #[error("failed to convert string {0:?} to int: {1}")]
    ParseInt(String, #[source] std::num::ParseIntError),
}



impl DataType {

    pub const fn new_nullable(kind: DataTypeKind) -> Self {
        Self::new(kind, true)
    }


    pub const fn new(kind: DataTypeKind, nullable: bool) -> Self {
        DataType {
            kind,
            nullable
        }
    }

    pub fn kind(&self) -> DataTypeKind {
        self.kind.clone()
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable.clone()
    }
}

#[derive(Debug, Clone,  PartialEq, PartialOrd, Ord, Hash, Eq)]
pub enum DataValue {
    Null,
    Bool(bool),
    Int32(i32),
    String(String),
}

impl Display for DataValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataValue::Null => write!(f, "{}", String::from("NULL")),
            DataValue::Bool(v) => write!(f, "{}", v.to_string()),
            DataValue::Int32(v) => write!(f, "{}", v.to_string()),
            DataValue::String(v) => write!(f, "{}", v.to_string()),
        }
    }
}

impl FromStr for DataValue {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

macro_rules! impl_arith_for_datavalue {
    ($Trait:ident, $name:ident) => {
        impl std::ops::$Trait for &DataValue {
            type Output = DataValue;

            fn $name(self, rhs: Self) -> Self::Output {
                use DataValue::*;
                match (self, rhs) {
                    (&Null, _) | (_, &Null) => Null,
                    (&Int32(x), &Int32(y)) => Int32(x.$name(y)),
                    _ => panic!(
                        "invalid operation: {:?} {} {:?}",
                        self,
                        stringify!($name),
                        rhs
                    ),
                }
            }
        }

        impl std::ops::$Trait for DataValue {
            type Output = DataValue;
            fn $name(self, rhs: Self) -> Self::Output {
                (&self).$name(&rhs)
            }
        }
    };
}

impl_arith_for_datavalue!(Add, add);



impl DataValue {

    pub fn add(self, other: Self) -> Self {
        if self.is_null() {
            other
        } else {
            self + other
        }
    }
   pub  fn or(self, other: Self) -> Self {
        if self.is_null() {
            other
        } else {
            self
        }
    }

    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn datatype(&self) -> Option<DataType> {
        match self {
            DataValue::Null => Some(DataType::new(DataTypeKind::String, false)),
            DataValue::Bool(_) => Some(DataType::new(DataTypeKind::Bool, false)),
            DataValue::Int32(_) => Some(DataType::new(DataTypeKind::Int32, false)),
            DataValue::String(_) => Some(DataType::new(DataTypeKind::String, false)),
        }
    }
}

#[derive(Debug, Display, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone)]
#[display("#{0}")]
pub struct ColumnIndex(pub u32);

#[derive(thiserror::Error, Debug, Clone)]
#[error("parse column index error: {}")]
pub enum ParseColumnIndexError {
    #[error("no leading '#'")]
    NoLeadingSign,
    #[error("invalid number: {0}")]
    InvalidNum(#[from] std::num::ParseIntError),
}

impl FromStr for ColumnIndex {

    type Err = ParseColumnIndexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let body = s.strip_prefix("#").ok_or(Self::Err::NoLeadingSign)?;
        let num = body.parse()?;
        Ok(Self(num))
    }
}
