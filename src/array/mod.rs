pub mod primitive_array;
pub mod iter;
pub mod utf8_array;
pub mod data_chunk;
mod data_chunk_builder;

use crate::array::iter::ArrayIter;
use crate::array::utf8_array::{Utf8Array, Utf8ArrayBuilder};
use crate::types::{ConvertError, DataType, DataTypeKind, DataValue};
pub use self::primitive_array::*;
pub use self::data_chunk::*;
pub use self::data_chunk_builder::*;

pub trait ArrayBuilder: Send + Sync + 'static {
    type Array: Array<Builder = Self>;

    fn with_capacity(capacity: usize) -> Self;

    fn push(&mut self, value: Option<&<Self::Array as Array>::Item>);

    fn append(&mut self, other: &Self::Array);

    fn finish(self) -> Self::Array;

    fn reserve(&mut self, capacity: usize);

    fn take(&mut self) -> Self::Array;
}


pub trait Array: Sized + Send + Sync + 'static {
    type Builder: ArrayBuilder<Array = Self>;

    type Item: ToOwned + ?Sized;

    fn get(&self, idx: usize) -> Option<&Self::Item>;

    fn len(&self) -> usize;

    fn iter(&self) -> ArrayIter<'_, Self> {
        ArrayIter::new(self)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub type BoolArray = PrimitiveArray<bool>;
pub type I32Array = PrimitiveArray<i32>;
pub type F64Array = PrimitiveArray<f64>;

#[derive(Clone, PartialEq)]
pub enum ArrayImpl {
    Bool(BoolArray),
    Int32(I32Array),
    Utf8(Utf8Array),
}

pub type BoolArrayBuilder = PrimitiveArrayBuilder<bool>;
pub type I32ArrayBuilder = PrimitiveArrayBuilder<i32>;
pub type F64ArrayBuilder = PrimitiveArrayBuilder<f64>;

/// Embeds all types of array builders in `array` module.
pub enum ArrayBuilderImpl {
    Bool(BoolArrayBuilder),
    Int32(I32ArrayBuilder),
    Utf8(Utf8ArrayBuilder),
}

#[derive(Debug, Clone)]
pub struct TypeMismatch;

macro_rules! impl_into {
    ($x: ty, $y:ident) => {
        impl From<$x> for ArrayImpl {
            fn from(array: $x) -> Self {
                Self::$y(array)
            }
        }

        impl TryFrom<ArrayImpl> for $x {
            type Error = TypeMismatch;

            fn try_from(array: ArrayImpl) -> Result<Self, Self::Error> {
                match array {
                    ArrayImpl::$y(array) => Ok(array),
                    _ => Err(TypeMismatch),
                }
            }
        }

        impl<'a> TryFrom<&'a ArrayImpl> for &'a $x {
           type Error = TypeMismatch;

           fn try_from(array: &'a ArrayImpl) -> Result<Self, Self::Error> {
               match array {
                   ArrayImpl::$y(array) => Ok(array),
                   _ => Err(TypeMismatch),
               }
           }
        }
    };
}

impl_into! { PrimitiveArray<bool>, Bool }
impl_into! { PrimitiveArray<i32>, Int32 }
impl_into! { Utf8Array, Utf8 }


impl ArrayBuilderImpl {

    pub fn new(ty: &DataType) -> Self {
        Self::with_capacity(0, ty)
    }

    pub fn with_capacity(capacity: usize, ty: &DataType) -> Self {
        match ty.kind() {
            DataTypeKind::Bool => Self::Bool(BoolArrayBuilder::with_capacity(capacity)),
            DataTypeKind::Int32 => Self::Int32(I32ArrayBuilder::with_capacity(capacity)),
            DataTypeKind::String => {
                Self::Utf8(Utf8ArrayBuilder::with_capacity(capacity))
            }
            _ => panic!("unsupported data type"),
        }
    }

    pub fn push_n(&mut self, n: usize, v: &DataValue) {
        let _ =
        (0 .. n).into_iter().map(|_| self.push(&(v.clone()))).count();
    }

    pub fn push(&mut self, v: &DataValue) {
        match (self, v) {
            (Self::Bool(a), DataValue::Bool(v)) => a.push(Some(v)),
            (Self::Int32(a), DataValue::Int32(v)) => a.push(Some(v)),
            (Self::Utf8(a), DataValue::String(v)) => a.push(Some(v)),
            (Self::Bool(a), DataValue::Null) => a.push(None),
            (Self::Int32(a), DataValue::Null) => a.push(None),
            (Self::Utf8(a), DataValue::Null) => a.push(None),
            _ => panic!("failed to push value: type mismatch"),
        }
    }

    pub fn push_str(&mut self, s: &str) -> Result<(), ConvertError> {
        let null = s.is_empty();
        match self {
            Self::Bool(a) if null => a.push(None),
            Self::Int32(a) if null => a.push(None),
            Self::Utf8(a) if null => a.push(None),
            _ => panic!("failed to push value: type mismatch"),
        }
        Ok(())
    }

    pub fn append(&mut self, array_impl:& ArrayImpl) {
        match (self, array_impl) {
            (Self::Bool(builder), ArrayImpl::Bool(array)) => builder.append(array),
            (Self::Int32(builder), ArrayImpl::Int32(arr)) => builder.append(arr),
            (Self::Utf8(builder), ArrayImpl::Utf8(arr)) => builder.append(arr),
            _ => panic!("failed to push value: type mismatch"),
        }
    }

    pub fn finish(self) -> ArrayImpl {
        match self {
            ArrayBuilderImpl::Bool(a) => ArrayImpl::Bool(a.finish()),
            ArrayBuilderImpl::Int32(a) => ArrayImpl::Int32(a.finish()),
            ArrayBuilderImpl::Utf8(a) => ArrayImpl::Utf8(a.finish()),
        }
    }

    pub fn take(&mut self) -> ArrayImpl {
        match self {
            ArrayBuilderImpl::Bool(a) =>
                {
                    ArrayImpl::Bool(a.take().into())
                }
            ArrayBuilderImpl::Int32(a) => {
                ArrayImpl::Int32(a.take().into())
            }
            ArrayBuilderImpl::Utf8(a) => {
                ArrayImpl::Utf8(a.take().into())
            }
        }
    }

    pub fn reserve(&mut self, capacity: usize) {
        match self {
            ArrayBuilderImpl::Bool(a) => {
                a.reserve(capacity)
            }
            ArrayBuilderImpl::Int32(a) => {
                a.reserve(capacity)
            }
            ArrayBuilderImpl::Utf8(a) => {
                a.reserve(capacity)
            }
        }
    }
}

impl ArrayImpl {
    pub fn get(&self, idx: usize) -> DataValue {
        match self {
            ArrayImpl::Bool(a) => match a.get(idx) {
                Some(val) => DataValue::Bool(*val),
                None => DataValue::Null,
            },
            ArrayImpl::Int32(a) => match a.get(idx) {
                Some(val) => DataValue::Int32(*val),
                None => DataValue::Null,
            },
            ArrayImpl::Utf8(a) => match a.get(idx) {
                Some(val) => DataValue::String(val.to_string()),
                None => DataValue::Null,
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ArrayImpl::Bool(a) => a.len(),
            ArrayImpl::Int32(a) => a.len(),
            ArrayImpl::Utf8(a) => a.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl From<&DataValue> for ArrayImpl {

    fn from(value: &DataValue) -> Self {
        match value {
            DataValue::Null => Self::Int32([None].into_iter().collect()),
            &DataValue::Bool(v) => Self::Bool([v].into_iter().collect()),
            &DataValue::Int32(v) => Self::Int32([v].into_iter().collect()),
            DataValue::String(v) => Self::Utf8([Some(v)].into_iter().collect()),
        }
    }
}



/// Implement dispatch functions for `ArrayBuilderImpl`.
macro_rules! impl_array_builder {
    ([], $( { $Abc:ident, $Type:ty, $abc:ident, $AbcArray:ty, $AbcArrayBuilder:ty, $Value:ident, $Pattern:pat } ),*) => {
        impl ArrayBuilderImpl {
            /// Reserve at least `capacity` values.
            pub fn reserve(&mut self, capacity: usize) {
                match self {
                    Self::Null(a) => a.reserve(capacity),
                    $(
                        Self::$Abc(a) => a.reserve(capacity),
                    )*
                }
            }

            /// Create a new array builder with the same type of given array.
            pub fn from_type_of_array(array: &ArrayImpl) -> Self {
                match array {
                    ArrayImpl::Null(_) => Self::Null(NullArrayBuilder::new()),
                    $(
                        ArrayImpl::$Abc(_) => Self::$Abc(<$AbcArrayBuilder>::new()),
                    )*
                }
            }

            /// Create a new array builder with data type
            pub fn with_capacity(capacity: usize, ty: &DataType) -> Self {
                use DataTypeKind::*;
                match ty.kind() {
                    Null => Self::Null(NullArrayBuilder::with_capacity(capacity)),
                    Struct(_) => todo!("array of Struct type"),
                    $(
                        $Pattern => Self::$Abc(<$AbcArrayBuilder>::with_capacity(capacity)),
                    )*
                }
            }

            /// Appends an element to the back of array.
            pub fn push(&mut self, v: &DataValue) {
                match (self, v) {
                    (Self::Null(a), DataValue::Null) => a.push(None),
                    $(
                        (Self::$Abc(a), DataValue::$Value(v)) => a.push(Some(v)),
                        (Self::$Abc(a), DataValue::Null) => a.push(None),
                    )*
                    _ => panic!("failed to push value: type mismatch"),
                }
            }

            /// Appends an element `n` times to the back of array.
            pub fn push_n(&mut self, n: usize, v: &DataValue) {
                match (self, v) {
                    (Self::Null(a), DataValue::Null) => a.push_n(n, None),
                    $(
                        (Self::$Abc(a), DataValue::$Value(v)) => a.push_n(n, Some(v)),
                        (Self::$Abc(a), DataValue::Null) => a.push_n(n, None),
                    )*
                    _ => panic!("failed to push value: type mismatch"),
                }
            }

            /// Take all elements to a new array.
            pub fn take(&mut self) -> ArrayImpl {
                match self {
                    Self::Null(a) => ArrayImpl::Null(a.take().into()),
                    $(
                        Self::$Abc(a) => ArrayImpl::$Abc(a.take().into()),
                    )*
                }
            }

            /// Finish build and return a new array.
            pub fn finish(self) -> ArrayImpl {
                match self {
                    Self::Null(a) => ArrayImpl::Null(a.finish().into()),
                    $(
                        Self::$Abc(a) => ArrayImpl::$Abc(a.finish().into()),
                    )*
                }
            }

            /// Appends an `ArrayImpl`
            pub fn append(&mut self, array_impl: &ArrayImpl) {
                match (self, array_impl) {
                    (Self::Null(builder), ArrayImpl::Null(arr)) => builder.append(arr),
                    $(
                        (Self::$Abc(builder), ArrayImpl::$Abc(arr)) => builder.append(arr),
                    )*
                    _ => panic!("failed to push value: type mismatch"),
                }
            }
        }
    }
}
















