use std::fmt::Debug;

pub trait NativeType:
PartialOrd + PartialEq + Debug + Copy + Send + Sync + Sized + Default + 'static
{
}

pub trait NumericType:
PartialOrd + PartialEq + Debug + Copy + Send + Sync + Sized + Default + 'static
{
}

macro_rules! impl_native {
    ($($t:ty),*) => {
        $(impl NativeType for $t {})*
    }
}

macro_rules! impl_numeric {
    ($($t:ty),*) => {
        $(impl NumericType for $t {})*
    }
}

#[rustfmt::skip]
impl_native!(
    u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, f32, f64, bool,()
);

impl_numeric!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, f32, f64, bool);