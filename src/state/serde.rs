use bytes::{Buf, BufMut, BytesMut};
use smallvec::SmallVec;
use crate::types::{DataType, DataTypeKind, DataValue};

pub fn serialize_row(row: &[DataValue]) -> Vec<u8> {
    let  buf = &mut BytesMut::new();
    for val in row {
        serialize_data_value(val, buf);
    }
    buf.to_vec()
}

pub fn deserialize_row(data_types: &[DataType],  data : &mut BytesMut) -> SmallVec<[DataValue; 12]> {
    let mut result = SmallVec::new();
    for data_type in data_types {
        result.push(deserialize_data_value(data_type, data));
    }
    result
}

fn serialize_data_value(date_value: &DataValue, buf:  &mut BytesMut) {
    match date_value {
        DataValue::Int32(v) => {
            buf.put_i32_le(*v);
        }
        _ => {
            todo!()
        }
    }
}

fn deserialize_data_value(data_type: &DataType, data: &mut BytesMut) -> DataValue {
    match data_type.kind() {
        DataTypeKind::Int32 => {
            DataValue::Int32(data.get_i32_le())
        },
        _ => todo!()
    }
}

#[cfg(test)]
mod tests {
    
    use smallvec::SmallVec;
    use crate::state::serde::{serialize_row};
    use crate::types::{DataValue};

    #[test]
    fn t1() {
        let mut vec = SmallVec::<[DataValue; 4]>::new();
        vec.push(DataValue::Int32(32));
        let t1 =
        serialize_row(vec.as_slice());
        println!("{:?}", t1);

        // let data_types = vec![DataType::new_nullable(DataTypeKind::Int(None))];
        // let buf = &mut BytesMut::from(t1.as_slice());
        // let f = deserialize_row(&data_types, buf);
        // println!("{:?}", f);
    }
}