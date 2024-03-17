use std::mem;
use bitvec::vec::BitVec;
use super::{Array, ArrayBuilder};

#[derive(Clone, PartialEq)]
pub struct Utf8Array {
    offset: Vec<usize>,
    valid: BitVec,
    data: Vec<u8>
}


impl Array for Utf8Array {

    type Builder = Utf8ArrayBuilder;
    type Item = str;

    fn get(&self, idx: usize) -> Option<&Self::Item> {
        if self.valid[idx] {
            let data_splice = &self.data[self.offset[idx]..self.offset[idx + 1]];
            Some(unsafe {std::str::from_utf8_unchecked(data_splice) })
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.valid.len()
    }
}

pub struct Utf8ArrayBuilder {
    offset: Vec<usize>,
    valid: BitVec,
    data: Vec<u8>
}

impl ArrayBuilder for Utf8ArrayBuilder {
    type Array = Utf8Array;

    fn with_capacity(capacity: usize) -> Self {
        let mut offset = Vec::with_capacity(capacity + 1);
        offset.push(0);
        Self {
            offset,
            data: Vec::with_capacity(capacity),
            valid: BitVec::with_capacity(capacity),
        }
    }

    fn push(&mut self, value: Option<&str>) {
        self.valid.push(value.is_some());
        if let Some(x) = value {
            self.data.extend_from_slice(x.as_bytes());
        }
        self.offset.push(self.data.len());
    }

    fn append(&mut self, other: &Utf8Array) {
        self.valid.extend_from_bitslice(&other.valid);
        self.data.extend_from_slice(&other.data);
        let start = *self.offset.last().unwrap();
        for other_offset in &other.offset[1..] {
            self.offset.push(*other_offset + start);
        }
    }

    fn finish(self) -> Self::Array {
        Utf8Array {
            valid: self.valid,
            data: self.data,
            offset: self.offset
        }
    }

    fn reserve(&mut self, capacity: usize) {
        self.offset.reserve(capacity + 1);
        self.valid.reserve(capacity);
        // For variable-length values, we cannot know the exact size of the value.
        // Therefore, we reserve `capacity` here, but it may overflow during use.
        self.data.reserve(capacity);
    }

    fn take(&mut self) -> Self::Array {
        Utf8Array {
            valid: mem::take(&mut self.valid),
            data: mem::take(&mut self.data),
            offset: mem::replace(&mut self.offset, vec![0]),
        }
    }

}

impl<Str: AsRef<str>> FromIterator<Option<Str>> for Utf8Array {

    fn from_iter<T: IntoIterator<Item=Option<Str>>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::with_capacity(iter.size_hint().0);
        for e  in iter {
            if let Some (s) = e {
                builder.push(Some(s.as_ref()))
            } else {
                builder.push(None)
            }
        }
        builder.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collect() {
        let iter = [None, Some("1"), None, Some("3")].into_iter();
        let array = iter.clone().collect::<Utf8Array>();
        assert_eq!(array.iter().collect::<Vec<_>>(), iter.collect::<Vec<_>>());
    }
}
