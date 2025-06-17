use std::{cmp::Ordering, hash::Hash, sync::Arc};

use fusio_log::{Decode, Encode};

use super::{Key, KeyRef};
use crate::record::{DynSchema, Value};

#[derive(Debug, Clone)]
pub struct PrimaryKey {
    keys: Vec<Value>,
}

impl PrimaryKey {
    pub fn new(keys: Vec<Value>) -> Self {
        Self { keys }
    }

    pub fn prefix_match(_schema: &DynSchema) -> bool {
        true
    }
}

impl Key for PrimaryKey {
    type Ref<'r> = PrimaryKey;

    fn as_key_ref(&self) -> Self::Ref<'_> {
        todo!()
    }

    fn to_arrow_datum(&self) -> Arc<dyn arrow::array::Datum> {
        todo!()
    }
}

impl<'r> KeyRef<'r> for PrimaryKey {
    type Key = PrimaryKey;

    fn to_key(self) -> Self::Key {
        todo!()
    }
}

impl Encode for PrimaryKey {
    type Error = fusio::Error;

    async fn encode<W>(&self, _writer: &mut W) -> Result<(), Self::Error>
    where
        W: fusio::Write,
    {
        todo!()
    }

    fn size(&self) -> usize {
        todo!()
    }
}

impl Decode for PrimaryKey {
    type Error = fusio::Error;

    async fn decode<R>(_reader: &mut R) -> Result<Self, Self::Error>
    where
        R: fusio::SeqRead,
    {
        todo!()
    }
}

impl PartialOrd for PrimaryKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrimaryKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        for (left, right) in self.keys.iter().zip(other.keys.iter()) {
            let ordering = left.cmp(right);
            if !ordering.is_eq() {
                return ordering;
            }
        }
        Ordering::Equal
    }
}

impl Eq for PrimaryKey {}

impl PartialEq for PrimaryKey {
    fn eq(&self, other: &Self) -> bool {
        for (left, right) in self.keys.iter().zip(other.keys.iter()) {
            if !left.eq(right) {
                return false;
            }
        }
        true
    }
}

impl Hash for PrimaryKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.keys.hash(state);
    }
}

impl From<Value> for PrimaryKey {
    fn from(value: Value) -> Self {
        Self { keys: vec![value] }
    }
}
