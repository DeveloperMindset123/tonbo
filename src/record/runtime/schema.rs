use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use parquet::{format::SortingColumn, schema::types::ColumnPath};

use super::{array::DynRecordImmutableArrays, DynRecord, ValueDesc};
use crate::{
    magic,
    record::{PrimaryKey, Schema},
};

#[derive(Debug)]
pub struct DynSchema {
    schema: Vec<ValueDesc>,
    primary_index: Vec<usize>,
    arrow_schema: Arc<ArrowSchema>,
}

impl DynSchema {
    pub fn new(schema: Vec<ValueDesc>, primary_index: Vec<usize>) -> Self {
        let primary_key = primary_index
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<String>>()
            .join(",");

        let mut metadata = HashMap::new();
        metadata.insert("primary_key_index".to_string(), primary_key);
        // metadata.insert("primary_key_index".to_string(), primary_index.to_string());
        let arrow_schema = Arc::new(ArrowSchema::new_with_metadata(
            [
                Field::new("_null", DataType::Boolean, false),
                Field::new(magic::TS, DataType::UInt32, false),
            ]
            .into_iter()
            .chain(schema.iter().map(|desc| desc.arrow_field()))
            .collect::<Vec<_>>(),
            metadata,
        ));
        Self {
            schema,
            primary_index,
            arrow_schema,
        }
    }
}

impl Schema for DynSchema {
    type Record = DynRecord;

    type Columns = DynRecordImmutableArrays;

    type Key = PrimaryKey;

    fn arrow_schema(&self) -> &Arc<ArrowSchema> {
        &self.arrow_schema
    }

    fn primary_key_index(&self) -> Vec<usize> {
        self.primary_index
            .iter()
            .map(|idx| idx + 2)
            .collect::<Vec<usize>>()
    }

    fn primary_key_path(&self) -> (ColumnPath, Vec<SortingColumn>) {
        let mut column_path = Vec::with_capacity(self.primary_index.len() + 1);
        let mut sorting_column = Vec::with_capacity(self.primary_index.len() + 1);
        column_path.push(magic::TS.to_string());
        sorting_column.push(SortingColumn::new(1_i32, true, true));
        for idx in self.primary_index.iter() {
            column_path.push(self.schema[*idx].name.clone());
            sorting_column.push(SortingColumn::new(*idx as i32 + 2, false, true));
        }
        (ColumnPath::new(column_path), sorting_column)
    }
}

/// Creates a [`DynSchema`] from literal slice of values and primary key index, suitable for rapid
/// testing and development.
///
/// ## Example:
///
/// ```no_run
/// // dyn_schema!(
/// //      (name, type, nullable),
/// //         ......
/// //      (name, type, nullable),
/// //      primary_key_index
/// // );
/// use tonbo::dyn_schema;
///
/// let schema = dyn_schema!(
///     ("foo", String, false),
///     ("bar", Int32, true),
///     ("baz", UInt64, true),
///     0
/// );
/// ```
#[macro_export]
macro_rules! dyn_schema {
    ($(($name: expr, $type: ident, $nullable: expr )),*, $primary: literal) => {
        {
            $crate::record::DynSchema::new(
                vec![
                    $(
                        $crate::record::ValueDesc::new($name.into(), $crate::record::DataType::$type, $nullable),
                    )*
                ],
                vec![$primary],
            )
        }
    }
}
