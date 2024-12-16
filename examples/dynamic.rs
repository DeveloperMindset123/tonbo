use std::fs;
use std::sync::Arc;

use fusio::path::Path;
use tonbo::executor::tokio::TokioExecutor;
use tonbo::record::{Column, ColumnDesc, Datatype, DynRecord};
use tonbo::{DbOption, DB};

#[tokio::main]
async fn main() {
    fs::create_dir_all("./db_path/users").unwrap();

    let options = DbOption::with_path(
        Path::from_filesystem_path("./db_path/users").unwrap(),
        "id".to_owned(),
        0,
    );
    let db = DB::with_schema(
        options,
        TokioExecutor::new(),
        vec![
            ColumnDesc::new("foo".into(), Datatype::String, false),
            ColumnDesc::new("bar".into(), Datatype::Int32, true),
        ],
        0,
    )
    .await
    .unwrap();

    {
        let mut txn = db.transaction().await;
        txn.insert(DynRecord::new(
            vec![
                Column::new(Datatype::String, "foo".into(), Arc::new("hello"), false),
                Column::new(Datatype::Int32, "bar".into(), Arc::new(1), true),
            ],
            0,
        ));

        txn.commit().await.unwrap();
    }

    // db.get(
    //     &Value::new(Datatype::String, "foo".into(), Arc::new("hello"), false),
    //     |v| {
    //         let v = v.get();
    //         println!("{:?}", v.columns[0].value.downcast_ref::<String>());
    //         Some(())
    //     },
    // )
    // .await
    // .unwrap();
}
