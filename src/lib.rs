extern crate self as typed_orm;

pub extern crate rusqlite;

pub mod error;
#[cfg(feature = "serde_json")]
pub mod json;
#[cfg(feature = "postcard")]
pub mod postcard;
pub mod sqlite;
pub mod table_name;
pub mod util;
#[cfg(feature = "uuid")]
pub mod uuid;

use crate::{
    error::Result,
    sqlite::{RowIter, SQLiteColumn},
    util::{ReferenceStruct, RowIdxAccumulator},
};
use rusqlite::{
    Params, Row,
    types::{ToSqlOutput, ValueRef},
};
use std::borrow::Cow;
pub use typed_orm_proc::{Table, TableData};

pub trait DBConnection {
    fn create_table<T: Table>(&mut self) -> Result<()>;

    fn select_all<T: Table>(&mut self) -> Result<RowIter<'_, T>>;
    fn insert<'a, T: 'a + Table>(&mut self, values: impl IntoIterator<Item = &'a T>) -> Result<()>;
}

pub trait SqlStatement {
    fn sql(self) -> Cow<'static, str>;
}
pub trait Table: TableData {
    type PrimaryKey<'a>: ReferenceStruct
    where
        Self: 'a;

    fn table_name() -> Cow<'static, str>;
    fn primary_key(&self) -> Self::PrimaryKey<'_>;

    fn from_row(row: &Row<'_>) -> Result<Self>;
    fn to_params(&self) -> Result<impl Params>;
    fn create_table_stmt() -> impl SqlStatement;
}
pub trait TableData: Sized {
    fn columns(
        parent_name: Option<Cow<str>>,
        name_separator: &str,
    ) -> impl Iterator<Item = SQLiteColumn>;
    fn from_row_inner(row_offset: &mut RowIdxAccumulator, row: &Row<'_>) -> Result<Self>;
    fn sql_output_iter(&self) -> Result<impl Iterator<Item = ToSqlOutput<'_>> + '_>;
    fn column_count() -> usize;
}
pub trait DataType<'a>: Sized {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn;
    fn from_sql_value(value: ValueRef<'a>) -> Result<Self>;
    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>>;
}

#[cfg(test)]
pub mod test {
    use crate::{DBConnection, Table, TableData, error::Result};
    use rand::{
        Rng, RngExt,
        distr::{Alphanumeric, Distribution, SampleString, StandardUniform},
    };

    #[derive(Debug, PartialEq, Eq, Table, TableData)]
    pub struct TestTable<T> {
        #[table(primary_key)]
        pub id: u32,
        pub name: String,
        pub other_data: String,
        #[table(flatten)]
        pub sub_table: SubTable<T>,
    }
    impl<T> Distribution<TestTable<T>> for StandardUniform
    where
        StandardUniform: Distribution<SubTable<T>>,
    {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> TestTable<T> {
            TestTable {
                id: rng.random::<u32>(),
                name: {
                    let len = rng.random_range(10..=15);
                    Alphanumeric.sample_string(rng, len)
                },
                other_data: {
                    let len = rng.random_range(25..=30);
                    Alphanumeric.sample_string(rng, len)
                },
                sub_table: rng.sample(self),
            }
        }
    }

    #[derive(Debug, PartialEq, Eq, TableData)]
    pub struct SubTable<T> {
        pub other: u32,
        pub blob: u128,
        pub data: T,
    }
    impl<T> Distribution<SubTable<T>> for StandardUniform
    where
        StandardUniform: Distribution<T>,
    {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> SubTable<T> {
            SubTable {
                other: rng.random::<u32>(),
                blob: rng.random::<u128>(),
                data: rng.sample(self),
            }
        }
    }

    #[test]
    fn test_flow() -> Result<()> {
        let mut rng = rand::rng();
        let values = (0..20)
            .map(|_| rng.random())
            .collect::<Vec<TestTable<i8>>>();

        let mut conn = rusqlite::Connection::open_in_memory()?;

        conn.create_table::<TestTable<i8>>()?;
        conn.insert(&values)?;
        let rows = conn.select_all::<TestTable<i8>>()?;

        for (row, value) in rows.zip(values) {
            assert_eq!(row?, value);
        }

        assert_eq!(SubTable::<u32>::column_count(), 3);
        assert_eq!(TestTable::<i8>::column_count(), 6);

        Ok(())
    }
}
