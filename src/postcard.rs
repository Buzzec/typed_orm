use crate::{DataType, error::Result, sqlite::SQLiteColumn};
use derive_more::From;
use rusqlite::types::{ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, From, Serialize, Deserialize,
)]
pub struct Postcard<T>(pub T);
impl<'a, T> DataType<'a> for Postcard<T>
where
    T: 'static + Serialize + Deserialize<'a>,
{
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "BLOB").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        postcard::from_bytes(value.as_blob()?).map_err(Into::into)
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        postcard::to_allocvec(&self.0).map_err(Into::into)
    }
}
