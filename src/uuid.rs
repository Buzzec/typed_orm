use crate::{DataType, error::Result, sqlite::SQLiteColumn};
use rusqlite::types::{ToSqlOutput, ValueRef};
use std::borrow::Cow;
use uuid::Uuid;

impl<'a> DataType<'a> for Uuid {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "BLOB").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        Ok(Uuid::from_bytes(<[u8; 16]>::try_from(value.as_blob()?)?))
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(*self)
    }
}
