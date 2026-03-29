use crate::{DataType, Result, sqlite::SQLiteColumn};
use derive_more::From;
use rusqlite::types::{ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, From, Serialize, Deserialize,
)]
pub struct Json<T>(pub T);

impl<'a, T> DataType<'a> for Json<T>
where
    T: 'static + Serialize + Deserialize<'a>,
{
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "TEXT").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        let text = value.as_str()?;
        serde_json::from_str(text).map_err(Into::into)
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        serde_json::to_string(&self.0).map_err(Into::into)
    }
}
