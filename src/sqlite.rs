use crate::{DBConnection, DataType, Result, SqlStatement, Table};
use bytemuck::bytes_of;
use rusqlite::{
    Connection, Rows, Statement,
    types::{ToSqlOutput, Value, ValueRef},
};
use std::{borrow::Cow, marker::PhantomData, mem::ManuallyDrop};

impl DBConnection for Connection {
    fn create_table<T: Table>(&mut self) -> Result<()> {
        self.execute(T::create_table_stmt().sql().as_ref(), [])?;
        Ok(())
    }

    fn select_all<T: Table>(&mut self) -> Result<RowIter<'_, T>> {
        RowIter::new(
            self.prepare(&format!("SELECT * FROM {}", T::table_name()))?,
            |stmt| stmt.query([]).map_err(Into::into),
        )
    }

    fn insert<'a, T: 'a + Table>(&mut self, values: impl IntoIterator<Item = &'a T>) -> Result<()> {
        let columns = T::columns(None, "__").map(|c| c.name).collect::<Vec<_>>();
        let parameters = (0..columns.len())
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");
        let columns = columns.join(", ");
        let mut stmt = self.prepare(&format!(
            "INSERT INTO {} ({columns}) VALUES ({parameters})",
            T::table_name()
        ))?;
        for value in values {
            stmt.execute(value.to_params()?)?;
        }
        Ok(())
    }
}
pub struct RowIter<'conn, T> {
    stmt: *mut Statement<'conn>,
    rows: ManuallyDrop<Rows<'conn>>,
    phantom_t: PhantomData<fn() -> T>,
}
impl<'conn, T> RowIter<'conn, T> {
    pub fn new(
        stmt: Statement<'conn>,
        rows: impl for<'a> FnOnce(&'a mut Statement<'conn>) -> Result<Rows<'a>>,
    ) -> Result<Self> {
        let stmt = Box::into_raw(Box::new(stmt));
        let rows = ManuallyDrop::new(rows(unsafe { &mut *stmt })?);
        Ok(Self {
            stmt,
            rows,
            phantom_t: PhantomData,
        })
    }
}
impl<'conn, T: Table> Iterator for RowIter<'conn, T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(
            self.rows
                .next()
                .transpose()?
                .map_err(Into::into)
                .and_then(|row| <T as Table>::from_row(row)),
        )
    }
}
impl<'conn, T> Drop for RowIter<'conn, T> {
    fn drop(&mut self) {
        unsafe { ManuallyDrop::drop(&mut self.rows) };
        drop(unsafe { Box::from_raw(self.stmt) });
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SQLiteColumn {
    pub name: Cow<'static, str>,
    pub type_name: Cow<'static, str>,
    pub not_null: Option<Option<ConflictClause>>,
}
impl SQLiteColumn {
    pub fn new(name: Cow<'static, str>, type_name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            name,
            type_name: type_name.into(),
            not_null: None,
        }
    }

    pub fn not_null(mut self, conflict_clause: impl Into<Option<ConflictClause>>) -> Self {
        self.not_null = Some(conflict_clause.into());
        self
    }

    pub fn nullable(mut self) -> Self {
        self.not_null = None;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SQLiteCreateTableStmt<I> {
    pub table_name: Cow<'static, str>,
    pub columns: I,
    pub if_not_exists: bool,
    pub strict: bool,
    pub without_rowid: bool,
}
impl<I: Iterator<Item = SQLiteColumn>> SqlStatement for SQLiteCreateTableStmt<I> {
    fn sql(self) -> Cow<'static, str> {
        let Self {
            table_name,
            columns,
            if_not_exists,
            strict,
            without_rowid,
        } = self;

        let if_not_exists = if if_not_exists { "IF NOT EXISTS" } else { "" };

        let columns = columns
            .map(
                |SQLiteColumn {
                     name,
                     type_name,
                     not_null,
                 }| match not_null {
                    None => format!("{name} {type_name}"),
                    Some(None) => format!("{name} {type_name} NOT NULL"),
                    Some(Some(c)) => {
                        format!("{name} {type_name} NOT NULL ON CONFLICT {}", c.sql())
                    }
                },
            )
            .collect::<Vec<_>>()
            .join(", ");
        let strict = if strict { "STRICT" } else { "" };
        let without_rowid = if without_rowid { "WITHOUT ROWID" } else { "" };

        format!(
            "CREATE TABLE {if_not_exists} \"{table_name}\" ({columns}) {strict} {without_rowid}",
        )
        .into()
    }
}

impl<'a> DataType<'a> for String {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "TEXT").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        value.as_str().map(str::to_string).map_err(Into::into)
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(self.as_str())
    }
}
impl<'a> DataType<'a> for &'a str {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "TEXT").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        value.as_str().map_err(Into::into)
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(*self)
    }
}
impl<'a> DataType<'a> for Cow<'a, str> {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "TEXT").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        value.as_str().map(Cow::from).map_err(Into::into)
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(self.as_ref())
    }
}
impl<'a> DataType<'a> for u8 {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "INTEGER").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        Ok(value.as_i64()?.try_into()?)
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(*self)
    }
}
impl<'a> DataType<'a> for u16 {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "INTEGER").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        Ok(value.as_i64()?.try_into()?)
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(*self)
    }
}
impl<'a> DataType<'a> for u32 {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "INTEGER").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        Ok(value.as_i64()?.try_into()?)
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(*self)
    }
}
impl<'a> DataType<'a> for u64 {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "BLOB").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        Ok(u64::from_le_bytes(value.as_blob()?.try_into()?))
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(self.to_le_bytes().to_vec())
    }
}
impl<'a> DataType<'a> for u128 {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "BLOB").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        Ok(u128::from_le_bytes(value.as_blob()?.try_into()?))
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(self.to_le_bytes().to_vec())
    }
}
impl<'a> DataType<'a> for i8 {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "INTEGER").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        Ok(value.as_i64()?.try_into()?)
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(*self)
    }
}
impl<'a> DataType<'a> for i16 {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "INTEGER").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        Ok(value.as_i64()?.try_into()?)
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(*self)
    }
}
impl<'a> DataType<'a> for i32 {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "INTEGER").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        Ok(value.as_i64()?.try_into()?)
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(*self)
    }
}
impl<'a> DataType<'a> for i64 {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "INTEGER").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        Ok(value.as_i64()?)
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(*self)
    }
}
impl<'a> DataType<'a> for i128 {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "BLOB").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        Ok(i128::from_le_bytes(value.as_blob()?.try_into()?))
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        if cfg!(target_endian = "little") {
            Ok(ToSqlOutput::Borrowed(bytes_of(self).into()))
        } else {
            Ok(ToSqlOutput::Owned(self.to_le_bytes().to_vec().into()))
        }
    }
}
impl<'a> DataType<'a> for f64 {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "REAL").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        Ok(value.as_f64()?)
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(*self)
    }
}
impl<'a> DataType<'a> for Vec<u8> {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "BLOB").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        Ok(value.as_blob()?.to_vec())
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(self.as_slice())
    }
}
impl<'a> DataType<'a> for &'a [u8] {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "BLOB").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        Ok(value.as_blob()?)
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(*self)
    }
}
impl<'a> DataType<'a> for bool {
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        SQLiteColumn::new(name.into(), "INTEGER").not_null(None)
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        Ok(value.as_i64()? != 0)
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        Ok(*self)
    }
}
impl<'a, T> DataType<'a> for Option<T>
where
    T: DataType<'a>,
{
    fn sql_data_type(name: impl Into<Cow<'static, str>>) -> SQLiteColumn {
        T::sql_data_type(name).nullable()
    }

    fn from_sql_value(value: ValueRef<'a>) -> Result<Self> {
        match value {
            ValueRef::Null => Ok(None),
            value => T::from_sql_value(value).map(Some),
        }
    }

    fn to_sql_value(&self) -> Result<impl Into<ToSqlOutput<'_>>> {
        match self {
            None => Ok(ToSqlOutput::Owned(Value::Null)),
            Some(v) => T::to_sql_value(v).map(Into::into),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum AscDesc {
    Asc,
    Desc,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct PrimaryKeyOptions {
    pub asc_desc: Option<AscDesc>,
    pub conflict_clause: Option<ConflictClause>,
    pub autoincrement: bool,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum ConflictClause {
    Rollback,
    Abort,
    Fail,
    Ignore,
    Replace,
}
impl ConflictClause {
    pub fn sql(&self) -> &'static str {
        match self {
            Self::Rollback => "ROLLBACK",
            Self::Abort => "ABORT",
            Self::Fail => "FAIL",
            Self::Ignore => "IGNORE",
            Self::Replace => "REPLACE",
        }
    }
}
