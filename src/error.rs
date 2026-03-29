use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Rusqlite error: {0}")]
    Rusqlite(#[from] rusqlite::Error),
    #[error("FromSql error: {0}")]
    FromSql(#[from] rusqlite::types::FromSqlError),
    #[cfg(feature = "serde_json")]
    #[error("Serde json error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[cfg(feature = "postcard")]
    #[error("Postcard error: {0}")]
    Postcard(#[from] postcard::Error),
    #[error("Try from slice error: {0}")]
    TryFromSlice(#[from] std::array::TryFromSliceError),
    #[error("Try from int error: {0}")]
    TryFromInt(#[from] std::num::TryFromIntError),
    #[error("User error: {0}")]
    User(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
