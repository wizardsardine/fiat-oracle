use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("toml error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("http: {0}")]
    Http(Box<reqwest::Error>),
    #[error("redb: {0}")]
    RedbDatabase(Box<redb::DatabaseError>),
    #[error("redb: {0}")]
    RedbTransaction(Box<redb::TransactionError>),
    #[error("redb: {0}")]
    RedbTable(Box<redb::TableError>),
    #[error("redb: {0}")]
    RedbStorage(Box<redb::StorageError>),
    #[error("redb: {0}")]
    RedbCommit(Box<redb::CommitError>),
    #[error("{0}")]
    Other(String),
    // Constructed by `spot::median_spot_price`, which is currently a
    // test-only convenience (production live-poll uses run_backfill).
    #[allow(dead_code)]
    #[error("fewer than {0} price sources responded")]
    InsufficientSources(usize),
    #[error("unknown currency: {0}")]
    UnknownCurrency(String),
    #[error("currency {0} not found in cache")]
    MissingCurrency(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<redb::DatabaseError> for Error {
    fn from(e: redb::DatabaseError) -> Self {
        Error::RedbDatabase(Box::new(e))
    }
}

impl From<redb::TransactionError> for Error {
    fn from(e: redb::TransactionError) -> Self {
        Error::RedbTransaction(Box::new(e))
    }
}

impl From<redb::TableError> for Error {
    fn from(e: redb::TableError) -> Self {
        Error::RedbTable(Box::new(e))
    }
}

impl From<redb::StorageError> for Error {
    fn from(e: redb::StorageError) -> Self {
        Error::RedbStorage(Box::new(e))
    }
}

impl From<redb::CommitError> for Error {
    fn from(e: redb::CommitError) -> Self {
        Error::RedbCommit(Box::new(e))
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Http(Box::new(e))
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Other(s.to_string())
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Other(s)
    }
}
