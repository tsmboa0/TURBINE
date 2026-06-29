//! Crate-wide error type. Per the plan, each crate defines a `thiserror` enum and
//! `anyhow` is reserved for binary boundaries only.

use thiserror::Error;

/// Convenient result alias used throughout TURBINE crates.
pub type Result<T> = std::result::Result<T, TurbineError>;

/// The unified error type for TURBINE. Variants are coarse-grained by component
/// so callers can match on the failing subsystem without leaking internals.
#[derive(Debug, Error)]
pub enum TurbineError {
    #[error("config error: {0}")]
    Config(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("toml parse error: {0}")]
    TomlParse(#[from] toml::de::Error),

    #[error("invalid pubkey: {0}")]
    Pubkey(String),

    #[error("ingestion error: {0}")]
    Ingest(String),

    #[error("processing error: {0}")]
    Process(String),

    #[error("execution error: {0}")]
    Execute(String),

    #[error("ai error: {0}")]
    Ai(String),

    #[error("ipc error: {0}")]
    Ipc(String),

    #[error("{0}")]
    Other(String),
}
