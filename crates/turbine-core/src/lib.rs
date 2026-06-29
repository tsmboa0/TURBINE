//! `turbine-core` — the foundation crate for TURBINE.
//!
//! Holds the pieces every other crate depends on:
//! - [`config`]: the single typed `Config` loaded from TOML (+ env overrides).
//! - [`error`]: the crate-wide [`TurbineError`] and [`Result`] alias.
//! - [`types`]: domain enums shared across components (congestion, percentile,
//!   lifecycle, failure classes).
//! - [`events`]: the [`events::TelemetryEvent`] type carried on the lossy
//!   broadcast bus to the TUI and web UI.
//!
//! This crate is deliberately lightweight and free of heavy/native deps so the
//! workspace scaffold compiles before the gRPC/proto crates are introduced.

pub mod ai;
pub mod blockhash;
pub mod config;
pub mod contention;
pub mod ema;
pub mod error;
pub mod events;
pub mod leader;
pub mod tips;
pub mod transaction_record;
pub mod types;

pub use error::{Result, TurbineError};
