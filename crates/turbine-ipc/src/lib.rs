//! `turbine-ipc` — Component 6 control plane (plan §9.2).
//!
//! Length-prefixed bincode frames over a Unix domain socket (loopback TCP fallback
//! on non-UNIX). The daemon (`turbine start`) runs [`server::serve`], handing each
//! [`Request`] to its command loop and replying with a [`Response`]; the CLI
//! subcommands (`run` / `stop` / `status`) are [`client::request`] callers.
//!
//! This crate is pure transport + schema: it has no Solana/engine dependencies, so
//! the protocol stays small and stable.

pub mod client;
pub mod frame;
pub mod proto;
pub mod server;

pub use client::request;
pub use frame::{decode, encode, read_frame, recv, send, write_frame, IpcError, Result};
pub use proto::{Request, Response, RunResult, StatusSnapshot};
pub use server::{serve, CommandTx};
