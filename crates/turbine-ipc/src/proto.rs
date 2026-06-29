//! Control-plane message schema (plan §9.2).
//!
//! These are plain, dependency-light DTOs so the transport crate stays free of
//! Solana/engine types. The daemon translates between these and its internal
//! state; the CLI client renders them.

use serde::{Deserialize, Serialize};

/// A request from a CLI client to the daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Build + submit a mock scenario (`happy-*` runs the pipeline; `fail-*`
    /// injects a synthetic failure to exercise the AI retry coordinator).
    RunScenario { scenario: String },
    /// Health snapshot.
    Status,
    /// Graceful shutdown of the daemon.
    Stop,
    /// Liveness probe.
    Ping,
}

/// A response from the daemon to a CLI client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Ack { message: String },
    RunResult(RunResult),
    Status(StatusSnapshot),
    Error { message: String },
    Pong,
}

/// Outcome of a `RunScenario` request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub label: String,
    pub tracking_id: u64,
    /// Gate outcome: `open` / `timed_out` / `stream_closed`.
    pub gate: String,
    pub tip_lamports: u64,
    pub tx_count: usize,
    pub bundle_id: Option<String>,
    pub dry_run: bool,
    /// True when the bundle actually went on the wire (gate opened).
    pub submitted: bool,
    /// Set for `fail-*` scenarios: a synthetic failure was routed to the AI.
    pub failure_injected: bool,
}

/// Daemon health snapshot (plan §9.1 `turbine status`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusSnapshot {
    pub uptime_secs: u64,
    pub slot: u64,
    pub geyser_healthy: bool,
    pub jito_connected: bool,
    pub next_jito_leader_slot: Option<u64>,
    pub slots_until_leader: Option<i64>,
    pub tip_p50: u64,
    pub tip_p95: u64,
    pub in_flight: usize,
    pub ai_decisions: usize,
    pub submission_killed: bool,
    pub dry_run: bool,
}
