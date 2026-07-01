//! Telemetry events carried on the lossy `tokio::sync::broadcast` bus from the
//! hot/cold paths to the TUI and web UI. Defined here in Phase 0 so every later
//! phase emits into one stable, serializable schema.
//!
//! Design split (plan §9.3 / §9.4):
//! - The **TUI** consumes only the live/current-state variants and renders them
//!   in a short, dense grid.
//! - The **Web UI** consumes everything, including the long/verbose payloads
//!   (transaction history, per-state deltas, AI reasoning, aggregate stats).

use serde::{Deserialize, Serialize};

use crate::types::{Congestion, LifecycleState};

/// A single event on the telemetry bus. Tagged for clean JSON over the WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum TelemetryEvent {
    /// New slot observed from Geyser (with status: processed/confirmed/finalized).
    Slot {
        slot: u64,
        parent: Option<u64>,
        status: String,
        /// Wall-clock interval since the previous slot tick, if known.
        interval_ms: Option<u64>,
    },

    /// Upcoming Jito-enabled leader window.
    ///
    /// `ready` is true when the next Jito leader is within `[gate_min, gate_max]`
    /// slots — i.e. the submission window is open. With `gate_min = gate_max = 1`
    /// this is only true one slot before the Jito leader.
    Leader {
        next_jito_leader_slot: u64,
        slots_until: i64,
        ready: bool,
        identity: Option<String>,
    },

    /// Smoothed Jito tip percentile snapshot (lamports).
    TipSnapshot {
        p25: u64,
        p50: u64,
        p75: u64,
        p95: u64,
        p99: u64,
        ema50: u64,
    },

    /// The live fee decision the engine would make right now: max contention
    /// across watched accounts → percentile tier → bumped/clamped tip (lamports).
    /// `watching` is false when no accounts are configured (no contention signal,
    /// so the tier defaults to the floor).
    Bid {
        congestion: Congestion,
        percentile: String,
        tip_lamports: u64,
        watching: bool,
    },

    /// Per-watched-account contention sample (TUI: gauge; Web: trend).
    ///
    /// `fast_ema` is the smoothed write-lock count *per slot* (i.e. how many target
    /// transactions are contending for this account right now); `total_hits` is the
    /// lifetime count. `zscore` drives the Quiet/Moderate/Hot tier in the background
    /// (the TUI surfaces the human-readable counts, not the z-score).
    Contention {
        account: String,
        fast_ema: f64,
        slow_ema: f64,
        zscore: f64,
        total_hits: u64,
        level: Congestion,
    },

    /// A bundle lifecycle transition. The optional fields carry the long/verbose
    /// detail the Web UI renders (history, deltas, explorer links).
    BundleState {
        bundle_id: Option<String>,
        /// First transaction signature, used as the explorer link + row key.
        primary_signature: Option<String>,
        state: LifecycleState,
        /// Tip paid (lamports) for this bundle.
        tip_lamports: Option<u64>,
        /// Percentile tier selected for the tip, e.g. "p95".
        percentile: Option<String>,
        /// Slot the bundle landed in.
        landed_slot: Option<u64>,
        /// Cumulative ms since `Built` for the current state (per-state delta).
        elapsed_ms: Option<u64>,
        attempt: u8,
    },

    /// AI analyst decision (Web-only reasoning log; not shown in the TUI).
    AiDecision {
        bundle_id: Option<String>,
        tier: u8,
        classification: String,
        root_cause: String,
        adjustments: String,
        should_retry: bool,
        confidence: f64,
    },

    /// Subsystem health flags (TUI status indicators).
    Health {
        geyser: bool,
        jito: bool,
        /// True when contention is fed by SubscribeDeshred (omitted from JSON when false).
        #[serde(default)]
        deshred_active: bool,
    },

    /// Aggregate live counters for the TUI status strip (the web UI renders the
    /// full per-bundle history; this is just the at-a-glance roll-up).
    Stats {
        in_flight: u64,
        tracked: u64,
        ai_decisions: u64,
        killed: bool,
        dry_run: bool,
    },

    /// Free-form log line (Web console).
    Log { level: String, message: String },
}
