//! Domain enums shared across components. Kept here so ingestion, processing,
//! execution, AI, TUI, and web all agree on the same vocabulary.

use serde::{Deserialize, Serialize};

/// Localized account-contention level, derived from the write-lock EMA z-score.
/// Ordered so `max()` over a bundle's write-locked accounts picks the bottleneck.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Congestion {
    Quiet,
    Moderate,
    Hot,
}

impl Congestion {
    /// Map a congestion level to the Jito tip percentile tier (plan §7.1).
    pub fn target_percentile(self) -> Percentile {
        match self {
            Congestion::Quiet => Percentile::P25,
            Congestion::Moderate => Percentile::P75,
            Congestion::Hot => Percentile::P95,
        }
    }
}

/// Jito landed-tip percentile buckets, matching the `tip_floor` / `tip_stream` schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Percentile {
    P25,
    P50,
    P75,
    P95,
    P99,
}

impl Percentile {
    /// Short uppercase label, e.g. `P95`.
    pub fn label(self) -> &'static str {
        match self {
            Percentile::P25 => "P25",
            Percentile::P50 => "P50",
            Percentile::P75 => "P75",
            Percentile::P95 => "P95",
            Percentile::P99 => "P99",
        }
    }
}

/// Lifecycle of an outbound bundle/transaction, tracked via Geyser + Jito results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleState {
    Built,
    Submitted,
    Processed,
    Confirmed,
    Finalized,
    Failed,
}

/// The observed failure class (from the Jito reject/drop reason, an on-chain error,
/// or a submit timeout). It is passed to the AI analyst as a *hint* only — the AI
/// performs the authoritative classification and decides the fix (plan §8, revised:
/// every failure is routed to the AI; there is no deterministic fix path).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureClass {
    BlockhashExpired,
    TipTooLow,
    AuctionTimeout,
    BundleDropped,
    Slippage,
    AccountInUse,
    ComputeBudgetExceeded,
    SimulationError,
    ProgramCustom(u32),
    Transient,
    Unknown,
}
