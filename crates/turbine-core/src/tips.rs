//! Smoothed Jito tip snapshot (plan §6, §7).
//!
//! The authoritative, smoothed tip state read by the fee matrix. Raw percentiles
//! arrive from the Jito tip feed (`turbine-ingest`); `turbine-process` folds each
//! percentile through its own [`crate::ema::DecayEma`] and publishes this snapshot.

use serde::{Deserialize, Serialize};

use crate::types::Percentile;

/// Smoothed landed-tip amounts in lamports, per percentile bucket.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TipSnapshot {
    pub p25: u64,
    pub p50: u64,
    pub p75: u64,
    pub p95: u64,
    pub p99: u64,
}

impl TipSnapshot {
    /// Read the smoothed lamport tip for a given percentile tier (O(1)).
    pub fn lamports(&self, p: Percentile) -> u64 {
        match p {
            Percentile::P25 => self.p25,
            Percentile::P50 => self.p50,
            Percentile::P75 => self.p75,
            Percentile::P95 => self.p95,
            Percentile::P99 => self.p99,
        }
    }
}
