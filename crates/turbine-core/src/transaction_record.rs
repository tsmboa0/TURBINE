//! Serializable transaction audit record for JSONL export (reporting / graphing).

use serde::Serialize;

use crate::ai::AiDecisionRecord;
use crate::tips::TipSnapshot;
use crate::types::{Congestion, LifecycleState, Percentile};

/// One terminal (or gate-skipped) bundle row — everything the web UI history shows,
/// plus submit-time context and linked AI decisions.
#[derive(Debug, Clone, Serialize)]
pub struct TransactionRecord {
    /// Wall-clock when this row was appended to the JSONL file.
    pub recorded_at_ms: u64,
    pub tracking_id: u64,
    pub label: Option<String>,
    pub bundle_id: Option<String>,
    /// Primary (first) transaction signature, base58.
    pub signature: Option<String>,
    /// All bundle transaction signatures, base58.
    pub signatures: Vec<String>,
    pub state: LifecycleState,
    pub attempt: u8,
    /// Percentile EMA lamports at submit (before bump / clamp).
    pub tip_floor_lamports: Option<u64>,
    /// Actual tip paid on the bundle.
    pub tip_lamports: Option<u64>,
    /// `tip_lamports - tip_floor_lamports` (bump + clamp overhead; negative if under floor).
    pub tip_delta_lamports: Option<i64>,
    pub percentile: Option<String>,
    pub landed_slot: Option<u64>,
    pub last_error: Option<String>,
    pub deltas: LifecycleDeltaRecord,
    /// Tip percentile EMAs at submit time.
    pub submit_tip_emas: TipSnapshot,
    /// Tip percentile EMAs at record time (may have drifted since submit).
    pub terminal_tip_emas: TipSnapshot,
    pub submit_slot: u64,
    pub next_jito_leader_slot: Option<u64>,
    pub gate_dist: Option<u64>,
    pub blockhash_age_ms: Option<u64>,
    pub congestion: Congestion,
    pub max_z: f64,
    pub bump_pct: f64,
    /// AI analyst records tied to this tracking id (may be empty on success).
    pub ai_decisions: Vec<AiDecisionRecord>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LifecycleDeltaRecord {
    pub submit_to_processed_ms: Option<f64>,
    pub processed_to_confirmed_ms: Option<f64>,
    pub confirmed_to_finalized_ms: Option<f64>,
}

/// Percentile EMA lamports at submit (the un-bumped floor).
pub fn tip_floor_lamports(percentile: Option<Percentile>, submit_tip_emas: &TipSnapshot) -> Option<u64> {
    percentile.map(|p| submit_tip_emas.lamports(p))
}

/// Paid tip minus floor.
pub fn tip_delta_lamports(paid: Option<u64>, floor: Option<u64>) -> Option<i64> {
    match (paid, floor) {
        (Some(p), Some(f)) => Some(p as i64 - f as i64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Percentile;

    #[test]
    fn tip_floor_reads_submit_ema_percentile() {
        let emas = TipSnapshot { p25: 1_000, p50: 2_000, p75: 9_000, p95: 50_000, p99: 200_000 };
        assert_eq!(tip_floor_lamports(Some(Percentile::P75), &emas), Some(9_000));
    }

    #[test]
    fn tip_delta_covers_bump_and_underpay() {
        assert_eq!(tip_delta_lamports(Some(9_900), Some(9_000)), Some(900));
        assert_eq!(tip_delta_lamports(Some(1), Some(1_000)), Some(-999));
    }
}
