//! AI retry contract types (plan §8.2–§8.3).
//!
//! These describe a failure to the router/analyst and carry the *proposed*
//! correction back. The LLM only ever produces an [`AnalystVerdict`]; the
//! deterministic governor turns proposals into a sanctioned [`RetryDecision`].

use serde::{Deserialize, Serialize};

use turbine_core::ai::AiDecisionRecord;
use turbine_core::tips::TipSnapshot;
use turbine_core::types::{Congestion, FailureClass, Percentile};

/// 64-byte signature (mirrors `turbine_state::lifecycle::SigBytes`).
pub type SigBytes = [u8; 64];

/// Parameters of the bundle that failed (no secrets, no keypair).
#[derive(Debug, Clone, Serialize)]
pub struct BundleParams {
    /// Blockhash embedded in the failed bundle's transactions.
    pub blockhash: String,
    /// Age of the warm cached blockhash at submit time (ms since fetch).
    pub blockhash_age_ms: Option<u64>,
    /// RPC context slot when the cached blockhash was fetched.
    pub blockhash_cached_slot: Option<u64>,
    /// Last block height at which the cached blockhash was valid.
    pub blockhash_last_valid_height: Option<u64>,
    /// True when a deliberate stale/forced blockhash was used (live fail-path tests).
    pub blockhash_forced_stale: bool,
    /// Percentile EMA lamports at submit — the un-bumped tip floor.
    pub tip_floor_lamports: u64,
    /// Actual tip paid on the failed bundle.
    pub tip_lamports: u64,
    pub percentile: Percentile,
    pub slippage_bps: Option<u32>,
    pub cu_limit: Option<u32>,
    #[serde(skip)]
    pub sigs: Vec<SigBytes>,
    pub bundle_id: Option<String>,
}

/// Everything the analyst needs to reason about one failure.
#[derive(Debug, Clone, Serialize)]
pub struct FailureContext {
    /// Engine-side hint — often wrong on silent timeouts; do not copy verbatim.
    #[serde(skip_serializing)]
    pub class: FailureClass,
    pub raw_reason: String,
    pub program_logs: Vec<String>,
    pub params: BundleParams,
    pub attempt: u8,
    pub contention: Congestion,
    pub tip_snapshot: TipSnapshot,
    /// Paid tip is below the percentile EMA floor — classify as tip_too_low.
    pub tip_below_floor: bool,
    /// Blockhash was forced stale or aged past the configured max at submit.
    pub blockhash_likely_stale: bool,
    /// Internal lifecycle tracking id (stamped into the audit record; not sent to
    /// the LLM).
    #[serde(skip)]
    pub tracking_id: Option<u64>,
}

/// Concrete corrections the AI proposes; the engine applies them on rebuild.
/// All optional; absent = unchanged.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RetryAdjustments {
    /// Multiplicative tip bump (e.g. 0.5 = +50%), applied to the prior tip.
    #[serde(default)]
    pub tip_bump_pct: Option<f64>,
    /// Advisory slippage tolerance (bps). Applied by a strategy-provided rebuild
    /// hook; the generic engine records it but cannot rewrite arbitrary program IX.
    #[serde(default)]
    pub slippage_bps: Option<u32>,
    /// Compute-unit limit (the engine sets a ComputeBudget instruction).
    #[serde(default)]
    pub cu_limit: Option<u32>,
    /// Re-fetch a fresh blockhash before re-signing (only when prior is dead).
    #[serde(default)]
    pub fresh_blockhash: bool,
    /// Rebuild the bundle from the intent (vs. resubmit the same bytes).
    #[serde(default)]
    pub rebuild: bool,
}

impl RetryAdjustments {
    /// Human-readable summary for the audit/reasoning log.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        if let Some(p) = self.tip_bump_pct {
            parts.push(format!("tip +{:.0}%", p * 100.0));
        }
        if let Some(s) = self.slippage_bps {
            parts.push(format!("slippage {s}bps"));
        }
        if let Some(cu) = self.cu_limit {
            parts.push(format!("cu_limit {cu}"));
        }
        if self.fresh_blockhash {
            parts.push("fresh blockhash".into());
        }
        if self.rebuild {
            parts.push("rebuild".into());
        }
        if parts.is_empty() {
            "no changes".into()
        } else {
            parts.join(", ")
        }
    }
}

/// What the governor sanctioned: either retry with adjustments, or stop.
#[derive(Debug, Clone, PartialEq)]
pub enum RetryAction {
    Resubmit(RetryAdjustments),
    Abort { reason: String },
}

/// Strict schema the LLM must return (validated before use).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalystVerdict {
    pub classification: String,
    pub root_cause: String,
    #[serde(default)]
    pub adjustments: RetryAdjustments,
    pub should_retry: bool,
    pub confidence: f64,
}

/// Final, sanctioned decision plus the persisted reasoning record.
#[derive(Debug, Clone)]
pub struct RetryDecision {
    pub action: RetryAction,
    /// The LLM's raw proposal, when the analyst ran (pre-governor).
    pub verdict: Option<AnalystVerdict>,
    /// The record stored in the audit log (mirrors what the web UI shows).
    pub record: AiDecisionRecord,
}

impl RetryDecision {
    pub fn should_retry(&self) -> bool {
        matches!(self.action, RetryAction::Resubmit(_))
    }
}
