//! AI decision records — the persisted reasoning + fix trail the web UI reads.
//!
//! Every failure is routed to the AI analyst (plan §8, revised): there is no
//! deterministic fix path. Each analysis produces one [`AiDecisionRecord`], stored
//! in a ring buffer (see `turbine_state::AiAuditLog`) so the web UI can show the
//! classification, root cause, the fix that was applied, and the outcome.

use serde::{Deserialize, Serialize};

/// What the engine decided to do about a failure. The eventual on-chain result of
/// any resubmit is tracked separately via the lifecycle (correlate by `bundle_id`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionOutcome {
    /// AI proposed a fix and the governor sanctioned an autonomous resubmit.
    Resubmitted,
    /// Skipped: a bundle signature already landed (idempotency guard).
    AbortedLanded,
    /// Blocked by a hard guardrail (kill switch, max attempts, or spend cap).
    AbortedGuardrail,
    /// AI analyzed the failure but advised against retrying.
    AbortedNoRetry,
    /// The analyst was unavailable or returned an unusable response.
    AnalystError,
}

impl DecisionOutcome {
    pub fn resubmitted(self) -> bool {
        matches!(self, DecisionOutcome::Resubmitted)
    }
}

/// One AI reasoning + fix record. Serializable for the web reasoning log (§9.4 C).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiDecisionRecord {
    /// Monotonic sequence assigned by the audit log.
    pub seq: u64,
    /// Unix epoch milliseconds when the decision was made.
    pub at_ms: u64,
    pub bundle_id: Option<String>,
    pub tracking_id: Option<u64>,
    pub attempt: u8,
    /// Raw failure reason fed to the analyst (Jito reason / on-chain err / timeout).
    pub raw_reason: String,
    /// The AI's classification label.
    pub classification: String,
    /// The AI's root-cause explanation.
    pub root_cause: String,
    /// Human-readable summary of the fix the AI proposed and the governor applied.
    pub fix: String,
    pub should_retry: bool,
    pub confidence: f64,
    pub outcome: DecisionOutcome,
}

impl AiDecisionRecord {
    /// Current wall-clock in unix epoch millis (for `at_ms`).
    pub fn now_ms() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}
