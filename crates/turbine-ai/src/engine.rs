//! AI engine orchestrator (plan §8, revised).
//!
//! **Every** failure is routed to the AI analyst — there is no deterministic fix
//! path. The flow is:
//!
//! ```text
//! failure → idempotency guard → AI analyst → governor → record reasoning → RetryDecision
//! ```
//!
//! The model classifies, explains, and proposes a fix; the deterministic
//! [`RetryGovernor`] enforces hard guardrails (tip/slippage caps, max attempts,
//! per-minute spend cap, kill switch); and every decision is persisted to the
//! audit log so the web UI can show the reasoning + fix. The engine never signs or
//! submits — the [`crate`]'s coordinator (in `turbine-execute`) applies a
//! sanctioned `Resubmit`.

use std::sync::Arc;

use tracing::{info, warn};

use turbine_core::ai::{AiDecisionRecord, DecisionOutcome};
use turbine_core::config::Config;
use turbine_state::HotState;

use crate::analyst::Analyst;
use crate::contract::{AnalystVerdict, FailureContext, RetryAction, RetryDecision};
use crate::governor::RetryGovernor;
use crate::idempotency::{self, LandedCheck};
use crate::normalize::normalize_verdict;

/// The cold-path AI failure analyst + retry decision engine.
pub struct AiEngine {
    state: Arc<HotState>,
    analyst: Analyst,
    governor: RetryGovernor,
}

impl AiEngine {
    pub fn new(cfg: &Config, state: Arc<HotState>) -> Self {
        Self {
            state,
            analyst: Analyst::from_config(&cfg.ai),
            governor: RetryGovernor::from_config(cfg),
        }
    }

    /// Test/escape hatch: inject a specific analyst backend.
    pub fn with_analyst(cfg: &Config, state: Arc<HotState>, analyst: Analyst) -> Self {
        Self { state, analyst, governor: RetryGovernor::from_config(cfg) }
    }

    /// Analyze one failed bundle and decide what to do. Always records the
    /// reasoning to the audit log (even on abort) before returning.
    pub async fn handle_failure(&self, ctx: FailureContext) -> RetryDecision {
        // 1) Idempotency — never act if the bundle might already be on-chain.
        if idempotency::check(&self.state, &ctx.params.sigs) == LandedCheck::AlreadyLanded {
            return self.finish(
                &ctx,
                None,
                RetryAction::Abort {
                    reason: "idempotency: a bundle signature already landed".into(),
                },
                DecisionOutcome::AbortedLanded,
                "already on-chain".into(),
                ("landed", "a signature from this bundle already landed", false, 1.0),
            );
        }

        // 2) Attempt bound is a hard guardrail (not a fix decision).
        if ctx.attempt >= self.governor.max_attempts() {
            return self.finish(
                &ctx,
                None,
                RetryAction::Abort {
                    reason: format!("max attempts reached ({})", self.governor.max_attempts()),
                },
                DecisionOutcome::AbortedGuardrail,
                "—".into(),
                ("exhausted", "retry budget exhausted", false, 1.0),
            );
        }

        // 3) Ask the AI to classify + propose a fix.
        let verdict = match self.analyst.analyze(&ctx).await {
            Ok(v) => normalize_verdict(&ctx, v),
            Err(e) => {
                // Deterministic fallback when the LLM is unavailable.
                let fallback = normalize_verdict(&ctx, AnalystVerdict {
                    classification: "unknown".into(),
                    root_cause: format!("analyst error: {e}"),
                    adjustments: Default::default(),
                    should_retry: false,
                    confidence: 0.0,
                });
                if fallback.should_retry {
                    return self.finish(
                        &ctx,
                        Some(fallback.clone()),
                        RetryAction::Resubmit(fallback.adjustments.clone()),
                        DecisionOutcome::Resubmitted,
                        fallback.adjustments.summary(),
                        (
                            fallback.classification.as_str(),
                            fallback.root_cause.as_str(),
                            true,
                            fallback.confidence,
                        ),
                    );
                }
                return self.finish(
                    &ctx,
                    None,
                    RetryAction::Abort { reason: format!("analyst error: {e}") },
                    DecisionOutcome::AnalystError,
                    "—".into(),
                    ("analyst_error", "analyst unavailable", false, 0.0),
                );
            }
        };

        // 4) Governor sanctions (clamps in-bounds, aborts out-of-bounds).
        let proposed = if verdict.should_retry {
            RetryAction::Resubmit(verdict.adjustments.clone())
        } else {
            RetryAction::Abort { reason: "analyst advised no retry".into() }
        };
        let action = self.governor.sanction(proposed, &ctx, &self.state);

        let outcome = match (&action, verdict.should_retry) {
            (RetryAction::Resubmit(_), _) => DecisionOutcome::Resubmitted,
            (RetryAction::Abort { .. }, false) => DecisionOutcome::AbortedNoRetry,
            (RetryAction::Abort { .. }, true) => DecisionOutcome::AbortedGuardrail,
        };
        let fix = match &action {
            RetryAction::Resubmit(adj) => adj.summary(),
            RetryAction::Abort { .. } => "—".into(),
        };
        let meta = (
            verdict.classification.as_str(),
            verdict.root_cause.as_str(),
            verdict.should_retry,
            verdict.confidence,
        );
        self.finish(&ctx, Some(verdict.clone()), action, outcome, fix, meta)
    }

    /// Build + persist the record, log it, and return the decision.
    fn finish(
        &self,
        ctx: &FailureContext,
        verdict: Option<AnalystVerdict>,
        action: RetryAction,
        outcome: DecisionOutcome,
        fix: String,
        meta: (&str, &str, bool, f64),
    ) -> RetryDecision {
        let (classification, root_cause, should_retry, confidence) = meta;
        let mut record = AiDecisionRecord {
            seq: 0,
            at_ms: AiDecisionRecord::now_ms(),
            bundle_id: ctx.params.bundle_id.clone(),
            tracking_id: ctx.tracking_id,
            attempt: ctx.attempt,
            raw_reason: ctx.raw_reason.clone(),
            classification: classification.to_string(),
            root_cause: root_cause.to_string(),
            fix,
            should_retry,
            confidence,
            outcome,
        };
        // Persist to the ring buffer the web UI reads; capture the assigned seq.
        record.seq = self.state.ai_audit.record(record.clone());

        match &action {
            RetryAction::Resubmit(_) => info!(
                seq = record.seq,
                class = %record.classification,
                fix = %record.fix,
                "AI: autonomous retry — {}", record.root_cause
            ),
            RetryAction::Abort { reason } => warn!(
                seq = record.seq,
                class = %record.classification,
                %reason,
                "AI: no retry — {}", record.root_cause
            ),
        }
        RetryDecision { action, verdict, record }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turbine_core::tips::TipSnapshot;
    use turbine_core::types::{Congestion, FailureClass, Percentile};

    use crate::analyst::Analyst;
    use crate::contract::{BundleParams, RetryAdjustments};

    const SAMPLE: &str = r#"
[geyser]
endpoint = "https://e:443"
[rpc]
http_url = "https://r"
[jito]
block_engine_url = "https://b"
[wallet]
keypair_path = "/tmp/k.json"
[targets]
programs = []
watched_accounts = []
[strategy]
max_tip_lamports = 100000
[ai]
enabled = false
max_attempts = 3
max_slippage_bps = 500
spend_cap_lamports_per_min = 100000000
"#;

    fn cfg() -> Config {
        Config::from_toml_str(SAMPLE).unwrap()
    }

    fn ctx(class: FailureClass, tip: u64, sigs: Vec<[u8; 64]>) -> FailureContext {
        FailureContext {
            class,
            raw_reason: "test".into(),
            program_logs: vec![],
            params: BundleParams {
                blockhash: "11111111111111111111111111111111".into(),
                blockhash_age_ms: Some(500),
                blockhash_cached_slot: Some(100),
                blockhash_last_valid_height: Some(200),
                blockhash_forced_stale: false,
                tip_floor_lamports: 9_000,
                tip_lamports: tip,
                percentile: Percentile::P50,
                slippage_bps: None,
                cu_limit: None,
                sigs,
                bundle_id: Some("b-1".into()),
            },
            attempt: 0,
            contention: Congestion::Hot,
            tip_snapshot: TipSnapshot::default(),
            tip_below_floor: false,
            blockhash_likely_stale: false,
            tracking_id: None,
        }
    }

    fn retry_verdict() -> AnalystVerdict {
        AnalystVerdict {
            classification: "blockhash_expired".into(),
            root_cause: "stale blockhash".into(),
            adjustments: RetryAdjustments {
                tip_bump_pct: Some(0.5),
                fresh_blockhash: true,
                rebuild: true,
                ..Default::default()
            },
            should_retry: true,
            confidence: 0.9,
        }
    }

    #[tokio::test]
    async fn every_failure_calls_the_analyst() {
        // With no analyst, even a "simple" blockhash failure cannot be fixed —
        // proving there is no deterministic fallback path anymore.
        let c = cfg();
        let state = Arc::new(HotState::new(&c));
        let eng = AiEngine::with_analyst(&c, state.clone(), Analyst::Disabled);
        let d = eng.handle_failure(ctx(FailureClass::BlockhashExpired, 10_000, vec![])).await;
        assert!(!d.should_retry());
        assert_eq!(d.record.outcome, DecisionOutcome::AnalystError);
        assert_eq!(state.ai_audit.len(), 1);
    }

    #[tokio::test]
    async fn ai_proposal_is_applied_and_clamped() {
        let c = cfg();
        let state = Arc::new(HotState::new(&c));
        let mut v = retry_verdict();
        v.adjustments.tip_bump_pct = Some(10.0); // absurd → clamp so projected ≤ 100k
        v.adjustments.slippage_bps = Some(100_000); // → clamp to 500
        let eng = AiEngine::with_analyst(&c, state.clone(), Analyst::Mock(Box::new(v)));
        let d = eng.handle_failure(ctx(FailureClass::Slippage, 80_000, vec![])).await;
        let RetryAction::Resubmit(adj) = d.action else { panic!("expected resubmit") };
        assert_eq!(adj.slippage_bps, Some(500));
        let projected = (80_000.0 * (1.0 + adj.tip_bump_pct.unwrap())) as u64;
        assert!(projected <= 100_000, "projected {projected}");
        assert_eq!(d.record.outcome, DecisionOutcome::Resubmitted);
        // Reasoning persisted for the web UI.
        assert_eq!(state.ai_audit.snapshot()[0].classification, "blockhash_expired");
    }

    #[tokio::test]
    async fn analyst_no_retry_is_recorded() {
        let c = cfg();
        let state = Arc::new(HotState::new(&c));
        let mut v = retry_verdict();
        v.should_retry = false;
        let eng = AiEngine::with_analyst(&c, state.clone(), Analyst::Mock(Box::new(v)));
        let d = eng.handle_failure(ctx(FailureClass::Unknown, 10_000, vec![])).await;
        assert!(!d.should_retry());
        assert_eq!(d.record.outcome, DecisionOutcome::AbortedNoRetry);
    }

    #[tokio::test]
    async fn idempotency_blocks_retry_when_landed() {
        let c = cfg();
        let state = Arc::new(HotState::new(&c));
        let sig = [7u8; 64];
        let id = state.lifecycle.on_submit(
            vec![sig],
            0,
            1_000,
            turbine_core::types::Percentile::P25,
            turbine_state::SubmitContext::default(),
        );
        state.lifecycle.on_self_tx(&sig, 123);
        assert!(state.lifecycle.get(id).unwrap().landed_slot.is_some()); 

        let eng = AiEngine::with_analyst(&c, state.clone(), Analyst::Mock(Box::new(retry_verdict())));
        let d = eng.handle_failure(ctx(FailureClass::BundleDropped, 10_000, vec![sig])).await;
        assert!(!d.should_retry());
        assert_eq!(d.record.outcome, DecisionOutcome::AbortedLanded);
    }
}
