//! Post-process LLM verdicts against deterministic bundle signals.
//!
//! The engine hint (`AuctionTimeout`) is often wrong; these rules align classification
//! and adjustments with observable params before the governor runs.

use crate::contract::{AnalystVerdict, FailureContext};

/// Max multiplicative tip bump per retry (30%).
pub const MAX_TIP_BUMP_PCT: f64 = 0.30;

/// Default bump when tip is below floor and the model omitted one.
pub const DEFAULT_TIP_BUMP_PCT: f64 = 0.25;

/// Apply deterministic corrections on top of the LLM verdict.
pub fn normalize_verdict(ctx: &FailureContext, mut verdict: AnalystVerdict) -> AnalystVerdict {
    if ctx.tip_below_floor { 
        verdict.classification = "tip_too_low".into();
        verdict.should_retry = true;
        verdict.adjustments.rebuild = true;
        verdict.adjustments.fresh_blockhash = false;
        verdict.adjustments.tip_bump_pct = Some(
            verdict
                .adjustments
                .tip_bump_pct
                .unwrap_or(DEFAULT_TIP_BUMP_PCT)
                .clamp(0.05, MAX_TIP_BUMP_PCT),
        );
        if verdict.root_cause.is_empty() {
            verdict.root_cause = format!(
                "paid tip {} lamports is below floor {} lamports",
                ctx.params.tip_lamports, ctx.params.tip_floor_lamports
            );
        }
        return clamp_bump(verdict);
    }

    if ctx.blockhash_likely_stale {
        verdict.classification = "blockhash_expired".into();
        verdict.should_retry = true;
        verdict.adjustments.rebuild = true;
        verdict.adjustments.fresh_blockhash = true;
        verdict.adjustments.tip_bump_pct = None;
        if verdict.root_cause.is_empty() {
            verdict.root_cause = "blockhash expired or deliberately stale at submit".into();
        }
        return verdict;
    }

    // Never copy the engine timeout label when evidence points elsewhere.
    if verdict.classification == "auction_timeout" && ctx.tip_below_floor {
        verdict.classification = "tip_too_low".into();
    }

    clamp_bump(verdict)
}

fn clamp_bump(mut verdict: AnalystVerdict) -> AnalystVerdict {
    if let Some(p) = verdict.adjustments.tip_bump_pct {
        verdict.adjustments.tip_bump_pct = Some(p.clamp(0.0, MAX_TIP_BUMP_PCT));
    }
    verdict
}

#[cfg(test)]
mod tests {
    use super::*;
    use turbine_core::tips::TipSnapshot;
    use turbine_core::types::{Congestion, FailureClass, Percentile};

    use crate::contract::{BundleParams, RetryAdjustments};

    fn ctx(tip: u64, floor: u64, forced_stale: bool) -> FailureContext {
        FailureContext {
            class: FailureClass::AuctionTimeout,
            raw_reason: "timeout".into(),
            program_logs: vec![],
            params: BundleParams {
                blockhash: "x".into(),
                blockhash_age_ms: Some(500),
                blockhash_cached_slot: None,
                blockhash_last_valid_height: None,
                blockhash_forced_stale: forced_stale,
                tip_floor_lamports: floor,
                tip_lamports: tip,
                percentile: Percentile::P50,
                slippage_bps: None,
                cu_limit: None,
                sigs: vec![],
                bundle_id: None,
            },
            attempt: 0,
            contention: Congestion::Quiet,
            tip_snapshot: TipSnapshot::default(),
            tracking_id: None,
            tip_below_floor: tip < floor,
            blockhash_likely_stale: forced_stale,
        }
    }

    fn empty_verdict() -> AnalystVerdict {
        AnalystVerdict {
            classification: "auction_timeout".into(),
            root_cause: "timed out".into(),
            adjustments: RetryAdjustments::default(),
            should_retry: false,
            confidence: 0.5,
        }
    }

    #[test]
    fn underfloor_forces_tip_bump() {
        let v = normalize_verdict(&ctx(1, 4_000, false), empty_verdict());
        assert_eq!(v.classification, "tip_too_low");
        assert!(v.should_retry);
        assert_eq!(v.adjustments.tip_bump_pct, Some(DEFAULT_TIP_BUMP_PCT));
        assert!(!v.adjustments.fresh_blockhash);
    }

    #[test]
    fn stale_blockhash_no_tip_bump() {
        let mut v = empty_verdict();
        v.adjustments.tip_bump_pct = Some(0.5);
        let v = normalize_verdict(&ctx(10_000, 9_000, true), v);
        assert_eq!(v.classification, "blockhash_expired");
        assert!(v.adjustments.fresh_blockhash);
        assert!(v.adjustments.tip_bump_pct.is_none());
    }
}
