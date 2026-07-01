//! Retry governor (plan §8.4): the deterministic safety layer.
//!
//! The LLM/Tier-0 only *propose*; the governor enforces hard guardrails the model
//! can never override: tip/slippage caps, max attempts, a per-minute spend cap,
//! and the global kill switch. It clamps in-bounds proposals and aborts the rest.
//!
//! Compute-unit limits are **not** set here — on sanctioned resubmit the execution
//! coordinator runs `simulateTransaction` (real measured CUs + headroom) before
//! recompiling, falling back to the LLM's optional `cu_limit` or the static
//! estimator if RPC simulation fails.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use turbine_core::config::Config;
use turbine_state::HotState;

use crate::contract::{FailureContext, RetryAction};

const SPEND_WINDOW: Duration = Duration::from_secs(60);

/// Enforces guardrails on proposed retries.
pub struct RetryGovernor {
    max_tip_lamports: u64,
    max_slippage_bps: u32,
    max_attempts: u8,
    spend_cap_per_min: u64,
    /// Sliding 60s window of committed (projected) tip spend.
    spend: Mutex<Vec<(Instant, u64)>>,
}

impl RetryGovernor {
    pub fn from_config(cfg: &Config) -> Self {
        Self {
            max_tip_lamports: cfg.strategy.max_tip_lamports,
            max_slippage_bps: cfg.ai.max_slippage_bps,
            max_attempts: cfg.ai.max_attempts,
            spend_cap_per_min: cfg.ai.spend_cap_lamports_per_min,
            spend: Mutex::new(Vec::new()),
        }
    }

    pub fn max_attempts(&self) -> u8 {
        self.max_attempts
    }

    /// Tip that would result from applying `tip_bump_pct` to the prior tip.
    fn projected_tip(&self, prior_tip: u64, bump_pct: Option<f64>) -> u64 {
        let bump = bump_pct.unwrap_or(0.0).max(0.0);
        ((prior_tip as f64) * (1.0 + bump)) as u64
    }

    /// Current spend in the sliding window (prunes expired entries).
    fn windowed_spend(&self) -> u64 {
        let now = Instant::now();
        let mut g = self.spend.lock().unwrap();
        g.retain(|(t, _)| now.saturating_duration_since(*t) < SPEND_WINDOW);
        g.iter().map(|(_, v)| *v).sum()
    }

    /// Try to reserve `amount` against the per-minute cap. Records it on success.
    fn try_reserve(&self, amount: u64) -> bool {
        let now = Instant::now();
        let mut g = self.spend.lock().unwrap();
        g.retain(|(t, _)| now.saturating_duration_since(*t) < SPEND_WINDOW);
        let used: u64 = g.iter().map(|(_, v)| *v).sum();
        if used.saturating_add(amount) > self.spend_cap_per_min {
            return false;
        }
        g.push((now, amount));
        true
    }

    /// Validate + clamp a proposed action against all guardrails.
    pub fn sanction(&self, action: RetryAction, ctx: &FailureContext, state: &HotState) -> RetryAction {
        if state.submission_killed() {
            return RetryAction::Abort { reason: "kill switch engaged".into() };
        }
        if ctx.attempt >= self.max_attempts {
            return RetryAction::Abort {
                reason: format!("max attempts reached ({})", self.max_attempts),
            };
        }

        let RetryAction::Resubmit(mut adj) = action else {
            return action; // Abort passes through unchanged
        };

        // Clamp slippage to the hard cap.
        if let Some(s) = adj.slippage_bps {
            adj.slippage_bps = Some(s.min(self.max_slippage_bps));
        }

        // Per-retry tip bump cap (+30% max).
        if let Some(p) = adj.tip_bump_pct {
            adj.tip_bump_pct = Some(p.clamp(0.0, crate::normalize::MAX_TIP_BUMP_PCT));
        }

        // Clamp the tip bump so the projected tip never exceeds the cap
        // and never falls below the submit-time percentile floor.
        let floor = ctx.params.tip_floor_lamports;
        let mut projected = self.projected_tip(ctx.params.tip_lamports, adj.tip_bump_pct);
        projected = projected.max(floor);
        if projected > self.max_tip_lamports {
            let prior = ctx.params.tip_lamports.max(1);
            let clamped_bump = (self.max_tip_lamports as f64 / prior as f64) - 1.0;
            adj.tip_bump_pct = Some(clamped_bump.max(0.0));
            projected = self.max_tip_lamports;
        }

        // Enforce the per-minute spend cap (reserve the projected tip).
        if !self.try_reserve(projected) {
            return RetryAction::Abort {
                reason: format!(
                    "per-minute spend cap hit ({} + {} > {})",
                    self.windowed_spend(),
                    projected,
                    self.spend_cap_per_min
                ),
            };
        }

        RetryAction::Resubmit(adj)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use turbine_core::config::Config;
    use turbine_core::tips::TipSnapshot;
    use turbine_core::types::{Congestion, FailureClass, Percentile};

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
spend_cap_lamports_per_min = 250000
"#;

    fn cfg() -> Config {
        Config::from_toml_str(SAMPLE).unwrap()
    }

    fn ctx(tip: u64, attempt: u8) -> FailureContext {
        FailureContext {
            class: FailureClass::TipTooLow,
            raw_reason: String::new(),
            program_logs: vec![],
            params: BundleParams {
                blockhash: "11111111111111111111111111111111".into(),
                blockhash_age_ms: None,
                blockhash_cached_slot: None,
                blockhash_last_valid_height: None,
                blockhash_forced_stale: false,
                tip_floor_lamports: 5_000,
                tip_lamports: tip,
                percentile: Percentile::P50,
                slippage_bps: None,
                cu_limit: None,
                sigs: vec![],
                bundle_id: None,
            },
            attempt,
            contention: Congestion::Hot,
            tip_snapshot: TipSnapshot::default(),
            tip_below_floor: false,
            blockhash_likely_stale: false,
            tracking_id: None,
        }
    }

    fn resubmit(bump: f64, slippage: Option<u32>) -> RetryAction {
        RetryAction::Resubmit(RetryAdjustments {
            tip_bump_pct: Some(bump),
            slippage_bps: slippage,
            ..Default::default()
        })
    }

    #[test]
    fn clamps_tip_and_slippage_to_caps() {
        let c = cfg();
        let g = RetryGovernor::from_config(&c);
        let state = HotState::new(&c);
        // tip 80k, +100% would be 160k > 100k cap → bump clamped to 0.25.
        let action = g.sanction(resubmit(1.0, Some(9999)), &ctx(80_000, 0), &state);
        let RetryAction::Resubmit(adj) = action else { panic!() };
        assert_eq!(adj.slippage_bps, Some(500));
        let bump = adj.tip_bump_pct.unwrap();
        assert!((bump - 0.25).abs() < 1e-9, "bump was {bump}");
    }

    #[test]
    fn kill_switch_aborts() {
        let c = cfg();
        let g = RetryGovernor::from_config(&c);
        let state = HotState::new(&c);
        state.kill_submission();
        assert!(matches!(
            g.sanction(resubmit(0.1, None), &ctx(10_000, 0), &state),
            RetryAction::Abort { .. }
        ));
    }

    #[test]
    fn max_attempts_aborts() {
        let c = cfg();
        let g = RetryGovernor::from_config(&c);
        let state = HotState::new(&c);
        assert!(matches!(
            g.sanction(resubmit(0.1, None), &ctx(10_000, 3), &state),
            RetryAction::Abort { .. }
        ));
    }

    #[test]
    fn per_minute_spend_cap_aborts() {
        let c = cfg();
        let g = RetryGovernor::from_config(&c);
        let state = Arc::new(HotState::new(&c));
        // cap 250k. First two 100k tips OK (200k), third 100k → 300k > cap.
        assert!(matches!(g.sanction(resubmit(0.0, None), &ctx(100_000, 0), &state), RetryAction::Resubmit(_)));
        assert!(matches!(g.sanction(resubmit(0.0, None), &ctx(100_000, 0), &state), RetryAction::Resubmit(_)));
        assert!(matches!(g.sanction(resubmit(0.0, None), &ctx(100_000, 0), &state), RetryAction::Abort { .. }));
    }
}
