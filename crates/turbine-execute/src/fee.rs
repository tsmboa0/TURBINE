//! Dynamic fee matrix (plan §7.1): contention → percentile → smoothed lamports.

use solana_pubkey::Pubkey;

use turbine_core::config::StrategyConfig;
use turbine_core::types::{Congestion, Percentile};
use turbine_state::HotState;

/// The tip decision for one bundle.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FeeDecision {
    pub congestion: Congestion,
    pub percentile: Percentile,
    pub tip_lamports: u64,
    /// Bottleneck account z-score that drove the contention bump (for telemetry/logs).
    pub max_z: f64,
    /// Total fractional bump actually applied = flat + contention-scaled (pre-clamp).
    pub bump_pct: f64,
}

/// Contention-scaled extra bump: 0 below `hot_z`, then `slope` per z-unit above it,
/// capped at `max_contention_bump_pct`. Lets the tip scale with *how* hot/volatile
/// the bottleneck account is, not just which discrete tier it landed in.
fn contention_bump(max_z: f64, strat: &StrategyConfig) -> f64 {
    let over = (max_z - strat.hot_z).max(0.0);
    (over * strat.contention_bump_slope).min(strat.max_contention_bump_pct)
}

/// Select the Jito tip for a bundle that will write-lock `bundle_writes`.
///
/// - Congestion tier = **max** over the bundle's write-locked accounts (bottleneck wins),
///   selecting the percentile floor.
/// - On top of the floor we apply a flat bump **plus** a contention-scaled bump driven
///   by the bottleneck account's z-score, so we bid harder exactly when contention spikes.
/// - Tip uses the **smoothed** percentile (never the raw spiky WS value).
/// - Always clamped to `[min_tip_lamports, max_tip_lamports]` (the upper bound is the
///   hard financial guardrail; the lower bound respects Jito's 1000-lamport min).
pub fn select_tip(state: &HotState, bundle_writes: &[Pubkey], strat: &StrategyConfig) -> FeeDecision {
    let congestion = state.contention.max_congestion(bundle_writes);
    let percentile = congestion.target_percentile();
    let base = state.tips().lamports(percentile);
    let max_z = state.contention.max_z(bundle_writes);
    let bump_pct = strat.tip_bump_pct + contention_bump(max_z, strat);
    let bumped = (base as f64 * (1.0 + bump_pct)) as u64;
    let tip_lamports = bumped.clamp(strat.min_tip_lamports, strat.max_tip_lamports);
    FeeDecision { congestion, percentile, tip_lamports, max_z, bump_pct }
}

/// Like [`select_tip`], but uses a fixed percentile floor instead of the
/// contention-driven tier. Used by live happy-path testing (P75 + bump).
pub fn select_tip_at_percentile(
    state: &HotState,
    bundle_writes: &[Pubkey],
    strat: &StrategyConfig,
    percentile: Percentile,
) -> FeeDecision {
    let congestion = state.contention.max_congestion(bundle_writes);
    let base = state.tips().lamports(percentile);
    let max_z = state.contention.max_z(bundle_writes);
    let bump_pct = strat.tip_bump_pct + contention_bump(max_z, strat);
    let bumped = (base as f64 * (1.0 + bump_pct)) as u64;
    let tip_lamports = bumped.clamp(strat.min_tip_lamports, strat.max_tip_lamports);
    FeeDecision { congestion, percentile, tip_lamports, max_z, bump_pct }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turbine_core::config::Config;
    use turbine_core::tips::TipSnapshot;

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
watched_accounts = ["So11111111111111111111111111111111111111112"]
"#;

    #[test]
    fn idle_selects_p25_and_clamps_floor() {
        let cfg = Config::from_toml_str(SAMPLE).unwrap();
        let state = HotState::new(&cfg);
        // No watched contention signal → Idle → P25; no tips → clamps to floor.
        let dec = select_tip(&state, &[], &cfg.strategy);
        assert_eq!(dec.congestion, Congestion::Idle);
        assert_eq!(dec.percentile, Percentile::P25);
        assert_eq!(dec.tip_lamports, cfg.strategy.min_tip_lamports);
    }

    #[test]
    fn applies_bump_and_percentile() {
        let cfg = Config::from_toml_str(SAMPLE).unwrap();
        let state = HotState::new(&cfg);
        state.set_tips(TipSnapshot { p25: 1_000, p50: 2_000, p75: 5_000, p95: 50_000, p99: 200_000 });
        // Idle → P25 = 1000, +10% bump = 1100.
        let dec = select_tip(&state, &[], &cfg.strategy);
        assert_eq!(dec.tip_lamports, 1_100);
    }

    #[test]
    fn hot_account_selects_p95() {
        use std::str::FromStr;
        use std::time::{Duration, Instant};

        let cfg = Config::from_toml_str(SAMPLE).unwrap();
        let state = HotState::new(&cfg);
        state.set_tips(TipSnapshot { p25: 1_000, p50: 2_000, p75: 5_000, p95: 50_000, p99: 200_000 });

        let hot = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
        let t0 = Instant::now();
        // Quiet baseline, then a sustained burst pushes the cell to Hot.
        for i in 0..150u64 {
            state.contention.record_writable(&[hot]);
            state.contention.flush_slot(t0 + Duration::from_millis(i * 400));
        }
        let mut now = t0 + Duration::from_millis(150 * 400);
        for _ in 0..6 {
            now += Duration::from_millis(400);
            for _ in 0..40 {
                state.contention.record_writable(&[hot]);
            }
            state.contention.flush_slot(now);
        }

        let dec = select_tip(&state, &[hot], &cfg.strategy);
        assert_eq!(dec.congestion, Congestion::Hot);
        assert_eq!(dec.percentile, Percentile::P95);
        // P95 = 50_000, flat +10% = 55_000 floor, plus a contention-scaled bump
        // (z above hot_z) capped at +50% → in [55_000, 80_000].
        assert!(dec.tip_lamports >= 55_000, "got {}", dec.tip_lamports);
        assert!(dec.tip_lamports <= 80_000, "got {}", dec.tip_lamports);
        assert!(dec.bump_pct >= cfg.strategy.tip_bump_pct);
    }

    #[test]
    fn fixed_percentile_applies_bump() {
        let cfg = Config::from_toml_str(SAMPLE).unwrap();
        let state = HotState::new(&cfg);
        state.set_tips(TipSnapshot { p25: 1_000, p50: 2_000, p75: 5_000, p95: 50_000, p99: 200_000 });
        let dec = select_tip_at_percentile(&state, &[], &cfg.strategy, Percentile::P75);
        assert_eq!(dec.percentile, Percentile::P75);
        assert_eq!(dec.tip_lamports, 5_500); // 5000 + 10%
    }

    #[test]
    fn contention_bump_scales_and_caps() {
        let cfg = Config::from_toml_str(SAMPLE).unwrap();
        // 0 below hot_z; slope per z-unit above; capped.
        assert_eq!(contention_bump(cfg.strategy.hot_z, &cfg.strategy), 0.0);
        assert_eq!(contention_bump(cfg.strategy.hot_z - 5.0, &cfg.strategy), 0.0);
        let one_over = contention_bump(cfg.strategy.hot_z + 1.0, &cfg.strategy);
        assert!((one_over - cfg.strategy.contention_bump_slope).abs() < 1e-9);
        assert_eq!(
            contention_bump(cfg.strategy.hot_z + 1000.0, &cfg.strategy),
            cfg.strategy.max_contention_bump_pct
        );
    }
}
