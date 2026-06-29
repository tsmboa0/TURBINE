//! Autonomous AI retry coordinator (plan §8, revised).
//!
//! Every bundle failure — a Jito reject/drop, an on-chain error, or a silent
//! submit timeout — is routed here. The coordinator:
//!
//! 1. resolves the failure to the original [`TradeIntent`] via the in-flight
//!    registry (populated by [`ExecutionEngine`] on submit),
//! 2. hands a [`FailureContext`] to the [`AiEngine`] which classifies it, stores
//!    the reasoning + fix in the audit log (for the web UI), and proposes a
//!    governor-sanctioned fix,
//! 3. if sanctioned, **rebuilds** the bundle with the fix applied (fresh blockhash
//!    from the warm cache, bumped tip, optional compute-unit limit) and
//!    **resubmits autonomously**.
//!
//! It runs on the services (cold) runtime and never blocks the hot loop.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use jito_sdk_rust::JitoJsonRpcSDK;
use tokio::sync::mpsc;
use tracing::{info, warn};

use turbine_ai::{AiEngine, BundleParams, FailureContext, RetryAction};
use turbine_core::config::Config;
use turbine_core::types::{FailureClass, Percentile};
use turbine_state::HotState;

use crate::fee::select_tip;
use crate::diagnostics;
use crate::{ExecutionEngine, RetryOverrides, TradeIntent};

/// Retained per-bundle state needed to rebuild + resubmit on failure.
#[derive(Debug, Clone)]
pub struct RetryState {
    pub intent: TradeIntent,
    pub attempt: u8,
    pub tip_lamports: u64,
    pub tip_floor_lamports: u64,
    pub percentile: Percentile,
    pub blockhash: String,
    pub blockhash_age_ms: Option<u64>,
    pub blockhash_cached_slot: Option<u64>,
    pub blockhash_last_valid_height: Option<u64>,
    pub blockhash_forced_stale: bool,
    pub sigs: Vec<[u8; 64]>,
    pub bundle_id: Option<String>,
}

/// Concurrent registry of in-flight bundles, keyed by lifecycle tracking id.
#[derive(Default)]
pub struct InFlightRegistry {
    map: DashMap<u64, RetryState>,
}

impl InFlightRegistry {
    pub fn new() -> Self {
        Self { map: DashMap::new() }
    }

    pub fn insert(&self, id: u64, state: RetryState) {
        self.map.insert(id, state);
    }

    pub fn set_bundle_id(&self, id: u64, bundle_id: String) {
        if let Some(mut s) = self.map.get_mut(&id) {
            s.bundle_id = Some(bundle_id);
        }
    }

    pub fn get(&self, id: u64) -> Option<RetryState> {
        self.map.get(&id).map(|s| s.clone())
    }

    pub fn remove(&self, id: u64) -> Option<RetryState> {
        self.map.remove(&id).map(|(_, s)| s)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// A failure signal handed to the AI coordinator. Either `tracking_id` (preferred)
/// or `bundle_id` must resolve to an in-flight record.
#[derive(Debug, Clone)]
pub struct FailureEvent {
    pub tracking_id: Option<u64>,
    pub bundle_id: Option<String>,
    pub raw_reason: String,
    /// Observed class (hint only — the AI does the authoritative classification).
    pub class_hint: FailureClass,
    pub logs: Vec<String>,
}

/// Apply a (governor-clamped) multiplicative tip bump.
fn bumped_tip(prior: u64, bump_pct: Option<f64>) -> u64 {
    let bump = bump_pct.unwrap_or(0.0).max(0.0);
    ((prior as f64) * (1.0 + bump)) as u64
}

/// Drives the autonomous failure → AI → rebuild → resubmit loop.
pub struct RetryCoordinator {
    cfg: Arc<Config>,
    engine: Arc<ExecutionEngine>,
    ai: Arc<AiEngine>,
    state: Arc<HotState>,
}

impl RetryCoordinator {
    pub fn new(
        cfg: Arc<Config>,
        engine: Arc<ExecutionEngine>,
        ai: Arc<AiEngine>,
        state: Arc<HotState>,
    ) -> Self {
        Self { cfg, engine, ai, state }
    }

    fn resolve_id(&self, ev: &FailureEvent) -> Option<u64> {
        ev.tracking_id.or_else(|| {
            ev.bundle_id
                .as_ref()
                .and_then(|b| self.state.lifecycle.id_for_bundle(b))
        })
    }

    /// Handle one failure end-to-end (analyze → maybe rebuild + resubmit).
    pub async fn on_failure(&self, ev: FailureEvent) {
        let Some(id) = self.resolve_id(&ev) else {
            warn!(?ev.bundle_id, "failure for an unknown bundle; ignoring");
            return;
        };
        let Some(rs) = self.engine.registry().get(id) else {
            warn!(tracking_id = id, "no retained intent for failed bundle; cannot retry");
            return;
        };

        // Build the context (contention recomputed now; tips from hot state).
        let fee = select_tip(&self.state, &rs.intent.write_accounts, &self.cfg.strategy);
        let ctx = FailureContext {
            class: ev.class_hint.clone(),
            raw_reason: ev.raw_reason.clone(),
            program_logs: ev.logs.clone(),
            params: BundleParams {
                blockhash: rs.blockhash.clone(),
                blockhash_age_ms: rs.blockhash_age_ms,
                blockhash_cached_slot: rs.blockhash_cached_slot,
                blockhash_last_valid_height: rs.blockhash_last_valid_height,
                blockhash_forced_stale: rs.blockhash_forced_stale,
                tip_floor_lamports: rs.tip_floor_lamports,
                tip_lamports: rs.tip_lamports,
                percentile: rs.percentile,
                slippage_bps: None,
                cu_limit: None,
                sigs: rs.sigs.clone(),
                bundle_id: ev.bundle_id.clone().or_else(|| rs.bundle_id.clone()),
            },
            attempt: rs.attempt,
            contention: fee.congestion,
            tip_snapshot: self.state.tips(),
            tip_below_floor: rs.tip_lamports < rs.tip_floor_lamports,
            blockhash_likely_stale: rs.blockhash_forced_stale
                || rs.blockhash_age_ms.is_some_and(|a| a > self.cfg.execution.blockhash_max_age_ms),
            tracking_id: Some(id),
        };

        let decision = self.ai.handle_failure(ctx).await;
        self.state
            .lifecycle
            .set_ai_classification(id, decision.record.classification.clone());

        match decision.action {
            RetryAction::Resubmit(adj) => {
                let overrides = RetryOverrides {
                    tip_lamports: Some(
                        bumped_tip(rs.tip_lamports, adj.tip_bump_pct).max(rs.tip_floor_lamports),
                    ),
                    cu_limit: adj.cu_limit,
                    fresh_blockhash: adj.fresh_blockhash,
                };
                let next_attempt = rs.attempt.saturating_add(1);
                match self.engine.execute_retry(rs.intent.clone(), overrides, next_attempt).await {
                    Ok(rep) => info!(
                        prior_id = id,
                        new_id = rep.tracking_id,
                        attempt = next_attempt,
                        tip_lamports = rep.tip_lamports,
                        fix = %adj.summary(),
                        "autonomous resubmit sent",
                    ),
                    Err(e) => warn!(tracking_id = id, "autonomous resubmit failed: {e}"),
                }
            }
            RetryAction::Abort { reason } => {
                info!(tracking_id = id, seq = decision.record.seq, %reason, "AI declined retry")
            }
        }

        // The prior attempt is terminal; drop its registry slot (a resubmit
        // registered a fresh one under a new id).
        self.engine.registry().remove(id);
    }

    /// Consume failure events until the channel closes.
    pub async fn run(self, mut rx: mpsc::Receiver<FailureEvent>) {
        info!("AI retry coordinator running");
        while let Some(ev) = rx.recv().await {
            self.on_failure(ev).await;
        }
        warn!("AI retry coordinator channel closed; stopping");
    }
}

/// Periodically sweep for bundles that never landed and never got a Jito result.
pub async fn run_timeout_sweeper(
    cfg: Arc<Config>,
    state: Arc<HotState>,
    failures: mpsc::Sender<FailureEvent>,
) {
    let timeout_ms = cfg.ai.retry_timeout_ms as u128;
    let tick = Duration::from_millis((cfg.ai.retry_timeout_ms / 2).max(500));
    loop {
        tokio::time::sleep(tick).await;
        for (id, bundle_id) in state.lifecycle.timed_out(timeout_ms) {
            let mut jito_summary = String::from("no bundle_id");
            let mut emit_timeout = true;

            if !cfg.execution.dry_run {
                if let Some(bid) = bundle_id.as_deref() {
                    let sdk =
                        JitoJsonRpcSDK::new(&cfg.jito.json_rpc_url, cfg.jito.auth_uuid.clone());
                    match diagnostics::sweeper_precheck(&state, &sdk, id, bid).await {
                        diagnostics::SweeperPrecheck::EmitTimeout { jito_summary: s } => {
                            jito_summary = s;
                        }
                        diagnostics::SweeperPrecheck::Skip => {
                            emit_timeout = false;
                        }
                        diagnostics::SweeperPrecheck::RouteFailure(ev) => {
                            emit_timeout = false;
                            state.lifecycle.mark_auction_watchdog_logged(id);
                            state.lifecycle.on_failure(id, FailureClass::Unknown);
                            if failures.send(ev).await.is_err() {
                                warn!("timeout sweeper: coordinator channel closed; stopping");
                                return;
                            }
                        }
                    }
                }
            }

            if !emit_timeout {
                continue;
            }

            state.lifecycle.mark_auction_watchdog_logged(id);
            state.lifecycle.on_failure(id, FailureClass::AuctionTimeout);

            if !cfg.execution.dry_run {
                if let Some(bid) = bundle_id.clone() {
                    diagnostics::spawn_jito_timeout_diagnostic(
                        state.clone(),
                        cfg.jito.json_rpc_url.clone(),
                        cfg.jito.auth_uuid.clone(),
                        id,
                        bid,
                    );
                }
            }

            let ev = FailureEvent {
                tracking_id: Some(id),
                bundle_id,
                raw_reason: format!(
                    "no land within {}ms; jito: {jito_summary}",
                    cfg.ai.retry_timeout_ms
                ),
                class_hint: FailureClass::AuctionTimeout,
                logs: vec![],
            };
            if failures.send(ev).await.is_err() {
                warn!("timeout sweeper: coordinator channel closed; stopping");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turbine_ai::{Analyst, AnalystVerdict, RetryAdjustments};
    use turbine_core::ai::DecisionOutcome;
    use turbine_core::blockhash::CachedBlockhash;
    use turbine_core::leader::LeaderView;
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
watched_accounts = []
[strategy]
max_tip_lamports = 100000000
[execution]
dry_run = true
[ai]
enabled = false
max_attempts = 3
spend_cap_lamports_per_min = 100000000000
"#;

    fn seeded() -> (Arc<Config>, Arc<HotState>) {
        let cfg = Arc::new(Config::from_toml_str(SAMPLE).unwrap());
        let state = Arc::new(HotState::new(&cfg));
        state.set_blockhash(CachedBlockhash {
            blockhash: "11111111111111111111111111111111".into(),
            last_valid_block_height: 0,
            slot: 0,
            fetched_at: std::time::Instant::now(),
        });
        state.set_tip_accounts(vec![solana_pubkey::Pubkey::new_from_array([9u8; 32])]);
        state.set_tips(TipSnapshot { p25: 1_000, p50: 6_700, p75: 20_000, p95: 460_000, p99: 1_800_000 });
        let slot = 1_000u64;
        state.set_slot(slot);
        state.set_leader(LeaderView {
            next_jito_leader_slot: Some(slot + cfg.strategy.gate_min),
            slots_until_leader: Some(cfg.strategy.gate_min),
        });
        state.set_geyser_healthy(true);
        (cfg, state)
    }

    fn retry_ai(cfg: &Config, state: Arc<HotState>) -> Arc<AiEngine> {
        let verdict = AnalystVerdict {
            classification: "blockhash_expired".into(),
            root_cause: "stale blockhash".into(),
            adjustments: RetryAdjustments {
                tip_bump_pct: Some(0.5),
                cu_limit: Some(300_000),
                fresh_blockhash: true,
                rebuild: true,
                ..Default::default()
            },
            should_retry: true,
            confidence: 0.9,
        };
        Arc::new(AiEngine::with_analyst(cfg, state, Analyst::Mock(Box::new(verdict))))
    }

    #[tokio::test]
    async fn autonomous_loop_rebuilds_and_resubmits() {
        let (cfg, state) = seeded();
        let engine = Arc::new(ExecutionEngine::new(cfg.clone(), state.clone(), None).unwrap());
        let ai = retry_ai(&cfg, state.clone());
        let coord = RetryCoordinator::new(cfg.clone(), engine.clone(), ai, state.clone());

        let rep = engine.execute(TradeIntent::mock("t")).await.unwrap();
        assert_eq!(rep.attempt, 0);
        let bundles_before = state.lifecycle.len();

        coord
            .on_failure(FailureEvent {
                tracking_id: Some(rep.tracking_id),
                bundle_id: None,
                raw_reason: "synthetic".into(),
                class_hint: FailureClass::BlockhashExpired,
                logs: vec![],
            })
            .await;

        // A new bundle (the retry) was submitted with attempt incremented + tip bumped.
        assert_eq!(state.lifecycle.len(), bundles_before + 1);
        let rec = &state.ai_audit.snapshot()[0];
        assert_eq!(rec.outcome, DecisionOutcome::Resubmitted);
        assert_eq!(rec.tracking_id, Some(rep.tracking_id));
        // Original attempt's registry slot was cleared; only the retry remains.
        assert!(engine.registry().get(rep.tracking_id).is_none());
        assert_eq!(engine.registry().len(), 1);
    }

    #[tokio::test]
    async fn idempotency_blocks_autonomous_retry_when_landed() {
        let (cfg, state) = seeded();
        let engine = Arc::new(ExecutionEngine::new(cfg.clone(), state.clone(), None).unwrap());
        let ai = retry_ai(&cfg, state.clone());
        let coord = RetryCoordinator::new(cfg.clone(), engine.clone(), ai, state.clone());

        let rep = engine.execute(TradeIntent::mock("t")).await.unwrap();
        // Pretend the "failed" bundle actually landed on-chain.
        let sig = engine.registry().get(rep.tracking_id).unwrap().sigs[0];
        state.lifecycle.on_self_tx(&sig, 1_234);
        let bundles_before = state.lifecycle.len();

        coord
            .on_failure(FailureEvent {
                tracking_id: Some(rep.tracking_id),
                bundle_id: None,
                raw_reason: "dropped".into(),
                class_hint: FailureClass::BundleDropped,
                logs: vec![],
            })
            .await;

        // No resubmit — guard prevented double execution.
        assert_eq!(state.lifecycle.len(), bundles_before);
        assert_eq!(state.ai_audit.snapshot()[0].outcome, DecisionOutcome::AbortedLanded);
    }
}
