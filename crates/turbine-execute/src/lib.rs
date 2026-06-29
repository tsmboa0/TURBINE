//! `turbine-execute` — the execution engine (plan §7).
//!
//! Pipeline: **fee matrix → bundle compiler → lookahead gate → submit**. The
//! engine reads the lock-free [`HotState`] (contention, smoothed tips, warm
//! blockhash, leader view, slot) and never touches the network on the hot path
//! except the final submit call.

pub mod compiler;
pub mod compute;
pub mod coordinator;
pub mod diagnostics;
pub mod fee;
pub mod gate;
pub mod leader;
pub mod outcomes;
pub mod schedule;
pub mod scenarios;
pub mod searcher;
pub mod submit;
pub mod tx;

/// Generated Jito searcher gRPC client (from vendored protos via `tonic-build`).
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/jito.rs"));
}

use std::sync::Arc;
use std::time::Instant;

use solana_pubkey::Pubkey;
use solana_sdk::instruction::Instruction;
use solana_sdk::signature::Keypair;
use tracing::{debug, info, warn};

use turbine_core::config::Config;
use turbine_core::error::{Result, TurbineError};
use turbine_core::types::{Congestion, FailureClass, Percentile};
use turbine_state::HotState;

pub use compiler::{
    compile_bundle, compile_single_tx_bundle, CompiledBundle, MAX_BUNDLE_TXS, MAX_TRADE_TXS,
};
pub use compute::estimate_transaction_compute_units;
pub use coordinator::{
    run_timeout_sweeper, FailureEvent, InFlightRegistry, RetryCoordinator, RetryState,
};
pub use fee::{select_tip, select_tip_at_percentile, FeeDecision};
pub use gate::{await_window, snapshot as gate_snapshot, GateOutcome, GateSnapshot};
pub use leader::{run_grpc_leader_poller, run_leader_poller};
pub use outcomes::run_bundle_results;
pub use schedule::{run_leader_tracker, run_schedule_refresher};
pub use scenarios::{
    fail_path, fail_path_blockhash, fail_path_tip, fail_path_with_rng, happy_path, happy_path_memo,
    happy_path_single_tx, FailMode, AUTOPILOT, FAIL_PATH, FAIL_PATH_BLOCKHASH, FAIL_PATH_TIP,
    HAPPY_PATH, HAPPY_PATH_MEMO, HAPPY_PATH_SINGLE_TX,
};
pub use searcher::{connect as searcher_connect, SearcherClient};
pub use submit::{SubmitOutcome, Submitter};

/// A unit of work for the engine: trade instruction groups (one tx each) plus the
/// accounts the bundle will write-lock (drives the contention-aware fee matrix).
#[derive(Debug, Clone, Default)]
pub struct TradeIntent {
    pub label: String,
    pub trade_ix_groups: Vec<Vec<Instruction>>,
    pub write_accounts: Vec<Pubkey>,
    /// Live fail-path testing only: force a specific (e.g. stale/bogus) blockhash on
    /// the **first attempt** so a *real* `BlockhashNotFound` failure is produced and
    /// routed to the AI. Cleared automatically on AI retries so the AI's fix (a fresh
    /// warm blockhash) actually takes effect. `None` in production.
    pub force_blockhash: Option<String>,
    /// Live fail-path testing only: force an absolute tip (e.g. below the Jito
    /// minimum) on the **first attempt** to trigger a real reject/drop. Bypasses the
    /// fee-matrix clamp. Cleared on AI retries so the AI's bump can land. `None` in
    /// production.
    pub force_tip_lamports: Option<u64>,
    /// Live happy-path testing only: force a percentile floor (e.g. P75) on the
    /// **first attempt** instead of the contention-driven tier. Still applies the
    /// normal bump + clamp. Cleared on AI retries. `None` in production.
    pub force_percentile: Option<Percentile>,
    /// Live testing only: compile the first trade leg + tip into **one** signed
    /// transaction (Jito `basic_bundle` style) instead of separate trade + tip txs.
    pub single_tx_bundle: bool,
}

impl TradeIntent {
    /// A minimal scenario with no trade txs — the bundle is just the tip tx.
    /// Useful for exercising the full pipeline without a strategy.
    pub fn mock(label: impl Into<String>) -> Self {
        Self { label: label.into(), ..Default::default() }
    }
}

/// AI-sanctioned overrides applied when rebuilding a bundle for an autonomous
/// retry. Produced by the [`coordinator::RetryCoordinator`] from a governor-clamped
/// [`turbine_ai::RetryAdjustments`].
#[derive(Debug, Clone, Default)]
pub struct RetryOverrides {
    /// Absolute tip to use (already bumped + clamped by the governor).
    pub tip_lamports: Option<u64>,
    /// Compute-unit limit to set via a ComputeBudget instruction.
    pub cu_limit: Option<u32>,
    /// Informational: the warm cache always provides the freshest blockhash, so a
    /// rebuild re-signs against it automatically; this flags intent for the audit.
    pub fresh_blockhash: bool,
}

/// Outcome of one execution attempt (logged + returned for tests/IPC).
#[derive(Debug, Clone)]
pub struct ExecReport {
    pub label: String,
    /// Internal lifecycle tracking id (used to resolve autonomous retries).
    pub tracking_id: u64,
    pub congestion: Congestion,
    pub percentile: Percentile,
    pub tip_lamports: u64,
    pub tip_account: Pubkey,
    pub tx_count: usize,
    pub bundle_b64_bytes: usize,
    pub build_us: u128,
    pub gate: GateOutcome,
    pub submit_us: Option<u128>,
    pub bundle_id: Option<String>,
    pub attempt: u8,
    pub dry_run: bool,
}

/// The execution engine: holds the payer keypair, the submitter, and shared state.
pub struct ExecutionEngine {
    cfg: Arc<Config>,
    state: Arc<HotState>,
    payer: Keypair,
    submitter: Submitter,
    /// In-flight intent registry so the AI coordinator can rebuild + resubmit.
    registry: Arc<InFlightRegistry>,
    /// Optional sink for *synchronous* submit rejections (e.g. a too-low tip the
    /// block engine rejects on the wire) so they reach the AI coordinator just like
    /// async stream/timeout failures. `None` for the offline demo paths.
    fail_tx: Option<tokio::sync::mpsc::Sender<FailureEvent>>,
}

impl ExecutionEngine {
    /// Construct the engine. In `dry_run`, a missing/invalid wallet keypair is
    /// tolerated (an ephemeral key is generated) so the pipeline stays runnable.
    ///
    /// When live and a `searcher` channel is supplied, submission uses gRPC primary
    /// with JSON-RPC `sendBundle` fallback; otherwise JSON-RPC only. The searcher
    /// channel also feeds the bundle-result stream in the daemon.
    pub fn new(
        cfg: Arc<Config>,
        state: Arc<HotState>,
        searcher: Option<SearcherClient>,
    ) -> Result<Self> {
        let payer = match tx::load_keypair(&cfg.wallet.keypair_path) {
            Ok(k) => k,
            Err(e) if cfg.execution.dry_run => {
                warn!("wallet keypair load failed ({e}); using ephemeral key (dry_run)");
                Keypair::new()
            }
            Err(e) => return Err(e),
        };
        let submitter = if cfg.execution.dry_run {
            Submitter::DryRun
        } else if let Some(client) = searcher {
            Submitter::grpc(client, &cfg.jito.json_rpc_url, cfg.jito.auth_uuid.clone())
        } else {
            Submitter::http(&cfg.jito.json_rpc_url, cfg.jito.auth_uuid.clone())
        };
        Ok(Self {
            cfg,
            state,
            payer,
            submitter,
            registry: Arc::new(InFlightRegistry::new()),
            fail_tx: None,
        })
    }

    /// Attach the AI failure bus so synchronous submit rejections are routed to the
    /// coordinator (chainable on construction in the live daemon).
    pub fn with_fail_sink(mut self, tx: tokio::sync::mpsc::Sender<FailureEvent>) -> Self {
        self.fail_tx = Some(tx);
        self
    }

    /// Payer public key — used by scenario builders to construct self-transfer legs.
    pub fn payer_pubkey(&self) -> Pubkey {
        use solana_sdk::signer::Signer;
        self.payer.pubkey()
    }

    /// Shared in-flight registry (hand to the [`RetryCoordinator`]).
    pub fn registry(&self) -> Arc<InFlightRegistry> {
        self.registry.clone()
    }

    /// Run the full pipeline for one intent (first attempt).
    pub async fn execute(&self, intent: TradeIntent) -> Result<ExecReport> {
        self.execute_inner(intent, 0, None).await
    }

    /// Rebuild + resubmit an intent with AI-sanctioned overrides (autonomous retry).
    pub async fn execute_retry(
        &self,
        intent: TradeIntent,
        overrides: RetryOverrides,
        attempt: u8,
    ) -> Result<ExecReport> {
        self.execute_inner(intent, attempt, Some(overrides)).await
    }

    async fn execute_inner(
        &self,
        intent: TradeIntent,
        attempt: u8,
        overrides: Option<RetryOverrides>,
    ) -> Result<ExecReport> {
        // 1) Price (contention → percentile → smoothed lamports). Priority:
        //    AI retry override → live fail-path force (attempt 0 only) → matrix
        //    (or happy-path forced percentile on attempt 0).
        let fee = if attempt == 0 {
            intent
                .force_percentile
                .map(|p| fee::select_tip_at_percentile(&self.state, &intent.write_accounts, &self.cfg.strategy, p))
        } else {
            None
        }
        .unwrap_or_else(|| select_tip(&self.state, &intent.write_accounts, &self.cfg.strategy));
        let tip_lamports = overrides
            .as_ref()
            .and_then(|o| o.tip_lamports)
            .or_else(|| if attempt == 0 { intent.force_tip_lamports } else { None })
            .unwrap_or(fee.tip_lamports);
        debug!(
            label = %intent.label,
            attempt,
            congestion = ?fee.congestion,
            percentile = fee.percentile.label(),
            max_z = fee.max_z,
            bump_pct = fee.bump_pct,
            floor_lamports = self.state.tips().lamports(fee.percentile),
            tip_lamports,
            ai_override = overrides.as_ref().and_then(|o| o.tip_lamports).is_some(),
            force_tip = attempt == 0 && intent.force_tip_lamports.is_some(),
            force_percentile = ?if attempt == 0 { intent.force_percentile } else { None },
            force_blockhash = attempt == 0 && intent.force_blockhash.is_some(),
            "fee decision"
        );

        // 2) Compile + sign (ComputeBudget first on every tx; tip tx last). Retries
        //    always use the warm blockhash; attempt 0 may force a stale hash for live
        //    fail-path testing. AI retry may override the CU limit.
        let cu_limit_override = overrides.as_ref().and_then(|o| o.cu_limit);
        let groups = intent.trade_ix_groups.clone();

        let t0 = Instant::now();
        let bh_arc = self.state.blockhash();
        let bh = bh_arc
            .as_ref()
            .as_ref()
            .ok_or_else(|| TurbineError::Execute("no warm blockhash cached".into()))?;
        let blockhash_str = if attempt == 0 {
            intent.force_blockhash.as_deref().unwrap_or(&bh.blockhash)
        } else {
            &bh.blockhash
        };
        let tip_accounts = self.state.tip_accounts();
        let bundle = if intent.single_tx_bundle {
            let trade_ixs = groups.into_iter().find(|g| !g.is_empty()).unwrap_or_default();
            compile_single_tx_bundle(
                &self.payer,
                trade_ixs,
                tip_lamports,
                &tip_accounts,
                blockhash_str,
                cu_limit_override,
            )?
        } else {
            compile_bundle(
                &self.payer,
                groups,
                tip_lamports,
                &tip_accounts,
                blockhash_str,
                self.cfg.strategy.max_trades_per_bundle,
                cu_limit_override,
            )?
        };
        let bundle_b64_bytes = bundle.base64.iter().map(String::len).sum();
        let build_us = t0.elapsed().as_micros();

        let mut report = ExecReport {
            label: intent.label.clone(),
            tracking_id: 0,
            congestion: fee.congestion,
            percentile: fee.percentile,
            tip_lamports,
            tip_account: bundle.tip_account,
            tx_count: bundle.txs.len(),
            bundle_b64_bytes,
            build_us,
            gate: GateOutcome::TimedOut,
            submit_us: None,
            bundle_id: None,
            attempt,
            dry_run: self.submitter.is_dry_run(),
        };

        // 4) Lookahead gate (event-driven on slot watch).
        report.gate = await_window(&self.state, &self.cfg).await;
        if report.gate != GateOutcome::Open {
            warn!(label = %report.label, gate = ?report.gate, "gate did not open; bundle not sent");
            return Ok(report);
        }

        let gate = gate::snapshot(&self.state, &self.cfg);
        let submit_ctx = turbine_state::SubmitContext {
            label: Some(intent.label.clone()),
            submit_tip_emas: self.state.tips(),
            submit_slot: gate.slot,
            next_jito_leader_slot: gate.next_jito_leader_slot,
            gate_dist: gate.dist,
            blockhash_age_ms: gate.blockhash_age_ms,
            congestion: fee.congestion,
            max_z: fee.max_z,
            bump_pct: fee.bump_pct,
        };
        info!(
            label = %intent.label,
            attempt,
            slot = gate.slot,
            next_jito_leader_slot = ?gate.next_jito_leader_slot,
            dist = ?gate.dist,
            blockhash_age_ms = ?gate.blockhash_age_ms,
            gate_min = gate.gate_min,
            gate_max = gate.gate_max,
            tip_lamports,
            single_tx_bundle = intent.single_tx_bundle,
            "gate open — submitting",
        );

        // 5) Submit (records lifecycle on the submit timestamp) and register the
        //    intent so the AI coordinator can rebuild + resubmit on failure.
        let id = self
            .state
            .lifecycle
            .on_submit(bundle.sigs.clone(), attempt, tip_lamports, fee.percentile, submit_ctx);
        report.tracking_id = id;
        let tip_floor_lamports = self.state.tips().lamports(fee.percentile);
        let bh_meta = bh_arc.as_ref().as_ref();
        self.registry.insert(
            id,
            RetryState {
                intent: intent.clone(),
                attempt,
                tip_lamports,
                tip_floor_lamports,
                percentile: fee.percentile,
                blockhash: blockhash_str.to_string(),
                blockhash_age_ms: gate.blockhash_age_ms,
                blockhash_cached_slot: bh_meta.map(|b| b.slot),
                blockhash_last_valid_height: bh_meta.map(|b| b.last_valid_block_height),
                blockhash_forced_stale: attempt == 0 && intent.force_blockhash.is_some(),
                sigs: bundle.sigs.clone(),
                bundle_id: None,
            },
        );

        let t_sub = Instant::now();
        match self.submitter.submit(&bundle).await {
            Ok(outcome) => {
                report.submit_us = Some(t_sub.elapsed().as_micros());
                if let Some(bid) = outcome.bundle_id {
                    if let Some(attach) = self.state.lifecycle.set_bundle_id(id, bid.clone()) {
                        info!(
                            tracking_id = attach.tracking_id,
                            bundle_id = %bid,
                            state = ?attach.state,
                            drained = attach.terminal_failure.is_some(),
                            "bundle result applied (drained pending gRPC result)",
                        );
                        if let Some(class) = attach.terminal_failure {
                            Self::route_failure(
                                &self.fail_tx,
                                attach.tracking_id,
                                Some(bid.clone()),
                                class,
                                "jito grpc stream (buffered before bundle id indexed)".into(),
                            );
                        }
                    }
                    self.registry.set_bundle_id(id, bid.clone());
                    report.bundle_id = Some(bid.clone());
                    diagnostics::spawn_bundle_status_watcher(
                        self.cfg.clone(),
                        self.state.clone(),
                        id,
                        bid,
                        self.fail_tx.clone(),
                    );
                } else {
                    warn!(
                        target: "turbine::jito_poll",
                        tracking_id = id,
                        attempt,
                        label = %report.label,
                        "submit accepted but returned no bundle_id; JSON-RPC watcher will not run",
                    );
                }
            }
            Err(e) => {
                let reason = e.to_string();
                let class = classify_submit_failure(&reason, attempt, &intent);
                self.state.lifecycle.on_failure(id, class.clone());
                report.submit_us = Some(t_sub.elapsed().as_micros());
                if let Some(tx) = &self.fail_tx {
                    let ev = FailureEvent {
                        tracking_id: Some(id),
                        bundle_id: None,
                        raw_reason: reason.clone(),
                        class_hint: class,
                        logs: Vec::new(),
                    };
                    let _ = tx.send(ev).await;
                }
                warn!(
                    label = %report.label,
                    tracking_id = id,
                    attempt,
                    %reason,
                    "submit rejected; routed to AI coordinator",
                );
            }
        }

        info!(
            label = %report.label,
            tracking_id = id,
            attempt,
            congestion = ?report.congestion,
            percentile = ?report.percentile,
            tip_lamports = report.tip_lamports,
            tx_count = report.tx_count,
            build_us = report.build_us,
            submit_us = ?report.submit_us,
            dry_run = report.dry_run,
            bundle_id = ?report.bundle_id,
            "bundle executed",
        );
        Ok(report)
    }

    fn route_failure(
        fail_tx: &Option<tokio::sync::mpsc::Sender<FailureEvent>>,
        tracking_id: u64,
        bundle_id: Option<String>,
        class: FailureClass,
        raw_reason: String,
    ) {
        if let Some(tx) = fail_tx {
            let ev = FailureEvent {
                tracking_id: Some(tracking_id),
                bundle_id,
                raw_reason,
                class_hint: class,
                logs: vec![],
            };
            let _ = tx.try_send(ev);
        }
    }
}

/// Classify a synchronous submit rejection from the Jito error text only.
fn classify_submit_failure(reason: &str, _attempt: u8, _intent: &TradeIntent) -> FailureClass {
    let lower = reason.to_ascii_lowercase();
    if lower.contains("blockhash") || lower.contains("block hash") {
        return FailureClass::BlockhashExpired;
    }
    if lower.contains("tip") {
        return FailureClass::TipTooLow;
    }
    FailureClass::Unknown
}
