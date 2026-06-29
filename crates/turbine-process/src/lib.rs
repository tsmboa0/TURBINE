//! `turbine-process` — Component 2, the Data Processing Unit (plan §5).
//!
//! Decodes the ingestion streams and mutates the shared [`HotState`]:
//! - extracts each target tx's **write-lock set** and feeds per-account contention,
//! - folds raw Jito tips through per-percentile EMAs and publishes a `TipSnapshot`,
//! - advances **transaction lifecycle** on own-wallet txs and slot commitments,
//! - mirrors the warm blockhash and tracks Geyser health/liveness.
//!
//! All hot state lives in `turbine-state`; this crate is the decode + fold logic
//! plus the single-consumer loop that drives it.

pub mod extract;
pub mod tips;

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tracing::{debug, info};
use yellowstone_grpc_proto::geyser::SlotStatus;

use turbine_core::config::Config;
use turbine_core::types::Congestion;
use turbine_ingest::{geyser::FILTER_SELF, geyser::FILTER_TARGETS, GeyserMessage, IngestChannels};
use turbine_state::HotState;

use crate::extract::{signature_bytes, writable_accounts};
use crate::tips::TipState;

/// How long without a Geyser update before we flag the stream unhealthy.
const GEYSER_STALE_AFTER: Duration = Duration::from_secs(5);

/// Spawn the processing loop, taking ownership of the ingestion receivers and a
/// shared handle to the hot state it mutates.
pub fn spawn(cfg: Arc<Config>, state: Arc<HotState>, channels: IngestChannels) -> JoinHandle<()> {
    tokio::spawn(run(cfg, state, channels))
}

async fn run(cfg: Arc<Config>, state: Arc<HotState>, channels: IngestChannels) {
    let IngestChannels { mut geyser_rx, mut tips_rx, mut blockhash_rx, .. } = channels;
    let mut tip_state = TipState::new(cfg.strategy.tip_ema_half_life_ms);

    // Counters + liveness for the periodic status line.
    let mut n_slots: u64 = 0;
    let mut n_target_tx: u64 = 0;
    let mut n_self_tx: u64 = 0;
    let mut last_flushed_slot: u64 = 0;
    let mut last_geyser: Option<Instant> = None;
    // Timestamp of the last chain-head advance, for an accurate slot-interval Δ.
    let mut last_head_at: Option<Instant> = None;

    let mut report = tokio::time::interval(Duration::from_secs(2));
    report.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    info!(watched = state.contention.watched_len(), "processing engine started");

    loop {
        tokio::select! {
            maybe = geyser_rx.recv() => {
                let Some(timed) = maybe else { break };
                last_geyser = Some(Instant::now());
                state.set_geyser_healthy(true);
                match timed.msg {
                    GeyserMessage::Slot(s) => {
                        n_slots += 1;
                        // Monotonic head; measure the interval only on real advances
                        // (ignores the older slot numbers carried by confirmed/finalized).
                        if state.set_slot(s.slot) {
                            let now = Instant::now();
                            if let Some(prev) = last_head_at {
                                state.set_slot_interval_ms(now.duration_since(prev).as_millis() as u64);
                            }
                            last_head_at = Some(now);
                        }
                        match SlotStatus::try_from(s.status) {
                            Ok(SlotStatus::SlotProcessed) => {
                                // One flush per slot drives EMA decay everywhere.
                                if s.slot > last_flushed_slot {
                                    last_flushed_slot = s.slot;
                                    state.contention.flush_slot(Instant::now());
                                }
                            }
                            Ok(SlotStatus::SlotConfirmed) => {
                                state.lifecycle.on_slot_commitment(s.slot, false);
                            }
                            Ok(SlotStatus::SlotFinalized) => {
                                for (id, d) in state.lifecycle.on_slot_commitment(s.slot, true) {
                                    debug!(
                                        bundle = id,
                                        confirmed_to_finalized_ms = d.confirmed_to_finalized_ms,
                                        "bundle finalized"
                                    );
                                }
                            }
                            _ => {}
                        }
                    }
                    GeyserMessage::Transaction { filters, update } => {
                        let Some(info) = update.transaction.as_ref() else { continue };
                        if filters.iter().any(|f| f == FILTER_TARGETS) {
                            n_target_tx += 1;
                            let writable = writable_accounts(info);
                            state.contention.record_writable(&writable);
                        }
                        if filters.iter().any(|f| f == FILTER_SELF) {
                            n_self_tx += 1;
                            if let Some(sig) = signature_bytes(info) {
                                if let Some(d) = state.lifecycle.on_self_tx(&sig, update.slot) {
                                    info!(
                                        slot = update.slot,
                                        submit_to_processed_ms = d.submit_to_processed_ms,
                                        sig = %bs58::encode(sig).into_string(),
                                        "own tx landed"
                                    );
                                }
                            }
                        }
                    }
                    GeyserMessage::BlockMeta(_) => {}
                }
            }
            maybe = tips_rx.recv() => {
                if let Some(raw) = maybe {
                    tip_state.observe(raw, Instant::now());
                    state.set_tips(tip_state.snapshot());
                }
            }
            changed = blockhash_rx.changed() => {
                if changed.is_err() {
                    // refresher gone; stop watching blockhash but keep processing.
                    continue;
                }
                let latest = blockhash_rx.borrow_and_update().clone();
                if let Some(bh) = latest {
                    state.set_blockhash(bh);
                }
            }
            _ = report.tick() => {
                // Mark the stream unhealthy if no Geyser update has arrived recently.
                if last_geyser.map(|t| t.elapsed() > GEYSER_STALE_AFTER).unwrap_or(true) {
                    state.set_geyser_healthy(false);
                }

                let tips = state.tips();
                let (mut hot, mut moderate, mut quiet) = (0usize, 0usize, 0usize);
                let mut max_z = f64::MIN;
                for pk in state.contention.watched().iter() {
                    if let Some(cs) = state.contention.snapshot(pk) {
                        max_z = max_z.max(cs.z);
                        match cs.congestion(cfg.strategy.quiet_z, cfg.strategy.hot_z) {
                            Congestion::Hot => hot += 1,
                            Congestion::Moderate => moderate += 1,
                            Congestion::Quiet => quiet += 1,
                        }
                    } else {
                        quiet += 1;
                    }
                }
                let max_z = if max_z == f64::MIN { 0.0 } else { max_z };
                let bh_age = state
                    .blockhash()
                    .as_ref()
                    .as_ref()
                    .map(|b| b.age_ms() as i64)
                    .unwrap_or(-1);

                info!(
                    slot = state.slot(),
                    healthy = state.geyser_healthy(),
                    slots = n_slots,
                    target_tx = n_target_tx,
                    self_tx = n_self_tx,
                    hot, moderate, quiet,
                    max_z = format!("{max_z:.2}"),
                    tip_p50 = tips.p50,
                    tip_p95 = tips.p95,
                    blockhash_age_ms = bh_age,
                    in_flight = state.lifecycle.in_flight(),
                    "processing status (2s)"
                );
            }
        }
    }
}
