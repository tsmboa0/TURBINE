//! Telemetry publisher (plan §9.3): a services-runtime task that snapshots hot
//! state on a fixed cadence and broadcasts [`TelemetryEvent`]s onto the lossy bus.
//!
//! This is the *only* place that reads `HotState` for the dashboard — the TUI
//! render task never touches state or a lock; it consumes the bus. The same bus
//! feeds the web UI (Phase 9), so a single publisher serves both surfaces.

use std::sync::Arc;
use std::time::Duration;

use solana_pubkey::Pubkey;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use turbine_core::config::Config;
use turbine_core::events::TelemetryEvent;
use turbine_state::HotState;

/// Publish cadence. Fast enough for a lively dashboard, far slower than the hot
/// path — and entirely decoupled from it (lossy broadcast).
const TICK: Duration = Duration::from_millis(250);

/// Short, glanceable account label: `AbCd…WxYz`.
fn short_pk(pk: &Pubkey) -> String {
    let s = pk.to_string();
    if s.len() > 9 {
        format!("{}…{}", &s[..4], &s[s.len() - 4..])
    } else {
        s
    }
}

/// Spawn the publisher. Returns the task handle so `start` can abort it on shutdown.
pub fn spawn(
    cfg: Arc<Config>,
    state: Arc<HotState>,
    bus: broadcast::Sender<TelemetryEvent>,
    jito_connected: bool,
) -> JoinHandle<()> {
    // Stable account order for the contention grid.
    let mut watched: Vec<Pubkey> = state.contention.watched().iter().copied().collect();
    watched.sort();

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(TICK);
        let mut last_slot = 0u64;
        let mut ema50 = 0.0f64;

        loop {
            ticker.tick().await;

            // Health.
            let _ = bus.send(TelemetryEvent::Health {
                geyser: state.geyser_healthy(),
                jito: jito_connected,
            });

            // Slot (only when it advances). The interval Δ is measured at ingestion
            // (chain-head leading edge), not sampled here, so it reflects the true
            // slot cadence rather than this 250ms publisher tick.
            let slot = state.slot();
            if slot != last_slot && slot != 0 {
                last_slot = slot;
                let interval_ms = match state.slot_interval_ms() {
                    0 => None,
                    ms => Some(ms),
                };
                let _ = bus.send(TelemetryEvent::Slot {
                    slot,
                    parent: None,
                    status: "processed".into(),
                    interval_ms,
                });
            }

            // Leader window. Derive the countdown against the *live* slot so it ticks
            // down every slot instead of freezing at the last refresh value.
            let leader = state.leader();
            if let Some(next) = leader.next_jito_leader_slot {
                let slots_until = next as i64 - slot as i64;
                let ready = slots_until >= cfg.strategy.gate_min as i64
                    && slots_until <= cfg.strategy.gate_max as i64;
                let _ = bus.send(TelemetryEvent::Leader {
                    next_jito_leader_slot: next,
                    slots_until,
                    ready,
                    identity: None,
                });
            }

            // Tip percentiles + our smoothed EMA baseline.
            let tips = state.tips();
            ema50 = if ema50 == 0.0 {
                tips.p50 as f64
            } else {
                ema50 * 0.8 + tips.p50 as f64 * 0.2
            };
            let _ = bus.send(TelemetryEvent::TipSnapshot {
                p25: tips.p25,
                p50: tips.p50,
                p75: tips.p75,
                p95: tips.p95,
                p99: tips.p99,
                ema50: ema50 as u64,
            });

            // Live fee decision: the tier/tip we'd bid *right now* given current
            // max contention across the watched accounts. Reuses the engine's
            // `select_tip` so the panel shows the real bid (incl. contention bump),
            // not an abstract EMA.
            let fee = turbine_execute::select_tip(&state, &watched, &cfg.strategy);
            let _ = bus.send(TelemetryEvent::Bid {
                congestion: fee.congestion,
                percentile: fee.percentile.label().to_string(),
                tip_lamports: fee.tip_lamports,
                watching: !watched.is_empty(),
            });

            // Per-watched-account contention (every watched account, even quiet
            // ones, so the grid is stable).
            for pk in &watched {
                let level = state.contention.congestion(pk);
                let snap = state.contention.snapshot(pk).unwrap_or_default();
                let _ = bus.send(TelemetryEvent::Contention {
                    account: short_pk(pk),
                    fast_ema: snap.fast,
                    slow_ema: snap.slow,
                    zscore: snap.z,
                    total_hits: snap.total_hits,
                    level,
                });
            }

            // Aggregate counters.
            let _ = bus.send(TelemetryEvent::Stats {
                in_flight: state.lifecycle.in_flight() as u64,
                tracked: state.lifecycle.len() as u64,
                ai_decisions: state.ai_audit.len() as u64,
                killed: state.submission_killed(),
                dry_run: cfg.execution.dry_run,
            });
        }
    })
}
