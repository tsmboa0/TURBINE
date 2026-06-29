//! Lookahead submission gate (plan §7.3).
//!
//! Event-driven (awaits the slot `watch`, never busy-spins). Fires when the next
//! Jito leader is `[gate_min, gate_max]` slots away — but only when Geyser is
//! healthy and the cached blockhash is fresh.

use std::time::Duration;

use turbine_core::config::Config;
use turbine_state::HotState;

/// Outcome of waiting for the submission window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateOutcome {
    /// Window is open — submit now.
    Open,
    /// No window within the configured max wait.
    TimedOut,
    /// Slot stream closed / unrecoverable.
    StreamClosed,
}

fn blockhash_fresh(state: &HotState, cfg: &Config) -> bool {
    state
        .blockhash()
        .as_ref()
        .as_ref()
        .map(|b| b.age_ms() <= cfg.execution.blockhash_max_age_ms)
        .unwrap_or(false)
}

/// Returns `true` when all submit preconditions hold right now.
///
/// Gate opens only when the next Jito-led slot is **`gate_min..=gate_max` slots
/// away** (`dist = next_jito_leader_slot - current_slot`). With `gate_min =
/// gate_max = 1` we submit one slot before the Jito leader (~400ms early) and
/// never fire early into a multi-slot non-Jito streak.
fn window_open(state: &HotState, cfg: &Config) -> bool {
    if state.submission_killed() {
        return false;
    }
    let cur = state.slot();
    let Some(next) = state.leader().next_jito_leader_slot else {
        return false;
    };
    let dist = next.saturating_sub(cur);
    (cfg.strategy.gate_min..=cfg.strategy.gate_max).contains(&dist)
        && state.geyser_healthy()
        && blockhash_fresh(state, cfg)
}

/// Read-only gate/window snapshot for structured logging at submit time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GateSnapshot {
    pub slot: u64,
    pub next_jito_leader_slot: Option<u64>,
    pub dist: Option<u64>,
    pub blockhash_age_ms: Option<u64>,
    pub gate_min: u64,
    pub gate_max: u64,
}

/// Capture the gate view right now (no I/O; safe on the hot path).
pub fn snapshot(state: &HotState, cfg: &Config) -> GateSnapshot {
    let slot = state.slot();
    let next = state.leader().next_jito_leader_slot;
    let dist = next.map(|n| n.saturating_sub(slot));
    let blockhash_age_ms = state
        .blockhash()
        .as_ref()
        .as_ref()
        .map(|b| b.age_ms());
    GateSnapshot {
        slot,
        next_jito_leader_slot: next,
        dist,
        blockhash_age_ms,
        gate_min: cfg.strategy.gate_min,
        gate_max: cfg.strategy.gate_max,
    }
}

/// Await the submission window, recomputing on each slot tick.
pub async fn await_window(state: &HotState, cfg: &Config) -> GateOutcome {
    let mut slot_rx = state.subscribe_slot();
    let deadline =
        tokio::time::Instant::now() + Duration::from_millis(cfg.execution.gate_max_wait_ms);

    loop {
        if window_open(state, cfg) {
            return GateOutcome::Open;
        }
        tokio::select! {
            changed = slot_rx.changed() => {
                if changed.is_err() {
                    return GateOutcome::StreamClosed;
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                return GateOutcome::TimedOut;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Instant;
    use turbine_core::blockhash::CachedBlockhash;
    use turbine_core::config::Config;
    use turbine_core::leader::LeaderView;

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
gate_min = 1
gate_max = 1
"#;

    fn seed_fresh_blockhash(state: &HotState) {
        state.set_blockhash(CachedBlockhash {
            blockhash: "11111111111111111111111111111111".into(),
            last_valid_block_height: 100,
            slot: 1000,
            fetched_at: Instant::now(),
        });
    }

    #[tokio::test]
    async fn opens_immediately_when_in_window() {
        let cfg = Config::from_toml_str(SAMPLE).unwrap();
        let state = HotState::new(&cfg);
        state.set_slot(1000);
        state.set_leader(LeaderView { next_jito_leader_slot: Some(1001), slots_until_leader: Some(1) });
        state.set_geyser_healthy(true);
        seed_fresh_blockhash(&state);
        assert_eq!(await_window(&state, &cfg).await, GateOutcome::Open);
    }

    #[tokio::test]
    async fn opens_when_leader_one_slot_away() {
        let cfg = Config::from_toml_str(SAMPLE).unwrap();
        let state = HotState::new(&cfg);
        state.set_slot(1000);
        state.set_leader(LeaderView { next_jito_leader_slot: Some(1001), slots_until_leader: Some(1) });
        state.set_geyser_healthy(true);
        seed_fresh_blockhash(&state);
        assert_eq!(await_window(&state, &cfg).await, GateOutcome::Open);
    }

    #[tokio::test]
    async fn refuses_when_leader_too_far() {
        let mut cfg = Config::from_toml_str(SAMPLE).unwrap();
        cfg.execution.gate_max_wait_ms = 80;
        let state = HotState::new(&cfg);
        state.set_slot(1000);
        state.set_leader(LeaderView { next_jito_leader_slot: Some(1003), slots_until_leader: Some(3) });
        state.set_geyser_healthy(true);
        seed_fresh_blockhash(&state);
        assert_eq!(await_window(&state, &cfg).await, GateOutcome::TimedOut);
    }

    #[tokio::test]
    async fn refuses_when_unhealthy() {
        let mut cfg = Config::from_toml_str(SAMPLE).unwrap();
        cfg.execution.gate_max_wait_ms = 80;
        let state = HotState::new(&cfg);
        state.set_slot(1000);
        state.set_leader(LeaderView { next_jito_leader_slot: Some(1002), slots_until_leader: Some(2) });
        state.set_geyser_healthy(false); // unhealthy → never opens
        seed_fresh_blockhash(&state);
        assert_eq!(await_window(&state, &cfg).await, GateOutcome::TimedOut);
    }

    #[test]
    fn snapshot_captures_dist_and_blockhash_age() {
        let cfg = Config::from_toml_str(SAMPLE).unwrap();
        let state = HotState::new(&cfg);
        state.set_slot(1000);
        state.set_leader(LeaderView { next_jito_leader_slot: Some(1001), slots_until_leader: Some(1) });
        seed_fresh_blockhash(&state);
        let snap = snapshot(&state, &cfg);
        assert_eq!(snap.slot, 1000);
        assert_eq!(snap.next_jito_leader_slot, Some(1001));
        assert_eq!(snap.dist, Some(1));
        assert!(snap.blockhash_age_ms.is_some());
        assert_eq!(snap.gate_min, 1);
        assert_eq!(snap.gate_max, 1);
    }

    #[tokio::test]
    async fn opens_when_slot_advances_into_window() {
        let cfg = Arc::new(Config::from_toml_str(SAMPLE).unwrap());
        let state = Arc::new(HotState::new(&cfg));
        // Leader fixed at 1005; start at dist=5, advance until dist=1.
        state.set_slot(1000);
        state.set_leader(LeaderView { next_jito_leader_slot: Some(1005), slots_until_leader: Some(5) });
        state.set_geyser_healthy(true);
        seed_fresh_blockhash(&state);

        let s2 = state.clone();
        let c2 = cfg.clone();
        let h = tokio::spawn(async move { await_window(&s2, &c2).await });

        for slot in 1001..=1004 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            state.set_slot(slot); // at 1004, dist = 1 → opens
        }
        assert_eq!(h.await.unwrap(), GateOutcome::Open);
    }
}
