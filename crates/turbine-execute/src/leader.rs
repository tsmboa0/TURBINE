//! Leader poller (plan §4.2, §7.2) — populates `HotState.leader`.
//!
//! Computes the next slot led by a Jito-enabled validator from the configured
//! `jito.validator_identities` via RPC `getLeaderSchedule` + `getEpochInfo`.
//!
//! If no identities are configured, it falls back to a synthetic next-leader slot
//! a few slots ahead of the current slot, so the lookahead gate can still be
//! exercised end to end. The true low-latency source is the Jito gRPC
//! `get_next_scheduled_leader`, which lands when the searcher client is wired.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, warn};

use turbine_core::config::Config;
use turbine_core::error::{Result, TurbineError};
use turbine_core::leader::LeaderView;
use turbine_state::HotState;

async fn rpc(url: &str, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
    let body = serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let resp: serde_json::Value = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(|e| TurbineError::Execute(format!("{method} request: {e}")))?
        .json()
        .await
        .map_err(|e| TurbineError::Execute(format!("{method} decode: {e}")))?;
    if let Some(err) = resp.get("error") {
        return Err(TurbineError::Execute(format!("{method} rpc error: {err}")));
    }
    Ok(resp)
}

/// Compute the next Jito leader slot from the leader schedule for the watched
/// validator identities (absolute slots strictly after `current_slot`).
async fn refresh(cfg: &Config, state: &HotState) -> Result<()> {
    let cur = state.slot();

    // Fallback when no validator identities are configured: synthesize a leader a
    // few slots ahead so the gate is testable. Only meaningful once slots flow.
    if cfg.jito.validator_identities.is_empty() {
        if cur > 0 {
            let next = cur + cfg.strategy.gate_max + 1;
            state.set_leader(LeaderView {
                next_jito_leader_slot: Some(next),
                slots_until_leader: Some(next - cur),
            });
        }
        return Ok(());
    }

    let epoch = rpc(&cfg.rpc.http_url, "getEpochInfo", serde_json::json!([])).await?;
    let absolute_slot = epoch["result"]["absoluteSlot"].as_u64().unwrap_or(cur);
    let slot_index = epoch["result"]["slotIndex"].as_u64().unwrap_or(0);
    let first_slot = absolute_slot.saturating_sub(slot_index);

    let sched = rpc(&cfg.rpc.http_url, "getLeaderSchedule", serde_json::json!([null])).await?;
    let map: HashMap<String, Vec<u64>> = serde_json::from_value(sched["result"].clone())
        .map_err(|e| TurbineError::Execute(format!("leader schedule shape: {e}")))?;

    let mut next: Option<u64> = None;
    for id in &cfg.jito.validator_identities {
        if let Some(rel) = map.get(&id.to_string()) {
            for r in rel {
                let abs = first_slot + *r;
                if abs > cur {
                    next = Some(next.map_or(abs, |n| n.min(abs)));
                }
            }
        }
    }
    state.set_leader(LeaderView {
        next_jito_leader_slot: next,
        slots_until_leader: next.map(|n| n.saturating_sub(cur)),
    });
    Ok(())
}

/// gRPC leader poller (preferred): asks the block engine for the next Jito leader
/// directly over the warm searcher channel — already filtered, no RPC round-trip.
pub async fn run_grpc_leader_poller(
    cfg: Arc<Config>,
    state: Arc<HotState>,
    client: crate::searcher::SearcherClient,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(
        cfg.execution.leader_refresh_ms.max(500),
    ));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let mut c = client.clone();
        match crate::searcher::next_scheduled_leader(&mut c, Vec::new()).await {
            Ok(resp) => {
                // Keep the slot fresh from the block engine's own view if Geyser
                // hasn't advanced it yet.
                if state.slot() == 0 {
                    state.set_slot(resp.current_slot);
                }
                let next = resp.next_leader_slot;
                state.set_leader(LeaderView {
                    next_jito_leader_slot: Some(next),
                    slots_until_leader: Some(next.saturating_sub(resp.current_slot)),
                });
                debug!(
                    next_leader_slot = next,
                    region = %resp.next_leader_region,
                    "gRPC leader view updated"
                );
            }
            Err(e) => warn!("gRPC leader poll failed: {e}"),
        }
    }
}

/// Long-running RPC leader poller (stand-in when no gRPC channel is available).
pub async fn run_leader_poller(cfg: Arc<Config>, state: Arc<HotState>) {
    let mut tick = tokio::time::interval(Duration::from_millis(
        cfg.execution.leader_refresh_ms.max(500),
    ));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        match refresh(&cfg, &state).await {
            Ok(()) => {
                if let Some(n) = state.leader().next_jito_leader_slot {
                    debug!(next_jito_leader_slot = n, "leader view updated");
                }
            }
            Err(e) => warn!("leader refresh failed: {e}"),
        }
    }
}
