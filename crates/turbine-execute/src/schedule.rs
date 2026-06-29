//! Local Jito leader-schedule cache (plan §7.2).
//!
//! Instead of polling for the next Jito leader every few seconds (which makes the
//! countdown jump), we build the **whole epoch's** Jito leader slot list once at
//! boot and again at each epoch boundary, then derive the next leader + live
//! countdown locally on every slot — no per-slot RPC/gRPC on the gate path.
//!
//! Schedule = (cluster leader schedule) ∩ (validators running Jito). The Jito set
//! comes from the public "kobe" endpoint, unioned with any configured identities.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tracing::{info, warn};

use turbine_core::config::Config;
use turbine_core::error::{Result, TurbineError};
use turbine_core::leader::{JitoSchedule, LeaderView};
use turbine_state::HotState;

/// Nominal mainnet slot time — used only to estimate when the epoch ends so we can
/// schedule the next rebuild. Not on any hot path.
const SLOT_MS: u64 = 400;
/// Don't rebuild more often than this, even near an epoch boundary.
const MIN_REBUILD_INTERVAL: Duration = Duration::from_secs(20);
/// Upper bound between rebuilds so a long epoch still re-fetches the Jito set.
const MAX_REBUILD_INTERVAL: Duration = Duration::from_secs(600);

#[derive(Deserialize)]
struct KobeResponse {
    #[serde(default)]
    validators: Vec<KobeValidator>,
}

#[derive(Deserialize)]
struct KobeValidator {
    identity_account: String,
    #[serde(default)]
    running_jito: bool,
}

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

/// Identities of validators currently running Jito, per the kobe endpoint.
async fn fetch_jito_identities(url: &str) -> Result<HashSet<String>> {
    let resp: KobeResponse = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .map_err(|e| TurbineError::Execute(format!("kobe request: {e}")))?
        .json()
        .await
        .map_err(|e| TurbineError::Execute(format!("kobe decode: {e}")))?;
    Ok(resp
        .validators
        .into_iter()
        .filter(|v| v.running_jito)
        .map(|v| v.identity_account)
        .collect())
}

/// Build the current epoch's Jito leader schedule by intersecting the cluster
/// leader schedule with the Jito-enabled identity set.
async fn build_schedule(cfg: &Config, identities: &HashSet<String>) -> Result<JitoSchedule> {
    let epoch = rpc(&cfg.rpc.http_url, "getEpochInfo", serde_json::json!([])).await?;
    let absolute_slot = epoch["result"]["absoluteSlot"].as_u64().unwrap_or(0);
    let slot_index = epoch["result"]["slotIndex"].as_u64().unwrap_or(0);
    let slots_in_epoch = epoch["result"]["slotsInEpoch"].as_u64().unwrap_or(432_000);
    let epoch_num = epoch["result"]["epoch"].as_u64().unwrap_or(0);
    let first_slot = absolute_slot.saturating_sub(slot_index);
    let last_slot = first_slot + slots_in_epoch.saturating_sub(1);

    let sched = rpc(&cfg.rpc.http_url, "getLeaderSchedule", serde_json::json!([null])).await?;
    let map: std::collections::HashMap<String, Vec<u64>> =
        serde_json::from_value(sched["result"].clone())
            .map_err(|e| TurbineError::Execute(format!("leader schedule shape: {e}")))?;

    let mut slots: Vec<u64> = Vec::new();
    for (identity, rels) in &map {
        if identities.contains(identity) {
            for r in rels {
                slots.push(first_slot + *r);
            }
        }
    }
    slots.sort_unstable();
    slots.dedup();

    Ok(JitoSchedule { epoch: epoch_num, first_slot, last_slot, slots })
}

/// Long-running task: build the Jito leader schedule at boot and rebuild around
/// each epoch boundary. Falls back gracefully — on any failure it keeps the last
/// good schedule (or the synthetic fallback if it never loaded).
pub async fn run_schedule_refresher(cfg: Arc<Config>, state: Arc<HotState>) {
    loop {
        // Jito identity set: kobe ∪ configured (kobe failure → configured only).
        let mut identities: HashSet<String> = match fetch_jito_identities(&cfg.jito.kobe_validators_url).await {
            Ok(ids) => {
                info!(jito_validators = ids.len(), "fetched Jito validator set (kobe)");
                ids
            }
            Err(e) => {
                warn!("kobe fetch failed ({e}); using configured validator_identities only");
                HashSet::new()
            }
        };
        for pk in &cfg.jito.validator_identities {
            identities.insert(pk.to_string());
        }

        let mut sleep = MAX_REBUILD_INTERVAL;
        if identities.is_empty() {
            warn!("no Jito identities available; leader view uses synthetic fallback");
        } else {
            match build_schedule(&cfg, &identities).await {
                Ok(sched) => {
                    let remaining = sched.last_slot.saturating_sub(state.slot().max(sched.first_slot));
                    info!(
                        epoch = sched.epoch,
                        jito_leader_slots = sched.slots.len(),
                        first_slot = sched.first_slot,
                        last_slot = sched.last_slot,
                        "Jito leader schedule loaded"
                    );
                    state.set_jito_schedule(sched);
                    // Rebuild shortly after the epoch ends (plus a small buffer).
                    let until_end = Duration::from_millis(remaining.saturating_mul(SLOT_MS)) + Duration::from_secs(5);
                    sleep = until_end.clamp(MIN_REBUILD_INTERVAL, MAX_REBUILD_INTERVAL);
                }
                Err(e) => {
                    warn!("leader schedule build failed ({e}); will retry");
                    sleep = MIN_REBUILD_INTERVAL;
                }
            }
        }
        tokio::time::sleep(sleep).await;
    }
}

/// Long-running task: recompute the leader view on every chain-head advance.
/// This is the single authoritative writer of `HotState.leader` — the countdown
/// is `next_jito_leader_slot - current_slot`, so it ticks down each slot.
pub async fn run_leader_tracker(cfg: Arc<Config>, state: Arc<HotState>) {
    let mut slot_rx = state.subscribe_slot();
    loop {
        let cur = state.slot();
        if cur > 0 {
            let sched = state.jito_schedule();
            let next = sched
                .next_after(cur)
                // Fallback when the schedule isn't loaded (or the epoch is exhausted
                // before the rebuild lands): synthesize a leader just past the gate
                // window so the pipeline stays exercisable.
                .or(Some(cur + cfg.strategy.gate_max + 1));
            if let Some(n) = next {
                state.set_leader(LeaderView {
                    next_jito_leader_slot: Some(n),
                    slots_until_leader: Some(n.saturating_sub(cur)),
                });
            }
        }
        if slot_rx.changed().await.is_err() {
            break;
        }
    }
}
