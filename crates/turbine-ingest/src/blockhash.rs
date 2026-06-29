//! Warm blockhash refresher (plan §6).
//!
//! Polls `getLatestBlockhash` on a fixed cadence and publishes the result on a
//! `watch` channel (latest-value only). The submit path then reads the cached
//! blockhash from `HotState` and never blocks on an RPC round-trip.

use std::time::{Duration, Instant};

use tokio::sync::watch;
use tracing::{info, warn};

use turbine_core::blockhash::CachedBlockhash;
use turbine_core::error::{Result, TurbineError};

/// Fetch the latest blockhash (confirmed commitment) via JSON-RPC.
pub async fn fetch_latest_blockhash(rpc_url: &str) -> Result<CachedBlockhash> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getLatestBlockhash",
        "params": [{ "commitment": "confirmed" }]
    });
    let resp: serde_json::Value = reqwest::Client::new()
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .map_err(|e| TurbineError::Ingest(format!("getLatestBlockhash request: {e}")))?
        .json()
        .await
        .map_err(|e| TurbineError::Ingest(format!("getLatestBlockhash decode: {e}")))?;

    if let Some(err) = resp.get("error") {
        return Err(TurbineError::Ingest(format!("getLatestBlockhash rpc error: {err}")));
    }

    let result = &resp["result"];
    let slot = result["context"]["slot"].as_u64().unwrap_or(0);
    let value = &result["value"];
    let blockhash = value["blockhash"]
        .as_str()
        .ok_or_else(|| TurbineError::Ingest("getLatestBlockhash: missing blockhash".into()))?
        .to_string();
    let last_valid_block_height = value["lastValidBlockHeight"].as_u64().unwrap_or(0);

    Ok(CachedBlockhash {
        blockhash,
        last_valid_block_height,
        slot,
        fetched_at: Instant::now(),
    })
}

/// Long-running task: refresh the blockhash every `interval_ms` and publish it.
pub async fn run_blockhash_refresher(
    rpc_url: String,
    interval_ms: u64,
    tx: watch::Sender<Option<CachedBlockhash>>,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(interval_ms.max(250)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        match fetch_latest_blockhash(&rpc_url).await {
            Ok(bh) => {
                if tx.send(Some(bh)).is_err() {
                    break; // all receivers dropped
                }
            }
            Err(e) => warn!("{e}; will retry next tick"),
        }
    }
    info!("blockhash refresher stopped");
}
