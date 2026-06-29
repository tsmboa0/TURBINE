//! Jito tip ingestion (plan §4.2).
//!
//! Tip percentile amounts are NOT in the provided JSON-RPC SDK — they come from
//! Jito's public tip endpoints:
//!   - REST  `tip_floor`  : seed at boot / fallback on WS disconnect.
//!   - WS    `tip_stream` : continuous push (primary).
//! Amounts are reported in SOL; we convert to lamports once at ingest so the rest
//! of the system works in integer lamports.
//!
//! Tip accounts (the 8 destinations) come from the JSON-RPC `getTipAccounts`.

use std::time::Duration;

use serde::Deserialize;
use solana_pubkey::Pubkey;
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use futures::StreamExt;
use turbine_core::error::{Result, TurbineError};

/// Smoothed/raw tip percentiles in lamports.
#[derive(Debug, Clone, Copy, Default)]
pub struct TipLamports {
    pub p25: u64,
    pub p50: u64,
    pub p75: u64,
    pub p95: u64,
    pub p99: u64,
    pub ema50: u64,
}

/// Raw row from `tip_floor` / `tip_stream` (amounts in SOL).
#[derive(Debug, Clone, Deserialize)]
struct TipFloorRow {
    #[serde(rename = "landed_tips_25th_percentile")]
    p25: f64,
    #[serde(rename = "landed_tips_50th_percentile")]
    p50: f64,
    #[serde(rename = "landed_tips_75th_percentile")]
    p75: f64,
    #[serde(rename = "landed_tips_95th_percentile")]
    p95: f64,
    #[serde(rename = "landed_tips_99th_percentile")]
    p99: f64,
    #[serde(rename = "ema_landed_tips_50th_percentile")]
    ema50: f64,
}

fn sol_to_lamports(sol: f64) -> u64 {
    (sol * 1_000_000_000.0).round().max(0.0) as u64
}

impl From<TipFloorRow> for TipLamports {
    fn from(r: TipFloorRow) -> Self {
        Self {
            p25: sol_to_lamports(r.p25),
            p50: sol_to_lamports(r.p50),
            p75: sol_to_lamports(r.p75),
            p95: sol_to_lamports(r.p95),
            p99: sol_to_lamports(r.p99),
            ema50: sol_to_lamports(r.ema50),
        }
    }
}

/// Both endpoints return either a one-element array or a bare object; handle both.
fn parse_tip_payload(txt: &str) -> Option<TipLamports> {
    if let Ok(rows) = serde_json::from_str::<Vec<TipFloorRow>>(txt) {
        return rows.into_iter().next().map(Into::into);
    }
    serde_json::from_str::<TipFloorRow>(txt).ok().map(Into::into)
}

/// Fetch the current tip floor once (boot seed / WS-disconnect fallback).
pub async fn fetch_tip_floor(url: &str) -> Result<TipLamports> {
    let txt = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .map_err(|e| TurbineError::Ingest(format!("tip_floor request: {e}")))?
        .text()
        .await
        .map_err(|e| TurbineError::Ingest(format!("tip_floor body: {e}")))?;
    parse_tip_payload(&txt)
        .ok_or_else(|| TurbineError::Ingest("tip_floor: empty/unparseable response".into()))
}

/// Fetch the 8 Jito tip accounts via JSON-RPC `getTipAccounts`.
pub async fn fetch_tip_accounts(json_rpc_base: &str) -> Result<Vec<Pubkey>> {
    let url = format!("{}/bundles", json_rpc_base.trim_end_matches('/'));
    let body = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "getTipAccounts", "params": []
    });
    let resp: serde_json::Value = reqwest::Client::new()
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| TurbineError::Ingest(format!("getTipAccounts request: {e}")))?
        .json()
        .await
        .map_err(|e| TurbineError::Ingest(format!("getTipAccounts decode: {e}")))?;

    let arr = resp["result"]
        .as_array()
        .ok_or_else(|| TurbineError::Ingest("getTipAccounts: missing result array".into()))?;
    let mut out = Vec::with_capacity(arr.len());
    for v in arr {
        if let Some(s) = v.as_str() {
            out.push(Pubkey::from_str(s).map_err(|e| TurbineError::Pubkey(e.to_string()))?);
        }
    }
    Ok(out)
}

/// One WS session: stream tip updates until the socket closes or errors.
async fn stream_once(url: &str, tx: &mpsc::Sender<TipLamports>) -> Result<()> {
    let (mut ws, _resp) = connect_async(url)
        .await
        .map_err(|e| TurbineError::Ingest(format!("tip_stream connect: {e}")))?;
    info!("tip stream connected");
    while let Some(message) = ws.next().await {
        let message = message.map_err(|e| TurbineError::Ingest(format!("tip_stream recv: {e}")))?;
        if let Message::Text(t) = message {
            if let Some(tips) = parse_tip_payload(t.as_str()) {
                if tx.send(tips).await.is_err() {
                    return Ok(()); // consumer gone
                }
            }
        }
    }
    Ok(())
}

/// Long-running task: stream tips, reconnecting (and re-seeding) on drops.
pub async fn run_tip_stream(url: String, tip_floor_url: String, tx: mpsc::Sender<TipLamports>) {
    loop {
        match stream_once(&url, &tx).await {
            Ok(()) => warn!("tip stream closed; reconnecting in 3s"),
            Err(e) => warn!("{e}; reconnecting in 3s"),
        }
        // Re-seed from REST so the EMA never goes stale during a WS outage.
        if let Ok(seed) = fetch_tip_floor(&tip_floor_url).await {
            let _ = tx.send(seed).await;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}
