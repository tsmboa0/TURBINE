//! `turbine-ingest` — Component 1, the Data Ingestion Engine (plan §4).
//!
//! Spawns the ingestion tasks and hands back the receiving ends of the channels
//! so the processing layer (and, for now, the CLI) can consume:
//! - `geyser_rx`: slots, target/own transactions, block meta (bounded, never-drop).
//! - `tips_rx`  : smoothed Jito tip percentiles in lamports.
//!
//! The leader poller is intentionally NOT here: the only consumer of "next Jito
//! leader" is the execution gate (Phase 4), and a non-Jito-filtered leader poll
//! carries no signal. It is therefore co-located with the Jito searcher client
//! in Phase 4 (see IMPLEMENTATION_PLAN.md §4.2).

pub mod blockhash;
pub mod geyser;
pub mod messages;
pub mod tips;

use std::sync::Arc;

use solana_pubkey::Pubkey;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use turbine_core::blockhash::CachedBlockhash;
use turbine_core::config::Config;

pub use messages::{GeyserMessage, TimedGeyser};
pub use tips::TipLamports;

/// Receiving ends of the ingestion channels, consumed by the processing layer.
pub struct IngestChannels {
    pub geyser_rx: mpsc::Receiver<TimedGeyser>,
    pub tips_rx: mpsc::Receiver<TipLamports>,
    /// Latest warm blockhash (latest-value-only `watch` channel).
    pub blockhash_rx: watch::Receiver<Option<CachedBlockhash>>,
    /// The Jito tip accounts fetched at boot (rarely change; seeded into HotState).
    pub tip_accounts_seed: Vec<Pubkey>,
}

/// Handles to the spawned background tasks (kept alive by the caller).
pub struct IngestHandles {
    pub geyser: JoinHandle<()>,
    pub tip_stream: JoinHandle<()>,
    pub blockhash: JoinHandle<()>,
}

/// Boot the ingestion engine. Performs a one-shot REST seed of the tip floor and
/// a tip-accounts fetch, then spawns the long-running Geyser and tip-stream tasks.
pub async fn spawn(cfg: Arc<Config>) -> (IngestChannels, IngestHandles) {
    let (geyser_tx, geyser_rx) = mpsc::channel(8192);
    let (tips_tx, tips_rx) = mpsc::channel(256);

    // Seed tip percentiles from REST so EMAs start warm (plan §4.2).
    match tips::fetch_tip_floor(&cfg.jito.tip_floor_url).await {
        Ok(seed) => {
            info!(
                p25 = seed.p25,
                p50 = seed.p50,
                p75 = seed.p75,
                p95 = seed.p95,
                p99 = seed.p99,
                ema50 = seed.ema50,
                "seeded tip floor (lamports)"
            );
            let _ = tips_tx.send(seed).await;
        }
        Err(e) => warn!("tip floor seed failed: {e}"),
    }

    // Fetch the 8 Jito tip accounts (rotated per-submit later, in Phase 4).
    let tip_accounts_seed = match tips::fetch_tip_accounts(&cfg.jito.json_rpc_url).await {
        Ok(accounts) => {
            info!(count = accounts.len(), "fetched Jito tip accounts");
            accounts
        }
        Err(e) => {
            warn!("tip accounts fetch failed: {e}");
            Vec::new()
        }
    };

    // Warm blockhash refresher → watch channel (latest-value only).
    let (blockhash_tx, blockhash_rx) = watch::channel(None);

    let geyser = tokio::spawn(geyser::run_geyser(cfg.clone(), geyser_tx));
    let tip_stream = tokio::spawn(tips::run_tip_stream(
        cfg.jito.tip_stream_url.clone(),
        cfg.jito.tip_floor_url.clone(),
        tips_tx,
    ));
    let blockhash = tokio::spawn(blockhash::run_blockhash_refresher(
        cfg.rpc.http_url.clone(),
        cfg.rpc.blockhash_refresh_ms,
        blockhash_tx,
    ));

    (
        IngestChannels { geyser_rx, tips_rx, blockhash_rx, tip_accounts_seed },
        IngestHandles { geyser, tip_stream, blockhash },
    )
}
