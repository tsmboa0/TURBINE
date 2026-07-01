//! `turbine-ingest` — Component 1, the Data Ingestion Engine (plan §4).
//!
//! Spawns the ingestion tasks and hands back the receiving ends of the channels
//! so the processing layer can consume:
//! - `geyser_rx`: slots, optional target/own transactions (bounded).
//! - `deshred_rx`: optional pre-execution target txs when `--deshred` is active.
//! - `tips_rx`  : smoothed Jito tip percentiles in lamports.

pub mod blockhash;
pub mod boot;
pub mod client;
pub mod deshred;
pub mod feed_control;
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

pub use boot::DeshredBootStatus;
pub use feed_control::ContentionFeedControl;
pub use messages::{GeyserMessage, TimedDeshred, TimedGeyser};
pub use tips::TipLamports;

/// Receiving ends of the ingestion channels, consumed by the processing layer.
pub struct IngestChannels {
    pub geyser_rx: mpsc::Receiver<TimedGeyser>,
    /// Present when deshred successfully probed at boot; contention-only feed.
    pub deshred_rx: Option<mpsc::Receiver<TimedDeshred>>,
    pub tips_rx: mpsc::Receiver<TipLamports>,
    pub blockhash_rx: watch::Receiver<Option<CachedBlockhash>>,
    pub tip_accounts_seed: Vec<Pubkey>,
    pub feed: ContentionFeedControl,
}

/// Handles to the spawned background tasks (kept alive by the caller).
pub struct IngestHandles {
    pub geyser: JoinHandle<()>,
    pub deshred: Option<JoinHandle<()>>,
    pub tip_stream: JoinHandle<()>,
    pub blockhash: JoinHandle<()>,
}

/// Boot the ingestion engine.
///
/// When `deshred_requested` is true, probes `SubscribeDeshred` once. On success,
/// target transactions move to the deshred stream; on failure, logs a Triton hint
/// and keeps the standard Geyser target filter (default behavior).
pub async fn spawn(cfg: Arc<Config>, deshred_requested: bool) -> (IngestChannels, IngestHandles, DeshredBootStatus) {
    let feed = ContentionFeedControl::new(deshred_requested);

    let deshred_boot = if deshred_requested {
        if cfg.targets.watched_accounts.is_empty() {
            let error =
                "deshred requested but targets.watched_accounts is empty".to_string();
            warn!(%error, "deshred unavailable — using standard Geyser target stream for contention");
            DeshredBootStatus::Fallback { error }
        } else {
            match deshred::probe(&cfg).await {
                Ok(()) => {
                    feed.activate_deshred();
                    info!("deshred contention feed active (pre-execution transactions via SubscribeDeshred)");
                    DeshredBootStatus::Active
                }
                Err(e) => {
                    let error = e.to_string();
                    warn!(
                        error = %error,
                        "deshred probe failed — SubscribeDeshred requires a Triton extension Geyser endpoint; \
                         continuing with standard Geyser target stream for contention"
                    );
                    DeshredBootStatus::Fallback { error }
                }
            }
        }
    } else {
        DeshredBootStatus::NotRequested
    };

    let (geyser_tx, geyser_rx) = mpsc::channel(8192);
    let (tips_tx, tips_rx) = mpsc::channel(256);

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

    let (blockhash_tx, blockhash_rx) = watch::channel(None);

    let geyser = tokio::spawn(geyser::run_geyser(cfg.clone(), feed.clone(), geyser_tx));

    let deshred = if feed.is_deshred_active() {
        let (deshred_tx, deshred_rx) = mpsc::channel(8192);
        let handle = tokio::spawn(deshred::run_deshred(cfg.clone(), feed.clone(), deshred_tx));
        (
            Some(handle),
            Some(deshred_rx),
        )
    } else {
        (None, None)
    };

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
        IngestChannels {
            geyser_rx,
            deshred_rx: deshred.1,
            tips_rx,
            blockhash_rx,
            tip_accounts_seed,
            feed,
        },
        IngestHandles {
            geyser,
            deshred: deshred.0,
            tip_stream,
            blockhash,
        },
        deshred_boot,
    )
}
