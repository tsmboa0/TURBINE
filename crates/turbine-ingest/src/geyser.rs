//! Yellowstone/Geyser gRPC ingestion (plan §4.1).
//!
//! Opens one combined subscription (slots + optional target transactions + own-wallet
//! transactions) at the configured commitment, then forwards decoded updates into the
//! processing channel. Target transactions are omitted when deshred owns contention.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeRequestPing,
};

use turbine_core::config::Config;
use turbine_core::error::{Result, TurbineError};

use crate::client;
use crate::feed_control::ContentionFeedControl;
use crate::messages::{GeyserMessage, TimedGeyser};

/// Filter bucket name for transactions touching our target programs/accounts.
pub const FILTER_TARGETS: &str = "targets";
/// Filter bucket name for transactions touching our own wallet.
pub const FILTER_SELF: &str = "self";

/// Build the combined subscription request from config and live contention feed mode.
fn build_request(cfg: &Config, include_targets: bool) -> SubscribeRequest {
    let commitment = match cfg.geyser.commitment.as_str() {
        "confirmed" => CommitmentLevel::Confirmed,
        "finalized" => CommitmentLevel::Finalized,
        _ => CommitmentLevel::Processed,
    };

    let mut slots = HashMap::new();
    slots.insert(
        "slots".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: Some(false),
            interslot_updates: Some(false),
        },
    );

    let mut transactions = HashMap::new();
    if include_targets && !cfg.targets.watched_accounts.is_empty() {
        transactions.insert(
            FILTER_TARGETS.to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: cfg
                    .targets
                    .watched_accounts
                    .iter()
                    .map(|p| p.to_string())
                    .collect(),
                account_exclude: vec![],
                account_required: vec![],
                token_accounts: None,
            },
        );
    }
    if let Some(pk) = &cfg.wallet.pubkey {
        transactions.insert(
            FILTER_SELF.to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: None,
                signature: None,
                account_include: vec![pk.to_string()],
                account_exclude: vec![],
                account_required: vec![],
                token_accounts: None,
            },
        );
    }

    SubscribeRequest {
        slots,
        accounts: HashMap::new(),
        transactions,
        transactions_status: HashMap::new(),
        entry: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta: HashMap::new(),
        commitment: Some(commitment as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    }
}

/// Connect, subscribe, and pump updates until the stream ends or reconnect is requested.
async fn connect_and_stream(
    cfg: &Config,
    feed: &ContentionFeedControl,
    tx: &mpsc::Sender<TimedGeyser>,
) -> Result<()> {
    let include_targets = !feed.is_deshred_active();
    let mut client = client::connect(cfg).await?;
    let request = build_request(cfg, include_targets);
    let (mut sink, mut stream) = client
        .subscribe_with_request(Some(request))
        .await
        .map_err(|e| TurbineError::Ingest(format!("geyser subscribe: {e}")))?;
    info!(
        include_targets,
        "geyser stream opened"
    );

    let reconnect = feed.geyser_reconnect();

    loop {
        tokio::select! {
            message = stream.next() => {
                let Some(message) = message else {
                    return Ok(());
                };
                let update = match message {
                    Ok(u) => u,
                    Err(status) => {
                        return Err(TurbineError::Ingest(format!("geyser stream status: {status}")))
                    }
                };
                let recv = Instant::now();
                let filters = update.filters;
                match update.update_oneof {
                    Some(UpdateOneof::Slot(s)) => {
                        if tx
                            .send(TimedGeyser { recv, msg: GeyserMessage::Slot(s) })
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    Some(UpdateOneof::Transaction(t)) => {
                        if tx
                            .send(TimedGeyser {
                                recv,
                                msg: GeyserMessage::Transaction { filters, update: t },
                            })
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    Some(UpdateOneof::BlockMeta(b)) => {
                        if tx
                            .send(TimedGeyser { recv, msg: GeyserMessage::BlockMeta(b) })
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        sink.send(SubscribeRequest {
                            ping: Some(SubscribeRequestPing { id: 1 }),
                            ..Default::default()
                        })
                        .await
                        .map_err(|e| TurbineError::Ingest(format!("geyser ping reply: {e:?}")))?;
                    }
                    _ => {}
                }
            }
            _ = reconnect.notified() => {
                info!("geyser reconnecting to pick up target transaction filter after deshred fallback");
                return Ok(());
            }
        }
    }
}

/// Long-running ingestion task: (re)connect forever with a fixed backoff.
pub async fn run_geyser(cfg: Arc<Config>, feed: ContentionFeedControl, tx: mpsc::Sender<TimedGeyser>) {
    loop {
        match connect_and_stream(&cfg, &feed, &tx).await {
            Ok(()) => warn!("geyser stream ended; reconnecting in 2s"),
            Err(e) => error!("{e}; reconnecting in 2s"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}
