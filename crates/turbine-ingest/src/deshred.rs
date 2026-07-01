//! Yellowstone `SubscribeDeshred` ingestion — pre-execution transactions for contention only.
//!
//! Slots, lifecycle, and self-wallet detection stay on the main Geyser `Subscribe` stream.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tonic::Code;
use tracing::{error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClientError;
use yellowstone_grpc_proto::geyser::{
    subscribe_update_deshred::UpdateOneof, SubscribeDeshredRequest,
    SubscribeRequestFilterDeshredTransactions, SubscribeRequestPing,
};

use turbine_core::config::Config;
use turbine_core::error::{Result, TurbineError};

use crate::client;
use crate::feed_control::ContentionFeedControl;
use crate::geyser::FILTER_TARGETS;
use crate::messages::TimedDeshred;

/// Build the deshred subscription (targets / watched accounts only).
pub fn build_deshred_request(cfg: &Config) -> SubscribeDeshredRequest {
    let mut deshred_transactions = HashMap::new();
    if !cfg.targets.watched_accounts.is_empty() {
        deshred_transactions.insert(
            FILTER_TARGETS.to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: Some(false),
                account_include: cfg
                    .targets
                    .watched_accounts
                    .iter()
                    .map(|p| p.to_string())
                    .collect(),
                account_exclude: vec![],
                account_required: vec![],
            },
        );
    }
    SubscribeDeshredRequest {
        deshred_transactions,
        ping: None,
        slots: HashMap::new(),
    }
}

/// Returns `Ok(())` when the provider accepts `SubscribeDeshred`.
pub async fn probe(cfg: &Config) -> Result<()> {
    probe_inner(cfg).await
}

async fn probe_inner(cfg: &Config) -> Result<()> {
    if cfg.targets.watched_accounts.is_empty() {
        return Err(TurbineError::Ingest(
            "deshred requested but targets.watched_accounts is empty".into(),
        ));
    }
    let mut client = client::connect(cfg).await?;
    let request = build_deshred_request(cfg);
    client
        .subscribe_deshred_with_request(Some(request))
        .await
        .map_err(map_deshred_subscribe_error)?;
    Ok(())
}

fn map_deshred_subscribe_error(err: GeyserGrpcClientError) -> TurbineError {
    match &err {
        GeyserGrpcClientError::TonicStatus(status) if status.code() == Code::Unimplemented => {
            TurbineError::Ingest(format!(
                "SubscribeDeshred is UNIMPLEMENTED on this Geyser server (Triton extension required): {status}"
            ))
        }
        _ => TurbineError::Ingest(format!("SubscribeDeshred subscribe failed: {err}")),
    }
}

async fn connect_and_stream(
    cfg: &Config,
    feed: &ContentionFeedControl,
    tx: &mpsc::Sender<TimedDeshred>,
) -> Result<()> {
    let mut client = client::connect(cfg).await?;
    let request = build_deshred_request(cfg);
    let (mut sink, mut stream) = client
        .subscribe_deshred_with_request(Some(request))
        .await
        .map_err(map_deshred_subscribe_error)?;
    info!("deshred stream opened (contention feed)");

    let reconnect = feed.geyser_reconnect();

    loop {
        tokio::select! {
            message = stream.next() => {
                let Some(message) = message else {
                    return Ok(());
                };
                let update = message.map_err(|status| {
                    TurbineError::Ingest(format!("deshred stream status: {status}"))
                })?;
                let recv = Instant::now();
                match update.update_oneof {
                    Some(UpdateOneof::DeshredTransaction(t)) => {
                        if tx.send(TimedDeshred { recv, update: t }).await.is_err() {
                            return Ok(());
                        }
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        sink.send(SubscribeDeshredRequest {
                            ping: Some(SubscribeRequestPing { id: 1 }),
                            ..Default::default()
                        })
                        .await
                        .map_err(|e| TurbineError::Ingest(format!("deshred ping reply: {e:?}")))?;
                    }
                    Some(UpdateOneof::Pong(_)) | Some(UpdateOneof::Slot(_)) => {}
                    None => {}
                }
            }
            _ = reconnect.notified() => {
                // Fallback disabled deshred; exit inner loop cleanly.
                return Ok(());
            }
        }
    }
}

/// Long-running deshred reader. On failure, falls back to Geyser targets automatically.
pub async fn run_deshred(cfg: Arc<Config>, feed: ContentionFeedControl, tx: mpsc::Sender<TimedDeshred>) {
    loop {
        if !feed.is_deshred_active() {
            return;
        }
        match connect_and_stream(&cfg, &feed, &tx).await {
            Ok(()) => {
                if feed.is_deshred_active() {
                    warn!("deshred stream ended; reconnecting in 2s");
                }
            }
            Err(e) => {
                error!("{e}");
                feed.fallback_to_geyser_targets(&e.to_string());
                return;
            }
        }
        if !feed.is_deshred_active() {
            return;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
