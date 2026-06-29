//! Yellowstone/Geyser gRPC ingestion (plan §4.1).
//!
//! Opens one combined subscription (slots + target transactions + own-wallet
//! transactions + block meta) at the configured commitment, then forwards
//! decoded updates into the processing channel. The underlying `GeyserStream`
//! auto-reconnects on transient drops; an outer loop here handles connect-time
//! failures with a fixed backoff and answers server pings to stay alive.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use tokio::sync::mpsc;
use tonic::transport::ClientTlsConfig;
use tracing::{error, info, warn};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeRequestPing,
};

use turbine_core::config::Config;
use turbine_core::error::{Result, TurbineError};

use crate::messages::{GeyserMessage, TimedGeyser};

/// Filter bucket name for transactions touching our target programs/accounts.
pub const FILTER_TARGETS: &str = "targets";
/// Filter bucket name for transactions touching our own wallet.
pub const FILTER_SELF: &str = "self";

/// Build the combined subscription request from config.
fn build_request(cfg: &Config) -> SubscribeRequest {
    let commitment = match cfg.geyser.commitment.as_str() {
        "confirmed" => CommitmentLevel::Confirmed,
        "finalized" => CommitmentLevel::Finalized,
        _ => CommitmentLevel::Processed,
    };

    let mut slots = HashMap::new();
    slots.insert(
        "slots".to_string(),
        SubscribeRequestFilterSlots {
            // We want every status transition (processed→confirmed→finalized)
            // to drive lifecycle tracking, so do not filter by commitment.
            filter_by_commitment: Some(false),
            interslot_updates: Some(false),
        },
    );

    let mut transactions = HashMap::new();
    // Contention is measured only on the WATCHED ACCOUNTS, so subscribe to exactly
    // the transactions that touch them — not the entire program firehose. Watching a
    // hot program (e.g. Pump AMM) network-wide floods the stream with transactions we
    // discard, which is the classic cause of server-side backpressure. Filtering by
    // the specific accounts delivers only the txs that actually contend with us.
    // (Only added when accounts are configured — an empty `account_include` would
    // match the entire firehose.)
    if !cfg.targets.watched_accounts.is_empty() {
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
    // Track our own wallet's transactions when its pubkey is configured.
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
        // No block_meta subscription: we never consumed it, and it's avoidable volume.
        blocks_meta: HashMap::new(),
        commitment: Some(commitment as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    }
}

/// Connect, subscribe, and pump updates until the stream ends or errors.
async fn connect_and_stream(cfg: &Config, tx: &mpsc::Sender<TimedGeyser>) -> Result<()> {
    let tls = ClientTlsConfig::new().with_native_roots();
    let mut client = GeyserGrpcClient::build_from_shared(cfg.geyser.endpoint.clone())
        .map_err(|e| TurbineError::Ingest(format!("geyser builder: {e}")))?
        .x_token(cfg.geyser.x_token.clone())
        .map_err(|e| TurbineError::Ingest(format!("geyser x_token: {e}")))?
        .tls_config(tls)
        .map_err(|e| TurbineError::Ingest(format!("geyser tls: {e}")))?
        .max_decoding_message_size(cfg.geyser.max_decoding_message_size_bytes)
        .tcp_nodelay(true)
        .http2_adaptive_window(true)
        .keep_alive_while_idle(true)
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await
        .map_err(|e| TurbineError::Ingest(format!("geyser connect: {e}")))?;

    let request = build_request(cfg);
    let (mut sink, mut stream) = client
        .subscribe_with_request(Some(request))
        .await
        .map_err(|e| TurbineError::Ingest(format!("geyser subscribe: {e}")))?;
    info!("geyser stream opened");

    while let Some(message) = stream.next().await {
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
                    return Ok(()); // consumer gone → unwind cleanly
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
                // Keep load balancers / the stream alive by echoing a ping.
                use futures::SinkExt;
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

    Ok(())
}

/// Long-running ingestion task: (re)connect forever with a fixed backoff.
pub async fn run_geyser(cfg: Arc<Config>, tx: mpsc::Sender<TimedGeyser>) {
    loop {
        match connect_and_stream(&cfg, &tx).await {
            Ok(()) => warn!("geyser stream ended; reconnecting in 2s"),
            Err(e) => error!("{e}; reconnecting in 2s"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
