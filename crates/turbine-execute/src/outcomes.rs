//! Bundle result subscription (plan §7.4, §5.5).
//!
//! One long-lived `subscribe_bundle_results` stream maps each Jito result onto the
//! [`LifecycleTracker`], feeding lifecycle deltas and the failure router. Live
//! submit uses JSON-RPC; this stream is a secondary path when the gRPC channel is
//! connected (same searcher session may still receive results for some bundles).

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{info, warn};

use turbine_core::types::{FailureClass, LifecycleState};
use turbine_state::lifecycle::BundleOutcome;
use turbine_state::HotState;

use crate::coordinator::FailureEvent;
use crate::pb::bundle::{bundle_result::Result as ResultKind, rejected::Reason, DroppedReason};
use crate::pb::searcher::SubscribeBundleResultsRequest;
use crate::searcher::SearcherClient;

fn result_label(kind: &ResultKind) -> &'static str {
    match kind {
        ResultKind::Accepted(_) => "Accepted",
        ResultKind::Processed(_) => "Processed",
        ResultKind::Finalized(_) => "Finalized",
        ResultKind::Dropped(_) => "Dropped",
        ResultKind::Rejected(_) => "Rejected",
    }
}

/// Translate a proto `BundleResult.result` into our lifecycle outcome.
fn to_outcome(kind: ResultKind) -> BundleOutcome {
    match kind {
        ResultKind::Accepted(a) => BundleOutcome::Accepted { slot: a.slot },
        ResultKind::Processed(p) => BundleOutcome::Processed { slot: p.slot },
        ResultKind::Finalized(_) => BundleOutcome::Finalized,
        ResultKind::Dropped(d) => {
            let class = match DroppedReason::try_from(d.reason).unwrap_or(DroppedReason::NotFinalized) {
                DroppedReason::BlockhashExpired => FailureClass::BlockhashExpired,
                DroppedReason::PartiallyProcessed => FailureClass::BundleDropped,
                DroppedReason::NotFinalized => FailureClass::Transient,
            };
            BundleOutcome::Dropped(class)
        }
        ResultKind::Rejected(r) => {
            let class = match r.reason {
                Some(Reason::StateAuctionBidRejected(_))
                | Some(Reason::WinningBatchBidRejected(_)) => FailureClass::TipTooLow,
                Some(Reason::SimulationFailure(_)) => FailureClass::SimulationError,
                Some(Reason::InternalError(_)) => FailureClass::Transient,
                Some(Reason::DroppedBundle(_)) => FailureClass::BundleDropped,
                None => FailureClass::Unknown,
            };
            BundleOutcome::Rejected(class)
        }
    }
}

fn route_terminal_failure(
    failures: &Option<mpsc::Sender<FailureEvent>>,
    bundle_id: &str,
    tracking_id: u64,
    class: FailureClass,
    raw_reason: String,
) {
    if let Some(tx) = failures {
        let ev = FailureEvent {
            tracking_id: Some(tracking_id),
            bundle_id: Some(bundle_id.to_string()),
            raw_reason,
            class_hint: class,
            logs: vec![],
        };
        let _ = tx.try_send(ev);
    }
}

fn bundle_already_on_chain(state: &HotState, tracking_id: u64) -> bool {
    state.lifecycle.get(tracking_id).is_some_and(|b| {
        b.landed_slot.is_some()
            || matches!(
                b.state,
                LifecycleState::Processed | LifecycleState::Confirmed | LifecycleState::Finalized
            )
    })
}

/// Subscribe to bundle results and apply them to the lifecycle tracker until the
/// stream ends or errors. Terminal failures are forwarded to the AI coordinator.
async fn run_once(
    mut client: SearcherClient,
    state: &HotState,
    failures: &Option<mpsc::Sender<FailureEvent>>,
) -> turbine_core::error::Result<()> {
    let mut stream = client
        .subscribe_bundle_results(SubscribeBundleResultsRequest {})
        .await
        .map_err(|e| turbine_core::error::TurbineError::Execute(format!("subscribe: {e}")))?
        .into_inner();

    info!("jito bundle result stream connected (gRPC)");

    while let Some(msg) = stream
        .message()
        .await
        .map_err(|e| turbine_core::error::TurbineError::Execute(format!("result stream: {e}")))?
    {
        let Some(kind) = msg.result else { continue };
        let label = result_label(&kind);
        let outcome = to_outcome(kind);
        if let Some((id, new_state, terminal_failure)) =
            state.lifecycle.on_bundle_result(&msg.bundle_id, outcome)
        {
            info!(
                bundle_id = %msg.bundle_id,
                tracking_id = id,
                result = label,
                state = ?new_state,
                "bundle result applied (gRPC stream)",
            );
            if let Some(class) = terminal_failure {
                if bundle_already_on_chain(state, id) {
                    info!(
                        bundle_id = %msg.bundle_id,
                        tracking_id = id,
                        result = label,
                        "ignoring gRPC terminal failure; bundle already on-chain",
                    );
                } else {
                    route_terminal_failure(
                        failures,
                        &msg.bundle_id,
                        id,
                        class,
                        format!("jito grpc stream: {label}"),
                    );
                }
            }
        } else {
            info!(
                bundle_id = %msg.bundle_id,
                result = label,
                "bundle result buffered (bundle id not indexed yet)",
            );
        }
    }
    Ok(())
}

/// Long-running result subscriber with reconnect/backoff. When a `failures` sender
/// is supplied, terminal Dropped/Rejected results are routed to the AI coordinator.
pub async fn run_bundle_results(
    client: SearcherClient,
    state: Arc<HotState>,
    failures: Option<mpsc::Sender<FailureEvent>>,
) {
    let mut backoff_ms = 500u64;
    loop {
        match run_once(client.clone(), &state, &failures).await {
            Ok(()) => {
                warn!("bundle result stream ended; reconnecting");
                backoff_ms = 500;
            }
            Err(e) => {
                warn!("bundle result stream error: {e}; retrying in {backoff_ms}ms");
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(10_000);
            }
        }
    }
}
