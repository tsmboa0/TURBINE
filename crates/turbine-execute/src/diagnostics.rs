//! Cold-path Jito JSON-RPC bundle status — the primary outcome path when gRPC
//! `subscribe_bundle_results` is unavailable (keyless / no auth).
//!
//! Follows the Jito SDK example (`basic_bundle.rs`): poll `getInflightBundleStatuses`,
//! on `Landed` fetch `getBundleStatuses`, treat `Invalid` as inconclusive until final
//! status confirms, and only route terminal failures to the AI when Jito returns an
//! explicit signal (`Failed` inflight, or on-chain `err` in final status).

use std::sync::Arc;
use std::time::{Duration, Instant};

use jito_sdk_rust::JitoJsonRpcSDK;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tracing::info;

use turbine_core::ai::AiDecisionRecord;
use turbine_core::config::Config;
use turbine_core::types::{FailureClass, LifecycleState};
use turbine_state::jito_poll_audit::JitoPollRecord;
use turbine_state::lifecycle::BundleOutcome;
use turbine_state::HotState;

use crate::coordinator::FailureEvent;

/// Default budget for a single inflight JSON-RPC poll.
pub const INFLIGHT_POLL_BUDGET: Duration = Duration::from_millis(2_000);

/// Fast poll before the timeout sweeper emits `AuctionTimeout`.
pub const SWEEPER_PRECHECK_BUDGET: Duration = Duration::from_millis(2_000);

/// Poll interval for the detached post-submit watcher.
const WATCHER_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Parsed inflight status from Jito JSON-RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InflightStatus {
    Pending,
    Landed,
    Failed,
    Invalid,
    Unknown(String),
    PollError(String),
    PollTimeout,
}

impl InflightStatus {
    pub fn summary(&self) -> String {
        match self {
            Self::Pending => "Pending".into(),
            Self::Landed => "Landed".into(),
            Self::Failed => "Failed".into(),
            Self::Invalid => "Invalid".into(),
            Self::Unknown(s) => format!("unknown({s})"),
            Self::PollError(e) => format!("poll_error({e})"),
            Self::PollTimeout => "poll_timeout".into(),
        }
    }
}

/// Snapshot of Jito JSON-RPC responses for one bundle.
#[derive(Debug, Clone)]
pub struct JitoStatusReport {
    pub inflight_status: InflightStatus,
    /// First entry from `getInflightBundleStatuses`, when present.
    pub inflight_entry: Option<Value>,
    /// First entry from `getBundleStatuses`, when fetched.
    pub final_entry: Option<Value>,
}

impl JitoStatusReport {
    /// Human-readable summary for logs and AI `raw_reason` payloads.
    pub fn summary(&self) -> String {
        let inflight = self.inflight_status.summary();
        let inflight_detail = self
            .inflight_entry
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "none".into());
        let final_detail = self
            .final_entry
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "none".into());
        format!(
            "inflight={inflight}; inflight_entry={inflight_detail}; final_entry={final_detail}"
        )
    }
}

/// Result of interpreting a [`JitoStatusReport`].
#[derive(Debug, Clone)]
pub enum JitoTerminal {
    /// Bundle reached chain without error.
    Success(BundleOutcome, String),
    /// Jito or on-chain reported a terminal failure — carry raw text for the AI.
    Failure {
        raw_reason: String,
        logs: Vec<String>,
    },
    /// Keep polling — no definitive Jito signal yet.
    Inconclusive,
}

/// Outcome of a timeout-sweeper pre-check poll.
#[derive(Debug, Clone)]
pub enum SweeperPrecheck {
    /// No terminal JSON-RPC signal — emit watchdog timeout with Jito snapshot.
    EmitTimeout { jito_summary: String },
    /// Lifecycle updated (landed/processed/finalized) — skip timeout.
    Skip,
    /// Real Jito failure — route to AI with raw API text.
    RouteFailure(FailureEvent),
}

/// Parse `getInflightBundleStatuses` → the first entry's `status` string.
pub fn parse_inflight_status(response: &Value) -> Option<String> {
    response
        .get("result")
        .and_then(|r| r.get("value"))
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|entry| entry.get("status"))
        .and_then(|s| s.as_str())
        .map(String::from)
}

fn first_result_entry(response: &Value) -> Option<Value> {
    response
        .get("result")
        .and_then(|r| r.get("value"))
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .cloned()
}

fn entry_has_on_chain_err(entry: &Value) -> bool {
    entry.get("err").is_some_and(|e| {
        !(e.is_null() || e.get("Ok").and_then(|v| v.as_null()).is_some())
    })
}

fn on_chain_err_string(entry: &Value) -> Option<String> {
    entry.get("err").and_then(|e| {
        if e.is_null() || e.get("Ok").and_then(|v| v.as_null()).is_some() {
            None
        } else {
            Some(e.to_string())
        }
    })
}

/// Parse `getBundleStatuses` → confirmation + on-chain error hint (logging).
pub fn parse_final_bundle_status(response: &Value) -> Option<String> {
    let entry = first_result_entry(response)?;

    let confirmation = entry
        .get("confirmation_status")
        .and_then(|s| s.as_str())
        .unwrap_or("unknown");

    let err = on_chain_err_string(&entry);

    Some(match err {
        Some(e) => format!("confirmation={confirmation}; err={e}"),
        None => format!("confirmation={confirmation}"),
    })
}

/// Map a `getBundleStatuses` response to a lifecycle outcome.
pub fn parse_final_bundle_outcome(response: &Value) -> Option<(BundleOutcome, String)> {
    let entry = first_result_entry(response)?;

    let slot = entry.get("slot").and_then(|s| s.as_u64()).unwrap_or(0);
    let confirmation = entry
        .get("confirmation_status")
        .and_then(|s| s.as_str())
        .unwrap_or("unknown");

    if entry_has_on_chain_err(&entry) {
        let err = entry.get("err").map(|e| e.to_string()).unwrap_or_default();
        return Some((
            BundleOutcome::Rejected(FailureClass::Unknown),
            format!("confirmation={confirmation}; err={err}"),
        ));
    }

    let outcome = match confirmation {
        "finalized" => BundleOutcome::Finalized,
        _ => BundleOutcome::Processed { slot },
    };
    Some((outcome, format!("confirmation={confirmation}")))
}

fn status_from_str(s: &str) -> InflightStatus {
    match s {
        "Pending" => InflightStatus::Pending,
        "Landed" => InflightStatus::Landed,
        "Failed" => InflightStatus::Failed,
        "Invalid" => InflightStatus::Invalid,
        other => InflightStatus::Unknown(other.to_string()),
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

fn terminal_failure_from_outcome(outcome: &BundleOutcome) -> Option<FailureClass> {
    match outcome {
        BundleOutcome::Dropped(c) | BundleOutcome::Rejected(c) => Some(c.clone()),
        _ => None,
    }
}

fn interpretation_label(terminal: &JitoTerminal) -> &'static str {
    match terminal {
        JitoTerminal::Success(_, _) => "success",
        JitoTerminal::Failure { .. } => "failure",
        JitoTerminal::Inconclusive => "inconclusive",
    }
}

/// Append one poll observation to `jito_polls.jsonl` and emit structured logs.
pub fn record_jito_poll(
    state: &HotState,
    tracking_id: u64,
    bundle_id: &str,
    source: &str,
    report: &JitoStatusReport,
    terminal: &JitoTerminal,
) {
    let interpretation = interpretation_label(terminal);
    let record = JitoPollRecord {
        at_ms: AiDecisionRecord::now_ms(),
        tracking_id,
        bundle_id: bundle_id.to_string(),
        source: source.to_string(),
        inflight_status: report.inflight_status.summary(),
        interpretation: interpretation.to_string(),
        summary: report.summary(),
        inflight_entry: report.inflight_entry.clone(),
        final_entry: report.final_entry.clone(),
    };
    state.jito_poll.append(record);
    info!(
        target: "turbine::jito_poll",
        tracking_id,
        bundle_id = %bundle_id,
        source = %source,
        inflight_status = %report.inflight_status.summary(),
        interpretation = %interpretation,
        summary = %report.summary(),
        "jito bundle status poll",
    );
}

/// One-shot inflight poll (JSON-RPC).
pub async fn poll_inflight(sdk: &JitoJsonRpcSDK, bundle_id: &str, budget: Duration) -> InflightStatus {
    let inflight_fut = sdk.get_in_flight_bundle_statuses(vec![bundle_id.to_string()]);
    let inflight_resp = match tokio::time::timeout(budget, inflight_fut).await {
        Ok(Ok(resp)) => resp,
        Ok(Err(e)) => return InflightStatus::PollError(e.to_string()),
        Err(_) => return InflightStatus::PollTimeout,
    };

    match parse_inflight_status(&inflight_resp) {
        Some(s) => status_from_str(&s),
        None => InflightStatus::Pending,
    }
}

/// Fetch inflight + (when useful) final Jito JSON-RPC status for a bundle.
pub async fn fetch_jito_status_report(
    sdk: &JitoJsonRpcSDK,
    bundle_id: &str,
    inflight_budget: Duration,
) -> JitoStatusReport {
    let inflight_fut = sdk.get_in_flight_bundle_statuses(vec![bundle_id.to_string()]);
    let inflight_resp = match tokio::time::timeout(inflight_budget, inflight_fut).await {
        Ok(Ok(resp)) => resp,
        Ok(Err(e)) => {
            return JitoStatusReport {
                inflight_status: InflightStatus::PollError(e.to_string()),
                inflight_entry: None,
                final_entry: None,
            };
        }
        Err(_) => {
            return JitoStatusReport {
                inflight_status: InflightStatus::PollTimeout,
                inflight_entry: None,
                final_entry: None,
            };
        }
    };

    let inflight_entry = first_result_entry(&inflight_resp);
    let inflight_status = parse_inflight_status(&inflight_resp)
        .map(|s| status_from_str(&s))
        .unwrap_or(InflightStatus::Pending);

    let needs_final = matches!(
        inflight_status,
        InflightStatus::Landed | InflightStatus::Invalid
    );

    let final_entry = if needs_final {
        match sdk.get_bundle_statuses(vec![bundle_id.to_string()]).await {
            Ok(resp) => first_result_entry(&resp),
            Err(_) => None,
        }
    } else {
        None
    };

    JitoStatusReport {
        inflight_status,
        inflight_entry,
        final_entry,
    }
}

/// Interpret a Jito status report per the official SDK example semantics.
pub fn interpret_jito_report(report: &JitoStatusReport) -> JitoTerminal {
    // Explicit inflight auction failure from Jito.
    if matches!(report.inflight_status, InflightStatus::Failed) {
        return JitoTerminal::Failure {
            raw_reason: format!("jito getInflightBundleStatuses: {}", report.summary()),
            logs: report
                .inflight_entry
                .as_ref()
                .map(|v| vec![v.to_string()])
                .unwrap_or_default(),
        };
    }

    // Final status (from Landed or Invalid follow-up) is authoritative for on-chain err.
    if let Some(ref entry) = report.final_entry {
        if entry_has_on_chain_err(entry) {
            let err = on_chain_err_string(entry).unwrap_or_default();
            return JitoTerminal::Failure {
                raw_reason: format!("jito getBundleStatuses: err={err}; {}", report.summary()),
                logs: vec![entry.to_string()],
            };
        }
        if let Some((outcome, reason)) = parse_final_bundle_outcome(&json!({
            "result": { "value": [entry.clone()] }
        })) {
            return JitoTerminal::Success(outcome, reason);
        }
    }

    if matches!(report.inflight_status, InflightStatus::Landed) {
        return JitoTerminal::Success(
            BundleOutcome::Processed { slot: 0 },
            format!("inflight=Landed; {}", report.summary()),
        );
    }

    // Invalid / Pending / empty — inconclusive (example keeps polling on Invalid).
    JitoTerminal::Inconclusive
}

/// Apply a JSON-RPC-derived outcome to the lifecycle tracker and optionally route
/// terminal failures to the AI coordinator.
pub fn apply_json_rpc_outcome(
    state: &HotState,
    bundle_id: &str,
    tracking_id: u64,
    outcome: BundleOutcome,
    raw_reason: String,
    fail_tx: &Option<mpsc::Sender<FailureEvent>>,
) -> Option<FailureClass> {
    if bundle_already_on_chain(state, tracking_id)
        && terminal_failure_from_outcome(&outcome).is_some()
    {
        info!(
            target: "turbine::diagnostic",
            bundle_id = %bundle_id,
            tracking_id,
            %raw_reason,
            "ignoring JSON-RPC failure signal; bundle already on-chain",
        );
        return None;
    }

    if let Some((id, new_state, terminal_failure)) =
        state.lifecycle.on_bundle_result(bundle_id, outcome)
    {
        info!(
            target: "turbine::diagnostic",
            bundle_id = %bundle_id,
            tracking_id = id,
            state = ?new_state,
            %raw_reason,
            "bundle result applied (JSON-RPC)",
        );
        if let Some(class) = terminal_failure {
            if let Some(tx) = fail_tx {
                let ev = FailureEvent {
                    tracking_id: Some(tracking_id),
                    bundle_id: Some(bundle_id.to_string()),
                    raw_reason: raw_reason.clone(),
                    class_hint: class.clone(),
                    logs: vec![],
                };
                let _ = tx.try_send(ev);
            }
            return Some(class);
        }
    }
    None
}

/// Resolve a bundle via Jito JSON-RPC; route real failures or update lifecycle on success.
pub async fn resolve_and_apply_jito_status(
    state: &HotState,
    sdk: &JitoJsonRpcSDK,
    tracking_id: u64,
    bundle_id: &str,
    inflight_budget: Duration,
    fail_tx: &Option<mpsc::Sender<FailureEvent>>,
) -> Option<FailureEvent> {
    if bundle_already_on_chain(state, tracking_id) {
        return None;
    }

    let report = fetch_jito_status_report(sdk, bundle_id, inflight_budget).await;
    let terminal = interpret_jito_report(&report);
    record_jito_poll(state, tracking_id, bundle_id, "watcher", &report, &terminal);
    match terminal {
        JitoTerminal::Success(outcome, reason) => {
            apply_json_rpc_outcome(
                state,
                bundle_id,
                tracking_id,
                outcome,
                format!("jito json-rpc: {reason}"),
                fail_tx,
            );
            None
        }
        JitoTerminal::Failure { raw_reason, logs } => {
            state
                .lifecycle
                .on_failure(tracking_id, FailureClass::Unknown);
            let ev = FailureEvent {
                tracking_id: Some(tracking_id),
                bundle_id: Some(bundle_id.to_string()),
                raw_reason,
                class_hint: FailureClass::Unknown,
                logs,
            };
            if let Some(tx) = fail_tx {
                let _ = tx.try_send(ev.clone());
            }
            Some(ev)
        }
        JitoTerminal::Inconclusive => None,
    }
}

/// Pre-check before the timeout sweeper emits a watchdog timeout.
pub async fn sweeper_precheck(
    state: &HotState,
    sdk: &JitoJsonRpcSDK,
    tracking_id: u64,
    bundle_id: &str,
) -> SweeperPrecheck {
    if bundle_already_on_chain(state, tracking_id) {
        return SweeperPrecheck::Skip;
    }

    let report = fetch_jito_status_report(sdk, bundle_id, SWEEPER_PRECHECK_BUDGET).await;
    let terminal = interpret_jito_report(&report);
    record_jito_poll(state, tracking_id, bundle_id, "sweeper", &report, &terminal);
    match terminal {
        JitoTerminal::Success(outcome, reason) => {
            apply_json_rpc_outcome(
                state,
                bundle_id,
                tracking_id,
                outcome,
                format!("jito json-rpc: {reason}"),
                &None,
            );
            SweeperPrecheck::Skip
        }
        JitoTerminal::Failure { raw_reason, logs } => SweeperPrecheck::RouteFailure(FailureEvent {
            tracking_id: Some(tracking_id),
            bundle_id: Some(bundle_id.to_string()),
            raw_reason,
            class_hint: FailureClass::Unknown,
            logs,
        }),
        JitoTerminal::Inconclusive => SweeperPrecheck::EmitTimeout {
            jito_summary: report.summary(),
        },
    }
}

/// Poll Jito JSON-RPC until terminal, deadline, or the bundle leaves `Submitted`.
pub fn spawn_bundle_status_watcher(
    cfg: Arc<Config>,
    state: Arc<HotState>,
    tracking_id: u64,
    bundle_id: String,
    fail_tx: Option<mpsc::Sender<FailureEvent>>,
) {
    if cfg.execution.dry_run {
        return;
    }
    tokio::spawn(async move {
        let sdk = JitoJsonRpcSDK::new(&cfg.jito.json_rpc_url, cfg.jito.auth_uuid.clone());
        let deadline =
            Instant::now() + Duration::from_millis(cfg.ai.retry_timeout_ms.saturating_sub(500));
        let mut ticker = tokio::time::interval(WATCHER_POLL_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;
            if Instant::now() >= deadline {
                break;
            }
            let still_submitted = state
                .lifecycle
                .get(tracking_id)
                .is_some_and(|b| b.state == LifecycleState::Submitted && b.landed_slot.is_none());
            if !still_submitted {
                return;
            }

            if resolve_and_apply_jito_status(
                &state,
                &sdk,
                tracking_id,
                &bundle_id,
                INFLIGHT_POLL_BUDGET,
                &fail_tx,
            )
            .await
            .is_some()
            {
                return;
            }

            // Success path may have updated lifecycle without returning a failure event.
            if bundle_already_on_chain(state.as_ref(), tracking_id) {
                return;
            }
            if state.lifecycle.get(tracking_id).is_some_and(|b| {
                !matches!(b.state, LifecycleState::Submitted)
            }) {
                return;
            }
        }
    });
}

/// One-shot diagnostic string (logging).
pub async fn jito_timeout_diagnostic(
    sdk: &JitoJsonRpcSDK,
    bundle_id: &str,
    budget: Duration,
) -> String {
    fetch_jito_status_report(sdk, bundle_id, budget)
        .await
        .summary()
}

/// Detached cold-path task: log Jito status after watchdog timeout.
pub fn spawn_jito_timeout_diagnostic(
    state: Arc<HotState>,
    json_rpc_url: String,
    auth_uuid: Option<String>,
    tracking_id: u64,
    bundle_id: String,
) {
    tokio::spawn(async move {
        let sdk = JitoJsonRpcSDK::new(&json_rpc_url, auth_uuid);
        let report = fetch_jito_status_report(&sdk, &bundle_id, INFLIGHT_POLL_BUDGET).await;
        let terminal = interpret_jito_report(&report);
        record_jito_poll(
            state.as_ref(),
            tracking_id,
            &bundle_id,
            "diagnostic",
            &report,
            &terminal,
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_inflight_status() {
        let resp = json!({
            "result": {
                "value": [{ "status": "Failed", "bundle_id": "abc" }]
            }
        });
        assert_eq!(parse_inflight_status(&resp).as_deref(), Some("Failed"));
    }

    #[test]
    fn parses_final_confirmation() {
        let resp = json!({
            "result": {
                "value": [{
                    "confirmation_status": "finalized",
                    "err": { "Ok": null }
                }]
            }
        });
        assert_eq!(
            parse_final_bundle_status(&resp).as_deref(),
            Some("confirmation=finalized")
        );
    }

    #[test]
    fn parses_final_outcome_rejected_on_err() {
        let resp = json!({
            "result": {
                "value": [{
                    "slot": 123,
                    "confirmation_status": "processed",
                    "err": { "InstructionError": [0, "Custom"] }
                }]
            }
        });
        let (outcome, _) = parse_final_bundle_outcome(&resp).unwrap();
        assert!(matches!(outcome, BundleOutcome::Rejected(FailureClass::Unknown)));
    }

    #[test]
    fn inflight_failed_is_terminal_failure() {
        let report = JitoStatusReport {
            inflight_status: InflightStatus::Failed,
            inflight_entry: Some(json!({ "status": "Failed", "bundle_id": "x" })),
            final_entry: None,
        };
        assert!(matches!(interpret_jito_report(&report), JitoTerminal::Failure { .. }));
    }

    #[test]
    fn inflight_invalid_without_final_err_is_inconclusive() {
        let report = JitoStatusReport {
            inflight_status: InflightStatus::Invalid,
            inflight_entry: Some(json!({ "status": "Invalid" })),
            final_entry: None,
        };
        assert!(matches!(interpret_jito_report(&report), JitoTerminal::Inconclusive));
    }

    #[test]
    fn final_on_chain_err_is_terminal_failure() {
        let entry = json!({
            "confirmation_status": "processed",
            "err": { "BlockhashNotFound": null }
        });
        let report = JitoStatusReport {
            inflight_status: InflightStatus::Invalid,
            inflight_entry: Some(json!({ "status": "Invalid" })),
            final_entry: Some(entry),
        };
        assert!(matches!(interpret_jito_report(&report), JitoTerminal::Failure { .. }));
    }

    #[test]
    fn empty_inflight_response_is_pending() {
        let resp = json!({ "result": { "value": [] } });
        assert!(parse_inflight_status(&resp).is_none());
    }
}
