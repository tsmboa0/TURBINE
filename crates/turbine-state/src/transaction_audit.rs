//! Append-only JSONL transaction audit log (cold path).
//!
//! A background sweeper scans lifecycle records for terminal bundles and appends
//! one JSON line per bundle exactly once. Used for offline reporting / graphing.

use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use tracing::{debug, warn};

use turbine_core::ai::AiDecisionRecord;
use turbine_core::transaction_record::{tip_delta_lamports, tip_floor_lamports, LifecycleDeltaRecord, TransactionRecord};
use turbine_core::types::LifecycleState;

use crate::lifecycle::BundleLifecycle;
use crate::HotState;

const DEFAULT_PATH: &str = "transactions.jsonl";
const SWEEP_INTERVAL: Duration = Duration::from_millis(400);
const MAX_WRITTEN_IDS: usize = 16_384;

/// Append-only JSONL writer with in-memory dedup by tracking id.
pub struct TransactionAuditLog {
    path: PathBuf,
    written: Mutex<HashSet<u64>>,
}

impl TransactionAuditLog {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            written: Mutex::new(HashSet::new()),
        }
    }

    pub fn default_path() -> PathBuf {
        PathBuf::from(DEFAULT_PATH)
    }

    /// Scan all tracked bundles; append any newly terminal rows.
    pub fn sweep(&self, state: &HotState) {
        for (id, bundle) in state.lifecycle.snapshot_all() {
            if !ready_for_audit(&bundle.state, &bundle) {
                continue;
            }
            if !self.mark_written(id) {
                continue;
            }
            match self.append_record(state, id, &bundle) {
                Ok(()) => debug!(tracking_id = id, path = %self.path.display(), "transaction audit row appended"),
                Err(e) => {
                    warn!(tracking_id = id, path = %self.path.display(), "transaction audit append failed: {e}");
                    // Allow retry on next sweep.
                    if let Ok(mut w) = self.written.lock() {
                        w.remove(&id);
                    }
                }
            }
        }
    }

    fn mark_written(&self, id: u64) -> bool {
        let Ok(mut w) = self.written.lock() else {
            return false;
        };
        if w.contains(&id) {
            return false;
        }
        if w.len() >= MAX_WRITTEN_IDS {
            w.clear();
        }
        w.insert(id);
        true
    }

    fn append_record(&self, state: &HotState, id: u64, bundle: &BundleLifecycle) -> std::io::Result<()> {
        let ai_decisions: Vec<AiDecisionRecord> = state
            .ai_audit
            .snapshot()
            .into_iter()
            .filter(|r| r.tracking_id == Some(id))
            .collect();
        let d = bundle.deltas();
        let tip_floor = tip_floor_lamports(bundle.percentile, &bundle.submit_tip_emas);
        let record = TransactionRecord {
            recorded_at_ms: AiDecisionRecord::now_ms(),
            tracking_id: id,
            label: bundle.label.clone(),
            bundle_id: bundle.bundle_id.clone(),
            signature: bundle.sigs.first().map(|s| bs58::encode(s).into_string()),
            signatures: bundle.sigs.iter().map(|s| bs58::encode(s).into_string()).collect(),
            state: bundle.state,
            attempt: bundle.attempt,
            tip_floor_lamports: tip_floor,
            tip_lamports: bundle.tip_lamports,
            tip_delta_lamports: tip_delta_lamports(bundle.tip_lamports, tip_floor),
            percentile: bundle.percentile.map(|p| p.label().to_string()),
            landed_slot: bundle.landed_slot,
            last_error: bundle.ai_classification.clone().or_else(|| {
                bundle.last_error.as_ref().map(|e| format!("{e:?}"))
            }),
            deltas: LifecycleDeltaRecord {
                submit_to_processed_ms: d.submit_to_processed_ms,
                processed_to_confirmed_ms: d.processed_to_confirmed_ms,
                confirmed_to_finalized_ms: d.confirmed_to_finalized_ms,
            },
            submit_tip_emas: bundle.submit_tip_emas,
            terminal_tip_emas: state.tips(),
            submit_slot: bundle.submit_slot,
            next_jito_leader_slot: bundle.next_jito_leader_slot,
            gate_dist: bundle.gate_dist,
            blockhash_age_ms: bundle.blockhash_age_ms,
            congestion: bundle.congestion,
            max_z: bundle.max_z,
            bump_pct: bundle.bump_pct,
            ai_decisions,
        };
        let line = serde_json::to_string(&record).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let mut file = std::fs::OpenOptions::new().create(true).append(true).open(&self.path)?;
        writeln!(file, "{line}")?;
        file.flush()?;
        Ok(())
    }
}

fn is_terminal(state: &LifecycleState) -> bool {
    matches!(state, LifecycleState::Finalized | LifecycleState::Failed)
}

/// Terminal bundles are audit-ready once AI has classified failures.
fn ready_for_audit(state: &LifecycleState, bundle: &BundleLifecycle) -> bool {
    if !is_terminal(state) {
        return false;
    }
    if matches!(state, LifecycleState::Failed) && bundle.ai_classification.is_none() {
        return false;
    }
    true
}

/// Background task: periodically flush terminal bundles to JSONL.
pub async fn run_sweeper(state: std::sync::Arc<HotState>, path: PathBuf) {
    let audit = TransactionAuditLog::new(path.clone());
    info_path(&path);
    loop {
        tokio::time::sleep(SWEEP_INTERVAL).await;
        audit.sweep(&state);
    }
}

fn info_path(path: &Path) {
    tracing::info!(path = %path.display(), "transaction audit JSONL sweeper running");
}
