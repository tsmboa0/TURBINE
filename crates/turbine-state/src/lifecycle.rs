//! Transaction lifecycle tracker (plan §5.5).
//!
//! Tracks our outbound bundles from `Submitted` → `Processed` → `Confirmed` →
//! `Finalized` (or `Failed`), keyed by their transaction signatures, and computes
//! the inter-state latency deltas (ms) that the web UI visualizes.
//!
//! Transition drivers (full set wired in Phases 4–5):
//! - submit                          → `Submitted`  (this crate exposes `on_submit`)
//! - own-wallet Geyser tx (our sig)  → `Processed` + `landed_slot`
//! - slot status reaches the slot    → `Confirmed` / `Finalized`
//! - gRPC bundle result / timeout    → `Failed`

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use dashmap::DashMap;

use turbine_core::tips::TipSnapshot;
use turbine_core::types::{Congestion, FailureClass, LifecycleState, Percentile};

/// 64-byte transaction signature.
pub type SigBytes = [u8; 64];

/// Per-bundle lifecycle record.
#[derive(Debug, Clone)]
pub struct BundleLifecycle {
    pub bundle_id: Option<String>,
    pub sigs: Vec<SigBytes>,
    pub landed_slot: Option<u64>,
    pub state: LifecycleState,
    /// Tip paid (lamports) and the percentile tier it was selected from — set at
    /// submit so the web UI can show the fee decision per bundle (plan §9.4 B).
    pub tip_lamports: Option<u64>,
    pub percentile: Option<Percentile>,
    pub t_built: Instant,
    pub t_submitted: Option<Instant>,
    pub t_processed: Option<Instant>,
    pub t_confirmed: Option<Instant>,
    pub t_finalized: Option<Instant>,
    pub last_error: Option<FailureClass>,
    pub attempt: u8,
    /// Scenario / intent label captured at submit (for audit export).
    pub label: Option<String>,
    /// Submit-time context for JSONL reporting.
    pub submit_tip_emas: TipSnapshot,
    pub submit_slot: u64,
    pub next_jito_leader_slot: Option<u64>,
    pub gate_dist: Option<u64>,
    pub blockhash_age_ms: Option<u64>,
    pub congestion: Congestion,
    pub max_z: f64,
    pub bump_pct: f64,
    /// Set when the auction watchdog logs an inconclusive timeout — prevents
    /// re-firing every sweeper tick while Geyser may still confirm the bundle.
    pub auction_watchdog_logged: bool,
    /// AI analyst classification after failure handling (replaces raw timeout labels in UI).
    pub ai_classification: Option<String>,
}

/// Gate + fee context captured when a bundle is submitted.
#[derive(Debug, Clone)]
pub struct SubmitContext {
    pub label: Option<String>,
    pub submit_tip_emas: TipSnapshot,
    pub submit_slot: u64,
    pub next_jito_leader_slot: Option<u64>,
    pub gate_dist: Option<u64>,
    pub blockhash_age_ms: Option<u64>,
    pub congestion: Congestion,
    pub max_z: f64,
    pub bump_pct: f64,
}

impl Default for SubmitContext {
    fn default() -> Self {
        Self {
            label: None,
            submit_tip_emas: TipSnapshot::default(),
            submit_slot: 0,
            next_jito_leader_slot: None,
            gate_dist: None,
            blockhash_age_ms: None,
            congestion: Congestion::Quiet,
            max_z: 0.0,
            bump_pct: 0.0,
        }
    }
}

impl BundleLifecycle {
    fn new(sigs: Vec<SigBytes>, attempt: u8) -> Self {
        Self {
            bundle_id: None,
            sigs,
            landed_slot: None,
            state: LifecycleState::Built,
            tip_lamports: None,
            percentile: None,
            t_built: Instant::now(),
            t_submitted: None,
            t_processed: None,
            t_confirmed: None,
            t_finalized: None,
            last_error: None,
            attempt,
            label: None,
            submit_tip_emas: TipSnapshot::default(),
            submit_slot: 0,
            next_jito_leader_slot: None,
            gate_dist: None,
            blockhash_age_ms: None,
            congestion: Congestion::Quiet,
            max_z: 0.0,
            bump_pct: 0.0,
            auction_watchdog_logged: false,
            ai_classification: None,
        }
    }
}

/// Inter-state latencies in milliseconds (whichever transitions have occurred).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct LifecycleDeltas {
    pub submit_to_processed_ms: Option<f64>,
    pub processed_to_confirmed_ms: Option<f64>,
    pub confirmed_to_finalized_ms: Option<f64>,
}

fn ms_between(a: Option<Instant>, b: Option<Instant>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) if b >= a => Some(b.saturating_duration_since(a).as_secs_f64() * 1000.0),
        _ => None,
    }
}

impl BundleLifecycle {
    pub fn deltas(&self) -> LifecycleDeltas {
        LifecycleDeltas {
            submit_to_processed_ms: ms_between(self.t_submitted, self.t_processed),
            processed_to_confirmed_ms: ms_between(self.t_processed, self.t_confirmed),
            confirmed_to_finalized_ms: ms_between(self.t_confirmed, self.t_finalized),
        }
    }
}

/// Terminal/intermediate outcome reported by the Jito searcher result stream.
#[derive(Debug, Clone)]
pub enum BundleOutcome {
    /// Accepted by the block engine and forwarded to a validator (still in flight).
    Accepted { slot: u64 },
    /// Reached processed commitment on-chain.
    Processed { slot: u64 },
    /// Reached finalized commitment.
    Finalized,
    /// Forwarded but never landed (terminal failure).
    Dropped(FailureClass),
    /// Rejected by the block engine (terminal failure).
    Rejected(FailureClass),
}

/// Outcome of attaching a Jito bundle id, including any result that arrived on
/// the gRPC stream before the index existed (race with submit return).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleIdAttach {
    pub tracking_id: u64,
    pub state: LifecycleState,
    /// Set when a buffered stream result was a terminal Rejected/Dropped.
    pub terminal_failure: Option<FailureClass>,
}

/// Concurrent lifecycle tracker. Bundles are keyed by an internal id, with a
/// secondary index from each signature to its bundle id, plus a Jito bundle-id
/// index so the searcher result stream can resolve records.
pub struct LifecycleTracker {
    bundles: DashMap<u64, BundleLifecycle>,
    sig_index: DashMap<SigBytes, u64>,
    bundle_index: DashMap<String, u64>,
    /// Results received before `set_bundle_id` indexed the uuid (stream/submit race).
    pending_results: DashMap<String, BundleOutcome>,
    next_id: AtomicU64,
}

/// Cap buffered stream results so a flood of foreign ids cannot grow memory.
const MAX_PENDING_RESULTS: usize = 256;

impl Default for LifecycleTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl LifecycleTracker {
    pub fn new() -> Self {
        Self {
            bundles: DashMap::new(),
            sig_index: DashMap::new(),
            bundle_index: DashMap::new(),
            pending_results: DashMap::new(),
            next_id: AtomicU64::new(1),
        }
    }

    fn apply_outcome(record: &mut BundleLifecycle, outcome: BundleOutcome) -> Option<FailureClass> {
        match outcome {
            BundleOutcome::Accepted { .. } => None,
            BundleOutcome::Processed { slot } => {
                if record.t_processed.is_none() {
                    record.t_processed = Some(Instant::now());
                    record.landed_slot = Some(slot);
                }
                if record.state == LifecycleState::Failed {
                    record.last_error = None;
                }
                if record.state == LifecycleState::Failed
                    || (record.state as u8) < (LifecycleState::Processed as u8)
                {
                    record.state = LifecycleState::Processed;
                }
                None
            }
            BundleOutcome::Finalized => {
                if record.t_finalized.is_none() {
                    record.t_finalized = Some(Instant::now());
                }
                record.state = LifecycleState::Finalized;
                None
            }
            BundleOutcome::Dropped(class) | BundleOutcome::Rejected(class) => {
                if (record.state as u8) >= (LifecycleState::Processed as u8) {
                    return None;
                }
                record.state = LifecycleState::Failed;
                record.last_error = Some(class.clone());
                Some(class)
            }
        }
    }

    /// Register a freshly submitted bundle and return its tracking id. `tip_lamports`
    /// / `percentile` capture the fee decision so the web UI can show it per bundle.
    pub fn on_submit(
        &self,
        sigs: Vec<SigBytes>,
        attempt: u8,
        tip_lamports: u64,
        percentile: Percentile,
        ctx: SubmitContext,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut record = BundleLifecycle::new(sigs.clone(), attempt);
        record.state = LifecycleState::Submitted;
        record.t_submitted = Some(Instant::now());
        record.tip_lamports = Some(tip_lamports);
        record.percentile = Some(percentile);
        record.label = ctx.label;
        record.submit_tip_emas = ctx.submit_tip_emas;
        record.submit_slot = ctx.submit_slot;
        record.next_jito_leader_slot = ctx.next_jito_leader_slot;
        record.gate_dist = ctx.gate_dist;
        record.blockhash_age_ms = ctx.blockhash_age_ms;
        record.congestion = ctx.congestion;
        record.max_z = ctx.max_z;
        record.bump_pct = ctx.bump_pct;
        for sig in sigs {
            self.sig_index.insert(sig, id);
        }
        self.bundles.insert(id, record);
        id
    }

    /// Attach the Jito-assigned bundle id once `send_bundle` returns (Phase 4),
    /// index it for the result stream, and drain any buffered stream result.
    pub fn set_bundle_id(&self, id: u64, bundle_id: String) -> Option<BundleIdAttach> {
        if let Some(mut b) = self.bundles.get_mut(&id) {
            b.bundle_id = Some(bundle_id.clone());
        }
        self.bundle_index.insert(bundle_id.clone(), id);
        if let Some((_, outcome)) = self.pending_results.remove(&bundle_id) {
            let mut b = self.bundles.get_mut(&id)?;
            let terminal_failure = Self::apply_outcome(&mut b, outcome);
            return Some(BundleIdAttach {
                tracking_id: id,
                state: b.state,
                terminal_failure,
            });
        }
        None
    }

    /// Apply a searcher bundle-result to the matching record (Phase 5).
    /// Returns `(id, new_state, terminal_failure)` when applied to a tracked bundle.
    pub fn on_bundle_result(
        &self,
        bundle_id: &str,
        outcome: BundleOutcome,
    ) -> Option<(u64, LifecycleState, Option<FailureClass>)> {
        let Some(id) = self.bundle_index.get(bundle_id).map(|e| *e.value()) else {
            if self.pending_results.len() < MAX_PENDING_RESULTS {
                self.pending_results.insert(bundle_id.to_string(), outcome);
            }
            return None;
        };
        let mut b = self.bundles.get_mut(&id)?;
        let terminal_failure = Self::apply_outcome(&mut b, outcome);
        Some((id, b.state, terminal_failure))
    }

    /// One of our own transactions was observed on-chain (own-wallet Geyser tx).
    /// Returns the deltas if a tracked bundle matched.
    pub fn on_self_tx(&self, sig: &SigBytes, slot: u64) -> Option<LifecycleDeltas> {
        let id = *self.sig_index.get(sig)?.value();
        let mut b = self.bundles.get_mut(&id)?;
        if b.t_processed.is_none() {
            b.t_processed = Some(Instant::now());
            b.landed_slot = Some(slot);
        }
        if b.state == LifecycleState::Failed {
            b.last_error = None;
        }
        if b.state == LifecycleState::Failed
            || (b.state as u8) < (LifecycleState::Processed as u8)
        {
            b.state = LifecycleState::Processed;
        }
        Some(b.deltas())
    }

    /// A slot reached a commitment level. Advances any bundle that landed in that
    /// slot to Confirmed/Finalized and returns (id, deltas) for those updated.
    pub fn on_slot_commitment(&self, slot: u64, finalized: bool) -> Vec<(u64, LifecycleDeltas)> {
        let mut updated = Vec::new();
        for mut entry in self.bundles.iter_mut() {
            if entry.landed_slot != Some(slot) {
                continue;
            }
            let now = Instant::now();
            if finalized {
                if entry.t_finalized.is_none() {
                    entry.t_finalized = Some(now);
                    entry.state = LifecycleState::Finalized;
                    updated.push((*entry.key(), entry.deltas()));
                }
            } else if entry.t_confirmed.is_none() {
                entry.t_confirmed = Some(now);
                if (entry.state as u8) < (LifecycleState::Confirmed as u8) {
                    entry.state = LifecycleState::Confirmed;
                }
                updated.push((*entry.key(), entry.deltas()));
            }
        }
        updated
    }

    /// Mark a bundle failed with a classification (Phase 5/6).
    pub fn on_failure(&self, id: u64, class: FailureClass) {
        if let Some(mut b) = self.bundles.get_mut(&id) {
            b.state = LifecycleState::Failed;
            b.last_error = Some(class);
        }
    }

    /// Record that the auction watchdog already logged an inconclusive timeout for
    /// this bundle (do not route AI or re-poll on every sweeper tick).
    pub fn mark_auction_watchdog_logged(&self, id: u64) {
        if let Some(mut b) = self.bundles.get_mut(&id) {
            b.auction_watchdog_logged = true;
        }
    }

    /// Store the AI analyst's failure classification for web UI / audit display.
    pub fn set_ai_classification(&self, id: u64, classification: String) {
        if let Some(mut b) = self.bundles.get_mut(&id) {
            b.ai_classification = Some(classification);
        }
    }

    /// Snapshot of a bundle by id.
    pub fn get(&self, id: u64) -> Option<BundleLifecycle> {
        self.bundles.get(&id).map(|b| b.clone())
    }

    /// Snapshot of every tracked bundle as `(id, record)`, for the web history
    /// surface (cold path; clones out of the sharded map).
    pub fn snapshot_all(&self) -> Vec<(u64, BundleLifecycle)> {
        self.bundles.iter().map(|e| (*e.key(), e.clone())).collect()
    }

    /// Lifecycle state + landed slot for a signature, if tracked. Used by the AI
    /// idempotency guard to confirm a bundle is *not* already on-chain before retry.
    pub fn sig_state(&self, sig: &SigBytes) -> Option<(LifecycleState, Option<u64>)> {
        let id = *self.sig_index.get(sig)?.value();
        self.bundles.get(&id).map(|b| (b.state, b.landed_slot))
    }

    /// Internal tracking id for a Jito bundle id (resolves a result/failure to its
    /// in-flight record so the retry coordinator can look up the original intent).
    pub fn id_for_bundle(&self, bundle_id: &str) -> Option<u64> {
        self.bundle_index.get(bundle_id).map(|v| *v.value())
    }

    /// Tracking ids of bundles still in `Submitted` whose submit timestamp is older
    /// than `max_age_ms` and that never landed — i.e. silent submit timeouts that the
    /// AI coordinator should treat as failures. Returns `(id, bundle_id)`.
    pub fn timed_out(&self, max_age_ms: u128) -> Vec<(u64, Option<String>)> {
        let now = Instant::now();
        self.bundles
            .iter()
            .filter_map(|b| {
                if b.state != LifecycleState::Submitted
                    || b.landed_slot.is_some()
                    || b.auction_watchdog_logged
                {
                    return None;
                }
                let ts = b.t_submitted?;
                (now.saturating_duration_since(ts).as_millis() >= max_age_ms)
                    .then(|| (*b.key(), b.bundle_id.clone()))
            })
            .collect()
    }

    /// Number of bundles currently tracked.
    pub fn len(&self) -> usize {
        self.bundles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bundles.is_empty()
    }

    /// Count of bundles still awaiting a terminal state.
    pub fn in_flight(&self) -> usize {
        self.bundles
            .iter()
            .filter(|b| !matches!(b.state, LifecycleState::Finalized | LifecycleState::Failed))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(b: u8) -> SigBytes {
        [b; 64]
    }

    #[test]
    fn full_happy_path_records_deltas() {
        let lt = LifecycleTracker::new();
        let id = lt.on_submit(vec![sig(1)], 0, 1_000, Percentile::P50, SubmitContext::default());
        assert_eq!(lt.get(id).unwrap().state, LifecycleState::Submitted);

        // small sleeps so deltas are measurable and ordered
        std::thread::sleep(std::time::Duration::from_millis(2));
        let d = lt.on_self_tx(&sig(1), 100).unwrap();
        assert!(d.submit_to_processed_ms.unwrap() > 0.0);
        assert_eq!(lt.get(id).unwrap().landed_slot, Some(100));
        assert_eq!(lt.get(id).unwrap().state, LifecycleState::Processed);

        std::thread::sleep(std::time::Duration::from_millis(2));
        let conf = lt.on_slot_commitment(100, false);
        assert_eq!(conf.len(), 1);
        assert!(conf[0].1.processed_to_confirmed_ms.unwrap() > 0.0);
        assert_eq!(lt.get(id).unwrap().state, LifecycleState::Confirmed);

        std::thread::sleep(std::time::Duration::from_millis(2));
        let fin = lt.on_slot_commitment(100, true);
        assert_eq!(fin.len(), 1);
        assert!(fin[0].1.confirmed_to_finalized_ms.unwrap() > 0.0);
        assert_eq!(lt.get(id).unwrap().state, LifecycleState::Finalized);
        assert_eq!(lt.in_flight(), 0);
    }

    #[test]
    fn unknown_sig_is_ignored() {
        let lt = LifecycleTracker::new();
        lt.on_submit(vec![sig(1)], 0, 1_000, Percentile::P50, SubmitContext::default());
        assert!(lt.on_self_tx(&sig(99), 5).is_none());
    }

    #[test]
    fn failure_marks_terminal() {
        let lt = LifecycleTracker::new();
        let id = lt.on_submit(vec![sig(1)], 0, 1_000, Percentile::P50, SubmitContext::default());
        lt.on_failure(id, FailureClass::BlockhashExpired);
        assert_eq!(lt.get(id).unwrap().state, LifecycleState::Failed);
        assert_eq!(lt.in_flight(), 0);
    }

    #[test]
    fn self_tx_recovers_from_false_failure() {
        let lt = LifecycleTracker::new();
        let id = lt.on_submit(vec![sig(1)], 0, 1_000, Percentile::P50, SubmitContext::default());
        lt.on_failure(id, FailureClass::BundleDropped);
        assert_eq!(lt.get(id).unwrap().state, LifecycleState::Failed);

        lt.on_self_tx(&sig(1), 200);
        let b = lt.get(id).unwrap();
        assert_eq!(b.state, LifecycleState::Processed);
        assert_eq!(b.landed_slot, Some(200));
        assert!(b.last_error.is_none());
    }

    #[test]
    fn early_stream_result_buffered_until_bundle_id_set() {
        let lt = LifecycleTracker::new();
        let id = lt.on_submit(vec![sig(1)], 0, 1_000, Percentile::P50, SubmitContext::default());
        assert!(lt
            .on_bundle_result("uuid-1", BundleOutcome::Rejected(FailureClass::TipTooLow))
            .is_none());
        assert_eq!(lt.get(id).unwrap().state, LifecycleState::Submitted);

        let attach = lt.set_bundle_id(id, "uuid-1".into()).unwrap();
        assert_eq!(attach.tracking_id, id);
        assert_eq!(attach.state, LifecycleState::Failed);
        assert_eq!(attach.terminal_failure, Some(FailureClass::TipTooLow));
    }
}
