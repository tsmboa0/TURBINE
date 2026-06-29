//! `turbine-state` — Component 3, the Hot State Storage façade (plan §6).
//!
//! A thin, well-typed wrapper over lock-free-friendly primitives:
//! - `AtomicU64` / `AtomicBool` for the current slot and Geyser health,
//! - `DashMap` (sharded) for per-account contention and bundle lifecycle,
//! - `ArcSwap` for whole-snapshot values (tips, tip accounts, leader, blockhash).
//!
//! Every execution-loop read is a lock-free `ArcSwap::load` or a sharded
//! `DashMap::get` — never a global lock. No business logic (fee selection,
//! gating, retry) lives here; that belongs to `turbine-execute` / `turbine-ai`.
//!
//! Note: per plan §6 the contention map is a `DashMap<Pubkey, ContentionCell>`.
//! We expose it through the small [`ContentionTracker`] façade (watched-set +
//! per-slot windowing live alongside the map) and keep the EMA math in
//! `turbine-core`. Equivalent guarantees (sharded locks), cleaner call sites.

pub mod contention;
pub mod jito_poll_audit;
pub mod lifecycle;
pub mod transaction_audit;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use solana_pubkey::Pubkey;
use tokio::sync::watch;

use turbine_core::ai::AiDecisionRecord;
use turbine_core::blockhash::CachedBlockhash;
use turbine_core::config::Config;
use turbine_core::leader::{JitoSchedule, LeaderView};
use turbine_core::tips::TipSnapshot;

pub use contention::ContentionTracker;
pub use jito_poll_audit::{JitoPollAuditLog, JitoPollRecord};
pub use lifecycle::{BundleLifecycle, LifecycleDeltas, LifecycleTracker, SigBytes, SubmitContext};

/// Capacity of the AI reasoning ring buffer (older records are evicted).
const AI_AUDIT_CAP: usize = 512;

/// Bounded, in-memory ring buffer of AI decision records. The web UI reads a
/// [`AiAuditLog::snapshot`] on connect and then follows live decisions. Cheap
/// `Mutex` (write rate is one entry per failure — cold path, never the hot loop).
pub struct AiAuditLog {
    inner: Mutex<VecDeque<AiDecisionRecord>>,
    cap: usize,
    seq: AtomicU64,
}

impl Default for AiAuditLog {
    fn default() -> Self {
        Self::new(AI_AUDIT_CAP)
    }
}

impl AiAuditLog {
    pub fn new(cap: usize) -> Self {
        Self { inner: Mutex::new(VecDeque::with_capacity(cap)), cap, seq: AtomicU64::new(1) }
    }

    /// Assign a monotonic `seq`, append (evicting the oldest if full), and return
    /// the assigned `seq`.
    pub fn record(&self, mut rec: AiDecisionRecord) -> u64 {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        rec.seq = seq;
        let mut g = self.inner.lock().unwrap();
        if g.len() >= self.cap {
            g.pop_front();
        }
        g.push_back(rec);
        seq
    }

    /// All retained records, oldest first.
    pub fn snapshot(&self) -> Vec<AiDecisionRecord> {
        self.inner.lock().unwrap().iter().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

/// The single shared hot-state container, held behind an `Arc` by every runtime.
pub struct HotState {
    current_slot: AtomicU64,
    /// Wall-clock interval (ms) between the two most recent chain-head advances,
    /// measured at ingestion (not sampled) so the dashboard Δ is accurate.
    slot_interval_ms: AtomicU64,
    /// Latest-slot `watch` so the execution gate is event-driven, not a busy loop.
    slot_tx: watch::Sender<u64>,
    geyser_healthy: AtomicBool,
    /// Global kill switch — when set, the gate refuses and the governor aborts.
    killed: AtomicBool,
    /// Per-account write-lock contention (sharded `DashMap` of EMA cells).
    pub contention: ContentionTracker,
    /// Outbound bundle lifecycle tracking (sharded `DashMap`).
    pub lifecycle: LifecycleTracker,
    /// Persisted AI reasoning + fix trail (cold path; read by the web UI).
    pub ai_audit: AiAuditLog,
    /// Append-only log of every Jito JSON-RPC bundle status poll.
    pub jito_poll: JitoPollAuditLog,
    tips: ArcSwap<TipSnapshot>,
    tip_accounts: ArcSwap<Vec<Pubkey>>,
    leader: ArcSwap<LeaderView>,
    /// Epoch-cached Jito leader slots — drives the local, real-time leader view.
    jito_schedule: ArcSwap<JitoSchedule>,
    blockhash: ArcSwap<Option<CachedBlockhash>>,
}

impl HotState {
    /// Build the hot state from config (seeds the watched contention set).
    pub fn new(cfg: &Config) -> Self {
        let watched = cfg.targets.watched_accounts.iter().copied().collect();
        let contention = ContentionTracker::new(
            watched,
            cfg.strategy.ema_half_life_ms,
            cfg.strategy.ema_slow_half_life_ms,
            cfg.strategy.quiet_z,
            cfg.strategy.hot_z,
        );
        let (slot_tx, _) = watch::channel(0);
        Self {
            current_slot: AtomicU64::new(0),
            slot_interval_ms: AtomicU64::new(0),
            slot_tx,
            geyser_healthy: AtomicBool::new(false),
            killed: AtomicBool::new(false),
            contention,
            lifecycle: LifecycleTracker::new(),
            ai_audit: AiAuditLog::default(),
            jito_poll: JitoPollAuditLog::default(),
            tips: ArcSwap::from_pointee(TipSnapshot::default()),
            tip_accounts: ArcSwap::from_pointee(Vec::new()),
            leader: ArcSwap::from_pointee(LeaderView::default()),
            jito_schedule: ArcSwap::from_pointee(JitoSchedule::default()),
            blockhash: ArcSwap::from_pointee(None),
        }
    }

    // --- current slot ---
    #[inline]
    pub fn slot(&self) -> u64 {
        self.current_slot.load(Ordering::Relaxed)
    }
    /// Advance the chain head **monotonically**. We subscribe to every slot status
    /// (processed/confirmed/finalized), and confirmed/finalized updates carry
    /// *older* slot numbers — so an unconditional store would yank the head
    /// backward and corrupt the gate's lookahead distance. `fetch_max` keeps the
    /// head at the highest slot ever seen (the processed leading edge).
    ///
    /// Returns `true` iff this call advanced the head (a genuinely new slot).
    #[inline]
    pub fn set_slot(&self, slot: u64) -> bool {
        let prev = self.current_slot.fetch_max(slot, Ordering::Relaxed);
        if slot > prev {
            // Notify gate watchers only on real advances (ignores send error when
            // there are no receivers).
            self.slot_tx.send_replace(slot);
            true
        } else {
            false
        }
    }
    /// Most recent measured chain-head advance interval (ms); 0 until two slots seen.
    #[inline]
    pub fn slot_interval_ms(&self) -> u64 {
        self.slot_interval_ms.load(Ordering::Relaxed)
    }
    #[inline]
    pub fn set_slot_interval_ms(&self, ms: u64) {
        self.slot_interval_ms.store(ms, Ordering::Relaxed);
    }

    // --- Jito leader schedule (epoch-cached) ---
    #[inline]
    pub fn jito_schedule(&self) -> arc_swap::Guard<Arc<JitoSchedule>> {
        self.jito_schedule.load()
    }
    #[inline]
    pub fn set_jito_schedule(&self, sched: JitoSchedule) {
        self.jito_schedule.store(Arc::new(sched));
    }

    /// Subscribe to slot updates (event-driven lookahead gate).
    pub fn subscribe_slot(&self) -> watch::Receiver<u64> {
        self.slot_tx.subscribe()
    }

    // --- geyser health ---
    #[inline]
    pub fn geyser_healthy(&self) -> bool {
        self.geyser_healthy.load(Ordering::Relaxed)
    }
    #[inline]
    pub fn set_geyser_healthy(&self, healthy: bool) {
        self.geyser_healthy.store(healthy, Ordering::Relaxed);
    }

    // --- global kill switch (halts all submission immediately, plan §8.4) ---
    #[inline]
    pub fn submission_killed(&self) -> bool {
        self.killed.load(Ordering::Relaxed)
    }
    /// Engage the kill switch: the gate refuses and the retry governor aborts.
    #[inline]
    pub fn kill_submission(&self) {
        self.killed.store(true, Ordering::Relaxed);
    }
    /// Re-arm submission after a kill (e.g. operator clears the circuit breaker).
    #[inline]
    pub fn rearm_submission(&self) {
        self.killed.store(false, Ordering::Relaxed);
    }

    // --- tips (smoothed percentiles) ---
    #[inline]
    pub fn tips(&self) -> TipSnapshot {
        **self.tips.load()
    }
    #[inline]
    pub fn set_tips(&self, tips: TipSnapshot) {
        self.tips.store(Arc::new(tips));
    }

    // --- Jito tip accounts (the 8) ---
    #[inline]
    pub fn tip_accounts(&self) -> Arc<Vec<Pubkey>> {
        self.tip_accounts.load_full()
    }
    #[inline]
    pub fn set_tip_accounts(&self, accounts: Vec<Pubkey>) {
        self.tip_accounts.store(Arc::new(accounts));
    }

    // --- leader view ---
    #[inline]
    pub fn leader(&self) -> Arc<LeaderView> {
        self.leader.load_full()
    }
    #[inline]
    pub fn set_leader(&self, leader: LeaderView) {
        self.leader.store(Arc::new(leader));
    }

    // --- warm cached blockhash ---
    #[inline]
    pub fn blockhash(&self) -> Arc<Option<CachedBlockhash>> {
        self.blockhash.load_full()
    }
    #[inline]
    pub fn set_blockhash(&self, blockhash: CachedBlockhash) {
        self.blockhash.store(Arc::new(Some(blockhash)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turbine_core::blockhash::CachedBlockhash;
    use std::time::Instant;

    fn cfg() -> Config {
        // Minimal config with no watched accounts is fine for facade tests.
        Config::from_toml_str(SAMPLE).expect("parse sample config")
    }

    const SAMPLE: &str = r#"
[geyser]
endpoint = "https://example:443"
[rpc]
http_url = "https://rpc.example"
[jito]
block_engine_url = "https://be.example"
[wallet]
keypair_path = "/tmp/k.json"
[targets]
programs = []
watched_accounts = []
"#;

    #[test]
    fn atomics_roundtrip() {
        let s = HotState::new(&cfg());
        assert_eq!(s.slot(), 0);
        assert!(!s.geyser_healthy());
        s.set_slot(123);
        s.set_geyser_healthy(true);
        assert_eq!(s.slot(), 123);
        assert!(s.geyser_healthy());
    }

    #[test]
    fn arcswap_snapshots_roundtrip() {
        let s = HotState::new(&cfg());
        assert_eq!(s.tips(), TipSnapshot::default());
        s.set_tips(TipSnapshot { p25: 1, p50: 2, p75: 3, p95: 4, p99: 5 });
        assert_eq!(s.tips().p95, 4);

        assert!(s.tip_accounts().is_empty());
        s.set_tip_accounts(vec![Pubkey::new_from_array([9u8; 32])]);
        assert_eq!(s.tip_accounts().len(), 1);

        assert!(s.blockhash().is_none());
        s.set_blockhash(CachedBlockhash {
            blockhash: "abc".into(),
            last_valid_block_height: 10,
            slot: 5,
            fetched_at: Instant::now(),
        });
        assert_eq!(s.blockhash().as_ref().as_ref().unwrap().blockhash, "abc");
    }
}
