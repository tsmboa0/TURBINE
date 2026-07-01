//! Per-account contention tracker (plan §5.2–§5.3).
//!
//! Accumulates write-lock hits per **micro-window** (one slot) for each *watched*
//! account, then on each slot boundary folds the window's hit count into that
//! account's time-decayed EMAs. Reads (`snapshot`/`congestion`) are taken by the
//! execution fee matrix; writes come from the processing loop.

use std::collections::HashSet;
use std::time::Instant;

use dashmap::DashMap;
use solana_pubkey::Pubkey;

use turbine_core::contention::{ContentionCell, ContentionSnapshot};
use turbine_core::types::Congestion;

/// Tracks contention only for the configured set of watched (writable) accounts.
pub struct ContentionTracker {
    watched: HashSet<Pubkey>,
    cells: DashMap<Pubkey, ContentionCell>,
    /// Per-slot accumulation, flushed into the EMAs on each slot boundary.
    window: DashMap<Pubkey, u64>,
    fast_half_life_secs: f64,
    slow_half_life_secs: f64,
    quiet_z: f64,
    hot_z: f64,
}

impl ContentionTracker {
    pub fn new(
        watched: HashSet<Pubkey>,
        fast_half_life_ms: u64,
        slow_half_life_ms: u64,
        quiet_z: f64,
        hot_z: f64,
    ) -> Self {
        let cells = DashMap::with_capacity(watched.len().max(1));
        let window = DashMap::with_capacity(watched.len().max(1));
        Self {
            watched,
            cells,
            window,
            fast_half_life_secs: fast_half_life_ms as f64 / 1000.0,
            slow_half_life_secs: slow_half_life_ms as f64 / 1000.0,
            quiet_z,
            hot_z,
        }
    }

    /// Number of accounts being tracked.
    pub fn watched_len(&self) -> usize {
        self.watched.len()
    }

    /// The set of watched accounts.
    pub fn watched(&self) -> &HashSet<Pubkey> {
        &self.watched
    }

    /// Record the writable set of one observed target transaction. Only accounts
    /// in the watched set increment their current-slot window counter.
    pub fn record_writable(&self, writable: &[Pubkey]) {
        if self.watched.is_empty() {
            return;
        }
        for pk in writable {
            if self.watched.contains(pk) {
                *self.window.entry(*pk).or_insert(0) += 1;
            }
        }
    }

    /// Flush the current slot's window into the EMAs. Called once per new slot so
    /// every watched account decays even in slots where it saw zero hits.
    pub fn flush_slot(&self, now: Instant) {
        for pk in self.watched.iter() {
            let hits = self
                .window
                .get(pk)
                .map(|v| *v.value())
                .unwrap_or(0);
            self.window.insert(*pk, 0);
            let mut cell = self.cells.entry(*pk).or_insert_with(ContentionCell::new);
            cell.observe(
                hits as f64,
                now,
                self.fast_half_life_secs,
                self.slow_half_life_secs,
            );
        }
    }

    /// Current contention snapshot for an account (if it has been observed).
    pub fn snapshot(&self, pk: &Pubkey) -> Option<ContentionSnapshot> {
        self.cells.get(pk).map(|c| c.snapshot())
    }

    /// Congestion tier for a single account (Idle if never observed).
    pub fn congestion(&self, pk: &Pubkey) -> Congestion {
        self.snapshot(pk)
            .map(|s| s.congestion(self.quiet_z, self.hot_z))
            .unwrap_or(Congestion::Idle)
    }

    /// Congestion across a bundle's write-locked accounts = the **max** (the
    /// bottleneck account dominates the fee tier — plan §7.1).
    pub fn max_congestion(&self, accounts: &[Pubkey]) -> Congestion {
        accounts
            .iter()
            .map(|a| self.congestion(a))
            .max()
            .unwrap_or(Congestion::Idle)
    }

    /// Self-normalized z-score for an account (0.0 if never observed).
    pub fn zscore(&self, pk: &Pubkey) -> f64 {
        self.snapshot(pk).map(|s| s.z).unwrap_or(0.0)
    }

    /// Highest z-score across a bundle's write-locked accounts — drives the
    /// contention-scaled tip bump (the most contended account sets the urgency).
    pub fn max_z(&self, accounts: &[Pubkey]) -> f64 {
        accounts.iter().map(|a| self.zscore(a)).fold(0.0, f64::max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn pk(b: u8) -> Pubkey {
        Pubkey::new_from_array([b; 32])
    }

    #[test]
    fn only_watched_accounts_count() {
        let watched: HashSet<Pubkey> = [pk(1), pk(2)].into_iter().collect();
        let tracker = ContentionTracker::new(watched, 500, 30_000, 0.5, 2.0);

        // pk(3) is not watched and must be ignored.
        tracker.record_writable(&[pk(1), pk(3)]);
        tracker.flush_slot(Instant::now());

        assert!(tracker.snapshot(&pk(1)).is_some());
        assert!(tracker.snapshot(&pk(3)).is_none());
    }

    #[test]
    fn never_observed_is_idle() {
        let watched: HashSet<Pubkey> = [pk(1)].into_iter().collect();
        let tracker = ContentionTracker::new(watched, 400, 30_000, 0.5, 2.0);
        assert_eq!(tracker.congestion(&pk(1)), Congestion::Idle);
    }

    #[test]
    fn burst_pushes_account_to_hot() {
        let watched: HashSet<Pubkey> = [pk(1)].into_iter().collect();
        let tracker = ContentionTracker::new(watched, 400, 30_000, 0.5, 2.0);
        let t0 = Instant::now();

        // Quiet baseline.
        for i in 0..150u64 {
            let now = t0 + Duration::from_millis(i * 400);
            tracker.record_writable(&[pk(1)]);
            tracker.flush_slot(now);
        }
        assert_eq!(tracker.congestion(&pk(1)), Congestion::Quiet);

        // Burst.
        let mut now = t0 + Duration::from_millis(150 * 400);
        for _ in 0..6 {
            now += Duration::from_millis(400);
            for _ in 0..40 {
                tracker.record_writable(&[pk(1)]);
            }
            tracker.flush_slot(now);
        }
        assert_eq!(tracker.congestion(&pk(1)), Congestion::Hot);
    }
}
