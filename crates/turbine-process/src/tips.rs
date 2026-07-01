//! Tip-percentile smoother (plan §5.3 case 2).
//!
//! Each percentile gets its own time-decayed EMA to filter WS outlier spikes.
//! Folds raw [`TipLamports`] from ingest into a smoothed [`TipSnapshot`].

use std::time::Instant;

use turbine_core::ema::DecayEma;
use turbine_core::tips::TipSnapshot;
use turbine_ingest::TipLamports;

/// Smoothing state for the five tip percentiles (p25–p99).
pub struct TipState {
    p25: DecayEma,
    p50: DecayEma,
    p75: DecayEma,
    p95: DecayEma,
    p99: DecayEma,
    last: Option<Instant>,
    half_life_secs: f64,
}

impl TipState {
    pub fn new(half_life_ms: u64) -> Self {
        Self {
            p25: DecayEma::new(),
            p50: DecayEma::new(),
            p75: DecayEma::new(),
            p95: DecayEma::new(),
            p99: DecayEma::new(),
            last: None,
            half_life_secs: half_life_ms as f64 / 1000.0,
        }
    }

    /// Fold one raw tip vector observed at `now` into the per-percentile EMAs.
    pub fn observe(&mut self, raw: TipLamports, now: Instant) {
        let dt = self
            .last
            .map(|t| now.saturating_duration_since(t).as_secs_f64())
            .unwrap_or(0.0);
        self.last = Some(now);
        self.p25.observe(raw.p25 as f64, dt, self.half_life_secs);
        self.p50.observe(raw.p50 as f64, dt, self.half_life_secs);
        self.p75.observe(raw.p75 as f64, dt, self.half_life_secs);
        self.p95.observe(raw.p95 as f64, dt, self.half_life_secs);
        self.p99.observe(raw.p99 as f64, dt, self.half_life_secs);
    }

    /// Current smoothed snapshot in integer lamports.
    pub fn snapshot(&self) -> TipSnapshot {
        TipSnapshot {
            p25: self.p25.value().round().max(0.0) as u64,
            p50: self.p50.value().round().max(0.0) as u64,
            p75: self.p75.value().round().max(0.0) as u64,
            p95: self.p95.value().round().max(0.0) as u64,
            p99: self.p99.value().round().max(0.0) as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smooths_toward_stable_input() {
        let mut s = TipState::new(1_000);
        let raw = TipLamports { p25: 1000, p50: 3000, p75: 10_000, p95: 180_000, p99: 2_000_000, ema50: 7000 };
        let t0 = Instant::now();
        for i in 0..500u64 {
            s.observe(raw, t0 + std::time::Duration::from_millis(i * 100));
        }
        let snap = s.snapshot();
        assert!((snap.p50 as i64 - 3000).abs() <= 1);
        assert!((snap.p99 as i64 - 2_000_000).abs() <= 5);
    }
}
