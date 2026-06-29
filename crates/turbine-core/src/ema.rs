//! Time-decayed exponential moving average (plan §5.3).
//!
//! Geyser/tip events arrive irregularly, so a naive fixed-α EMA is wrong. We
//! weight each sample by the elapsed time since the last one using a configurable
//! **half-life**: after one half-life, the previous estimate's weight halves.
//!
//! ```text
//! decay = 0.5 ^ (Δt / half_life)   = exp(-ln2 · Δt / half_life)
//! α     = 1 - decay
//! EMAₜ  = α · xₜ + decay · EMAₜ₋₁
//! ```

/// A single time-decayed EMA accumulator. Cheap to copy; holds no clock of its
/// own — callers pass `dt` so the same primitive serves slots, tips, and tests.
#[derive(Debug, Clone, Copy, Default)]
pub struct DecayEma {
    value: f64,
    initialized: bool,
}

impl DecayEma {
    /// A fresh, uninitialized EMA. The first `observe` seeds it with the sample.
    pub const fn new() -> Self {
        Self { value: 0.0, initialized: false }
    }

    /// Current estimate (0.0 before the first observation).
    #[inline]
    pub fn value(&self) -> f64 {
        self.value
    }

    /// Whether at least one sample has been observed.
    #[inline]
    pub fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Fold a new sample `x` observed `dt_secs` after the previous one, decaying
    /// toward it with the given `half_life_secs`. The first call seeds the value.
    #[inline]
    pub fn observe(&mut self, x: f64, dt_secs: f64, half_life_secs: f64) {
        if !self.initialized {
            self.value = x;
            self.initialized = true;
            return;
        }
        let half_life = half_life_secs.max(1e-9);
        let dt = dt_secs.max(0.0);
        let decay = (-std::f64::consts::LN_2 * dt / half_life).exp();
        let alpha = 1.0 - decay;
        self.value = alpha * x + decay * self.value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_sample_seeds_value() {
        let mut e = DecayEma::new();
        assert!(!e.is_initialized());
        e.observe(42.0, 0.0, 1.0);
        assert_eq!(e.value(), 42.0);
        assert!(e.is_initialized());
    }

    #[test]
    fn converges_to_constant_input() {
        let mut e = DecayEma::new();
        for _ in 0..1000 {
            e.observe(10.0, 0.1, 1.0);
        }
        assert!((e.value() - 10.0).abs() < 1e-6);
    }

    #[test]
    fn half_life_halves_weight() {
        // Seed at 0, then observe 1.0 after exactly one half-life: the new value
        // should be 0.5·1 + 0.5·0 = 0.5.
        let mut e = DecayEma::new();
        e.observe(0.0, 0.0, 2.0);
        e.observe(1.0, 2.0, 2.0);
        assert!((e.value() - 0.5).abs() < 1e-9, "got {}", e.value());
    }

    #[test]
    fn larger_dt_weights_new_sample_more() {
        let mut slow = DecayEma::new();
        slow.observe(0.0, 0.0, 1.0);
        slow.observe(1.0, 0.1, 1.0);

        let mut fast = DecayEma::new();
        fast.observe(0.0, 0.0, 1.0);
        fast.observe(1.0, 5.0, 1.0);

        assert!(fast.value() > slow.value());
    }
}
