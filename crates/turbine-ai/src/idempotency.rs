//! Idempotency guard (plan §8.4) — the thing we must not get wrong.
//!
//! A "rejected"/"dropped" bundle may actually have **landed**. Before any retry we
//! confirm none of its signatures are on-chain (via the lifecycle tracker, which is
//! fed by the own-wallet Geyser stream and the searcher bundle-result stream).

use turbine_core::types::LifecycleState;
use turbine_state::HotState;

use crate::contract::SigBytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LandedCheck {
    /// No tracked signature is on-chain — safe to retry.
    SafeToRetry,
    /// At least one signature already landed — must NOT resubmit.
    AlreadyLanded,
}

/// Confirm a bundle is not (partially) on-chain before retrying.
pub fn check(state: &HotState, sigs: &[SigBytes]) -> LandedCheck {
    for sig in sigs {
        if let Some((st, landed_slot)) = state.lifecycle.sig_state(sig) {
            let on_chain = landed_slot.is_some()
                || matches!(
                    st,
                    LifecycleState::Processed
                        | LifecycleState::Confirmed
                        | LifecycleState::Finalized
                );
            if on_chain {
                return LandedCheck::AlreadyLanded;
            }
        }
    }
    LandedCheck::SafeToRetry
}
