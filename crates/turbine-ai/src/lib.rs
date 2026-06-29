//! `turbine-ai` — Component 5, the AI failure analyst & autonomous retry loop.
//!
//! Runs entirely on the **cold path** (plan §8): an LLM call adds hundreds of ms
//! to seconds and must never touch the hot loop. **Every** failure is routed to the
//! AI — there is no deterministic fix path:
//!
//! ```text
//! failure → idempotency guard → AI analyst → governor → record reasoning → RetryDecision
//! ```
//!
//! The model classifies, explains, and *proposes* a fix; the deterministic
//! [`governor::RetryGovernor`] enforces tip/slippage caps, max attempts, a
//! per-minute spend cap, and the global kill switch; and every decision is
//! persisted to the audit log for the web UI. The execution engine's coordinator
//! applies a sanctioned `Resubmit` (rebuild + autonomous resubmit).

pub mod analyst;
pub mod contract;
pub mod engine;
pub mod governor;
pub mod idempotency;
pub mod normalize;

pub use analyst::Analyst;
pub use contract::{
    AnalystVerdict, BundleParams, FailureContext, RetryAction, RetryAdjustments, RetryDecision,
};
pub use engine::AiEngine;
pub use governor::RetryGovernor;
pub use idempotency::LandedCheck;

// Re-export the persisted record types so consumers have one import path.
pub use turbine_core::ai::{AiDecisionRecord, DecisionOutcome};
