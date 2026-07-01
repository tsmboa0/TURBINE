//! Per-transaction compute-unit estimation (hot-path safe: pure, sync, no I/O).
//!
//! Tight limits avoid over-reserving CUs on simple test bundles (Jito auction
//! grouping). Costs mirror Solana's built-in instruction meters where known.

use solana_sdk::instruction::Instruction;

use crate::tx::{compute_budget_program_id, memo_program_id, system_program_id};

/// One ed25519 signature in the transaction budget.
const SIGNATURE_CU: u32 = 720;

/// `SetComputeUnitLimit` instruction meter (prepended by the compiler).
const SET_COMPUTE_UNIT_LIMIT_IX_CU: u32 = 150;

/// System program `Transfer` instruction meter.
const SYSTEM_TRANSFER_IX_CU: u32 = 150;

/// Memo program base meter + one CU per payload byte (v2).
const MEMO_IX_BASE_CU: u32 = 689;

/// Fallback when the program is not modeled yet (swaps etc.) — well below 200k.
const UNKNOWN_IX_CU: u32 = 25_000;

/// Minimum limit Solana accepts for trivial txs after headroom.
const MIN_TX_CU: u32 = 600;

/// Headroom fraction (1/8 ≈ 12.5%) so small meter drift does not fail execution.
const HEADROOM_DIVISOR: u32 = 8;

/// Apply the same headroom policy to an RPC `unitsConsumed` sample.
pub fn limit_from_consumed(consumed: u64) -> u32 {
    let base = u32::try_from(consumed).unwrap_or(u32::MAX);
    let with_headroom = base.saturating_add(base / HEADROOM_DIVISOR);
    with_headroom.max(MIN_TX_CU)
}

/// Estimate the `SetComputeUnitLimit` for a transaction whose body is `body_ixs`
/// (ComputeBudget ix **not** included — the compiler prepends it).
pub fn estimate_transaction_compute_units(body_ixs: &[Instruction]) -> u32 {
    let mut cu = SIGNATURE_CU.saturating_add(SET_COMPUTE_UNIT_LIMIT_IX_CU);
    for ix in body_ixs {
        cu = cu.saturating_add(estimate_instruction_compute_units(ix));
    }
    let with_headroom = cu.saturating_add(cu / HEADROOM_DIVISOR);
    with_headroom.max(MIN_TX_CU)
}

fn estimate_instruction_compute_units(ix: &Instruction) -> u32 {
    if ix.program_id == system_program_id() {
        return SYSTEM_TRANSFER_IX_CU;
    }
    if ix.program_id == memo_program_id() {
        return MEMO_IX_BASE_CU.saturating_add(ix.data.len() as u32);
    }
    if ix.program_id == compute_budget_program_id() {
        // Should not appear in body_ixs; if it does, meter the ix itself.
        return SET_COMPUTE_UNIT_LIMIT_IX_CU;
    }
    UNKNOWN_IX_CU
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx;

    fn payer() -> solana_pubkey::Pubkey {
        solana_pubkey::Pubkey::new_from_array([9u8; 32])
    }

    #[test]
    fn limit_from_consumed_adds_headroom() {
        assert_eq!(limit_from_consumed(800), 900); // +12.5%
        assert_eq!(limit_from_consumed(0), MIN_TX_CU);
    }

    #[test]
    fn self_transfer_tx_is_tight() {
        let ix = tx::system_transfer(&payer(), &payer(), 1);
        let cu = estimate_transaction_compute_units(&[ix]);
        assert!(cu < 5_000, "self-transfer CU should be tight, got {cu}");
        assert!(cu >= MIN_TX_CU);
    }

    #[test]
    fn memo_tx_scales_with_payload() {
        let short = tx::memo(&payer(), b"hi");
        let long = tx::memo(&payer(), b"turbine happy-path-memo");
        let cu_short = estimate_transaction_compute_units(&[short]);
        let cu_long = estimate_transaction_compute_units(&[long]);
        assert!(cu_long > cu_short);
        assert!(cu_long < 5_000, "memo CU should be tight, got {cu_long}");
    }

    #[test]
    fn single_tx_self_transfer_plus_tip() {
        let trade = tx::system_transfer(&payer(), &payer(), 1);
        let tip = tx::system_transfer(&payer(), &payer(), 1_000);
        let cu = estimate_transaction_compute_units(&[trade, tip]);
        assert!(cu < 8_000, "two-transfer tx should stay tight, got {cu}");
    }
}
