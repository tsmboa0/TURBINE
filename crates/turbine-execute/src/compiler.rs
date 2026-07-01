//! Bundle compiler (plan §7.2): build + sign trade txs and the tip tx.

use std::str::FromStr;

use base64::Engine;
use rand::seq::SliceRandom;
use solana_pubkey::Pubkey;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::{v0, VersionedMessage};
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::VersionedTransaction;

use turbine_core::error::{Result, TurbineError};

use crate::compute::estimate_transaction_compute_units;
use crate::tx::{set_compute_unit_limit, system_transfer};

/// Jito hard cap on transactions per bundle.
pub const MAX_BUNDLE_TXS: usize = 5;

/// At most three trade transactions per bundle; the tip tx is always last.
pub const MAX_TRADE_TXS: usize = 3;

/// Resolve CU limit for one transaction: explicit override, else tight estimate.
#[inline]
fn resolve_compute_unit_limit(body_ixs: &[Instruction], override_limit: Option<u32>) -> u32 {
    override_limit.unwrap_or_else(|| estimate_transaction_compute_units(body_ixs))
}

/// Per-transaction CU limits for bundle compile (uniform fallback or per-tx vector).
#[derive(Debug, Clone, Default)]
pub struct CuLimitPlan {
    /// Applied to every tx when `per_tx` is absent or short.
    pub uniform: Option<u32>,
    /// One limit per compiled tx (trade txs in order, then tip).
    pub per_tx: Option<Vec<u32>>,
}

impl CuLimitPlan {
    pub fn uniform(v: Option<u32>) -> Self {
        Self { uniform: v, per_tx: None }
    }

    pub fn per_tx(v: Vec<u32>) -> Self {
        Self { uniform: None, per_tx: Some(v) }
    }

    fn for_tx(&self, tx_index: usize, body_ixs: &[Instruction]) -> u32 {
        if let Some(ref limits) = self.per_tx {
            if let Some(&lim) = limits.get(tx_index) {
                return lim;
            }
        }
        resolve_compute_unit_limit(body_ixs, self.uniform)
    }
}

/// Prepend `SetComputeUnitLimit` as the first instruction in a transaction.
fn with_compute_budget_first(body_ixs: Vec<Instruction>, cu_limit: u32) -> Vec<Instruction> {
    let mut ixs = body_ixs;
    ixs.insert(0, set_compute_unit_limit(cu_limit));
    ixs
}

/// A compiled, signed bundle ready for submission.
pub struct CompiledBundle {
    pub txs: Vec<VersionedTransaction>,
    /// Raw bincode wire bytes per tx (the gRPC packet form).
    pub raw: Vec<Vec<u8>>,
    /// base64-encoded wire bytes (the JSON-RPC fallback form).
    pub base64: Vec<String>,
    /// Signatures of every tx in the bundle (for lifecycle tracking).
    pub sigs: Vec<[u8; 64]>,
    pub tip_account: Pubkey,
    pub tip_lamports: u64,
}

fn sig_bytes(tx: &VersionedTransaction) -> Option<[u8; 64]> {
    tx.signatures.first().and_then(|s| s.as_ref().try_into().ok())
}

/// Build and sign a bundle: up to `max_trades` trade transactions (one per
/// instruction group, capped at [`MAX_TRADE_TXS`]) followed by the tip transfer
/// to a randomly rotated tip account. Every transaction begins with a
/// ComputeBudget `SetComputeUnitLimit` instruction; the tip tx ends with the
/// transfer instruction.
pub fn compile_bundle(
    payer: &Keypair,
    trade_ix_groups: Vec<Vec<Instruction>>,
    tip_lamports: u64,
    tip_accounts: &[Pubkey],
    blockhash: &str,
    max_trades: usize,
    cu_limits: CuLimitPlan,
) -> Result<CompiledBundle> {
    if tip_accounts.is_empty() {
        return Err(TurbineError::Execute("no Jito tip accounts available".into()));
    }
    let bh = Hash::from_str(blockhash)
        .map_err(|e| TurbineError::Execute(format!("bad blockhash '{blockhash}': {e}")))?;

    // Rotate the tip account per submit (reduces tip-account contention).
    let tip_account = *tip_accounts
        .choose(&mut rand::thread_rng())
        .expect("tip_accounts non-empty checked above");

    let payer_pk = payer.pubkey();
    let mut txs: Vec<VersionedTransaction> = Vec::new();

    let mut tx_index = 0usize;
    // Trade transactions (cap at 3 trades, leave room for the tip tx).
    let cap = max_trades.min(MAX_TRADE_TXS).min(MAX_BUNDLE_TXS - 1);
    for group in trade_ix_groups.into_iter().take(cap) {
        if group.is_empty() {
            continue;
        }
        let cu = cu_limits.for_tx(tx_index, &group);
        let group = with_compute_budget_first(group, cu);
        tx_index += 1;
        let msg = v0::Message::try_compile(&payer_pk, &group, &[], bh)
            .map_err(|e| TurbineError::Execute(format!("compile trade message: {e}")))?;
        let tx = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer])
            .map_err(|e| TurbineError::Execute(format!("sign trade tx: {e}")))?;
        txs.push(tx);
    }

    // Tip transaction (always last): ComputeBudget first, transfer last.
    let tip_ix = system_transfer(&payer_pk, &tip_account, tip_lamports);
    let tip_cu = cu_limits.for_tx(tx_index, std::slice::from_ref(&tip_ix));
    let tip_ixs = with_compute_budget_first(vec![tip_ix], tip_cu);
    let tip_msg = v0::Message::try_compile(&payer_pk, &tip_ixs, &[], bh)
        .map_err(|e| TurbineError::Execute(format!("compile tip message: {e}")))?;
    let tip_tx = VersionedTransaction::try_new(VersionedMessage::V0(tip_msg), &[payer])
        .map_err(|e| TurbineError::Execute(format!("sign tip tx: {e}")))?;
    txs.push(tip_tx);

    if txs.len() > MAX_BUNDLE_TXS {
        return Err(TurbineError::Execute(format!(
            "bundle has {} txs, exceeds Jito max {MAX_BUNDLE_TXS}",
            txs.len()
        )));
    }

    // Serialize → raw bytes + base64, and collect signatures.
    let mut raw = Vec::with_capacity(txs.len());
    let mut base64 = Vec::with_capacity(txs.len());
    let mut sigs = Vec::with_capacity(txs.len());
    for tx in &txs {
        let bytes = bincode::serialize(tx)
            .map_err(|e| TurbineError::Execute(format!("serialize tx: {e}")))?;
        base64.push(base64::engine::general_purpose::STANDARD.encode(&bytes));
        raw.push(bytes);
        if let Some(s) = sig_bytes(tx) {
            sigs.push(s);
        }
    }

    Ok(CompiledBundle { txs, raw, base64, sigs, tip_account, tip_lamports })
}

/// Build and sign a **single-transaction** bundle: ComputeBudget, trade
/// instructions, then tip transfer — all in one v0 message (Jito `basic_bundle`
/// layout).
pub fn compile_single_tx_bundle(
    payer: &Keypair,
    trade_ixs: Vec<Instruction>,
    tip_lamports: u64,
    tip_accounts: &[Pubkey],
    blockhash: &str,
    cu_limits: CuLimitPlan,
) -> Result<CompiledBundle> {
    if tip_accounts.is_empty() {
        return Err(TurbineError::Execute("no Jito tip accounts available".into()));
    }
    let bh = Hash::from_str(blockhash)
        .map_err(|e| TurbineError::Execute(format!("bad blockhash '{blockhash}': {e}")))?;

    let tip_account = *tip_accounts
        .choose(&mut rand::thread_rng())
        .expect("tip_accounts non-empty checked above");

    let payer_pk = payer.pubkey();
    let tip_ix = system_transfer(&payer_pk, &tip_account, tip_lamports);
    let mut body: Vec<Instruction> = trade_ixs;
    body.push(tip_ix);
    let cu = cu_limits.for_tx(0, &body);
    let ixs = with_compute_budget_first(body, cu);

    let msg = v0::Message::try_compile(&payer_pk, &ixs, &[], bh)
        .map_err(|e| TurbineError::Execute(format!("compile single-tx message: {e}")))?;
    let tx = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer])
        .map_err(|e| TurbineError::Execute(format!("sign single-tx bundle: {e}")))?;

    let bytes = bincode::serialize(&tx)
        .map_err(|e| TurbineError::Execute(format!("serialize tx: {e}")))?;
    let sigs = sig_bytes(&tx).into_iter().collect();

    Ok(CompiledBundle {
        txs: vec![tx],
        raw: vec![bytes.clone()],
        base64: vec![base64::engine::general_purpose::STANDARD.encode(&bytes)],
        sigs,
        tip_account,
        tip_lamports,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx;
    use solana_sdk::message::VersionedMessage;

    fn bh() -> String {
        // 32 zero bytes base58 = "111...111".
        Pubkey::new_from_array([0u8; 32]).to_string()
    }

    fn first_ix_program_id(tx: &VersionedTransaction) -> Pubkey {
        match &tx.message {
            VersionedMessage::V0(m) => {
                let ix = &m.instructions[0];
                m.account_keys[ix.program_id_index as usize]
            }
            VersionedMessage::Legacy(m) => {
                let ix = &m.instructions[0];
                m.account_keys[ix.program_id_index as usize]
            }
            VersionedMessage::V1(_) => panic!("unexpected V1 message in test"),
        }
    }

    fn last_ix_program_id(tx: &VersionedTransaction) -> Pubkey {
        match &tx.message {
            VersionedMessage::V0(m) => {
                let ix = m.instructions.last().expect("ix");
                m.account_keys[ix.program_id_index as usize]
            }
            VersionedMessage::Legacy(m) => {
                let ix = m.instructions.last().expect("ix");
                m.account_keys[ix.program_id_index as usize]
            }
            VersionedMessage::V1(_) => panic!("unexpected V1 message in test"),
        }
    }

    fn cu_limit_from_tx(tx: &VersionedTransaction) -> u32 {
        match &tx.message {
            VersionedMessage::V0(m) => {
                let ix = &m.instructions[0];
                let prog = m.account_keys[ix.program_id_index as usize];
                assert_eq!(prog, tx::compute_budget_program_id());
                u32::from_le_bytes(ix.data[1..5].try_into().unwrap())
            }
            VersionedMessage::Legacy(m) => {
                let ix = &m.instructions[0];
                u32::from_le_bytes(ix.data[1..5].try_into().unwrap())
            }
            VersionedMessage::V1(_) => panic!("unexpected V1 message in test"),
        }
    }

    #[test]
    fn tip_only_bundle_has_cu_then_tip() {
        let payer = Keypair::new();
        let tips = vec![Pubkey::new_from_array([7u8; 32])];
        let b = compile_bundle(&payer, vec![], 1_000, &tips, &bh(), 4, CuLimitPlan::default()).unwrap();
        assert_eq!(b.txs.len(), 1);
        assert_eq!(first_ix_program_id(&b.txs[0]), tx::compute_budget_program_id());
        assert_eq!(last_ix_program_id(&b.txs[0]), tx::system_program_id());
        let cu = cu_limit_from_tx(&b.txs[0]);
        assert!(cu < 5_000, "tip tx CU should be tight, got {cu}");
    }

    #[test]
    fn trades_plus_tip_respects_cap() {
        let payer = Keypair::new();
        let tips = vec![Pubkey::new_from_array([7u8; 32])];
        let to = Pubkey::new_from_array([9u8; 32]);
        // 10 trade groups, max_trades=4 → capped at MAX_TRADE_TXS(3) + tip = 4.
        let groups: Vec<Vec<Instruction>> = (0..10)
            .map(|_| vec![system_transfer(&payer.pubkey(), &to, 1)])
            .collect();
        let b = compile_bundle(&payer, groups, 1_000, &tips, &bh(), 4, CuLimitPlan::default()).unwrap();
        assert_eq!(b.txs.len(), MAX_TRADE_TXS + 1);
        for tx in &b.txs {
            assert_eq!(first_ix_program_id(tx), tx::compute_budget_program_id());
            assert!(cu_limit_from_tx(tx) < 5_000);
        }
        assert_eq!(last_ix_program_id(b.txs.last().unwrap()), tx::system_program_id());
    }

    #[test]
    fn errors_without_tip_accounts() {
        let payer = Keypair::new();
        assert!(compile_bundle(&payer, vec![], 1_000, &[], &bh(), 4, CuLimitPlan::default()).is_err());
    }

    #[test]
    fn single_tx_bundle_has_cu_trades_then_tip() {
        let payer = Keypair::new();
        let tips = vec![Pubkey::new_from_array([7u8; 32])];
        let to = Pubkey::new_from_array([9u8; 32]);
        let trade = vec![system_transfer(&payer.pubkey(), &to, 1)];
        let b = compile_single_tx_bundle(&payer, trade, 1_000, &tips, &bh(), CuLimitPlan::default()).unwrap();
        assert_eq!(b.txs.len(), 1);
        assert_eq!(first_ix_program_id(&b.txs[0]), tx::compute_budget_program_id());
        assert_eq!(last_ix_program_id(&b.txs[0]), tx::system_program_id());
        let cu = cu_limit_from_tx(&b.txs[0]);
        assert!(cu < 8_000, "single-tx bundle CU should be tight, got {cu}");
    }
}
