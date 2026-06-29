//! Low-level transaction helpers: System transfer + ComputeBudget instructions and
//! keypair loading.

use std::path::Path;
use std::str::FromStr;

use solana_pubkey::Pubkey;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::signature::Keypair;

use turbine_core::error::{Result, TurbineError};

/// The System Program ID is the all-zero pubkey (`111…111`).
#[inline]
pub fn system_program_id() -> Pubkey {
    Pubkey::new_from_array([0u8; 32])
}

/// The ComputeBudget program ID.
#[inline]
pub fn compute_budget_program_id() -> Pubkey {
    Pubkey::from_str("ComputeBudget111111111111111111111111111111")
        .expect("valid ComputeBudget program id")
}

/// The Memo program v2 ID (`MemoSq4…`).
#[inline]
pub fn memo_program_id() -> Pubkey {
    Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr")
        .expect("valid Memo program id")
}

/// Build a Memo program v2 instruction (signer-attached UTF-8 payload).
pub fn memo(signer: &Pubkey, data: &[u8]) -> Instruction {
    Instruction {
        program_id: memo_program_id(),
        accounts: vec![AccountMeta::new(*signer, true)],
        data: data.to_vec(),
    }
}

/// `ComputeBudgetInstruction::SetComputeUnitLimit` (variant index `2`), built by
/// hand to avoid an extra dependency. Used by the AI retry path to apply a
/// `cu_limit` fix when rebuilding a bundle.
pub fn set_compute_unit_limit(units: u32) -> Instruction {
    let mut data = Vec::with_capacity(5);
    data.push(2u8);
    data.extend_from_slice(&units.to_le_bytes());
    Instruction {
        program_id: compute_budget_program_id(),
        accounts: vec![],
        data,
    }
}

/// Build a System Program `Transfer` instruction by hand (avoids an extra dep).
/// Layout: u32 little-endian variant index `2`, then u64 little-endian lamports.
pub fn system_transfer(from: &Pubkey, to: &Pubkey, lamports: u64) -> Instruction {
    let mut data = Vec::with_capacity(12);
    data.extend_from_slice(&2u32.to_le_bytes());
    data.extend_from_slice(&lamports.to_le_bytes());
    Instruction {
        program_id: system_program_id(),
        accounts: vec![AccountMeta::new(*from, true), AccountMeta::new(*to, false)],
        data,
    }
}

/// Load a signing keypair from a Solana CLI JSON keypair file (a 64-byte array).
pub fn load_keypair(path: &Path) -> Result<Keypair> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| TurbineError::Execute(format!("read keypair '{}': {e}", path.display())))?;
    let bytes: Vec<u8> = serde_json::from_str(&raw)
        .map_err(|e| TurbineError::Execute(format!("parse keypair json: {e}")))?;
    Keypair::try_from(bytes.as_slice())
        .map_err(|e| TurbineError::Execute(format!("invalid keypair bytes: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_encoding_is_correct() {
        let from = Pubkey::new_from_array([1u8; 32]);
        let to = Pubkey::new_from_array([2u8; 32]);
        let ix = system_transfer(&from, &to, 5000);
        assert_eq!(ix.program_id, system_program_id());
        assert_eq!(ix.accounts.len(), 2);
        assert!(ix.accounts[0].is_signer && ix.accounts[0].is_writable);
        assert!(!ix.accounts[1].is_signer && ix.accounts[1].is_writable);
        // [2,0,0,0, 0x88,0x13,0,0,0,0,0,0] = variant 2, 5000 lamports
        assert_eq!(&ix.data[0..4], &[2, 0, 0, 0]);
        assert_eq!(u64::from_le_bytes(ix.data[4..12].try_into().unwrap()), 5000);
    }

    #[test]
    fn memo_encoding_is_correct() {
        let signer = Pubkey::new_from_array([3u8; 32]);
        let ix = memo(&signer, b"turbine happy-path-memo");
        assert_eq!(ix.program_id, memo_program_id());
        assert_eq!(ix.accounts.len(), 1);
        assert!(ix.accounts[0].is_signer && ix.accounts[0].is_writable);
        assert_eq!(ix.data, b"turbine happy-path-memo");
    }

    #[test]
    fn compute_unit_limit_encoding_is_correct() {
        let ix = set_compute_unit_limit(200_000);
        assert_eq!(ix.program_id, compute_budget_program_id());
        assert!(ix.accounts.is_empty());
        assert_eq!(ix.data[0], 2);
        assert_eq!(u32::from_le_bytes(ix.data[1..5].try_into().unwrap()), 200_000);
    }
}
