//! Write-lock extraction (plan §5.2) — the core contention metric.
//!
//! Given a Geyser transaction update, determine the set of accounts the tx
//! **write-locks**. Solana's message layout encodes writability positionally via
//! the [`MessageHeader`]; ALT-loaded writable accounts come from the tx meta.
//!
//! ```text
//! S  = num_required_signatures
//! RS = num_readonly_signed_accounts
//! RU = num_readonly_unsigned_accounts
//! K  = account_keys.len()
//!
//! writable signers      = keys[0       .. (S - RS)]
//! writable non-signers  = keys[S       .. (K - RU)]
//! ```
//!
//! Final writable set = static writable ∪ `meta.loaded_writable_addresses`.
//!
//! Program IDs (Raydium, pump.fun) are executable and almost always read-only —
//! they are correctly excluded here; only mutable state accounts (pools, vaults,
//! bonding curves, oracles) appear in the writable set.
//!
//! Every access is bounds-checked: with `panic = "abort"` a stray panic on the
//! hot path would kill the daemon, so this module is panic-free by construction.

use solana_pubkey::Pubkey;
use yellowstone_grpc_proto::geyser::SubscribeUpdateTransactionInfo;

/// Convert a 32-byte slice to a [`Pubkey`], or `None` if the length is wrong.
#[inline]
fn to_pubkey(bytes: &[u8]) -> Option<Pubkey> {
    let arr: [u8; 32] = bytes.try_into().ok()?;
    Some(Pubkey::new_from_array(arr))
}

/// Extract the writable account set (static + ALT-loaded) from a tx update.
///
/// Returns an empty vec for malformed/partial updates rather than panicking.
pub fn writable_accounts(info: &SubscribeUpdateTransactionInfo) -> Vec<Pubkey> {
    let mut out = Vec::new();

    if let Some(tx) = info.transaction.as_ref() {
        if let Some(msg) = tx.message.as_ref() {
            if let Some(header) = msg.header.as_ref() {
                let s = header.num_required_signatures as usize;
                let rs = header.num_readonly_signed_accounts as usize;
                let ru = header.num_readonly_unsigned_accounts as usize;
                let keys = &msg.account_keys;
                let k = keys.len();

                // Only trust the positional layout if the header is self-consistent;
                // otherwise we skip the static set and rely on loaded addresses.
                let consistent = rs <= s && s <= k && ru <= k.saturating_sub(s);
                if consistent {
                    // Writable signers: 0 .. (S - RS)
                    let ws_end = s - rs;
                    for key in keys.iter().take(ws_end) {
                        if let Some(pk) = to_pubkey(key) {
                            out.push(pk);
                        }
                    }
                    // Writable non-signers: S .. (K - RU)
                    let wn_end = k - ru;
                    if s < wn_end {
                        for key in keys.iter().take(wn_end).skip(s) {
                            if let Some(pk) = to_pubkey(key) {
                                out.push(pk);
                            }
                        }
                    }
                }
            }
        }
    }

    // ALT-loaded writable addresses (resolved by the validator, present in meta).
    if let Some(meta) = info.meta.as_ref() {
        for key in &meta.loaded_writable_addresses {
            if let Some(pk) = to_pubkey(key) {
                out.push(pk);
            }
        }
    }

    out
}

/// The 64-byte signature of a tx update, if present and well-formed.
pub fn signature_bytes(info: &SubscribeUpdateTransactionInfo) -> Option<[u8; 64]> {
    info.signature.as_slice().try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use yellowstone_grpc_proto::prelude::{Message, MessageHeader, Transaction, TransactionStatusMeta};

    fn key(b: u8) -> Vec<u8> {
        vec![b; 32]
    }

    fn info_with(
        header: MessageHeader,
        account_keys: Vec<Vec<u8>>,
        loaded_writable: Vec<Vec<u8>>,
    ) -> SubscribeUpdateTransactionInfo {
        SubscribeUpdateTransactionInfo {
            signature: vec![7u8; 64],
            is_vote: false,
            transaction: Some(Transaction {
                signatures: vec![vec![7u8; 64]],
                message: Some(Message {
                    header: Some(header),
                    account_keys,
                    recent_blockhash: vec![0u8; 32],
                    instructions: vec![],
                    versioned: false,
                    address_table_lookups: vec![],
                }),
            }),
            meta: Some(TransactionStatusMeta {
                loaded_writable_addresses: loaded_writable,
                ..Default::default()
            }),
            index: 0,
        }
    }

    #[test]
    fn extracts_static_writable_legacy() {
        // S=2, RS=1, RU=1, K=4 → writable signers = [0], writable non-signers = [2]
        let header = MessageHeader {
            num_required_signatures: 2,
            num_readonly_signed_accounts: 1,
            num_readonly_unsigned_accounts: 1,
        };
        let keys = vec![key(0), key(1), key(2), key(3)];
        let info = info_with(header, keys, vec![]);
        let w = writable_accounts(&info);
        assert_eq!(w.len(), 2);
        assert!(w.contains(&to_pubkey(&key(0)).unwrap())); // writable signer
        assert!(w.contains(&to_pubkey(&key(2)).unwrap())); // writable non-signer
        assert!(!w.contains(&to_pubkey(&key(1)).unwrap())); // readonly signer
        assert!(!w.contains(&to_pubkey(&key(3)).unwrap())); // readonly non-signer (program id)
    }

    #[test]
    fn includes_alt_loaded_writable() {
        let header = MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 1,
        };
        // K=2 → writable signers=[0], writable non-signers = 1..(2-1)=empty
        let keys = vec![key(10), key(11)];
        let info = info_with(header, keys, vec![key(20), key(21)]);
        let w = writable_accounts(&info);
        assert!(w.contains(&to_pubkey(&key(10)).unwrap()));
        assert!(w.contains(&to_pubkey(&key(20)).unwrap()));
        assert!(w.contains(&to_pubkey(&key(21)).unwrap()));
        assert_eq!(w.len(), 3);
    }

    #[test]
    fn malformed_header_does_not_panic() {
        // S greater than K — inconsistent; static set skipped, ALT still returned.
        let header = MessageHeader {
            num_required_signatures: 9,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        };
        let keys = vec![key(1), key(2)];
        let info = info_with(header, keys, vec![key(30)]);
        let w = writable_accounts(&info);
        assert_eq!(w, vec![to_pubkey(&key(30)).unwrap()]);
    }

    #[test]
    fn signature_roundtrip() {
        let header = MessageHeader::default();
        let info = info_with(header, vec![], vec![]);
        assert_eq!(signature_bytes(&info), Some([7u8; 64]));
    }
}
